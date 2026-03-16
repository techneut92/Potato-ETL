//! Session-managed Thrift client for Databricks SQL warehouses.
//!
//! Provides high-level `execute_dml` and `execute_query` methods on top of
//! the low-level RPC builders in `rpc.rs`.  A Thrift session is opened lazily
//! on the first call and reused for subsequent calls.  The session is closed
//! on drop (best-effort via a spawned task).

use arrow::record_batch::RecordBatch;
use reqwest::Client;

use crate::conn::{build_client, DatabricksConnParams};
use super::arrow_convert::columns_to_record_batch;
use super::rpc::*;

/// Status poll interval for async statements.
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// Default fetch size for query results (introspection queries return small sets).
const INTROSPECTION_FETCH_SIZE: i64 = 10_000;

// ── ThriftClient ─────────────────────────────────────────────────────────────

/// A Thrift-based SQL client for Databricks, with automatic session management.
///
/// The session is opened lazily on the first SQL call and cached for reuse.
/// On drop, the session is closed asynchronously.
pub struct ThriftClient {
    pub params: DatabricksConnParams,
    client: Client,
    url: String,
    auth: String,
    session: Option<SessionHandle>,
}

impl ThriftClient {
    /// Create a new `ThriftClient`.  Does NOT open a session yet.
    pub async fn new(params: DatabricksConnParams) -> anyhow::Result<Self> {
        let client = build_client()?;
        let url = format!("https://{}{}", params.host, params.http_path);
        let auth = params.auth_header(&client).await?;
        Ok(Self {
            params,
            client,
            url,
            auth,
            session: None,
        })
    }

    /// Create with a caller-provided reqwest client.
    pub async fn with_client(params: DatabricksConnParams, client: Client) -> anyhow::Result<Self> {
        let url = format!("https://{}{}", params.host, params.http_path);
        let auth = params.auth_header(&client).await?;
        Ok(Self {
            params,
            client,
            url,
            auth,
            session: None,
        })
    }

    /// Ensure a session is open, opening one if necessary.
    async fn ensure_session(&mut self) -> anyhow::Result<&SessionHandle> {
        if self.session.is_none() {
            tracing::debug!(host = %self.params.host, mode = "thrift", "Opening Thrift session");
            let resp = thrift_post(
                &self.client, &self.url, &self.auth,
                build_open_session(self.params.catalog.as_deref(), self.params.schema.as_deref()),
            ).await?;
            let session = parse_open_session(resp)?;

            // Explicitly set catalog + schema via USE statements.
            // The initialNamespace field in OpenSession is unreliable on
            // Databricks Unity Catalog.
            if let Some(ref cat) = self.params.catalog {
                let use_sql = format!("USE CATALOG `{cat}`");
                tracing::debug!(sql = %use_sql, "Setting default catalog");
                let resp = thrift_post(
                    &self.client, &self.url, &self.auth,
                    build_execute_statement(&session, &use_sql),
                ).await?;
                let use_op = parse_execute_statement(resp)?;
                self.poll_until_finished(&use_op).await?;
                let _ = thrift_post(
                    &self.client, &self.url, &self.auth,
                    build_close_operation(&use_op),
                ).await;
            }
            if let Some(ref sch) = self.params.schema {
                let use_sql = format!("USE SCHEMA `{sch}`");
                tracing::debug!(sql = %use_sql, "Setting default schema");
                let resp = thrift_post(
                    &self.client, &self.url, &self.auth,
                    build_execute_statement(&session, &use_sql),
                ).await?;
                let use_op = parse_execute_statement(resp)?;
                self.poll_until_finished(&use_op).await?;
                let _ = thrift_post(
                    &self.client, &self.url, &self.auth,
                    build_close_operation(&use_op),
                ).await;
            }

            self.session = Some(session);
        }
        Ok(self.session.as_ref().unwrap())
    }

    /// Refresh the auth token (for long-running sinks with OAuth2).
    async fn refresh_auth(&mut self) -> anyhow::Result<()> {
        self.auth = self.params.auth_header(&self.client).await?;
        Ok(())
    }

    /// Poll GetOperationStatus until FINISHED.
    async fn poll_until_finished(&self, op: &OperationHandle) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(POLL_INTERVAL).await;
            let resp = thrift_post(
                &self.client, &self.url, &self.auth,
                build_get_operation_status(op),
            ).await?;
            let (state, err) = parse_get_operation_status(resp)?;
            match state {
                OP_STATE_FINISHED => return Ok(()),
                OP_STATE_CANCELED => anyhow::bail!("Operation cancelled"),
                OP_STATE_CLOSED => anyhow::bail!("Operation closed unexpectedly"),
                OP_STATE_ERROR => {
                    let msg = err.unwrap_or_else(|| "unknown".into());
                    anyhow::bail!("Operation error: {}", msg);
                },
                _ => {} // still running
            }
        }
    }

    /// Execute a DML/DDL statement (fire-and-forget, discards results).
    ///
    /// Used for: INSERT, MERGE, CREATE TABLE, DROP TABLE, TRUNCATE.
    pub async fn execute_dml(&mut self, sql: &str) -> anyhow::Result<()> {
        self.refresh_auth().await?;
        let session = self.ensure_session().await?.clone();

        tracing::debug!(sql = %truncate_sql(sql, 200), "Thrift execute_dml");

        let resp = thrift_post(
            &self.client, &self.url, &self.auth,
            build_execute_statement(&session, sql),
        ).await?;
        let op = parse_execute_statement(resp)?;

        // Poll until finished.
        self.poll_until_finished(&op).await.map_err(|e| {
            tracing::error!(error = %e, "Thrift DML failed");
            e
        })?;

        // Close the operation handle.
        let _ = thrift_post(
            &self.client, &self.url, &self.auth,
            build_close_operation(&op),
        ).await;

        Ok(())
    }

    /// Execute a SQL query and return all result batches.
    ///
    /// Used for: DESCRIBE TABLE (introspection), SELECT queries.
    pub async fn execute_query(&mut self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        self.refresh_auth().await?;
        let session = self.ensure_session().await?.clone();

        tracing::debug!(sql = %truncate_sql(sql, 200), "Thrift execute_query");

        let resp = thrift_post(
            &self.client, &self.url, &self.auth,
            build_execute_statement(&session, sql),
        ).await?;
        let op = parse_execute_statement(resp)?;

        // Poll until finished.
        self.poll_until_finished(&op).await.map_err(|e| {
            tracing::error!(error = %e, "Thrift query failed");
            e
        })?;

        // Get column metadata (names + types) before fetching rows.
        let col_meta = match thrift_post(
            &self.client, &self.url, &self.auth,
            build_get_result_set_metadata(&op),
        ).await {
            Ok(resp) => match parse_get_result_set_metadata(resp) {
                Ok(meta) if !meta.is_empty() => Some(meta),
                _ => None,
            },
            Err(_) => None,
        };

        // Fetch all result pages.
        let mut batches = Vec::new();
        loop {
            let resp = thrift_post(
                &self.client, &self.url, &self.auth,
                build_fetch_results(&op, INTROSPECTION_FETCH_SIZE),
            ).await?;
            let (has_more, columns) = parse_fetch_results(resp)?;

            if columns.is_empty() { break; }
            let n_rows = columns[0].len();
            if n_rows == 0 { break; }

            batches.push(columns_to_record_batch(columns, col_meta.as_deref())?);
            if !has_more { break; }
        }

        // Close the operation handle.
        let _ = thrift_post(
            &self.client, &self.url, &self.auth,
            build_close_operation(&op),
        ).await;

        Ok(batches)
    }

    /// Explicitly close the session.  Called by the sink's `flush` or on drop.
    pub async fn close_session(&mut self) {
        if let Some(session) = self.session.take() {
            let _ = thrift_post(
                &self.client, &self.url, &self.auth,
                build_close_session(&session),
            ).await;
        }
    }
}

impl Drop for ThriftClient {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            let client = self.client.clone();
            let url = self.url.clone();
            let auth = self.auth.clone();
            tokio::spawn(async move {
                let _ = thrift_post(&client, &url, &auth, build_close_session(&session)).await;
            });
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn truncate_sql(sql: &str, max: usize) -> String {
    if sql.len() <= max {
        sql.to_string()
    } else {
        format!("{}...", &sql[..max])
    }
}

// ── Table introspection ──────────────────────────────────────────────────────

/// Introspect table columns via `DESCRIBE TABLE` through the Thrift client.
///
/// Returns `None` if the table does not exist.
pub async fn introspect_table_columns(
    client: &mut ThriftClient,
    full_table: &str,
) -> anyhow::Result<Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>> {
    use arrow::array::StringArray;
    use potato_etl_common::db::common::alignment::TargetColumn;

    let sql = format!("DESCRIBE TABLE {full_table}");
    let batches = match client.execute_query(&sql).await {
        Ok(b) => b,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("TABLE_OR_VIEW_NOT_FOUND")
                || msg.contains("does not exist")
                || msg.contains("not found")
            {
                return Ok(None);
            }
            return Err(e);
        }
    };

    let mut cols: Vec<TargetColumn> = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let name_idx = schema.index_of("col_name").unwrap_or(0);
        let type_idx = schema.index_of("data_type").unwrap_or(1);
        let names = batch.column(name_idx).as_any().downcast_ref::<StringArray>();
        let types = batch.column(type_idx).as_any().downcast_ref::<StringArray>();
        if let (Some(names), Some(types)) = (names, types) {
            for i in 0..batch.num_rows() {
                let name = names.value(i);
                if name.is_empty() || name.starts_with('#') {
                    break;
                }
                cols.push(TargetColumn {
                    name: name.to_string(),
                    data_type: types.value(i).to_ascii_lowercase(),
                    nullable: true,
                    has_default: false,
                });
            }
        }
    }
    if cols.is_empty() { Ok(None) } else { Ok(Some(cols)) }
}