//! Databricks REST SQL Statement Execution API transport.
//!
//! This module provides [`StatementClient`] — a thin wrapper around
//! [`DatabricksConnParams`] + [`reqwest::Client`] that encapsulates all
//! interaction with the `/api/2.0/sql/statements` endpoint.
//!
//! Sub-modules implement [`SourceBuilder`], [`SinkBuilder`], and
//! [`Scd2Builder`] on top of `StatementClient`.

pub mod source;
pub mod sink;
pub mod scd2;

use std::io::Cursor;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use reqwest::Client;
use serde::Deserialize;

use crate::conn::{build_client, DatabricksConnParams};

// ── API response types ───────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct StatementResponse {
    pub statement_id: String,
    pub status: StatementStatus,
    #[serde(default)]
    pub result: Option<StatementResult>,
}

#[derive(Deserialize)]
pub struct StatementStatus {
    pub state: String,
    #[serde(default)]
    pub error: Option<StatementError>,
}

#[derive(Deserialize, Default)]
pub struct StatementResult {
    #[serde(default)]
    pub external_links: Vec<ExternalLink>,
    #[serde(default)]
    pub next_chunk_internal_link: Option<String>,
}

#[derive(Deserialize)]
pub struct StatementError {
    pub error_code: String,
    pub message: String,
}

#[derive(Clone, Deserialize)]
pub struct ExternalLink {
    pub chunk_index: usize,
    pub external_link: String,
    #[allow(dead_code)]
    pub row_count: Option<usize>,
    #[serde(default)]
    pub next_chunk_internal_link: Option<String>,
}

#[derive(Deserialize)]
pub struct ChunkLinksResponse {
    #[serde(default)]
    pub external_links: Vec<ExternalLink>,
    #[serde(default)]
    pub next_chunk_internal_link: Option<String>,
}

// ── StatementClient ──────────────────────────────────────────────────────────

/// REST API transport for Databricks SQL warehouses.
///
/// Wraps connection params + an HTTP client and provides methods to submit
/// statements, poll for completion, download Arrow IPC chunks, and introspect
/// table schemas.
pub struct StatementClient {
    pub params: DatabricksConnParams,
    client: Client,
}

impl StatementClient {
    /// Create a `StatementClient` with a default HTTPS-only reqwest client.
    pub fn new(params: DatabricksConnParams) -> anyhow::Result<Self> {
        let client = build_client()?;
        Ok(Self { params, client })
    }

    /// Create a `StatementClient` with a caller-provided reqwest client
    /// (e.g. with custom pool size, timeouts, or proxy config).
    pub fn with_client(params: DatabricksConnParams, client: Client) -> Self {
        Self { params, client }
    }

    /// Execute a SQL statement and collect all result batches (handles pagination).
    pub async fn execute_statement(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let resp = self.submit_statement(sql).await?;
        let mut all_links: Vec<ExternalLink> = resp
            .result
            .as_ref()
            .map(|r| r.external_links.clone())
            .unwrap_or_default();
        let mut next_link = resolve_next_link(
            resp.result
                .as_ref()
                .and_then(|r| r.next_chunk_internal_link.as_deref()),
            &all_links,
        );
        while let Some(link) = next_link {
            let page = self.fetch_chunk_links_page(&link).await?;
            next_link = resolve_next_link(
                page.next_chunk_internal_link.as_deref(),
                &page.external_links,
            );
            all_links.extend(page.external_links);
        }
        let mut batches = Vec::new();
        for link in all_links {
            batches.extend(Self::download_chunk(&self.client, &link).await?);
        }
        Ok(batches)
    }

    /// Execute a DML/DDL statement (fire-and-forget, discards results).
    pub async fn execute_dml(&self, sql: &str) -> anyhow::Result<()> {
        self.submit_statement(sql).await?;
        Ok(())
    }

    /// Execute all `init_sql` statements from the connection params.
    ///
    /// Each statement is submitted as a separate DML call.  For the REST API
    /// transport, these are best-effort: statements like `SET spark.sql.…`
    /// take effect within their own execution context, but the Databricks
    /// Statement API does not share session state across separate API calls.
    ///
    /// For ODBC/Thrift transports (which have persistent connections), the
    /// init_sql is executed directly on the connection instead.
    pub async fn execute_init_sql(&self) -> anyhow::Result<()> {
        if self.params.init_sql.is_empty() {
            return Ok(());
        }
        tracing::info!(
            count = self.params.init_sql.len(),
            "Databricks: executing {} init_sql statement(s)",
            self.params.init_sql.len(),
        );
        for stmt in &self.params.init_sql {
            tracing::debug!(sql = %stmt, "Databricks init_sql: executing");
            self.execute_dml(stmt).await
                .map_err(|e| anyhow::anyhow!("Databricks init_sql failed: {stmt}: {e}"))?;
        }
        Ok(())
    }

    /// Submit a SQL statement and poll until completion.
    pub async fn submit_statement(&self, sql: &str) -> anyhow::Result<StatementResponse> {
        let url = format!("{}/api/2.0/sql/statements", self.params.api_base());
        let auth = self.params.auth_header(&self.client).await?;
        let mut body = serde_json::json!({
            "warehouse_id": self.params.warehouse_id,
            "statement": sql,
            "format": "ARROW_STREAM",
            "disposition": "EXTERNAL_LINKS",
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
        });
        if let Some(cat) = &self.params.catalog {
            body["catalog"] = serde_json::Value::String(cat.clone());
        }
        if let Some(sch) = &self.params.schema {
            body["schema"] = serde_json::Value::String(sch.clone());
        }

        let resp: StatementResponse = self
            .client
            .post(&url)
            .header("Authorization", &auth)
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        let stmt_id = resp.statement_id.clone();
        let mut current = resp;
        loop {
            match current.status.state.as_str() {
                "SUCCEEDED" => return Ok(current),
                "PENDING" | "RUNNING" => {
                    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
                    let auth = self.params.auth_header(&self.client).await?;
                    current = self
                        .client
                        .get(format!(
                            "{}/api/2.0/sql/statements/{stmt_id}",
                            self.params.api_base()
                        ))
                        .header("Authorization", &auth)
                        .send()
                        .await?
                        .error_for_status()?
                        .json()
                        .await?;
                }
                state => {
                    let msg = current
                        .status
                        .error
                        .as_ref()
                        .map(|e| format!("{}: {}", e.error_code, e.message))
                        .unwrap_or_default();
                    anyhow::bail!("Statement {stmt_id} failed: {state}: {msg}");
                }
            }
        }
    }

    /// Fetch the next page of chunk external links.
    pub async fn fetch_chunk_links_page(
        &self,
        internal_link: &str,
    ) -> anyhow::Result<ChunkLinksResponse> {
        let chunk_url = format!("{}{internal_link}", self.params.api_base());
        let mut backoff = std::time::Duration::from_secs(2);
        let max_retries = 5u32;
        let mut attempt = 0u32;
        loop {
            let auth = self.params.auth_header(&self.client).await?;
            match self
                .client
                .get(&chunk_url)
                .header("Authorization", &auth)
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return resp
                            .json()
                            .await
                            .map_err(|e| anyhow::anyhow!("JSON parse: {e}"));
                    }
                    if matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504) {
                        attempt += 1;
                        if attempt > max_retries {
                            anyhow::bail!("HTTP {status} after {max_retries} retries");
                        }
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                        continue;
                    }
                    anyhow::bail!("HTTP {status}");
                }
                Err(e) if e.is_timeout() || e.is_connect() => {
                    attempt += 1;
                    if attempt > max_retries {
                        anyhow::bail!("Network error after {max_retries}: {e}");
                    }
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Download and parse a single Arrow IPC chunk from an external link.
    pub async fn download_chunk(
        client: &Client,
        link: &ExternalLink,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        let bytes = client
            .get(&link.external_link)
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let mut reader = StreamReader::try_new(Cursor::new(&bytes[..]), None)?;
        let mut batches = Vec::new();
        for result in &mut reader {
            batches.push(result?);
        }
        Ok(batches)
    }
}

// ── Table introspection ──────────────────────────────────────────────────────

/// Introspect table columns via `DESCRIBE TABLE`.  Returns `None` if the table
/// does not exist.
pub async fn introspect_table_columns(
    api: &StatementClient,
    full_table: &str,
) -> anyhow::Result<Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>> {
    use arrow::array::StringArray;
    use potato_etl_common::db::common::alignment::TargetColumn;

    let sql = format!("DESCRIBE TABLE {full_table}");
    let batches = match api.execute_statement(&sql).await {
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
        let names = batch
            .column(name_idx)
            .as_any()
            .downcast_ref::<StringArray>();
        let types = batch
            .column(type_idx)
            .as_any()
            .downcast_ref::<StringArray>();
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
    if cols.is_empty() {
        Ok(None)
    } else {
        Ok(Some(cols))
    }
}

// ── Link helpers ─────────────────────────────────────────────────────────────

pub fn resolve_next_link(
    result_level_link: Option<&str>,
    links: &[ExternalLink],
) -> Option<String> {
    links
        .last()
        .and_then(|l| l.next_chunk_internal_link.clone())
        .or_else(|| result_level_link.map(|s| s.to_string()))
}