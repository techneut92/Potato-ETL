//! Databricks Thrift source — reads via the TCLIService Thrift binary protocol.
//!
//! Performance optimisations:
//! - **Concurrent prefetch**: while the current batch is yielded downstream
//!   (and processed by the sink), the next page is already being fetched from
//!   Databricks in a background task.  This hides network latency almost
//!   entirely — the source is never idle waiting for the network while the
//!   sink has data to process.
//! - Columns are converted directly to Arrow arrays, no intermediate `Option<T>` vecs
//! - String column data uses zero-copy `Bytes` slices from the response body
//! - Timestamp strings use a hand-rolled parser (~5× faster than chrono)
//! - Large fetch batches (100k rows default) to minimize RPC round trips

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use reqwest::Client;
use tokio::task::JoinHandle;

use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::traits::SourceBuilder;
use crate::conn::{DatabricksConnParams, dbx_full_table};

use super::arrow_convert::columns_to_record_batch;
use super::rpc::*;

/// Default number of rows per FetchResults RPC.
const DEFAULT_FETCH_SIZE: i64 = 100_000;

/// Status poll interval.
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// Result of a prefetch task: the parsed RecordBatch ready for downstream,
/// plus the has_more flag indicating whether there are more pages.
type PrefetchResult = anyhow::Result<(bool, Option<RecordBatch>)>;

pub struct DatabricksThriftSource {
    params: DatabricksConnParams,
    table: String,
    schema_name: String,
    custom_query: Option<String>,
    fetch_size: i64,
}

impl DatabricksThriftSource {
    pub fn new(params: DatabricksConnParams) -> Self {
        Self {
            params,
            table: String::new(),
            schema_name: String::new(),
            custom_query: None,
            fetch_size: DEFAULT_FETCH_SIZE,
        }
    }
}

impl SourceBuilder for DatabricksThriftSource {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn SourceBuilder> { self.table = t; self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn SourceBuilder> { self.schema_name = s; self }
    fn query(mut self: Box<Self>, q: String) -> Box<dyn SourceBuilder> { self.custom_query = Some(q); self }
    fn cursor(self: Box<Self>, _c: String) -> Box<dyn SourceBuilder> { self }
    fn batch_size(mut self: Box<Self>, n: usize) -> Box<dyn SourceBuilder> {
        // Do NOT override fetch_size with the pipeline's generic batch_size.
        // The Thrift protocol benefits from large fetch sizes (100K default) to
        // minimise RPC round trips.  The generic batch_size controls channel
        // transport granularity, not the optimal wire-level fetch size.
        //
        // Users who DO want to tune the Thrift fetch size should use the
        // driver-specific option:
        //   options:
        //     databricks:
        //       thrift_fetch_size: 200000
        //
        // Only apply batch_size if it is LARGER than the current fetch_size
        // (i.e. user explicitly wants bigger batches), never shrink it.
        if (n as i64) > self.fetch_size {
            self.fetch_size = n as i64;
        }
        self
    }

    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SourceBuilder> {
        if let Some(ref dbx) = opts.databricks {
            if let Some(fs) = dbx.thrift_fetch_size { self.fetch_size = fs as i64; }
        }
        self
    }

    fn read_schema<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<SchemaRef>> + Send + 'a>> {
        Box::pin(async { anyhow::bail!("Databricks Thrift read_schema: schema is derived from first batch") })
    }

    fn exec(self: Box<Self>) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
        let Self { params, table, schema_name, custom_query, fetch_size } = *self;

        let stream = async_stream::try_stream! {
            let sql = build_query(
                &table, &schema_name,
                params.catalog.as_deref(), params.schema.as_deref(),
                custom_query.as_deref(),
            );

            // Build a tuned HTTP client for the Thrift endpoint.
            let client = Client::builder()
                .https_only(true)
                .pool_max_idle_per_host(4)
                .tcp_nodelay(true)
                .connect_timeout(std::time::Duration::from_secs(30))
                .timeout(std::time::Duration::from_secs(300))
                .build()?;

            // Thrift endpoint URL.
            let url = format!("https://{}{}", params.host, params.http_path);
            let auth = params.auth_header(&client).await?;

            tracing::debug!(host = %params.host, mode = "thrift", "Opening Thrift session");

            // 1. Open session (with catalog + schema so custom queries resolve correctly)
            let resp = thrift_post(
                &client, &url, &auth,
                build_open_session(params.catalog.as_deref(), params.schema.as_deref()),
            ).await?;
            let session = parse_open_session(resp)?;

            // Guard: always close session on exit
            let _session_guard = ThriftSessionGuard {
                client: client.clone(), url: url.clone(), auth: auth.clone(),
                session: session.clone(),
            };

            // 1b. Explicitly set catalog + schema via USE statements.
            //     The initialNamespace field in OpenSession is unreliable on
            //     Databricks Unity Catalog — USE CATALOG / USE SCHEMA is the
            //     only way to guarantee the default namespace for unqualified
            //     table references in custom queries.
            if let Some(cat) = params.catalog.as_deref() {
                let use_sql = format!("USE CATALOG `{cat}`");
                tracing::debug!(sql = %use_sql, "Setting default catalog");
                let resp = thrift_post(&client, &url, &auth, build_execute_statement(&session, &use_sql)).await?;
                let use_op = parse_execute_statement(resp)?;
                poll_until_finished(&client, &url, &auth, &use_op).await?;
                let _ = thrift_post(&client, &url, &auth, build_close_operation(&use_op)).await;
            }
            if let Some(sch) = params.schema.as_deref() {
                let use_sql = format!("USE SCHEMA `{sch}`");
                tracing::debug!(sql = %use_sql, "Setting default schema");
                let resp = thrift_post(&client, &url, &auth, build_execute_statement(&session, &use_sql)).await?;
                let use_op = parse_execute_statement(resp)?;
                poll_until_finished(&client, &url, &auth, &use_op).await?;
                let _ = thrift_post(&client, &url, &auth, build_close_operation(&use_op)).await;
            }

            tracing::debug!(sql = %sql, "Executing statement via Thrift");

            // 2. Execute statement (async)
            let resp = thrift_post(&client, &url, &auth, build_execute_statement(&session, &sql)).await?;
            let op = parse_execute_statement(resp)?;

            // Guard: always close operation on exit
            let _op_guard = ThriftOperationGuard {
                client: client.clone(), url: url.clone(), auth: auth.clone(),
                op: op.clone(),
            };

            // 3. Poll until finished
            loop {
                tokio::time::sleep(POLL_INTERVAL).await;
                let resp = thrift_post(&client, &url, &auth, build_get_operation_status(&op)).await?;
                let (state, err) = parse_get_operation_status(resp)?;
                match state {
                    OP_STATE_FINISHED => break,
                    OP_STATE_CANCELED => Err(anyhow::anyhow!("Thrift operation cancelled"))?,
                    OP_STATE_CLOSED   => Err(anyhow::anyhow!("Thrift operation closed unexpectedly"))?,
                    OP_STATE_ERROR    => {
                        let msg = err.unwrap_or_else(|| "unknown error (no message from server)".into());
                        tracing::error!(error = %msg, "Thrift operation failed");
                        Err(anyhow::anyhow!("Thrift operation error: {}", msg))?
                    },
                    _ => {} // still running
                }
            }

            // 4. Get column metadata (names + types) from the operation
            let col_meta = match thrift_post(
                &client, &url, &auth,
                build_get_result_set_metadata(&op),
            ).await {
                Ok(resp) => match parse_get_result_set_metadata(resp) {
                    Ok(meta) if !meta.is_empty() => {
                        tracing::debug!(columns = ?meta, "Got column metadata from Thrift");
                        Some(Arc::from(meta))
                    }
                    Ok(_) => {
                        tracing::warn!("GetResultSetMetadata returned no columns, using synthetic names");
                        None
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to parse result set metadata, using synthetic names");
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!(error = %e, "GetResultSetMetadata RPC failed, using synthetic names");
                    None
                }
            };

            tracing::debug!("Query finished, fetching results");

            // ── 5. Concurrent prefetch loop ──────────────────────────────
            //
            // Strategy: while the current batch is being yielded (and
            // processed by the downstream sink), a background tokio task
            // is already fetching + parsing the next page from Databricks.
            //
            //   Time ──────────────────────────────────────────────────►
            //   ┌──────────┐  ┌──────────┐  ┌──────────┐
            //   │ fetch p1 │  │ fetch p2 │  │ fetch p3 │  ← network
            //   └────┬─────┘  └────┬─────┘  └────┬─────┘
            //        │ parse       │ parse       │ parse
            //        ▼             ▼             ▼
            //   ┌──────────┐  ┌──────────┐  ┌──────────┐
            //   │ yield p1 │  │ yield p2 │  │ yield p3 │  ← downstream
            //   └──────────┘  └──────────┘  └──────────┘
            //
            // Without prefetch, fetch and yield are sequential (2× wall time).

            let mut total_rows = 0usize;

            // Kick off the first fetch (no prefetch yet — nothing to overlap with).
            let resp = thrift_post(&client, &url, &auth, build_fetch_results(&op, fetch_size)).await?;
            let (mut has_more, columns) = parse_fetch_results(resp)?;

            if columns.is_empty() || columns[0].len() == 0 {
                tracing::info!(total_rows = 0, "Thrift fetch complete (no rows)");
            } else {
                // Start prefetching the second page in the background.
                let mut prefetch: Option<JoinHandle<PrefetchResult>> =
                    if has_more {
                        Some(spawn_prefetch(
                            client.clone(), url.clone(), auth.clone(), op.clone(), fetch_size,
                            col_meta.clone(),
                        ))
                    } else {
                        None
                    };

                // Convert + yield the first page.
                let batch = columns_to_record_batch(columns, col_meta.as_ref().map(|m| &m[..]))?;
                total_rows += batch.num_rows();
                yield batch;

                // Drain remaining pages: await prefetch → start next prefetch → yield.
                loop {
                    let handle = match prefetch.take() {
                        Some(h) => h,
                        None => break, // no more pages
                    };

                    // Await the in-flight prefetch.
                    let prefetch_result: PrefetchResult = handle.await
                        .map_err(|e| anyhow::anyhow!("Prefetch task panicked: {e}"))?;
                    let (more, batch) = prefetch_result?;
                    has_more = more;

                    if let Some(batch) = batch {
                        // Immediately start fetching the NEXT page before we spend
                        // time converting + yielding this one.
                        prefetch = if has_more {
                            Some(spawn_prefetch(
                                client.clone(), url.clone(), auth.clone(), op.clone(), fetch_size,
                                col_meta.clone(),
                            ))
                        } else {
                            None
                        };

                        // Convert + yield this page (overlaps with the prefetch above).
                        total_rows += batch.num_rows();
                        yield batch;
                    } else {
                        break;
                    }
                }

                tracing::info!(total_rows, "Thrift fetch complete");
            }
        };
        Box::pin(stream)
    }

    fn try_clone(&self) -> Option<Box<dyn SourceBuilder>> { None }
}

// ── Prefetch helper ──────────────────────────────────────────────────────────

/// Spawn a background task that fetches, parses, AND converts the next page of
/// Thrift results into an Arrow `RecordBatch`.
///
/// By doing the Arrow conversion inside the prefetch task, the CPU work of
/// `columns_to_record_batch` overlaps with downstream processing (sink COPY,
/// channel transfer).  The `col_meta` Arc is cheap to clone.
fn spawn_prefetch(
    client: Client,
    url: String,
    auth: String,
    op: OperationHandle,
    fetch_size: i64,
    col_meta: Option<Arc<[ColumnMeta]>>,
) -> JoinHandle<PrefetchResult> {
    tokio::spawn(async move {
        let resp = thrift_post(&client, &url, &auth, build_fetch_results(&op, fetch_size)).await?;
        let (has_more, columns) = parse_fetch_results(resp)?;

        if columns.is_empty() || columns[0].len() == 0 {
            Ok((has_more, None))
        } else {
            let batch = columns_to_record_batch(columns, col_meta.as_ref().map(|m| &m[..]))?;
            Ok((has_more, Some(batch)))
        }
    })
}

// ── Query builder ────────────────────────────────────────────────────────────

fn build_query(
    table: &str,
    schema: &str,
    catalog: Option<&str>,
    conn_schema: Option<&str>,
    custom_q: Option<&str>,
) -> String {
    let eff = if !schema.is_empty() { schema } else { conn_schema.unwrap_or("") };
    match custom_q {
        Some(q) => q.to_string(),
        None => format!("SELECT * FROM {}", dbx_full_table(catalog, eff, table)),
    }
}

/// Poll GetOperationStatus until FINISHED (used for USE CATALOG/SCHEMA).
async fn poll_until_finished(
    client: &Client,
    url: &str,
    auth: &str,
    op: &OperationHandle,
) -> anyhow::Result<()> {
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        let resp = thrift_post(client, url, auth, build_get_operation_status(op)).await?;
        let (state, err) = parse_get_operation_status(resp)?;
        match state {
            OP_STATE_FINISHED => return Ok(()),
            OP_STATE_CANCELED => Err(anyhow::anyhow!("USE statement cancelled"))?,
            OP_STATE_CLOSED   => Err(anyhow::anyhow!("USE statement closed unexpectedly"))?,
            OP_STATE_ERROR    => {
                let msg = err.unwrap_or_else(|| "unknown".into());
                Err(anyhow::anyhow!("USE statement error: {}", msg))?
            },
            _ => {} // still running
        }
    }
}

// ── Cleanup guards ───────────────────────────────────────────────────────────

/// RAII guard that closes the Thrift operation on drop (best-effort).
struct ThriftOperationGuard {
    client: Client,
    url: String,
    auth: String,
    op: OperationHandle,
}

impl Drop for ThriftOperationGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let url = self.url.clone();
        let auth = self.auth.clone();
        let op = self.op.clone();
        tokio::spawn(async move {
            let _ = thrift_post(&client, &url, &auth, build_close_operation(&op)).await;
        });
    }
}

/// RAII guard that closes the Thrift session on drop (best-effort).
struct ThriftSessionGuard {
    client: Client,
    url: String,
    auth: String,
    session: SessionHandle,
}

impl Drop for ThriftSessionGuard {
    fn drop(&mut self) {
        let client = self.client.clone();
        let url = self.url.clone();
        let auth = self.auth.clone();
        let session = self.session.clone();
        tokio::spawn(async move {
            let _ = thrift_post(&client, &url, &auth, build_close_session(&session)).await;
        });
    }
}