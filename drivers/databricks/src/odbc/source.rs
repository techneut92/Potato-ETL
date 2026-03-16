//! Databricks ODBC read source — zero-copy Arrow RecordBatch streaming via
//! server-side cursors and `arrow-odbc`.
//!
//! ## Performance characteristics
//!
//! - **No 16 MB statement limit** — server-side cursors stream data in chunks.
//! - **Columnar fetch** — `arrow-odbc` binds ODBC column buffers directly to
//!   Arrow arrays (near-zero-copy for numeric types).
//! - **Dedicated OS thread** — ODBC calls are synchronous; we offload the
//!   entire fetch loop to a dedicated thread and bridge back to async via a
//!   bounded `flume` channel.
//!
//! ## Warehouse cold-start handling
//!
//! Databricks SQL warehouses may be stopped or hibernated.  The first query
//! triggers an auto-resume that can take 2-10+ minutes.  During this time the
//! ODBC `execute` call blocks on the dedicated thread.
//!
//! To avoid silent hangs:
//! - A **progress watchdog** logs a message every 30 seconds while waiting for
//!   the first batch ("warehouse may be starting up…").
//! - A **hard timeout** (default: 600 s / 10 min, configurable via
//!   `warehouse_timeout`) aborts the query if the warehouse doesn't respond.
//!
//! ## Connection string
//!
//! ```text
//! databricks://host/sql/1.0/warehouses/abc?token=dapi…&mode=odbc
//! ```

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_odbc::{OdbcReaderBuilder, TextEncoding};
use futures::stream::BoxStream;
use odbc_api::ConnectionOptions;

use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::traits::SourceBuilder;
use crate::conn::{DatabricksConnParams, dbx_full_table};
use super::conn_str::build_odbc_connection_string;
use super::odbc_env;

const DEFAULT_BATCH_SIZE: usize = 65_536;

/// Default maximum seconds to wait for the warehouse to start up and return
/// the first batch.  `0` = disabled (no timeout).  Users can set a positive
/// value via `warehouse_timeout` in driver options if cold-start hangs are
/// a concern.
const DEFAULT_WAREHOUSE_TIMEOUT_SECS: u64 = 0;

/// Interval between "still waiting…" log messages during warehouse startup.
const WATCHDOG_LOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

pub struct DatabricksOdbcSource {
    params: DatabricksConnParams,
    table: String,
    schema_name: String,
    custom_query: Option<String>,
    batch_size: usize,
    /// Maximum seconds to wait for the first batch (warehouse startup).
    /// `0` disables the timeout entirely.
    warehouse_timeout: u64,
}

impl DatabricksOdbcSource {
    pub fn new(params: DatabricksConnParams) -> Self {
        Self {
            params,
            table: String::new(),
            schema_name: String::new(),
            custom_query: None,
            batch_size: DEFAULT_BATCH_SIZE,
            warehouse_timeout: DEFAULT_WAREHOUSE_TIMEOUT_SECS,
        }
    }
}

impl SourceBuilder for DatabricksOdbcSource {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn SourceBuilder> { self.table = t; self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn SourceBuilder> { self.schema_name = s; self }
    fn query(mut self: Box<Self>, q: String) -> Box<dyn SourceBuilder> { self.custom_query = Some(q); self }

    /// No-op: ODBC uses server-side cursors, no client-side cursor column needed.
    fn cursor(self: Box<Self>, _col: String) -> Box<dyn SourceBuilder> {
        tracing::debug!(
            "Databricks ODBC source ignores `cursor` — \
             ODBC server-side cursors handle pagination"
        );
        self
    }

    /// Sets the maximum number of rows per RecordBatch fetched from ODBC.
    /// Default: 65 536 rows.
    fn batch_size(mut self: Box<Self>, n: usize) -> Box<dyn SourceBuilder> {
        self.batch_size = n;
        self
    }

    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SourceBuilder> {
        if let Some(ref dbx) = opts.databricks {
            if let Some(cp) = dbx.chunk_prefetch { self.batch_size = cp; }
            if let Some(wt) = dbx.warehouse_timeout { self.warehouse_timeout = wt; }
        }
        if !opts.init_sql.is_empty() {
            self.params.init_sql = opts.init_sql.clone();
        }
        self
    }

    fn read_schema<'a>(
        &'a self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<SchemaRef>> + Send + 'a>> {
        Box::pin(async {
            anyhow::bail!("Databricks ODBC read_schema: schema is derived from first batch")
        })
    }

    fn exec(self: Box<Self>) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
        let Self { params, table, schema_name, custom_query, batch_size, warehouse_timeout } = *self;

        Box::pin(async_stream::try_stream! {
            // ── Resolve the fully-qualified table name ────────────────────────
            let effective_schema = if !schema_name.is_empty() {
                schema_name.as_str()
            } else {
                params.schema.as_deref().unwrap_or("")
            };
            let table_fqn = dbx_full_table(
                params.catalog.as_deref(),
                effective_schema,
                &table,
            );

            // ── Build the SQL query ──────────────────────────────────────────
            let sql = match custom_query.as_deref() {
                Some(q) => q.to_string(),
                None    => format!("SELECT * FROM {table_fqn}"),
            };
            tracing::debug!(sql = %sql, "DatabricksOdbcSource execute");

            // ── Build ODBC connection string ─────────────────────────────────
            let odbc_conn_str = build_odbc_connection_string(&params, batch_size).await?;

            // ── ODBC is blocking — offload to a dedicated thread ─────────────
            //
            // `odbc-api` and `arrow-odbc` are synchronous.  We run the entire
            // fetch loop on a dedicated OS thread and send RecordBatches back
            // via a bounded `flume` channel so the async stream can yield them.
            //
            // Why flume instead of tokio::sync::mpsc?
            //   flume is lock-free and designed for blocking→async bridges.
            //   It avoids the tokio scheduler wakeup overhead that
            //   `blocking_send` incurs.
            //
            // Why std::thread::spawn instead of tokio::task::spawn_blocking?
            //   A dedicated OS thread avoids tokio blocking-pool scheduling
            //   overhead.  The ODBC fetch loop is long-lived (entire query
            //   execution), so it doesn't benefit from pool reuse.
            let (tx, rx) = flume::bounded::<anyhow::Result<RecordBatch>>(2);

            let init_sql = params.init_sql.clone();
            std::thread::spawn(move || {
                // Wrap in catch_unwind: the Simba Databricks ODBC driver can
                // panic during cursor cleanup ("Driver already destroyed!")
                // when the connection is torn down before the cursor.  This
                // happens when the downstream pipeline fails and the async
                // stream is dropped, causing abrupt ODBC resource cleanup.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_odbc_fetch(
                        &odbc_conn_str,
                        &sql,
                        batch_size,
                        &init_sql,
                        &tx,
                    )
                }));
                match result {
                    Ok(Err(e)) => { let _ = tx.send(Err(e)); }
                    Err(panic_info) => {
                        let msg = if let Some(s) = panic_info.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                            s.to_string()
                        } else {
                            "unknown panic during ODBC cleanup".to_string()
                        };
                        tracing::warn!(
                            "Databricks ODBC thread caught panic (usually harmless \
                             driver cleanup race): {msg}"
                        );
                    }
                    Ok(Ok(())) => {}
                }
            });

            // ── Receive batches with warehouse-startup watchdog ──────────────
            //
            // The first batch may take minutes if the warehouse is cold.
            // We use a select loop with a periodic timer to:
            //   1. Log progress every 30s so the user knows we're not stuck
            //   2. Enforce a hard timeout to avoid infinite hangs
            //
            // After the first batch arrives, subsequent batches stream without
            // the startup timeout (the warehouse is warm by then).

            let deadline = if warehouse_timeout > 0 {
                Some(tokio::time::Instant::now() + std::time::Duration::from_secs(warehouse_timeout))
            } else {
                None
            };
            let started_at = std::time::Instant::now();
            let mut first_batch_received = false;
            let mut watchdog_interval = tokio::time::interval(WATCHDOG_LOG_INTERVAL);
            // First tick completes immediately — skip it.
            watchdog_interval.tick().await;

            // Result carrier: moves data out of `select!` arms so `?` is
            // used in the outer `try_stream!` context (where it compiles).
            enum WatchdogOutcome {
                Batch(anyhow::Result<arrow::record_batch::RecordBatch>),
                ChannelClosed,
                Tick,          // watchdog timer fired — no data yet
                TimedOut(u64), // hard timeout exceeded (seconds)
            }

            loop {
                if !first_batch_received {
                    // Waiting for the first batch — apply watchdog + timeout.
                    let outcome = tokio::select! {
                        result = rx.recv_async() => {
                            match result {
                                Ok(batch_result) => WatchdogOutcome::Batch(batch_result),
                                Err(_) => WatchdogOutcome::ChannelClosed,
                            }
                        }
                        _ = watchdog_interval.tick() => {
                            let elapsed = started_at.elapsed();
                            if warehouse_timeout > 0 {
                                tracing::warn!(
                                    elapsed_secs = elapsed.as_secs(),
                                    timeout_secs = warehouse_timeout,
                                    "Databricks ODBC: still waiting for first batch \
                                     ({elapsed_secs}s / {timeout_secs}s)",
                                    elapsed_secs = elapsed.as_secs(),
                                    timeout_secs = warehouse_timeout,
                                );
                            } else {
                                tracing::warn!(
                                    elapsed_secs = elapsed.as_secs(),
                                    "Databricks ODBC: still waiting for first batch \
                                     ({elapsed_secs}s elapsed)",
                                    elapsed_secs = elapsed.as_secs(),
                                );
                            }

                            if let Some(dl) = deadline {
                                if tokio::time::Instant::now() >= dl {
                                    WatchdogOutcome::TimedOut(warehouse_timeout)
                                } else {
                                    WatchdogOutcome::Tick
                                }
                            } else {
                                WatchdogOutcome::Tick
                            }
                        }
                    };

                    // Handle outcome outside `select!` so `?` / `yield` work.
                    match outcome {
                        WatchdogOutcome::Batch(batch_result) => {
                            let batch = batch_result?;
                            let elapsed = started_at.elapsed();
                            tracing::info!(
                                elapsed_secs = elapsed.as_secs(),
                                "Databricks ODBC: first batch received"
                            );
                            first_batch_received = true;
                            yield batch;
                        }
                        WatchdogOutcome::ChannelClosed => break,
                        WatchdogOutcome::Tick => { /* continue waiting */ }
                        WatchdogOutcome::TimedOut(secs) => {
                            Err(anyhow::anyhow!(
                                "Databricks ODBC: no response within \
                                 {secs}s timeout. Increase \
                                 `warehouse_timeout` in driver options or \
                                 set to 0 to disable."
                            ))?;
                        }
                    }
                } else {
                    // Warehouse is warm — stream without timeout overhead.
                    match rx.recv_async().await {
                        Ok(batch_result) => {
                            let batch = batch_result?;
                            yield batch;
                        }
                        Err(_) => break,
                    }
                }
            }
        })
    }

    fn try_clone(&self) -> Option<Box<dyn SourceBuilder>> { None }
}

// ── ODBC fetch (blocking) ─────────────────────────────────────────────────────

/// Runs the synchronous ODBC fetch loop and sends batches through the channel.
fn run_odbc_fetch(
    odbc_conn_str: &str,
    sql: &str,
    batch_size: usize,
    init_sql: &[String],
    tx: &flume::Sender<anyhow::Result<RecordBatch>>,
) -> anyhow::Result<()> {
    let env = odbc_env();

    let conn = env
        .connect_with_connection_string(odbc_conn_str, ConnectionOptions::default())
        .map_err(|e| anyhow::anyhow!("Databricks ODBC connection failed: {e}"))?;

    // Execute init_sql statements on the ODBC connection.
    if !init_sql.is_empty() {
        tracing::info!(
            count = init_sql.len(),
            "Databricks ODBC: executing {} init_sql statement(s)",
            init_sql.len(),
        );
        for stmt in init_sql {
            tracing::debug!(sql = %stmt, "Databricks ODBC init_sql: executing");
            conn.execute(stmt, (), None)
                .map_err(|e| anyhow::anyhow!("Databricks ODBC init_sql failed: {stmt}: {e}"))?;
        }
    }

    tracing::info!(
        sql = %sql,
        batch_size = batch_size,
        "Databricks ODBC: executing query"
    );

    // Execute the query and get a cursor.
    let cursor = conn
        .execute(sql, (), Some(batch_size))
        .map_err(|e| anyhow::anyhow!("Databricks ODBC execute failed: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("Databricks ODBC: statement returned no result set"))?;

    // `OdbcReaderBuilder` wraps the ODBC cursor and yields Arrow RecordBatches.
    //
    // `with_max_text_size(16384)`:
    //   Generous text buffer for STRING/VARCHAR columns.  The Simba driver
    //   reports STRING as SQL_WVARCHAR(255) — this override ensures arrow-odbc
    //   allocates enough buffer for larger values.
    //
    // `with_max_binary_size(65536)`:
    //   Buffer size for BINARY/VARBINARY columns.
    //
    // `with_payload_text_encoding(TextEncoding::Auto)`:
    //   Automatically detect character encoding instead of assuming UTF-8.
    let reader = OdbcReaderBuilder::new()
        .with_max_num_rows_per_batch(batch_size)
        .with_max_text_size(16384)
        .with_max_binary_size(65536)
        .with_payload_text_encoding(TextEncoding::Auto)
        .build(cursor)
        .map_err(|e| anyhow::anyhow!("Databricks ODBC arrow-odbc reader failed: {e}"))?;

    let mut total_rows: usize = 0;
    let mut batch_count: usize = 0;

    for result in reader {
        let batch: RecordBatch = result
            .map_err(|e| anyhow::anyhow!("Databricks ODBC fetch error on batch {batch_count}: {e}"))?;

        total_rows += batch.num_rows();
        batch_count += 1;

        // If the async receiver is dropped (stream cancelled), stop fetching.
        if tx.send(Ok(batch)).is_err() {
            tracing::debug!(
                batch_count = batch_count,
                total_rows = total_rows,
                "Databricks ODBC: receiver dropped, stopping fetch"
            );
            return Ok(());
        }
    }

    tracing::info!(
        total_rows = total_rows,
        batch_count = batch_count,
        "Databricks ODBC: fetch complete"
    );

    Ok(())
}