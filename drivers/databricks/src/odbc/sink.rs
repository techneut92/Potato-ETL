//! Databricks ODBC write sink — bulk INSERT via ODBC columnar parameter
//! binding, with DDL and MERGE operations delegated to the REST API.
//!
//! ## Why a hybrid approach?
//!
//! | Operation       | Transport | Reason                                         |
//! |-----------------|-----------|------------------------------------------------|
//! | CREATE / DROP   | REST API  | DDL is more reliable via the REST endpoint      |
//! | TRUNCATE        | REST API  | Same as DDL                                     |
//! | INSERT (bulk)   | ODBC      | Columnar binding avoids 16 MB SQL size limit,   |
//! |                 |           | no string escaping, prepared statement reuse     |
//! | MERGE (upsert)  | REST API  | MERGE syntax requires VALUES literals; ODBC      |
//! |                 |           | doesn't support parameterized MERGE              |
//!
//! For `Append` and `Truncate` write strategies the sink uses ODBC prepared
//! INSERT with `arrow-odbc`'s `OdbcWriter`, which binds Arrow columns directly
//! to ODBC parameter buffers — near-zero serialization overhead.
//!
//! For `Upsert`, `InsertIgnore`, and `MergeDelete` strategies the sink
//! falls back to the REST API `StatementClient` (same path as `api::sink`).

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use potato_etl_common::config::{DatabaseSchemaConfig, StepDriverOptions};
use potato_etl_common::db::{TableMode, WriteStrategy};
use potato_etl_common::db::common::SinkConfig;
use potato_etl_common::db::traits::SinkBuilder;
use potato_etl_common::schema::ddl::{generate_post_create, DdlOptions, SqlDialect};
use potato_etl_common::util::arrow::record_batch_to_string_rows;
use crate::conn::{backtick, dbx_full_table, DatabricksConnParams};
use crate::api::{StatementClient, introspect_table_columns};
use crate::sql_helpers::{
    create_table_sql, delta_on_clause, delta_update_set, delta_insert_vals,
    build_delta_merge, format_sql_value,
};

use super::conn_str::build_odbc_connection_string;
use super::odbc_env;

pub struct DatabricksOdbcSink {
    /// REST API client for DDL, MERGE, and introspection.
    api: StatementClient,
    pub cfg: SinkConfig,
    first_batch: bool,
    alignment: Option<potato_etl_common::db::common::alignment::ColumnAlignment>,
    target_columns: Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>,
}

impl DatabricksOdbcSink {
    pub fn new(params: DatabricksConnParams) -> anyhow::Result<Self> {
        let api = StatementClient::new(params)?;
        Ok(Self {
            api,
            cfg: SinkConfig::new(""),
            first_batch: true,
            alignment: None,
            target_columns: None,
        })
    }

    /// Whether the current write strategy can use ODBC bulk insert.
    fn can_use_odbc_insert(&self) -> bool {
        matches!(
            self.cfg.write_strategy,
            WriteStrategy::Append | WriteStrategy::Truncate
        )
    }

    async fn ensure_alignment(&mut self, batch_schema: &SchemaRef) -> anyhow::Result<()> {
        use potato_etl_common::db::common::alignment::{self as align};
        if self.alignment.is_some() { return Ok(()); }
        if matches!(self.cfg.table_mode, TableMode::DropAndReplace) { return Ok(()); }

        let eff = if !self.cfg.schema_name.is_empty() {
            &self.cfg.schema_name
        } else {
            self.api.params.schema.as_deref().unwrap_or("")
        };
        let ft = dbx_full_table(self.api.params.catalog.as_deref(), eff, &self.cfg.table);
        let tc = introspect_table_columns(&self.api, &ft).await?;
        let tc = match tc { Some(c) => c, None => return Ok(()) };
        let result = align::compute_alignment(batch_schema, &tc, self.cfg.missing_column_behavior, &self.cfg.rename_targets(), &ft)?;
        self.target_columns = Some(tc);
        self.alignment = Some(result);
        Ok(())
    }

    fn align_batch(&self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        let batch = match &self.alignment {
            Some(a) => potato_etl_common::db::common::alignment::apply_alignment(batch, a)?,
            None => batch,
        };
        use potato_etl_common::db::common::type_coercion::{coerce_batch_for_target, TargetColumn};
        use crate::type_registry::DatabricksTypeRegistry;
        let tc = self.target_columns.as_ref().map(|cols| {
            cols.iter()
                .map(|c| TargetColumn { name: c.name.clone(), data_type: c.data_type.clone() })
                .collect::<Vec<_>>()
        });
        coerce_batch_for_target(batch, tc.as_deref(), &DatabricksTypeRegistry)
    }

    /// Execute DDL via REST API (shared between ODBC and API sinks).
    async fn prepare_table(&mut self, ddl_schema: &SchemaRef, full_table: &str, eff_schema: &str) -> anyhow::Result<()> {
        if self.cfg.table_prepared { return Ok(()); }
        self.cfg.table_prepared = true;

        match &self.cfg.table_mode {
            TableMode::UseExisting => {}
            TableMode::CreateIfNotExists => {
                self.api.execute_dml(&create_table_sql(ddl_schema, full_table, true)).await?;
                let ds = if eff_schema.is_empty() { None } else { Some(eff_schema) };
                for stmt in &generate_post_create(&self.cfg.table, ds, ddl_schema, SqlDialect::Databricks, DdlOptions::default()) {
                    self.api.execute_dml(stmt).await?;
                }
                for stmt in &self.cfg.named_ddl_post_create(SqlDialect::Databricks) {
                    let _ = self.api.execute_dml(stmt).await;
                }
            }
            TableMode::DropAndReplace => {
                self.api.execute_dml(&format!("DROP TABLE IF EXISTS {full_table}")).await?;
                self.api.execute_dml(&create_table_sql(ddl_schema, full_table, false)).await?;
                let ds = if eff_schema.is_empty() { None } else { Some(eff_schema) };
                for stmt in &generate_post_create(&self.cfg.table, ds, ddl_schema, SqlDialect::Databricks, DdlOptions::default()) {
                    self.api.execute_dml(stmt).await?;
                }
                for stmt in &self.cfg.named_ddl_post_create(SqlDialect::Databricks) {
                    let _ = self.api.execute_dml(stmt).await;
                }
            }
        }
        Ok(())
    }

    /// Bulk INSERT via ODBC columnar parameter binding.
    async fn write_odbc(&self, batch: RecordBatch, full_table: &str) -> anyhow::Result<usize> {
        let num_rows = batch.num_rows();
        let params = self.api.params.clone();
        let full_table = full_table.to_string();

        // ODBC is blocking — offload to a dedicated thread.
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        std::thread::spawn(move || {
            let result = run_odbc_insert(&params, &full_table, batch);
            let _ = result_tx.send(result);
        });

        result_rx
            .await
            .map_err(|_| anyhow::anyhow!("ODBC insert thread panicked"))??;

        Ok(num_rows)
    }

    /// Fallback INSERT via REST API (same as api::sink).
    async fn write_rest_insert(&self, batch: &RecordBatch, full_table: &str) -> anyhow::Result<()> {
        let schema = batch.schema();
        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let rows = record_batch_to_string_rows(batch);
        let cols_sql = col_names.iter().map(|c| backtick(c)).collect::<Vec<_>>().join(", ");

        for chunk in rows.chunks(500) {
            let values: Vec<String> = chunk.iter().map(|row| {
                let vals: Vec<String> = schema.fields().iter().zip(row.iter())
                    .map(|(f, v)| format_sql_value(v.as_deref(), f.data_type()))
                    .collect();
                format!("({})", vals.join(", "))
            }).collect();
            self.api.execute_dml(&format!(
                "INSERT INTO {full_table} ({cols_sql}) VALUES {}",
                values.join(", ")
            )).await?;
        }
        Ok(())
    }

    /// MERGE via REST API (ODBC doesn't support parameterized MERGE).
    async fn write_rest_merge(
        &self,
        batch: &RecordBatch,
        full_table: &str,
        strategy: &WriteStrategy,
    ) -> anyhow::Result<()> {
        let schema = batch.schema();
        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let rows = record_batch_to_string_rows(batch);
        let cols_sql = col_names.iter().map(|c| backtick(c)).collect::<Vec<_>>().join(", ");
        let pk = potato_etl_common::db::pk_columns(&schema);

        match strategy {
            WriteStrategy::InsertIgnore => {
                anyhow::ensure!(!pk.is_empty(), "insert_ignore requires primary_key");
                let on = delta_on_clause(&pk);
                let iv = delta_insert_vals(&col_names);
                for chunk in rows.chunks(500) {
                    self.api.execute_dml(&build_delta_merge(&schema, full_table, &cols_sql, &on, chunk, None, &iv, false)).await?;
                }
            }
            WriteStrategy::Upsert => {
                anyhow::ensure!(!pk.is_empty(), "upsert requires primary_key");
                let on = delta_on_clause(&pk);
                let us = delta_update_set(&col_names, &pk);
                let iv = delta_insert_vals(&col_names);
                for chunk in rows.chunks(500) {
                    self.api.execute_dml(&build_delta_merge(&schema, full_table, &cols_sql, &on, chunk, Some(&us), &iv, false)).await?;
                }
            }
            WriteStrategy::MergeDelete => {
                anyhow::ensure!(!pk.is_empty(), "merge_delete requires primary_key");
                let on = delta_on_clause(&pk);
                let us = delta_update_set(&col_names, &pk);
                let iv = delta_insert_vals(&col_names);
                for chunk in rows.chunks(500) {
                    self.api.execute_dml(&build_delta_merge(&schema, full_table, &cols_sql, &on, chunk, Some(&us), &iv, true)).await?;
                }
            }
            _ => unreachable!("Append/Truncate handled by ODBC path"),
        }
        Ok(())
    }

    async fn write_impl(&mut self, batch: RecordBatch) -> anyhow::Result<usize> {
        if batch.num_rows() == 0 { return Ok(0); }

        let schema = batch.schema();
        let ddl_schema = self.cfg.ddl_schema.clone().unwrap_or_else(|| schema.clone());
        let eff = if !self.cfg.schema_name.is_empty() {
            self.cfg.schema_name.clone()
        } else {
            self.api.params.schema.clone().unwrap_or_default()
        };
        let ft = dbx_full_table(self.api.params.catalog.as_deref(), &eff, &self.cfg.table);
        let num_rows = batch.num_rows();

        // DDL via REST API.
        self.prepare_table(&ddl_schema, &ft, &eff).await?;

        // TRUNCATE via REST API (first batch only).
        let first = self.first_batch;
        self.first_batch = false;
        if first
            && matches!(self.cfg.write_strategy, WriteStrategy::Truncate)
            && !matches!(self.cfg.table_mode, TableMode::DropAndReplace)
        {
            self.api.execute_dml(&format!("TRUNCATE TABLE {ft}")).await?;
        }

        // Column alignment.
        self.ensure_alignment(&batch.schema()).await?;
        let batch = self.align_batch(batch)?;
        let strategy = self.cfg.write_strategy.clone();

        if self.can_use_odbc_insert() {
            // ── ODBC bulk INSERT ──────────────────────────────────────────
            match self.write_odbc(batch.clone(), &ft).await {
                Ok(n) => {
                    tracing::debug!(table = %self.cfg.table, rows = n, "ODBC bulk INSERT complete");
                    return Ok(n);
                }
                Err(e) => {
                    // Fall back to REST API INSERT on ODBC failure.
                    tracing::warn!(
                        table = %self.cfg.table,
                        error = %e,
                        "ODBC bulk INSERT failed, falling back to REST API"
                    );
                    self.write_rest_insert(&batch, &ft).await?;
                }
            }
        } else {
            // ── MERGE via REST API ────────────────────────────────────────
            self.write_rest_merge(&batch, &ft, &strategy).await?;
        }

        Ok(num_rows)
    }

    async fn flush_impl(&mut self) -> anyhow::Result<()> {
        self.first_batch = true;
        self.cfg.table_prepared = false;
        self.alignment = None;
        Ok(())
    }
}

impl SinkBuilder for DatabricksOdbcSink {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.table(t); self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.schema(s); self }
    fn use_existing(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.use_existing(); self }
    fn create_if_not_exists(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.create_if_not_exists(); self }
    fn drop_and_replace(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.drop_and_replace(); self }
    fn insert(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.insert(); self }
    fn insert_ignore(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.insert_ignore(); self }
    fn upsert(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.upsert(); self }
    fn merge_delete(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.merge_delete(); self }
    fn clear_and_insert(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.clear_and_insert(); self }
    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SinkBuilder> {
        if let Some(b) = opts.on_missing_column { self.cfg.missing_column_behavior = b; }
        if !opts.init_sql.is_empty() {
            self.api.params.init_sql = opts.init_sql.clone();
        }
        self
    }
    fn set_database_schema_config(&mut self, config: DatabaseSchemaConfig) { self.cfg.database_schema_config = Some(config); }
    fn set_ddl_schema(&mut self, schema: SchemaRef) { self.cfg.ddl_schema = Some(schema); }
    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<usize>> + Send + 'a>> { Box::pin(self.write_impl(batch)) }
    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> { Box::pin(self.flush_impl()) }
}

// ── Blocking ODBC INSERT ──────────────────────────────────────────────────────

/// Runs the synchronous ODBC bulk INSERT on a dedicated OS thread.
///
/// Uses `arrow-odbc`'s `OdbcWriter` which binds Arrow column arrays directly
/// to ODBC parameter buffers — no string escaping, no SQL size limits,
/// prepared statement reuse across chunks.
///
/// The `OdbcWriter` buffer size is set to `batch.num_rows()` so it matches
/// the incoming batch exactly — no separate tuning knob needed.  The
/// step-level `batch_size` in the pipeline YAML already controls how large
/// each `RecordBatch` is.
fn run_odbc_insert(
    params: &DatabricksConnParams,
    full_table: &str,
    batch: RecordBatch,
) -> anyhow::Result<()> {
    use arrow_odbc::OdbcWriter;
    use odbc_api::ConnectionOptions;

    let env = odbc_env();
    let num_rows = batch.num_rows();

    // Build connection string synchronously — OAuth2 token exchange needs
    // a tokio runtime, so we create a temporary one for just this call.
    let odbc_conn_str = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(build_odbc_connection_string(params, num_rows))?
    };

    let conn = env
        .connect_with_connection_string(&odbc_conn_str, ConnectionOptions::default())
        .map_err(|e| anyhow::anyhow!("Databricks ODBC sink connection failed: {e}"))?;

    tracing::info!(
        table = %full_table,
        rows = num_rows,
        "Databricks ODBC: bulk INSERT via columnar binding"
    );

    // Build a prepared INSERT statement from the table name and schema.
    // `OdbcWriter` generates `INSERT INTO <table> (col1, col2, …) VALUES (?, ?, …)`
    // and binds Arrow columns as parameter arrays.
    //
    // Buffer size = batch.num_rows(): the incoming RecordBatch is already
    // sized by the pipeline's step-level batch_size, so we match it exactly.
    let arrow_schema = batch.schema();
    let mut writer = OdbcWriter::from_connection(conn, &arrow_schema, full_table, num_rows)
        .map_err(|e| anyhow::anyhow!(
            "Databricks ODBC: failed to create writer for {full_table}: {e}"
        ))?;

    // Write the batch — OdbcWriter handles chunking internally if the batch
    // exceeds the buffer size (shouldn't happen since we sized it to match).
    writer.write_batch(&batch)
        .map_err(|e| anyhow::anyhow!(
            "Databricks ODBC: write_batch failed for {full_table}: {e}"
        ))?;

    // Flush any remaining rows in the internal buffer.
    writer.flush()
        .map_err(|e| anyhow::anyhow!(
            "Databricks ODBC: flush failed for {full_table}: {e}"
        ))?;

    tracing::info!(
        table = %full_table,
        rows = num_rows,
        "Databricks ODBC: bulk INSERT complete"
    );

    Ok(())
}