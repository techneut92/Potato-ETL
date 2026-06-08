//! Databricks Thrift write sink — Delta Lake tables via Thrift SQL
//! (INSERT / MERGE / DDL).
//!
//! Uses the same SQL generation as the REST API sink (`sql_helpers.rs`) but
//! executes all statements through the Thrift binary protocol, matching the
//! transport used by the Thrift source.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use potato_etl_common::config::{DatabaseSchemaConfig, StepDriverOptions};
use potato_etl_common::db::{TableMode, WriteStrategy};
use potato_etl_common::db::common::SinkConfig;
use potato_etl_common::db::traits::SinkBuilder;
use potato_etl_common::schema::ddl::{generate_post_create, DdlOptions, SqlDialect};
use potato_etl_common::util::arrow::record_batch_to_string_rows;
use crate::conn::{backtick, dbx_full_table, DatabricksConnParams};
use crate::sql_helpers::{
    create_table_sql, delta_on_clause, delta_update_set, delta_insert_vals,
    build_delta_merge, format_sql_value,
};

use super::client::{introspect_table_columns, ThriftClient};

/// Maximum rows per INSERT / MERGE VALUES chunk to stay within Spark's SQL size limits.
const CHUNK_SIZE: usize = 500;

pub struct DatabricksThriftSink {
    client: Option<ThriftClient>,
    params: DatabricksConnParams,
    pub cfg: SinkConfig,
    first_batch: bool,
    alignment: Option<potato_etl_common::db::common::alignment::ColumnAlignment>,
    target_columns: Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>,
}

impl DatabricksThriftSink {
    pub fn new(params: DatabricksConnParams) -> anyhow::Result<Self> {
        Ok(Self {
            client: None,
            params,
            cfg: SinkConfig::new(""),
            first_batch: true,
            alignment: None,
            target_columns: None,
        })
    }

    /// Ensure the Thrift client is initialised.  After this call,
    /// `self.client` is guaranteed to be `Some`.
    async fn ensure_client_init(&mut self) -> anyhow::Result<()> {
        if self.client.is_none() {
            self.client = Some(ThriftClient::new(self.params.clone()).await?);
        }
        Ok(())
    }

    /// Convenience: get a mutable ref to the client.
    /// Panics if `ensure_client_init` was not called first.
    fn client_mut(&mut self) -> &mut ThriftClient {
        self.client.as_mut().expect("ThriftClient not initialised; call ensure_client_init first")
    }

    async fn ensure_alignment(&mut self, batch_schema: &SchemaRef) -> anyhow::Result<()> {
        use potato_etl_common::db::common::alignment::{self as align};
        if self.alignment.is_some() { return Ok(()); }
        if matches!(self.cfg.table_mode, TableMode::DropAndReplace) { return Ok(()); }

        let eff = if !self.cfg.schema_name.is_empty() {
            self.cfg.schema_name.clone()
        } else {
            self.params.schema.clone().unwrap_or_default()
        };
        let ft = dbx_full_table(self.params.catalog.as_deref(), &eff, &self.cfg.table);
        self.ensure_client_init().await?;
        let client = self.client_mut();
        let tc = introspect_table_columns(client, &ft).await?;
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

    /// Execute a list of SQL statements through the Thrift client.
    /// `fail_fast`: if true, errors propagate immediately.
    /// `best_effort`: if true, errors are silently ignored.
    async fn execute_stmts(
        &mut self,
        stmts: &[String],
        best_effort: bool,
    ) -> anyhow::Result<()> {
        for stmt in stmts {
            let result = self.client_mut().execute_dml(stmt).await;
            if !best_effort {
                result?;
            }
        }
        Ok(())
    }

    async fn write_impl(&mut self, batch: RecordBatch) -> anyhow::Result<usize> {
        if batch.num_rows() == 0 { return Ok(0); }

        self.ensure_client_init().await?;

        let schema = batch.schema();
        let ddl_schema = self.cfg.ddl_schema.clone().unwrap_or_else(|| schema.clone());
        let eff = if !self.cfg.schema_name.is_empty() {
            self.cfg.schema_name.clone()
        } else {
            self.params.schema.clone().unwrap_or_default()
        };
        let ft = dbx_full_table(self.params.catalog.as_deref(), &eff, &self.cfg.table);
        let num_rows = batch.num_rows();

        // ── DDL: table creation / drop ───────────────────────────────────
        //
        // Collect all DDL statements upfront to avoid holding a mutable
        // borrow on `self.client` across `self.cfg` access.
        if !self.cfg.table_prepared {
            self.cfg.table_prepared = true;
            let ds = if eff.is_empty() { None } else { Some(eff.as_str()) };

            match &self.cfg.table_mode {
                TableMode::UseExisting => {}
                TableMode::CreateIfNotExists => {
                    let mut ddl = vec![create_table_sql(&ddl_schema, &ft, true)];
                    ddl.extend(generate_post_create(&self.cfg.table, ds, &ddl_schema, SqlDialect::Databricks, DdlOptions::default()));
                    let named = self.cfg.named_ddl_post_create(SqlDialect::Databricks);

                    self.execute_stmts(&ddl, false).await?;
                    self.execute_stmts(&named, true).await?;
                }
                TableMode::DropAndReplace => {
                    let mut ddl = vec![
                        format!("DROP TABLE IF EXISTS {ft}"),
                        create_table_sql(&ddl_schema, &ft, false),
                    ];
                    ddl.extend(generate_post_create(&self.cfg.table, ds, &ddl_schema, SqlDialect::Databricks, DdlOptions::default()));
                    let named = self.cfg.named_ddl_post_create(SqlDialect::Databricks);

                    self.execute_stmts(&ddl, false).await?;
                    self.execute_stmts(&named, true).await?;
                }
            }
        }

        // ── Truncate on first batch if requested ─────────────────────────
        let first = self.first_batch;
        self.first_batch = false;
        if first
            && matches!(self.cfg.write_strategy, WriteStrategy::Truncate)
            && !matches!(self.cfg.table_mode, TableMode::DropAndReplace)
        {
            self.client_mut().execute_dml(&format!("TRUNCATE TABLE {ft}")).await?;
        }

        // ── Alignment + coercion ─────────────────────────────────────────
        self.ensure_alignment(&batch.schema()).await?;
        let batch = self.align_batch(batch)?;
        let schema = batch.schema();
        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let rows = record_batch_to_string_rows(&batch);
        let cols_sql = col_names.iter().map(|c| backtick(c)).collect::<Vec<_>>().join(", ");

        // ── DML: write rows in chunks ────────────────────────────────────
        let strategy = self.cfg.write_strategy.clone();
        match &strategy {
            WriteStrategy::Append | WriteStrategy::Truncate => {
                for chunk in rows.chunks(CHUNK_SIZE) {
                    let values: Vec<String> = chunk.iter().map(|row| {
                        let vals: Vec<String> = schema.fields().iter().zip(row.iter())
                            .map(|(f, v)| format_sql_value(v.as_deref(), f.data_type()))
                            .collect();
                        format!("({})", vals.join(", "))
                    }).collect();
                    let sql = format!(
                        "INSERT INTO {ft} ({cols_sql}) VALUES {}",
                        values.join(", ")
                    );
                    self.client_mut().execute_dml(&sql).await?;
                }
            }
            WriteStrategy::InsertIgnore => {
                let pk = potato_etl_common::db::pk_columns(&schema);
                anyhow::ensure!(!pk.is_empty(), "insert_ignore requires primary_key");
                let on = delta_on_clause(&pk);
                let iv = delta_insert_vals(&col_names);
                for chunk in rows.chunks(CHUNK_SIZE) {
                    let sql = build_delta_merge(
                        &schema, &ft, &cols_sql, &on, chunk, None, &iv, false,
                    );
                    self.client_mut().execute_dml(&sql).await?;
                }
            }
            WriteStrategy::Upsert => {
                let pk = potato_etl_common::db::pk_columns(&schema);
                anyhow::ensure!(!pk.is_empty(), "upsert requires primary_key");
                let on = delta_on_clause(&pk);
                let us = delta_update_set(&col_names, &pk);
                let iv = delta_insert_vals(&col_names);
                for chunk in rows.chunks(CHUNK_SIZE) {
                    let sql = build_delta_merge(
                        &schema, &ft, &cols_sql, &on, chunk, Some(&us), &iv, false,
                    );
                    self.client_mut().execute_dml(&sql).await?;
                }
            }
            WriteStrategy::MergeDelete => {
                let pk = potato_etl_common::db::pk_columns(&schema);
                anyhow::ensure!(!pk.is_empty(), "merge_delete requires primary_key");
                let on = delta_on_clause(&pk);
                let us = delta_update_set(&col_names, &pk);
                let iv = delta_insert_vals(&col_names);
                for chunk in rows.chunks(CHUNK_SIZE) {
                    let sql = build_delta_merge(
                        &schema, &ft, &cols_sql, &on, chunk, Some(&us), &iv, true,
                    );
                    self.client_mut().execute_dml(&sql).await?;
                }
            }
        }
        Ok(num_rows)
    }

    async fn flush_impl(&mut self) -> anyhow::Result<()> {
        self.first_batch = true;
        self.cfg.table_prepared = false;
        self.alignment = None;
        self.target_columns = None;
        // Close and discard the session so a fresh one is opened next time.
        if let Some(ref mut client) = self.client {
            client.close_session().await;
        }
        self.client = None;
        Ok(())
    }
}

impl SinkBuilder for DatabricksThriftSink {
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
        self
    }
    fn set_database_schema_config(&mut self, config: DatabaseSchemaConfig) { self.cfg.database_schema_config = Some(config); }
    fn set_ddl_schema(&mut self, schema: SchemaRef) { self.cfg.ddl_schema = Some(schema); }

    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<usize>> + Send + 'a>> {
        Box::pin(self.write_impl(batch))
    }

    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(self.flush_impl())
    }
}
