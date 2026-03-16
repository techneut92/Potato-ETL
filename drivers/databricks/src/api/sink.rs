//! Databricks REST API write sink — Delta Lake tables via SQL Statement
//! Execution API (INSERT / MERGE / DDL).

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use potato_etl_common::config::{DatabaseSchemaConfig, StepDriverOptions};
use potato_etl_common::db::{TableMode, WriteStrategy};
use potato_etl_common::db::common::SinkConfig;
use potato_etl_common::db::traits::SinkBuilder;
use potato_etl_common::schema::ddl::{generate_post_create, DdlOptions, SqlDialect};
use potato_etl_common::util::arrow::record_batch_to_string_rows;
use crate::conn::{backtick, dbx_full_table, DatabricksConnParams};
use crate::sql_helpers::{create_table_sql, delta_on_clause, delta_update_set, delta_insert_vals, build_delta_merge, format_sql_value};
use super::{introspect_table_columns, StatementClient};

pub struct DatabricksApiSink {
    api: StatementClient,
    pub cfg: SinkConfig,
    first_batch: bool,
    alignment: Option<potato_etl_common::db::common::alignment::ColumnAlignment>,
    target_columns: Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>,
}

impl DatabricksApiSink {
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

    async fn ensure_alignment(&mut self, batch_schema: &SchemaRef) -> anyhow::Result<()> {
        use potato_etl_common::db::common::alignment::{self as align, MissingColumnBehavior};
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
        let result = align::compute_alignment(batch_schema, &tc, MissingColumnBehavior::Skip, &ft)?;
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

    async fn write_impl(&mut self, batch: RecordBatch) -> anyhow::Result<usize> {
        if batch.num_rows() == 0 { return Ok(0); }

        // Execute init_sql on the first batch only.
        if self.first_batch {
            self.api.execute_init_sql().await?;
        }

        let schema = batch.schema();
        let ddl_schema = self.cfg.ddl_schema.clone().unwrap_or_else(|| schema.clone());
        let eff = if !self.cfg.schema_name.is_empty() {
            self.cfg.schema_name.clone()
        } else {
            self.api.params.schema.clone().unwrap_or_default()
        };
        let ft = dbx_full_table(self.api.params.catalog.as_deref(), &eff, &self.cfg.table);
        let num_rows = batch.num_rows();

        if !self.cfg.table_prepared {
            self.cfg.table_prepared = true;
            match &self.cfg.table_mode {
                TableMode::UseExisting => {}
                TableMode::CreateIfNotExists => {
                    let ddl = create_table_sql(&ddl_schema, &ft, true);
                    tracing::debug!(table = %self.cfg.table, "Databricks DDL:\n{ddl}");
                    self.api.execute_dml(&ddl).await?;
                    let ds = if eff.is_empty() { None } else { Some(eff.as_str()) };
                    for stmt in &generate_post_create(&self.cfg.table, ds, &ddl_schema, SqlDialect::Databricks, DdlOptions::default()) {
                        tracing::trace!(table = %self.cfg.table, "Databricks post-create DDL:\n{stmt}");
                        self.api.execute_dml(stmt).await?;
                    }
                    for stmt in &self.cfg.named_ddl_post_create(SqlDialect::Databricks) {
                        let _ = self.api.execute_dml(stmt).await;
                    }
                    tracing::info!(table = %self.cfg.table, "Databricks CREATE TABLE IF NOT EXISTS applied");
                }
                TableMode::DropAndReplace => {
                    self.api.execute_dml(&format!("DROP TABLE IF EXISTS {ft}")).await?;
                    let ddl = create_table_sql(&ddl_schema, &ft, false);
                    tracing::debug!(table = %self.cfg.table, "Databricks DDL:\n{ddl}");
                    self.api.execute_dml(&ddl).await?;
                    let ds = if eff.is_empty() { None } else { Some(eff.as_str()) };
                    for stmt in &generate_post_create(&self.cfg.table, ds, &ddl_schema, SqlDialect::Databricks, DdlOptions::default()) {
                        tracing::trace!(table = %self.cfg.table, "Databricks post-create DDL:\n{stmt}");
                        self.api.execute_dml(stmt).await?;
                    }
                    for stmt in &self.cfg.named_ddl_post_create(SqlDialect::Databricks) {
                        let _ = self.api.execute_dml(stmt).await;
                    }
                    tracing::info!(table = %self.cfg.table, "Databricks DROP + CREATE TABLE applied");
                }
            }
        }

        let first = self.first_batch;
        self.first_batch = false;
        if first
            && matches!(self.cfg.write_strategy, WriteStrategy::Truncate)
            && !matches!(self.cfg.table_mode, TableMode::DropAndReplace)
        {
            self.api.execute_dml(&format!("TRUNCATE TABLE {ft}")).await?;
            tracing::info!(table = %self.cfg.table, "Databricks TRUNCATE applied");
        }

        self.ensure_alignment(&batch.schema()).await?;
        let batch = self.align_batch(batch)?;
        let schema = batch.schema();
        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let rows = record_batch_to_string_rows(&batch);
        let chunk_size = 500usize;
        let cols_sql = col_names.iter().map(|c| backtick(c)).collect::<Vec<_>>().join(", ");

        match &self.cfg.write_strategy.clone() {
            WriteStrategy::Append | WriteStrategy::Truncate => {
                for chunk in rows.chunks(chunk_size) {
                    let values: Vec<String> = chunk.iter().map(|row| {
                        let vals: Vec<String> = schema.fields().iter().zip(row.iter())
                            .map(|(f, v)| format_sql_value(v.as_deref(), f.data_type()))
                            .collect();
                        format!("({})", vals.join(", "))
                    }).collect();
                    self.api.execute_dml(&format!(
                        "INSERT INTO {ft} ({cols_sql}) VALUES {}",
                        values.join(", ")
                    )).await?;
                }
            }
            WriteStrategy::InsertIgnore => {
                let pk = potato_etl_common::db::pk_columns(&schema);
                anyhow::ensure!(!pk.is_empty(), "insert_ignore requires primary_key");
                let on = delta_on_clause(&pk);
                let iv = delta_insert_vals(&col_names);
                for chunk in rows.chunks(chunk_size) {
                    self.api.execute_dml(&build_delta_merge(&schema, &ft, &cols_sql, &on, chunk, None, &iv, false)).await?;
                }
            }
            WriteStrategy::Upsert => {
                let pk = potato_etl_common::db::pk_columns(&schema);
                anyhow::ensure!(!pk.is_empty(), "upsert requires primary_key");
                let on = delta_on_clause(&pk);
                let us = delta_update_set(&col_names, &pk);
                let iv = delta_insert_vals(&col_names);
                for chunk in rows.chunks(chunk_size) {
                    self.api.execute_dml(&build_delta_merge(&schema, &ft, &cols_sql, &on, chunk, Some(&us), &iv, false)).await?;
                }
            }
            WriteStrategy::MergeDelete => {
                let pk = potato_etl_common::db::pk_columns(&schema);
                anyhow::ensure!(!pk.is_empty(), "merge_delete requires primary_key");
                let on = delta_on_clause(&pk);
                let us = delta_update_set(&col_names, &pk);
                let iv = delta_insert_vals(&col_names);
                for chunk in rows.chunks(chunk_size) {
                    self.api.execute_dml(&build_delta_merge(&schema, &ft, &cols_sql, &on, chunk, Some(&us), &iv, true)).await?;
                }
            }
        }
        tracing::debug!(table = %self.cfg.table, rows = num_rows, "Databricks batch written");
        Ok(num_rows)
    }

    async fn flush_impl(&mut self) -> anyhow::Result<()> {
        tracing::info!(table = %self.cfg.table, "Databricks flush complete");
        self.first_batch = true;
        self.cfg.table_prepared = false;
        self.alignment = None;
        Ok(())
    }
}

impl SinkBuilder for DatabricksApiSink {
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