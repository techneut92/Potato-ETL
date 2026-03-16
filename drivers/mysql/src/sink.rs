//! MySQL write sink — `MySqlWriteDB`.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use sqlx::mysql::MySqlPool;

use potato_etl_common::config::{DatabaseSchemaConfig, StepDriverOptions};
use potato_etl_common::db::{TableMode, WriteStrategy};
use potato_etl_common::db::common::SinkConfig;
use potato_etl_common::db::traits::SinkBuilder;
use potato_etl_common::schema::ddl::{generate_ddl_with_schema, DdlOptions, SqlDialect};
use potato_etl_common::util::arrow::record_batch_to_string_rows;
use crate::util::{backtick, mysql_full_table};

// ── MySqlWriteDB ──────────────────────────────────────────────────────────────

pub struct MySqlWriteDB {
    conn_str:    String,
    pub cfg:     SinkConfig,
    pool:        Option<MySqlPool>,
    first_batch: bool,
    alignment:   Option<potato_etl_common::db::common::alignment::ColumnAlignment>,
    target_columns: Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>,
    /// SQL statements executed on every new connection in the pool.
    init_sql:    Vec<String>,
}

impl MySqlWriteDB {
    pub fn new(conn_str: &str) -> Self {
        Self {
            conn_str:    conn_str.to_string(),
            cfg:         SinkConfig::new(""),
            pool:        None,
            first_batch: true,
            alignment:   None,
            target_columns: None,
            init_sql:    Vec::new(),
        }
    }

    async fn pool(&mut self) -> anyhow::Result<&MySqlPool> {
        if self.pool.is_none() {
            self.pool = Some(crate::util::mysql_pool_with_init_sql(&self.conn_str, 3, &self.init_sql).await?);
        }
        Ok(self.pool.as_ref().unwrap())
    }

    async fn ensure_alignment(&mut self, batch_schema: &SchemaRef) -> anyhow::Result<()> {
        use potato_etl_common::db::common::alignment::{self as align, MissingColumnBehavior};
        if self.alignment.is_some() { return Ok(()); }
        if matches!(self.cfg.table_mode, TableMode::DropAndReplace) { return Ok(()); }
        let pool = self.pool.as_ref().unwrap();
        let target_cols = crate::util::mysql_introspect_table_columns(pool, &self.cfg.schema_name, &self.cfg.table).await?;
        let target_cols = match target_cols { Some(c) => c, None => return Ok(()) };
        let table_display = mysql_full_table(&self.cfg.schema_name, &self.cfg.table);
        let result = align::compute_alignment(batch_schema, &target_cols, MissingColumnBehavior::Skip, &table_display)?;
        self.target_columns = Some(target_cols);
        self.alignment = Some(result);
        Ok(())
    }

    fn align_batch(&self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        let batch = match &self.alignment {
            Some(a) => potato_etl_common::db::common::alignment::apply_alignment(batch, a)?,
            None    => batch,
        };
        use potato_etl_common::db::common::type_coercion::{coerce_batch_for_target, TargetColumn};
        use crate::type_registry::MysqlTypeRegistry;
        let target_cols_adapted = self.target_columns.as_ref().map(|cols| {
            cols.iter().map(|c| TargetColumn { name: c.name.clone(), data_type: c.data_type.clone() }).collect::<Vec<_>>()
        });
        coerce_batch_for_target(batch, target_cols_adapted.as_deref(), &MysqlTypeRegistry)
    }

    async fn write_impl(&mut self, batch: RecordBatch) -> anyhow::Result<usize> {
        if batch.num_rows() == 0 { return Ok(0); }
        let pool = self.pool().await?.clone();
        let schema    = batch.schema();
        let ddl_schema = self.cfg.ddl_schema.clone().unwrap_or_else(|| schema.clone());
        let full_table = mysql_full_table(&self.cfg.schema_name, &self.cfg.table);
        let num_rows   = batch.num_rows();

        // ── DDL ───────────────────────────────────────────────────────────────
        if !self.cfg.table_prepared {
            self.cfg.table_prepared = true;
            match &self.cfg.table_mode {
                TableMode::UseExisting => {}
                TableMode::CreateIfNotExists => {
                    let db_schema = if self.cfg.schema_name.is_empty() { None } else { Some(self.cfg.schema_name.as_str()) };
                    let db_config = self.cfg.database_schema_config.as_ref();
                    let stmts = generate_ddl_with_schema(&self.cfg.table, db_schema, &ddl_schema, SqlDialect::Mysql, None, db_config, DdlOptions::default());
                    for stmt in &stmts.pre_create {
                        tracing::trace!(table = %self.cfg.table, "MySQL pre-create DDL:\n{stmt}");
                        sqlx::query(stmt).execute(&pool).await?;
                    }
                    tracing::debug!(table = %self.cfg.table, "MySQL DDL:\n{}", stmts.create_table);
                    sqlx::query(&stmts.create_table).execute(&pool).await?;
                    for stmt in &stmts.post_create {
                        tracing::trace!(table = %self.cfg.table, "MySQL post-create DDL:\n{stmt}");
                        if let Err(e) = sqlx::query(stmt).execute(&pool).await {
                            tracing::warn!(table = %self.cfg.table, "MySQL post-create DDL ignored: {e:#}");
                        }
                    }
                    tracing::info!(table = %self.cfg.table, "MySQL CREATE TABLE IF NOT EXISTS applied");
                }
                TableMode::DropAndReplace => {
                    sqlx::query(&format!("DROP TABLE IF EXISTS {full_table}")).execute(&pool).await?;
                    let db_schema = if self.cfg.schema_name.is_empty() { None } else { Some(self.cfg.schema_name.as_str()) };
                    let db_config = self.cfg.database_schema_config.as_ref();
                    let stmts = generate_ddl_with_schema(&self.cfg.table, db_schema, &ddl_schema, SqlDialect::Mysql, None, db_config, DdlOptions::default());
                    for stmt in &stmts.pre_create {
                        tracing::trace!(table = %self.cfg.table, "MySQL pre-create DDL:\n{stmt}");
                        sqlx::query(stmt).execute(&pool).await?;
                    }
                    // Strip "IF NOT EXISTS" for DropAndReplace since we just dropped
                    let create_sql = stmts.create_table.replace("IF NOT EXISTS ", "");
                    tracing::debug!(table = %self.cfg.table, "MySQL DDL:\n{}", create_sql);
                    sqlx::query(&create_sql).execute(&pool).await?;
                    for stmt in &stmts.post_create {
                        tracing::trace!(table = %self.cfg.table, "MySQL post-create DDL:\n{stmt}");
                        if let Err(e) = sqlx::query(stmt).execute(&pool).await {
                            tracing::warn!(table = %self.cfg.table, "MySQL post-create DDL ignored: {e:#}");
                        }
                    }
                    tracing::info!(table = %self.cfg.table, "MySQL DROP + CREATE TABLE applied");
                }
            }
        }

        // ── Alignment + coercion ──────────────────────────────────────────────
        self.ensure_alignment(&batch.schema()).await?;
        let batch = self.align_batch(batch)?;
        let schema    = batch.schema();
        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let cols_sql   = col_names.iter().map(|c| backtick(c)).collect::<Vec<_>>().join(", ");

        // ── TRUNCATE ──────────────────────────────────────────────────────────
        let first = self.first_batch;
        self.first_batch = false;
        if first && matches!(self.cfg.write_strategy, WriteStrategy::Truncate)
            && !matches!(self.cfg.table_mode, TableMode::DropAndReplace)
        {
            sqlx::query(&format!("TRUNCATE TABLE {full_table}")).execute(&pool).await?;
            tracing::info!(table = %self.cfg.table, "MySQL TRUNCATE applied");
        }

        // ── INSERT / UPSERT ───────────────────────────────────────────────────
        let chunk_size = (65_535 / col_names.len().max(1)).min(500).max(1);
        let rows       = record_batch_to_string_rows(&batch);

        match &self.cfg.write_strategy.clone() {
            WriteStrategy::Append | WriteStrategy::Truncate => {
                for chunk in rows.chunks(chunk_size) {
                    let mut qb = sqlx::QueryBuilder::<sqlx::MySql>::new(format!("INSERT INTO {full_table} ({cols_sql}) "));
                    qb.push_values(chunk.iter(), |mut b, row| { for v in row { b.push_bind(v.as_deref()); } });
                    qb.build().execute(&pool).await?;
                }
            }
            WriteStrategy::InsertIgnore => {
                for chunk in rows.chunks(chunk_size) {
                    let mut qb = sqlx::QueryBuilder::<sqlx::MySql>::new(format!("INSERT IGNORE INTO {full_table} ({cols_sql}) "));
                    qb.push_values(chunk.iter(), |mut b, row| { for v in row { b.push_bind(v.as_deref()); } });
                    qb.build().execute(&pool).await?;
                }
            }
            WriteStrategy::Upsert | WriteStrategy::MergeDelete => {
                let pk_cols = potato_etl_common::db::pk_columns(&batch.schema());
                anyhow::ensure!(!pk_cols.is_empty(), "mode=upsert/merge_delete requires primary_key: true");
                for chunk in rows.chunks(chunk_size) {
                    let updates = col_names.iter()
                        .filter(|c| !pk_cols.contains(c))
                        .map(|c| format!("{bt} = VALUES({bt})", bt = backtick(c)))
                        .collect::<Vec<_>>().join(", ");
                    let mut qb = sqlx::QueryBuilder::<sqlx::MySql>::new(format!("INSERT INTO {full_table} ({cols_sql}) "));
                    qb.push_values(chunk.iter(), |mut b, row| { for v in row { b.push_bind(v.as_deref()); } });
                    if !updates.is_empty() { qb.push(format!(" ON DUPLICATE KEY UPDATE {updates}")); }
                    qb.build().execute(&pool).await?;
                }
                if matches!(self.cfg.write_strategy, WriteStrategy::MergeDelete) {
                    let pk_col = &pk_cols[0];
                    let pk_vals: Vec<String> = rows.iter().map(|row| {
                        let idx = col_names.iter().position(|c| c == pk_col).unwrap_or(0);
                        match row.get(idx).and_then(|v| v.as_deref()) {
                            Some(v) => format!("'{}'", v.replace('\'', "\\'")),
                            None    => "NULL".into(),
                        }
                    }).collect();
                    for chunk in pk_vals.chunks(chunk_size) {
                        let in_list = chunk.join(", ");
                        sqlx::query(&format!("DELETE FROM {full_table} WHERE {} NOT IN ({in_list})", backtick(pk_col))).execute(&pool).await?;
                    }
                }
            }
        }
        tracing::debug!(table = %self.cfg.table, rows = num_rows, "MySQL batch written");
        Ok(num_rows)
    }

    async fn flush_impl(&mut self) -> anyhow::Result<()> {
        if let Some(pool) = self.pool.take() { pool.close().await; }
        tracing::info!(table = %self.cfg.table, "MySQL flush complete");
        self.first_batch        = true;
        self.cfg.table_prepared = false;
        self.alignment          = None;
        Ok(())
    }
}

// ── SinkBuilder trait ────────────────────────────────────────────────────────

impl SinkBuilder for MySqlWriteDB {
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
        if let Some(ref my) = opts.mysql {
            if !my.init_sql.is_empty() {
                self.init_sql = my.init_sql.clone();
            }
        }
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