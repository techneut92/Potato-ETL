//! Unified database API — `ReadDB`, `WriteDB`, `Scd2Sink`.
//!
//! These are thin wrappers around `Box<dyn SourceBuilder>` / `Box<dyn SinkBuilder>`
//! / `Box<dyn Scd2Builder>` from the common crate.
//!
//! Backend detection uses the URL scheme to find the correct driver:
//!
//! | Prefix                | Driver crate                    | Feature flag     |
//! |-----------------------|---------------------------------|------------------|
//! | `postgresql://`       | `potato-etl-driver-postgres`    | `postgres`       |
//! | `postgres://`         | `potato-etl-driver-postgres`    | `postgres`       |
//! | `mssql://`            | `potato-etl-driver-mssql`       | `mssql`          |
//! | `oracle://`           | `potato-etl-driver-oracle`      | `oracle`         |
//! | `mysql://`            | `potato-etl-driver-mysql`       | `mysql`          |
//! | `mariadb://`          | `potato-etl-driver-mysql`       | `mysql`          |
//! | `databricks://`       | `potato-etl-driver-databricks`  | `databricks`     |

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;

use potato_etl_common::config::{
    CreateTableMode, DatabaseSchemaConfig, StepDriverOptions,
};
use potato_etl_common::db::traits::{
    DriverRegistry, SourceBuilder, SinkBuilder, Scd2Builder,
};
use potato_etl_common::schema::{
    DdlOptions, Scd2DdlInfo, SqlDialect, generate_ddl_with_schema,
};

// Re-export common DB types for backward compatibility.
pub use potato_etl_common::db::{
    TableMode, WriteStrategy, pk_columns, Scd2ColumnNames, Scd2Stats,
};
// Re-export the common module so `potato_etl_runtime::db::common` still works.
pub use potato_etl_common::db::common;
// Re-export traits so downstream code can use them.
pub use potato_etl_common::db::traits;

// ── Default registry ──────────────────────────────────────────────────────────

/// Build a `DriverRegistry` with all drivers that are compiled in via features.
///
/// This is the backward-compatible path: enabling `--features postgres` in
/// `potato-etl-runtime` automatically registers the Postgres driver.
pub fn default_registry() -> DriverRegistry {
    let mut reg = DriverRegistry::new();

    #[cfg(feature = "postgres")]
    potato_etl_driver_postgres::register(&mut reg);

    #[cfg(feature = "mssql")]
    potato_etl_driver_mssql::register(&mut reg);

    #[cfg(feature = "mysql")]
    potato_etl_driver_mysql::register(&mut reg);

    #[cfg(feature = "oracle")]
    potato_etl_driver_oracle::register(&mut reg);

    #[cfg(feature = "databricks")]
    potato_etl_driver_databricks::register(&mut reg);

    reg
}

// ── ReadDB ────────────────────────────────────────────────────────────────────

/// Unified database reader.  Detects the backend from the URL scheme.
pub struct ReadDB(Box<dyn SourceBuilder>);

impl ReadDB {
    /// Create a new source using the default driver registry (feature-flag based).
    pub fn new(conn_str: impl Into<String>) -> anyhow::Result<Self> {
        let s = conn_str.into();
        let reg = default_registry();
        Ok(ReadDB(reg.create_source(&s)?))
    }

    /// Create a new source using a custom driver registry.
    pub fn with_registry(conn_str: impl Into<String>, registry: &DriverRegistry) -> anyhow::Result<Self> {
        let s = conn_str.into();
        Ok(ReadDB(registry.create_source(&s)?))
    }

    pub fn table(self, t: impl Into<String>) -> Self {
        ReadDB(self.0.table(t.into()))
    }

    pub fn schema(self, s: impl Into<String>) -> Self {
        ReadDB(self.0.schema(s.into()))
    }

    pub fn query(self, q: impl Into<String>) -> Self {
        ReadDB(self.0.query(q.into()))
    }

    pub fn cursor(self, col: impl Into<String>) -> Self {
        ReadDB(self.0.cursor(col.into()))
    }

    pub fn batch_size(self, n: usize) -> Self {
        ReadDB(self.0.batch_size(n))
    }

    pub fn with_driver_options(self, opts: &StepDriverOptions) -> Self {
        if opts.is_empty() { return self; }
        ReadDB(self.0.with_driver_options(opts))
    }

    pub async fn read_schema(&self) -> anyhow::Result<SchemaRef> {
        self.0.read_schema().await
    }

    pub fn exec(self) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
        self.0.exec()
    }
}

impl Clone for ReadDB {
    fn clone(&self) -> Self {
        ReadDB(self.0.try_clone().expect("Clone not supported for this source driver"))
    }
}

// ── WriteDB ──────────────────────────────────────────────────────────────────

/// Unified database writer.
pub struct WriteDB(Box<dyn SinkBuilder>);

impl WriteDB {
    pub fn new(conn_str: impl Into<String>) -> anyhow::Result<Self> {
        let s = conn_str.into();
        let reg = default_registry();
        Ok(WriteDB(reg.create_sink(&s)?))
    }

    pub fn with_registry(conn_str: impl Into<String>, registry: &DriverRegistry) -> anyhow::Result<Self> {
        let s = conn_str.into();
        Ok(WriteDB(registry.create_sink(&s)?))
    }

    pub fn table(self, t: impl Into<String>) -> Self {
        WriteDB(self.0.table(t.into()))
    }

    pub fn schema(self, s: impl Into<String>) -> Self {
        WriteDB(self.0.schema(s.into()))
    }

    pub fn use_existing(self) -> Self { WriteDB(self.0.use_existing()) }
    pub fn create_if_not_exists(self) -> Self { WriteDB(self.0.create_if_not_exists()) }
    pub fn drop_and_replace(self) -> Self { WriteDB(self.0.drop_and_replace()) }
    pub fn insert(self) -> Self { WriteDB(self.0.insert()) }
    pub fn insert_ignore(self) -> Self { WriteDB(self.0.insert_ignore()) }
    pub fn upsert(self) -> Self { WriteDB(self.0.upsert()) }
    pub fn merge_delete(self) -> Self { WriteDB(self.0.merge_delete()) }
    pub fn clear_and_insert(self) -> Self { WriteDB(self.0.clear_and_insert()) }

    /// Apply a [`CreateTableMode`] to this sink.
    pub fn create_mode(self, mode: CreateTableMode) -> Self {
        match mode {
            CreateTableMode::Never        => self,
            CreateTableMode::IfNotExists  => self.create_if_not_exists(),
            CreateTableMode::Replace      => self.drop_and_replace(),
        }
    }

    pub fn with_driver_options(self, opts: &StepDriverOptions) -> Self {
        if opts.is_empty() { return self; }
        WriteDB(self.0.with_driver_options(opts))
    }

    pub fn set_database_schema_config(&mut self, config: DatabaseSchemaConfig) {
        self.0.set_database_schema_config(config);
    }

    pub fn set_ddl_schema(&mut self, schema: SchemaRef) {
        self.0.set_ddl_schema(schema);
    }

    pub async fn write(&mut self, batch: RecordBatch) -> anyhow::Result<usize> {
        self.0.write(batch).await
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.0.flush().await
    }
}

// ── Scd2Sink ──────────────────────────────────────────────────────────────────

/// Unified SCD Type 2 sink.
pub struct Scd2Sink {
    inner:         Box<dyn Scd2Builder>,
    create_table:  CreateTableMode,
    table_ensured: bool,
    ddl_schema:    Option<SchemaRef>,
    database_schema_config: Option<DatabaseSchemaConfig>,
}

impl Scd2Sink {
    pub fn new(conn_str: impl Into<String>) -> anyhow::Result<Self> {
        let s = conn_str.into();
        let reg = default_registry();
        Ok(Self {
            inner: reg.create_scd2(&s)?,
            create_table: CreateTableMode::Never,
            table_ensured: false,
            ddl_schema: None,
            database_schema_config: None,
        })
    }

    pub fn with_registry(conn_str: impl Into<String>, registry: &DriverRegistry) -> anyhow::Result<Self> {
        let s = conn_str.into();
        Ok(Self {
            inner: registry.create_scd2(&s)?,
            create_table: CreateTableMode::Never,
            table_ensured: false,
            ddl_schema: None,
            database_schema_config: None,
        })
    }

    pub fn table(mut self, t: impl Into<String>) -> Self {
        self.inner = self.inner.table(t.into());
        self
    }

    pub fn schema(mut self, s: impl Into<String>) -> Self {
        self.inner = self.inner.schema(s.into());
        self
    }

    pub fn key(mut self, col: impl Into<String>) -> Self {
        self.inner = self.inner.key(col.into());
        self
    }

    pub fn track(mut self, cols: Vec<String>) -> Self {
        self.inner = self.inner.track(cols);
        self
    }

    pub fn col_names(mut self, n: Scd2ColumnNames) -> Self {
        self.inner = self.inner.col_names(n);
        self
    }

    pub fn close_missing(mut self, close: bool) -> Self {
        self.inner = self.inner.close_missing(close);
        self
    }

    pub fn chunk_size(mut self, n: usize) -> Self {
        self.inner = self.inner.chunk_size(n);
        self
    }

    pub fn create_table(mut self, mode: CreateTableMode) -> Self {
        self.create_table  = mode;
        self.table_ensured = false;
        self
    }

    pub fn set_ddl_schema(&mut self, schema: SchemaRef) {
        self.ddl_schema = Some(schema);
    }

    pub fn set_database_schema_config(&mut self, config: DatabaseSchemaConfig) {
        self.database_schema_config = Some(config);
    }

    pub fn with_driver_options(mut self, opts: &StepDriverOptions) -> Self {
        if opts.is_empty() { return self; }
        self.inner = self.inner.with_driver_options(opts);
        self
    }

    // ── DDL helpers ──────────────────────────────────────────────────────────

    async fn ensure_table(&mut self, ar_schema: &arrow::datatypes::Schema) -> anyhow::Result<()> {
        if self.table_ensured || self.create_table == CreateTableMode::Never {
            return Ok(());
        }

        let (table, schema, key_col, col_names, conn_str) = self.inner.ddl_params();
        let dialect = SqlDialect::from_conn_str(&conn_str);

        if self.create_table == CreateTableMode::Replace {
            let drop_sql = dialect.drop_table_if_exists(&table, schema.as_deref());
            self.inner.execute_ddl(&drop_sql).await?;
        }

        let scd2_info = Scd2DdlInfo { col_names: &col_names, key_col: &key_col };
        let effective_schema = self.ddl_schema.as_ref()
            .map(|s| s.as_ref())
            .unwrap_or(ar_schema);

        let stmts = generate_ddl_with_schema(
            &table,
            schema.as_deref(),
            effective_schema,
            dialect,
            Some(&scd2_info),
            self.database_schema_config.as_ref(),
            DdlOptions::default(),
        );

        for stmt in &stmts.pre_create {
            if let Err(e) = self.inner.execute_ddl(stmt).await {
                tracing::warn!("Pre-create statement failed (non-fatal): {e:#}");
            }
        }
        self.inner.execute_ddl(&stmts.create_table).await?;
        for stmt in &stmts.post_create {
            if let Err(e) = self.inner.execute_ddl(stmt).await {
                tracing::warn!("Post-create statement ignored (non-fatal): {e:#}");
            }
        }

        self.table_ensured = true;
        Ok(())
    }

    // ── Data operations ───────────────────────────────────────────────────────

    pub async fn write(&mut self, batch: RecordBatch) -> anyhow::Result<Scd2Stats> {
        if batch.num_rows() == 0 { return Ok(Scd2Stats::default()); }
        self.ensure_table(batch.schema_ref()).await?;
        self.inner.write(batch).await
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.inner.flush().await
    }
}