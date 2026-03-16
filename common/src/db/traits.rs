//! Driver traits — the contracts that each database driver crate implements.
//!
//! These traits define the interface between the DAG executor (in `potato-etl-runtime`)
//! and the individual driver implementations (in `potato-etl-driver-*` crates).
//!
//! ## Design
//!
//! The traits use `Box<Self>` receivers for builder methods to support dynamic
//! dispatch while preserving the builder pattern.  The DAG executor stores
//! `Box<dyn SourceBuilder>`, `Box<dyn SinkBuilder>`, etc.
//!
//! ## Registration
//!
//! Each driver crate exposes a `register(registry: &mut DriverRegistry)` function
//! that registers factory functions for its supported URL schemes.  The binary
//! crate (CLI, Python bindings) calls these at startup.

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;

use crate::config::{DatabaseSchemaConfig, StepDriverOptions};
use super::Scd2ColumnNames;
use super::Scd2Stats;

// ── Source trait ──────────────────────────────────────────────────────────────

/// A database source that produces a stream of `RecordBatch`es.
///
/// Builder methods consume and return `Box<Self>` to enable chaining through
/// trait objects.  All builder methods have default no-op implementations so
/// drivers only override what they support.
pub trait SourceBuilder: Send {
    /// Set the table name to read from.
    fn table(self: Box<Self>, table: String) -> Box<dyn SourceBuilder>;
    /// Set the database schema (e.g. `public`, `dbo`, `hr`).
    fn schema(self: Box<Self>, schema: String) -> Box<dyn SourceBuilder>;
    /// Set a custom SQL query instead of reading a whole table.
    fn query(self: Box<Self>, query: String) -> Box<dyn SourceBuilder>;
    /// Set the cursor column for keyset pagination.
    fn cursor(self: Box<Self>, col: String) -> Box<dyn SourceBuilder>;
    /// Set the batch size (rows per RecordBatch).
    fn batch_size(self: Box<Self>, n: usize) -> Box<dyn SourceBuilder>;
    /// Apply per-step driver-specific options.
    fn with_driver_options(self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SourceBuilder>;
    /// Read the Arrow schema without fetching data (optional, not all drivers support this).
    fn read_schema<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<SchemaRef>> + Send + 'a>> {
        Box::pin(async { anyhow::bail!("read_schema() not implemented for this driver") })
    }
    /// Start streaming data. Consumes the builder.
    fn exec(self: Box<Self>) -> BoxStream<'static, anyhow::Result<RecordBatch>>;
    /// Clone the source builder (needed for schema introspection paths).
    /// Returns `None` if cloning is not supported.
    fn try_clone(&self) -> Option<Box<dyn SourceBuilder>> { None }
}

// ── Sink trait ────────────────────────────────────────────────────────────────

/// A database sink that writes `RecordBatch`es.
pub trait SinkBuilder: Send {
    /// Set the target table name.
    fn table(self: Box<Self>, table: String) -> Box<dyn SinkBuilder>;
    /// Set the target database schema.
    fn schema(self: Box<Self>, schema: String) -> Box<dyn SinkBuilder>;
    /// Set table mode to `UseExisting` (no DDL).
    fn use_existing(self: Box<Self>) -> Box<dyn SinkBuilder>;
    /// Set table mode to `CreateIfNotExists`.
    fn create_if_not_exists(self: Box<Self>) -> Box<dyn SinkBuilder>;
    /// Set table mode to `DropAndReplace`.
    fn drop_and_replace(self: Box<Self>) -> Box<dyn SinkBuilder>;
    /// Set write strategy to `Append` (plain INSERT).
    fn insert(self: Box<Self>) -> Box<dyn SinkBuilder>;
    /// Set write strategy to `InsertIgnore`.
    fn insert_ignore(self: Box<Self>) -> Box<dyn SinkBuilder>;
    /// Set write strategy to `Upsert`.
    fn upsert(self: Box<Self>) -> Box<dyn SinkBuilder>;
    /// Set write strategy to `MergeDelete`.
    fn merge_delete(self: Box<Self>) -> Box<dyn SinkBuilder>;
    /// Set write strategy to `Truncate`.
    fn clear_and_insert(self: Box<Self>) -> Box<dyn SinkBuilder>;
    /// Apply per-step driver-specific options.
    fn with_driver_options(self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SinkBuilder>;
    /// Set the unified database schema config (named indexes and constraints).
    fn set_database_schema_config(&mut self, config: DatabaseSchemaConfig);
    /// Set the DDL schema for DDL-only columns.
    fn set_ddl_schema(&mut self, schema: SchemaRef);
    /// Write a single batch. Returns the number of rows written.
    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<usize>> + Send + 'a>>;
    /// Flush / commit all pending writes.
    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>>;
}

// ── SCD2 sink trait ──────────────────────────────────────────────────────────

/// An SCD Type 2 sink.
pub trait Scd2Builder: Send + 'static {
    /// Set the target table name.
    fn table(self: Box<Self>, table: String) -> Box<dyn Scd2Builder>;
    /// Set the target database schema.
    fn schema(self: Box<Self>, schema: String) -> Box<dyn Scd2Builder>;
    /// Set the natural business key column.
    fn key(self: Box<Self>, col: String) -> Box<dyn Scd2Builder>;
    /// Set the tracked columns (empty = track all).
    fn track(self: Box<Self>, cols: Vec<String>) -> Box<dyn Scd2Builder>;
    /// Set custom SCD2 column names.
    fn col_names(self: Box<Self>, names: Scd2ColumnNames) -> Box<dyn Scd2Builder>;
    /// Enable or disable close-missing mode (full-snapshot semantics).
    fn close_missing(self: Box<Self>, close: bool) -> Box<dyn Scd2Builder>;
    /// Set the maximum number of keys per SQL `WHERE key IN (...)` chunk.
    /// Used in both write (close-changed) and flush (close-missing) paths.
    fn chunk_size(self: Box<Self>, n: usize) -> Box<dyn Scd2Builder>;
    /// Apply per-step driver-specific options.
    fn with_driver_options(self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn Scd2Builder>;
    /// Returns `(table, schema, key_col, col_names, conn_str)` for DDL generation.
    fn ddl_params(&self) -> (String, Option<String>, String, Scd2ColumnNames, String);
    /// Execute a single DDL statement.
    fn execute_ddl<'a>(&'a mut self, sql: &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>>;
    /// Write a batch with SCD2 semantics.
    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Scd2Stats>> + Send + 'a>>;
    /// Flush / commit.
    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>>;
}

// ── Driver registry ──────────────────────────────────────────────────────────

/// Factory function that creates a source builder from a connection string.
pub type SourceFactory = Box<dyn Fn(&str) -> anyhow::Result<Box<dyn SourceBuilder>> + Send + Sync>;
/// Factory function that creates a sink builder from a connection string.
pub type SinkFactory = Box<dyn Fn(&str) -> anyhow::Result<Box<dyn SinkBuilder>> + Send + Sync>;
/// Factory function that creates an SCD2 builder from a connection string.
pub type Scd2Factory = Box<dyn Fn(&str) -> anyhow::Result<Box<dyn Scd2Builder>> + Send + Sync>;

/// Registry of driver factories, keyed by URL scheme prefix.
///
/// The binary crate (CLI, Python bindings) populates this at startup by calling
/// each driver crate's `register()` function.
///
/// ```rust,ignore
/// let mut registry = DriverRegistry::new();
/// potato_etl_driver_postgres::register(&mut registry);
/// potato_etl_driver_mssql::register(&mut registry);
/// // ...
/// let dag = Dag::from_yaml(&yaml)?;
/// dag.run_with_registry(&registry).await?;
/// ```
pub struct DriverRegistry {
    sources: Vec<(Vec<String>, SourceFactory)>,
    sinks:   Vec<(Vec<String>, SinkFactory)>,
    scd2s:   Vec<(Vec<String>, Scd2Factory)>,
}

impl DriverRegistry {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
            sinks:   Vec::new(),
            scd2s:   Vec::new(),
        }
    }

    /// Register a source factory for the given URL scheme prefixes.
    ///
    /// ```rust,ignore
    /// registry.register_source(
    ///     &["postgresql://", "postgres://"],
    ///     Box::new(|conn_str| Ok(Box::new(PgReadDB::new(conn_str)?))),
    /// );
    /// ```
    pub fn register_source(&mut self, schemes: &[&str], factory: SourceFactory) {
        self.sources.push((schemes.iter().map(|s| s.to_string()).collect(), factory));
    }

    /// Register a sink factory for the given URL scheme prefixes.
    pub fn register_sink(&mut self, schemes: &[&str], factory: SinkFactory) {
        self.sinks.push((schemes.iter().map(|s| s.to_string()).collect(), factory));
    }

    /// Register an SCD2 factory for the given URL scheme prefixes.
    pub fn register_scd2(&mut self, schemes: &[&str], factory: Scd2Factory) {
        self.scd2s.push((schemes.iter().map(|s| s.to_string()).collect(), factory));
    }

    /// Create a source from a connection string by matching the URL scheme.
    pub fn create_source(&self, conn_str: &str) -> anyhow::Result<Box<dyn SourceBuilder>> {
        for (schemes, factory) in &self.sources {
            if schemes.iter().any(|s| conn_str.starts_with(s.as_str())) {
                return factory(conn_str);
            }
        }
        anyhow::bail!(
            "No driver registered for connection: '{conn_str}'.  \
             Available schemes: {}",
            self.source_schemes().join(", ")
        )
    }

    /// Create a sink from a connection string by matching the URL scheme.
    pub fn create_sink(&self, conn_str: &str) -> anyhow::Result<Box<dyn SinkBuilder>> {
        for (schemes, factory) in &self.sinks {
            if schemes.iter().any(|s| conn_str.starts_with(s.as_str())) {
                return factory(conn_str);
            }
        }
        anyhow::bail!(
            "No driver registered for connection: '{conn_str}'.  \
             Available schemes: {}",
            self.sink_schemes().join(", ")
        )
    }

    /// Create an SCD2 sink from a connection string by matching the URL scheme.
    pub fn create_scd2(&self, conn_str: &str) -> anyhow::Result<Box<dyn Scd2Builder>> {
        for (schemes, factory) in &self.scd2s {
            if schemes.iter().any(|s| conn_str.starts_with(s.as_str())) {
                return factory(conn_str);
            }
        }
        anyhow::bail!(
            "No driver registered for connection: '{conn_str}'.  \
             Available schemes: {}",
            self.scd2_schemes().join(", ")
        )
    }

    fn source_schemes(&self) -> Vec<String> {
        self.sources.iter().flat_map(|(s, _)| s.clone()).collect()
    }

    fn sink_schemes(&self) -> Vec<String> {
        self.sinks.iter().flat_map(|(s, _)| s.clone()).collect()
    }

    fn scd2_schemes(&self) -> Vec<String> {
        self.scd2s.iter().flat_map(|(s, _)| s.clone()).collect()
    }
}

impl Default for DriverRegistry {
    fn default() -> Self { Self::new() }
}