//! potato-etl-runtime — database-agnostic ETL library.
//!
//! This crate is the main entry point for the potato-etl library.  It provides:
//! - The DAG-based pipeline orchestrator (`Dag`)
//! - Unified database API (`ReadDB`, `WriteDB`, `Scd2Sink`) that dispatches
//!   to driver crates based on the connection URL scheme
//! - File transport registration — wires up transport crates based on features
//! - Re-exports of all common types (schema, config, transforms, etc.)
//!
//! ## Architecture
//!
//! ```text
//! potato-etl-common                ← shared types, traits, schema, transforms
//!     ↑                               (no driver/transport deps)
//! potato-etl-driver-*              ← each implements SourceBuilder/SinkBuilder/Scd2Builder
//!     ↑                               (depends on common only)
//! potato-etl-transport-*           ← each implements FileTransport
//!     ↑                               (depends on common only)
//! potato-etl-runtime (this crate)  ← DAG + ReadDB/WriteDB dispatch + transport registration
//!                                    (depends on common + optional driver/transport crates)
//! ```
//!
//! ## Feature flags
//!
//! ### Database drivers
//! `postgres`, `mssql`, `mssql-bcp`, `mssql-odbc`, `oracle`, `mysql`,
//! `databricks`, `databricks-odbc`, `databricks-thrift`
//!
//! ### File transports
//! `transport-cloud` (S3/Azure/GCS), `transport-sftp`, `transport-ftp`,
//! `transport-sharepoint`, `transport-smb`, `transport-all`
//!
//! ### Everything
//! `all` — enables every driver + every transport.
//!
//! ## Two usage patterns
//!
//! ### 1. Simple pipeline (ReadDB / WriteDB / Scd2Sink)
//! ```rust,no_run
//! use potato_etl_runtime::{ReadDB, WriteDB};
//! use futures::StreamExt;
//!
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! let source = ReadDB::new("mssql://sa:pw@host/DB")?.table("orders").cursor("id");
//! let mut sink = WriteDB::new("postgresql://…")?.table("orders_copy").clear_and_insert();
//! let mut stream = source.exec();
//! while let Some(batch) = stream.next().await { sink.write(batch?).await?; }
//! sink.flush().await?;
//! # Ok(()) }
//! ```
//!
//! ### 2. DAG-based pipeline
//! ```rust,no_run
//! use potato_etl_runtime::{Dag, ETLConfig, ReadOptions, WriteOptions, SinkMode, JoinHow};
//!
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! let mut dag = Dag::new(ETLConfig { batch_size: 500 });
//! dag.add_source("orders",    "postgresql://…", ReadOptions { table: Some("orders".into()), ..Default::default() });
//! dag.add_source("customers", "mssql://…",      ReadOptions { table: Some("customers".into()), ..Default::default() });
//! dag.add_join("joined", "orders", "customers", "customer_id", JoinHow::Inner);
//! dag.add_filter("active", "joined", Some("status".into()), Some("active".into()), None);
//! dag.add_sink("out", "active", "postgresql://…", WriteOptions { table: "output".into(), ..Default::default() });
//! let report = dag.run().await?;
//! println!("{} rows read, {} written", report.rows_read, report.rows_written);
//! # Ok(()) }
//! ```

// ── Re-export everything from common ──────────────────────────────────────
//
// This ensures backward compatibility: `potato_etl_runtime::SomeType` still works
// even though `SomeType` actually lives in `potato_etl_common`.

pub use potato_etl_common::*;

// Common modules re-exported for path-based access
// (e.g. `potato_etl_runtime::schema::META_PRIMARY_KEY`)
pub use potato_etl_common::schema;
pub use potato_etl_common::config;
pub use potato_etl_common::util;
pub use potato_etl_common::transform;
pub use potato_etl_common::http;

// ── Core-specific modules ─────────────────────────────────────────────────────

/// Unified database API (ReadDB, WriteDB, Scd2Sink) — dispatches to driver crates.
pub mod db;

/// DAG-based pipeline orchestrator and config loaders.
pub mod dag;

/// File transport registration — wires up transport crates based on feature flags.
pub mod transports;

// ── Core-specific re-exports ──────────────────────────────────────────────────

pub use dag::{ComponentId, Dag, RunReport, TransformFn, StepKind, StepSummary};
pub use db::{ReadDB, WriteDB, Scd2Sink};

/// Initialize all optional subsystems.
///
/// Call this once at startup before running pipelines.  It registers
/// file transport factories for all transport crates enabled via features.
///
/// ```rust,ignore
/// potato_etl_runtime::init();
/// let dag = Dag::from_config_file("pipeline.yaml").await?;
/// dag.run().await?;
/// ```
pub fn init() {
    transports::register_all();
}