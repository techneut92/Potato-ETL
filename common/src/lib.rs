//! potato-etl-common — shared types, schema, config, transforms, and utilities.
//!
//! This crate is the **foundation** that every driver crate, transport crate,
//! and the runtime orchestrator depend on.  It contains no database driver code,
//! no remote transport code, and no DAG executor — only the building blocks.
//!
//! ## Architecture
//!
//! ```text
//! common (this crate)
//! ├── FileTransport trait + LocalTransport
//! ├── ConnParams / FileAuth config types
//! └── Parquet / CSV / JSON file I/O (bytes-level)
//!
//! transports/ (separate crates, each depends on common)
//! ├── cloud       — S3, Azure Blob, GCS (object_store)
//! ├── sftp        — SFTP (russh)
//! ├── ftp         — FTP/FTPS (suppaftp)
//! ├── sharepoint  — SharePoint Online (reqwest + MS Graph)
//! └── smb         — SMB/CIFS (pavao)
//!
//! drivers/ (separate crates, each depends on common)
//! ├── postgres    — PostgreSQL
//! ├── mysql       — MySQL / MariaDB / Aurora
//! ├── mssql       — SQL Server
//! ├── oracle      — Oracle
//! └── databricks  — Databricks SQL
//! ```
//!
//! ## Feature flags
//!
//! | Feature | Dependencies | Description |
//! |---------|-------------|-------------|
//! | `parquet` | `parquet`, `bytes` | Parquet file I/O (enabled by default) |

// ── Modules ───────────────────────────────────────────────────────────────────

/// Database common layer: shared types, SinkConfig, Scd2Config, driver traits.
pub mod db;

/// Schema model: ColumnOption, TableSchema, DDL generation, Arrow metadata.
pub mod schema;

/// Shared configuration and options types.
pub mod config;

/// Shared Arrow utilities and schema-mapping helpers.
pub mod util;

/// Stateless transform functions and the `EtlTransform` trait.
pub mod transform;

/// HTTP / REST API source and sink.
pub mod http;

/// JSON file source — read local JSON files into Arrow RecordBatches.
pub mod json_file;

/// CSV file source and sink — read/write local CSV files as Arrow RecordBatches.
pub mod csv_file;

/// Parquet file source and sink — read/write Parquet files as Arrow RecordBatches.
#[cfg(feature = "parquet")]
pub mod parquet_file;

/// Secret manager integration (HashiCorp Vault, Azure Key Vault, GCP Secret Manager).
pub mod secrets;

/// File transport trait + local filesystem transport.
///
/// Remote transports (S3, SFTP, FTP, SharePoint, SMB) live in their own crates
/// under `/transports/`.  The `create_transport()` function here only handles
/// `ConnParams::Local` — the runtime crate wires up remote transports via the
/// transport registry.
pub mod file_transport;

// ── Re-exports ────────────────────────────────────────────────────────────────

pub use arrow::datatypes::SchemaRef;
pub use arrow::record_batch::RecordBatch;

pub use file_transport::GlobSortOrder;
pub use config::{
    AuthConfig,
    ConnParams, ConnectionDef,
    CreateTableMode, DbAuth, ETLConfig,
    IdentifierCase, JoinHow, LogLevel, ReadOptions, SinkMode,
    SinkSchemaConfig, SourceSchemaConfig, StepDriverOptions, WriteOptions,
    // Unified schema types
    ComponentSchema, DatabaseSchemaConfig, DatabaseColumnDef,
    ArrowSchemaConfig, ArrowColumnDef, IndexDef, ConstraintDef,
    // Target / source location (YAML/JSON config layer)
    SourceLocation, SinkTarget, FileSourceLocation, FileSinkTarget,
};
pub use config::{pct_decode, pct_encode};
pub use config::SecretsConfig;
pub use config::FileAuth;
pub use db::{Scd2ColumnNames, Scd2Stats, TableMode, WriteStrategy, pk_columns};
pub use http::{
    HttpMethod, PaginationConfig,
    RestApiOptions, RestApiSinkOptions, RestHttpCommon, SinkWriteMode,
};
pub use schema::{
    // per-column DDL hints for sinks
    ColumnOption, ColumnOptionsMap, ForeignKey, LogicalType,
    // whole-table structure
    TableSchema, ColumnDef,
    // application
    apply_arrow_overrides, apply_rename, apply_column_options,
    // compile-once plans
    ArrowOverridesPlan, compile_arrow_overrides, apply_arrow_overrides_plan,
    MetadataStampPlan,  compile_metadata_stamps,  apply_metadata_stamp_plan,
    // DDL + dialect
    generate_ddl, generate_ddl_with_schema, generate_post_create, DdlStatements, Scd2DdlInfo, SqlDialect,
    arrow_type_to_sql_dialect, resolve_sql_type, source_type_to_target_sql,
    logical_type_for_source, logical_type_to_target_sql,
    // Arrow metadata constants
    META_CHECK_EXPR, META_DB_TYPE, META_DEFAULT_EXPR, META_DESCRIPTION,
    META_ENUM_VALUES, META_FOREIGN_KEY, META_INDEX, META_LOGICAL_TYPE, META_NULLABLE,
    META_PRIMARY_KEY, META_SOURCE_DB, META_UNIQUE,
};
pub use transform::{EtlTransform, FilterTransform, apply_flatten};
pub use transform::unnest::{apply_unnest, UnnestConfig};
pub use transform::map::apply_map;
pub use transform::aggregate::apply_aggregate;
pub use transform::objects::{build_objects, resolve_url_template};
pub use transform::schema::{apply_rename_all, RenameAllTransform};