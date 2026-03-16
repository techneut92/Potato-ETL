//! Pipeline step options: ReadOptions, WriteOptions, SinkMode, CreateTableMode,
//! and source/sink location types.

use serde::{Deserialize, Serialize};

use super::schema_config::{SourceSchemaConfig, SinkSchemaConfig};
use super::driver_options::StepDriverOptions;

// ── SourceLocation ────────────────────────────────────────────────────────────

/// Location of a database source for `read_db` steps.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceLocation {
    /// Named connection reference.
    pub connection: String,
    /// Database schema / namespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Table name to read from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Custom SQL query (alternative to `table`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Column name for keyset (cursor-based) pagination.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Location of a database target for `write_db` and `scd2_sink` steps.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SinkTarget {
    /// Named connection reference.
    pub connection: String,
    /// Database schema / namespace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// Target table name.
    pub table: String,
}

// ── File locations ────────────────────────────────────────────────────────────

/// Location of a file source for `read_csv` / `read_json` steps
/// (connection-based mode).
///
/// ```yaml
/// from:
///   connection: reports_sftp
///   path: incoming/data.csv     # relative to connection's base_path
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSourceLocation {
    /// Named file connection reference (local, sftp, s3, etc.).
    pub connection: String,
    /// File path relative to the connection's `base_path`.
    pub path: String,
}

/// Location of a file sink for `write_csv` / `write_json` steps
/// (connection-based mode).
///
/// ```yaml
/// target:
///   connection: data_lake_s3
///   path: processed/result.json  # relative to connection's base_path
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSinkTarget {
    /// Named file connection reference (local, sftp, s3, etc.).
    pub connection: String,
    /// File path relative to the connection's `base_path`.
    pub path: String,
}

// ── ReadOptions ───────────────────────────────────────────────────────────────

/// Options for a `read_db` pipeline step.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// Table name to read from.
    pub table: Option<String>,
    /// Database schema / namespace.
    pub db_schema: Option<String>,
    /// Custom SQL query (alternative to `table`).
    pub query: Option<String>,
    /// Column name for keyset (cursor-based) pagination.
    pub cursor: Option<String>,
    /// Source-side schema settings.
    pub source_schema: SourceSchemaConfig,
    /// Per-step read batch size override.
    pub batch_size: Option<usize>,
    /// Per-step driver-specific options.
    pub options: StepDriverOptions,
}

// ── WriteOptions ──────────────────────────────────────────────────────────────

/// Options for a `write_db` pipeline step.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Target table name.
    pub table: String,
    /// Database schema / namespace.
    pub db_schema: Option<String>,
    /// Write mode (append, upsert, etc.).
    pub mode: SinkMode,
    /// DDL table creation mode.
    pub create_table: CreateTableMode,
    /// Per-step write batch size override.
    pub batch_size: Option<usize>,
    /// Per-step driver-specific options.
    pub options: StepDriverOptions,
    /// Sink-side schema settings.
    pub sink_schema: SinkSchemaConfig,
}

// ── SinkMode ──────────────────────────────────────────────────────────────────

/// How rows are written to the target table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkMode {
    /// INSERT (default).
    Append,
    /// INSERT … ON CONFLICT DO NOTHING.
    InsertIgnore,
    /// INSERT … ON CONFLICT DO UPDATE (merge).
    Upsert,
    /// MERGE with DELETE for missing rows.
    MergeDelete,
    /// TRUNCATE + INSERT.
    Truncate,
}

impl Default for SinkMode {
    fn default() -> Self { Self::Append }
}

// ── CreateTableMode ───────────────────────────────────────────────────────────

/// Whether (and how) the sink should create the target table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreateTableMode {
    /// Do not create the table — it must already exist.
    #[serde(alias = "")]
    Never,
    /// CREATE TABLE IF NOT EXISTS.
    IfNotExists,
    /// DROP TABLE IF EXISTS + CREATE TABLE.
    Replace,
}

impl Default for CreateTableMode {
    fn default() -> Self { Self::Never }
}

// ── JoinHow ───────────────────────────────────────────────────────────────────

/// Join strategy for two-input join steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinHow {
    Inner,
    Left,
    Full,
}

impl Default for JoinHow {
    fn default() -> Self { Self::Inner }
}