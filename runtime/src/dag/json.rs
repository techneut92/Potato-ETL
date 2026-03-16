//! JSON (and shared YAML) pipeline configuration.
//!
//! ## Format
//! ```json
//! {
//!   "config": { "batch_size": 1000 },
//!   "connections": {
//!     "pg_src":  { "driver": "postgres", "host": "...", "database": "...", "username": "...", "password": "..." },
//!     "api_src": { "driver": "rest_api", "base_url": "https://api.example.com", "auth": { "type": "bearer", "token": "..." } }
//!   },
//!   "environment": {
//!     "API_KEY": "abc123",
//!     "BASE_URL": "https://api.example.com"
//!   },
//!   "steps": [
//!     { "id": "orders",  "type": "read_db",  "from": { "connection": "pg_src", "table": "orders", "cursor": "id" }, "batch_size": 500 },
//!     { "id": "api_src", "type": "rest_api", "conn": "api_src", "url": "/v1/customers", "data_path": "data" },
//!     { "id": "out",     "type": "write_db", "input": "orders", "target": { "connection": "pg_src", "table": "output" }, "mode": "truncate", "batch_size": 200 }
//!   ]
//! }
//! ```
//!
//! ## Connections
//!
//! The top-level `connections` map gives names to structured connection params.
//! Database steps reference a connection by name in their `from.connection` or
//! `target.connection` field.  REST API steps use `conn: <name>`.
//!
//! Every connection is a `ConnParams` object discriminated by the `driver` field:
//! `postgres` | `mssql` | `oracle` | `mysql` | `aurora` | `mariadb` |
//! `databricks` | `rest_api`.
//!
//! ## File connections
//!
//! File-transport connections (`local` | `sftp` | `s3` | `azure_blob` | `gcs` |
//! `sharepoint` | `ftp` | `smb`) are referenced from file steps (`read_csv`,
//! `read_json`, `write_csv`, `write_json`) via `from.connection` (sources) or
//! `target.connection` (sinks).  The step path is resolved relative to the
//! connection's `base_path`.  Legacy `path:` syntax (direct filesystem path)
//! remains supported for backwards compatibility.
//!
//! ## Glob patterns
//!
//! File source steps (`read_json`, `read_csv`, `read_parquet`) support glob
//! patterns in their `path` field to read multiple files at once:
//!
//! | Pattern  | Meaning                                    |
//! |----------|--------------------------------------------|
//! | `*`      | Matches any sequence of non-`/` characters |
//! | `?`      | Matches any single non-`/` character       |
//! | `[abc]`  | Character class                            |
//! | `[a-z]`  | Character range                            |
//! | `[!0-9]` | Negated class                              |
//! | `**`     | Recursive: matches zero or more directory levels |
//!
//! ```yaml
//! # All JSON files in a directory
//! - id: all_data
//!   type: read_json
//!   path: data/export_*.json
//!
//! # Recursive: search subdirectories
//! - id: deep_search
//!   type: read_csv
//!   path: incoming/**/report_*.csv
//!
//! # Control processing order
//! - id: newest_first
//!   type: read_parquet
//!   path: lake/events_*.parquet
//!   sort_glob: name_desc   # descending alphabetical
//! ```
//!
//! When `path` contains no glob characters, the step reads a single file
//! (backwards compatible).  The optional `sort_glob` field controls the order
//! in which matched files are processed: `name` (ascending, default) or
//! `name_desc` (descending).
//!
//! ## REST API connections
//!
//! Use `driver: rest_api` to store a base URL, default auth, and default headers.
//! REST API steps reference the connection with `conn: <name>` and supply a
//! path in `url` (e.g. `url: /v1/employees`).  Step-level `auth` and `headers`
//! override connection defaults; full URLs in `url` bypass `base_url` entirely.
//!
//! ## Python transforms
//!
//! Two styles are supported:
//! - **Named** (`function`): references a transform registered via `dag.register_transform(name, fn)`.
//! - **Inline** (`code`): Python source string; auto-registered by the Python wheel.
//!   The code runs with `table` (pyarrow.Table) and `pa` (pyarrow module) in scope.
//!   Assign the output to `result`.

use std::collections::HashMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use potato_etl_common::file_transport::GlobSortOrder;
use crate::config::{AuthConfig, ConnParams, ConnectionDef, CreateTableMode, ETLConfig, JoinHow, ReadOptions, SinkMode, SinkSchemaConfig, SinkTarget, SourceLocation, SourceSchemaConfig, StepDriverOptions, WriteOptions};
use crate::config::{DatabaseSchemaConfig, FileSourceLocation, FileSinkTarget};
use crate::db::Scd2ColumnNames;
use crate::http::{RestApiOptions, RestApiSinkOptions};
use crate::http::RestHttpCommon;
use super::Dag;

// ── Shared serde types ────────────────────────────────────────────────────────
//
// These structs are pub(crate) so that `dag::yaml` can reuse them without
// duplicating the deserialisation logic.

#[derive(Serialize, Deserialize)]
pub(crate) struct PipelineDoc {
    #[serde(default)]
    pub config: ETLConfig,
    /// Named connections, referenced by name in step `from.connection` /
    /// `target.connection` fields (database steps) or `conn` (REST API steps).
    /// Every entry must be a structured `ConnParams` object (discriminated by
    /// `driver`).  Passwords are automatically percent-encoded; no manual
    /// encoding is required.
    #[serde(default)]
    pub connections: HashMap<String, ConnectionDef>,
    /// Pipeline environment variables — expression strings evaluated once at
    /// pipeline start.  Referenced in step expressions via `$name` or
    /// `env("name")`.
    #[serde(default)]
    pub environment: IndexMap<String, String>,
    /// The list of pipeline steps.
    pub steps: Vec<StepDef>,
}

/// Shared fields for all database sink steps (`write_db`, `scd2_sink`).
///
/// Extracted to eliminate duplication between `WriteDb` and `Scd2Sink` enum
/// variants.  Flattened into each variant so the YAML/JSON format is unchanged.
#[derive(Serialize, Deserialize)]
pub(crate) struct DbSinkCommon {
    /// Database target location: connection, schema, table.
    target: SinkTarget,
    #[serde(default)]
    create_table: CreateTableMode,
    /// Optional database-side DDL hints (DDL-only columns, indexes, constraints).
    ///
    /// Step-level field — sits alongside `target`, not nested inside it.
    /// Used for target-table DDL concerns like `default_expr`, `on_update_expr`,
    /// or DDL-only columns that don't appear in the data batches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    database: Option<DatabaseSchemaConfig>,
    /// Sink-side schema settings: `schema` (unified).
    /// Flattened — these fields appear at the same YAML/JSON level as `target`.
    #[serde(flatten)]
    sink_schema: SinkSchemaConfig,
    /// Per-sink write batch size.
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_size: Option<usize>,
    /// Per-step driver-specific options.
    #[serde(default, deserialize_with = "crate::config::deserialize_null_as_default", skip_serializing_if = "StepDriverOptions::is_empty")]
    options: StepDriverOptions,
}

impl DbSinkCommon {
    /// Resolves the connection, merges driver options, and folds the step-level
    /// `database` config into `sink_schema`.
    fn resolve(self, conns: &HashMap<String, ConnectionDef>) -> anyhow::Result<ResolvedDbSink> {
        let (conn_str, conn_opts) = resolve_conn_with_options(&self.target.connection, conns)?;
        let mut options = self.options;
        options.merge_from(&conn_opts);
        let sink_schema = merge_target_database(self.sink_schema, self.database);
        Ok(ResolvedDbSink {
            conn_str,
            options,
            sink_schema,
            table: self.target.table,
            db_schema: self.target.schema,
            create_table: self.create_table,
            batch_size: self.batch_size,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum StepDef {
    ReadDb {
        id:   String,
        /// Database source location: connection, schema, table/query/cursor.
        from: SourceLocation,
        /// Source-side schema settings: `schema` (unified), `normalize_columns`, `exclude`.
        /// Flattened — these fields appear at the same YAML/JSON level as `from`.
        #[serde(flatten)]
        source_schema: SourceSchemaConfig,
        /// Per-step read batch size. Overrides `config.batch_size` for this source only.
        #[serde(skip_serializing_if = "Option::is_none")]
        batch_size: Option<usize>,
        /// Per-step driver-specific options (e.g. Oracle prefetch_rows).
        #[serde(default, deserialize_with = "crate::config::deserialize_null_as_default", skip_serializing_if = "StepDriverOptions::is_empty")]
        options: StepDriverOptions,
    },
    RestApi {
        id:  String,
        #[serde(default)]
        url: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        conn: Option<String>,
        /// Source-side schema settings: `arrow_overrides`, `normalize_columns`, `exclude`.
        /// Flattened — these fields appear at the same YAML/JSON level as `url`, etc.
        #[serde(flatten)]
        source_schema: SourceSchemaConfig,
        #[serde(flatten)]
        opts: RestApiOptions,
    },
    /// Read a JSON file as a source (local or remote via file connection).
    ///
    /// Supports glob patterns (`*`, `?`, `[a-z]`) and recursive globs (`**`).
    ///
    /// ```yaml
    /// # Single file
    /// - id: candidates
    ///   type: read_json
    ///   path: data/candidates.json
    ///   data_path: candidates
    ///
    /// # Glob: all JSON files in a directory
    /// - id: all_exports
    ///   type: read_json
    ///   path: data/export_*.json
    ///   sort_glob: name_desc   # newest-named first (optional, default: name)
    ///
    /// # Recursive glob: search subdirectories
    /// - id: all_reports
    ///   type: read_json
    ///   path: data/**/report_*.json
    ///
    /// # Connection-based:
    /// - id: candidates
    ///   type: read_json
    ///   from:
    ///     connection: data_sftp
    ///     path: incoming/xyz_*.json
    ///   data_path: candidates
    /// ```
    ReadJson {
        id:   String,
        /// Filesystem path to the JSON file (legacy — backwards compatible).
        /// Mutually exclusive with `from`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        /// Connection-based file location.
        /// Mutually exclusive with `path`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from: Option<FileSourceLocation>,
        /// Dot-notation path to the array of rows within the JSON document.
        /// `null` or `""` means the top-level value is the array.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        data_path: Option<String>,
        /// Source-side schema settings: `arrow_overrides`, `normalize_columns`, `exclude`.
        #[serde(flatten)]
        source_schema: SourceSchemaConfig,
        /// Per-step read batch size.
        #[serde(skip_serializing_if = "Option::is_none")]
        batch_size: Option<usize>,
        /// Sort order for glob-matched files: `name` (default), `name_desc`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sort_glob: Option<String>,
    },
    WriteDb {
        id:     String,
        input:  String,
        #[serde(default = "default_mode_str")]
        mode:   String,
        #[serde(flatten)]
        sink:   DbSinkCommon,
    },
    Scd2Sink {
        id:     String,
        input:  String,
        key:    String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        track:   Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scd2_columns: Option<Scd2ColumnNames>,
        /// When `true`, keys present in the database but absent from the
        /// incoming data are expired (`is_current = false`).
        /// Default: `false` (delta mode).
        #[serde(default)]
        close_missing: bool,
        #[serde(flatten)]
        sink:   DbSinkCommon,
    },
    Filter {
        id:    String,
        input: String,
        /// Legacy: `column: status` + `value: active`  →  keep rows where column == value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        column: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value:  Option<String>,
        /// Expression form: `condition: "status == \"active\" and total >= 1000"`
        #[serde(default, skip_serializing_if = "Option::is_none")]
        condition: Option<String>,
    },
    /// Add, compute, or rename columns using the expression DSL.
    ///
    /// ```yaml
    /// - id: enrich
    ///   type: map
    ///   input: source
    ///   columns:
    ///     load_ts:    now()
    ///     revenue:    price * quantity
    ///     order_year: year(order_date)
    ///     uid:        json_get(payload, "user.id")
    ///   # select_only: true  # drop all columns not listed above
    /// ```
    Map {
        id:    String,
        input: String,
        /// output_col_name → expression string.
        ///
        /// `IndexMap` preserves YAML document order so that:
        /// 1. Columns are evaluated in declaration order (later expressions can
        ///    reference columns produced by earlier ones without a retry pass).
        /// 2. `select_only: true` output column order matches the YAML order.
        #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
        columns: IndexMap<String, String>,
        /// When `true`, only columns listed in `columns` appear in the output.
        #[serde(default)]
        select_only: bool,
        /// **Deprecated — no-op.** Arrow metadata is propagated
        /// through the map step automatically.  This field is accepted
        /// for backward compatibility but has no effect.  Will be
        /// removed in a future version.
        #[serde(default, skip_serializing)]
        preserve_metadata: bool,
    },
    /// Group rows and compute aggregate metrics.
    ///
    /// Materialises all incoming batches before computing.
    ///
    /// ```yaml
    /// - id: by_customer
    ///   type: aggregate
    ///   input: map_step
    ///   group_by: [customer_id, region]
    ///   metrics:
    ///     total_sales: sum(revenue)
    ///     order_count: count()
    ///     avg_value:   avg(revenue)
    /// ```
    Aggregate {
        id:    String,
        input: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_by: Vec<String>,
        /// output_col_name → aggregate expression string
        #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
        metrics: IndexMap<String, String>,
    },
    Rename {
        id:      String,
        input:   String,
        #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
        columns: IndexMap<String, String>,
    },
    Join {
        id:    String,
        left:  String,
        right: String,
        on:    String,
        #[serde(default = "default_join_how_str")]
        how:   String,
    },
    Flatten {
        id:    String,
        input: String,
        #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
        select: IndexMap<String, String>,
    },
    /// Explode an array column into one row per element.
    ///
    /// ```yaml
    /// - id: explode_pools
    ///   type: unnest
    ///   input: raw_candidates
    ///   column: talent_pools
    ///   parent_fields:
    ///     candidate_id: id
    ///   fields:
    ///     talentpool_id: id
    ///     pool_data: data
    /// ```
    Unnest {
        id:     String,
        input:  String,
        /// Name of the column containing the array to explode.
        column: String,
        /// Sub-fields to extract from each array element.
        /// Key = output column name, value = dot-notation path within the element.
        #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
        fields: IndexMap<String, String>,
        /// Parent columns to carry forward into the output.
        /// Key = output column name, value = source column name.
        #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
        parent_fields: IndexMap<String, String>,
    },
    PythonTransform {
        id:    String,
        input: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        function: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        code: Option<String>,
    },
    RestApiSink {
        id:    String,
        input: String,
        #[serde(default)]
        url:   String,
        #[serde(skip_serializing_if = "Option::is_none")]
        conn:  Option<String>,
        #[serde(flatten)]
        opts:  RestApiSinkOptions,
    },
    /// Read a CSV file as a source (local or remote via file connection).
    ///
    /// Supports glob patterns (`*`, `?`, `[a-z]`) and recursive globs (`**`).
    ///
    /// ```yaml
    /// # Single file
    /// - id: employees
    ///   type: read_csv
    ///   path: data/employees.csv
    ///   delimiter: ","
    ///   has_header: true
    ///
    /// # Glob: all CSV files in a directory
    /// - id: daily_logs
    ///   type: read_csv
    ///   path: logs/2024-01-*.csv
    ///   sort_glob: name   # ascending alphabetical (default)
    ///
    /// # Recursive glob: search subdirectories
    /// - id: all_csvs
    ///   type: read_csv
    ///   path: data/**/report_*.csv
    ///
    /// # Connection-based with glob:
    /// - id: employees
    ///   type: read_csv
    ///   from:
    ///     connection: incoming_sftp
    ///     path: reports/*.csv
    /// ```
    ReadCsv {
        id:   String,
        /// Filesystem path to the CSV file (legacy — backwards compatible).
        /// Mutually exclusive with `from`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        /// Connection-based file location.
        /// Mutually exclusive with `path`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from: Option<FileSourceLocation>,
        /// Column delimiter character (default: `","`).
        #[serde(default = "default_csv_delimiter")]
        delimiter: String,
        /// Whether the first row is a header (default: `true`).
        #[serde(default = "default_true")]
        has_header: bool,
        /// Source-side schema settings.
        #[serde(flatten)]
        source_schema: SourceSchemaConfig,
        /// Per-step read batch size.
        #[serde(skip_serializing_if = "Option::is_none")]
        batch_size: Option<usize>,
        /// Sort order for glob-matched files: `name` (default), `name_desc`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sort_glob: Option<String>,
    },
    /// Write Arrow data to a CSV file (local or remote via file connection).
    ///
    /// ```yaml
    /// # Legacy: direct path
    /// - id: output
    ///   type: write_csv
    ///   input: transformed
    ///   path: output/result.csv
    ///
    /// # Connection-based:
    /// - id: output
    ///   type: write_csv
    ///   input: transformed
    ///   target:
    ///     connection: reports_sftp
    ///     path: monthly/result.csv
    /// ```
    WriteCsv {
        id:    String,
        input: String,
        /// Filesystem path for the output CSV file (legacy — backwards compatible).
        /// Mutually exclusive with `target`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        /// Connection-based file target.
        /// Mutually exclusive with `path`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<FileSinkTarget>,
        /// Column delimiter character (default: `","`).
        #[serde(default = "default_csv_delimiter")]
        delimiter: String,
        /// Whether to write a header row (default: `true`).
        #[serde(default = "default_true")]
        has_header: bool,
    },
    /// Write Arrow data to a JSON file (local or remote via file connection).
    ///
    /// ```yaml
    /// # Legacy: direct path
    /// - id: output
    ///   type: write_json
    ///   input: transformed
    ///   path: output/result.json
    ///   pretty: true
    ///   wrap_key: candidates
    ///
    /// # Connection-based:
    /// - id: output
    ///   type: write_json
    ///   input: transformed
    ///   target:
    ///     connection: data_lake_s3
    ///     path: processed/result.json
    ///   pretty: true
    /// ```
    WriteJson {
        id:    String,
        input: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<FileSinkTarget>,
        #[serde(default)]
        pretty: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wrap_key: Option<String>,
    },
    /// Read a Parquet file as a source (local or remote via file connection).
    ///
    /// Supports glob patterns (`*`, `?`, `[a-z]`) and recursive globs (`**`).
    ///
    /// ```yaml
    /// # Single file
    /// - id: events
    ///   type: read_parquet
    ///   path: data/events.parquet
    ///   columns: [event_id, user_id]   # optional: column projection
    ///
    /// # Glob: all Parquet partitions
    /// - id: partitions
    ///   type: read_parquet
    ///   path: warehouse/events_*.parquet
    ///   sort_glob: name   # ascending (default)
    ///
    /// # Recursive glob: search subdirectories
    /// - id: all_parquet
    ///   type: read_parquet
    ///   path: lake/**/*.parquet
    ///
    /// # Connection-based:
    /// - id: events
    ///   type: read_parquet
    ///   from:
    ///     connection: data_lake_s3
    ///     path: events/*.parquet
    ///   sort_glob: name_desc
    /// ```
    ReadParquet {
        id:   String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from: Option<FileSourceLocation>,
        /// Column projection -- only read these columns.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        columns: Option<Vec<String>>,
        #[serde(flatten)]
        source_schema: SourceSchemaConfig,
        #[serde(skip_serializing_if = "Option::is_none")]
        batch_size: Option<usize>,
        /// Sort order for glob-matched files: `name` (default), `name_desc`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sort_glob: Option<String>,
    },
    /// Write Arrow data to a Parquet file (local or remote via file connection).
    ///
    /// ```yaml
    /// - id: output
    ///   type: write_parquet
    ///   input: transformed
    ///   path: output/result.parquet
    ///   compression: zstd
    ///
    /// - id: output
    ///   type: write_parquet
    ///   input: transformed
    ///   target:
    ///     connection: data_lake_s3
    ///     path: processed/result.parquet
    /// ```
    WriteParquet {
        id:    String,
        input: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<FileSinkTarget>,
        /// Compression codec: none, snappy, gzip, lz4, zstd (default: snappy).
        #[serde(default = "default_parquet_compression")]
        compression: String,
    },
}

impl StepDef {
    /// Returns the step ID regardless of variant.
    fn id(&self) -> &str {
        match self {
            Self::ReadDb          { id, .. } => id,
            Self::RestApi         { id, .. } => id,
            Self::ReadJson        { id, .. } => id,
            Self::ReadCsv         { id, .. } => id,
            Self::WriteDb         { id, .. } => id,
            Self::Scd2Sink        { id, .. } => id,
            Self::Filter          { id, .. } => id,
            Self::Map             { id, .. } => id,
            Self::Aggregate       { id, .. } => id,
            Self::Rename          { id, .. } => id,
            Self::Join            { id, .. } => id,
            Self::Flatten         { id, .. } => id,
            Self::Unnest          { id, .. } => id,
            Self::PythonTransform { id, .. } => id,
            Self::RestApiSink     { id, .. } => id,
            Self::WriteCsv        { id, .. } => id,
            Self::WriteJson       { id, .. } => id,
            Self::ReadParquet     { id, .. } => id,
            Self::WriteParquet    { id, .. } => id,
        }
    }
}

fn default_mode_str()      -> String { "append".into() }
fn default_join_how_str()  -> String { "inner".into()  }
fn default_csv_delimiter() -> String { ",".into()      }
fn default_true()          -> bool   { true            }
fn default_parquet_compression() -> String { "snappy".into() }

/// Parse an optional `sort_glob` string into a [`GlobSortOrder`].
///
/// Returns `GlobSortOrder::Name` (ascending) when the string is `None` or empty.
fn parse_sort_glob(step_id: &str, step_type: &str, raw: Option<String>) -> anyhow::Result<GlobSortOrder> {
    match raw.as_deref() {
        None | Some("") => Ok(GlobSortOrder::default()),
        Some(s) => GlobSortOrder::from_str_opt(s).ok_or_else(|| anyhow::anyhow!(
            "{step_type} '{step_id}': unknown sort_glob value '{s}'. \
             Valid values: name, name_asc, name_desc"
        )),
    }
}

// ── File connection resolvers ─────────────────────────────────────────────────

/// Resolved file location: a fully-resolved path and the connection definition
/// (used by the executor to create the right `FileTransport`).
struct ResolvedFilePath {
    /// Fully resolved path (base_path + relative path).
    path: String,
    /// The connection definition (cloned for the executor).
    conn: ConnParams,
}

/// Resolves a file source location: either a direct `path` (legacy, local
/// filesystem) or a `from` block referencing a named file connection.
///
/// Exactly one of `path` or `from` must be provided.
fn resolve_file_source(
    step_id: &str,
    step_type: &str,
    path: Option<String>,
    from: Option<FileSourceLocation>,
    conns: &HashMap<String, ConnectionDef>,
) -> anyhow::Result<ResolvedFilePath> {
    match (path, from) {
        (Some(p), None) => {
            // Legacy: direct filesystem path -> implicit Local connection.
            Ok(ResolvedFilePath {
                path: p,
                conn: ConnParams::Local { base_path: String::new() },
            })
        }
        (None, Some(loc)) => {
            let conn_def = conns.get(&loc.connection).ok_or_else(|| anyhow::anyhow!(
                "{step_type} '{step_id}': connection '{}' is not defined in the \
                 'connections' map. Add it under the top-level 'connections' key.",
                loc.connection
            ))?;
            anyhow::ensure!(conn_def.is_file_connection(),
                "{step_type} '{step_id}': connection '{}' is a '{}' connection -- \
                 file steps require a file-transport connection (local, sftp, s3, \
                 azure_blob, gcs, sharepoint, ftp, smb).",
                loc.connection, conn_def.driver_name()
            );
            let resolved_path = conn_def.resolve_file_path(&loc.path)?;
            Ok(ResolvedFilePath {
                path: resolved_path,
                conn: conn_def.clone(),
            })
        }
        (Some(_), Some(_)) => anyhow::bail!(
            "{step_type} '{step_id}': supply either 'path' (direct) or 'from' \
             (connection-based), not both."
        ),
        (None, None) => anyhow::bail!(
            "{step_type} '{step_id}': must supply either 'path' (direct filesystem \
             path) or 'from' (connection-based file location)."
        ),
    }
}

/// Resolves a file sink target: either a direct `path` (legacy, local
/// filesystem) or a `target` block referencing a named file connection.
///
/// Exactly one of `path` or `target` must be provided.
fn resolve_file_sink(
    step_id: &str,
    step_type: &str,
    path: Option<String>,
    target: Option<FileSinkTarget>,
    conns: &HashMap<String, ConnectionDef>,
) -> anyhow::Result<ResolvedFilePath> {
    match (path, target) {
        (Some(p), None) => {
            Ok(ResolvedFilePath {
                path: p,
                conn: ConnParams::Local { base_path: String::new() },
            })
        }
        (None, Some(loc)) => {
            let conn_def = conns.get(&loc.connection).ok_or_else(|| anyhow::anyhow!(
                "{step_type} '{step_id}': connection '{}' is not defined in the \
                 'connections' map. Add it under the top-level 'connections' key.",
                loc.connection
            ))?;
            anyhow::ensure!(conn_def.is_file_connection(),
                "{step_type} '{step_id}': connection '{}' is a '{}' connection -- \
                 file steps require a file-transport connection (local, sftp, s3, \
                 azure_blob, gcs, sharepoint, ftp, smb).",
                loc.connection, conn_def.driver_name()
            );
            let resolved_path = conn_def.resolve_file_path(&loc.path)?;
            Ok(ResolvedFilePath {
                path: resolved_path,
                conn: conn_def.clone(),
            })
        }
        (Some(_), Some(_)) => anyhow::bail!(
            "{step_type} '{step_id}': supply either 'path' (direct) or 'target' \
             (connection-based), not both."
        ),
        (None, None) => anyhow::bail!(
            "{step_type} '{step_id}': must supply either 'path' (direct filesystem \
             path) or 'target' (connection-based file location)."
        ),
    }
}

// ── Connection resolvers ──────────────────────────────────────────────────────

/// Looks up a connection and returns both the URL and the connection-level
/// driver options as `StepDriverOptions` defaults.
///
/// Step-level options should be merged on top of these via
/// [`StepDriverOptions::merge_from`].
fn resolve_conn_with_options(
    conn:        &str,
    connections: &HashMap<String, ConnectionDef>,
) -> anyhow::Result<(String, StepDriverOptions)> {
    let conn_def = connections
        .get(conn)
        .ok_or_else(|| anyhow::anyhow!(
            "Connection '{conn}' is not defined in the 'connections' map. \
             Add it under the top-level 'connections' key."
        ))?;
    let url = conn_def.to_url()?;
    let conn_opts = conn_def.to_step_driver_options();
    Ok((url, conn_opts))
}

/// Resolved configuration for a REST API connection.
struct RestApiConn {
    base_url:       String,
    auth:           Option<AuthConfig>,
    headers:        HashMap<String, String>,
    timeout_secs:   u64,
    rate_limit_rps: Option<f64>,
}

/// Looks up a named `rest_api` connection.
///
/// Returns an error if the name is absent or if the connection uses a
/// database driver (which cannot be used in a `rest_api` step).
fn resolve_rest_conn(
    conn_name:   &str,
    connections: &HashMap<String, ConnectionDef>,
) -> anyhow::Result<RestApiConn> {
    match connections.get(conn_name) {
        Some(ConnParams::RestApi { base_url, auth, headers, timeout_secs, rate_limit_rps }) => {
            Ok(RestApiConn {
                base_url:       base_url.clone(),
                auth:           auth.clone(),
                headers:        headers.clone(),
                timeout_secs:   *timeout_secs,
                rate_limit_rps: *rate_limit_rps,
            })
        }
        Some(_) => anyhow::bail!(
            "Connection '{conn_name}' is a database connection -- use a \
             `driver: rest_api` connection for `rest_api` and `rest_api_sink` steps."
        ),
        None => anyhow::bail!(
            "Connection '{conn_name}' is not defined in the 'connections' map."
        ),
    }
}

/// Resolves the final URL for a REST API step.
///
/// - If `url` starts with `http://` or `https://` -- used as-is (ignores `base_url`).
/// - If `url` starts with `/` -- appended to `base_url` (path under the base).
/// - If `url` is empty -- `base_url` is used directly.
fn resolve_rest_url(url: &str, base_url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_owned()
    } else {
        let base = base_url.trim_end_matches('/');
        if url.is_empty() {
            base.to_owned()
        } else {
            format!("{base}{}", if url.starts_with('/') { url.to_owned() } else { format!("/{url}") })
        }
    }
}

/// Resolves a REST API step's URL and merges connection defaults into the
/// step options.  Shared by `RestApi` and `RestApiSink` to avoid duplicate
/// connection resolution logic.
fn resolve_rest_step<T: RestHttpCommon>(
    url:  String,
    conn: Option<String>,
    mut opts: T,
    conns: &HashMap<String, ConnectionDef>,
) -> anyhow::Result<(String, T)> {
    match conn {
        Some(conn_name) => {
            let rc = resolve_rest_conn(&conn_name, conns)?;
            let final_url = resolve_rest_url(&url, &rc.base_url);
            opts.merge_conn_defaults(
                &rc.auth, &rc.headers, rc.timeout_secs, &rc.rate_limit_rps,
            );
            Ok((final_url, opts))
        }
        None => Ok((url, opts)),
    }
}

/// Merges the step-level `database` config into the step-level `SinkSchemaConfig`.
///
/// This allows users to define DDL-only columns (e.g. `meta_updated_at` with
/// `default_expr` / `on_update_expr`) under `database.columns` -- a step-level
/// field that sits alongside `target`, `mode`, etc.
///
/// Step-level `schema.database.columns` entries take precedence: if the same
/// column name appears in both places, the step-level definition wins.
fn merge_target_database(
    mut sink_schema: SinkSchemaConfig,
    target_db: Option<DatabaseSchemaConfig>,
) -> SinkSchemaConfig {
    let Some(target_db) = target_db else { return sink_schema };

    let schema_db = sink_schema.schema.database.get_or_insert_with(Default::default);

    // Merge columns: database.columns are the base; step-level schema.database.columns win.
    for (col_name, col_def) in target_db.columns {
        // Only insert if the step-level schema.database doesn't already define this column.
        if !schema_db.columns.iter().any(|(k, _)| k.eq_ignore_ascii_case(&col_name)) {
            tracing::debug!(
                column = %col_name,
                default_expr = ?col_def.default_expr,
                on_update_expr = ?col_def.on_update_expr,
                "merge_target_database: merged DDL column from step-level database"
            );
            schema_db.columns.insert(col_name, col_def);
        }
    }

    // Merge indexes (same precedence rule).
    for (idx_name, idx_def) in target_db.indexes {
        if !schema_db.indexes.contains_key(&idx_name) {
            schema_db.indexes.insert(idx_name, idx_def);
        }
    }

    // Merge constraints (same precedence rule).
    for (cst_name, cst_def) in target_db.constraints {
        if !schema_db.constraints.contains_key(&cst_name) {
            schema_db.constraints.insert(cst_name, cst_def);
        }
    }

    sink_schema
}

// ── Database sink connection resolution (shared by WriteDb + Scd2Sink) ────────

/// Resolved database sink connection: URL, merged driver options, and merged
/// schema config.  Used by both `WriteDb` and `Scd2Sink` to avoid duplicating
/// the resolve -> merge -> merge_target_database sequence.
struct ResolvedDbSink {
    conn_str:   String,
    options:    StepDriverOptions,
    sink_schema: SinkSchemaConfig,
    table:      String,
    db_schema:  Option<String>,
    create_table: CreateTableMode,
    batch_size: Option<usize>,
}

// ── Shared builder ────────────────────────────────────────────────────────────

/// Consumes a `PipelineDoc` (shared between JSON and YAML loaders) and builds a `Dag`.
pub(crate) fn build_dag_from_doc(doc: PipelineDoc) -> anyhow::Result<Dag> {
    let mut dag = Dag::new(doc.config);
    dag.environment = doc.environment;
    let conns   = &doc.connections;

    // ── Early duplicate-ID check ─────────────────────────────────────────
    // Catch duplicate step IDs at load time with a clear error message,
    // before the DAG starts building (where IndexMap silently overwrites).
    {
        let mut seen_ids = std::collections::HashSet::new();
        for step in &doc.steps {
            let id = step.id();
            anyhow::ensure!(
                seen_ids.insert(id),
                "Duplicate step id '{id}'. Every step in the pipeline must have \
                 a unique `id`. Rename one of the duplicate '{id}' steps \
                 (e.g. '{id}_pg' and '{id}_oracle')."
            );
        }
    }

    for step in doc.steps {
        match step {
            StepDef::ReadDb { id, from, source_schema, batch_size, options } => {
                let (conn_str, conn_opts) = resolve_conn_with_options(&from.connection, conns)?;
                // Merge connection-level defaults into step-level options.
                // Currently a no-op for most drivers (options are in the URL),
                // but ensures future connection-level options propagate.
                let mut merged_options = options;
                merged_options.merge_from(&conn_opts);
                dag.add_source(id, conn_str, ReadOptions {
                    table: from.table, db_schema: from.schema,
                    query: from.query, cursor: from.cursor,
                    source_schema,
                    batch_size,
                    options: merged_options,
                });
            }

            StepDef::RestApi { id, url, conn, source_schema, opts } => {
                let (final_url, final_opts) = resolve_rest_step(url, conn, opts, conns)?;
                dag.add_rest_api(id, final_url, final_opts, source_schema);
            }

            StepDef::ReadJson { id, path, from, data_path, source_schema, batch_size, sort_glob } => {
                let resolved = resolve_file_source(&id, "ReadJson", path, from, conns)?;
                let sort = parse_sort_glob(&id, "ReadJson", sort_glob)?;
                dag.add_json_source_from(id, resolved.conn, resolved.path, data_path, source_schema, batch_size, sort);
            }

            StepDef::ReadCsv { id, path, from, delimiter, has_header, source_schema, batch_size, sort_glob } => {
                let resolved = resolve_file_source(&id, "ReadCsv", path, from, conns)?;
                let sort = parse_sort_glob(&id, "ReadCsv", sort_glob)?;
                dag.add_csv_source_from(id, resolved.conn, resolved.path, delimiter, has_header, source_schema, batch_size, sort);
            }

            StepDef::WriteDb { id, input, mode, sink } => {
                let resolved = sink.resolve(conns)?;
                let sink_mode = match mode.as_str() {
                    "append"        => SinkMode::Append,
                    "insert_ignore" => SinkMode::InsertIgnore,
                    "upsert"        => SinkMode::Upsert,
                    "merge_delete"  => SinkMode::MergeDelete,
                    "truncate"      => SinkMode::Truncate,
                    other           => anyhow::bail!(
                        "WriteDb '{id}': unknown mode '{other}'. \
                         Valid modes: append, insert_ignore, upsert, merge_delete, truncate"
                    ),
                };
                dag.add_sink(id, input, resolved.conn_str, WriteOptions {
                    table: resolved.table, db_schema: resolved.db_schema,
                    mode: sink_mode, create_table: resolved.create_table,
                    batch_size: resolved.batch_size, options: resolved.options,
                    sink_schema: resolved.sink_schema,
                });
            }

            StepDef::Scd2Sink { id, input, key, track, scd2_columns, close_missing, sink } => {
                let resolved = sink.resolve(conns)?;
                let col_names = scd2_columns.unwrap_or_default();
                dag.add_scd2_sink(id, input, resolved.conn_str, resolved.table, resolved.db_schema,
                                  key, track, col_names, resolved.create_table,
                                  resolved.sink_schema, resolved.batch_size, resolved.options, close_missing);
            }

            StepDef::Filter { id, input, column, value, condition } => {
                dag.add_filter(id, input, column, value, condition);
            }

            StepDef::Map { id, input, columns, select_only, preserve_metadata: _, } => {
                dag.add_map(id, input, columns, select_only);
            }

            StepDef::Aggregate { id, input, group_by, metrics } => {
                dag.add_aggregate(id, input, group_by, metrics);
            }

            StepDef::Rename { id, input, columns } => {
                dag.add_rename(id, input, columns);
            }

            StepDef::Join { id, left, right, on, how } => {
                let join_how = match how.as_str() {
                    "inner"                => JoinHow::Inner,
                    "left" | "left_outer"  => JoinHow::Left,
                    "full" | "full_outer"  => JoinHow::Full,
                    other => anyhow::bail!(
                        "Unknown join type '{other}'. Valid: inner, left, left_outer, full, full_outer"
                    ),
                };
                dag.add_join(id, left, right, on, join_how);
            }

            StepDef::Flatten { id, input, select } => {
                dag.add_flatten(id, input, select);
            }

            StepDef::Unnest { id, input, column, fields, parent_fields } => {
                dag.add_unnest(id, input, column, fields, parent_fields);
            }

            StepDef::PythonTransform { id, input, function, code } => {
                match (function, code) {
                    (Some(name), None) => {
                        dag.add_named_transform(id.clone(), input.clone(), name);
                    }
                    (None, Some(src)) => {
                        let internal_name = format!("__inline_{id}");
                        dag.inline_python_codes.insert(internal_name.clone(), src);
                        dag.add_named_transform(id.clone(), input.clone(), internal_name);
                    }
                    (Some(_), Some(_)) => anyhow::bail!(
                        "PythonTransform '{id}': supply either 'function' or 'code', not both"
                    ),
                    (None, None) => anyhow::bail!(
                        "PythonTransform '{id}': must supply either 'function' or 'code'"
                    ),
                }
            }

            StepDef::RestApiSink { id, input, url, conn, opts } => {
                let (final_url, final_opts) = resolve_rest_step(url, conn, opts, conns)?;
                dag.add_rest_api_sink(id, input, final_url, final_opts);
            }

            StepDef::WriteCsv { id, input, path, target, delimiter, has_header } => {
                let resolved = resolve_file_sink(&id, "WriteCsv", path, target, conns)?;
                dag.add_csv_sink_from(id, input, resolved.conn, resolved.path, delimiter, has_header);
            }

            StepDef::WriteJson { id, input, path, target, pretty, wrap_key } => {
                let resolved = resolve_file_sink(&id, "WriteJson", path, target, conns)?;
                dag.add_json_sink_from(id, input, resolved.conn, resolved.path, pretty, wrap_key);
            }

            StepDef::ReadParquet { id, path, from, columns, source_schema, batch_size, sort_glob } => {
                let resolved = resolve_file_source(&id, "ReadParquet", path, from, conns)?;
                let sort = parse_sort_glob(&id, "ReadParquet", sort_glob)?;
                dag.add_parquet_source_from(id, resolved.conn, resolved.path, columns, source_schema, batch_size, sort);
            }

            StepDef::WriteParquet { id, input, path, target, compression } => {
                let resolved = resolve_file_sink(&id, "WriteParquet", path, target, conns)?;
                dag.add_parquet_sink_from(id, input, resolved.conn, resolved.path, compression);
            }
        }
    }

    Ok(dag)
}

// ── Dag::from_json ────────────────────────────────────────────────────────────

impl Dag {
    /// Loads a complete pipeline definition from a JSON string.
    pub fn from_json(json: &str) -> anyhow::Result<Self> {
        if json.contains("secret::") {
            anyhow::bail!(
                "Pipeline JSON contains `secret::` references. \
                 Use `Dag::from_json_async()` instead of `Dag::from_json()` \
                 to enable secret resolution."
            );
        }
        let doc: PipelineDoc = serde_json::from_str(json)
            .map_err(|e| anyhow::anyhow!("JSON parse error: {e}"))?;
        build_dag_from_doc(doc)
    }

    /// Loads a pipeline definition from a JSON string, resolving secret references.
    ///
    /// This is the async version of [`from_json`] that supports `secret::*`
    /// references in connection strings and other config values.
    pub async fn from_json_async(json: &str) -> anyhow::Result<Self> {
        use potato_etl_common::secrets::resolve::{resolve_secrets_in_value, extract_secrets_config};

        // Parse JSON -> serde_yaml Value (serde_yaml Value is a superset that handles JSON fine).
        let json_value: serde_json::Value = serde_json::from_str(json)
            .map_err(|e| anyhow::anyhow!("JSON parse error: {e}"))?;
        let mut value: serde_yaml_ng::Value = serde_yaml_ng::to_value(&json_value)
            .map_err(|e| anyhow::anyhow!("JSON->YAML value conversion: {e}"))?;

        let secrets_config = extract_secrets_config(&value)?;
        if !secrets_config.is_empty() || json.contains("secret::") {
            value = resolve_secrets_in_value(value, &secrets_config).await?;
        }

        // Remove secrets key.
        if let serde_yaml_ng::Value::Mapping(ref mut map) = value {
            map.remove(&serde_yaml_ng::Value::String("secrets".into()));
        }

        let doc: PipelineDoc = serde_yaml_ng::from_value(value)
            .map_err(|e| anyhow::anyhow!("JSON parse error (after secret resolution): {e}"))?;

        build_dag_from_doc(doc)
    }
}