//! Per-driver connection options and per-step driver overrides.

use serde::{Deserialize, Serialize};

// ── Helper for skip_serializing_if ────────────────────────────────────────────

/// Helper for `#[serde(skip_serializing_if = "is_default")]`.
pub(crate) fn is_default<T: Default + PartialEq>(t: &T) -> bool { *t == T::default() }

// ── PostgresOptions ───────────────────────────────────────────────────────────

/// Connection options specific to **PostgreSQL**.
///
/// All fields are optional; omitting `options:` entirely is valid.
///
/// ```yaml
/// options:
///   ssl: require              # sslmode parameter (disable|allow|prefer|require|verify-ca|verify-full)
///   # connect_timeout: 30     # seconds before the connection attempt times out
///   # application_name: potatoflow   # shown in pg_stat_activity / slow-query logs
///   # max_connections: 5       # pool size
///   # staging_table: true      # use COPY BINARY via temp staging table for upsert/insert_ignore
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PostgresOptions {
    /// `sslmode` query parameter appended to the connection URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl: Option<String>,

    /// Seconds before a connection attempt is abandoned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_timeout: Option<u32>,

    /// Application name shown in `pg_stat_activity` and slow-query logs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub application_name: Option<String>,

    /// Maximum number of connections in the pool.  Default: `5`.
    ///
    /// For high-throughput bulk loads the default is usually sufficient.
    /// Increase when multiple concurrent sinks write to the same Postgres
    /// instance, or when upsert staging-table operations benefit from
    /// parallelism.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<u32>,

    /// Use a temporary staging table for `insert_ignore`, `upsert`, and
    /// `merge_delete` write modes.  Default: `false`.
    ///
    /// When enabled, the sink COPY-BINARY-loads data into a temp table and
    /// then performs a single `INSERT … ON CONFLICT` from that staging table.
    /// This is ~5-10× faster than the default chunked parameterized INSERTs
    /// for large batches, but requires permissions to create temp tables.
    ///
    /// ```yaml
    /// options:
    ///   postgres:
    ///     staging_table: true
    /// ```
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub staging_table: bool,

    /// SQL statements executed on every new connection in the pool.
    ///
    /// Use this to set session-level variables (e.g. `work_mem`, `statement_timeout`,
    /// `search_path`) that must persist for the lifetime of each connection.
    ///
    /// ```yaml
    /// options:
    ///   init_sql:
    ///     - "SET work_mem = '256MB'"
    ///     - "SET statement_timeout = 60000"
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub init_sql: Vec<String>,
}

impl PostgresOptions {
    /// Merges connection-level defaults into this step-level config.
    ///
    /// Step-level values take precedence for `Option` fields.  For `bool`
    /// fields (default `false`), the connection-level value is inherited
    /// when the step hasn't explicitly set it to `true`.
    pub fn merge_from(&mut self, conn_defaults: &PostgresOptions) {
        if self.ssl.is_none()              { self.ssl = conn_defaults.ssl.clone(); }
        if self.connect_timeout.is_none()  { self.connect_timeout = conn_defaults.connect_timeout; }
        if self.application_name.is_none() { self.application_name = conn_defaults.application_name.clone(); }
        if self.max_connections.is_none()  { self.max_connections = conn_defaults.max_connections; }
        // Bool fields: inherit connection-level `true` when step is default (false).
        if !self.staging_table { self.staging_table = conn_defaults.staging_table; }
        // Vec fields: inherit connection-level when step is empty.
        if self.init_sql.is_empty() { self.init_sql = conn_defaults.init_sql.clone(); }
    }
}

// ── MssqlOptions ─────────────────────────────────────────────────────────────

/// Connection options specific to **SQL Server** (tiberius / bcp / odbc).
///
/// ```yaml
/// options:
///   mode: tiberius             # tiberius (default) | bcp | odbc
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MssqlOptions {
    /// Write-path mode: `tiberius` (default), `bcp`, or `odbc`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    /// Application name sent to the server during login.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub application_name: Option<String>,

    /// Seconds before the login handshake is considered timed out.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub login_timeout: Option<u32>,

    /// Skip TLS certificate validation.  **Never enable this in production.**
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub trust_cert: bool,

    /// Explicit path to the `bcp` binary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bcp_path: Option<String>,

    /// Rows per bulk write operation (tiberius bulk insert, BCP `-b` flag,
    /// ODBC bulk insert chunk size).  Default: `10 000`.
    ///
    /// This is the MSSQL-specific batch size that controls how many rows are
    /// written in a single bulk operation.  It is separate from the pipeline-
    /// level `batch_size` which controls RecordBatch chunking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<usize>,

    /// Use a staging table for bulk operations.  Default: `None` (auto).
    ///
    /// - **Auto (`None`)**: staging is used for `upsert`, `insert_ignore`, and
    ///   `merge_delete` strategies (required — MERGE needs a source table).
    ///   Append and truncate go directly to the target.
    /// - **`true`**: force staging for ALL strategies, including append.
    /// - **`false`**: disable staging.  **WARNING:** disabling staging for
    ///   upsert/insert_ignore/merge_delete will cause data integrity errors.
    ///
    /// BCP staging is controlled separately by `bcp_staging` in
    /// `StepDriverOptions`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staging_table: Option<bool>,

    /// SQL statements executed on every new connection.
    ///
    /// Appended after the built-in session options (`NOCOUNT ON`, etc.).
    ///
    /// ```yaml
    /// options:
    ///   init_sql:
    ///     - "SET LOCK_TIMEOUT 5000"
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub init_sql: Vec<String>,
}

impl MssqlOptions {
    /// Resolves the effective write-path mode.
    ///
    /// Returns `"tiberius"` (default), `"bcp"`, or `"odbc"`.
    pub fn effective_mode(&self) -> &str {
        self.mode.as_deref().unwrap_or("tiberius")
    }

    /// Merges connection-level defaults into this step-level config.
    ///
    /// Step-level values take precedence — connection-level values are only
    /// used when the step doesn't specify them.  This lets users set
    /// `staging_table: true` once on the connection and have all steps inherit
    /// it without repetition.
    pub fn merge_from(&mut self, conn_defaults: &MssqlOptions) {
        macro_rules! merge_opt {
            ($field:ident) => {
                if self.$field.is_none() {
                    self.$field = conn_defaults.$field.clone();
                }
            };
        }
        merge_opt!(mode);
        merge_opt!(application_name);
        merge_opt!(login_timeout);
        merge_opt!(bcp_path);
        merge_opt!(batch_size);
        merge_opt!(staging_table);
        if !self.trust_cert { self.trust_cert = conn_defaults.trust_cert; }
        if self.init_sql.is_empty() { self.init_sql = conn_defaults.init_sql.clone(); }
    }
}

// ── OracleOptions ─────────────────────────────────────────────────────────────

/// Connection options specific to **Oracle Database**.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OracleOptions {
    /// Use a session-scoped staging table for `upsert` / `merge_delete` writes.
    ///
    /// Each batch is bulk-INSERTed into `_ETL_STG_<target>`; at flush a single
    /// set-based `MERGE INTO target USING staging` runs. Trades one DDL +
    /// staging space for orders-of-magnitude faster server-side merging vs the
    /// default per-row `MERGE INTO target USING (SELECT … FROM DUAL)` path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staging_table: Option<bool>,
}

// ── MySqlOptions ──────────────────────────────────────────────────────────────

/// Connection options specific to **MySQL / Aurora / MariaDB**.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MySqlOptions {
    /// SSL mode: `disabled` | `preferred` | `required`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl_mode: Option<String>,

    /// Seconds before a connection attempt times out.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_timeout: Option<u32>,

    /// MySQL character set.  Defaults to `utf8mb4`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charset: Option<String>,

    /// SQL statements executed on every new connection in the pool.
    ///
    /// ```yaml
    /// options:
    ///   init_sql:
    ///     - "SET SESSION group_concat_max_len = 1048576"
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub init_sql: Vec<String>,
}

// ── DatabricksOdbcOptions ─────────────────────────────────────────────────────

/// ODBC transport configuration for the Databricks Simba ODBC driver.
///
/// Accepts either a bare `bool` (backward-compatible shorthand) or the full struct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatabricksOdbcOptions {
    /// Whether ODBC transport is enabled.  Default: `false`.
    #[serde(default)]
    pub enable: bool,

    /// Explicit path to the ODBC driver shared library.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub driver_path: Option<String>,

    /// TCP port for the ODBC connection.  Default: `443`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

    /// Enable SSL/TLS.  Default: `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ssl: Option<bool>,

    /// Thrift transport mode.  `2` = HTTP (default), `0` = binary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thrift_transport: Option<u8>,

    /// Pass SQL through verbatim (no Simba rewriting).  Default: `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_native_query: Option<bool>,

    /// Maximum string column length reported by the Simba driver.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string_column_length: Option<u32>,

    /// Report string columns as Unicode SQL types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_unicode_sql_character_types: Option<bool>,

    /// Report STRING columns as SQL_WLONGVARCHAR.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_long_varchar: Option<bool>,
}

impl Default for DatabricksOdbcOptions {
    fn default() -> Self {
        Self {
            enable: false,
            driver_path: None,
            port: None,
            ssl: None,
            thrift_transport: None,
            use_native_query: None,
            string_column_length: None,
            use_unicode_sql_character_types: None,
            use_long_varchar: None,
        }
    }
}

/// Custom deserializer that accepts either `bool` or the full struct.
pub(crate) fn deserialize_databricks_odbc<'de, D>(deserializer: D) -> Result<DatabricksOdbcOptions, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de;

    struct OdbcVisitor;

    impl<'de> de::Visitor<'de> for OdbcVisitor {
        type Value = DatabricksOdbcOptions;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a boolean or an ODBC options map")
        }

        fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
            Ok(DatabricksOdbcOptions { enable: v, ..Default::default() })
        }

        fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(OdbcVisitor)
}

// ── DatabricksOptions ─────────────────────────────────────────────────────────

/// Connection options specific to **Databricks SQL warehouse**.
///
/// ```yaml
/// options:
///   mode: api                 # api (default) | odbc | thrift
///   catalog: main
///   schema: analytics
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DatabricksOptions {
    /// Read-path mode: `api` (default), `odbc`, or `thrift`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    /// Unity Catalog catalog.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog: Option<String>,

    /// Default database / schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// HTTP client timeout in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_timeout: Option<u32>,

    /// ODBC transport configuration.
    #[serde(default, deserialize_with = "deserialize_databricks_odbc")]
    pub odbc: DatabricksOdbcOptions,

    /// Additional key=value parameters passed to the underlying driver.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub url_params: Vec<String>,

    /// SQL statements executed at the start of each source/sink operation.
    ///
    /// For the REST API transport, each statement is executed as a separate
    /// DML call before the main query.  For ODBC and Thrift transports,
    /// statements are executed on the connection and persist for its lifetime.
    ///
    /// Common use cases:
    /// - `USE catalog.schema`
    /// - `SET spark.sql.shuffle.partitions = 200`
    /// - `SET spark.databricks.delta.optimizeWrite.enabled = true`
    ///
    /// ```yaml
    /// options:
    ///   init_sql:
    ///     - "SET spark.sql.shuffle.partitions = 200"
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub init_sql: Vec<String>,
}

impl DatabricksOptions {
    /// Resolves the effective read-path mode.
    ///
    /// Priority: `mode` field > legacy `odbc.enable` boolean > default (`api`).
    pub fn effective_mode(&self) -> &str {
        if let Some(ref m) = self.mode {
            return m.as_str();
        }
        if self.odbc.enable { return "odbc"; }
        "api"
    }
}

// ── IdentifierCase ────────────────────────────────────────────────────────────

/// Identifier case transformation strategy for SQL identifiers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierCase {
    /// Keep identifiers exactly as they appear.
    #[default]
    AsIs,
    /// Transform all identifiers to UPPERCASE.
    Upper,
    /// Transform all identifiers to lowercase.
    Lower,
}

impl IdentifierCase {
    /// Applies the case transformation to the given identifier.
    #[inline]
    pub fn transform(&self, ident: &str) -> String {
        match self {
            Self::AsIs  => ident.to_string(),
            Self::Upper => ident.to_uppercase(),
            Self::Lower => ident.to_lowercase(),
        }
    }
}

// ── StepDriverOptions ────────────────────────────────────────────────────────

/// Driver-specific options for a pipeline step (`read_db`, `write_db`, `scd2_sink`).
///
/// These override the corresponding settings from the connection-level `options:`
/// block **for this step only**.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StepDriverOptions {
    /// Driver-path mode override for this step.
    ///
    /// MSSQL: `tiberius` (default), `bcp`, `odbc`.
    /// Databricks: `api` (default), `odbc`, `thrift`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    /// Explicit path to the `bcp` binary (MSSQL only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bcp_path: Option<String>,

    /// Override bcp staging behaviour (MSSQL only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bcp_staging: Option<bool>,

    /// Use a session-scoped staging table for `upsert` / `merge_delete` writes.
    ///
    /// Cross-driver flag honored by:
    /// - **Oracle** — bulk-INSERT into staging, single set-based MERGE at flush.
    /// - **Postgres** — `staging_table` in `PostgresOptions` takes precedence
    ///   when set; this is the top-level fallback.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staging_table: Option<bool>,

    /// Rows fetched per OCI round-trip (Oracle only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefetch_rows: Option<u32>,

    /// Internal OCI array fetch size (Oracle only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_array_size: Option<u32>,

    /// Use Oracle direct-path INSERT via `APPEND_VALUES` hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direct_path: Option<bool>,

    /// Oracle parallel DML degree.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel: Option<u32>,

    /// Maximum rows per OCI `batch.execute()` call (Oracle only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oci_batch_size: Option<usize>,

    /// Case transformation strategy for SQL identifiers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier_case: Option<IdentifierCase>,

    /// What to do when the target table has columns NOT present in the incoming
    /// batch. `error` (default) fails the write; `skip` leaves those columns at
    /// their DEFAULT/NULL. Applies to every database sink.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_missing_column: Option<crate::db::common::alignment::MissingColumnBehavior>,

    /// Databricks-specific REST API options.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub databricks: Option<DatabricksSourceOptions>,

    /// Postgres-specific connection options (pool size, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres: Option<PostgresOptions>,

    /// MSSQL-specific connection options (staging, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mssql: Option<MssqlOptions>,

    /// MySQL-specific connection options.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mysql: Option<MySqlOptions>,

    /// Oracle-specific connection options (staging-table flow, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oracle: Option<OracleOptions>,

    /// SQL statements executed at the start of each source/sink operation.
    ///
    /// This is a driver-agnostic field that propagates `init_sql` from
    /// connection-level options to drivers that don't have a dedicated
    /// per-driver sub-struct on `StepDriverOptions` (e.g. Databricks).
    ///
    /// Drivers that DO have a per-driver sub-struct (Postgres, MySQL, MSSQL)
    /// read `init_sql` from their own options instead.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub init_sql: Vec<String>,
}

impl StepDriverOptions {
    /// Returns `true` when all fields are `None`.
    pub fn is_empty(&self) -> bool {
        self.mode.is_none()
            && self.bcp_path.is_none()
            && self.bcp_staging.is_none()
            && self.staging_table.is_none()
            && self.prefetch_rows.is_none()
            && self.fetch_array_size.is_none()
            && self.direct_path.is_none()
            && self.parallel.is_none()
            && self.oci_batch_size.is_none()
            && self.identifier_case.is_none()
            && self.on_missing_column.is_none()
            && self.databricks.is_none()
            && self.postgres.is_none()
            && self.mssql.is_none()
            && self.mysql.is_none()
            && self.oracle.is_none()
            && self.init_sql.is_empty()
    }

    /// Resolves the effective MSSQL write-path mode for this step.
    pub fn effective_mssql_mode(&self) -> Option<&str> {
        self.mode.as_deref()
    }

    /// Resolves the effective Databricks read-path mode for this step.
    pub fn effective_databricks_mode(&self) -> Option<&str> {
        self.mode.as_deref()
    }

    /// Merges connection-level defaults into this step-level config.
    ///
    /// Step-level values take precedence — connection-level values are only
    /// used when the step doesn't specify them.  This lets users set
    /// `staging_table: true` once on the connection and have all steps inherit
    /// it without repetition.
    pub fn merge_from(&mut self, conn_defaults: &StepDriverOptions) {
        macro_rules! merge_opt {
            ($field:ident) => {
                if self.$field.is_none() {
                    self.$field = conn_defaults.$field.clone();
                }
            };
        }
        merge_opt!(mode);
        merge_opt!(bcp_path);
        merge_opt!(bcp_staging);
        merge_opt!(staging_table);
        merge_opt!(prefetch_rows);
        merge_opt!(fetch_array_size);
        merge_opt!(direct_path);
        merge_opt!(parallel);
        merge_opt!(oci_batch_size);
        merge_opt!(identifier_case);
        merge_opt!(on_missing_column);
        merge_opt!(databricks);

        // Driver-specific sub-structs: field-level merge.
        match (&mut self.postgres, &conn_defaults.postgres) {
            (Some(step_pg), Some(conn_pg)) => step_pg.merge_from(conn_pg),
            (None, Some(conn_pg)) => self.postgres = Some(conn_pg.clone()),
            _ => {}
        }
        match (&mut self.mssql, &conn_defaults.mssql) {
            (Some(step_ms), Some(conn_ms)) => step_ms.merge_from(conn_ms),
            (None, Some(conn_ms)) => self.mssql = Some(conn_ms.clone()),
            _ => {}
        }
        // MySQL: no field-level merge needed yet — just inherit if absent.
        match (&self.mysql, &conn_defaults.mysql) {
            (None, Some(conn_my)) => self.mysql = Some(conn_my.clone()),
            _ => {}
        }
        // Oracle: inherit when step is absent; OracleOptions fields use Option
        // so step-level None falls back to connection default.
        match (&mut self.oracle, &conn_defaults.oracle) {
            (Some(step_or), Some(conn_or)) => {
                if step_or.staging_table.is_none() { step_or.staging_table = conn_or.staging_table; }
            }
            (None, Some(conn_or)) => self.oracle = Some(conn_or.clone()),
            _ => {}
        }
        // Top-level init_sql: inherit when step is empty.
        if self.init_sql.is_empty() {
            self.init_sql = conn_defaults.init_sql.clone();
        }
    }
}

// ── DatabricksSourceOptions ───────────────────────────────────────────────────

/// Databricks REST SQL Statement API transport options.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DatabricksSourceOptions {
    /// Number of Arrow IPC chunks to download in parallel.  Default: `4`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_prefetch: Option<usize>,

    /// Rows per FetchResults RPC call in Thrift mode.  Default: `100_000`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thrift_fetch_size: Option<usize>,

    /// Maximum seconds to wait for the first batch from the Databricks ODBC
    /// source.  Covers cold-start / auto-resume latency for SQL warehouses.
    /// Default: `0` (disabled — no timeout, watchdog logs only).
    /// Set to e.g. `600` (10 minutes) to abort if the warehouse doesn't respond.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warehouse_timeout: Option<u64>,
}