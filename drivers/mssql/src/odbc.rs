//! ODBC-based MSSQL write path — no KEEPNULLS, SQL Server fills DEFAULTs.
//!
//! ## Why ODBC instead of tiberius for writes?
//!
//! tiberius `bulk_insert` uses the TDS `INSERT BULK` protocol which **implicitly
//! enables KEEPNULLS**.  This means NULL values in the data stream are inserted
//! as SQL NULL — even when the target column has a `DEFAULT` constraint, an
//! `IDENTITY` specification, or is a computed column.
//!
//! The ODBC path (via auto-detected Microsoft ODBC Driver 17+) uses prepared `INSERT` with
//! columnar array parameter binding.  **KEEPNULLS is NOT set**, matching the
//! behavior of:
//!
//! - .NET `SqlBulkCopy` (with `SqlBulkCopyOptions.Default`)
//! - Talend tMSSqlOutput
//! - SSIS OLE DB Destination
//!
//! Result: NULL columns -> SQL Server applies DEFAULT / IDENTITY / computed
//! values automatically.  Only columns present in the Arrow batch are sent.
//!
//! ## Performance
//!
//! Columnar array binding (`SQLSetStmtAttr(SQL_ATTR_PARAMSET_SIZE, N)`) sends
//! N rows in a single ODBC call.  The Microsoft ODBC Driver converts this to
//! an efficient TDS batch internally.  Typical throughput: 50k-200k rows/sec
//! (comparable to tiberius `bulk_insert`, well above parameterized INSERT).
//!
//! ## System requirements
//!
//! - **unixODBC** development package:
//!   - Debian/Ubuntu: `sudo apt install unixodbc-dev`
//!   - RHEL/Fedora: `sudo dnf install unixODBC-devel`
//!   - macOS: `brew install unixodbc`
//!
//! - **Microsoft ODBC Driver 17+ for SQL Server** (auto-detected; highest version preferred):
//!   <https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server>
//!
//! ## Feature flag
//!
//! `cargo build --features odbc`
//!
//! ## odbc-api version
//!
//! This module targets **odbc-api 21**.  Key API differences vs. v8:
//!
//! - `Environment::new()` is no longer `unsafe` (v21 removed the requirement).
//! - `BufferDesc` enum is unchanged.
//! - `AnySliceMut` is unchanged.
//! - `Prepared::into_column_inserter(...)` replaces `into_columnar_inserter`.
//! - `Connection::execute()` now takes a 3rd parameter `Option<usize>` (max rows).

use std::sync::{LazyLock, OnceLock};

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray,
    Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    Decimal128Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use odbc_api::{
    buffers::{BufferDesc, AnySliceMut},
    Connection, ConnectionOptions, Cursor, Environment,
};

use super::util::{MssqlConnParams, TableColumnInfo};

// ── SendWriter: module-level wrapper for thread safety ────────────────────────

/// Newtype wrapper to send an `OdbcWriter` pointer across thread boundaries
/// in `tokio::task::spawn_blocking` closures.
///
/// Uses `usize` internally instead of `*const OdbcWriter` because raw pointers
/// are `!Send` and Rust's auto-trait analysis sees through newtype wrappers
/// when checking closures captured by `spawn_blocking`.  `usize` is trivially
/// `Send`, so no `unsafe impl` is needed on the wrapper itself.
///
/// SAFETY contract: the caller must guarantee exclusive access to the
/// `OdbcWriter` (via `&mut self`) and must `.await` the `spawn_blocking`
/// handle immediately so the pointer is not used after the writer moves
/// or drops.
pub(crate) struct SendWriter(usize);

impl SendWriter {
    /// Wrap a reference to an `OdbcWriter` for cross-thread transfer.
    #[inline]
    pub(crate) fn new(writer: &OdbcWriter) -> Self {
        Self(writer as *const OdbcWriter as usize)
    }

    /// Recover the `&OdbcWriter` reference inside a `spawn_blocking` closure.
    ///
    /// # Safety
    ///
    /// The caller must ensure the original `OdbcWriter` is still alive and
    /// exclusively accessed (guaranteed by the `&mut self` + immediate `.await`
    /// pattern in `MssqlWriteDB`).
    #[inline]
    pub(crate) unsafe fn as_ref(&self) -> &OdbcWriter {
        unsafe { &*(self.0 as *const OdbcWriter) }
    }
}

// ── Global ODBC environment ───────────────────────────────────────────────────

/// Lazily-initialized global ODBC environment.
///
/// `odbc_api::Environment` is `Send + Sync` — safe to share across threads.
/// Created once on first use; lives for the program lifetime.
///
/// Panics on creation failure (missing unixODBC).  This is intentional:
/// if the user enables `odbc` without installing unixODBC, we want
/// a clear error at startup, not silent fallback.
static ODBC_ENV: LazyLock<Environment> = LazyLock::new(|| {
    // odbc-api v21: Environment::new() is no longer unsafe.
    // LazyLock guarantees single initialization (one Environment per process).
    Environment::new().expect(
        "ODBC: failed to create environment.\n\
         Is unixODBC installed?\n\
         - Debian/Ubuntu: sudo apt install unixodbc-dev\n\
         - RHEL/Fedora:   sudo dnf install unixODBC-devel\n\
         - macOS:         brew install unixodbc"
    )
});

// ── ODBC driver detection ─────────────────────────────────────────────────────

/// Detected Microsoft ODBC Driver for SQL Server.
///
/// Contains the full driver name (e.g. `"ODBC Driver 18 for SQL Server"`) and
/// the parsed major version number.
#[derive(Clone, Debug)]
struct DetectedDriver {
    /// Full driver name as registered in odbcinst.ini, e.g.
    /// `"ODBC Driver 18 for SQL Server"`.
    name: String,
    /// Major version number (e.g. 17, 18, 19).
    major_version: u32,
}

/// Cached driver detection result.  `None` = detection ran but found nothing.
static DETECTED_DRIVER: OnceLock<Option<DetectedDriver>> = OnceLock::new();

/// Detect the highest-version Microsoft ODBC Driver for SQL Server installed
/// on this system.
///
/// Uses `odbcinst -q -d` (part of unixODBC, which is already a requirement for
/// the `odbc` feature).  Output looks like:
///
/// ```text
/// [ODBC Driver 17 for SQL Server]
/// [ODBC Driver 18 for SQL Server]
/// [PostgreSQL ANSI]
/// ```
///
/// We parse for `[ODBC Driver {N} for SQL Server]`, extract `N`, and return
/// the entry with the highest major version.
///
/// Falls back to `None` if `odbcinst` is not found or no MS driver is installed.
fn detect_mssql_driver() -> &'static Option<DetectedDriver> {
    DETECTED_DRIVER.get_or_init(|| {
        let output = match std::process::Command::new("odbcinst")
            .args(["-q", "-d"])
            .output()
        {
            Ok(o) if o.status.success() => o.stdout,
            Ok(o) => {
                tracing::warn!(
                    status = ?o.status,
                    stderr = %String::from_utf8_lossy(&o.stderr),
                    "ODBC: `odbcinst -q -d` failed"
                );
                return None;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "ODBC: failed to run `odbcinst` — is unixODBC installed?"
                );
                return None;
            }
        };

        let stdout = String::from_utf8_lossy(&output);
        let mut best: Option<DetectedDriver> = None;

        for line in stdout.lines() {
            let trimmed = line.trim();
            // Expected format: [ODBC Driver 18 for SQL Server]
            if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
                continue;
            }
            let inner = &trimmed[1..trimmed.len() - 1]; // strip brackets
            // Match "ODBC Driver {N} for SQL Server"
            if let Some(rest) = inner.strip_prefix("ODBC Driver ") {
                if let Some(ver_str) = rest.strip_suffix(" for SQL Server") {
                    if let Ok(ver) = ver_str.trim().parse::<u32>() {
                        if best.as_ref().map_or(true, |b| ver > b.major_version) {
                            best = Some(DetectedDriver {
                                name: inner.to_string(),
                                major_version: ver,
                            });
                        }
                    }
                }
            }
        }

        if let Some(ref d) = best {
            tracing::info!(
                driver = %d.name,
                version = d.major_version,
                bulk_copy_api = d.major_version >= 17,
                "ODBC: detected Microsoft SQL Server driver"
            );
        } else {
            tracing::warn!(
                "ODBC: no Microsoft ODBC Driver for SQL Server found.\n\
                 Install: https://learn.microsoft.com/en-us/sql/connect/odbc/\
                 linux-mac/installing-the-microsoft-odbc-driver-for-sql-server"
            );
        }

        best
    })
}

// ── ODBC connection string builder ────────────────────────────────────────────

/// Build an ODBC connection string from `MssqlConnParams`.
///
/// Auto-detects the installed Microsoft ODBC Driver version via `odbcinst`.
/// Picks the highest available version (prefers 18 > 17 > 13).
///
/// When the detected driver is version **17 or higher**, appends
/// `UseBulkCopyForBatchInsert=yes;` — this tells the driver to internally
/// convert parameterized INSERT with array binding into the TDS BULK INSERT
/// protocol (same fast-path as tiberius/bcp), without KEEPNULLS.
///
/// Example output (driver 18):
/// ```text
/// Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=MyDB;
/// Uid=sa;Pwd=Password1!;TrustServerCertificate=yes;UseBulkCopyForBatchInsert=yes;
/// ```
pub(crate) fn build_connection_string(params: &MssqlConnParams) -> anyhow::Result<String> {
    let driver = detect_mssql_driver()
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!(
            "ODBC: no Microsoft ODBC Driver for SQL Server found.\n\
             Install the driver:\n\
             - Debian/Ubuntu: curl https://packages.microsoft.com/keys/microsoft.asc | \
               sudo tee /etc/apt/trusted.gpg.d/microsoft.asc && \
               sudo apt update && sudo apt install msodbcsql18\n\
             - RHEL/Fedora: sudo dnf install msodbcsql18\n\
             - macOS: brew install microsoft/mssql-release/msodbcsql18\n\
             Check: odbcinst -q -d"
        ))?;

    let mut s = format!(
        "Driver={{{}}};Server={},{};Database={};Uid={};Pwd={};",
        driver.name,
        params.host, params.port, params.database, params.user, params.pass,
    );
    if params.trust_cert {
        s.push_str("TrustServerCertificate=yes;");
    }
    if let Some(ref app_name) = params.application_name {
        s.push_str(&format!("APP={app_name};"));
    }

    // ── UseBulkCopyForBatchInsert (driver 17.3+) ─────────────────────────
    //
    // Tells the ODBC driver to internally convert parameterized INSERT with
    // SQL_ATTR_PARAMSET_SIZE > 1 into the TDS BULK INSERT protocol.
    //
    // Result: same throughput as tiberius bulk_insert / bcp, but WITHOUT
    // KEEPNULLS — SQL Server applies DEFAULT / IDENTITY / computed values.
    //
    // On drivers < 17.3 the attribute is silently ignored (no error),
    // so gating on major >= 17 is safe.
    if driver.major_version >= 17 {
        s.push_str("UseBulkCopyForBatchInsert=yes;");
        tracing::debug!(
            driver = %driver.name,
            "ODBC: UseBulkCopyForBatchInsert=yes enabled (driver >= 17)"
        );
    }

    Ok(s)
}

// ── OdbcWriter ────────────────────────────────────────────────────────────────

/// Self-contained ODBC writer for MSSQL bulk inserts.
///
/// Manages a single ODBC connection and provides:
/// - SQL execution (DDL, DML, transactions)
/// - Columnar bulk insert from Arrow RecordBatches
///
/// ## Lifetime
///
/// The `'static` lifetime comes from the global `ODBC_ENV` `LazyLock`.
/// The connection is `Send` (odbc-api 21) so it can be held across
/// `await` points in async code (wrapped in `spawn_blocking` for the
/// actual ODBC calls which are blocking).
pub(crate) struct OdbcWriter {
    conn: Connection<'static>,
}

impl OdbcWriter {
    /// Open an ODBC connection to SQL Server.
    ///
    /// Uses the global `ODBC_ENV` and the auto-detected Microsoft ODBC Driver.
    ///
    /// After connecting, executes one-time session-level `SET` options that
    /// remain active for the lifetime of the connection:
    ///
    /// | Setting              | Why                                                        |
    /// |----------------------|------------------------------------------------------------|
    /// | `NOCOUNT ON`         | Suppresses "N rows affected" messages -> less network chatter. |
    /// | `XACT_ABORT ON`      | Any runtime error automatically rolls back the entire transaction. |
    /// | `ARITHABORT ON`      | Required by SQL Server for sessions that touch indexed views, etc. |
    ///
    /// These are session-scoped — they persist until the connection is closed
    /// and do NOT need to be repeated per batch or per transaction.
    pub(crate) fn connect(params: &MssqlConnParams) -> anyhow::Result<Self> {
        let conn_str = build_connection_string(params)?;
        let driver = detect_mssql_driver().as_ref().unwrap(); // safe: build_connection_string succeeded
        tracing::info!(
            host   = %params.host,
            port   = params.port,
            db     = %params.database,
            driver = %driver.name,
            "ODBC: connecting via {}", driver.name
        );
        let conn = ODBC_ENV
            .connect_with_connection_string(&conn_str, ConnectionOptions::default())
            .map_err(|e| anyhow::anyhow!(
                "ODBC: connection failed: {e}\n\
                 Driver: {}\n\
                 Check: odbcinst -q -d",
                driver.name
            ))?;

        // ── Session-level SET options (once per connection) ──────────────
        conn.execute(
            "SET NOCOUNT ON; SET XACT_ABORT ON; SET ARITHABORT ON;",
            (),
            None,
        )
        .map_err(|e| anyhow::anyhow!(
            "ODBC: session SET options failed: {e}\n\
             SQL: SET NOCOUNT ON; SET XACT_ABORT ON; SET ARITHABORT ON;"
        ))?;
        tracing::debug!("ODBC: session options applied (NOCOUNT, XACT_ABORT, ARITHABORT)");

        Ok(Self { conn })
    }

    /// Execute a SQL statement (DDL, DML, or transaction control).
    ///
    /// Consumes and discards all result sets.
    pub(crate) fn execute_sql(&self, sql: &str) -> anyhow::Result<()> {
        self.conn
            .execute(sql, (), None)
            .map_err(|e| anyhow::anyhow!("ODBC execute failed: {e}\nSQL: {sql}"))?;
        Ok(())
    }

    /// `BEGIN TRANSACTION`.
    pub(crate) fn begin_transaction(&self) -> anyhow::Result<()> {
        self.execute_sql("BEGIN TRANSACTION")
    }

    /// `COMMIT TRANSACTION`.
    pub(crate) fn commit(&self) -> anyhow::Result<()> {
        self.execute_sql("COMMIT TRANSACTION")
    }

    /// `ROLLBACK TRANSACTION`.
    pub(crate) fn rollback(&self) -> anyhow::Result<()> {
        self.execute_sql("ROLLBACK TRANSACTION")
    }

    /// Introspect target table columns via ODBC.
    ///
    /// Equivalent to [`super::util::introspect_table_columns`] but runs over
    /// the existing ODBC connection — **no tiberius connection needed**.
    ///
    /// Returns `Ok(None)` when the table does not exist.
    pub(crate) fn introspect_table_columns(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> anyhow::Result<Option<Vec<TableColumnInfo>>> {
        // Use string-interpolated WHERE (safe — values come from config, not
        // user input).  ODBC parameter binding for VARCHAR works differently
        // per driver version, and the string-literal approach is simpler here.
        let safe_schema = schema_name.replace('\'', "''");
        let safe_table  = table_name.replace('\'', "''");

        let sql = format!(
            "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.COLUMN_DEFAULT, \
                    COLUMNPROPERTY( \
                        OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), \
                        c.COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY, \
                    COLUMNPROPERTY( \
                        OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), \
                        c.COLUMN_NAME, 'IsComputed') AS IS_COMPUTED \
               FROM INFORMATION_SCHEMA.COLUMNS c \
              WHERE c.TABLE_SCHEMA = '{safe_schema}' AND c.TABLE_NAME = '{safe_table}' \
              ORDER BY c.ORDINAL_POSITION"
        );

        let cursor = match self.conn.execute(&sql, (), None) {
            Ok(Some(c)) => c,
            Ok(None) => return Ok(None), // No result set (shouldn't happen for SELECT)
            Err(e) => return Err(anyhow::anyhow!(
                "ODBC: introspect query failed: {e}\nSQL: {sql}"
            )),
        };

        let mut cols: Vec<TableColumnInfo> = Vec::new();

        // Iterate rows using the Cursor trait's next_row() method.
        let mut row_cursor = cursor;
        while let Some(mut row) = row_cursor.next_row()
            .map_err(|e| anyhow::anyhow!("ODBC: introspect fetch failed: {e}"))?
        {
            let mut name_buf    = Vec::new();
            let mut dtype_buf   = Vec::new();
            let mut nullable_buf = Vec::new();
            let mut default_buf = Vec::new();
            let mut ident_buf   = Vec::new();
            let mut computed_buf = Vec::new();

            // ODBC columns are 1-based.
            row.get_text(1, &mut name_buf).map_err(|e| anyhow::anyhow!("ODBC introspect col 1: {e}"))?;
            row.get_text(2, &mut dtype_buf).map_err(|e| anyhow::anyhow!("ODBC introspect col 2: {e}"))?;
            row.get_text(3, &mut nullable_buf).map_err(|e| anyhow::anyhow!("ODBC introspect col 3: {e}"))?;
            let has_default_val = row.get_text(4, &mut default_buf)
                .map_err(|e| anyhow::anyhow!("ODBC introspect col 4: {e}"))?;
            row.get_text(5, &mut ident_buf).map_err(|e| anyhow::anyhow!("ODBC introspect col 5: {e}"))?;
            row.get_text(6, &mut computed_buf).map_err(|e| anyhow::anyhow!("ODBC introspect col 6: {e}"))?;

            let name     = String::from_utf8_lossy(&name_buf).to_string();
            let dtype    = String::from_utf8_lossy(&dtype_buf).to_ascii_lowercase();
            let nullable = String::from_utf8_lossy(&nullable_buf)
                .eq_ignore_ascii_case("YES");
            let has_default = has_default_val; // true if COLUMN_DEFAULT was non-NULL
            let is_identity = String::from_utf8_lossy(&ident_buf)
                .trim()
                .parse::<i32>()
                .unwrap_or(0)
                == 1;
            let is_computed = String::from_utf8_lossy(&computed_buf)
                .trim()
                .parse::<i32>()
                .unwrap_or(0)
                == 1;

            cols.push(TableColumnInfo {
                name,
                data_type: dtype,
                nullable,
                has_default: has_default || is_identity || is_computed,
            });
        }

        if cols.is_empty() {
            Ok(None)
        } else {
            tracing::debug!(
                table = %table_name,
                columns = cols.len(),
                "ODBC: introspected {} columns from INFORMATION_SCHEMA", cols.len()
            );
            Ok(Some(cols))
        }
    }

    /// Bulk insert an Arrow `RecordBatch` into `table`.
    ///
    /// Only the columns present in `batch.schema()` are sent.  Missing
    /// target columns get their SQL Server DEFAULT / IDENTITY values
    /// because **KEEPNULLS is not set**.
    ///
    /// Uses ODBC columnar array parameter binding for maximum throughput.
    ///
    /// ## Arguments
    ///
    /// - `table` — fully-qualified table name, e.g. `"[dbo].[orders]"`
    /// - `batch` — the Arrow RecordBatch to insert
    /// - `batch_size` — max rows per ODBC execute call (default: 10000)
    ///
    /// ## Returns
    ///
    /// Number of rows inserted.
    pub(crate) fn bulk_insert_batch(
        &self,
        table: &str,
        batch: &RecordBatch,
        batch_size: usize,
    ) -> anyhow::Result<usize> {
        let schema = batch.schema();
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(0);
        }

        // ── Build INSERT SQL with column list ────────────────────────────────
        let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let col_list = col_names
            .iter()
            .map(|c| format!("[{c}]"))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = col_names.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!("INSERT INTO {table} ({col_list}) VALUES ({placeholders})");

        // ── Buffer descriptions (odbc-api 21: BufferDesc) ────────────────────
        let descs: Vec<BufferDesc> = schema
            .fields()
            .iter()
            .map(|f| arrow_to_buffer_desc(f.data_type()))
            .collect();

        // ── Prepare statement and create columnar inserter ───────────────────
        let prepared = self.conn.prepare(&sql).map_err(|e| {
            anyhow::anyhow!("ODBC: prepare failed: {e}\nSQL: {sql}")
        })?;

        let chunk_size = batch_size.min(num_rows);
        let mut inserter = prepared
            .into_column_inserter(chunk_size, descs.iter().copied())
            .map_err(|e| anyhow::anyhow!("ODBC: columnar inserter creation failed: {e}"))?;

        // ── Insert in chunks ─────────────────────────────────────────────────
        let mut total_inserted = 0usize;
        let mut offset = 0usize;

        while offset < num_rows {
            let end = (offset + chunk_size).min(num_rows);
            let chunk_rows = end - offset;

            inserter.set_num_rows(chunk_rows);

            // Fill each column buffer from the Arrow arrays.
            for col_idx in 0..schema.fields().len() {
                let col = batch.column(col_idx);
                let dt = schema.field(col_idx).data_type();
                let slice = inserter.column_mut(col_idx);
                fill_column_buffer(slice, col.as_ref(), dt, offset, chunk_rows)?;
            }

            inserter.execute().map_err(|e| {
                anyhow::anyhow!(
                    "ODBC: bulk insert execute failed at offset {offset}: {e}\n\
                     Table: {table}, chunk_rows: {chunk_rows}"
                )
            })?;

            total_inserted += chunk_rows;
            offset = end;
        }

        Ok(total_inserted)
    }
}

// ── Arrow -> ODBC buffer description ──────────────────────────────────────────

/// Map an Arrow `DataType` to an ODBC `BufferDesc`.
///
/// Strategy:
/// - Integer and float types use native ODBC buffers (binary transfer).
/// - Strings, temporal types, decimals, and binary use text or binary buffers.
/// - All buffers are nullable (we never know if a column has NULLs until
///   we see the data).
///
/// ## Text encoding for temporal types
///
/// Temporal types (Timestamp, Date, Time) are sent as ISO 8601 formatted
/// strings.  SQL Server parses these efficiently via implicit conversion.
/// This avoids ODBC driver-specific quirks with C_TYPE_TIMESTAMP etc.
fn arrow_to_buffer_desc(dt: &DataType) -> BufferDesc {
    match dt {
        // ── Integers (native binary) ─────────────────────────────────────
        DataType::Boolean => BufferDesc::I8 { nullable: true },
        // Int8 -> I16 (SMALLINT) because TINYINT is unsigned in SQL Server
        DataType::Int8 => BufferDesc::I16 { nullable: true },
        DataType::Int16 | DataType::UInt8 => BufferDesc::I16 { nullable: true },
        DataType::Int32 | DataType::UInt16 => BufferDesc::I32 { nullable: true },
        DataType::Int64 | DataType::UInt32 => BufferDesc::I64 { nullable: true },
        // UInt64 -> text (DECIMAL(20,0) exceeds i64 range)
        DataType::UInt64 => BufferDesc::Text { max_str_len: 20 },

        // ── Floats (native binary) ──────────────────────────────────────
        DataType::Float32 => BufferDesc::F32 { nullable: true },
        DataType::Float64 => BufferDesc::F64 { nullable: true },

        // ── Temporal (ISO 8601 text) ─────────────────────────────────────
        // "2024-01-15 13:45:30.123456" = 26 chars
        DataType::Timestamp(TimeUnit::Microsecond, None) => BufferDesc::Text { max_str_len: 27 },
        // "2024-01-15 13:45:30.123456 +00:00" = 33 chars
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => BufferDesc::Text { max_str_len: 34 },
        // "2024-01-15" = 10 chars
        DataType::Date32 => BufferDesc::Text { max_str_len: 10 },
        // "13:45:30.123456" = 15 chars
        DataType::Time64(TimeUnit::Microsecond) => BufferDesc::Text { max_str_len: 16 },

        // ── Decimal (text) ───────────────────────────────────────────────
        DataType::Decimal128(_, _) => BufferDesc::Text { max_str_len: 40 },

        // ── Binary ───────────────────────────────────────────────────────
        DataType::LargeBinary | DataType::Binary => BufferDesc::Binary { length: 8000 },
        DataType::FixedSizeBinary(n) => BufferDesc::Binary { length: *n as usize },

        // ── String types + fallback ──────────────────────────────────────
        // max_str_len = 4000 covers most real-world text.  NVARCHAR(MAX)
        // columns accept any length — ODBC truncates silently if exceeded,
        // but 4000 UTF-8 bytes is generous for non-LOB data.
        _ => BufferDesc::Text { max_str_len: 4000 },
    }
}

// ── Arrow -> ODBC column buffer filling ───────────────────────────────────────

/// Fill an ODBC column buffer from an Arrow array.
///
/// Handles all Arrow types supported by the MSSQL sink.
/// `offset` and `len` define the row slice within the Arrow array.
///
/// odbc-api v21: `NullableSliceMut::set_cell(index, Option<T>)` takes values
/// **by value** (not by reference).  Text and Binary slices still take
/// `Option<&[u8]>` by reference.
fn fill_column_buffer(
    slice: AnySliceMut<'_>,
    col: &dyn Array,
    dt: &DataType,
    offset: usize,
    len: usize,
) -> anyhow::Result<()> {
    match (dt, slice) {
        // ── Boolean -> NullableI8 ─────────────────────────────────────────
        (DataType::Boolean, AnySliceMut::NullableI8(mut buf)) => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row) as i8));
                }
            }
        }

        // ── Int8 -> NullableI16 (SMALLINT, signed) ───────────────────────
        (DataType::Int8, AnySliceMut::NullableI16(mut buf)) => {
            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row) as i16));
                }
            }
        }

        // ── Int16 / UInt8 -> NullableI16 ─────────────────────────────────
        (DataType::Int16, AnySliceMut::NullableI16(mut buf)) => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row)));
                }
            }
        }
        (DataType::UInt8, AnySliceMut::NullableI16(mut buf)) => {
            let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row) as i16));
                }
            }
        }

        // ── Int32 / UInt16 -> NullableI32 ────────────────────────────────
        (DataType::Int32, AnySliceMut::NullableI32(mut buf)) => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row)));
                }
            }
        }
        (DataType::UInt16, AnySliceMut::NullableI32(mut buf)) => {
            let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row) as i32));
                }
            }
        }

        // ── Int64 / UInt32 -> NullableI64 ────────────────────────────────
        (DataType::Int64, AnySliceMut::NullableI64(mut buf)) => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row)));
                }
            }
        }
        (DataType::UInt32, AnySliceMut::NullableI64(mut buf)) => {
            let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row) as i64));
                }
            }
        }

        // ── Float32 -> NullableF32 ───────────────────────────────────────
        (DataType::Float32, AnySliceMut::NullableF32(mut buf)) => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row)));
                }
            }
        }

        // ── Float64 -> NullableF64 ───────────────────────────────────────
        (DataType::Float64, AnySliceMut::NullableF64(mut buf)) => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row)));
                }
            }
        }

        // ── UInt64 -> Text (DECIMAL(20,0)) ────────────────────────────────
        (DataType::UInt64, AnySliceMut::Text(mut buf)) => {
            let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    let s = arr.value(row).to_string();
                    buf.set_cell(i, Some(s.as_bytes()));
                }
            }
        }

        // ── Timestamp(us, None) -> Text "YYYY-MM-DD HH:MM:SS.ffffff" ─────
        (DataType::Timestamp(TimeUnit::Microsecond, None), AnySliceMut::Text(mut buf)) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    let s = format_timestamp_naive(arr.value(row));
                    buf.set_cell(i, Some(s.as_bytes()));
                }
            }
        }

        // ── Timestamp(us, Some(tz)) -> Text "...+00:00" ──────────────────
        (DataType::Timestamp(TimeUnit::Microsecond, Some(_)), AnySliceMut::Text(mut buf)) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    let s = format_timestamp_tz(arr.value(row));
                    buf.set_cell(i, Some(s.as_bytes()));
                }
            }
        }

        // ── Date32 -> Text "YYYY-MM-DD" ──────────────────────────────────
        (DataType::Date32, AnySliceMut::Text(mut buf)) => {
            let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    let s = format_date32(arr.value(row));
                    buf.set_cell(i, Some(s.as_bytes()));
                }
            }
        }

        // ── Time64(us) -> Text "HH:MM:SS.ffffff" ─────────────────────────
        (DataType::Time64(TimeUnit::Microsecond), AnySliceMut::Text(mut buf)) => {
            let arr = col
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    let s = format_time64(arr.value(row));
                    buf.set_cell(i, Some(s.as_bytes()));
                }
            }
        }

        // ── Decimal128(p, s) -> Text ──────────────────────────────────────
        (DataType::Decimal128(_, scale), AnySliceMut::Text(mut buf)) => {
            let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let s = *scale as u32;
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    let txt = format_decimal128(arr.value(row), s);
                    buf.set_cell(i, Some(txt.as_bytes()));
                }
            }
        }

        // ── Binary types -> Binary buffer ─────────────────────────────────
        (DataType::LargeBinary, AnySliceMut::Binary(mut buf)) => {
            let arr = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row)));
                }
            }
        }
        (DataType::Binary, AnySliceMut::Binary(mut buf)) => {
            let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row)));
                }
            }
        }
        (DataType::FixedSizeBinary(_), AnySliceMut::Binary(mut buf)) => {
            let arr = col
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row)));
                }
            }
        }

        // ── Utf8 -> Text ─────────────────────────────────────────────────
        (DataType::Utf8, AnySliceMut::Text(mut buf)) => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row).as_bytes()));
                }
            }
        }
        (DataType::LargeUtf8, AnySliceMut::Text(mut buf)) => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            for i in 0..len {
                let row = offset + i;
                if arr.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    buf.set_cell(i, Some(arr.value(row).as_bytes()));
                }
            }
        }

        // ── Fallback: stringify -> Text ───────────────────────────────────
        (_, AnySliceMut::Text(mut buf)) => {
            for i in 0..len {
                let row = offset + i;
                if col.is_null(row) {
                    buf.set_cell(i, None);
                } else {
                    match arrow::util::display::array_value_to_string(col, row) {
                        Ok(s) => buf.set_cell(i, Some(s.as_bytes())),
                        Err(_) => buf.set_cell(i, None),
                    }
                }
            }
        }

        // ── Unexpected buffer type mismatch ──────────────────────────────
        (dt, _) => {
            anyhow::bail!(
                "ODBC: buffer type mismatch for Arrow type {dt:?}. \
                 This is a bug in arrow_to_buffer_desc."
            );
        }
    }

    Ok(())
}

// ── Temporal formatting helpers ──────────────────────────────────────────────
//
// SQL Server parses ISO 8601 strings efficiently.  These formatters produce
// the exact string format SQL Server expects for implicit conversion.

/// Microseconds since Unix epoch -> `"YYYY-MM-DD HH:MM:SS.ffffff"`.
fn format_timestamp_naive(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, frac * 1000)
        .unwrap_or_default()
        .naive_utc();
    dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

/// Microseconds since Unix epoch -> `"YYYY-MM-DD HH:MM:SS.ffffff +00:00"`.
fn format_timestamp_tz(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, frac * 1000)
        .unwrap_or_default();
    dt.format("%Y-%m-%d %H:%M:%S%.6f %:z").to_string()
}

/// Days since Unix epoch -> `"YYYY-MM-DD"`.
fn format_date32(days: i32) -> String {
    let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
        .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    date.format("%Y-%m-%d").to_string()
}

/// Microseconds since midnight -> `"HH:MM:SS.ffffff"`.
fn format_time64(us: i64) -> String {
    let h = us / 3_600_000_000;
    let m = (us % 3_600_000_000) / 60_000_000;
    let s = (us % 60_000_000) / 1_000_000;
    let f = us % 1_000_000;
    format!("{h:02}:{m:02}:{s:02}.{f:06}")
}

/// `Decimal128` value with `scale` -> decimal string like `"-1234.5678"`.
fn format_decimal128(value: i128, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let divisor = 10i128.pow(scale);
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.unsigned_abs();
    let int_part = abs / divisor as u128;
    let frac_part = abs % divisor as u128;
    format!("{sign}{int_part}.{frac_part:0>width$}", width = scale as usize)
}

// ── Staging SQL generators for ODBC path ─────────────────────────────────────
//
// When the ODBC path needs staging (MERGE strategies), it creates a temp table,
// bulk-inserts via ODBC, then runs MERGE SQL over the same ODBC connection.

/// DDL for the staging table (`#etl_odbc_stage`).
///
/// Uses the same type mapping as `staging_sql_type` in the tiberius path,
/// but with a different table name to avoid confusion.
pub(crate) fn odbc_staging_ddl(schema: &SchemaRef) -> String {
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let sql_type = odbc_staging_sql_type(f.data_type());
            format!("    [{}] {} NULL", f.name(), sql_type)
        })
        .collect();
    format!("CREATE TABLE #etl_odbc_stage (\n{}\n)", cols.join(",\n"))
}

/// SQL type for ODBC staging columns — matches what the ODBC columnar
/// inserter sends.
fn odbc_staging_sql_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BIT",
        DataType::Int8 => "SMALLINT",
        DataType::Int16 | DataType::UInt8 => "SMALLINT",
        DataType::Int32 | DataType::UInt16 => "INT",
        DataType::Int64 | DataType::UInt32 => "BIGINT",
        DataType::UInt64 => "DECIMAL(20,0)",
        DataType::Float32 => "REAL",
        DataType::Float64 => "FLOAT",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "DATETIME2(6)",
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "DATETIMEOFFSET(6)",
        DataType::Date32 => "DATE",
        DataType::Time64(_) => "TIME(7)",
        DataType::Decimal128(_p, _s) => {
            // Can't return a dynamically-formatted string as &'static str.
            // DECIMAL(38,18) covers all Decimal128 values.
            "DECIMAL(38,18)"
        }
        DataType::LargeBinary | DataType::Binary | DataType::FixedSizeBinary(_) => {
            "VARBINARY(MAX)"
        }
        _ => "NVARCHAR(MAX)",
    }
}

/// `INSERT ... SELECT` from `#etl_odbc_stage` -> target.
pub(crate) fn odbc_insert_select_sql(full_table: &str, col_names: &[String]) -> String {
    let cols = col_names
        .iter()
        .map(|c| format!("[{c}]"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {full_table} WITH (TABLOCK) ({cols}) \
         SELECT {cols} FROM #etl_odbc_stage"
    )
}

/// MERGE from `#etl_odbc_stage` -> target.
pub(crate) fn odbc_merge_sql(
    full_table: &str,
    col_names: &[String],
    pk_cols: &[String],
    with_delete: bool,
    insert_only: bool,
) -> String {
    let cols_sql = col_names
        .iter()
        .map(|c| format!("[{c}]"))
        .collect::<Vec<_>>()
        .join(", ");
    let on_clause = pk_cols
        .iter()
        .map(|k| format!("[T].[{k}] = [S].[{k}]"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let insert_vals = col_names
        .iter()
        .map(|c| format!("[S].[{c}]"))
        .collect::<Vec<_>>()
        .join(", ");

    let update_clause = if insert_only {
        String::new()
    } else {
        let update_set = col_names
            .iter()
            .filter(|c| !pk_cols.contains(c))
            .map(|c| format!("[T].[{c}] = [S].[{c}]"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" WHEN MATCHED THEN UPDATE SET {update_set}")
    };

    let del = if with_delete {
        "\n         WHEN NOT MATCHED BY SOURCE THEN DELETE"
    } else {
        ""
    };

    format!(
        "MERGE INTO {full_table} WITH (HOLDLOCK) AS [T] \
         USING #etl_odbc_stage AS [S] ON {on_clause}{update_clause} \
         WHEN NOT MATCHED BY TARGET THEN INSERT ({cols_sql}) VALUES ({insert_vals}){del};"
    )
}

/// Clustered PK index on `#etl_odbc_stage`.
pub(crate) fn odbc_staging_pk_index_sql(pk_cols: &[String]) -> String {
    let cols = pk_cols
        .iter()
        .map(|c| format!("[{c}] ASC"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE CLUSTERED INDEX [IX_etl_odbc_pk] ON #etl_odbc_stage ({cols})")
}
