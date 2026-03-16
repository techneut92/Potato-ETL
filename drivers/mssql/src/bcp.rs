//! bcp-based bulk loader for SQL Server — **native format file mode**.
//!
//! Spawns the `bcp` CLI tool (from `mssql-tools18`) as a child process and
//! feeds it a binary data file described by a non-XML BCP format file.
//! Numeric types (ints, floats, booleans) are sent as raw native bytes;
//! text-based types (strings, dates, timestamps, decimals) are sent as
//! length-prefixed SQLCHAR.
//!
//! ## Per-batch architecture
//!
//! Each `bcp` invocation handles one buffered batch of rows.  Peak disk
//! usage is proportional to `batch_threshold`, not total pipeline rows.
//!
//! ```text
//! write_batch()  ->  buffer rows (native binary) in memory
//!     |  when buffer >= batch_threshold
//! flush_buffer() ->  write temp file -> spawn bcp -> wait -> clear buffer
//!     |  bcp loads into target table (or staging table)
//! SQL Server
//! ```
//!
//! For staging paths (MERGE/Upsert), each invocation appends rows to the
//! staging table.  The final reconciliation happens in the sink's `flush()`.
//!
//! ## Architecture
//!
//! ```text
//! Rust (RecordBatch -> native binary bytes via format file spec)
//!   |  each batch written to temp file, bcp spawned immediately
//! bcp subprocess (reads temp file + format file)
//!   |  SQL Server native bulk-copy stream
//! target table (direct)  -or-  staging table -> INSERT...SELECT / MERGE -> target
//! ```
//!
//! The staging table uses **typed columns** (matching the Arrow schema) so
//! native binary data loads without conversion.
//!
//! ## NULL handling
//!
//! NULLs are indicated by the length prefix:
//! - 1-byte prefix: `0x00` = NULL
//! - 4-byte prefix: `0xFFFFFFFF` = NULL
//!
//! ## Temp directory
//!
//! Defaults to `$POTATO_BCP_TMPDIR`, falling back to `$TMPDIR` / system.
//! Peak disk = one batch of data; tmpfs is usually fine.
//!
//! ## Performance Tuning
//!
//! Configure via connection string parameters:
//!
//! | Parameter          | BCP Flag | Range         | Default | Description |
//! |--------------------|----------|---------------|---------|-------------|
//! | `batch_size`       | `-b`     | 1-1000000     | 10000   | Rows per BCP invocation |
//! | `bcp_packet_size`  | `-a`     | 4096-65535    | 4096    | Network packet size (bytes) |
//! | `bcp_max_errors`   | `-m`     | 0-unlimited   | 1       | Max errors before abort (0 = unlimited) |
//!
//! Example:
//! ```text
//! mssql://sa:Pass@localhost/DB?mode=bcp&batch_size=50000&bcp_packet_size=32768&bcp_max_errors=10
//! ```
//!
//! ## Requirements
//!
//! * `mssql-tools18` on Linux (`sudo apt install mssql-tools18`).
//! * When `trust_cert` is `true`, the `-u` flag is passed to bcp.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array,
    Decimal128Array,
    FixedSizeBinaryArray, BinaryArray, LargeBinaryArray,
    Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use tokio::io::AsyncReadExt;  // for stdout/stderr drain in flush_buffer()
use tokio::process::Command;

use super::util::MssqlConnParams;

// ── Temp directory selection ──────────────────────────────────────────────────

/// Choose the base temp directory for bcp data files.
///
/// Priority: `$POTATO_BCP_TMPDIR` > `$TMPDIR` > system default.
/// With per-batch sending, peak disk usage is proportional to batch_threshold
/// (not total pipeline rows), so tmpfs is usually fine.
fn bcp_temp_base() -> PathBuf {
    if let Ok(dir) = std::env::var("POTATO_BCP_TMPDIR") {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    std::env::temp_dir()
}

// ── Binary discovery ──────────────────────────────────────────────────────────

/// Find the `bcp` binary.  Returns the first usable path found, or `None`.
///
/// Search order:
/// 1. `/opt/mssql-tools18/bin/bcp`  -- default install location for mssql-tools18
/// 2. `/opt/mssql-tools/bin/bcp`    -- older mssql-tools package
/// 3. `/usr/local/bin/bcp`          -- manual/custom install
/// 4. `/usr/bin/bcp`                -- system package manager install
/// 5. `bcp` on `$PATH`              -- resolved via `command -v`
pub(crate) fn discover_bcp() -> Option<String> {
    // Fixed paths first -- fastest check.
    for candidate in [
        "/opt/mssql-tools18/bin/bcp",
        "/opt/mssql-tools/bin/bcp",
        "/usr/local/bin/bcp",
        "/usr/bin/bcp",
    ] {
        if std::path::Path::new(candidate).exists() {
            return Some(candidate.to_string());
        }
    }
    // Fallback: ask the shell.
    let output = std::process::Command::new("sh")
        .args(["-c", "command -v bcp"])
        .output()
        .ok()?;
    if output.status.success() {
        let path = String::from_utf8(output.stdout).ok()?;
        let path = path.trim().to_string();
        if !path.is_empty() {
            return Some(path);
        }
    }
    None
}

// ── Staging table name ────────────────────────────────────────────────────────

/// Generate a unique bcp staging table name.
///
/// Returns `(bare_name, full_name)` where:
/// - `bare_name` is used in the three-part `database.schema.bare_name` bcp argument
/// - `full_name` is `[schema].[bare_name]` for tiberius DDL and the final reconciliation SQL
///
/// The name is guaranteed to be a valid SQL Server identifier (no brackets, no
/// dots, no spaces) and unique per invocation so concurrent pipeline runs on the
/// same table do not collide.
pub(crate) fn staging_table_names(schema_name: &str, target_table: &str) -> (String, String) {
    let uuid_part = &uuid::Uuid::new_v4().simple().to_string()[..16];
    // Keep only alphanumeric + underscore from the target table name.
    let safe: String = target_table
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .take(40)
        .collect();
    let bare = format!("_etl_bcp_{safe}_{uuid_part}");
    let full = format!("[{schema_name}].[{bare}]");
    (bare, full)
}

// ── DDL for bcp staging table ─────────────────────────────────────────────────

/// Generate `CREATE TABLE` DDL for the bcp staging table.
///
/// Columns use **typed** SQL Server types matching the Arrow schema so that
/// native-mode bcp can load binary data directly without conversion.
pub(crate) fn staging_ddl(staging_full: &str, schema: &SchemaRef) -> String {
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("    [{}] {} NULL", f.name(), arrow_to_sql_type(f.data_type())))
        .collect();
    format!("CREATE TABLE {staging_full} (\n{}\n)", cols.join(",\n"))
}

/// Map Arrow DataType to a SQL Server column type for the staging table.
fn arrow_to_sql_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean                         => "BIT".into(),
        DataType::Int8                            => "SMALLINT".into(),  // TINYINT is unsigned in SQL Server
        DataType::UInt8                           => "TINYINT".into(),
        DataType::Int16                           => "SMALLINT".into(),
        DataType::UInt16                          => "INT".into(),       // widen: UInt16 max 65535 > SMALLINT max 32767
        DataType::Int32                           => "INT".into(),
        DataType::UInt32                          => "BIGINT".into(),    // widen: UInt32 max ~4.3B > INT max ~2.1B
        DataType::Int64                           => "BIGINT".into(),
        DataType::UInt64                          => "DECIMAL(20, 0)".into(), // UInt64 max > BIGINT max
        DataType::Float32                         => "REAL".into(),
        DataType::Float64                         => "FLOAT".into(),
        DataType::Utf8 | DataType::LargeUtf8      => "NVARCHAR(MAX)".into(),
        DataType::Binary | DataType::LargeBinary   => "VARBINARY(MAX)".into(),
        DataType::FixedSizeBinary(n)              => format!("VARBINARY({n})"),
        DataType::Date32                          => "DATE".into(),
        DataType::Time64(TimeUnit::Microsecond)   => "TIME(6)".into(),
        DataType::Timestamp(TimeUnit::Microsecond, None)    => "DATETIME2(6)".into(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "DATETIMEOFFSET(6)".into(),
        DataType::Decimal128(p, s)                => format!("DECIMAL({p}, {s})"),
        _                                         => "NVARCHAR(MAX)".into(),
    }
}

// ── SQL for final reconciliation ──────────────────────────────────────────────

/// Build the `SELECT` projection used in the final `INSERT ... SELECT` / `MERGE`.
fn select_list(schema: &SchemaRef) -> String {
    schema
        .fields()
        .iter()
        .map(|f| {
            let n = f.name();
            format!("[{n}]")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn col_list(schema: &SchemaRef) -> String {
    schema
        .fields()
        .iter()
        .map(|f| format!("[{}]", f.name()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `INSERT ... SELECT` from the bcp staging table into the target.
pub(crate) fn insert_select_sql(
    staging_full: &str,
    target_full:  &str,
    schema:       &SchemaRef,
) -> String {
    let cols = col_list(schema);
    let sel  = select_list(schema);
    format!(
        "INSERT INTO {target_full} WITH (TABLOCK) ({cols})\n\
         SELECT {sel}\n\
         FROM   {staging_full}"
    )
}

/// `MERGE` from the bcp staging table into the target.
///
/// `insert_only = true` -> insert-ignore semantics (no UPDATE on match).
/// `with_delete = true` -> delete rows in target that are absent from staging.
pub(crate) fn merge_sql(
    staging_full: &str,
    target_full:  &str,
    schema:       &SchemaRef,
    pk_cols:      &[String],
    with_delete:  bool,
    insert_only:  bool,
) -> String {
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let sel       = select_list(schema);
    let on_clause = pk_cols
        .iter()
        .map(|k| format!("[T].[{k}] = [S].[{k}]"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let update_set = col_names
        .iter()
        .filter(|c| !pk_cols.contains(c))
        .map(|c| format!("[T].[{c}] = [S].[{c}]"))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_cols = col_names.iter().map(|c| format!("[{c}]")).collect::<Vec<_>>().join(", ");
    let insert_vals = col_names.iter().map(|c| format!("[S].[{c}]")).collect::<Vec<_>>().join(", ");
    let del = if with_delete {
        "\n         WHEN NOT MATCHED BY SOURCE THEN DELETE"
    } else {
        ""
    };

    if insert_only {
        format!(
            "MERGE INTO {target_full} WITH (HOLDLOCK) AS [T]\n\
             USING (\n    SELECT {sel}\n    FROM   {staging_full}\n) AS [S]\n\
             ON    {on_clause}\n\
             WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
        )
    } else {
        format!(
            "MERGE INTO {target_full} WITH (HOLDLOCK) AS [T]\n\
             USING (\n    SELECT {sel}\n    FROM   {staging_full}\n) AS [S]\n\
             ON    {on_clause}\n\
             WHEN MATCHED            THEN UPDATE SET {update_set}\n\
             WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_cols}) VALUES ({insert_vals}){del};"
        )
    }
}

// ── BCP format file specification ─────────────────────────────────────────────

/// Describes how a single column is encoded in the bcp data file.
struct BcpFieldSpec {
    /// BCP host data type name (e.g. "SQLINT", "SQLFLT8", "SQLCHAR").
    host_type: &'static str,
    /// Length of the prefix that precedes each field value.
    /// - 1: for fixed-size native types (0x00 = NULL, N = data length)
    /// - 4: for variable-length types (0xFFFFFFFF = NULL, otherwise = byte count)
    prefix_len: u8,
    /// Maximum data length in the host file.  For fixed types, this is the
    /// exact size.  For variable types with a prefix, set to a safe upper
    /// bound (bcp uses the per-row prefix to determine actual length).
    host_data_len: u32,
    /// SQL Server collation for character types, or "" for non-character.
    collation: &'static str,
}

/// Map Arrow DataType to BCP format file field specification.
fn arrow_to_bcp_spec(dt: &DataType) -> BcpFieldSpec {
    match dt {
        DataType::Boolean => BcpFieldSpec {
            host_type: "SQLBIT", prefix_len: 1, host_data_len: 1, collation: "",
        },
        DataType::Int8 => BcpFieldSpec {
            host_type: "SQLSMALLINT", prefix_len: 1, host_data_len: 2, collation: "",
        },
        DataType::UInt8 => BcpFieldSpec {
            host_type: "SQLTINYINT", prefix_len: 1, host_data_len: 1, collation: "",
        },
        DataType::Int16 => BcpFieldSpec {
            host_type: "SQLSMALLINT", prefix_len: 1, host_data_len: 2, collation: "",
        },
        DataType::UInt16 => BcpFieldSpec {
            host_type: "SQLINT", prefix_len: 1, host_data_len: 4, collation: "",
        },
        DataType::Int32 => BcpFieldSpec {
            host_type: "SQLINT", prefix_len: 1, host_data_len: 4, collation: "",
        },
        DataType::UInt32 => BcpFieldSpec {
            host_type: "SQLBIGINT", prefix_len: 1, host_data_len: 8, collation: "",
        },
        DataType::Int64 => BcpFieldSpec {
            host_type: "SQLBIGINT", prefix_len: 1, host_data_len: 8, collation: "",
        },
        DataType::UInt64 => BcpFieldSpec {
            host_type: "SQLCHAR", prefix_len: 4, host_data_len: 20, collation: "",
        },
        DataType::Float32 => BcpFieldSpec {
            host_type: "SQLFLT4", prefix_len: 1, host_data_len: 4, collation: "",
        },
        DataType::Float64 => BcpFieldSpec {
            host_type: "SQLFLT8", prefix_len: 1, host_data_len: 8, collation: "",
        },
        DataType::Utf8 | DataType::LargeUtf8 => BcpFieldSpec {
            host_type: "SQLCHAR", prefix_len: 4, host_data_len: 0, collation: "",
        },
        DataType::Date32 => BcpFieldSpec {
            host_type: "SQLCHAR", prefix_len: 4, host_data_len: 10, collation: "",
        },
        DataType::Time64(TimeUnit::Microsecond) => BcpFieldSpec {
            host_type: "SQLCHAR", prefix_len: 4, host_data_len: 15, collation: "",
        },
        // Naive: "YYYY-MM-DD HH:MM:SS.ffffff" = 26 chars
        DataType::Timestamp(TimeUnit::Microsecond, None) => BcpFieldSpec {
            host_type: "SQLCHAR", prefix_len: 4, host_data_len: 27, collation: "",
        },
        // TZ-aware: "YYYY-MM-DD HH:MM:SS.ffffff +00:00" = 33 chars
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => BcpFieldSpec {
            host_type: "SQLCHAR", prefix_len: 4, host_data_len: 34, collation: "",
        },
        DataType::Decimal128(_, _) => BcpFieldSpec {
            host_type: "SQLCHAR", prefix_len: 4, host_data_len: 42, collation: "",
        },
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => BcpFieldSpec {
            host_type: "SQLBINARY", prefix_len: 4, host_data_len: 0, collation: "",
        },
        _ => BcpFieldSpec {
            host_type: "SQLCHAR", prefix_len: 4, host_data_len: 0, collation: "",
        },
    }
}

// ── BCP format file generation ────────────────────────────────────────────────

/// Generate a non-XML BCP format file for the given Arrow schema.
///
/// Format version 14.0 (SQL Server 2017+).
///
/// ## Column mapping
///
/// If `target_column_indices` is provided, it must be the same length as
/// `schema.fields()`.  Each value is the **1-based column index** in the
/// target table.  This allows sending a **subset** of columns — columns not
/// in the map will be filled by SQL Server defaults/identity/computed values.
///
/// If `None`, assumes the Arrow schema matches the target table 1:1 in order.
pub(crate) fn generate_format_file(
    schema: &SchemaRef,
    target_column_indices: Option<&[usize]>,
) -> String {
    let nc = schema.fields().len();
    let mut lines = Vec::with_capacity(nc + 2);
    lines.push("14.0".to_string());
    lines.push(nc.to_string());

    for (i, field) in schema.fields().iter().enumerate() {
        let spec = arrow_to_bcp_spec(field.data_type());
        let file_order = i + 1;  // 1-based position in the data file
        let table_col = if let Some(indices) = target_column_indices {
            indices[i]  // explicit target column index
        } else {
            file_order  // assume 1:1 mapping
        };
        lines.push(format!(
            "{order:<6}{type:<16}{prefix:<4}{len:<8}{term:<6}{scol:<6}{name:<40}{coll}",
            order = file_order,
            r#type = spec.host_type,
            prefix = spec.prefix_len,
            len = spec.host_data_len,
            term = "\"\"",
            scol = table_col,
            name = field.name(),
            coll = if spec.collation.is_empty() { "\"\"" } else { spec.collation },
        ));
    }

    lines.join("\n") + "\n"
}

// ── BcpProcess ───────────────────────────────────────────────────────────────

/// A `bcp` bulk-load session using native format file mode.
///
/// Each call to [`write_batch`] buffers rows.  When the buffer reaches
/// `batch_threshold` rows, the buffer is flushed: data is written to a temp
/// file, `bcp` is spawned, and the subprocess completes before returning.
/// [`finish`] drains any remaining rows and cleans up.
///
/// Peak disk usage = `batch_threshold` rows * row width.  No multi-GB temp
/// file accumulation.
pub(crate) struct BcpProcess {
    /// Native binary buffer, flushed to disk per bcp invocation.
    buffer:           Vec<u8>,
    /// Number of rows currently in `buffer`.
    buffer_rows:      usize,
    /// Flush when `buffer_rows >= batch_threshold`.
    batch_threshold:  usize,

    /// Total rows sent to SQL Server across all invocations.
    total_rows:       usize,
    /// Number of bcp invocations completed.
    invocations:      usize,

    /// Temp directory holding the data + format files.
    tmp_dir:          PathBuf,
    /// Path of the binary data file inside `tmp_dir` (overwritten each flush).
    data_path:        PathBuf,
    /// Path of the format file inside `tmp_dir` (written once).
    fmt_path:         PathBuf,
    /// Arrow schema (for logging).
    schema:           Arc<arrow::datatypes::Schema>,

    // ── bcp launch parameters ────────────────────────────────────────────
    bcp_bin:          String,
    bcp_table:        String,
    server:           String,
    user:             String,
    pass:             String,
    batch_str:        String,
    trust_cert:       bool,

    // ── bcp performance tuning ───────────────────────────────────────────
    /// Network packet size (`-a` flag, default 4096).
    packet_size:      Option<usize>,
    /// Max errors before abort (`-m` flag, default 1).
    max_errors:       usize,
}

impl BcpProcess {
    /// Prepare a bcp session for loading into `table` (three-part name:
    /// `database.schema.bare_name`).
    ///
    /// The format file is generated immediately from `schema`.  No subprocess
    /// is spawned until the first flush.
    ///
    /// `batch_threshold` controls how many rows accumulate before a `bcp`
    /// subprocess is spawned.  For the bcp `-b` flag, we always pass
    /// `batch_threshold` so each invocation is a single commit.
    pub(crate) fn spawn(
        bcp_bin:               &str,
        params:                &MssqlConnParams,
        table:                 &str,
        batch_threshold:       usize,
        schema:                &SchemaRef,
        target_column_indices: Option<Vec<usize>>,
    ) -> anyhow::Result<Self> {
        let tmp_dir = bcp_temp_base()
            .join(format!("potato_bcp_{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&tmp_dir)
            .map_err(|e| anyhow::anyhow!("failed to create bcp temp dir {tmp_dir:?}: {e}"))?;
        let data_path = tmp_dir.join("data.bin");
        let fmt_path  = tmp_dir.join("format.fmt");

        // Generate and write format file once.
        let fmt_content = generate_format_file(
            schema,
            target_column_indices.as_deref(),
        );
        std::fs::write(&fmt_path, &fmt_content)
            .map_err(|e| anyhow::anyhow!("failed to write bcp format file: {e}"))?;

        tracing::info!(
            dir       = %tmp_dir.display(),
            threshold = batch_threshold,
            columns   = schema.fields().len(),
            "bcp: session created (per-batch mode)",
        );

        Ok(Self {
            buffer:           Vec::with_capacity(batch_threshold * schema.fields().len() * 20),
            buffer_rows:      0,
            batch_threshold,
            total_rows:       0,
            invocations:      0,
            tmp_dir,
            data_path,
            fmt_path,
            schema:           Arc::clone(schema),
            bcp_bin:          bcp_bin.to_string(),
            bcp_table:        table.to_string(),
            server:           format!("{},{}", params.host, params.port),
            user:             params.user.clone(),
            pass:             params.pass.clone(),
            batch_str:        batch_threshold.to_string(),
            trust_cert:       params.trust_cert,
            packet_size:      params.bcp_packet_size,
            max_errors:       params.bcp_max_errors.unwrap_or(1),
        })
    }

    /// Serialize `batch` to native binary and append to the internal buffer.
    ///
    /// When the buffer reaches `batch_threshold` rows, it is automatically
    /// flushed to SQL Server via a `bcp` subprocess.
    pub(crate) async fn write_batch(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
        let n = batch.num_rows();
        if n == 0 { return Ok(()); }

        // Serialize on blocking thread pool.
        let batch_clone = batch.clone();
        let native = tokio::task::spawn_blocking(move || batch_to_native(&batch_clone))
            .await
            .map_err(|e| anyhow::anyhow!("write_batch spawn_blocking panicked: {e}"))?;

        self.buffer.extend_from_slice(&native);
        self.buffer_rows += n;

        // Auto-flush if threshold reached.
        if self.buffer_rows >= self.batch_threshold {
            self.flush_buffer().await?;
        }

        Ok(())
    }

    /// Flush the internal buffer to SQL Server via a `bcp` subprocess.
    ///
    /// This is called automatically when buffer >= threshold, and also
    /// called by `finish()` to drain remaining rows.
    async fn flush_buffer(&mut self) -> anyhow::Result<()> {
        if self.buffer_rows == 0 {
            return Ok(());
        }

        let rows_in_batch = self.buffer_rows;
        let bytes_in_batch = self.buffer.len();

        // Write buffer to data file (truncate + write — no accumulation).
        {
            let data_path = self.data_path.clone();
            let buf = std::mem::take(&mut self.buffer);
            let data_path2 = data_path.clone();
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                std::fs::write(&data_path2, &buf)
                    .map_err(|e| anyhow::anyhow!("failed to write bcp data file: {e}"))
            })
            .await
            .map_err(|e| anyhow::anyhow!("flush_buffer write panicked: {e}"))??;
        }

        // Re-allocate buffer for next batch.
        self.buffer = Vec::with_capacity(
            self.batch_threshold * self.schema.fields().len() * 20,
        );
        self.buffer_rows = 0;

        // ── Spawn bcp
        let error_file = self.tmp_dir.join("bcp_errors.txt");
        let mut cmd = Command::new(&self.bcp_bin);
        cmd .arg(&self.bcp_table)
            .arg("in")
            .arg(&self.data_path)
            .arg("-f").arg(&self.fmt_path)
            .arg("-S").arg(&self.server)
            .arg("-U").arg(&self.user)
            .arg("-P").arg(&self.pass)
            .arg("-b").arg(&self.batch_str)
            .arg("-h").arg("TABLOCK")
            .arg("-m").arg(self.max_errors.to_string())
            .arg("-e").arg(&error_file);

        // ── Performance tuning flags ──
        if let Some(size) = self.packet_size {
            cmd.arg("-a").arg(size.to_string());
        }

        if self.trust_cert {
            cmd.arg("-u");
        }

        cmd .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Debug: log the command (first invocation only, to avoid log spam).
        if self.invocations == 0 {
            let packet_arg = self.packet_size.map(|s| format!(" -a {s}")).unwrap_or_default();
            let trust_arg = if self.trust_cert { " -u" } else { "" };
            let cmd_str = format!(
                "{bin} {table} in {data} -f {fmt} -S {server} -U {user} -P ******** \
                 -b {batch} -h TABLOCK -m {max_err} -e {err}{packet}{trust}",
                bin     = self.bcp_bin,
                table   = self.bcp_table,
                data    = self.data_path.display(),
                fmt     = self.fmt_path.display(),
                server  = self.server,
                user    = self.user,
                batch   = self.batch_str,
                max_err = self.max_errors,
                err     = error_file.display(),
                packet  = packet_arg,
                trust   = trust_arg,
            );
            tracing::debug!("bcp: command (logged once):\n  {cmd_str}");
        }

        tracing::debug!(
            invocation = self.invocations + 1,
            rows       = rows_in_batch,
            bytes      = bytes_in_batch,
            "bcp: sending batch to SQL Server"
        );

        let mut child = cmd.spawn()
            .map_err(|e| anyhow::anyhow!("failed to spawn bcp ({}): {e}\n\
                Ensure mssql-tools18 is installed: \
                sudo apt install mssql-tools18  or  sudo dnf install mssql-tools18",
                self.bcp_bin))?;

        let mut stdout = child.stdout.take();
        let mut stderr = child.stderr.take();
        let mut stdout_buf = Vec::new();
        let mut stderr_buf = Vec::new();

        let (status, _, _) = tokio::join!(
            child.wait(),
            async {
                if let Some(ref mut s) = stdout {
                    let _ = s.read_to_end(&mut stdout_buf).await;
                }
            },
            async {
                if let Some(ref mut s) = stderr {
                    let _ = s.read_to_end(&mut stderr_buf).await;
                }
            },
        );

        let status = status.map_err(|e| anyhow::anyhow!("bcp wait failed: {e}"))?;

        if !status.success() {
            let out = String::from_utf8_lossy(&stdout_buf);
            let err = String::from_utf8_lossy(&stderr_buf);
            let error_rows = std::fs::read_to_string(&error_file).unwrap_or_default();
            let error_preview = if error_rows.is_empty() {
                String::from("(no error file content)")
            } else {
                error_rows.chars().take(2000).collect()
            };
            anyhow::bail!(
                "bcp exited with code {:?} (invocation {}, {} rows)\n\
                 stdout:\n{out}\nstderr:\n{err}\n\
                 bcp error file ({}):\n{error_preview}",
                status.code(),
                self.invocations + 1,
                rows_in_batch,
                error_file.display()
            );
        }

        self.total_rows  += rows_in_batch;
        self.invocations += 1;

        tracing::debug!(
            invocation  = self.invocations,
            rows_batch  = rows_in_batch,
            rows_total  = self.total_rows,
            "bcp: batch sent successfully"
        );

        Ok(())
    }

    /// Flush remaining buffered rows and clean up temp directory.
    ///
    /// Returns the total number of rows sent across all bcp invocations.
    pub(crate) async fn finish(mut self) -> anyhow::Result<usize> {
        // Drain remaining buffer.
        self.flush_buffer().await?;

        let total = self.total_rows;
        tracing::info!(
            rows        = total,
            invocations = self.invocations,
            "bcp: session complete"
        );

        // Clean up temp directory.
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
        Ok(total)
    }

    /// Clean up temp directory without sending remaining data.
    /// Used when aborting on error.
    pub(crate) fn abort(self) {
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

// Ensure temp directory is always cleaned up, even on panic or early drop.
impl Drop for BcpProcess {
    fn drop(&mut self) {
        // Best-effort cleanup — errors are silently ignored since we're
        // already in a destructor (logging might fail too).
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

// ── Native binary serialization ───────────────────────────────────────────────

/// Serialize a `RecordBatch` to native binary bytes matching the format file.
///
/// For each row, for each column: write prefix + data.  No row terminators,
/// no column delimiters -- the format file tells bcp exactly how to parse
/// each field.
pub(crate) fn batch_to_native(batch: &RecordBatch) -> Vec<u8> {
    let n   = batch.num_rows();
    let nc  = batch.num_columns();
    let sch = batch.schema();

    let mut out = Vec::with_capacity(n * nc * 20);

    for row in 0..n {
        for col_idx in 0..nc {
            let col = batch.column(col_idx);
            let dt  = sch.field(col_idx).data_type();
            let spec = arrow_to_bcp_spec(dt);

            if col.is_null(row) {
                write_null_prefix(&mut out, spec.prefix_len);
                continue;
            }

            write_native_field(&mut out, col.as_ref(), row, dt);
        }
    }
    out
}

/// Write the NULL sentinel for the given prefix length.
#[inline]
fn write_null_prefix(out: &mut Vec<u8>, prefix_len: u8) {
    match prefix_len {
        1 => out.push(0x00),
        4 => out.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes()),
        _ => unreachable!("unsupported prefix_len={prefix_len}"),
    }
}

/// Write a single non-NULL field in native binary format.
fn write_native_field(out: &mut Vec<u8>, col: &dyn Array, row: usize, dt: &DataType) {
    match dt {
        // ── Boolean (SQLBIT, prefix=1, 1 byte) ───────────────────────────
        DataType::Boolean => {
            let v = col.as_any().downcast_ref::<BooleanArray>().unwrap().value(row);
            out.push(1);
            out.push(if v { 1 } else { 0 });
        }

        // ── Integers (native LE bytes) ───────────────────────────────────
        DataType::Int8 => {
            let v = col.as_any().downcast_ref::<Int8Array>().unwrap().value(row);
            out.push(2);
            out.extend_from_slice(&(v as i16).to_le_bytes());
        }
        DataType::UInt8 => {
            let v = col.as_any().downcast_ref::<UInt8Array>().unwrap().value(row);
            out.push(1);
            out.push(v);
        }
        DataType::Int16 => {
            let v = col.as_any().downcast_ref::<Int16Array>().unwrap().value(row);
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        DataType::UInt16 => {
            let v = col.as_any().downcast_ref::<UInt16Array>().unwrap().value(row);
            out.push(4);
            out.extend_from_slice(&(v as i32).to_le_bytes());
        }
        DataType::Int32 => {
            let v = col.as_any().downcast_ref::<Int32Array>().unwrap().value(row);
            out.push(4);
            out.extend_from_slice(&v.to_le_bytes());
        }
        DataType::UInt32 => {
            let v = col.as_any().downcast_ref::<UInt32Array>().unwrap().value(row);
            out.push(8);
            out.extend_from_slice(&(v as i64).to_le_bytes());
        }
        DataType::Int64 => {
            let v = col.as_any().downcast_ref::<Int64Array>().unwrap().value(row);
            out.push(8);
            out.extend_from_slice(&v.to_le_bytes());
        }
        DataType::UInt64 => {
            let v = col.as_any().downcast_ref::<UInt64Array>().unwrap().value(row);
            let s = v.to_string();
            let bytes = s.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }

        // ── Floats (native IEEE 754 LE bytes) ────────────────────────────
        DataType::Float32 => {
            let v = col.as_any().downcast_ref::<Float32Array>().unwrap().value(row);
            if v.is_finite() {
                out.push(4);
                out.extend_from_slice(&v.to_le_bytes());
            } else {
                out.push(0x00); // NULL for NaN/Inf
            }
        }
        DataType::Float64 => {
            let v = col.as_any().downcast_ref::<Float64Array>().unwrap().value(row);
            if v.is_finite() {
                out.push(8);
                out.extend_from_slice(&v.to_le_bytes());
            } else {
                out.push(0x00); // NULL for NaN/Inf
            }
        }

        // ── Date32 -> "YYYY-MM-DD" (SQLCHAR, prefix=4) ──────────────────
        DataType::Date32 => {
            let days = col.as_any().downcast_ref::<Date32Array>().unwrap().value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            let s = format!("{}", date.format("%Y-%m-%d"));
            let bytes = s.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }

        // ── Time64(us) -> "HH:MM:SS.ffffff" (SQLCHAR, prefix=4) ─────────
        DataType::Time64(TimeUnit::Microsecond) => {
            let us = col.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap().value(row);
            let h  = us / 3_600_000_000_i64;
            let m  = (us % 3_600_000_000_i64) / 60_000_000_i64;
            let s  = (us % 60_000_000_i64) / 1_000_000_i64;
            let f  = us % 1_000_000_i64;
            let text = format!("{h:02}:{m:02}:{s:02}.{f:06}");
            let bytes = text.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }

        // ── Timestamp(us, None) -> "YYYY-MM-DD HH:MM:SS.ffffff" ─────────
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let us      = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(row);
            let days_i  = us.div_euclid(86_400_000_000_i64);
            let us_day  = us.rem_euclid(86_400_000_000_i64);
            let date    = chrono::NaiveDate::from_num_days_from_ce_opt(
                (days_i + 719_163) as i32
            ).unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            let h = us_day / 3_600_000_000;
            let m = (us_day % 3_600_000_000) / 60_000_000;
            let s = (us_day % 60_000_000) / 1_000_000;
            let f = us_day % 1_000_000;
            let text = format!(
                "{} {:02}:{:02}:{:02}.{:06}",
                date.format("%Y-%m-%d"), h, m, s, f
            );
            let bytes = text.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }

        // ── Timestamp(us, Some(_)) -> "YYYY-MM-DD HH:MM:SS.ffffff +00:00"
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            let us      = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(row);
            let days_i  = us.div_euclid(86_400_000_000_i64);
            let us_day  = us.rem_euclid(86_400_000_000_i64);
            let date    = chrono::NaiveDate::from_num_days_from_ce_opt(
                (days_i + 719_163) as i32
            ).unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            let h = us_day / 3_600_000_000;
            let m = (us_day % 3_600_000_000) / 60_000_000;
            let s = (us_day % 60_000_000) / 1_000_000;
            let f = us_day % 1_000_000;
            let text = format!(
                "{} {:02}:{:02}:{:02}.{:06} +00:00",
                date.format("%Y-%m-%d"), h, m, s, f
            );
            let bytes = text.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }

        // ── Strings (SQLCHAR, prefix=4) ──────────────────────────────────
        DataType::Utf8 => {
            let s = col.as_any().downcast_ref::<StringArray>().unwrap().value(row);
            let bytes = s.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        DataType::LargeUtf8 => {
            let s = col.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row);
            let bytes = s.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }

        // ── Binary (SQLBINARY, prefix=4) ─────────────────────────────────
        DataType::Binary => {
            let b = col.as_any().downcast_ref::<BinaryArray>().unwrap().value(row);
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        DataType::LargeBinary => {
            let b = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap().value(row);
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        DataType::FixedSizeBinary(_) => {
            let b = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap().value(row);
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }

        // ── Decimal128 -> fixed-point text (SQLCHAR, prefix=4) ───────────
        DataType::Decimal128(_, scale) => {
            let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let v = arr.value(row);
            let scale = *scale as u32;
            let text = format_decimal128(v, scale);
            let bytes = text.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }

        // ── Fallback: Arrow display formatter -> SQLCHAR ─────────────────
        _ => {
            if let Ok(s) = arrow::util::display::array_value_to_string(col, row) {
                let bytes = s.as_bytes();
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            } else {
                out.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
            }
        }
    }
}

/// Format a Decimal128 unscaled value as a fixed-point string.
fn format_decimal128(v: i128, scale: u32) -> String {
    use std::fmt::Write as _;

    let mut s = String::with_capacity(42);

    let abs = if v < 0 {
        s.push('-');
        // Use wrapping_neg() to handle i128::MIN gracefully.
        v.wrapping_neg()
    } else {
        v
    };

    let divisor = 10_i128.pow(scale);
    let int_part  = abs / divisor;
    let frac_part = abs % divisor;

    let _ = write!(s, "{int_part}");

    if scale > 0 {
        s.push('.');
        let frac_str = frac_part.to_string();
        for _ in 0..(scale as usize).saturating_sub(frac_str.len()) {
            s.push('0');
        }
        s.push_str(&frac_str);
    }

    s
}