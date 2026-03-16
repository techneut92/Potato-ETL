//! Arrow `Field` metadata keys and builder helpers for source DB type info.
//!
//! Every source driver sets these keys on each [`arrow::datatypes::Field`] so
//! that downstream components (sinks, transforms, the PotatoFlow UI) can:
//!
//! - Warn when a source type has no direct equivalent in the target DB.
//! - Reconstruct the original DDL with correct precision / scale / length.
//! - Display human-readable type info in the pipeline builder.
//!
//! ## Keys stored per field
//!
//! | Key                | Example value              | When present                     |
//! |--------------------|----------------------------|----------------------------------|
//! | `source_db_type`   | `"numeric"`, `"timetz"`    | Always                           |
//! | `source_precision` | `"20"`                     | NUMERIC/DECIMAL, TIMESTAMP(n), TIME(n) |
//! | `source_scale`     | `"10"`                     | NUMERIC/DECIMAL only             |
//! | `source_length`    | `"255"`, `"-1"` (= MAX)    | VARCHAR / CHAR / NVARCHAR / RAW  |
//! | `source_db`        | `"postgres"`, `"mssql"`    | Always                           |
//!
//! ## Streaming vs. introspection path
//!
//! - **`read_schema()`** — queries `INFORMATION_SCHEMA` / `ALL_TAB_COLUMNS` and
//!   populates all four keys where the DB has the information.
//! - **`exec()` streaming path** — only the wire type tag is available, so only
//!   `source_db_type` is set (except Oracle, which exposes precision/scale on
//!   `ColumnInfo::oracle_type()`).
//!
//! Consumers should treat `source_precision`, `source_scale`, and
//! `source_length` as **advisory / best-effort** and handle their absence.

use std::collections::HashMap;

/// Arrow Field metadata key: the original DB type name, lowercase.
///
/// Examples: `"numeric"`, `"timetz"`, `"jsonb"`, `"nvarchar"`,
/// `"varchar2"`, `"number"`.
pub const SOURCE_DB_TYPE: &str = "source_db_type";

/// Arrow Field metadata key: precision for numeric or datetime types.
///
/// - `NUMERIC(20, 10)` -> `"20"`
/// - `TIMESTAMP(6)` or `TIME(3)` -> `"6"` or `"3"`
pub const SOURCE_PRECISION: &str = "source_precision";

/// Arrow Field metadata key: scale (decimal places) for numeric types.
///
/// - `NUMERIC(20, 10)` -> `"10"`
pub const SOURCE_SCALE: &str = "source_scale";

/// Arrow Field metadata key: maximum character / byte length.
///
/// - `VARCHAR(255)` -> `"255"`
/// - `NVARCHAR(MAX)` (MSSQL) -> `"-1"`
pub const SOURCE_LENGTH: &str = "source_length";

/// Arrow Field metadata key: which database engine produced this column.
///
/// Value is a lowercase driver name: `"postgres"`, `"mssql"`, `"oracle"`,
/// `"mysql"`, `"databricks"`.
///
/// Used by [`logical_type_for_source`][crate::schema::field::logical_type_for_source]
/// to disambiguate type names that collide across databases (e.g. `"bit"` means
/// boolean in MSSQL but a bit-string in Postgres).
pub const SOURCE_DB: &str = "source_db";

// -- Builder helpers ----------------------------------------------------------

/// Build Arrow Field metadata from source DB type information.
///
/// `precision` is used for both numeric precision and datetime fractional-
/// seconds digits.  Pass `None` for types that carry no precision.
pub fn make(
    db_type:   impl Into<String>,
    precision: Option<i32>,
    scale:     Option<i32>,
    length:    Option<i64>,
) -> HashMap<String, String> {
    let mut m = HashMap::with_capacity(4);
    m.insert(SOURCE_DB_TYPE.to_string(), db_type.into());
    if let Some(p) = precision { m.insert(SOURCE_PRECISION.to_string(), p.to_string()); }
    if let Some(s) = scale     { m.insert(SOURCE_SCALE.to_string(),     s.to_string()); }
    if let Some(l) = length    { m.insert(SOURCE_LENGTH.to_string(),     l.to_string()); }
    m
}

/// Shorthand: type name only, no precision / scale / length.
///
/// Use this in streaming read paths where only the wire type tag is available.
#[inline]
pub fn type_only(db_type: impl Into<String>) -> HashMap<String, String> {
    make(db_type, None, None, None)
}

/// Stamps `etl.source_db` and `etl.logical_type` into an existing metadata map.
///
/// Call this immediately after [`make`] or [`type_only`] in each source
/// connector's `read_schema()` path.  The `exec()` (streaming) path should also
/// call it when the wire type is available.
///
/// ```rust,no_run
/// use potato_etl_common::db::common::field_meta;
///
/// let mut meta = field_meta::make("jsonb", None, None, None);
/// field_meta::stamp_logical(&mut meta, "jsonb", "postgres");
/// // meta now contains:
/// //   source_db_type = "jsonb"
/// //   source_db      = "postgres"
/// //   etl.logical_type = "json"
/// ```
pub fn stamp_logical(
    meta:       &mut HashMap<String, String>,
    db_type:    &str,
    source_db:  &str,
) {
    meta.insert(SOURCE_DB.to_string(), source_db.to_lowercase());

    if let Some(lt) = crate::schema::field::logical_type_for_source(db_type, source_db) {
        meta.insert(
            crate::schema::constants::META_LOGICAL_TYPE.to_string(),
            lt.as_str().to_string(),
        );
    }
}

/// Full constructor: build metadata map and immediately stamp logical type.
///
/// Equivalent to calling [`make`] then [`stamp_logical`].
/// Preferred in new source connector code.
pub fn make_with_source(
    db_type:   impl Into<String>,
    precision: Option<i32>,
    scale:     Option<i32>,
    length:    Option<i64>,
    source_db: &str,
) -> HashMap<String, String> {
    let db_type_str: String = db_type.into();
    let mut m = make(&db_type_str, precision, scale, length);
    stamp_logical(&mut m, &db_type_str, source_db);
    m
}
