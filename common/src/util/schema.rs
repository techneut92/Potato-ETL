//! Schema mapping: DB type names → Arrow `DataType`, plus per-column overrides.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

// ── Arrow type name → DataType ────────────────────────────────────────────────

/// Converts a user-supplied type name to an Arrow `DataType`.
///
/// Supports both plain names and bracket notation:
///
/// Plain names (case-insensitive):
/// `boolean`, `bool`, `int8`, `int16`, `int32`, `int`, `int64`, `bigint`,
/// `float32`, `real`, `float64`, `double`, `utf8`, `string`, `text`, `varchar`,
/// `large_utf8`, `date32`, `timestamp`, `ts`
///
/// Bracket notation (time unit in brackets):
/// `timestamp[s]`, `timestamp[ms]`, `timestamp[us]`, `timestamp[ns]`
/// `timestamp[us, UTC]`  — timezone is taken verbatim (uppercase)
/// `time64[us]`, `time64[ns]`
/// `time32[s]`, `time32[ms]`
/// `duration[s]`, `duration[ms]`, `duration[us]`, `duration[ns]`
pub fn parse_arrow_type(s: &str) -> anyhow::Result<DataType> {
    // ── Bracket notation: type[unit] or type[unit, TZ] ────────────────────────
    if let Some(bracket_pos) = s.find('[') {
        let type_part = s[..bracket_pos].trim().to_lowercase();
        let inner     = s[bracket_pos + 1..].trim_end_matches(']').trim();

        // Split on the first comma: unit[, optional_tz]
        let (unit_str, tz_raw) = match inner.splitn(2, ',').collect::<Vec<_>>().as_slice() {
            [u, tz] => (u.trim(), Some(tz.trim())),
            [u]     => (u.trim(), None),
            _       => anyhow::bail!("Malformed Arrow type bracket in '{s}'"),
        };

        let unit = parse_time_unit(unit_str, s)?;

        return match type_part.as_str() {
            "timestamp" => {
                let tz: Option<Arc<str>> = tz_raw
                    .filter(|t| !t.is_empty())
                    .map(|t| Arc::from(t.to_uppercase().as_str()));
                Ok(DataType::Timestamp(unit, tz))
            }
            "time64"   => Ok(DataType::Time64(unit)),
            "time32"   => Ok(DataType::Time32(unit)),
            "duration" => Ok(DataType::Duration(unit)),
            _ => anyhow::bail!(
                "Unknown Arrow type '{s}'. \
                 Bracket notation supports: timestamp, time32, time64, duration"
            ),
        };
    }

    // ── Plain names ───────────────────────────────────────────────────────────
    match s.to_lowercase().replace(['-', ' '], "_").as_str() {
        "boolean" | "bool"                           => Ok(DataType::Boolean),
        "int8"                                        => Ok(DataType::Int8),
        "int16" | "smallint"                          => Ok(DataType::Int16),
        "int32" | "int" | "integer"                   => Ok(DataType::Int32),
        "int64" | "bigint"                            => Ok(DataType::Int64),
        "float32" | "real"                            => Ok(DataType::Float32),
        "float64" | "double" | "double_precision"     => Ok(DataType::Float64),
        "utf8" | "string" | "text" | "varchar"        => Ok(DataType::Utf8),
        "large_utf8" | "longtext"                     => Ok(DataType::LargeUtf8),
        "date32" | "date"                             => Ok(DataType::Date32),
        // Plain "timestamp" / "ts" / "datetime" → microseconds, no tz
        // (use bracket notation for full control: timestamp[us, UTC])
        "timestamp" | "ts" | "datetime"               => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "binary"                                      => Ok(DataType::Binary),
        "large_binary"                                => Ok(DataType::LargeBinary),
        _ => anyhow::bail!(
            "Unknown Arrow type '{s}'. \
             Supported plain names: boolean, int8/16/32/64, float32/64, utf8, \
             date32, timestamp. \
             Bracket notation: timestamp[us], timestamp[us, UTC], time64[us], etc."
        ),
    }
}

fn parse_time_unit(unit: &str, context: &str) -> anyhow::Result<TimeUnit> {
    match unit {
        "s"  => Ok(TimeUnit::Second),
        "ms" => Ok(TimeUnit::Millisecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ns" => Ok(TimeUnit::Nanosecond),
        _    => anyhow::bail!(
            "Unknown time unit '{unit}' in Arrow type '{context}'. \
             Valid units: s, ms, us, ns"
        ),
    }
}

/// Lowercases all column names in a `RecordBatch`.
///
/// Examples:
/// - `"EMPLOYEE_ID"` → `"employee_id"`
/// - `"FirstName"` → `"firstname"`
/// - `"salary"` → `"salary"` (no change)
///
/// Column data and Arrow field metadata are preserved; only the field names change.
pub fn apply_normalize_columns(batch: RecordBatch) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();

    // Fast path: already all lowercase.
    if schema.fields().iter().all(|f| f.name() == &f.name().to_lowercase()) {
        return Ok(batch);
    }

    // Preserve metadata when lowercasing field names.
    let fields: Vec<Field> = schema.fields().iter()
        .map(|f| {
            Field::new(f.name().to_lowercase().as_str(), f.data_type().clone(), f.is_nullable())
                .with_metadata(f.metadata().clone())
        })
        .collect();

    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())?)
}

/// Returns whether column names should be normalised for a given connection string.
///
/// Auto-logic:
/// - `oracle://` → `true` (Oracle returns UPPERCASE by default)
/// - All others  → `false`
///
/// MySQL, Databricks, and PostgreSQL preserve original case.  Use
/// `normalize_columns: true` in `ReadOptions` to force normalisation.
pub fn should_normalize(conn_str: &str, explicit: Option<bool>) -> bool {
    explicit.unwrap_or_else(|| conn_str.starts_with("oracle://"))
}

// ── Vendor-specific default type maps (informational) ────────────────────────

/// Default Postgres SQL type → Arrow `DataType` mapping.
pub fn postgres_type_map() -> HashMap<&'static str, DataType> {
    [
        ("smallint",          DataType::Int16),
        ("integer",           DataType::Int32),
        ("bigint",            DataType::Int64),
        ("real",              DataType::Float32),
        ("double precision",  DataType::Float64),
        ("numeric",           DataType::Float64),
        ("decimal",           DataType::Float64),
        ("boolean",           DataType::Boolean),
        ("text",              DataType::Utf8),
        ("character varying", DataType::Utf8),
        ("varchar",           DataType::Utf8),
        ("date",              DataType::Date32),
        ("timestamp without time zone", DataType::Timestamp(TimeUnit::Microsecond, None)),
        ("timestamp with time zone",    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
    ].into()
}

/// Default MSSQL type → Arrow `DataType` mapping.
pub fn mssql_type_map() -> HashMap<&'static str, DataType> {
    [
        ("tinyint",   DataType::Int8),
        ("smallint",  DataType::Int16),
        ("int",       DataType::Int32),
        ("bigint",    DataType::Int64),
        ("real",      DataType::Float32),
        ("float",     DataType::Float64),
        ("numeric",   DataType::Float64),
        ("decimal",   DataType::Float64),
        ("bit",       DataType::Boolean),
        ("nvarchar",  DataType::Utf8),
        ("varchar",   DataType::Utf8),
        ("nchar",     DataType::Utf8),
        ("char",      DataType::Utf8),
        ("ntext",     DataType::Utf8),
        ("text",      DataType::Utf8),
        ("date",      DataType::Date32),
        ("datetime",  DataType::Timestamp(TimeUnit::Millisecond, None)),
        ("datetime2", DataType::Timestamp(TimeUnit::Microsecond, None)),
    ].into()
}

/// Default Oracle type → Arrow `DataType` mapping.
///
/// Oracle has no native boolean before 23c — `NUMBER(1)` is the convention.
/// Use `arrow_overrides` to map NUMBER columns to boolean.
pub fn oracle_type_map() -> HashMap<&'static str, DataType> {
    [
        ("NUMBER",    DataType::Float64),  // may be int or decimal; use overrides for precision
        ("FLOAT",     DataType::Float64),
        ("BINARY_FLOAT",  DataType::Float32),
        ("BINARY_DOUBLE", DataType::Float64),
        ("VARCHAR2",  DataType::Utf8),
        ("NVARCHAR2", DataType::Utf8),
        ("CHAR",      DataType::Utf8),
        ("NCHAR",     DataType::Utf8),
        ("CLOB",      DataType::Utf8),
        ("NCLOB",     DataType::Utf8),
        ("DATE",      DataType::Timestamp(TimeUnit::Second, None)),
        ("TIMESTAMP", DataType::Timestamp(TimeUnit::Microsecond, None)),
    ].into()
}
