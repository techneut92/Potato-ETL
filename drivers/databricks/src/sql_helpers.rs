//! Shared Databricks SQL generation helpers.
//!
//! Used by both the REST API sink (`api::sink`) and the ODBC sink
//! (`odbc::sink`) for DDL, INSERT value formatting, and Delta MERGE
//! statement construction.

use arrow::datatypes::{DataType, SchemaRef};

use potato_etl_common::schema::ddl::{resolve_sql_type, SqlDialect};
use crate::conn::backtick;

// ── DDL ──────────────────────────────────────────────────────────────────────

/// Generates a `CREATE TABLE … USING DELTA` statement.
pub(crate) fn create_table_sql(schema: &SchemaRef, full_table: &str, if_not_exists: bool) -> String {
    let guard = if if_not_exists { "IF NOT EXISTS " } else { "" };
    let cols: Vec<String> = schema.fields().iter().map(|f| {
        let st = resolve_sql_type(f, SqlDialect::Databricks);
        let nn = if f.is_nullable() { "" } else { " NOT NULL" };
        format!("  {} {}{}", backtick(f.name()), st, nn)
    }).collect();
    format!("CREATE TABLE {guard}{full_table} (\n{}\n)\nUSING DELTA", cols.join(",\n"))
}

// ── MERGE helpers ────────────────────────────────────────────────────────────

/// Builds the `ON target.pk = source.pk AND …` clause for a Delta MERGE.
pub(crate) fn delta_on_clause(pk: &[String]) -> String {
    pk.iter()
        .map(|k| format!("target.{} = source.{}", backtick(k), backtick(k)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Builds the `SET col = source.col, …` clause for MERGE's WHEN MATCHED UPDATE.
/// Primary-key columns are excluded (they are join keys, not update targets).
pub(crate) fn delta_update_set(cols: &[String], pk: &[String]) -> String {
    cols.iter()
        .filter(|c| !pk.contains(c))
        .map(|c| format!("target.{} = source.{}", backtick(c), backtick(c)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Builds the `source.col1, source.col2, …` value list for MERGE's
/// WHEN NOT MATCHED INSERT.
pub(crate) fn delta_insert_vals(cols: &[String]) -> String {
    cols.iter()
        .map(|c| format!("source.{}", backtick(c)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Builds a complete `MERGE INTO … USING (VALUES …) AS source ON … WHEN …`
/// statement for Delta Lake upsert / insert-ignore / merge-delete.
pub(crate) fn build_delta_merge(
    schema: &SchemaRef,
    ft: &str,
    cols_sql: &str,
    on: &str,
    chunk: &[Vec<Option<String>>],
    update: Option<&str>,
    insert_vals: &str,
    with_delete: bool,
) -> String {
    let cnbt = schema.fields().iter().map(|f| backtick(f.name())).collect::<Vec<_>>().join(", ");
    let rows: Vec<String> = chunk.iter().map(|row| {
        let vals: Vec<String> = schema.fields().iter().zip(row.iter())
            .map(|(f, v)| format_sql_value(v.as_deref(), f.data_type()))
            .collect();
        format!("({})", vals.join(", "))
    }).collect();
    let uc = update
        .map(|u| format!("WHEN MATCHED THEN UPDATE SET {} ", u))
        .unwrap_or_default();
    let dc = if with_delete { "WHEN NOT MATCHED BY SOURCE THEN DELETE " } else { "" };
    format!(
        "MERGE INTO {ft} AS target USING (SELECT * FROM (VALUES {r}) AS _src({cnbt})) AS source ON {on} {uc}WHEN NOT MATCHED THEN INSERT ({cols_sql}) VALUES ({insert_vals}) {dc}",
        r = rows.join(", ")
    )
}

// ── Value formatting ─────────────────────────────────────────────────────────

/// Formats a single cell value as a Databricks SQL literal.
///
/// - `None` → `NULL`
/// - Booleans → `true` / `false`
/// - Numeric types → unquoted
/// - Everything else → single-quoted with `'` escaping
pub(crate) fn format_sql_value(v: Option<&str>, dt: &DataType) -> String {
    match v {
        None => "NULL".to_string(),
        Some(s) => match dt {
            DataType::Boolean => {
                if s == "true" || s == "1" { "true".into() } else { "false".into() }
            }
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
                s.to_string()
            }
            _ => format!("'{}'", s.replace('\'', "''")),
        },
    }
}
