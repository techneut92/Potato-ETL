//! Arrow utility functions shared across all I/O backends.
//!
//! - `json_rows_to_record_batch` — `&[serde_json::Value]` → `RecordBatch`
//! - `record_batch_to_string_rows` — `RecordBatch` → `Vec<Vec<Option<String>>>` (for SQL binding)
//! - `extract_scd2_key_strings` — key-column → `Vec<String>` (used by all SCD2 backends)

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::sync::Arc;

// ── JSON → RecordBatch ────────────────────────────────────────────────────────

/// Converts a slice of JSON objects (one per row) into an Arrow `RecordBatch`.
///
/// Column types are inferred from the first non-null value per column:
/// - i64 → `Int64`, f64 → `Float64`, bool → `Boolean`, everything else → `Utf8`
pub fn json_rows_to_record_batch(rows: &[Value]) -> RecordBatch {
    if rows.is_empty() {
        return RecordBatch::new_empty(Arc::new(Schema::empty()));
    }

    let first = match rows[0].as_object() {
        Some(o) => o,
        None    => return RecordBatch::new_empty(Arc::new(Schema::empty())),
    };

    let keys: Vec<String> = first.keys().cloned().collect();
    let mut fields: Vec<Field>    = Vec::with_capacity(keys.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(keys.len());

    for key in &keys {
        let vals: Vec<&Value> = rows.iter().map(|r| &r[key]).collect();
        let first_non_null    = vals.iter().find(|v| !v.is_null()).copied();

        match first_non_null {
            Some(v) if v.is_i64() => {
                let arr: Int64Array = vals.iter().map(|v| v.as_i64()).collect();
                fields.push(Field::new(key, DataType::Int64, true));
                arrays.push(Arc::new(arr));
            }
            Some(v) if v.is_f64() => {
                let arr: Float64Array = vals.iter().map(|v| v.as_f64()).collect();
                fields.push(Field::new(key, DataType::Float64, true));
                arrays.push(Arc::new(arr));
            }
            Some(v) if v.is_boolean() => {
                let arr: BooleanArray = vals.iter().map(|v| v.as_bool()).collect();
                fields.push(Field::new(key, DataType::Boolean, true));
                arrays.push(Arc::new(arr));
            }
            _ => {
                // Anything without a clear numeric/boolean type → Utf8.
                let strs: Vec<Option<String>> = vals
                    .iter()
                    .map(|v| {
                        if v.is_null() {
                            None
                        } else {
                            Some(
                                v.as_str()
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| v.to_string()),
                            )
                        }
                    })
                    .collect();
                let arr: StringArray = strs.iter().map(|s| s.as_deref()).collect();
                fields.push(Field::new(key, DataType::Utf8, true));
                arrays.push(Arc::new(arr));
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .expect("RecordBatch construction failed in json_rows_to_record_batch")
}

// ── RecordBatch → rows of Option<String> (for SQL parameter binding) ──────────

/// Converts a `RecordBatch` into row-major `Vec<Vec<Option<String>>>`.
///
/// Each cell is serialised as text.  Used by the Postgres upsert path and
/// both the MSSQL and Oracle write paths, which bind parameters as strings.
pub fn record_batch_to_string_rows(batch: &RecordBatch) -> Vec<Vec<Option<String>>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let mut rows: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(num_cols); num_rows];

    for col_idx in 0..num_cols {
        let col = batch.column(col_idx);
        for row_idx in 0..num_rows {
            rows[row_idx].push(col_value_to_string(col.as_ref(), row_idx));
        }
    }

    rows
}

fn col_value_to_string(array: &dyn Array, idx: usize) -> Option<String> {
    if array.is_null(idx) {
        return None;
    }
    // arrow::util::display::array_value_to_string handles all Arrow DataTypes:
    // integers, floats, booleans, strings, dates, timestamps, decimals, …
    // Using it as the single source of truth avoids a fragile type-dispatch
    // table and guarantees correct output for types previously unhandled here
    // (Date32, Timestamp*, Int8, Decimal128, Binary, …).
    arrow::util::display::array_value_to_string(array, idx).ok()
}

// ── SCD2 key extraction ───────────────────────────────────────────────────────

/// Extracts the SCD2 key column values as `Vec<String>`.
///
/// All Arrow types that produce a meaningful string representation are
/// supported (INT32, INT64, UTF8, DATE32, TIMESTAMP, …).  NULL cells are
/// represented as an empty string so the key set never contains `None`.
pub fn extract_scd2_key_strings(
    batch:   &RecordBatch,
    key_idx: usize,
) -> anyhow::Result<Vec<String>> {
    let col = batch.column(key_idx);
    let mut out = Vec::with_capacity(col.len());

    for i in 0..col.len() {
        if col.is_null(i) {
            out.push(String::new());
        } else {
            out.push(
                arrow::util::display::array_value_to_string(col.as_ref(), i)
                    .map_err(|e| anyhow::anyhow!(
                        "SCD2 key column (type {:?}): cannot convert row {i} to string: {e}",
                        col.data_type()
                    ))?
            );
        }
    }

    Ok(out)
}
