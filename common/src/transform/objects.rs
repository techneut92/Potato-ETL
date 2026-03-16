//! `build_objects` — transform a flat `RecordBatch` into nested JSON objects.
//!
//! Each row is converted to a JSON object and stored as a UTF-8 string in a
//! new column (default `_json`).  The output `RecordBatch` contains:
//!
//! - all **original columns** (so downstream steps can still access field values)
//! - one extra `_json` column with the constructed object as a JSON string
//!
//! # Field mapping
//!
//! `field_map` maps column names to dot-notation paths in the output object.
//! Empty `field_map` → use **all columns** as flat JSON keys.
//!
//! # URL-template helper
//!
//! [`resolve_url_template`] replaces `{column_name}` placeholders in a URL string
//! with the row value of that column.  Used by the REST API sink.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::{Map, Value};

// ── Public API ────────────────────────────────────────────────────────────────

/// Adds a `_json` column to `batch` with a constructed JSON object per row.
pub fn build_objects(
    batch:      RecordBatch,
    field_map:  &HashMap<String, String>,
    output_col: &str,
) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let n      = batch.num_rows();

    let mut json_strings: Vec<Option<String>> = Vec::with_capacity(n);

    for row_idx in 0..n {
        let obj = if field_map.is_empty() {
            let mut map = Map::new();
            for col_idx in 0..batch.num_columns() {
                let col_name = schema.field(col_idx).name().clone();
                let value    = array_value_at(batch.column(col_idx), row_idx);
                map.insert(col_name, value);
            }
            Value::Object(map)
        } else {
            let mut root = Map::new();
            for (col_name, json_path) in field_map {
                let col_idx = schema.index_of(col_name).map_err(|_| anyhow::anyhow!(
                    "build_objects: column '{col_name}' not found. \
                     Available columns: {:?}",
                    schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                ))?;
                let value = array_value_at(batch.column(col_idx), row_idx);
                set_nested_value(&mut root, json_path, value);
            }
            Value::Object(root)
        };

        json_strings.push(Some(serde_json::to_string(&obj)?));
    }

    let json_array: StringArray = json_strings.into_iter().collect();

    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(output_col, DataType::Utf8, true));

    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(json_array));

    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}

/// Replaces `{column_name}` placeholders in a URL template with row values.
pub fn resolve_url_template(template: &str, batch: &RecordBatch, row_idx: usize) -> String {
    let schema = batch.schema();
    let mut url = template.to_string();

    let mut start = 0;
    while let Some(open) = url[start..].find('{') {
        let abs_open = start + open;
        if let Some(close) = url[abs_open..].find('}') {
            let abs_close  = abs_open + close;
            let placeholder = &url[abs_open + 1..abs_close];

            let replacement = schema.index_of(placeholder)
                .ok()
                .map(|idx| scalar_to_url_string(array_value_at(batch.column(idx), row_idx)))
                .unwrap_or_else(|| format!("{{{placeholder}}}"));

            url = format!("{}{}{}", &url[..abs_open], replacement, &url[abs_close + 1..]);
            start = abs_open + replacement.len();
        } else {
            break;
        }
    }
    url
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Extracts the value at `idx` from an Arrow column as `serde_json::Value`.
pub(crate) fn array_value_at(arr: &dyn Array, idx: usize) -> Value {
    if arr.is_null(idx) { return Value::Null; }

    match arr.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(a.value(idx).to_string())
        }
        DataType::Int8   => json_int(arr.as_any().downcast_ref::<Int8Array>().unwrap().value(idx) as i64),
        DataType::Int16  => json_int(arr.as_any().downcast_ref::<Int16Array>().unwrap().value(idx) as i64),
        DataType::Int32  => json_int(arr.as_any().downcast_ref::<Int32Array>().unwrap().value(idx) as i64),
        DataType::Int64  => json_int(arr.as_any().downcast_ref::<Int64Array>().unwrap().value(idx)),
        DataType::UInt8  => json_int(arr.as_any().downcast_ref::<UInt8Array>().unwrap().value(idx)   as i64),
        DataType::UInt16 => json_int(arr.as_any().downcast_ref::<UInt16Array>().unwrap().value(idx)  as i64),
        DataType::UInt32 => json_int(arr.as_any().downcast_ref::<UInt32Array>().unwrap().value(idx)  as i64),
        DataType::UInt64 => json_int(arr.as_any().downcast_ref::<UInt64Array>().unwrap().value(idx)  as i64),
        DataType::Float32 => {
            let v = arr.as_any().downcast_ref::<Float32Array>().unwrap().value(idx) as f64;
            serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
        }
        DataType::Float64 => {
            let v = arr.as_any().downcast_ref::<Float64Array>().unwrap().value(idx);
            serde_json::Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
        }
        DataType::Boolean => {
            Value::Bool(arr.as_any().downcast_ref::<BooleanArray>().unwrap().value(idx))
        }
        _ => cast_to_string(arr, idx),
    }
}

fn cast_to_string(arr: &dyn Array, idx: usize) -> Value {
    cast(arr, &DataType::Utf8)
        .ok()
        .and_then(|a| a.as_any().downcast_ref::<StringArray>().map(|s| Value::String(s.value(idx).to_string())))
        .unwrap_or(Value::Null)
}

fn json_int(v: i64) -> Value { Value::Number(v.into()) }

fn scalar_to_url_string(v: Value) -> String {
    match v {
        Value::String(s)  => s,
        Value::Number(n)  => n.to_string(),
        Value::Bool(b)    => b.to_string(),
        Value::Null       => String::new(),
        other             => other.to_string(),
    }
}

/// Sets a value in a nested `Map` via a dot-notation path.
fn set_nested_value(obj: &mut Map<String, Value>, path: &str, value: Value) {
    match path.split_once('.') {
        None => { obj.insert(path.to_string(), value); }
        Some((key, rest)) => {
            let child = obj
                .entry(key.to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            if let Value::Object(child_map) = child {
                set_nested_value(child_map, rest, value);
            } else {
                let mut new_map = Map::new();
                set_nested_value(&mut new_map, rest, value);
                *child = Value::Object(new_map);
            }
        }
    }
}
