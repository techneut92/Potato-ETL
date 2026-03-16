//! Array-unnesting transform — explode a List/LargeList/JSON-array column into
//! one row per element, optionally extracting sub-fields and carrying forward
//! parent columns.
//!
//! ## When to use this
//!
//! REST APIs often return nested arrays (e.g. a candidate with a list of
//! talent pools).  The `unnest` step lets you normalize this into flat
//! relational rows — perfect for star-schema or junction-table patterns.
//!
//! ## JSON string columns
//!
//! When the target column is a `Utf8`/`LargeUtf8` string containing a JSON
//! array, `unnest` automatically parses the JSON and explodes the elements.
//! Sub-field extraction via `fields` uses dot-notation (same as `flatten`).
//!
//! ## Example
//!
//! Input batch (2 rows):
//!
//! | id   | name  | talent_pools (JSON string)                          |
//! |------|-------|-----------------------------------------------------|
//! | 1    | Alice | `[{"id":"tp1","data":"x"},{"id":"tp2","data":"y"}]` |
//! | 2    | Bob   | `[{"id":"tp3","data":"z"}]`                         |
//!
//! Config:
//! ```yaml
//! - type: unnest
//!   column: talent_pools
//!   parent_fields:
//!     candidate_id: id
//!   fields:
//!     talentpool_id: id
//!     pool_data: data
//! ```
//!
//! Output batch (3 rows):
//!
//! | candidate_id | talentpool_id | pool_data |
//! |--------------|---------------|-----------|
//! | 1            | tp1           | x         |
//! | 1            | tp2           | y         |
//! | 2            | tp3           | z         |

use std::sync::Arc;

use indexmap::IndexMap;

use arrow::array::{Array, ArrayRef, StringArray, ListArray, LargeListArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::expr::json_path_get;

// ── Config ───────────────────────────────────────────────────────────────────

/// Configuration for the `unnest` transform.
#[derive(Debug, Clone, Default)]
pub struct UnnestConfig {
    /// Name of the column containing the array to explode.
    pub column: String,
    /// Parent columns to carry forward into the output.
    /// Key = output column name, value = source column name (from the parent row).
    pub parent_fields: IndexMap<String, String>,
    /// Sub-fields to extract from each array element.
    /// Key = output column name, value = dot-notation path within each element.
    /// If empty, the raw element is emitted as a string column named after `column`.
    pub fields: IndexMap<String, String>,
}

// ── Public API ───────────────────────────────────────────────────────────────

/// Explodes an array column into one row per element, extracting sub-fields
/// and carrying forward parent columns.
pub fn apply_unnest(
    batch:  RecordBatch,
    config: &UnnestConfig,
) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();

    // ── Locate the array column ──────────────────────────────────────────
    let col_idx = schema.index_of(&config.column).map_err(|_| anyhow::anyhow!(
        "unnest: column '{}' not found in batch schema. Available: {:?}",
        config.column,
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    ))?;
    let array_col = batch.column(col_idx);

    // ── Resolve parent column refs ───────────────────────────────────────
    let parent_sources: Vec<(&str, usize)> = config.parent_fields.iter()
        .map(|(out_name, src_name)| {
            let idx = schema.index_of(src_name).map_err(|_| anyhow::anyhow!(
                "unnest: parent_fields references column '{}' which is not in the batch schema",
                src_name
            ))?;
            Ok((out_name.as_str(), idx))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // ── Determine element format ─────────────────────────────────────────
    // We support:
    // 1. ListArray / LargeListArray (native Arrow arrays)
    // 2. Utf8/LargeUtf8 containing JSON array strings
    let elements = extract_elements(array_col, num_rows)?;

    // ── Build output columns ─────────────────────────────────────────────
    let total_elements: usize = elements.iter().map(|e| e.len()).sum();

    // Parent columns: repeat each parent value for every element in that row.
    let mut parent_builders: Vec<Vec<Option<String>>> = vec![Vec::with_capacity(total_elements); parent_sources.len()];

    for row_idx in 0..num_rows {
        let n_elements = elements[row_idx].len();
        for (pi, (_out_name, src_idx)) in parent_sources.iter().enumerate() {
            let val = array_value_as_string(batch.column(*src_idx), row_idx);
            for _ in 0..n_elements {
                parent_builders[pi].push(val.clone());
            }
        }
    }

    // Child fields: extract from each element.
    let child_columns = if config.fields.is_empty() {
        // No field extraction — emit raw element as string.
        let mut raw_vals: Vec<Option<String>> = Vec::with_capacity(total_elements);
        for row_elements in &elements {
            for elem in row_elements {
                match elem {
                    serde_json::Value::Null => raw_vals.push(None),
                    serde_json::Value::String(s) => raw_vals.push(Some(s.clone())),
                    other => raw_vals.push(Some(other.to_string())),
                }
            }
        }
        vec![(config.column.clone(), raw_vals)]
    } else {
        config.fields.iter().map(|(out_name, path)| {
            let mut vals: Vec<Option<String>> = Vec::with_capacity(total_elements);
            for row_elements in &elements {
                for elem in row_elements {
                    let extracted = if path.contains('.') {
                        json_path_get(elem, path)
                    } else {
                        elem.get(path).map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                    };
                    vals.push(extracted);
                }
            }
            (out_name.clone(), vals)
        }).collect::<Vec<_>>()
    };

    // ── Assemble output RecordBatch ──────────────────────────────────────
    let mut out_fields: Vec<Field> = Vec::new();
    let mut out_cols: Vec<ArrayRef> = Vec::new();

    // Parent columns first.
    for (pi, (out_name, _src_idx)) in parent_sources.iter().enumerate() {
        out_fields.push(Field::new(*out_name, DataType::Utf8, true));
        let arr: StringArray = parent_builders[pi].iter()
            .map(|v| v.as_deref())
            .collect();
        out_cols.push(Arc::new(arr) as ArrayRef);
    }

    // Child columns.
    for (col_name, vals) in &child_columns {
        out_fields.push(Field::new(col_name, DataType::Utf8, true));
        let arr: StringArray = vals.iter()
            .map(|v| v.as_deref())
            .collect();
        out_cols.push(Arc::new(arr) as ArrayRef);
    }

    let out_schema = Arc::new(Schema::new(out_fields));
    Ok(RecordBatch::try_new(out_schema, out_cols)?)
}

// ── Internal helpers ─────────────────────────────────────────────────────────

/// Extracts array elements for each row.  Returns a Vec (one per row) of
/// Vec<Value> (one per array element).
fn extract_elements(
    col:      &ArrayRef,
    num_rows: usize,
) -> anyhow::Result<Vec<Vec<serde_json::Value>>> {
    match col.data_type() {
        // ── Native List/LargeList ────────────────────────────────────────
        DataType::List(_) => {
            let list_arr = col.as_any().downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow::anyhow!("unnest: failed to downcast to ListArray"))?;
            let mut result = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                if list_arr.is_null(row) {
                    result.push(Vec::new());
                } else {
                    let slice = list_arr.value(row);
                    result.push(arrow_array_to_json_elements(&slice)?);
                }
            }
            Ok(result)
        }
        DataType::LargeList(_) => {
            let list_arr = col.as_any().downcast_ref::<LargeListArray>()
                .ok_or_else(|| anyhow::anyhow!("unnest: failed to downcast to LargeListArray"))?;
            let mut result = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                if list_arr.is_null(row) {
                    result.push(Vec::new());
                } else {
                    let slice = list_arr.value(row);
                    result.push(arrow_array_to_json_elements(&slice)?);
                }
            }
            Ok(result)
        }
        // ── JSON string ──────────────────────────────────────────────────
        DataType::Utf8 | DataType::LargeUtf8 => {
            let str_arr = arrow::compute::cast(col, &DataType::Utf8)
                .map_err(|e| anyhow::anyhow!("unnest: cast to Utf8: {e}"))?;
            let str_arr = str_arr.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("unnest: downcast to StringArray failed"))?;
            let mut result = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                if str_arr.is_null(row) {
                    result.push(Vec::new());
                } else {
                    let s = str_arr.value(row);
                    let parsed: serde_json::Value = serde_json::from_str(s)
                        .map_err(|e| anyhow::anyhow!(
                            "unnest: row {row}: failed to parse JSON array: {e}\n  value: {s}"
                        ))?;
                    match parsed {
                        serde_json::Value::Array(arr) => result.push(arr),
                        serde_json::Value::Null => result.push(Vec::new()),
                        other => anyhow::bail!(
                            "unnest: row {row}: expected JSON array, got {}",
                            json_type_name(&other)
                        ),
                    }
                }
            }
            Ok(result)
        }
        other => anyhow::bail!(
            "unnest: column type {other:?} is not supported. \
             Expected List, LargeList, Utf8, or LargeUtf8."
        ),
    }
}

/// Convert a native Arrow array (from a ListArray element) to JSON values.
fn arrow_array_to_json_elements(arr: &dyn Array) -> anyhow::Result<Vec<serde_json::Value>> {
    match arr.data_type() {
        DataType::Struct(fields) => {
            let struct_arr = arr.as_any().downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow::anyhow!("unnest: downcast struct element"))?;
            let mut elements = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    elements.push(serde_json::Value::Null);
                } else {
                    let mut obj = serde_json::Map::new();
                    for (fi, field) in fields.iter().enumerate() {
                        let val_str = array_value_as_string(struct_arr.column(fi), row);
                        obj.insert(
                            field.name().clone(),
                            val_str.map(serde_json::Value::String)
                                .unwrap_or(serde_json::Value::Null),
                        );
                    }
                    elements.push(serde_json::Value::Object(obj));
                }
            }
            Ok(elements)
        }
        // Scalar elements (strings, numbers, etc.)
        _ => {
            let mut elements = Vec::with_capacity(arr.len());
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    elements.push(serde_json::Value::Null);
                } else {
                    let s = array_value_as_string_ref(arr, row)
                        .unwrap_or_default();
                    elements.push(serde_json::Value::String(s));
                }
            }
            Ok(elements)
        }
    }
}

/// Format a single array cell as an Option<String>.
fn array_value_as_string(col: &ArrayRef, row: usize) -> Option<String> {
    array_value_as_string_ref(col.as_ref(), row)
}

fn array_value_as_string_ref(col: &dyn Array, row: usize) -> Option<String> {
    if col.is_null(row) {
        return None;
    }
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    let opts = FormatOptions::default();
    ArrayFormatter::try_new(col, &opts)
        .ok()
        .map(|fmt| fmt.value(row).to_string())
}

fn json_type_name(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_json_array_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id",           DataType::Int32, false),
            Field::new("name",         DataType::Utf8,  false),
            Field::new("talent_pools", DataType::Utf8,  true),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                r#"[{"id":"tp1","data":"x"},{"id":"tp2","data":"y"}]"#,
                r#"[{"id":"tp3","data":"z"}]"#,
                r#"[]"#,
            ])) as ArrayRef,
        ]).unwrap()
    }

    #[test]
    fn test_unnest_with_fields_and_parent() {
        let batch = make_json_array_batch();
        let config = UnnestConfig {
            column: "talent_pools".into(),
            parent_fields: IndexMap::from([
                ("candidate_id".into(), "id".into()),
            ]),
            fields: IndexMap::from([
                ("talentpool_id".into(), "id".into()),
                ("pool_data".into(),     "data".into()),
            ]),
        };
        let out = apply_unnest(batch, &config).unwrap();
        assert_eq!(out.num_rows(), 3); // 2 + 1 + 0
        assert_eq!(out.num_columns(), 3); // candidate_id, talentpool_id, pool_data

        let cand_ids = out.column_by_name("candidate_id").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(cand_ids.value(0), "1");
        assert_eq!(cand_ids.value(1), "1");
        assert_eq!(cand_ids.value(2), "2");

        let pool_ids = out.column_by_name("talentpool_id").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(pool_ids.value(0), "tp1");
        assert_eq!(pool_ids.value(1), "tp2");
        assert_eq!(pool_ids.value(2), "tp3");
    }

    #[test]
    fn test_unnest_raw_elements() {
        let batch = make_json_array_batch();
        let config = UnnestConfig {
            column: "talent_pools".into(),
            parent_fields: IndexMap::from([
                ("candidate_id".into(), "id".into()),
            ]),
            fields: IndexMap::new(), // no field extraction
        };
        let out = apply_unnest(batch, &config).unwrap();
        assert_eq!(out.num_rows(), 3);
        assert_eq!(out.num_columns(), 2); // candidate_id, talent_pools (raw element)
    }

    #[test]
    fn test_unnest_empty_array_rows_skipped() {
        let batch = make_json_array_batch();
        let config = UnnestConfig {
            column: "talent_pools".into(),
            parent_fields: IndexMap::new(),
            fields: IndexMap::from([("pool_id".into(), "id".into())]),
        };
        let out = apply_unnest(batch, &config).unwrap();
        // Row 3 (Charlie) has empty array → 0 output rows.
        assert_eq!(out.num_rows(), 3); // 2 + 1 + 0
    }

    #[test]
    fn test_unnest_null_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id",   DataType::Int32, false),
            Field::new("tags", DataType::Utf8,  true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ]).unwrap();

        let config = UnnestConfig {
            column: "tags".into(),
            parent_fields: IndexMap::new(),
            fields: IndexMap::new(),
        };
        let out = apply_unnest(batch, &config).unwrap();
        assert_eq!(out.num_rows(), 0); // null array → 0 rows
    }

    #[test]
    fn test_unnest_missing_column_error() {
        let batch = make_json_array_batch();
        let config = UnnestConfig {
            column: "nonexistent".into(),
            parent_fields: IndexMap::new(),
            fields: IndexMap::new(),
        };
        assert!(apply_unnest(batch, &config).is_err());
    }
}