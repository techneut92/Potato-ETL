//! Struct-flattening transform — extract nested fields from Arrow StructArray columns
//! or from JSON string columns.
//!
//! ## When to use this
//!
//! REST APIs often return nested JSON objects.  The `flatten` step lets you
//! extract specific sub-fields into top-level columns using dot-notation paths.
//!
//! ## JSON string columns
//!
//! When the root column is a `Utf8` or `LargeUtf8` string containing JSON,
//! `flatten` automatically switches to JSON extraction mode.  Array indexing
//! and nested dot paths both work.
//!
//! ## Empty `select` — auto-expand
//!
//! When `select` is omitted every `StructArray` column is expanded one level.

use std::sync::Arc;

use indexmap::IndexMap;

use arrow::array::{Array, ArrayRef, StringArray, StructArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::expr::json_path_get;

// ── Public API ────────────────────────────────────────────────────────────────

/// Extracts / renames columns from a `RecordBatch` using dot-notation paths.
pub fn apply_flatten(
    batch:  RecordBatch,
    select: &IndexMap<String, String>,
) -> anyhow::Result<RecordBatch> {
    if select.is_empty() {
        return auto_expand(batch);
    }

    let mut out_fields: Vec<Field>         = Vec::with_capacity(select.len());
    let mut out_cols:   Vec<Arc<dyn Array>> = Vec::with_capacity(select.len());

    for (output_name, path) in select {
        let (col, dt, nullable) = extract_path(&batch, path)?;
        out_fields.push(Field::new(output_name, dt, nullable));
        out_cols.push(col);
    }

    Ok(RecordBatch::try_new(Arc::new(Schema::new(out_fields)), out_cols)?)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn extract_path(
    batch: &RecordBatch,
    path:  &str,
) -> anyhow::Result<(ArrayRef, DataType, bool)> {
    let (root, rest) = match path.find('.') {
        Some(dot) => (&path[..dot], Some(&path[dot + 1..])),
        None      => (path, None),
    };

    let schema  = batch.schema();
    let col_idx = schema.index_of(root).map_err(|_| anyhow::anyhow!(
        "flatten: column '{root}' not found in batch schema (path: '{path}')"
    ))?;
    let root_col   = batch.column(col_idx);
    let root_field = &schema.fields()[col_idx];

    match rest {
        None => Ok((
            root_col.clone(),
            root_field.data_type().clone(),
            root_field.is_nullable(),
        )),
        Some(remainder) => {
            match root_col.data_type() {
                DataType::Struct(_) => {
                    let (col, field) = extract_from_struct(root_col, root_field, remainder, path)?;
                    Ok((col, field.data_type().clone(), field.is_nullable()))
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    extract_from_json(root_col, remainder)
                }
                other => anyhow::bail!(
                    "flatten: cannot traverse into column '{root}' (type: {other:?}) \
                     at path '{path}'; only StructArray or JSON string columns can be traversed"
                ),
            }
        }
    }
}

fn extract_from_json(
    col:       &ArrayRef,
    json_path: &str,
) -> anyhow::Result<(ArrayRef, DataType, bool)> {
    let as_utf8 = cast(col, &DataType::Utf8)
        .map_err(|e| anyhow::anyhow!("flatten: cast JSON column to Utf8: {e}"))?;
    let str_arr = as_utf8.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("flatten: downcast to StringArray failed"))?;

    let result: StringArray = str_arr.iter()
        .map(|v| {
            v.and_then(|s| {
                let val: serde_json::Value = serde_json::from_str(s).ok()?;
                json_path_get(&val, json_path)
            })
        })
        .collect();

    Ok((Arc::new(result) as ArrayRef, DataType::Utf8, true))
}

fn extract_from_struct(
    col:       &ArrayRef,
    field:     &Field,
    remainder: &str,
    full_path: &str,
) -> anyhow::Result<(ArrayRef, Arc<Field>)> {
    let struct_arr = col
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| anyhow::anyhow!(
            "flatten: cannot descend into '{name}' (type: {dt:?}) via path '{full_path}'",
            name = field.name(),
            dt   = col.data_type(),
        ))?;

    let sub_fields = match field.data_type() {
        DataType::Struct(f) => f,
        _ => unreachable!("StructArray must have DataType::Struct"),
    };

    let (next_seg, rest) = match remainder.find('.') {
        Some(dot) => (&remainder[..dot], Some(&remainder[dot + 1..])),
        None      => (remainder, None),
    };

    let sub_idx = sub_fields
        .iter()
        .position(|f| f.name() == next_seg)
        .ok_or_else(|| anyhow::anyhow!(
            "flatten: struct '{parent}' has no field '{next_seg}' (path: '{full_path}')",
            parent = field.name(),
        ))?;

    let sub_col   = struct_arr.column(sub_idx);
    let sub_field = Arc::clone(&sub_fields[sub_idx]);

    match rest {
        None         => Ok((sub_col.clone(), sub_field)),
        Some(deeper) => extract_from_struct(sub_col, &sub_field, deeper, full_path),
    }
}

fn auto_expand(batch: RecordBatch) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let mut out_fields: Vec<Field>          = Vec::new();
    let mut out_cols:   Vec<Arc<dyn Array>> = Vec::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        match field.data_type() {
            DataType::Struct(sub_fields) => {
                let struct_arr = batch
                    .column(col_idx)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("column with DataType::Struct must downcast to StructArray");

                for (sub_idx, sub_field) in sub_fields.iter().enumerate() {
                    let flat_name = format!("{}_{}", field.name(), sub_field.name());
                    out_fields.push(
                        Field::new(&flat_name, sub_field.data_type().clone(), sub_field.is_nullable())
                            .with_metadata(sub_field.metadata().clone())
                    );
                    out_cols.push(struct_arr.column(sub_idx).clone());
                }
            }
            _ => {
                out_fields.push((**field).clone());
                out_cols.push(batch.column(col_idx).clone());
            }
        }
    }

    Ok(RecordBatch::try_new(Arc::new(Schema::new(out_fields)), out_cols)?)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, StructArray as ArrowStructArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema};

    fn make_nested_batch() -> RecordBatch {
        let street_arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["Main St",   "High Rd"]));
        let city_arr:   Arc<dyn Array> = Arc::new(StringArray::from(vec!["Amsterdam", "Utrecht"]));
        let address_fields = Fields::from(vec![
            Field::new("street", DataType::Utf8, true),
            Field::new("city",   DataType::Utf8, true),
        ]);
        let address_arr: Arc<dyn Array> = Arc::new(
            ArrowStructArray::new(address_fields.clone(), vec![street_arr, city_arr], None)
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id",      DataType::Int32,                          false),
            Field::new("address", DataType::Struct(address_fields.clone()),  true),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            address_arr,
        ]).unwrap()
    }

    fn make_json_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id",      DataType::Int32, false),
            Field::new("payload", DataType::Utf8,  true),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                r#"{"user":{"id":"u001","name":"Alice"},"tags":["rust","arrow"]}"#,
                r#"{"user":{"id":"u002","name":"Bob"},  "tags":["python"]}"#,
                r#"invalid json"#,
            ])) as ArrayRef,
        ]).unwrap()
    }

    #[test]
    fn test_select_top_level() {
        let batch  = make_nested_batch();
        let select = IndexMap::from([("emp_id".into(), "id".into())]);
        let out    = apply_flatten(batch, &select).unwrap();
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.schema().field(0).name(), "emp_id");
    }

    #[test]
    fn test_select_nested_struct() {
        let batch  = make_nested_batch();
        let select = IndexMap::from([
            ("id".into(),     "id".into()),
            ("city".into(),   "address.city".into()),
            ("street".into(), "address.street".into()),
        ]);
        let out = apply_flatten(batch, &select).unwrap();
        assert_eq!(out.num_columns(), 3);
        let city_col = out.column_by_name("city").unwrap();
        let city_arr = city_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(city_arr.value(0), "Amsterdam");
    }

    #[test]
    fn test_json_nested_path() {
        let batch  = make_json_batch();
        let select = IndexMap::from([("user_id".into(), "payload.user.id".into())]);
        let out    = apply_flatten(batch, &select).unwrap();
        let arr    = out.column_by_name("user_id").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "u001");
        assert_eq!(arr.value(1), "u002");
        assert!(arr.is_null(2));
    }

    #[test]
    fn test_json_array_index() {
        let batch  = make_json_batch();
        let select = IndexMap::from([("tag_0".into(), "payload.tags[0]".into())]);
        let out = apply_flatten(batch, &select).unwrap();
        let arr = out.column_by_name("tag_0").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "rust");
        assert_eq!(arr.value(1), "python");
    }

    #[test]
    fn test_auto_expand() {
        let batch = make_nested_batch();
        let out   = apply_flatten(batch, &IndexMap::new()).unwrap();
        assert_eq!(out.num_columns(), 3);
        assert!(out.schema().index_of("address_street").is_ok());
        assert!(out.schema().index_of("address_city").is_ok());
    }

    #[test]
    fn test_missing_column_error() {
        let batch  = make_nested_batch();
        let select = IndexMap::from([("x".into(), "nonexistent.field".into())]);
        assert!(apply_flatten(batch, &select).is_err());
    }
}