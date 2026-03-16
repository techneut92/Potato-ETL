//! `map` step — add, replace, or derive columns using the expression DSL.
//!
//! Given a `columns: IndexMap<String, String>` (output name → expression string):
//! 1. Each expression is evaluated **in insertion order** against the current batch.
//! 2. The result column is added to (or replaces) the batch.
//! 3. All original columns **not** listed in `columns` are preserved unchanged
//!    unless `select_only: true` is set.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;

use super::expr::{eval, parse};

/// Add / replace / derive columns in `batch` according to `columns`.
pub fn apply_map(
    batch:       RecordBatch,
    columns:     &IndexMap<String, String>,
    select_only: bool,
) -> anyhow::Result<RecordBatch> {
    if columns.is_empty() {
        return Ok(batch);
    }

    let mut current = batch;

    let mut pending: Vec<(&str, &str)> = columns.iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let max_passes = pending.len() + 1;
    let mut pass = 0;

    while !pending.is_empty() {
        pass += 1;
        if pass > max_passes {
            let failed: Vec<_> = pending.iter().map(|(k, _)| *k).collect();
            anyhow::bail!(
                "map: could not resolve expressions for columns after {} passes: {}\n\
                 Hint: check for circular references or typos in column names.",
                max_passes,
                failed.join(", ")
            );
        }

        let mut still_pending: Vec<(&str, &str)> = Vec::new();
        for (out_name, expr_str) in pending {
            let expr = parse(expr_str).map_err(|e| {
                anyhow::anyhow!("map: column '{out_name}': parse error in '{expr_str}': {e}")
            })?;

            match eval(&expr, &current) {
                Ok(arr) => {
                    current = replace_or_add_column(current, out_name, arr)?;
                }
                Err(e) if e.to_string().contains("not found in batch") && pass == 1 => {
                    still_pending.push((out_name, expr_str));
                }
                Err(e) => return Err(e.context(format!(
                    "map: column '{out_name}': eval error in '{expr_str}'"
                ))),
            }
        }
        pending = still_pending;
    }

    if !select_only {
        return Ok(current);
    }

    let schema = current.schema();
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for out_name in columns.keys() {
        match schema.index_of(out_name.as_str()) {
            Ok(idx) => {
                fields.push(schema.field(idx).clone());
                arrays.push(current.column(idx).clone());
            }
            Err(_) => anyhow::bail!(
                "map: select_only column '{out_name}' not found after evaluation — this is a bug"
            ),
        }
    }
    let new_schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(new_schema, arrays)?)
}

/// Replace or add a column in `batch`.
fn replace_or_add_column(
    batch: RecordBatch,
    name:  &str,
    arr:   ArrayRef,
) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let mut fields:  Vec<Field>  = schema.fields().iter().map(|f| (**f).clone()).collect();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

    let nullable = arr.null_count() > 0;

    if let Ok(idx) = schema.index_of(name) {
        let preserved_meta = fields[idx].metadata().clone();
        fields[idx]  = Field::new(name, arr.data_type().clone(), nullable)
            .with_metadata(preserved_meta);
        columns[idx] = arr;
    } else {
        fields.push(Field::new(name, arr.data_type().clone(), nullable));
        columns.push(arr);
    }

    let new_schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(new_schema, columns)?)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Float64, true),
            Field::new("qty",   DataType::Int64,   true),
            Field::new("tag",   DataType::Utf8,    true),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(Float64Array::from(vec![5.0_f64, 10.0])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2_i64, 3]))         as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"]))        as ArrayRef,
        ]).unwrap()
    }

    #[test]
    fn adds_computed_column() {
        let batch = sample_batch();
        let cols: IndexMap<String, String> = [
            ("total".into(), "price * qty".into()),
        ].into_iter().collect();
        let out = apply_map(batch, &cols, false).unwrap();
        assert!(out.schema().index_of("total").is_ok());
        assert!(out.schema().index_of("price").is_ok());
        let f = out.column_by_name("total").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(f.value(0), 10.0);
        assert_eq!(f.value(1), 30.0);
    }

    #[test]
    fn select_only_drops_others_and_preserves_order() {
        let batch = sample_batch();
        let cols: IndexMap<String, String> = [
            ("total".into(),     "price * qty".into()),
            ("tag_upper".into(), "upper(tag)".into()),
        ].into_iter().collect();
        let out = apply_map(batch, &cols, true).unwrap();
        assert_eq!(out.schema().fields().len(), 2);
        assert_eq!(out.schema().field(0).name(), "total");
        assert_eq!(out.schema().field(1).name(), "tag_upper");
    }

    #[test]
    fn replace_preserves_field_metadata() {
        use std::collections::HashMap;
        let mut meta = HashMap::new();
        meta.insert("etl.db_type".to_string(), "NUMERIC(10,2)".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Float64, false).with_metadata(meta.clone()),
            Field::new("qty",   DataType::Int64,   false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Float64Array::from(vec![5.0_f64])) as ArrayRef,
            Arc::new(Int64Array::from(vec![2_i64]))      as ArrayRef,
        ]).unwrap();

        let cols: IndexMap<String, String> = [
            ("price".into(), "price * 1.1".into()),
        ].into_iter().collect();
        let out = apply_map(batch, &cols, false).unwrap();
        let out_schema = out.schema();
        let price_field = out_schema.field_with_name("price").unwrap();
        assert_eq!(
            price_field.metadata().get("etl.db_type").map(|s| s.as_str()),
            Some("NUMERIC(10,2)"),
        );
    }

    #[test]
    fn forward_reference_resolved() {
        let batch = sample_batch();
        let cols: IndexMap<String, String> = [
            ("revenue".into(),          "price * qty".into()),
            ("discount_revenue".into(), "revenue * 0.9".into()),
        ].into_iter().collect();
        let out = apply_map(batch, &cols, false).unwrap();
        let disc = out.column_by_name("discount_revenue").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((disc.value(0) - 9.0).abs() < 0.001);
    }
}