//! `aggregate` step — group-by + metric aggregation over Arrow `RecordBatch`es.
//!
//! The aggregate step **materialises** all incoming batches before computing
//! the result.  Output is a **single** `RecordBatch` with one row per unique
//! group key.

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

// ── Metric descriptor ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Metric {
    pub output_col: String,
    pub func:       AggFunc,
    pub arg:        Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc { Sum, Count, CountCol, Min, Max, Avg, First }

impl Metric {
    pub fn parse(output_col: &str, expr_str: &str) -> anyhow::Result<Self> {
        let s = expr_str.trim();
        let (func_name, arg) = if let Some(paren) = s.find('(') {
            let name = s[..paren].trim().to_lowercase();
            let inner = s[paren + 1..s.rfind(')').unwrap_or(s.len())].trim();
            let arg = if inner.is_empty() { None } else { Some(inner.to_string()) };
            (name, arg)
        } else {
            anyhow::bail!("metric '{}': expected function call like 'sum(col)', got '{s}'", output_col);
        };

        let func = match func_name.as_str() {
            "sum"                 => AggFunc::Sum,
            "count" if arg.is_none() => AggFunc::Count,
            "count"               => AggFunc::CountCol,
            "min"                 => AggFunc::Min,
            "max"                 => AggFunc::Max,
            "avg" | "mean"        => AggFunc::Avg,
            "first"               => AggFunc::First,
            other => anyhow::bail!("unknown aggregate function '{other}()'"),
        };

        Ok(Metric { output_col: output_col.to_string(), func, arg })
    }
}

// ── Group key representation ──────────────────────────────────────────────────

/// Represents a single value in a composite group key.
///
/// Distinguishes NULL from empty string — critical for correct SQL-like
/// grouping semantics where NULL ≠ "".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum KeyPart {
    Null,
    Value(String),
}

/// A composite group key: one `KeyPart` per group_by column.
type GroupKey = Vec<KeyPart>;

fn group_key_from_row(key_arrs: &[StringArray], row: usize) -> GroupKey {
    key_arrs.iter()
        .map(|arr| {
            if arr.is_null(row) {
                KeyPart::Null
            } else {
                KeyPart::Value(arr.value(row).to_string())
            }
        })
        .collect()
}

// ── Public entry point ────────────────────────────────────────────────────────

pub fn apply_aggregate(
    batches:  Vec<RecordBatch>,
    group_by: &[String],
    metrics:  &IndexMap<String, String>,
) -> anyhow::Result<RecordBatch> {
    if batches.is_empty() { return empty_result(group_by, metrics); }

    let all = concat_batches(&batches)?;

    let metric_defs: Vec<Metric> = metrics.iter()
        .map(|(out, expr)| Metric::parse(out, expr))
        .collect::<anyhow::Result<_>>()?;

    if group_by.is_empty() {
        let all_indices: Vec<usize> = (0..all.num_rows()).collect();
        return compute_agg_row(&all, &[], &[], &metric_defs, &all_indices);
    }

    let key_arrs: Vec<StringArray> = group_by.iter()
        .map(|col| {
            let arr = all.column_by_name(col)
                .ok_or_else(|| anyhow::anyhow!("aggregate group_by column '{col}' not found"))?;
            let s = cast(arr, &DataType::Utf8)?;
            s.as_any().downcast_ref::<StringArray>()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("cast to StringArray failed"))
        })
        .collect::<anyhow::Result<_>>()?;

    // Build groups using proper NULL-aware keys.
    let mut group_keys: Vec<GroupKey>                = Vec::new();
    let mut group_rows: HashMap<GroupKey, Vec<usize>> = HashMap::new();

    for row in 0..all.num_rows() {
        let key = group_key_from_row(&key_arrs, row);
        group_rows.entry(key.clone()).or_insert_with(|| { group_keys.push(key.clone()); Vec::new() }).push(row);
    }

    let group_batches: Vec<RecordBatch> = group_keys.iter()
        .map(|key| {
            let rows = &group_rows[key];
            compute_agg_row(&all, group_by, key, &metric_defs, rows)
        })
        .collect::<anyhow::Result<_>>()?;

    concat_batches(&group_batches)
}

// ── Compute one output row for a group ────────────────────────────────────────

fn compute_agg_row(
    all:        &RecordBatch,
    group_cols: &[String],
    key:        &[KeyPart],
    metrics:    &[Metric],
    row_idx:    &[usize],
) -> anyhow::Result<RecordBatch> {
    let mut fields:  Vec<Field>    = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for (col, part) in group_cols.iter().zip(key.iter()) {
        let arr = all.column_by_name(col).unwrap();
        fields.push(Field::new(col, arr.data_type().clone(), true));
        match part {
            KeyPart::Null => {
                // Produce a single-element null array of the correct type.
                let null_arr = arrow::array::new_null_array(arr.data_type(), 1);
                columns.push(null_arr);
            }
            KeyPart::Value(val) => {
                let str_arr: ArrayRef = Arc::new(StringArray::from(vec![val.as_str()]));
                let casted = cast(&str_arr, arr.data_type()).unwrap_or(str_arr);
                columns.push(casted);
            }
        }
    }

    for m in metrics {
        let (field, arr) = compute_metric(all, m, row_idx)?;
        fields.push(field);
        columns.push(arr);
    }

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn compute_metric(
    all:     &RecordBatch,
    metric:  &Metric,
    row_idx: &[usize],
) -> anyhow::Result<(Field, ArrayRef)> {
    let n_rows = row_idx.len();

    match metric.func {
        AggFunc::Count => {
            let arr: Int64Array = [Some(n_rows as i64)].into_iter().collect();
            Ok((Field::new(&metric.output_col, DataType::Int64, false), Arc::new(arr)))
        }
        AggFunc::CountCol => {
            let col = metric.arg.as_deref()
                .ok_or_else(|| anyhow::anyhow!("count(col) requires a column name"))?;
            let arr = get_col(all, col)?;
            let non_null = row_idx.iter().filter(|&&i| arr.is_valid(i)).count() as i64;
            let result: Int64Array = [Some(non_null)].into_iter().collect();
            Ok((Field::new(&metric.output_col, DataType::Int64, false), Arc::new(result)))
        }
        AggFunc::Sum => {
            let col = require_arg(metric)?;
            let arr = get_col_float(all, col, row_idx)?;
            let sum: f64 = arr.iter().filter_map(|v| v).sum();
            let result: Float64Array = [Some(sum)].into_iter().collect();
            Ok((Field::new(&metric.output_col, DataType::Float64, false), Arc::new(result)))
        }
        AggFunc::Avg => {
            let col = require_arg(metric)?;
            let arr = get_col_float(all, col, row_idx)?;
            let vals: Vec<f64> = arr.iter().filter_map(|v| v).collect();
            let avg = if vals.is_empty() { None } else { Some(vals.iter().sum::<f64>() / vals.len() as f64) };
            let result: Float64Array = [avg].into_iter().collect();
            Ok((Field::new(&metric.output_col, DataType::Float64, true), Arc::new(result)))
        }
        AggFunc::Min => {
            let col = require_arg(metric)?;
            let arr = get_col(all, col)?;
            let (min_val, dt) = agg_min_max(arr, row_idx, false)?;
            Ok((Field::new(&metric.output_col, dt, true), min_val))
        }
        AggFunc::Max => {
            let col = require_arg(metric)?;
            let arr = get_col(all, col)?;
            let (max_val, dt) = agg_min_max(arr, row_idx, true)?;
            Ok((Field::new(&metric.output_col, dt, true), max_val))
        }
        AggFunc::First => {
            let col = require_arg(metric)?;
            let arr = get_col(all, col)?;
            let first_idx = row_idx.iter().find(|&&i| arr.is_valid(i));
            // Preserve the original column type instead of always returning Utf8.
            let original_dt = arr.data_type().clone();
            if let Some(&idx) = first_idx {
                // Slice a single element from the original array to preserve type.
                let single = arr.slice(idx, 1);
                Ok((Field::new(&metric.output_col, original_dt, true), single))
            } else {
                // All values are NULL — return a typed null array.
                let null_arr = arrow::array::new_null_array(&original_dt, 1);
                Ok((Field::new(&metric.output_col, original_dt, true), null_arr))
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn require_arg(m: &Metric) -> anyhow::Result<&str> {
    m.arg.as_deref().ok_or_else(|| anyhow::anyhow!(
        "aggregate: metric '{}': function '{:?}' requires a column argument",
        m.output_col, m.func,
    ))
}

fn get_col<'a>(batch: &'a RecordBatch, col: &str) -> anyhow::Result<&'a ArrayRef> {
    batch.column_by_name(col)
        .ok_or_else(|| anyhow::anyhow!("aggregate: column '{col}' not found in batch"))
}

fn get_col_float(batch: &RecordBatch, col: &str, row_idx: &[usize]) -> anyhow::Result<Float64Array> {
    let arr = get_col(batch, col)?;
    let f64_arr = cast(arr, &DataType::Float64)
        .map_err(|e| anyhow::anyhow!("aggregate: cannot cast '{col}' to Float64: {e}"))?;
    let f64_arr = f64_arr.as_any().downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("downcast to Float64Array failed"))?;
    let result: Float64Array = row_idx.iter()
        .map(|&i| if f64_arr.is_valid(i) { Some(f64_arr.value(i)) } else { None })
        .collect();
    Ok(result)
}

fn agg_min_max(arr: &ArrayRef, row_idx: &[usize], want_max: bool) -> anyhow::Result<(ArrayRef, DataType)> {
    // For Int64 columns, compare natively to avoid Float64 precision loss (>2^53).
    if let Some(i64_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        let mut best: Option<i64> = None;
        for &i in row_idx {
            if i64_arr.is_null(i) { continue; }
            let v = i64_arr.value(i);
            best = Some(match best {
                None      => v,
                Some(cur) => if want_max { cur.max(v) } else { cur.min(v) },
            });
        }
        let result: Int64Array = [best].into_iter().collect();
        return Ok((Arc::new(result) as ArrayRef, DataType::Int64));
    }

    let use_numeric = matches!(arr.data_type(),
        DataType::Int8   | DataType::Int16  | DataType::Int32  |
        DataType::UInt8  | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        DataType::Float32 | DataType::Float64 |
        DataType::Date32 | DataType::Date64 |
        DataType::Timestamp(_, _) | DataType::Time32(_) | DataType::Time64(_) | DataType::Duration(_)
    );

    if use_numeric {
        if let Ok(casted) = cast(arr, &DataType::Float64) {
            if let Some(f64_arr) = casted.as_any().downcast_ref::<Float64Array>() {
                let mut best: Option<f64> = None;
                for &i in row_idx {
                    if f64_arr.is_null(i) { continue; }
                    let v = f64_arr.value(i);
                    best = Some(match best {
                        None      => v,
                        Some(cur) => if want_max { cur.max(v) } else { cur.min(v) },
                    });
                }
                let result_f64: Float64Array = [best].into_iter().collect();
                let result = cast(&(Arc::new(result_f64) as ArrayRef), arr.data_type())
                    .unwrap_or_else(|_| {
                        let v: Float64Array = [best].into_iter().collect();
                        Arc::new(v)
                    });
                return Ok((result, arr.data_type().clone()));
            }
        }
    }

    let str_arr = cast(arr, &DataType::Utf8)?;
    let str_arr = str_arr.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("agg_min_max: downcast to StringArray failed"))?;
    let mut best: Option<String> = None;
    for &i in row_idx {
        if str_arr.is_null(i) { continue; }
        let v = str_arr.value(i).to_string();
        best = Some(match best {
            None => v,
            Some(cur) => if want_max {
                if v > cur { v } else { cur }
            } else {
                if v < cur { v } else { cur }
            },
        });
    }
    let result: StringArray = [best.as_deref()].into_iter().collect();
    Ok((Arc::new(result) as ArrayRef, DataType::Utf8))
}

fn concat_batches(batches: &[RecordBatch]) -> anyhow::Result<RecordBatch> {
    anyhow::ensure!(!batches.is_empty(), "concat_batches: empty input");
    let schema = batches[0].schema();
    let cols: Vec<Vec<ArrayRef>> = (0..schema.fields().len())
        .map(|i| batches.iter().map(|b| b.column(i).clone()).collect())
        .collect();
    let merged: Vec<ArrayRef> = cols.iter()
        .map(|col_slices| {
            let refs: Vec<&dyn Array> = col_slices.iter().map(|a| a.as_ref()).collect();
            arrow::compute::concat(&refs).map_err(|e| anyhow::anyhow!("concat: {e}"))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(RecordBatch::try_new(schema, merged)?)
}

fn empty_result(group_by: &[String], metrics: &IndexMap<String, String>) -> anyhow::Result<RecordBatch> {
    let mut fields = Vec::new();
    for col in group_by { fields.push(Field::new(col, DataType::Utf8, true)); }
    for out_col in metrics.keys() { fields.push(Field::new(out_col, DataType::Utf8, true)); }
    let schema = Arc::new(Schema::new(fields));
    let empty_arrs: Vec<ArrayRef> = schema.fields().iter()
        .map(|_| Arc::new(StringArray::from(Vec::<Option<&str>>::new())) as ArrayRef)
        .collect();
    Ok(RecordBatch::try_new(schema, empty_arrs)?)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_orders() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8,    true),
            Field::new("total",       DataType::Float64, true),
        ]));
        let b1 = RecordBatch::try_new(schema.clone(), vec![
            Arc::new(StringArray::from(vec!["A", "B", "A"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![10.0_f64, 5.0, 20.0])) as ArrayRef,
        ]).unwrap();
        let b2 = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["B", "A"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![15.0_f64, 5.0])) as ArrayRef,
        ]).unwrap();
        vec![b1, b2]
    }

    #[test]
    fn group_by_sum() {
        let batches = make_orders();
        let group_by = vec!["customer_id".to_string()];
        let mut metrics = IndexMap::new();
        metrics.insert("total_sales".to_string(), "sum(total)".to_string());
        metrics.insert("order_count".to_string(), "count()".to_string());
        let result = apply_aggregate(batches, &group_by, &metrics).unwrap();
        assert_eq!(result.num_rows(), 2);
        let cust = result.column_by_name("customer_id").unwrap()
            .as_any().downcast_ref::<StringArray>().unwrap();
        let sales = result.column_by_name("total_sales").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        for row in 0..result.num_rows() {
            match cust.value(row) {
                "A" => assert_eq!(sales.value(row), 35.0),
                "B" => assert_eq!(sales.value(row), 20.0),
                other => panic!("unexpected customer {other}"),
            }
        }
    }

    #[test]
    fn global_count() {
        let batches = make_orders();
        let metrics: IndexMap<String, String> = [("n".to_string(), "count()".to_string())].into_iter().collect();
        let result = apply_aggregate(batches, &[], &metrics).unwrap();
        assert_eq!(result.num_rows(), 1);
        let n = result.column_by_name("n").unwrap()
            .as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(n.value(0), 5);
    }

    #[test]
    fn first_preserves_type() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["A", "A"])) as ArrayRef,
            Arc::new(Float64Array::from(vec![42.5, 99.0])) as ArrayRef,
        ]).unwrap();
        let mut metrics = IndexMap::new();
        metrics.insert("first_val".to_string(), "first(value)".to_string());
        let result = apply_aggregate(vec![batch], &["group".to_string()], &metrics).unwrap();
        // first() should preserve Float64 type, not convert to Utf8.
        assert_eq!(result.column_by_name("first_val").unwrap().data_type(), &DataType::Float64);
        let arr = result.column_by_name("first_val").unwrap()
            .as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 42.5);
    }

    #[test]
    fn null_and_empty_string_are_separate_groups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec![Some(""), None, Some(""), None])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        ]).unwrap();
        let mut metrics = IndexMap::new();
        metrics.insert("n".to_string(), "count()".to_string());
        let result = apply_aggregate(vec![batch], &["key".to_string()], &metrics).unwrap();
        // Should have 2 groups: "" (count=2) and NULL (count=2), not 1 merged group.
        assert_eq!(result.num_rows(), 2);
    }
}
