//! In-memory hash join for Arrow `RecordBatch`es.
//!
//! Strategy:
//!   1. Buffer the RIGHT-side fully (expected to be the smaller, e.g. dimension table).
//!   2. Stream the LEFT-side (the larger fact table); for each row: lookup in the hash map.
//!   3. Output: combined columns of left + right (join key not duplicated).
//!
//! ## NULL key semantics
//!
//! Following SQL semantics, NULL keys never match — not even other NULLs.
//! Rows with NULL join keys are only emitted for `LEFT` and `FULL` joins
//! (with NULLs in the unmatched columns from the other side).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::config::JoinHow;
use crate::util::arrow::json_rows_to_record_batch;

// ── Public streaming API ──────────────────────────────────────────────────────

/// Pre-built hash map of the right (build) side of a join.
///
/// Keys are non-NULL string representations of the join column.  NULL keys
/// are stored separately in `null_rows` for FULL join emission.
pub struct RightHashMap {
    /// Non-NULL key → right-side rows.
    map: HashMap<String, Vec<serde_json::Value>>,
    /// Right-side rows whose join key was NULL (never matched, but emitted
    /// in FULL joins).
    null_rows: Vec<serde_json::Value>,
    key: String,
    /// Number of right-side columns (excluding the join key).  Used to
    /// generate NULL-padded right columns for unmatched left rows.
    right_non_key_fields: Vec<String>,
    /// Keys that were matched by at least one left-side row.  Only tracked
    /// when `how == Full` so that unmatched right rows can be emitted.
    matched_keys: Option<std::sync::Mutex<HashSet<String>>>,
}

// RightHashMap fields are all Send+Sync-safe types (HashMap, Vec, String,
// Mutex<HashSet>).  No unsafe impl needed.

/// Builds the right-side hash map from a fully-materialised slice of batches.
pub fn build_right_hash_map(
    batches: &[RecordBatch],
    key:     &str,
    how:     &JoinHow,
) -> anyhow::Result<RightHashMap> {
    let right_non_key_fields: Vec<String> = batches.first()
        .map(|b| b.schema().fields().iter()
            .filter(|f| f.name() != key)
            .map(|f| f.name().clone())
            .collect())
        .unwrap_or_default();

    let (map, null_rows) = build_right_map(batches, key)?;

    let matched_keys = if *how == JoinHow::Full {
        Some(std::sync::Mutex::new(HashSet::new()))
    } else {
        None
    };

    Ok(RightHashMap { map, null_rows, key: key.to_string(), right_non_key_fields, matched_keys })
}

/// Probes one left-side batch against a pre-built [`RightHashMap`].
pub fn probe_left_batch(
    batch:     RecordBatch,
    right_map: &RightHashMap,
    how:       &JoinHow,
) -> anyhow::Result<Option<RecordBatch>> {
    if batch.num_rows() == 0 { return Ok(None); }

    let key     = &right_map.key;
    let key_col = batch.column_by_name(key)
        .ok_or_else(|| anyhow::anyhow!("Join key '{key}' not found in left batch"))?;
    let key_str = cast(key_col, &DataType::Utf8)?;
    let key_arr = key_str.as_any().downcast_ref::<StringArray>().unwrap();

    let left_rows  = batch_to_json(&batch);
    let mut output = Vec::new();

    for (i, left_row) in left_rows.iter().enumerate() {
        // NULL keys never match (SQL semantics).
        if key_arr.is_null(i) {
            if *how == JoinHow::Left || *how == JoinHow::Full {
                // Emit left row with NULLs for right-side columns.
                let padded = pad_right_nulls(left_row.clone(), &right_map.right_non_key_fields);
                output.push(padded);
            }
            continue;
        }

        let k = key_arr.value(i);
        match right_map.map.get(k) {
            Some(right_rows) => {
                // Track matched key for FULL join unmatched-right emission.
                if let Some(ref matched) = right_map.matched_keys {
                    matched.lock().unwrap().insert(k.to_string());
                }
                for right_row in right_rows {
                    let mut merged = left_row.clone();
                    if let (serde_json::Value::Object(m), serde_json::Value::Object(r)) =
                        (&mut merged, right_row)
                    {
                        for (k2, v) in r {
                            if k2 != key { m.insert(k2.clone(), v.clone()); }
                        }
                    }
                    output.push(merged);
                }
            }
            None if *how == JoinHow::Left || *how == JoinHow::Full => {
                let padded = pad_right_nulls(left_row.clone(), &right_map.right_non_key_fields);
                output.push(padded);
            }
            None => {}
        }
    }

    if output.is_empty() { return Ok(None); }
    Ok(Some(json_rows_to_record_batch(&output)))
}

/// Emit unmatched right-side rows for a FULL join.
///
/// Must be called AFTER all left-side batches have been probed.
/// Returns `None` if there are no unmatched right rows, or if the join
/// type is not `Full`.
pub fn emit_unmatched_right(
    right_map:   &RightHashMap,
    left_schema: &Schema,
    key:         &str,
) -> anyhow::Result<Option<RecordBatch>> {
    let matched = match &right_map.matched_keys {
        Some(m) => m.lock().unwrap().clone(),
        None => return Ok(None), // Not a FULL join.
    };

    let left_non_key_fields: Vec<String> = left_schema.fields().iter()
        .filter(|f| f.name() != key)
        .map(|f| f.name().clone())
        .collect();

    let mut output = Vec::new();

    // Emit right rows whose key was never matched.
    for (k, rows) in &right_map.map {
        if !matched.contains(k) {
            for row in rows {
                let padded = pad_left_nulls(row.clone(), &left_non_key_fields);
                output.push(padded);
            }
        }
    }

    // Emit right rows with NULL keys (never matchable).
    for row in &right_map.null_rows {
        let padded = pad_left_nulls(row.clone(), &left_non_key_fields);
        output.push(padded);
    }

    if output.is_empty() { return Ok(None); }
    Ok(Some(json_rows_to_record_batch(&output)))
}

// ── Batch API (kept for backward compat / tests) ──────────────────────────────

pub fn hash_join(
    left:  &[RecordBatch],
    right: &[RecordBatch],
    key:   &str,
    how:   &JoinHow,
) -> anyhow::Result<Vec<RecordBatch>> {
    if left.is_empty() || right.is_empty() {
        if *how == JoinHow::Inner { return Ok(vec![]); }
    }
    let right_map = build_right_hash_map(right, key, how)?;
    let left_schema  = match left.first()  { Some(b) => b.schema(), None => return Ok(vec![]) };
    let right_schema = match right.first() { Some(b) => b.schema(), None => return Ok(vec![]) };
    let _ = merged_schema(&left_schema, &right_schema, key)?;
    let mut results = Vec::new();
    for batch in left {
        if let Some(out) = probe_left_batch(batch.clone(), &right_map, how)? {
            results.push(out);
        }
    }
    // FULL join: emit unmatched right-side rows.
    if *how == JoinHow::Full {
        if let Some(unmatched) = emit_unmatched_right(&right_map, &left_schema, key)? {
            results.push(unmatched);
        }
    }
    Ok(results)
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn build_right_map(
    batches: &[RecordBatch],
    key:     &str,
) -> anyhow::Result<(HashMap<String, Vec<serde_json::Value>>, Vec<serde_json::Value>)> {
    let mut map: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    let mut null_rows: Vec<serde_json::Value> = Vec::new();
    for batch in batches {
        let key_col = batch.column_by_name(key)
            .ok_or_else(|| anyhow::anyhow!("Join key '{key}' not found in right side"))?;
        let key_str = cast(key_col, &DataType::Utf8)?;
        let key_arr = key_str.as_any().downcast_ref::<StringArray>().unwrap();
        let rows    = batch_to_json(batch);
        for (i, row) in rows.into_iter().enumerate() {
            if key_arr.is_null(i) {
                null_rows.push(row);
            } else {
                let k = key_arr.value(i).to_string();
                map.entry(k).or_default().push(row);
            }
        }
    }
    Ok((map, null_rows))
}

/// Pad a left-side JSON row with NULL values for all right-side non-key columns.
fn pad_right_nulls(mut row: serde_json::Value, right_fields: &[String]) -> serde_json::Value {
    if let serde_json::Value::Object(ref mut m) = row {
        for field in right_fields {
            m.entry(field.clone()).or_insert(serde_json::Value::Null);
        }
    }
    row
}

/// Pad a right-side JSON row with NULL values for all left-side non-key columns.
fn pad_left_nulls(mut row: serde_json::Value, left_fields: &[String]) -> serde_json::Value {
    if let serde_json::Value::Object(ref mut m) = row {
        for field in left_fields {
            m.entry(field.clone()).or_insert(serde_json::Value::Null);
        }
    }
    row
}

fn batch_to_json(batch: &RecordBatch) -> Vec<serde_json::Value> {
    use arrow::array::*;
    (0..batch.num_rows()).map(|i| {
        let mut map = serde_json::Map::new();
        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let val = if col.is_null(i) {
                serde_json::Value::Null
            } else {
                col_to_json_value(col.as_ref(), i)
            };
            map.insert(field.name().clone(), val);
        }
        serde_json::Value::Object(map)
    }).collect()
}

fn col_to_json_value(col: &dyn Array, i: usize) -> serde_json::Value {
    use arrow::array::*;
    use serde_json::json;
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>()  { return json!(a.value(i)); }
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>()  { return json!(a.value(i)); }
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>(){ return json!(a.value(i)); }
    if let Some(a) = col.as_any().downcast_ref::<Float32Array>(){ return json!(a.value(i)); }
    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() { return json!(a.value(i)); }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() { return json!(a.value(i)); }
    if let Ok(s) = cast(col, &DataType::Utf8) {
        if let Some(a) = s.as_any().downcast_ref::<StringArray>() {
            return json!(a.value(i));
        }
    }
    serde_json::Value::Null
}

fn merged_schema(
    left:  &Schema,
    right: &Schema,
    key:   &str,
) -> anyhow::Result<Arc<Schema>> {
    let mut fields: Vec<Field> = left.fields().iter().map(|f| (**f).clone()).collect();
    for f in right.fields() {
        if f.name() != key {
            fields.push((**f).clone());
        }
    }
    Ok(Arc::new(Schema::new(fields)))
}
