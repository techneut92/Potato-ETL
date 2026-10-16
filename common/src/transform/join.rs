//! In-memory hash join for Arrow `RecordBatch`es.
//!
//! Strategy:
//!   1. Buffer the RIGHT-side fully (expected to be the smaller, e.g. dimension table).
//!   2. Stream the LEFT-side (the larger fact table); for each row: lookup in the hash map.
//!   3. Output: combined columns of left + right (join key not duplicated).
//!
//! Output columns are gathered from the input arrays with Arrow's `take`, so
//! every column keeps its type: a batch without a single match still gets the
//! right side's types (all NULL), and timestamps stay timestamps. The output
//! schema is the left schema followed by the right side's non-key columns; it
//! is the same for every batch of one join. A right column whose name is also
//! a left column replaces the left value on matched rows.
//!
//! ## NULL key semantics
//!
//! Following SQL semantics, NULL keys never match — not even other NULLs.
//! Rows with NULL join keys are only emitted for `LEFT` and `FULL` joins
//! (with NULLs in the unmatched columns from the other side).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{new_null_array, Array, ArrayRef, BooleanArray, StringArray, UInt32Array};
use arrow::compute::{cast, concat_batches, take};
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::config::JoinHow;

// ── Public streaming API ──────────────────────────────────────────────────────

/// Pre-built hash map of the right (build) side of a join.
///
/// The right side is concatenated into one batch; the map points from each
/// non-NULL key (as a string) to its row numbers in that batch.
pub struct RightHashMap {
    /// All right-side rows, or `None` when the right side was empty.
    right: Option<RecordBatch>,
    /// Non-NULL key → right-side row numbers.
    map: HashMap<String, Vec<u32>>,
    /// Right-side rows whose join key was NULL (never matched, but emitted
    /// in FULL joins).
    null_rows: Vec<u32>,
    key: String,
    /// Keys that were matched by at least one left-side row.  Only tracked
    /// when `how == Full` so that unmatched right rows can be emitted.
    matched_keys: Option<std::sync::Mutex<HashSet<String>>>,
}

/// Builds the right-side hash map from a fully-materialised slice of batches.
pub fn build_right_hash_map(
    batches: &[RecordBatch],
    key:     &str,
    how:     &JoinHow,
) -> anyhow::Result<RightHashMap> {
    let right = match batches.first() {
        Some(first) => Some(
            concat_batches(&first.schema(), batches)
                .map_err(|e| anyhow::anyhow!("join: right-side batches do not share one schema: {e}"))?,
        ),
        None => None,
    };

    let mut map: HashMap<String, Vec<u32>> = HashMap::new();
    let mut null_rows = Vec::new();
    if let Some(right) = &right {
        let keys = key_strings(right, key, "right side")?;
        for i in 0..keys.len() {
            if keys.is_null(i) {
                null_rows.push(i as u32);
            } else {
                map.entry(keys.value(i).to_string()).or_default().push(i as u32);
            }
        }
    }

    let matched_keys = if *how == JoinHow::Full {
        Some(std::sync::Mutex::new(HashSet::new()))
    } else {
        None
    };

    Ok(RightHashMap { right, map, null_rows, key: key.to_string(), matched_keys })
}

/// Probes one left-side batch against a pre-built [`RightHashMap`].
pub fn probe_left_batch(
    batch:     RecordBatch,
    right_map: &RightHashMap,
    how:       &JoinHow,
) -> anyhow::Result<Option<RecordBatch>> {
    if batch.num_rows() == 0 { return Ok(None); }

    let keys = key_strings(&batch, &right_map.key, "left batch")?;
    let keep_unmatched = *how == JoinHow::Left || *how == JoinHow::Full;

    let mut left_idx: Vec<u32> = Vec::new();
    let mut right_idx: Vec<Option<u32>> = Vec::new();
    for i in 0..batch.num_rows() {
        // NULL keys never match (SQL semantics).
        let matches = if keys.is_null(i) { None } else { right_map.map.get(keys.value(i)) };
        match matches {
            Some(rows) => {
                // Track matched key for FULL join unmatched-right emission.
                if let Some(ref matched) = right_map.matched_keys {
                    matched.lock().unwrap().insert(keys.value(i).to_string());
                }
                for &r in rows {
                    left_idx.push(i as u32);
                    right_idx.push(Some(r));
                }
            }
            None if keep_unmatched => {
                left_idx.push(i as u32);
                right_idx.push(None);
            }
            None => {}
        }
    }
    if left_idx.is_empty() { return Ok(None); }

    let left_idx  = UInt32Array::from(left_idx);
    let right_idx = UInt32Array::from(right_idx);
    let schema = output_schema(&batch.schema(), right_map, how);

    let mut columns: Vec<ArrayRef> = batch.columns().iter()
        .map(|c| take(c.as_ref(), &left_idx, None))
        .collect::<Result<_, _>>()?;
    if let Some(right) = &right_map.right {
        let matched = BooleanArray::from_iter(right_idx.iter().map(|r| Some(r.is_some())));
        for (field, col) in right.schema().fields().iter().zip(right.columns()) {
            if field.name() == &right_map.key { continue; }
            let taken = take(col.as_ref(), &right_idx, None)?;
            match batch.schema().index_of(field.name()) {
                // Same name on both sides: the right value wins where a row matched.
                Ok(pos) => {
                    let right_vals = cast(&taken, columns[pos].data_type())?;
                    columns[pos] = zip(&matched, &right_vals, &columns[pos])?;
                }
                Err(_) => columns.push(taken),
            }
        }
    }
    Ok(Some(RecordBatch::try_new(schema, columns)?))
}

/// Emit unmatched right-side rows for a FULL join.
///
/// Must be called AFTER all left-side batches have been probed. `out_schema`
/// is the schema of the probed batches; the result uses it unchanged: the key
/// comes from the right side, the other left columns are NULL.
/// Returns `None` if there are no unmatched right rows, or if the join
/// type is not `Full`. The key name is taken from `right_map`; `_key` stays
/// for callers.
pub fn emit_unmatched_right(
    right_map:  &RightHashMap,
    out_schema: &Schema,
    _key:       &str,
) -> anyhow::Result<Option<RecordBatch>> {
    let matched = match &right_map.matched_keys {
        Some(m) => m.lock().unwrap().clone(),
        None => return Ok(None), // Not a FULL join.
    };
    let Some(right) = &right_map.right else { return Ok(None) };

    // Right rows whose key was never matched, then those with a NULL key.
    let mut rows: Vec<u32> = right_map.map.iter()
        .filter(|(k, _)| !matched.contains(*k))
        .flat_map(|(_, r)| r.iter().copied())
        .collect();
    rows.sort_unstable();
    rows.extend(&right_map.null_rows);
    if rows.is_empty() { return Ok(None); }
    let idx = UInt32Array::from(rows);

    let columns: Vec<ArrayRef> = out_schema.fields().iter()
        .map(|field| match right.column_by_name(field.name()) {
            // The key and every right column come from the right side.
            Some(col) => Ok(cast(&take(col.as_ref(), &idx, None)?, field.data_type())?),
            None => Ok(new_null_array(field.data_type(), idx.len())),
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(Some(RecordBatch::try_new(Arc::new(out_schema.clone()), columns)?))
}

// ── Batch API (kept for backward compat / tests) ──────────────────────────────

pub fn hash_join(
    left:  &[RecordBatch],
    right: &[RecordBatch],
    key:   &str,
    how:   &JoinHow,
) -> anyhow::Result<Vec<RecordBatch>> {
    if (left.is_empty() || right.is_empty()) && *how == JoinHow::Inner {
        return Ok(vec![]);
    }
    let right_map = build_right_hash_map(right, key, how)?;
    if left.is_empty() { return Ok(vec![]); }
    let mut results: Vec<RecordBatch> = Vec::new();
    for batch in left {
        if let Some(out) = probe_left_batch(batch.clone(), &right_map, how)? {
            results.push(out);
        }
    }
    // FULL join: emit unmatched right-side rows.
    if *how == JoinHow::Full {
        let out_schema = output_schema(&left[0].schema(), &right_map, how);
        if let Some(unmatched) = emit_unmatched_right(&right_map, &out_schema, key)? {
            results.push(unmatched);
        }
    }
    Ok(results)
}

// ── Private helpers ───────────────────────────────────────────────────────────

/// The join key of `batch` as strings, so keys of different types can match.
fn key_strings(batch: &RecordBatch, key: &str, side: &str) -> anyhow::Result<StringArray> {
    let col = batch.column_by_name(key)
        .ok_or_else(|| anyhow::anyhow!("Join key '{key}' not found in {side}"))?;
    let strings = cast(col, &DataType::Utf8)?;
    Ok(strings.as_any().downcast_ref::<StringArray>().unwrap().clone())
}

/// Left fields, then the right side's non-key fields that the left side does
/// not have. Fields that can be NULL-padded are nullable: the right ones for
/// LEFT and FULL joins, the left ones for FULL joins, and a left field that a
/// nullable right field of the same name overwrites.
fn output_schema(left: &Schema, right_map: &RightHashMap, how: &JoinHow) -> SchemaRef {
    let full = *how == JoinHow::Full;
    let right_schema = right_map.right.as_ref().map(|r| r.schema());
    let overwritten_by_null = |name: &str| right_schema.as_ref()
        .and_then(|s| s.field_with_name(name).ok())
        .is_some_and(|f| f.is_nullable() && name != right_map.key);
    let mut fields: Vec<Field> = left.fields().iter()
        .map(|f| f.as_ref().clone().with_nullable(f.is_nullable() || full || overwritten_by_null(f.name())))
        .collect();
    if let Some(right) = &right_map.right {
        for f in right.schema().fields() {
            if f.name() != &right_map.key && left.index_of(f.name()).is_err() {
                fields.push(f.as_ref().clone().with_nullable(f.is_nullable() || *how != JoinHow::Inner));
            }
        }
    }
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampMicrosecondArray};

    fn left_batch(keys: Vec<Option<&str>>, start: i64) -> RecordBatch {
        let n = keys.len() as i64;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("ts", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None), true),
            Field::new("v", DataType::Int64, true),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(TimestampMicrosecondArray::from((start..start + n).collect::<Vec<_>>())),
            Arc::new(Int64Array::from((start..start + n).collect::<Vec<_>>())),
        ]).unwrap()
    }

    fn right_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("flag", DataType::Boolean, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["a", "b", "b", "z"])),
            Arc::new(BooleanArray::from(vec![true, true, false, true])),
            Arc::new(Int64Array::from(vec![100, 200, 201, 900])),
        ]).unwrap()
    }

    #[test]
    fn left_join_keeps_types_also_without_matches() {
        let right = build_right_hash_map(&[right_batch()], "k", &JoinHow::Left).unwrap();
        let with_match = probe_left_batch(left_batch(vec![Some("a"), Some("x")], 0), &right, &JoinHow::Left).unwrap().unwrap();
        let no_match   = probe_left_batch(left_batch(vec![Some("x"), None], 10), &right, &JoinHow::Left).unwrap().unwrap();
        // The same schema for both batches: Boolean `flag`, timestamp `ts`.
        assert_eq!(with_match.schema(), no_match.schema());
        let names: Vec<_> = with_match.schema().fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(names, vec!["k", "ts", "v", "flag"]);
        assert!(matches!(with_match.schema().field(1).data_type(), DataType::Timestamp(_, _)));
        let flag = no_match.column(3).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(flag.null_count(), 2);
        // `v` exists on both sides: the right value wins on the matched row only.
        let v = with_match.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(v.values().to_vec(), vec![100, 1]);
    }

    #[test]
    fn inner_join_repeats_rows_per_match_and_drops_the_rest() {
        let right = build_right_hash_map(&[right_batch()], "k", &JoinHow::Inner).unwrap();
        let out = probe_left_batch(left_batch(vec![Some("b"), Some("x"), None], 0), &right, &JoinHow::Inner).unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        let flag = out.column(3).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(flag.iter().collect::<Vec<_>>(), vec![Some(true), Some(false)]);
        assert!(probe_left_batch(left_batch(vec![Some("x")], 0), &right, &JoinHow::Inner).unwrap().is_none());
    }

    #[test]
    fn full_join_emits_unmatched_right_rows_in_the_same_schema() {
        let out = hash_join(&[left_batch(vec![Some("a")], 0)], &[right_batch()], "k", &JoinHow::Full).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].schema(), out[1].schema());
        let unmatched = &out[1];
        let keys = unmatched.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(keys.iter().collect::<Vec<_>>(), vec![Some("b"), Some("b"), Some("z")]);
        assert_eq!(unmatched.column(1).null_count(), 3, "left-only `ts` is NULL");
        let v = unmatched.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(v.values().to_vec(), vec![200, 201, 900], "`v` comes from the right side");
    }

    #[test]
    fn empty_right_side_keeps_left_rows_for_a_left_join() {
        let right = build_right_hash_map(&[], "k", &JoinHow::Left).unwrap();
        let out = probe_left_batch(left_batch(vec![Some("a"), None], 0), &right, &JoinHow::Left).unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.num_columns(), 3);
    }
}
