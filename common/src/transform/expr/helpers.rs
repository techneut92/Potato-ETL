//! Broadcast helpers, cast helpers, temporal extraction, and JSON path utilities.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use super::ast::Expr;

// ── Broadcast from 1-element scalar ───────────────────────────────────────────

/// Broadcast a 1-element `ArrayRef` to `n` rows by repeating the single value.
pub fn broadcast_array(scalar: &ArrayRef, n: usize) -> anyhow::Result<ArrayRef> {
    if n == 0 {
        return Ok(scalar.slice(0, 0));
    }
    let dt = scalar.data_type().clone();
    match &dt {
        DataType::Timestamp(unit, tz) => {
            let casted = cast(scalar, &DataType::Timestamp(unit.clone(), tz.clone()))?;
            let ts_arr = casted.as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let val = if ts_arr.is_null(0) { None } else { Some(ts_arr.value(0)) };
            let arr: TimestampMicrosecondArray = (0..n).map(|_| val).collect();
            let arr = match tz {
                Some(tz) => arr.with_timezone(tz.as_ref()),
                None => arr,
            };
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Float64 => {
            let f = scalar.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = if f.is_null(0) { None } else { Some(f.value(0)) };
            let arr: Float64Array = (0..n).map(|_| val).collect();
            Ok(Arc::new(arr))
        }
        DataType::Int64 => {
            let i = scalar.as_any().downcast_ref::<Int64Array>().unwrap();
            let val = if i.is_null(0) { None } else { Some(i.value(0)) };
            let arr: Int64Array = (0..n).map(|_| val).collect();
            Ok(Arc::new(arr))
        }
        DataType::Int32 => {
            let i = scalar.as_any().downcast_ref::<Int32Array>().unwrap();
            let val = if i.is_null(0) { None } else { Some(i.value(0)) };
            let arr: Int32Array = (0..n).map(|_| val).collect();
            Ok(Arc::new(arr))
        }
        DataType::Boolean => {
            let b = scalar.as_any().downcast_ref::<BooleanArray>().unwrap();
            let val = if b.is_null(0) { None } else { Some(b.value(0)) };
            let arr: BooleanArray = (0..n).map(|_| val).collect();
            Ok(Arc::new(arr))
        }
        _ => {
            // Generic fallback: cast to Utf8, broadcast, then cast back.
            let s = to_string_array(scalar)?;
            let val = if s.is_null(0) { None } else { Some(s.value(0).to_string()) };
            let arr: StringArray = (0..n).map(|_| val.as_deref()).collect();
            Ok(Arc::new(arr))
        }
    }
}

// ── Broadcast helpers ─────────────────────────────────────────────────────────
// Fill an Arrow array with n copies of a scalar literal.

pub(crate) fn broadcast_i64(v: i64, n: usize) -> Int64Array {
    (0..n).map(|_| Some(v)).collect()
}

pub(crate) fn broadcast_f64(v: f64, n: usize) -> Float64Array {
    (0..n).map(|_| Some(v)).collect()
}

pub(crate) fn broadcast_str(s: &str, n: usize) -> StringArray {
    (0..n).map(|_| Some(s)).collect()
}

pub(crate) fn broadcast_bool(b: bool, n: usize) -> BooleanArray {
    (0..n).map(|_| Some(b)).collect()
}

// ── Cast helpers ──────────────────────────────────────────────────────────────

pub(crate) fn to_float64(arr: &ArrayRef) -> anyhow::Result<Float64Array> {
    let casted = cast(arr, &DataType::Float64)?;
    casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("internal: cast to Float64 succeeded but downcast failed"))
}

pub(crate) fn to_boolean(arr: &ArrayRef) -> anyhow::Result<BooleanArray> {
    let casted = cast(arr, &DataType::Boolean)?;
    casted
        .as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("internal: cast to Boolean succeeded but downcast failed"))
}

/// Cast `arr` to a `StringArray` (Arrow `Utf8`).
/// Exposed as `pub` so `flatten.rs` can reuse it via `super::expr::to_string_array`.
pub fn to_string_array(arr: &ArrayRef) -> anyhow::Result<StringArray> {
    let casted = cast(arr, &DataType::Utf8)?;
    casted
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("internal: cast to Utf8 succeeded but downcast failed"))
}

pub(crate) fn to_int64(arr: &ArrayRef) -> anyhow::Result<Int64Array> {
    let casted = cast(arr, &DataType::Int64)?;
    casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("internal: cast to Int64 succeeded but downcast failed"))
}

// ── Temporal part extraction ──────────────────────────────────────────────────

#[derive(Clone, Copy)]
pub(crate) enum TemporalPart { Year, Month, Day, Hour, Minute, Second }

pub(crate) fn temporal_part(args: &[Expr], batch: &RecordBatch, part: TemporalPart) -> anyhow::Result<ArrayRef> {
    anyhow::ensure!(args.len() == 1, "temporal function takes exactly 1 argument");
    let arr = super::eval::eval(&args[0], batch)?;

    match arr.data_type() {
        DataType::Timestamp(_, _) => {
            let casted = cast(&arr, &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None))?;
            let ts_arr = casted.as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let values: Int32Array = ts_arr.iter().map(|v| {
                v.and_then(|micros| {
                    let ndt = chrono::DateTime::from_timestamp_micros(micros)
                        .map(|dt| dt.naive_utc())?;
                    Some(extract_part(part, ndt))
                })
            }).collect();
            return Ok(Arc::new(values));
        }
        DataType::Date32 => {
            let date_arr = arr.as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .unwrap();
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let values: Int32Array = date_arr.iter().map(|v| {
                v.and_then(|days| {
                    let date = epoch.checked_add_signed(chrono::Duration::days(days as i64))?;
                    let ndt  = date.and_hms_opt(0, 0, 0)?;
                    Some(extract_part(part, ndt))
                })
            }).collect();
            return Ok(Arc::new(values));
        }
        DataType::Date64 => {
            let date_arr = arr.as_any()
                .downcast_ref::<arrow::array::Date64Array>()
                .unwrap();
            let values: Int32Array = date_arr.iter().map(|v| {
                v.and_then(|millis| {
                    let ndt = chrono::DateTime::from_timestamp_millis(millis)
                        .map(|dt| dt.naive_utc())?;
                    Some(extract_part(part, ndt))
                })
            }).collect();
            return Ok(Arc::new(values));
        }
        _ => {}
    }

    // Fall back: parse as date string ("YYYY-MM-DD…")
    let strs = to_string_array(&arr)?;
    let values: Int32Array = strs.iter().map(|v| {
        v.and_then(|s| {
            use chrono::{DateTime, NaiveDate, NaiveDateTime};
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                return Some(extract_part(part, dt.naive_utc()));
            }
            for fmt in &["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"] {
                if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
                    return Some(extract_part(part, ndt));
                }
            }
            if let Ok(nd) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                let ndt = nd.and_hms_opt(0, 0, 0)?;
                return Some(extract_part(part, ndt));
            }
            None
        })
    }).collect();
    Ok(Arc::new(values))
}

fn extract_part(part: TemporalPart, ndt: chrono::NaiveDateTime) -> i32 {
    use chrono::Datelike;
    use chrono::Timelike;
    match part {
        TemporalPart::Year   => ndt.year(),
        TemporalPart::Month  => ndt.month() as i32,
        TemporalPart::Day    => ndt.day() as i32,
        TemporalPart::Hour   => ndt.hour() as i32,
        TemporalPart::Minute => ndt.minute() as i32,
        TemporalPart::Second => ndt.second() as i32,
    }
}

// ── JSON path helper ──────────────────────────────────────────────────────────

/// Walk a dot-separated path (optionally with bracket indices) into a JSON value.
///
/// Examples:
/// - `"name"`           → `val["name"]`
/// - `"address.city"`   → `val["address"]["city"]`
/// - `"items[0].id"`    → `val["items"][0]["id"]`
///
/// Returns the value serialised to a `String`, or `None` if any segment is missing.
/// JSON strings are returned without surrounding quotes; all other types use their
/// `serde_json::Value::to_string()` representation.
pub fn json_path_get(val: &serde_json::Value, path: &str) -> Option<String> {
    let mut cur = val;
    for segment in path.split('.') {
        if let Some(bracket_pos) = segment.find('[') {
            let field = &segment[..bracket_pos];
            if !field.is_empty() {
                cur = cur.get(field)?;
            }
            let mut remaining = &segment[bracket_pos..];
            while remaining.starts_with('[') {
                let close = remaining.find(']')?;
                let idx: usize = remaining[1..close].parse().ok()?;
                cur = cur.get(idx)?;
                remaining = &remaining[close + 1..];
            }
        } else {
            cur = cur.get(segment)?;
        }
    }
    Some(match cur {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null      => return None,
        other                        => other.to_string(),
    })
}
