//! Convert Thrift columnar result sets → Arrow RecordBatches.
//!
//! Shared by `source.rs` (streaming reads) and `client.rs` (introspection queries).
//!
//! When `ColumnMeta` (from `GetResultSetMetadata`) is available, the Databricks
//! SQL type is used to produce the correct Arrow type — in particular,
//! TIMESTAMP/TIMESTAMP_NTZ strings are parsed into Arrow `Timestamp(Microsecond, UTC)`
//! and DATE strings into Arrow `Date32`.
//!
//! Performance notes:
//! - Null bitmaps are inverted in a single pass (no two-pass scan-then-invert)
//! - Timestamp strings use a hand-rolled parser for the common Databricks format,
//!   falling back to chrono only for unusual formats
//! - Numeric columns map directly to Arrow buffers via `ScalarBuffer::from(Vec<T>)`

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use super::rpc::{is_null, dbx_type, ColumnMeta, ThriftColData};

/// Convert Thrift column data directly into an Arrow RecordBatch.
///
/// Each `ThriftColData` variant maps to an Arrow array type.  The Thrift null
/// bitmap (1 = null) is inverted to Arrow's validity bitmap (1 = valid) via
/// bitwise NOT on the raw bytes — no per-bit iteration needed.
///
/// If `col_meta` is provided and has the correct length, column names and
/// Databricks SQL types are used for proper Arrow type mapping.  Otherwise
/// synthetic names and default type mappings are used as a fallback.
pub fn columns_to_record_batch(
    columns: Vec<ThriftColData>,
    col_meta: Option<&[ColumnMeta]>,
) -> anyhow::Result<RecordBatch> {
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());

    for (i, col) in columns.into_iter().enumerate() {
        let meta = col_meta.and_then(|m| m.get(i));
        let col_name = meta
            .map(|m| m.name.as_str())
            .filter(|n| !n.is_empty())
            .unwrap_or("");
        // Use a cheap stack string for synthetic names to avoid heap alloc
        let col_name = if col_name.is_empty() {
            format!("col_{i}")
        } else {
            col_name.to_owned()
        };
        let dbx_tid = meta.map(|m| m.type_id).unwrap_or(-1);

        match col {
            ThriftColData::Bool { values, nulls } => {
                let nb = thrift_nulls_to_arrow(&nulls, values.len());
                fields.push(Field::new(&col_name, DataType::Boolean, true));
                arrays.push(Arc::new(
                    BooleanArray::new(BooleanBuffer::from(values), nb),
                ));
            }
            ThriftColData::Byte { values, nulls } => {
                let nb = thrift_nulls_to_arrow(&nulls, values.len());
                fields.push(Field::new(&col_name, DataType::Int8, true));
                arrays.push(Arc::new(Int8Array::new(values.into(), nb)));
            }
            ThriftColData::I16 { values, nulls } => {
                let nb = thrift_nulls_to_arrow(&nulls, values.len());
                fields.push(Field::new(&col_name, DataType::Int16, true));
                arrays.push(Arc::new(Int16Array::new(values.into(), nb)));
            }
            ThriftColData::I32 { values, nulls } => {
                if dbx_tid == dbx_type::DATE {
                    let nb = thrift_nulls_to_arrow(&nulls, values.len());
                    fields.push(Field::new(&col_name, DataType::Date32, true));
                    arrays.push(Arc::new(Date32Array::new(values.into(), nb)));
                } else {
                    let nb = thrift_nulls_to_arrow(&nulls, values.len());
                    fields.push(Field::new(&col_name, DataType::Int32, true));
                    arrays.push(Arc::new(Int32Array::new(values.into(), nb)));
                }
            }
            ThriftColData::I64 { values, nulls } => {
                if dbx_tid == dbx_type::TIMESTAMP || dbx_tid == dbx_type::TIMESTAMP_NTZ {
                    // Native TIMESTAMP columns from Databricks Thrift arrive as
                    // i64 microseconds since epoch in TI64Column — pass through
                    // directly into TimestampMicrosecondArray.
                    //
                    // NOTE: If you have INT/BIGINT columns that store epoch
                    // *seconds* (e.g. `created`, `updated`), use the schema
                    // conversion step with `type: "timestamp[s, UTC]"` — those
                    // columns arrive here as plain Int64, not TIMESTAMP.
                    let nb = thrift_nulls_to_arrow(&nulls, values.len());
                    let ts_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
                    fields.push(Field::new(&col_name, ts_type, true));
                    arrays.push(Arc::new(
                        TimestampMicrosecondArray::new(values.into(), nb)
                            .with_timezone("UTC"),
                    ));
                } else {
                    let nb = thrift_nulls_to_arrow(&nulls, values.len());
                    fields.push(Field::new(&col_name, DataType::Int64, true));
                    arrays.push(Arc::new(Int64Array::new(values.into(), nb)));
                }
            }
            ThriftColData::Double { values, nulls } => {
                let nb = thrift_nulls_to_arrow(&nulls, values.len());
                fields.push(Field::new(&col_name, DataType::Float64, true));
                arrays.push(Arc::new(Float64Array::new(values.into(), nb)));
            }
            ThriftColData::Str { values, nulls } => {
                if dbx_tid == dbx_type::TIMESTAMP || dbx_tid == dbx_type::TIMESTAMP_NTZ {
                    let (arr, dt) = parse_timestamp_strings(&values, &nulls);
                    fields.push(Field::new(&col_name, dt, true));
                    arrays.push(arr);
                } else if dbx_tid == dbx_type::DATE {
                    let (arr, dt) = parse_date_strings(&values, &nulls);
                    fields.push(Field::new(&col_name, dt, true));
                    arrays.push(arr);
                } else {
                    let strs: Vec<Option<&str>> = values
                        .iter()
                        .enumerate()
                        .map(|(i, b)| {
                            if is_null(&nulls, i) {
                                None
                            } else {
                                Some(unsafe { std::str::from_utf8_unchecked(b) })
                            }
                        })
                        .collect();
                    fields.push(Field::new(&col_name, DataType::Utf8, true));
                    arrays.push(Arc::new(StringArray::from(strs)));
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// Convert Thrift null bitmap (1=null) → Arrow NullBuffer (1=valid).
///
/// Single-pass: inverts and checks for any null bit simultaneously.
fn thrift_nulls_to_arrow(thrift_nulls: &[u8], len: usize) -> Option<NullBuffer> {
    if thrift_nulls.is_empty() {
        return None;
    }
    let mut has_any_null = false;
    let inverted: Vec<u8> = thrift_nulls
        .iter()
        .map(|&b| {
            has_any_null |= b != 0;
            !b
        })
        .collect();
    if !has_any_null {
        return None;
    }
    let buf = Buffer::from(inverted);
    Some(NullBuffer::new(BooleanBuffer::new(buf, 0, len)))
}

// ── Fast timestamp parsing ───────────────────────────────────────────────────
//
// Databricks almost always sends timestamps as "YYYY-MM-DD HH:MM:SS.ffffff".
// A hand-rolled parser for this exact format avoids chrono's generic parsing
// overhead (format string interpretation, locale handling, etc.).

/// Parse "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD HH:MM:SS.f+" → microseconds since epoch.
///
/// Returns `None` if the string doesn't match the expected format.
#[inline]
fn fast_parse_timestamp_micros(s: &[u8]) -> Option<i64> {
    // Minimum: "YYYY-MM-DD HH:MM:SS" = 19 chars
    if s.len() < 19 { return None; }

    // Quick format check: s[4]=='-', s[7]=='-', s[10]==' ' or 'T', s[13]==':', s[16]==':'
    if s[4] != b'-' || s[7] != b'-' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let sep = s[10];
    if sep != b' ' && sep != b'T' {
        return None;
    }

    let year  = parse_digits::<4>(s, 0)? as i32;
    let month = parse_digits::<2>(s, 5)? as u32;
    let day   = parse_digits::<2>(s, 8)? as u32;
    let hour  = parse_digits::<2>(s, 11)? as u32;
    let min   = parse_digits::<2>(s, 14)? as u32;
    let sec   = parse_digits::<2>(s, 17)? as u32;

    // Parse fractional seconds if present
    let mut micros_frac: u32 = 0;
    if s.len() > 19 && s[19] == b'.' {
        let frac = &s[20..];
        let digits = frac.len().min(6);
        let mut val: u32 = 0;
        for &ch in &frac[..digits] {
            if !ch.is_ascii_digit() { break; }
            val = val * 10 + (ch - b'0') as u32;
        }
        // Pad to 6 digits (microseconds)
        for _ in digits..6 { val *= 10; }
        micros_frac = val;
    }

    // Convert to epoch micros using a simplified calendar
    // (valid for dates 1970-01-01 to 2399-12-31)
    let days = days_from_civil(year, month, day)?;
    let secs = days as i64 * 86400 + hour as i64 * 3600 + min as i64 * 60 + sec as i64;
    Some(secs * 1_000_000 + micros_frac as i64)
}

/// Parse N ASCII digit characters at offset → u32. Returns None if any non-digit.
#[inline]
fn parse_digits<const N: usize>(s: &[u8], offset: usize) -> Option<u32> {
    let mut val: u32 = 0;
    for i in 0..N {
        let ch = s[offset + i];
        if !ch.is_ascii_digit() { return None; }
        val = val * 10 + (ch - b'0') as u32;
    }
    Some(val)
}

/// Civil date → days since Unix epoch (1970-01-01).
/// Algorithm from Howard Hinnant (public domain).
#[inline]
fn days_from_civil(mut y: i32, m: u32, d: u32) -> Option<i64> {
    if m < 1 || m > 12 || d < 1 || d > 31 { return None; }
    if m <= 2 { y -= 1; }
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era as i64 * 146097 + doe as i64 - 719468)
}

/// Parse Thrift string column values into Arrow Timestamp(Microsecond, UTC).
///
/// Uses the fast hand-rolled parser for the common format, falling back to
/// chrono for unusual formats.
fn parse_timestamp_strings(
    values: &[bytes::Bytes],
    nulls: &[u8],
) -> (ArrayRef, DataType) {
    let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let mut micros: Vec<i64> = Vec::with_capacity(values.len());
    let mut valid_bits: Vec<bool> = Vec::with_capacity(values.len());

    for (i, b) in values.iter().enumerate() {
        if is_null(nulls, i) || b.is_empty() {
            micros.push(0);
            valid_bits.push(false);
            continue;
        }

        // Fast path: hand-rolled parser
        if let Some(us) = fast_parse_timestamp_micros(b) {
            micros.push(us);
            valid_bits.push(true);
            continue;
        }

        // Slow path: chrono fallback for unusual formats
        let s = std::str::from_utf8(b).unwrap_or("");
        let mut parsed = false;
        for fmt in &["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S",
                     "%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S"] {
            if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
                micros.push(ndt.and_utc().timestamp_micros());
                valid_bits.push(true);
                parsed = true;
                break;
            }
        }
        if !parsed {
            tracing::warn!(value = %s, "Failed to parse Databricks timestamp string, treating as NULL");
            micros.push(0);
            valid_bits.push(false);
        }
    }

    // Build validity buffer
    let validity = BooleanBuffer::from(valid_bits);
    let null_buf = if validity.count_set_bits() == validity.len() {
        None
    } else {
        Some(NullBuffer::from(validity))
    };
    let arr = TimestampMicrosecondArray::new(micros.into(), null_buf)
        .with_timezone("UTC");
    (Arc::new(arr), dt)
}

/// Parse Thrift string column values into Arrow Date32 (days since epoch).
fn parse_date_strings(
    values: &[bytes::Bytes],
    nulls: &[u8],
) -> (ArrayRef, DataType) {
    let mut days_vec: Vec<i32> = Vec::with_capacity(values.len());
    let mut valid_bits: Vec<bool> = Vec::with_capacity(values.len());

    for (i, b) in values.iter().enumerate() {
        if is_null(nulls, i) || b.is_empty() {
            days_vec.push(0);
            valid_bits.push(false);
            continue;
        }

        // Fast path: "YYYY-MM-DD" (10 chars)
        if b.len() >= 10 && b[4] == b'-' && b[7] == b'-' {
            if let (Some(y), Some(m), Some(d)) = (
                parse_digits::<4>(b, 0),
                parse_digits::<2>(b, 5),
                parse_digits::<2>(b, 8),
            ) {
                if let Some(days) = days_from_civil(y as i32, m, d) {
                    days_vec.push(days as i32);
                    valid_bits.push(true);
                    continue;
                }
            }
        }

        // If the string is a full timestamp, take the date part
        if let Some(us) = fast_parse_timestamp_micros(b) {
            days_vec.push((us / 86_400_000_000) as i32);
            valid_bits.push(true);
            continue;
        }

        let s = std::str::from_utf8(b).unwrap_or("");
        tracing::warn!(value = %s, "Failed to parse Databricks date string, treating as NULL");
        days_vec.push(0);
        valid_bits.push(false);
    }

    let validity = BooleanBuffer::from(valid_bits);
    let null_buf = if validity.count_set_bits() == validity.len() {
        None
    } else {
        Some(NullBuffer::from(validity))
    };
    (Arc::new(Date32Array::new(days_vec.into(), null_buf)), DataType::Date32)
}