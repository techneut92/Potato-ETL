//! Oracle-specific type coercion registry.

use arrow::datatypes::{DataType, TimeUnit};
use potato_etl_common::db::common::type_coercion::{CoercionOperation, TypeCoercionRegistry};

pub struct OracleTypeRegistry;

impl TypeCoercionRegistry for OracleTypeRegistry {
    fn sql_to_arrow_type(&self, sql_type: &str) -> Option<DataType> {
        let lower = sql_type.to_lowercase();
        let base_type = lower.split('(').next().unwrap_or(&lower);
        match base_type {
            "timestamp" => {
                if lower.contains("with time zone") {
                    Some(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))
                } else {
                    Some(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
            }
            "number" => {
                if let Some(params) = lower.split('(').nth(1) {
                    let params = params.trim_end_matches(')');
                    let parts: Vec<&str> = params.split(',').collect();
                    if parts.len() == 2 {
                        if let (Ok(p), Ok(s)) = (parts[0].trim().parse::<u8>(), parts[1].trim().parse::<i8>()) {
                            return if s == 0 { Some(if p <= 9 { DataType::Int32 } else { DataType::Int64 }) }
                                   else { Some(DataType::Decimal128(p, s)) };
                        }
                    } else if parts.len() == 1 {
                        if let Ok(p) = parts[0].trim().parse::<u8>() {
                            return Some(if p <= 9 { DataType::Int32 } else { DataType::Int64 });
                        }
                    }
                }
                Some(DataType::Decimal128(38, 10))
            }
            "varchar2" | "char" | "nvarchar2" | "nchar" => Some(DataType::Utf8),
            "clob" | "nclob" => Some(DataType::LargeUtf8),
            "raw" | "blob" => Some(DataType::Binary),
            "date" => Some(DataType::Date32),
            _ => None,
        }
    }

    fn needs_coercion(&self, arrow_type: &DataType, sql_type: &str) -> Option<CoercionOperation> {
        let lower = sql_type.to_lowercase();
        match arrow_type {
            // ── Timestamps → TIMESTAMP WITH TIME ZONE ────────────────────────
            DataType::Timestamp(..) if lower.contains("with time zone") => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
                if *arrow_type == target {
                    None
                } else {
                    Some(CoercionOperation::Cast(target))
                }
            }
            // ── Timestamps → TIMESTAMP (naive) ──────────────────────────────
            DataType::Timestamp(..) if lower.starts_with("timestamp") => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, None);
                if *arrow_type == target {
                    None
                } else {
                    Some(CoercionOperation::Cast(target))
                }
            }
            DataType::LargeUtf8 if lower.starts_with("varchar2") || lower.starts_with("nvarchar2") || lower.starts_with("char") => {
                Some(CoercionOperation::Cast(DataType::Utf8))
            }
            DataType::Decimal128(curr_p, curr_s) if lower.starts_with("number") => {
                if let Some(params) = lower.split('(').nth(1) {
                    let params = params.trim_end_matches(')');
                    let parts: Vec<&str> = params.split(',').collect();
                    if parts.len() == 2 {
                        if let (Ok(tp), Ok(ts)) = (parts[0].trim().parse::<u8>(), parts[1].trim().parse::<i8>()) {
                            if *curr_p != tp || *curr_s != ts {
                                return Some(CoercionOperation::AdjustDecimal { target_precision: tp, target_scale: ts });
                            }
                        }
                    }
                }
                None
            }
            // ── UUID: FixedSizeBinary(16) → VARCHAR2 (format to string) ─────
            DataType::FixedSizeBinary(16) if lower.starts_with("varchar2") || lower.starts_with("nvarchar2") => {
                Some(CoercionOperation::FormatUuid)
            }
            // ── UUID: Utf8 → RAW(16) (parse to binary) ──────────────────────
            DataType::Utf8 | DataType::LargeUtf8 if lower == "raw(16)" => {
                Some(CoercionOperation::ParseUuid)
            }
            _ => None,
        }
    }
}