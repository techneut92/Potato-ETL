//! Databricks-specific type coercion registry.

use arrow::datatypes::{DataType, TimeUnit};
use potato_etl_common::db::common::type_coercion::{CoercionOperation, TypeCoercionRegistry};

pub struct DatabricksTypeRegistry;

impl TypeCoercionRegistry for DatabricksTypeRegistry {
    fn sql_to_arrow_type(&self, sql_type: &str) -> Option<DataType> {
        let lower = sql_type.to_lowercase();
        let base_type = lower.split('(').next().unwrap_or(&lower);
        match base_type {
            "timestamp" => Some(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
            "tinyint" | "byte" => Some(DataType::Int8),
            "smallint" | "short" => Some(DataType::Int16),
            "int" | "integer" => Some(DataType::Int32),
            "bigint" | "long" => Some(DataType::Int64),
            "float" => Some(DataType::Float32),
            "double" => Some(DataType::Float64),
            "string" => Some(DataType::Utf8),
            "binary" => Some(DataType::Binary),
            "boolean" => Some(DataType::Boolean),
            "decimal" => {
                if let Some(params) = lower.split('(').nth(1) {
                    let params = params.trim_end_matches(')');
                    let parts: Vec<&str> = params.split(',').collect();
                    if parts.len() == 2 {
                        if let (Ok(p), Ok(s)) = (parts[0].trim().parse::<u8>(), parts[1].trim().parse::<i8>()) {
                            return Some(DataType::Decimal128(p, s));
                        }
                    } else if parts.len() == 1 {
                        if let Ok(p) = parts[0].trim().parse::<u8>() { return Some(DataType::Decimal128(p, 0)); }
                    }
                }
                Some(DataType::Decimal128(38, 18))
            }
            "date" => Some(DataType::Date32),
            _ => None,
        }
    }

    fn needs_coercion(&self, arrow_type: &DataType, sql_type: &str) -> Option<CoercionOperation> {
        let lower = sql_type.to_lowercase();
        let base_type = lower.split('(').next().unwrap_or(&lower);
        match (arrow_type, base_type) {
            // ── Timestamps → TIMESTAMP (tz-aware in Databricks) ──────────────
            // Databricks TIMESTAMP is always UTC. Target: Timestamp(Microsecond, UTC).
            (DataType::Timestamp(..), "timestamp") => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
                if *arrow_type == target {
                    None
                } else {
                    Some(CoercionOperation::Cast(target))
                }
            }
            (DataType::Int64, "int" | "integer") => Some(CoercionOperation::Cast(DataType::Int32)),
            (DataType::Int32, "smallint" | "short") => Some(CoercionOperation::Cast(DataType::Int16)),
            (DataType::Int16, "tinyint" | "byte") => Some(CoercionOperation::Cast(DataType::Int8)),
            (DataType::LargeUtf8, "string") => Some(CoercionOperation::Cast(DataType::Utf8)),
            // ── UUID: FixedSizeBinary(16) → STRING (format to string) ────────
            (DataType::FixedSizeBinary(16), "string") => Some(CoercionOperation::FormatUuid),
            (DataType::Decimal128(cp, cs), "decimal") => {
                if let Some(params) = lower.split('(').nth(1) {
                    let params = params.trim_end_matches(')');
                    let parts: Vec<&str> = params.split(',').collect();
                    if parts.len() == 2 {
                        if let (Ok(tp), Ok(ts)) = (parts[0].trim().parse::<u8>(), parts[1].trim().parse::<i8>()) {
                            if *cp != tp || *cs != ts { return Some(CoercionOperation::AdjustDecimal { target_precision: tp, target_scale: ts }); }
                        }
                    } else if parts.len() == 1 {
                        if let Ok(tp) = parts[0].trim().parse::<u8>() {
                            if *cp != tp || *cs != 0 { return Some(CoercionOperation::AdjustDecimal { target_precision: tp, target_scale: 0 }); }
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }
}