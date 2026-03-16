//! MySQL-specific type coercion registry.

use arrow::datatypes::{DataType, TimeUnit};

use potato_etl_common::db::common::type_coercion::{CoercionOperation, TypeCoercionRegistry};

pub struct MysqlTypeRegistry;

impl TypeCoercionRegistry for MysqlTypeRegistry {
    fn sql_to_arrow_type(&self, sql_type: &str) -> Option<DataType> {
        let lower = sql_type.to_lowercase();
        let base_type = lower.split('(').next().unwrap_or(&lower);

        match base_type {
            "datetime" => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
            "timestamp" => Some(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
            "tinyint" => Some(DataType::Int8),
            "smallint" => Some(DataType::Int16),
            "mediumint" | "int" | "integer" => Some(DataType::Int32),
            "bigint" => Some(DataType::Int64),
            "float" => Some(DataType::Float32),
            "double" | "real" => Some(DataType::Float64),
            "varchar" | "char" | "text" | "tinytext" | "mediumtext" => Some(DataType::Utf8),
            "longtext" => Some(DataType::LargeUtf8),
            "varbinary" | "binary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => Some(DataType::Binary),
            "boolean" | "bool" => Some(DataType::Boolean),
            "decimal" | "numeric" => {
                if let Some(params) = lower.split('(').nth(1) {
                    let params = params.trim_end_matches(')');
                    let parts: Vec<&str> = params.split(',').collect();
                    if parts.len() == 2 {
                        if let (Ok(p), Ok(s)) = (parts[0].trim().parse::<u8>(), parts[1].trim().parse::<i8>()) {
                            return Some(DataType::Decimal128(p, s));
                        }
                    } else if parts.len() == 1 {
                        if let Ok(p) = parts[0].trim().parse::<u8>() {
                            return Some(DataType::Decimal128(p, 0));
                        }
                    }
                }
                Some(DataType::Decimal128(10, 0))
            }
            "date" => Some(DataType::Date32),
            "time" => Some(DataType::Time64(TimeUnit::Microsecond)),
            "json" => Some(DataType::Utf8),
            _ => None,
        }
    }

    fn needs_coercion(&self, arrow_type: &DataType, sql_type: &str) -> Option<CoercionOperation> {
        let lower = sql_type.to_lowercase();
        let base_type = lower.split('(').next().unwrap_or(&lower);

        match (arrow_type, base_type) {
            // ── Timestamps → DATETIME (tz-naive) ─────────────────────────────
            // Target is always Timestamp(Microsecond, None).
            (DataType::Timestamp(..), "datetime") => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, None);
                if *arrow_type == target {
                    None
                } else {
                    Some(CoercionOperation::Cast(target))
                }
            }
            // ── Timestamps → TIMESTAMP (tz-aware in MySQL) ───────────────────
            // Target is always Timestamp(Microsecond, UTC).
            (DataType::Timestamp(..), "timestamp") => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
                if *arrow_type == target {
                    None
                } else {
                    Some(CoercionOperation::Cast(target))
                }
            }
            (DataType::Int64, "int" | "integer" | "mediumint") => Some(CoercionOperation::Cast(DataType::Int32)),
            (DataType::Int32, "smallint") => Some(CoercionOperation::Cast(DataType::Int16)),
            (DataType::Int16, "tinyint") => Some(CoercionOperation::Cast(DataType::Int8)),
            (DataType::LargeUtf8, "varchar" | "char" | "text" | "tinytext" | "mediumtext") => {
                Some(CoercionOperation::Cast(DataType::Utf8))
            }
            (DataType::Decimal128(curr_p, curr_s), "decimal" | "numeric") => {
                if let Some(params) = lower.split('(').nth(1) {
                    let params = params.trim_end_matches(')');
                    let parts: Vec<&str> = params.split(',').collect();
                    if parts.len() == 2 {
                        if let (Ok(target_p), Ok(target_s)) = (parts[0].trim().parse::<u8>(), parts[1].trim().parse::<i8>()) {
                            if *curr_p != target_p || *curr_s != target_s {
                                return Some(CoercionOperation::AdjustDecimal { target_precision: target_p, target_scale: target_s });
                            }
                        }
                    } else if parts.len() == 1 {
                        if let Ok(target_p) = parts[0].trim().parse::<u8>() {
                            if *curr_p != target_p || *curr_s != 0 {
                                return Some(CoercionOperation::AdjustDecimal { target_precision: target_p, target_scale: 0 });
                            }
                        }
                    }
                }
                None
            }
            // ── UUID: FixedSizeBinary(16) → VARCHAR/CHAR (format to string) ──
            (DataType::FixedSizeBinary(16), "varchar" | "char") => {
                Some(CoercionOperation::FormatUuid)
            }
            // ── UUID: Utf8 → BINARY(16) (parse to binary) ───────────────────
            (DataType::Utf8 | DataType::LargeUtf8, "binary") if lower == "binary(16)" => {
                Some(CoercionOperation::ParseUuid)
            }
            _ => None,
        }
    }
}