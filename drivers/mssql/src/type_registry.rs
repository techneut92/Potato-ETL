//! MSSQL type coercion registry.

use potato_etl_common::db::common::type_coercion::{CoercionOperation, TypeCoercionRegistry};
use arrow::datatypes::{DataType, TimeUnit};

/// MSSQL type coercion rules.
///
/// ## Timezone Handling
///
/// - **`DATETIME2`** — timezone-naive → strip timezone
/// - **`DATETIMEOFFSET`** — timezone-aware → keep/add timezone
pub struct MssqlTypeRegistry;

impl TypeCoercionRegistry for MssqlTypeRegistry {
    fn sql_to_arrow_type(&self, sql_type: &str) -> Option<DataType> {
        let lower = sql_type.to_lowercase();
        let base_type = lower.split('(').next().unwrap_or(&lower);

        match base_type {
            "datetime2" | "datetime" | "smalldatetime" => {
                Some(DataType::Timestamp(TimeUnit::Microsecond, None))
            }
            "datetimeoffset" => {
                Some(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))
            }
            "tinyint" => Some(DataType::UInt8),
            "smallint" => Some(DataType::Int16),
            "int" => Some(DataType::Int32),
            "bigint" => Some(DataType::Int64),
            "real" => Some(DataType::Float32),
            "float" => Some(DataType::Float64),
            "varchar" | "nvarchar" | "char" | "nchar" | "text" | "ntext" => {
                Some(DataType::Utf8)
            }
            "varbinary" | "binary" | "image" => Some(DataType::Binary),
            "bit" => Some(DataType::Boolean),
            "decimal" | "numeric" | "money" | "smallmoney" => {
                if let Some(params) = lower.split('(').nth(1) {
                    let params = params.trim_end_matches(')');
                    let parts: Vec<&str> = params.split(',').collect();
                    if parts.len() == 2 {
                        if let (Ok(p), Ok(s)) = (parts[0].trim().parse::<u8>(), parts[1].trim().parse::<i8>()) {
                            return Some(DataType::Decimal128(p, s));
                        }
                    }
                }
                Some(DataType::Decimal128(18, 2))
            }
            "date" => Some(DataType::Date32),
            "time" => Some(DataType::Time64(TimeUnit::Nanosecond)),
            "uniqueidentifier" => Some(DataType::Utf8),
            _ => None,
        }
    }

    fn needs_coercion(
        &self,
        arrow_type: &DataType,
        sql_type: &str,
    ) -> Option<CoercionOperation> {
        let lower = sql_type.to_lowercase();
        let base_type = lower.split('(').next().unwrap_or(&lower);

        match (arrow_type, base_type) {
            // ── Timestamps → DATETIME2/DATETIME/SMALLDATETIME (tz-naive) ─────
            // Target is always Timestamp(Microsecond, None).
            // Handles unit conversion (s/ms/ns → µs) AND timezone strip in one cast.
            (DataType::Timestamp(..), "datetime2" | "datetime" | "smalldatetime") => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, None);
                if *arrow_type == target {
                    None
                } else {
                    Some(CoercionOperation::Cast(target))
                }
            }
            // ── Timestamps → DATETIMEOFFSET (tz-aware) ───────────────────────
            // Target is always Timestamp(Microsecond, UTC).
            (DataType::Timestamp(..), "datetimeoffset") => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
                if *arrow_type == target {
                    None
                } else {
                    Some(CoercionOperation::Cast(target))
                }
            }
            (DataType::Int64, "int") => {
                Some(CoercionOperation::Cast(DataType::Int32))
            }
            (DataType::Int32, "smallint") => {
                Some(CoercionOperation::Cast(DataType::Int16))
            }
            (DataType::Int16, "tinyint") => {
                Some(CoercionOperation::Cast(DataType::UInt8))
            }
            (DataType::LargeUtf8, "varchar" | "nvarchar" | "char" | "nchar" | "text" | "ntext") => {
                Some(CoercionOperation::Cast(DataType::Utf8))
            }
            // ── UNIQUEIDENTIFIER ─────────────────────────────────────────────
            // Utf8 is already the expected type; LargeUtf8 needs narrowing.
            (DataType::Utf8, "uniqueidentifier") => None,
            (DataType::LargeUtf8, "uniqueidentifier") => {
                Some(CoercionOperation::Cast(DataType::Utf8))
            }
            // FixedSizeBinary(16) from Postgres UUID → parse back to text for tiberius.
            (DataType::FixedSizeBinary(16), "uniqueidentifier") => {
                Some(CoercionOperation::FormatUuid)
            }
            (DataType::Decimal128(curr_p, curr_s), "decimal" | "numeric") => {
                if let Some(params) = lower.split('(').nth(1) {
                    let params = params.trim_end_matches(')');
                    let parts: Vec<&str> = params.split(',').collect();
                    if parts.len() == 2 {
                        if let (Ok(target_p), Ok(target_s)) = (
                            parts[0].trim().parse::<u8>(),
                            parts[1].trim().parse::<i8>(),
                        ) {
                            if *curr_p != target_p || *curr_s != target_s {
                                return Some(CoercionOperation::AdjustDecimal {
                                    target_precision: target_p,
                                    target_scale: target_s,
                                });
                            }
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datetime2_microsecond_naive_no_coercion() {
        let registry = MssqlTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(registry.needs_coercion(&arrow_type, "DATETIME2"), None);
    }

    #[test]
    fn test_datetime2_strips_timezone() {
        let registry = MssqlTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let expected = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            registry.needs_coercion(&arrow_type, "DATETIME2"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_datetime2_second_converts_unit_and_strips_tz() {
        let registry = MssqlTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
        let expected = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            registry.needs_coercion(&arrow_type, "DATETIME2"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_datetime2_second_naive_converts_unit() {
        let registry = MssqlTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Second, None);
        let expected = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            registry.needs_coercion(&arrow_type, "DATETIME2"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_datetimeoffset_microsecond_utc_no_coercion() {
        let registry = MssqlTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(registry.needs_coercion(&arrow_type, "DATETIMEOFFSET"), None);
    }

    #[test]
    fn test_datetimeoffset_adds_timezone() {
        let registry = MssqlTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Microsecond, None);
        let expected = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            registry.needs_coercion(&arrow_type, "DATETIMEOFFSET"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_datetimeoffset_second_converts_unit_and_adds_tz() {
        let registry = MssqlTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Second, None);
        let expected = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            registry.needs_coercion(&arrow_type, "DATETIMEOFFSET"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_int64_to_int_requires_downcast() {
        let registry = MssqlTypeRegistry;
        let op = registry.needs_coercion(&DataType::Int64, "INT");
        assert_eq!(op, Some(CoercionOperation::Cast(DataType::Int32)));
    }

    #[test]
    fn test_sql_to_arrow_datetime2() {
        let registry = MssqlTypeRegistry;
        let arrow_type = registry.sql_to_arrow_type("DATETIME2").unwrap();
        assert_eq!(arrow_type, DataType::Timestamp(TimeUnit::Microsecond, None));
    }

    #[test]
    fn test_sql_to_arrow_datetimeoffset() {
        let registry = MssqlTypeRegistry;
        let arrow_type = registry.sql_to_arrow_type("DATETIMEOFFSET").unwrap();
        assert_eq!(arrow_type, DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
    }

    #[test]
    fn test_decimal_with_precision() {
        let registry = MssqlTypeRegistry;
        let arrow_type = registry.sql_to_arrow_type("DECIMAL(10,3)").unwrap();
        assert_eq!(arrow_type, DataType::Decimal128(10, 3));
    }

    // ── UNIQUEIDENTIFIER ─────────────────────────────────────────────

    #[test]
    fn test_sql_to_arrow_uniqueidentifier() {
        let registry = MssqlTypeRegistry;
        let arrow_type = registry.sql_to_arrow_type("UNIQUEIDENTIFIER").unwrap();
        assert_eq!(arrow_type, DataType::Utf8);
    }

    #[test]
    fn test_utf8_to_uniqueidentifier_no_coercion() {
        let registry = MssqlTypeRegistry;
        assert_eq!(
            registry.needs_coercion(&DataType::Utf8, "UNIQUEIDENTIFIER"),
            None
        );
    }

    #[test]
    fn test_large_utf8_to_uniqueidentifier_cast() {
        let registry = MssqlTypeRegistry;
        assert_eq!(
            registry.needs_coercion(&DataType::LargeUtf8, "UNIQUEIDENTIFIER"),
            Some(CoercionOperation::Cast(DataType::Utf8))
        );
    }

    #[test]
    fn test_fixed_binary16_to_uniqueidentifier_format() {
        let registry = MssqlTypeRegistry;
        assert_eq!(
            registry.needs_coercion(&DataType::FixedSizeBinary(16), "UNIQUEIDENTIFIER"),
            Some(CoercionOperation::FormatUuid)
        );
    }
}