//! Postgres type coercion registry.

use potato_etl_common::db::common::type_coercion::{CoercionOperation, TypeCoercionRegistry};
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};

/// Postgres type coercion rules.
///
/// ## Timezone Handling (CRITICAL!)
///
/// Postgres has **different timezone semantics than MSSQL**:
/// - **`TIMESTAMPTZ`** — stores UTC, displays in session timezone
///   - **KEEP** timezone metadata! Arrow `Timestamp[us, UTC]` is correct.
/// - **`TIMESTAMP`** (without time zone) — stores local time, no timezone info
///   - **STRIP** timezone if source is timezone-aware.
pub struct PostgresTypeRegistry;

impl TypeCoercionRegistry for PostgresTypeRegistry {
    fn sql_to_arrow_type(&self, sql_type: &str) -> Option<DataType> {
        let lower = sql_type.to_lowercase();
        let base_type = lower.split('(').next().unwrap_or(&lower);

        match base_type {
            // ── Timestamps ────────────────────────────────────────────────────
            "timestamptz" | "timestamp with time zone" => {
                Some(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))
            }
            "timestamp" | "timestamp without time zone" => {
                Some(DataType::Timestamp(TimeUnit::Microsecond, None))
            }

            // ── Integers ──────────────────────────────────────────────────────
            "smallint" | "int2" => Some(DataType::Int16),
            "integer" | "int" | "int4" => Some(DataType::Int32),
            "bigint" | "int8" => Some(DataType::Int64),

            // ── Floats ────────────────────────────────────────────────────────
            "real" | "float4" => Some(DataType::Float32),
            "double precision" | "float8" => Some(DataType::Float64),

            // ── Strings ───────────────────────────────────────────────────────
            "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" => {
                Some(DataType::Utf8)
            }

            // ── Binary ────────────────────────────────────────────────────────
            "bytea" => Some(DataType::Binary),

            // ── Boolean ───────────────────────────────────────────────────────
            "boolean" | "bool" => Some(DataType::Boolean),

            // ── Decimal ───────────────────────────────────────────────────────
            "numeric" | "decimal" => {
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
                Some(DataType::Decimal128(38, 10))
            }

            // ── Date/Time ─────────────────────────────────────────────────────
            "date" => Some(DataType::Date32),
            "time" | "time without time zone" => Some(DataType::Time64(TimeUnit::Microsecond)),
            "timetz" | "time with time zone" => Some(DataType::Time64(TimeUnit::Microsecond)),

            // ── Interval ──────────────────────────────────────────────────────
            "interval" => Some(DataType::Interval(IntervalUnit::MonthDayNano)),

            // ── JSON / JSONB ──────────────────────────────────────────────────
            "json" | "jsonb" => Some(DataType::Utf8),

            // ── UUID ──────────────────────────────────────────────────────────
            // Postgres UUID binary format = 16 raw bytes.
            "uuid" => Some(DataType::FixedSizeBinary(16)),

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
            // ── Timestamps → TIMESTAMPTZ ──────────────────────────────────────
            // Target is always Timestamp(Microsecond, UTC).
            // Handles unit conversion (s/ms/ns → µs) AND timezone add in one cast.
            (DataType::Timestamp(..), "timestamptz" | "timestamp with time zone") => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
                if *arrow_type == target {
                    None
                } else {
                    Some(CoercionOperation::Cast(target))
                }
            }

            // ── Timestamps → TIMESTAMP (without tz) ──────────────────────────
            // Target is always Timestamp(Microsecond, None).
            // Handles unit conversion AND timezone strip in one cast.
            (DataType::Timestamp(..), "timestamp" | "timestamp without time zone") => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, None);
                if *arrow_type == target {
                    None
                } else {
                    Some(CoercionOperation::Cast(target))
                }
            }

            // ── Integer downcasting ───────────────────────────────────────────
            (DataType::Int64, "integer" | "int" | "int4") => {
                Some(CoercionOperation::Cast(DataType::Int32))
            }
            (DataType::Int32, "smallint" | "int2") => {
                Some(CoercionOperation::Cast(DataType::Int16))
            }
            (DataType::Int64, "smallint" | "int2") => {
                Some(CoercionOperation::Cast(DataType::Int16))
            }

            // ── String type normalization ─────────────────────────────────────
            (DataType::LargeUtf8, "text" | "varchar" | "character varying" | "char" | "character" | "bpchar") => {
                Some(CoercionOperation::Cast(DataType::Utf8))
            }

            // ── UUID ──────────────────────────────────────────────────────────
            // Utf8 UUID strings ("550e8400-...") → FixedSizeBinary(16) for COPY BINARY.
            (DataType::Utf8 | DataType::LargeUtf8, "uuid") => {
                Some(CoercionOperation::ParseUuid)
            }
            (DataType::FixedSizeBinary(16), "uuid") => None,

            // ── Interval unit normalization ────────────────────────────────────
            // Postgres INTERVAL stores months + days + microseconds.
            // Arrow MonthDayNano is the best match (months + days + nanoseconds).
            (DataType::Interval(IntervalUnit::YearMonth), "interval") |
            (DataType::Interval(IntervalUnit::DayTime), "interval") => {
                Some(CoercionOperation::Cast(DataType::Interval(IntervalUnit::MonthDayNano)))
            }
            (DataType::Interval(IntervalUnit::MonthDayNano), "interval") => None,

            // ── Decimal precision/scale adjustment ───────────────────────────
            (DataType::Decimal128(curr_p, curr_s), "numeric" | "decimal") => {
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
                    } else if parts.len() == 1 {
                        if let Ok(target_p) = parts[0].trim().parse::<u8>() {
                            if *curr_p != target_p || *curr_s != 0 {
                                return Some(CoercionOperation::AdjustDecimal {
                                    target_precision: target_p,
                                    target_scale: 0,
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

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Timestamp unit + timezone coercion ────────────────────────────────

    #[test]
    fn test_timestamptz_microsecond_utc_no_coercion() {
        let registry = PostgresTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(registry.needs_coercion(&arrow_type, "TIMESTAMPTZ"), None);
    }

    #[test]
    fn test_timestamptz_microsecond_naive_adds_tz() {
        let registry = PostgresTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Microsecond, None);
        let expected = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            registry.needs_coercion(&arrow_type, "TIMESTAMPTZ"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_timestamptz_second_utc_converts_unit() {
        let registry = PostgresTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
        let expected = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            registry.needs_coercion(&arrow_type, "TIMESTAMPTZ"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_timestamptz_second_naive_converts_unit_and_adds_tz() {
        let registry = PostgresTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Second, None);
        let expected = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            registry.needs_coercion(&arrow_type, "TIMESTAMPTZ"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_timestamptz_millisecond_converts_unit() {
        let registry = PostgresTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let expected = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            registry.needs_coercion(&arrow_type, "TIMESTAMPTZ"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_timestamptz_nanosecond_converts_unit() {
        let registry = PostgresTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        let expected = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(
            registry.needs_coercion(&arrow_type, "TIMESTAMPTZ"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_timestamp_naive_microsecond_no_coercion() {
        let registry = PostgresTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(registry.needs_coercion(&arrow_type, "TIMESTAMP"), None);
    }

    #[test]
    fn test_timestamp_naive_strips_tz() {
        let registry = PostgresTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let expected = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            registry.needs_coercion(&arrow_type, "TIMESTAMP"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    #[test]
    fn test_timestamp_second_naive_converts_unit() {
        let registry = PostgresTypeRegistry;
        let arrow_type = DataType::Timestamp(TimeUnit::Second, None);
        let expected = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(
            registry.needs_coercion(&arrow_type, "TIMESTAMP"),
            Some(CoercionOperation::Cast(expected))
        );
    }

    // ── Other coercions (unchanged) ──────────────────────────────────────

    #[test]
    fn test_int64_to_integer_requires_downcast() {
        let registry = PostgresTypeRegistry;
        let op = registry.needs_coercion(&DataType::Int64, "INTEGER");
        assert_eq!(op, Some(CoercionOperation::Cast(DataType::Int32)));
    }

    #[test]
    fn test_sql_to_arrow_timestamptz() {
        let registry = PostgresTypeRegistry;
        let arrow_type = registry.sql_to_arrow_type("TIMESTAMPTZ").unwrap();
        assert_eq!(
            arrow_type,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_sql_to_arrow_timestamp() {
        let registry = PostgresTypeRegistry;
        let arrow_type = registry.sql_to_arrow_type("TIMESTAMP").unwrap();
        assert_eq!(
            arrow_type,
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_numeric_with_precision() {
        let registry = PostgresTypeRegistry;
        let arrow_type = registry.sql_to_arrow_type("NUMERIC(10,3)").unwrap();
        assert_eq!(arrow_type, DataType::Decimal128(10, 3));
    }

    #[test]
    fn test_numeric_without_scale() {
        let registry = PostgresTypeRegistry;
        let arrow_type = registry.sql_to_arrow_type("NUMERIC(18)").unwrap();
        assert_eq!(arrow_type, DataType::Decimal128(18, 0));
    }

    // ── UUID coercion ────────────────────────────────────────────────────

    #[test]
    fn test_sql_to_arrow_uuid() {
        let registry = PostgresTypeRegistry;
        let arrow_type = registry.sql_to_arrow_type("uuid").unwrap();
        assert_eq!(arrow_type, DataType::FixedSizeBinary(16));
    }

    #[test]
    fn test_utf8_to_uuid_needs_parse() {
        let registry = PostgresTypeRegistry;
        assert_eq!(
            registry.needs_coercion(&DataType::Utf8, "uuid"),
            Some(CoercionOperation::ParseUuid)
        );
    }

    #[test]
    fn test_large_utf8_to_uuid_needs_parse() {
        let registry = PostgresTypeRegistry;
        assert_eq!(
            registry.needs_coercion(&DataType::LargeUtf8, "uuid"),
            Some(CoercionOperation::ParseUuid)
        );
    }

    #[test]
    fn test_fixed_binary_16_to_uuid_no_coercion() {
        let registry = PostgresTypeRegistry;
        assert_eq!(
            registry.needs_coercion(&DataType::FixedSizeBinary(16), "uuid"),
            None
        );
    }

    // ── Interval coercion ────────────────────────────────────────────────

    #[test]
    fn test_sql_to_arrow_interval() {
        let registry = PostgresTypeRegistry;
        let arrow_type = registry.sql_to_arrow_type("interval").unwrap();
        assert_eq!(arrow_type, DataType::Interval(IntervalUnit::MonthDayNano));
    }

    #[test]
    fn test_interval_year_month_needs_coercion() {
        let registry = PostgresTypeRegistry;
        assert_eq!(
            registry.needs_coercion(&DataType::Interval(IntervalUnit::YearMonth), "interval"),
            Some(CoercionOperation::Cast(DataType::Interval(IntervalUnit::MonthDayNano)))
        );
    }

    #[test]
    fn test_interval_month_day_nano_no_coercion() {
        let registry = PostgresTypeRegistry;
        assert_eq!(
            registry.needs_coercion(&DataType::Interval(IntervalUnit::MonthDayNano), "interval"),
            None
        );
    }
}