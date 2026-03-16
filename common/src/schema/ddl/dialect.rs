//! Arrow DataType → SQL type mapping for each dialect.

use arrow::datatypes::DataType;
use super::SqlDialect;

/// Converts an Arrow `DataType` to a SQL type string for the given dialect.
pub fn arrow_type_to_sql_dialect(dt: &DataType, dialect: SqlDialect) -> String {
    match dialect {
        SqlDialect::Postgres   => arrow_to_sql_pg(dt),
        SqlDialect::Mssql      => arrow_to_sql_mssql(dt),
        SqlDialect::Oracle     => arrow_to_sql_oracle(dt),
        SqlDialect::Mysql      => arrow_to_sql_mysql(dt),
        SqlDialect::Databricks => arrow_to_sql_databricks(dt),
    }
}

fn arrow_to_sql_pg(dt: &DataType) -> String {
    match dt {
        DataType::Boolean                          => "BOOLEAN".into(),
        DataType::Int8                             => "SMALLINT".into(),
        DataType::Int16                            => "SMALLINT".into(),
        DataType::Int32                            => "INTEGER".into(),
        DataType::Int64                            => "BIGINT".into(),
        DataType::UInt8  | DataType::UInt16        => "SMALLINT".into(),
        DataType::UInt32                           => "INTEGER".into(),
        DataType::UInt64                           => "BIGINT".into(),
        DataType::Float32                          => "REAL".into(),
        DataType::Float64                          => "DOUBLE PRECISION".into(),
        DataType::Utf8   | DataType::LargeUtf8     => "TEXT".into(),
        DataType::Date32 | DataType::Date64        => "DATE".into(),
        DataType::Timestamp(_, None)               => "TIMESTAMP".into(),
        DataType::Timestamp(_, Some(_))            => "TIMESTAMPTZ".into(),
        DataType::Time32(_) | DataType::Time64(_)  => "TIME".into(),
        DataType::Duration(_) | DataType::Interval(_) => "INTERVAL".into(),
        DataType::FixedSizeBinary(16)              => "UUID".into(),
        DataType::Binary | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)             => "BYTEA".into(),
        DataType::Decimal128(p, s) if *s > 0      => format!("NUMERIC({p},{s})"),
        DataType::Decimal128(p, _)                 => format!("NUMERIC({p})"),
        DataType::List(_) | DataType::LargeList(_) => "JSONB".into(),
        DataType::Struct(_)                        => "JSONB".into(),
        DataType::Map(_, _)                        => "JSONB".into(),
        _                                          => "TEXT".into(),
    }
}

fn arrow_to_sql_mssql(dt: &DataType) -> String {
    match dt {
        DataType::Boolean                               => "BIT".into(),
        DataType::Int8                                  => "SMALLINT".into(),
        DataType::Int16                                 => "SMALLINT".into(),
        DataType::Int32                                 => "INT".into(),
        DataType::Int64                                 => "BIGINT".into(),
        DataType::UInt8                                 => "TINYINT".into(),
        DataType::UInt16                                => "INT".into(),
        DataType::UInt32                                => "BIGINT".into(),
        DataType::UInt64                                => "DECIMAL(20,0)".into(),
        DataType::Float32                               => "REAL".into(),
        DataType::Float64                               => "FLOAT".into(),
        DataType::Utf8   | DataType::LargeUtf8          => "NVARCHAR(MAX)".into(),
        DataType::Date32 | DataType::Date64             => "DATE".into(),
        DataType::Timestamp(_, None)                    => "DATETIME2".into(),
        DataType::Timestamp(_, Some(_))                 => "DATETIMEOFFSET".into(),
        DataType::Time32(_) | DataType::Time64(_)       => "TIME".into(),
        DataType::FixedSizeBinary(16)                   => "UNIQUEIDENTIFIER".into(),
        DataType::Binary | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)                  => "VARBINARY(MAX)".into(),
        DataType::Decimal128(p, s) if *s > 0           => format!("DECIMAL({p},{s})"),
        DataType::Decimal128(p, _)                      => format!("DECIMAL({p},0)"),
        DataType::List(_) | DataType::LargeList(_)
        | DataType::Struct(_) | DataType::Map(_, _)    => "NVARCHAR(MAX)".into(),
        _                                               => "NVARCHAR(MAX)".into(),
    }
}

fn arrow_to_sql_oracle(dt: &DataType) -> String {
    match dt {
        DataType::Boolean                               => "NUMBER(1)".into(),
        DataType::Int8                                  => "NUMBER(3)".into(),
        DataType::Int16                                 => "NUMBER(5)".into(),
        DataType::Int32                                 => "NUMBER(10)".into(),
        DataType::Int64                                 => "NUMBER(19)".into(),
        DataType::UInt8                                 => "NUMBER(3)".into(),
        DataType::UInt16                                => "NUMBER(5)".into(),
        DataType::UInt32                                => "NUMBER(10)".into(),
        DataType::UInt64                                => "NUMBER(20)".into(),
        DataType::Float32                               => "BINARY_FLOAT".into(),
        DataType::Float64                               => "BINARY_DOUBLE".into(),
        DataType::Utf8   | DataType::LargeUtf8          => "VARCHAR2(4000)".into(),
        DataType::Date32 | DataType::Date64             => "DATE".into(),
        DataType::Timestamp(_, None)                    => "TIMESTAMP".into(),
        DataType::Timestamp(_, Some(_))                 => "TIMESTAMP WITH TIME ZONE".into(),
        DataType::Time32(_) | DataType::Time64(_)       => "INTERVAL DAY(1) TO SECOND(6)".into(),
        DataType::Duration(_) | DataType::Interval(_)     => "INTERVAL DAY(9) TO SECOND(6)".into(),
        DataType::FixedSizeBinary(16)                   => "RAW(16)".into(),
        DataType::Binary | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)                  => "BLOB".into(),
        DataType::Decimal128(p, s) if *s > 0           => format!("NUMBER({p},{s})"),
        DataType::Decimal128(p, _)                      => format!("NUMBER({p})"),
        DataType::List(_) | DataType::LargeList(_)
        | DataType::Struct(_) | DataType::Map(_, _)    => "CLOB".into(),
        _                                               => "VARCHAR2(4000)".into(),
    }
}

fn arrow_to_sql_mysql(dt: &DataType) -> String {
    match dt {
        DataType::Boolean                               => "TINYINT(1)".into(),
        DataType::Int8                                  => "TINYINT".into(),
        DataType::Int16                                 => "SMALLINT".into(),
        DataType::Int32                                 => "INT".into(),
        DataType::Int64                                 => "BIGINT".into(),
        DataType::UInt8                                 => "TINYINT UNSIGNED".into(),
        DataType::UInt16                                => "SMALLINT UNSIGNED".into(),
        DataType::UInt32                                => "INT UNSIGNED".into(),
        DataType::UInt64                                => "BIGINT UNSIGNED".into(),
        DataType::Float32                               => "FLOAT".into(),
        DataType::Float64                               => "DOUBLE".into(),
        DataType::Utf8   | DataType::LargeUtf8          => "TEXT".into(),
        DataType::Date32 | DataType::Date64             => "DATE".into(),
        DataType::Timestamp(_, _)                       => "DATETIME(6)".into(),
        DataType::Time32(_) | DataType::Time64(_)       => "TIME(6)".into(),
        DataType::FixedSizeBinary(16)                   => "BINARY(16)".into(),
        DataType::Binary | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)                  => "LONGBLOB".into(),
        DataType::Decimal128(p, s) if *s > 0           => format!("DECIMAL({p},{s})"),
        DataType::Decimal128(p, _)                      => format!("DECIMAL({p},0)"),
        DataType::List(_) | DataType::LargeList(_)
        | DataType::Struct(_) | DataType::Map(_, _)    => "JSON".into(),
        _                                               => "TEXT".into(),
    }
}

fn arrow_to_sql_databricks(dt: &DataType) -> String {
    match dt {
        DataType::Boolean                               => "BOOLEAN".into(),
        DataType::Int8                                  => "TINYINT".into(),
        DataType::Int16                                 => "SMALLINT".into(),
        DataType::Int32                                 => "INT".into(),
        DataType::Int64                                 => "BIGINT".into(),
        DataType::UInt8                                 => "SMALLINT".into(),
        DataType::UInt16                                => "INT".into(),
        DataType::UInt32                                => "BIGINT".into(),
        DataType::UInt64                                => "BIGINT".into(),
        DataType::Float32                               => "FLOAT".into(),
        DataType::Float64                               => "DOUBLE".into(),
        DataType::Utf8   | DataType::LargeUtf8          => "STRING".into(),
        DataType::Date32 | DataType::Date64             => "DATE".into(),
        DataType::Timestamp(_, _)                       => "TIMESTAMP".into(),
        DataType::Time32(_) | DataType::Time64(_)       => "STRING".into(),
        DataType::Binary | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)                  => "BINARY".into(),
        DataType::Decimal128(p, s) if *s > 0           => format!("DECIMAL({p},{s})"),
        DataType::Decimal128(p, _)                      => format!("DECIMAL({p},0)"),
        DataType::List(_) | DataType::LargeList(_)
        | DataType::Struct(_) | DataType::Map(_, _)    => "STRING".into(),
        _                                               => "STRING".into(),
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_fixed_binary_16_is_uuid() {
        assert_eq!(arrow_to_sql_pg(&DataType::FixedSizeBinary(16)), "UUID");
    }

    #[test]
    fn test_pg_fixed_binary_other_is_bytea() {
        assert_eq!(arrow_to_sql_pg(&DataType::FixedSizeBinary(32)), "BYTEA");
    }

    #[test]
    fn test_mssql_fixed_binary_16_is_uniqueidentifier() {
        assert_eq!(arrow_to_sql_mssql(&DataType::FixedSizeBinary(16)), "UNIQUEIDENTIFIER");
    }

    #[test]
    fn test_mssql_fixed_binary_other_is_varbinary() {
        assert_eq!(arrow_to_sql_mssql(&DataType::FixedSizeBinary(32)), "VARBINARY(MAX)");
    }

    #[test]
    fn test_pg_utf8_is_text() {
        assert_eq!(arrow_to_sql_pg(&DataType::Utf8), "TEXT");
    }

    #[test]
    fn test_pg_list_is_jsonb() {
        let list_type = DataType::List(
            std::sync::Arc::new(arrow::datatypes::Field::new("item", DataType::Utf8, true))
        );
        assert_eq!(arrow_to_sql_pg(&list_type), "JSONB");
    }

    #[test]
    fn test_pg_struct_is_jsonb() {
        let struct_type = DataType::Struct(
            vec![arrow::datatypes::Field::new("a", DataType::Int32, true)].into()
        );
        assert_eq!(arrow_to_sql_pg(&struct_type), "JSONB");
    }

    #[test]
    fn test_oracle_fixed_binary_16_is_raw16() {
        assert_eq!(arrow_to_sql_oracle(&DataType::FixedSizeBinary(16)), "RAW(16)");
    }

    #[test]
    fn test_oracle_fixed_binary_other_is_blob() {
        assert_eq!(arrow_to_sql_oracle(&DataType::FixedSizeBinary(32)), "BLOB");
    }

    #[test]
    fn test_mysql_fixed_binary_16_is_binary16() {
        assert_eq!(arrow_to_sql_mysql(&DataType::FixedSizeBinary(16)), "BINARY(16)");
    }

    #[test]
    fn test_mysql_fixed_binary_other_is_longblob() {
        assert_eq!(arrow_to_sql_mysql(&DataType::FixedSizeBinary(32)), "LONGBLOB");
    }

    #[test]
    fn test_mysql_struct_is_json() {
        let struct_type = DataType::Struct(
            vec![arrow::datatypes::Field::new("a", DataType::Int32, true)].into()
        );
        assert_eq!(arrow_to_sql_mysql(&struct_type), "JSON");
    }

    #[test]
    fn test_public_api_dispatches_correctly() {
        // Verify the public function dispatches to the right dialect.
        assert_eq!(
            arrow_type_to_sql_dialect(&DataType::FixedSizeBinary(16), SqlDialect::Postgres),
            "UUID"
        );
        assert_eq!(
            arrow_type_to_sql_dialect(&DataType::FixedSizeBinary(16), SqlDialect::Mssql),
            "UNIQUEIDENTIFIER"
        );
        assert_eq!(
            arrow_type_to_sql_dialect(&DataType::FixedSizeBinary(16), SqlDialect::Oracle),
            "RAW(16)"
        );
        assert_eq!(
            arrow_type_to_sql_dialect(&DataType::FixedSizeBinary(16), SqlDialect::Mysql),
            "BINARY(16)"
        );
        assert_eq!(
            arrow_type_to_sql_dialect(&DataType::FixedSizeBinary(16), SqlDialect::Databricks),
            "BINARY"
        );
    }
}