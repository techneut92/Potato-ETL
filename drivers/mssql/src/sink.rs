//! MSSQL write sink — `MssqlWriteDB`.
//!
//! Four write paths: tiberius direct, tiberius staging, bcp subprocess, ODBC.
//!
//! ## Staging table behaviour
//!
//! For the tiberius path, staging (`#etl_bulk_stage`) is:
//! - **Auto-enabled** for `upsert`, `insert_ignore`, and `merge_delete`
//!   strategies (MERGE requires a source table — there is no alternative).
//! - **Off** for `append` and `truncate` (direct bulk insert).
//! - **Opt-in** for `append` via `staging_table: true` — uses
//!   `INSERT INTO target WITH (TABLOCK) SELECT … FROM #staging` which
//!   enables minimal logging when the target has no clustered index or the
//!   database recovery model is `SIMPLE`/`BULK_LOGGED`.
//!
//! This can be overridden via `options.mssql.staging_table`:
//!
//! ```yaml
//! write_db:
//!   options:
//!     mssql:
//!       staging_table: true    # force staging for all strategies
//!       # staging_table: false # disable staging (dangerous for merge modes!)
//! ```
//!
//! BCP staging is controlled separately via `bcp_staging` in step options.

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array,
    DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray,
    FixedSizeBinaryArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeStringArray, StringArray,
    Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray,
    TimestampSecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use tiberius::{ColumnData, TokenRow};
use tiberius::time::{
    Date              as MssqlDate,
    DateTime2         as MssqlDateTime2,
    DateTimeOffset    as MssqlDateTimeOffset,
    Time              as MssqlTime,
};
use tiberius::numeric::Numeric as MssqlNumeric;
use tiberius::xml::XmlData as MssqlXmlData;
use uuid::Uuid;

use potato_etl_common::config::{DatabaseSchemaConfig, StepDriverOptions};
use potato_etl_common::db::{TableMode, WriteStrategy};
use potato_etl_common::db::common::SinkConfig;
use potato_etl_common::db::traits::SinkBuilder;
use potato_etl_common::schema::constants::META_DB_TYPE;
use potato_etl_common::schema::ddl::{generate_ddl_with_schema, DdlOptions, SqlDialect};
use crate::util::{MssqlClient, MssqlConnParams, MssqlWriteMode, introspect_table_columns};
#[cfg(feature = "bcp")]
use crate::bcp as bcp_mod;

// ── Decimal string parser ─────────────────────────────────────────────────────

fn parse_decimal_str(s: &str) -> Option<MssqlNumeric> {
    let s = s.trim();
    if s.is_empty() { return None; }
    let negative = s.starts_with('-');
    let digits   = if negative { &s[1..] } else { s };
    let (int_part, frac_part) = match digits.find('.') {
        Some(p) => (&digits[..p], &digits[p + 1..]),
        None    => (digits, ""),
    };
    let scale: u8 = frac_part.len() as u8;
    let combined: i128 = format!("{int_part}{frac_part}").parse().ok()?;
    let value = if negative { -combined } else { combined };
    Some(MssqlNumeric::new_with_scale(value, scale))
}

// ── Row conversion ────────────────────────────────────────────────────────────

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
enum RowConversionMode { Direct, Staging }

#[inline]
fn batch_to_direct_rows(batch: &RecordBatch) -> Vec<TokenRow<'static>> {
    batch_to_rows(batch, RowConversionMode::Direct)
}

#[inline]
fn batch_to_staging_rows(batch: &RecordBatch) -> Vec<TokenRow<'static>> {
    batch_to_rows(batch, RowConversionMode::Staging)
}

fn batch_to_rows(batch: &RecordBatch, mode: RowConversionMode) -> Vec<TokenRow<'static>> {
    let n   = batch.num_rows();
    let nc  = batch.num_columns();
    let sch = batch.schema();

    let mut rows: Vec<TokenRow<'static>> = (0..n).map(|_| TokenRow::new()).collect();

    for col_idx in 0..nc {
        let col      = batch.column(col_idx);
        let field    = sch.field(col_idx);
        let dt       = field.data_type();
        let src_type = field.metadata().get("source_db_type").map(|s| s.as_str()).unwrap_or("");

        match dt {
            DataType::Boolean => {
                let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::Bit(
                        if arr.is_null(i) { None } else { Some(arr.value(i)) }
                    ));
                }
            }
            DataType::Int8 => {
                let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::I16(
                        if arr.is_null(i) { None } else { Some(arr.value(i) as i16) }
                    ));
                }
            }
            DataType::Int16 => {
                let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::I16(
                        if arr.is_null(i) { None } else { Some(arr.value(i)) }
                    ));
                }
            }
            DataType::UInt8 => {
                let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::I16(
                        if arr.is_null(i) { None } else { Some(arr.value(i) as i16) }
                    ));
                }
            }
            DataType::Int32 => {
                let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::I32(
                        if arr.is_null(i) { None } else { Some(arr.value(i)) }
                    ));
                }
            }
            DataType::UInt16 => {
                let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::I32(
                        if arr.is_null(i) { None } else { Some(arr.value(i) as i32) }
                    ));
                }
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::I64(
                        if arr.is_null(i) { None } else { Some(arr.value(i)) }
                    ));
                }
            }
            DataType::UInt32 => {
                let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::I64(
                        if arr.is_null(i) { None } else { Some(arr.value(i) as i64) }
                    ));
                }
            }
            DataType::UInt64 => {
                let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::Numeric(if arr.is_null(i) {
                        None
                    } else {
                        Some(MssqlNumeric::new_with_scale(arr.value(i) as i128, 0))
                    }));
                }
            }
            DataType::Float32 => {
                let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::F32(
                        if arr.is_null(i) { None } else { Some(arr.value(i)) }
                    ));
                }
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::F64(
                        if arr.is_null(i) { None } else { Some(arr.value(i)) }
                    ));
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::DateTimeOffset(if arr.is_null(i) {
                        None
                    } else {
                        let us            = arr.value(i);
                        let days_i        = us.div_euclid(86_400_000_000_i64) as i32;
                        let micros_in_day = us.rem_euclid(86_400_000_000_i64) as u64;
                        let sql_days      = (days_i + 719_162) as u32;
                        let date = MssqlDate::new(sql_days);
                        let time = MssqlTime::new(micros_in_day, 6);
                        let dt2  = MssqlDateTime2::new(date, time);
                        Some(MssqlDateTimeOffset::new(dt2, 0))
                    }));
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::DateTime2(if arr.is_null(i) {
                        None
                    } else {
                        let us            = arr.value(i);
                        let days_i        = us.div_euclid(86_400_000_000_i64) as i32;
                        let micros_in_day = us.rem_euclid(86_400_000_000_i64) as u64;
                        let sql_days      = (days_i + 719_162) as u32;
                        let date = MssqlDate::new(sql_days);
                        let time = MssqlTime::new(micros_in_day, 6);
                        Some(MssqlDateTime2::new(date, time))
                    }));
                }
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                let arr = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::DateTime2(if arr.is_null(i) {
                        None
                    } else {
                        let us            = arr.value(i) * 1_000_000;
                        let days_i        = us.div_euclid(86_400_000_000_i64) as i32;
                        let micros_in_day = us.rem_euclid(86_400_000_000_i64) as u64;
                        let sql_days      = (days_i + 719_162) as u32;
                        let date = MssqlDate::new(sql_days);
                        let time = MssqlTime::new(micros_in_day, 6);
                        Some(MssqlDateTime2::new(date, time))
                    }));
                }
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let arr = col.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::DateTime2(if arr.is_null(i) {
                        None
                    } else {
                        let us            = arr.value(i) * 1_000;
                        let days_i        = us.div_euclid(86_400_000_000_i64) as i32;
                        let micros_in_day = us.rem_euclid(86_400_000_000_i64) as u64;
                        let sql_days      = (days_i + 719_162) as u32;
                        let date = MssqlDate::new(sql_days);
                        let time = MssqlTime::new(micros_in_day, 6);
                        Some(MssqlDateTime2::new(date, time))
                    }));
                }
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::DateTime2(if arr.is_null(i) {
                        None
                    } else {
                        let us            = arr.value(i) / 1_000;
                        let days_i        = us.div_euclid(86_400_000_000_i64) as i32;
                        let micros_in_day = us.rem_euclid(86_400_000_000_i64) as u64;
                        let sql_days      = (days_i + 719_162) as u32;
                        let date = MssqlDate::new(sql_days);
                        let time = MssqlTime::new(micros_in_day, 6);
                        Some(MssqlDateTime2::new(date, time))
                    }));
                }
            }
            DataType::Date32 => {
                let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::Date(if arr.is_null(i) {
                        None
                    } else {
                        let sql_days = (arr.value(i) as i64 + 719_162_i64) as u32;
                        Some(MssqlDate::new(sql_days))
                    }));
                }
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let arr = col.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
                if mode == RowConversionMode::Direct {
                    for i in 0..n {
                        rows[i].push(ColumnData::Time(if arr.is_null(i) {
                            None
                        } else {
                            let us = arr.value(i);
                            Some(MssqlTime::new(us as u64 * 10, 7))
                        }));
                    }
                } else {
                    for i in 0..n {
                        rows[i].push(if arr.is_null(i) {
                            ColumnData::String(None)
                        } else {
                            let us = arr.value(i);
                            let h  = us / 3_600_000_000;
                            let m  = (us % 3_600_000_000) / 60_000_000;
                            let s  = (us % 60_000_000) / 1_000_000;
                            let f  = us % 1_000_000;
                            ColumnData::String(Some(Cow::Owned(format!("{h:02}:{m:02}:{s:02}.{f:06}"))))
                        });
                    }
                }
            }
            DataType::LargeBinary => {
                let arr = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::Binary(
                        if arr.is_null(i) { None } else { Some(Cow::Owned(arr.value(i).to_vec())) }
                    ));
                }
            }
            DataType::Binary => {
                let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::Binary(
                        if arr.is_null(i) { None } else { Some(Cow::Owned(arr.value(i).to_vec())) }
                    ));
                }
            }
            DataType::FixedSizeBinary(_) => {
                let arr = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                for i in 0..n {
                    rows[i].push(ColumnData::Binary(
                        if arr.is_null(i) { None } else { Some(Cow::Owned(arr.value(i).to_vec())) }
                    ));
                }
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                if mode == RowConversionMode::Staging {
                    for i in 0..n {
                        rows[i].push(if arr.is_null(i) {
                            ColumnData::String(None)
                        } else {
                            ColumnData::String(Some(Cow::Owned(arr.value(i).to_owned())))
                        });
                    }
                } else {
                    let target_db = field.metadata()
                        .get(META_DB_TYPE)
                        .map(|s| s.to_ascii_uppercase());
                    let target_db = target_db.as_deref().unwrap_or("");
                    let is_numeric = target_db.starts_with("NUMERIC")
                        || target_db.starts_with("DECIMAL")
                        || target_db.starts_with("NUMBER")
                        || target_db.starts_with("MONEY")
                        || target_db.starts_with("SMALLMONEY")
                        || (target_db.is_empty()
                            && matches!(src_type, "numeric" | "decimal" | "number"));
                    let is_int_target = matches!(target_db, "BIGINT" | "INT" | "SMALLINT" | "TINYINT");
                    let is_guid = target_db == "UNIQUEIDENTIFIER";
                    let is_xml  = target_db == "XML";
                    for i in 0..n {
                        rows[i].push(if arr.is_null(i) {
                            if is_numeric        { ColumnData::Numeric(None) }
                            else if is_int_target { ColumnData::I64(None) }
                            else if is_guid       { ColumnData::Guid(None) }
                            else if is_xml        { ColumnData::Xml(None) }
                            else                  { ColumnData::String(None) }
                        } else if is_numeric {
                            ColumnData::Numeric(parse_decimal_str(arr.value(i)))
                        } else if is_int_target {
                            ColumnData::I64(arr.value(i).trim().parse::<i64>().ok())
                        } else if is_guid {
                            ColumnData::Guid(arr.value(i).trim().parse::<Uuid>().ok())
                        } else if is_xml {
                            ColumnData::Xml(Some(Cow::Owned(MssqlXmlData::new(arr.value(i).to_owned()))))
                        } else {
                            ColumnData::String(Some(Cow::Owned(arr.value(i).to_owned())))
                        });
                    }
                }
            }
            DataType::LargeUtf8 => {
                let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                for i in 0..n {
                    rows[i].push(if arr.is_null(i) {
                        ColumnData::String(None)
                    } else {
                        ColumnData::String(Some(Cow::Owned(arr.value(i).to_owned())))
                    });
                }
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let arr = col.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
                if mode == RowConversionMode::Direct {
                    for i in 0..n {
                        rows[i].push(ColumnData::Time(if arr.is_null(i) {
                            None
                        } else {
                            // MSSQL TIME(7) uses 100ns increments.
                            // Arrow Time64Nanosecond is in nanoseconds → divide by 100.
                            let ns = arr.value(i);
                            Some(MssqlTime::new((ns / 100) as u64, 7))
                        }));
                    }
                } else {
                    for i in 0..n {
                        rows[i].push(if arr.is_null(i) {
                            ColumnData::String(None)
                        } else {
                            let ns = arr.value(i);
                            let us = ns / 1_000;
                            let h  = us / 3_600_000_000;
                            let m  = (us % 3_600_000_000) / 60_000_000;
                            let s  = (us % 60_000_000) / 1_000_000;
                            let f  = us % 1_000_000;
                            ColumnData::String(Some(Cow::Owned(format!("{h:02}:{m:02}:{s:02}.{f:06}"))))
                        });
                    }
                }
            }
            // ── Duration → NVARCHAR string (HH:MM:SS.ffffff) ────────────────
            // MSSQL has no INTERVAL type; durations are sent as formatted strings.
            DataType::Duration(TimeUnit::Second) => {
                let arr = col.as_any().downcast_ref::<DurationSecondArray>().unwrap();
                for i in 0..n {
                    rows[i].push(if arr.is_null(i) {
                        ColumnData::String(None)
                    } else {
                        ColumnData::String(Some(Cow::Owned(format_duration_us(arr.value(i) * 1_000_000))))
                    });
                }
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                let arr = col.as_any().downcast_ref::<DurationMillisecondArray>().unwrap();
                for i in 0..n {
                    rows[i].push(if arr.is_null(i) {
                        ColumnData::String(None)
                    } else {
                        ColumnData::String(Some(Cow::Owned(format_duration_us(arr.value(i) * 1_000))))
                    });
                }
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                let arr = col.as_any().downcast_ref::<DurationMicrosecondArray>().unwrap();
                for i in 0..n {
                    rows[i].push(if arr.is_null(i) {
                        ColumnData::String(None)
                    } else {
                        ColumnData::String(Some(Cow::Owned(format_duration_us(arr.value(i)))))
                    });
                }
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                let arr = col.as_any().downcast_ref::<DurationNanosecondArray>().unwrap();
                for i in 0..n {
                    rows[i].push(if arr.is_null(i) {
                        ColumnData::String(None)
                    } else {
                        ColumnData::String(Some(Cow::Owned(format_duration_us(arr.value(i) / 1_000))))
                    });
                }
            }
            _ => {
                for i in 0..n {
                    rows[i].push(if col.is_null(i) {
                        ColumnData::String(None)
                    } else {
                        match arrow::util::display::array_value_to_string(col.as_ref(), i) {
                            Ok(s)  => ColumnData::String(Some(Cow::Owned(s))),
                            Err(_) => ColumnData::String(None),
                        }
                    });
                }
            }
        }
    }
    rows
}

/// Formats a duration in microseconds as `[-]HH:MM:SS.ffffff`.
fn format_duration_us(us: i64) -> String {
    let neg = us < 0;
    let abs = us.unsigned_abs();
    let h = abs / 3_600_000_000;
    let m = (abs % 3_600_000_000) / 60_000_000;
    let s = (abs % 60_000_000) / 1_000_000;
    let f = abs % 1_000_000;
    if neg {
        format!("-{h:02}:{m:02}:{s:02}.{f:06}")
    } else {
        format!("{h:02}:{m:02}:{s:02}.{f:06}")
    }
}

// ── SQL generators ────────────────────────────────────────────────────────────

/// True if any column name in `schema` is a T-SQL reserved word.
///
/// tiberius's `bulk_insert(table)` sends an `INSERT BULK <table> (col type, …)`
/// statement to the server unquoted, so columns named after reserved words
/// (e.g. `KEY`, `USER`, `ORDER`) trigger a syntax error. When this returns
/// `true` we route through `do_bulk_insert_partial`, which uses an explicit
/// bracketed column list (`[KEY]`) and avoids the issue.
fn schema_has_reserved_column(schema: &SchemaRef) -> bool {
    static RESERVED: &[&str] = &[
        "ABSOLUTE","ACTION","ADD","ALL","ALTER","AND","ANY","AS","ASC","AUTHORIZATION",
        "BACKUP","BEGIN","BETWEEN","BREAK","BROWSE","BULK","BY","CASCADE","CASE","CHECK",
        "CHECKPOINT","CLOSE","CLUSTERED","COALESCE","COLLATE","COLUMN","COMMIT","COMPUTE",
        "CONSTRAINT","CONTAINS","CONTAINSTABLE","CONTINUE","CONVERT","CREATE","CROSS",
        "CURRENT","CURRENT_DATE","CURRENT_TIME","CURRENT_TIMESTAMP","CURRENT_USER","CURSOR",
        "DATABASE","DBCC","DEALLOCATE","DECLARE","DEFAULT","DELETE","DENY","DESC","DISK",
        "DISTINCT","DISTRIBUTED","DOUBLE","DROP","DUMP","ELSE","END","ERRLVL","ESCAPE",
        "EXCEPT","EXEC","EXECUTE","EXISTS","EXIT","EXTERNAL","FETCH","FILE","FILLFACTOR",
        "FOR","FOREIGN","FREETEXT","FREETEXTTABLE","FROM","FULL","FUNCTION","GOTO","GRANT",
        "GROUP","HAVING","HOLDLOCK","IDENTITY","IDENTITY_INSERT","IDENTITYCOL","IF","IN",
        "INDEX","INNER","INSERT","INTERSECT","INTO","IS","JOIN","KEY","KILL","LEFT","LIKE",
        "LINENO","LOAD","MERGE","NATIONAL","NOCHECK","NONCLUSTERED","NOT","NULL","NULLIF",
        "OF","OFF","OFFSETS","ON","OPEN","OPENDATASOURCE","OPENQUERY","OPENROWSET","OPENXML",
        "OPTION","OR","ORDER","OUTER","OVER","PERCENT","PIVOT","PLAN","PRECISION","PRIMARY",
        "PRINT","PROC","PROCEDURE","PUBLIC","RAISERROR","READ","READTEXT","RECONFIGURE",
        "REFERENCES","REPLICATION","RESTORE","RESTRICT","RETURN","REVERT","REVOKE","RIGHT",
        "ROLLBACK","ROWCOUNT","ROWGUIDCOL","RULE","SAVE","SCHEMA","SECURITYAUDIT","SELECT",
        "SEMANTICKEYPHRASETABLE","SEMANTICSIMILARITYDETAILSTABLE","SEMANTICSIMILARITYTABLE",
        "SESSION_USER","SET","SETUSER","SHUTDOWN","SOME","STATISTICS","SYSTEM_USER","TABLE",
        "TABLESAMPLE","TEXTSIZE","THEN","TO","TOP","TRAN","TRANSACTION","TRIGGER","TRUNCATE",
        "TRY_CONVERT","TSEQUAL","UNION","UNIQUE","UNPIVOT","UPDATE","UPDATETEXT","USE","USER",
        "VALUES","VARYING","VIEW","WAITFOR","WHEN","WHERE","WHILE","WITH","WITHIN GROUP",
        "WRITETEXT",
    ];
    schema.fields().iter().any(|f| {
        let upper = f.name().to_ascii_uppercase();
        RESERVED.iter().any(|kw| *kw == upper)
    })
}

fn staging_sql_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean                                      => "BIT",
        DataType::Int8                                         => "SMALLINT",
        DataType::Int16 | DataType::UInt8                      => "SMALLINT",
        DataType::Int32 | DataType::UInt16                     => "INT",
        DataType::Int64 | DataType::UInt32                     => "BIGINT",
        DataType::UInt64                                       => "DECIMAL(20,0)",
        DataType::Float32                                      => "REAL",
        DataType::Float64                                      => "FLOAT",
        DataType::Timestamp(TimeUnit::Microsecond, None)       => "DATETIME2(6)",
        DataType::Timestamp(TimeUnit::Microsecond, Some(_))    => "DATETIMEOFFSET(6)",
        DataType::Timestamp(TimeUnit::Second, _)               => "DATETIME2(6)",
        DataType::Timestamp(TimeUnit::Millisecond, _)          => "DATETIME2(6)",
        DataType::Timestamp(TimeUnit::Nanosecond, _)           => "DATETIME2(6)",
        DataType::Date32                                       => "DATE",
        DataType::Time64(_)                                    => "NVARCHAR(MAX)",
        // NOTE: FixedSizeBinary(16) stays VARBINARY(MAX) here because
        // `batch_to_staging_rows` sends it as ColumnData::Binary.  The coercion
        // plan converts FixedSizeBinary(16) → Utf8 (FormatUuid) before this
        // point anyway, so staging sees Utf8 → NVARCHAR(MAX).
        DataType::LargeBinary | DataType::Binary | DataType::FixedSizeBinary(_) => "VARBINARY(MAX)",
        _                                                      => "NVARCHAR(MAX)",
    }
}

fn create_staging_ddl(schema: &SchemaRef) -> String {
    let cols: Vec<String> = schema.fields().iter().map(|f| {
        format!("    [{}] {} NULL", f.name(), staging_sql_type(f.data_type()))
    }).collect();
    format!("CREATE TABLE #etl_bulk_stage (\n{}\n)", cols.join(",\n"))
}

fn staging_insert_select_sql(full_table: &str, col_names: &[String]) -> String {
    let cols = col_names.iter().map(|c| format!("[{c}]")).collect::<Vec<_>>().join(", ");
    format!("INSERT INTO {full_table} WITH (TABLOCK) ({cols}) SELECT {cols} FROM #etl_bulk_stage")
}

fn staging_insert_ignore_sql(full_table: &str, col_names: &[String], pk_cols: &[String]) -> String {
    let cols_sql    = col_names.iter().map(|c| format!("[{c}]")).collect::<Vec<_>>().join(", ");
    let insert_vals = col_names.iter().map(|c| format!("[S].[{c}]")).collect::<Vec<_>>().join(", ");
    let on_clause   = pk_cols.iter()
        .map(|k| format!("[T].[{k}] = [S].[{k}]")).collect::<Vec<_>>().join(" AND ");
    format!(
        "MERGE INTO {full_table} WITH (HOLDLOCK) AS [T] \
         USING #etl_bulk_stage AS [S] ON {on_clause} \
         WHEN NOT MATCHED THEN INSERT ({cols_sql}) VALUES ({insert_vals});"
    )
}

fn staging_merge_sql(
    full_table:  &str,
    col_names:   &[String],
    pk_cols:     &[String],
    with_delete: bool,
) -> String {
    let cols_sql    = col_names.iter().map(|c| format!("[{c}]")).collect::<Vec<_>>().join(", ");
    let on_clause   = pk_cols.iter()
        .map(|k| format!("[T].[{k}] = [S].[{k}]")).collect::<Vec<_>>().join(" AND ");
    let update_set  = col_names.iter()
        .filter(|c| !pk_cols.contains(c))
        .map(|c| format!("[T].[{c}] = [S].[{c}]"))
        .collect::<Vec<_>>().join(", ");
    let insert_vals = col_names.iter().map(|c| format!("[S].[{c}]")).collect::<Vec<_>>().join(", ");
    let del = if with_delete { "\n         WHEN NOT MATCHED BY SOURCE THEN DELETE" } else { "" };
    format!(
        "MERGE INTO {full_table} WITH (HOLDLOCK) AS [T] \
         USING #etl_bulk_stage AS [S] ON {on_clause} \
         WHEN MATCHED THEN UPDATE SET {update_set} \
         WHEN NOT MATCHED BY TARGET THEN INSERT ({cols_sql}) VALUES ({insert_vals}){del};"
    )
}

fn create_staging_pk_index_sql(pk_cols: &[String]) -> String {
    let cols = pk_cols.iter().map(|c| format!("[{c}] ASC")).collect::<Vec<_>>().join(", ");
    format!("CREATE CLUSTERED INDEX [IX_etl_stage_pk] ON #etl_bulk_stage ({cols})")
}

fn schema_has_date_before_time64(schema: &SchemaRef) -> bool {
    let mut seen_date = false;
    for f in schema.fields() {
        match f.data_type() {
            DataType::Date32 | DataType::Date64 => seen_date = true,
            DataType::Time64(_) if seen_date    => return true,
            _ => {}
        }
    }
    false
}

// ── MssqlWriteDB ──────────────────────────────────────────────────────────────

pub struct MssqlWriteDB {
    params:          MssqlConnParams,
    pub cfg:         SinkConfig,
    client:          Option<MssqlClient>,
    saved_schema:    Option<SchemaRef>,
    first_batch:     bool,
    in_transaction:  bool,
    staging_rows:    usize,
    row_buffer:      Vec<TokenRow<'static>>,
    bulk_threshold:  usize,
    use_staging:     bool,
    /// User-configured staging preference from driver options.
    /// `None` = auto (staging for upsert/insert_ignore/merge_delete),
    /// `Some(true)` = force, `Some(false)` = disable (dangerous for merge modes).
    staging_table_opt: Option<bool>,
    mode:             MssqlWriteMode,
    bcp_path:         Option<String>,
    #[cfg(feature = "bcp")]
    bcp_process:      Option<bcp_mod::BcpProcess>,
    bcp_staging_full: Option<String>,
    bcp_staging:      Option<bool>,
    #[cfg(feature = "odbc")]
    odbc_writer:      Option<crate::odbc::OdbcWriter>,
    alignment:        Option<potato_etl_common::db::common::alignment::ColumnAlignment>,
    target_columns:   Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>,
}

impl MssqlWriteDB {
    pub fn new(conn_str: &str) -> anyhow::Result<Self> {
        let params = MssqlConnParams::parse(conn_str)?;
        let mode            = params.mode;
        let bcp_path        = params.bcp_path.clone();
        let bulk_threshold  = params.batch_size.unwrap_or(10_000);
        let bcp_staging     = params.bcp_staging;
        Ok(Self {
            params,
            cfg:               SinkConfig::new("dbo"),
            client:            None,
            saved_schema:      None,
            first_batch:       true,
            in_transaction:    false,
            staging_rows:      0,
            row_buffer:        Vec::new(),
            bulk_threshold,
            use_staging:       false,
            staging_table_opt: None,
            mode,
            bcp_path,
            #[cfg(feature = "bcp")]
            bcp_process:       None,
            bcp_staging_full:  None,
            bcp_staging,
            #[cfg(feature = "odbc")]
            odbc_writer:       None,
            alignment:         None,
            target_columns:    None,
        })
    }

    pub fn apply_driver_options(mut self, opts: &StepDriverOptions) -> Self {
        if let Some(m) = opts.effective_mssql_mode() {
            match m {
                "bcp"      => self.mode = MssqlWriteMode::Bcp,
                "odbc"     => self.mode = MssqlWriteMode::Odbc,
                "tiberius" => self.mode = MssqlWriteMode::Tiberius,
                _ => tracing::warn!(mode = m, "Unknown MSSQL step mode, ignoring"),
            }
        }
        if let Some(ref path) = opts.bcp_path {
            self.bcp_path = Some(path.clone());
            self.mode     = MssqlWriteMode::Bcp;
        }
        if let Some(n) = opts.mssql.as_ref().and_then(|m| m.batch_size) {
            self.bulk_threshold = n;
        }
        if let Some(s) = opts.bcp_staging {
            self.bcp_staging = Some(s);
        }
        if let Some(case) = opts.identifier_case {
            self.cfg.identifier_case = Some(case);
        }
        if let Some(ref mssql_opts) = opts.mssql {
            if let Some(st) = mssql_opts.staging_table {
                self.staging_table_opt = Some(st);
            }
            if !mssql_opts.init_sql.is_empty() {
                self.params.init_sql = mssql_opts.init_sql.clone();
            }
        }
        self
    }

    #[cfg(feature = "bcp")]
    fn bcp_uses_staging(&self) -> bool {
        match self.bcp_staging {
            Some(v) => v,
            None => matches!(
                self.cfg.write_strategy,
                WriteStrategy::Upsert | WriteStrategy::MergeDelete | WriteStrategy::InsertIgnore
            ),
        }
    }

    // ── write / flush (public, error-wrapped) ────────────────────────────────

    pub async fn write_batch(&mut self, batch: RecordBatch) -> anyhow::Result<usize> {
        if batch.num_rows() == 0 { return Ok(0); }
        let result = self.write_impl(batch).await;
        if let Err(ref e) = result {
            tracing::error!(error = %e, "MssqlWriteDB: write_impl failed");
            self.rollback_and_reset().await;
        }
        result
    }

    pub async fn flush_all(&mut self) -> anyhow::Result<()> {
        #[cfg(feature = "odbc")]
        if self.mode == MssqlWriteMode::Odbc {
            let result = self.flush_odbc().await;
            if let Err(ref e) = result {
                tracing::error!(error = %e, "MssqlWriteDB/odbc: flush failed");
                self.rollback_and_reset().await;
            }
            return result;
        }

        if !self.in_transaction {
            self.reset_state();
            return Ok(());
        }

        let full_table = format!("[{}].[{}]", self.cfg.schema_name, self.cfg.table);

        // ── bcp flush path ────────────────────────────────────────────────
        #[cfg(feature = "bcp")]
        if self.mode == MssqlWriteMode::Bcp {
            if let Some(proc) = self.bcp_process.take() {
                proc.finish().await.map_err(|e| e.context("bcp load failed"))?;
            }
            let uses_staging = self.bcp_staging_full.is_some();
            if !uses_staging {
                self.reset_state();
                return Ok(());
            }

            let staging_full = self.bcp_staging_full.clone().unwrap();
            let schema = match self.saved_schema.clone() {
                Some(s) => s,
                None    => { self.drop_bcp_staging().await; self.reset_state(); return Ok(()); }
            };
            let client = self.client.as_mut()
                .ok_or_else(|| anyhow::anyhow!("MssqlWriteDB/bcp: tiberius client missing at flush"))?;
            client.simple_query("BEGIN TRANSACTION").await?.into_results().await?;
            self.in_transaction = true;

            let pk_cols = potato_etl_common::db::pk_columns(&schema);
            if !pk_cols.is_empty() && matches!(
                &self.cfg.write_strategy,
                WriteStrategy::Upsert | WriteStrategy::MergeDelete | WriteStrategy::InsertIgnore
            ) {
                let idx_cols = pk_cols.iter().map(|c| format!("[{c}] ASC")).collect::<Vec<_>>().join(", ");
                let idx_sql  = format!("CREATE CLUSTERED INDEX [IX_etl_bcp_pk] ON {staging_full} ({idx_cols})");
                let idx_result = {
                    let c = self.client.as_mut().unwrap();
                    match c.simple_query(idx_sql.as_str()).await {
                        Ok(stream) => stream.into_results().await.map_err(|e| anyhow::anyhow!("{e}")),
                        Err(e) => Err(anyhow::anyhow!("{e}")),
                    }
                };
                if let Err(e) = idx_result {
                    self.do_rollback().await; self.drop_bcp_staging().await;
                    return Err(anyhow::anyhow!("bcp staging index failed: {e}"));
                }
            }

            let flush_sql = match &self.cfg.write_strategy {
                WriteStrategy::InsertIgnore => bcp_mod::merge_sql(&staging_full, &full_table, &schema, &pk_cols, false, true),
                WriteStrategy::Upsert       => bcp_mod::merge_sql(&staging_full, &full_table, &schema, &pk_cols, false, false),
                WriteStrategy::MergeDelete  => bcp_mod::merge_sql(&staging_full, &full_table, &schema, &pk_cols, true, false),
                _                           => bcp_mod::insert_select_sql(&staging_full, &full_table, &schema),
            };
            let flush_result = {
                let c = self.client.as_mut().unwrap();
                match c.simple_query(flush_sql.as_str()).await {
                    Ok(stream) => stream.into_results().await.map_err(|e| anyhow::anyhow!("{e}")),
                    Err(e) => Err(anyhow::anyhow!("{e}")),
                }
            };
            if let Err(e) = flush_result {
                self.do_rollback().await; self.drop_bcp_staging().await;
                return Err(anyhow::anyhow!("bcp flush: {e}"));
            }
            let commit_result = {
                let c = self.client.as_mut().unwrap();
                match c.simple_query("COMMIT TRANSACTION").await {
                    Ok(stream) => stream.into_results().await.map_err(|e| anyhow::anyhow!("{e}")),
                    Err(e) => Err(anyhow::anyhow!("{e}")),
                }
            };
            if let Err(e) = commit_result {
                self.do_rollback().await; self.drop_bcp_staging().await;
                return Err(anyhow::anyhow!("bcp COMMIT: {e}"));
            }
            self.in_transaction = false;
            tracing::info!(table = %self.cfg.table, rows = self.staging_rows, "MSSQL flush complete (bcp)");
            self.drop_bcp_staging().await;
            self.reset_state();
            return Ok(());
        }
        #[cfg(not(feature = "bcp"))]
        if self.mode == MssqlWriteMode::Bcp {
            anyhow::bail!("mode=bcp requires the `bcp` feature.");
        }

        let staging = self.use_staging;

        // ── Drain remaining row buffer ────────────────────────────────────
        if !self.row_buffer.is_empty() {
            let rows = std::mem::take(&mut self.row_buffer);
            let dest = if staging { "#etl_bulk_stage" } else { full_table.as_str() };
            let c = self.client.as_mut().unwrap();
            let is_partial = !staging && (
                self.alignment.as_ref().map(|a| !a.is_identity()).unwrap_or(false)
                || self.saved_schema.as_ref().map(|s| schema_has_reserved_column(s)).unwrap_or(false)
            );
            let insert_result = if is_partial {
                if let Some(ref schema) = self.saved_schema {
                    do_bulk_insert_partial(c, dest, schema, rows).await
                } else {
                    do_bulk_insert(c, dest, rows).await
                }
            } else {
                do_bulk_insert(c, dest, rows).await
            };
            if let Err(e) = insert_result {
                self.do_rollback().await;
                return Err(e.context("MSSQL buffer drain failed"));
            }
        }

        // ── Staging: PK index + MERGE ─────────────────────────────────────
        if staging {
            if let Some(ref schema) = self.saved_schema {
                let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
                let pk_cols = potato_etl_common::db::pk_columns(schema);
                if !pk_cols.is_empty() && matches!(
                    &self.cfg.write_strategy,
                    WriteStrategy::Upsert | WriteStrategy::MergeDelete | WriteStrategy::InsertIgnore
                ) {
                    let idx_sql = create_staging_pk_index_sql(&pk_cols);
                    let idx_result = {
                        let c = self.client.as_mut().unwrap();
                        match c.simple_query(idx_sql.as_str()).await {
                            Ok(stream) => stream.into_results().await.map_err(|e| anyhow::anyhow!("{e}")),
                            Err(e) => Err(anyhow::anyhow!("{e}")),
                        }
                    };
                    if let Err(e) = idx_result {
                        self.do_rollback().await;
                        return Err(anyhow::anyhow!("staging index: {e}"));
                    }
                }
                let flush_sql = match &self.cfg.write_strategy {
                    WriteStrategy::InsertIgnore => staging_insert_ignore_sql(&full_table, &col_names, &pk_cols),
                    WriteStrategy::Upsert       => staging_merge_sql(&full_table, &col_names, &pk_cols, false),
                    WriteStrategy::MergeDelete  => staging_merge_sql(&full_table, &col_names, &pk_cols, true),
                    _                           => staging_insert_select_sql(&full_table, &col_names),
                };
                let flush_result = {
                    let c = self.client.as_mut().unwrap();
                    match c.simple_query(flush_sql.as_str()).await {
                        Ok(stream) => stream.into_results().await.map_err(|e| anyhow::anyhow!("{e}")),
                        Err(e) => Err(anyhow::anyhow!("{e}")),
                    }
                };
                if let Err(e) = flush_result {
                    self.do_rollback().await;
                    return Err(anyhow::anyhow!("flush: {e}"));
                }
            }
        }

        // ── COMMIT ────────────────────────────────────────────────────────
        let commit_result = {
            let c = self.client.as_mut().unwrap();
            match c.simple_query("COMMIT TRANSACTION").await {
                Ok(stream) => stream.into_results().await.map_err(|e| anyhow::anyhow!("{e}")),
                Err(e) => Err(anyhow::anyhow!("{e}")),
            }
        };
        if let Err(e) = commit_result {
            self.do_rollback().await;
            return Err(anyhow::anyhow!("COMMIT: {e}"));
        }
        tracing::info!(table = %self.cfg.table, rows = self.staging_rows, "MSSQL flush complete");
        self.reset_state();
        Ok(())
    }

    // ── Column alignment ──────────────────────────────────────────────────────

    async fn compute_column_alignment(
        &mut self,
        batch_schema: &SchemaRef,
        did_create_table: bool,
    ) -> anyhow::Result<()> {
        use potato_etl_common::db::common::alignment::{
            self as align, MissingColumnBehavior, TargetColumn,
        };
        if self.alignment.is_some() { return Ok(()); }
        if did_create_table { return Ok(()); }
        if self.client.is_none() {
            self.client = Some(self.params.connect().await?);
        }
        let client = self.client.as_mut().unwrap();
        let table_cols = introspect_table_columns(client, &self.cfg.schema_name, &self.cfg.table).await?;
        let table_cols = match table_cols { Some(c) => c, None => return Ok(()) };
        let target_cols: Vec<TargetColumn> = table_cols.into_iter()
            .map(|c| TargetColumn { name: c.name, data_type: c.data_type, nullable: c.nullable, has_default: c.has_default })
            .collect();
        let table_display = format!("{}.{}", self.cfg.schema_name, self.cfg.table);
        let batch_schema_transformed = if let Some(case) = self.cfg.identifier_case {
            use arrow::datatypes::{Field, Schema};
            let fields: Vec<Field> = batch_schema.fields().iter()
                .map(|f| Field::new(case.transform(f.name()), f.data_type().clone(), f.is_nullable()).with_metadata(f.metadata().clone()))
                .collect();
            Arc::new(Schema::new_with_metadata(fields, batch_schema.metadata().clone()))
        } else {
            batch_schema.clone()
        };
        let result = align::compute_alignment(&batch_schema_transformed, &target_cols, MissingColumnBehavior::Skip, &table_display)?;
        self.alignment = Some(result);
        self.target_columns = Some(target_cols);
        Ok(())
    }

    fn align_batch(&self, mut batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        if let Some(case) = self.cfg.identifier_case {
            use arrow::datatypes::{Field, Schema};
            let old_schema = batch.schema();
            let fields: Vec<Field> = old_schema.fields().iter()
                .map(|f| Field::new(case.transform(f.name()), f.data_type().clone(), f.is_nullable()).with_metadata(f.metadata().clone()))
                .collect();
            let new_schema = Arc::new(Schema::new_with_metadata(fields, old_schema.metadata().clone()));
            batch = RecordBatch::try_new(new_schema, batch.columns().to_vec())?;
        }
        batch = match &self.alignment {
            Some(a) => potato_etl_common::db::common::alignment::apply_alignment(batch, a)?,
            None    => batch,
        };
        use potato_etl_common::db::common::type_coercion::{coerce_batch_for_target, TargetColumn};
        use crate::type_registry::MssqlTypeRegistry;
        let target_cols_adapted = self.target_columns.as_ref().map(|cols| {
            cols.iter().map(|c| TargetColumn { name: c.name.clone(), data_type: c.data_type.clone() }).collect::<Vec<_>>()
        });
        batch = coerce_batch_for_target(batch, target_cols_adapted.as_deref(), &MssqlTypeRegistry)?;
        Ok(batch)
    }

    // ── write_impl (tiberius path) ────────────────────────────────────────────

    async fn write_impl(&mut self, batch: RecordBatch) -> anyhow::Result<usize> {
        let num_rows = batch.num_rows();
        let schema   = batch.schema();
        let ddl_schema = self.cfg.ddl_schema.clone().unwrap_or_else(|| schema.clone());
        let full_table  = format!("[{}].[{}]", self.cfg.schema_name, self.cfg.table);
        let table       = self.cfg.table.clone();
        let schema_name = self.cfg.schema_name.clone();
        let first       = self.first_batch;
        self.first_batch = false;
        let table_prepared      = self.cfg.table_prepared;
        self.cfg.table_prepared = true;
        let is_create_if_not_exists = matches!(self.cfg.table_mode, TableMode::CreateIfNotExists);
        let is_drop_and_replace     = matches!(self.cfg.table_mode, TableMode::DropAndReplace);
        let is_clear_and_insert     = matches!(self.cfg.write_strategy, WriteStrategy::Truncate);

        // ── bcp path (feature-gated) ──────────────────────────────────────
        #[cfg(feature = "bcp")]
        if self.mode == MssqlWriteMode::Bcp {
            // (Abbreviated — full BCP path logic preserved from core)
            let bcp_needs_init = self.bcp_process.is_none();
            let mut use_staging = false;
            if bcp_needs_init {
                use_staging = self.bcp_uses_staging();
                if self.client.is_none() { self.client = Some(self.params.connect().await?); }
                let table_already_exists = if is_create_if_not_exists && !is_drop_and_replace {
                    let client = self.client.as_mut().unwrap();
                    introspect_table_columns(client, &schema_name, &table).await?.is_some()
                } else {
                    false
                };
                {
                    let client = self.client.as_mut().unwrap();
                    if is_drop_and_replace {
                        client.simple_query(format!("DROP TABLE IF EXISTS {full_table}")).await?.into_results().await?;
                    }
                    if (is_drop_and_replace || is_create_if_not_exists) && !table_already_exists {
                        let db_config = self.cfg.database_schema_config.as_ref();
                        let stmts = generate_ddl_with_schema(&table, Some(&schema_name), &ddl_schema, SqlDialect::Mssql, None, db_config, DdlOptions::default());
                        for stmt in &stmts.pre_create {
                            client.simple_query(stmt.to_string()).await?.into_results().await?;
                        }
                        client.simple_query(stmts.create_table).await?.into_results().await?;
                        for stmt in &stmts.post_create {
                            for batch_sql in stmt.split("\nGO\n").map(|s| s.trim()).filter(|s| !s.is_empty()) {
                                match client.simple_query(batch_sql.to_string()).await {
                                    Ok(q) => { let _ = q.into_results().await; }
                                    Err(e) => tracing::warn!("MSSQL post-create DDL ignored: {e:#}"),
                                }
                            }
                        }
                    } else if table_already_exists {
                        tracing::info!(table = %self.cfg.table, "MSSQL table exists — skipping DDL (bcp)");
                    }
                    if is_clear_and_insert && !use_staging {
                        client.simple_query(format!("TRUNCATE TABLE {full_table}")).await?.into_results().await?;
                    }
                }
                self.compute_column_alignment(&schema, is_drop_and_replace).await?;
            }
            let batch = self.align_batch(batch)?;
            let schema = batch.schema();
            let num_rows = batch.num_rows();
            if bcp_needs_init {
                let bcp_bin = self.bcp_path.clone()
                    .or_else(bcp_mod::discover_bcp)
                    .ok_or_else(|| anyhow::anyhow!("bcp binary not found"))?;
                let (bcp_target_table, staging_full) = if use_staging {
                    let (bare_name, full_staging) = bcp_mod::staging_table_names(&self.cfg.schema_name, &self.cfg.table);
                    let ddl = bcp_mod::staging_ddl(&full_staging, &schema);
                    let client = self.client.as_mut().unwrap();
                    client.simple_query(ddl).await?.into_results().await?;
                    let bcp_tbl = format!("{}.{}.{bare_name}", self.params.database, self.cfg.schema_name);
                    (bcp_tbl, Some(full_staging))
                } else {
                    let bcp_tbl = format!("{}.{}.{}", self.params.database, self.cfg.schema_name, self.cfg.table);
                    (bcp_tbl, None)
                };
                let target_column_indices = if use_staging || self.alignment.is_none() {
                    None
                } else {
                    let align = self.alignment.as_ref().unwrap();
                    if align.is_identity() { None } else {
                        Some(align.mapping.iter().map(|ac| ac.target_ordinal.unwrap_or(0)).collect())
                    }
                };
                let proc = bcp_mod::BcpProcess::spawn(&bcp_bin, &self.params, &bcp_target_table, self.bulk_threshold, &schema, target_column_indices)?;
                self.bcp_process      = Some(proc);
                self.bcp_staging_full = staging_full;
                self.saved_schema     = Some(Arc::clone(&schema));
                self.in_transaction   = true;
            }
            let proc = self.bcp_process.as_mut().unwrap();
            proc.write_batch(&batch).await?;
            self.staging_rows += num_rows;
            return Ok(num_rows);
        }
        #[cfg(not(feature = "bcp"))]
        if self.mode == MssqlWriteMode::Bcp {
            anyhow::bail!("mode=bcp requires the `bcp` feature.");
        }

        // ── ODBC path (feature-gated) ─────────────────────────────────────
        #[cfg(feature = "odbc")]
        if self.mode == MssqlWriteMode::Odbc {
            return self.write_impl_odbc(batch, first, table_prepared).await;
        }
        #[cfg(not(feature = "odbc"))]
        if self.mode == MssqlWriteMode::Odbc {
            anyhow::bail!("mode=odbc requires the `odbc` feature.");
        }

        // ── tiberius DATE→TIME guard ──────────────────────────────────────
        if first && self.mode == MssqlWriteMode::Tiberius && schema_has_date_before_time64(&schema) {
            anyhow::bail!(
                "MSSQL tiberius path cannot handle DATE before TIME columns. Enable BCP or ODBC."
            );
        }

        let mut staging = self.use_staging;

        // ── Connect + BEGIN TRANSACTION ───────────────────────────────────
        if self.client.is_none() { self.client = Some(self.params.connect().await?); }
        if !self.in_transaction {
            self.client.as_mut().unwrap().simple_query("BEGIN TRANSACTION").await?.into_results().await?;
            self.in_transaction = true;
        }

        // ── DDL + TRUNCATE (first batch) ──────────────────────────────────
        if !table_prepared {
            let did_drop_and_create = is_drop_and_replace;
            // For `create_if_not_exists`, probe the table first. If it exists,
            // skip ALL DDL — including post_create constraints/indexes whose
            // definitions may be incompatible with the existing schema (e.g.
            // a NVARCHAR(MAX) PK that fails the SQL Server key-length check).
            let table_already_exists = if is_create_if_not_exists && !is_drop_and_replace {
                let client = self.client.as_mut().unwrap();
                introspect_table_columns(client, &schema_name, &table).await?.is_some()
            } else {
                false
            };
            {
                let client = self.client.as_mut().unwrap();
                if is_drop_and_replace {
                    client.simple_query(format!("DROP TABLE IF EXISTS {full_table}")).await?.into_results().await?;
                }
                if (is_drop_and_replace || is_create_if_not_exists) && !table_already_exists {
                    let db_config = self.cfg.database_schema_config.as_ref();
                    let stmts = generate_ddl_with_schema(&table, Some(&schema_name), &ddl_schema, SqlDialect::Mssql, None, db_config, DdlOptions::default());
                    for stmt in &stmts.pre_create {
                        client.simple_query(stmt.to_string()).await?.into_results().await?;
                    }
                    tracing::debug!(table = %self.cfg.table, "MSSQL DDL:\n{}", stmts.create_table);
                    client.simple_query(stmts.create_table).await?.into_results().await?;
                    for stmt in &stmts.post_create {
                        for batch_sql in stmt.split("\nGO\n").map(|s| s.trim()).filter(|s| !s.is_empty()) {
                            tracing::trace!(table = %self.cfg.table, "MSSQL post-create DDL:\n{batch_sql}");
                            match client.simple_query(batch_sql.to_string()).await {
                                Ok(q) => { let _ = q.into_results().await; }
                                Err(e) => tracing::warn!(table = %self.cfg.table, "MSSQL post-create DDL ignored: {e:#}"),
                            }
                        }
                    }
                    if is_drop_and_replace {
                        tracing::info!(table = %self.cfg.table, "MSSQL DROP + CREATE TABLE applied");
                    } else {
                        tracing::info!(table = %self.cfg.table, "MSSQL CREATE TABLE IF NOT EXISTS applied");
                    }
                } else if table_already_exists {
                    tracing::info!(table = %self.cfg.table, "MSSQL table exists — skipping DDL");
                }
                if is_clear_and_insert && !did_drop_and_create {
                    client.simple_query(format!("TRUNCATE TABLE {full_table}")).await?.into_results().await?;
                    tracing::info!(table = %self.cfg.table, "MSSQL TRUNCATE applied");
                }
            }
            self.compute_column_alignment(&schema, did_drop_and_create).await?;
            // Determine staging based on user config or auto-detect.
            //
            // `staging_table_opt`:
            //   None        → auto: staging for upsert/insert_ignore/merge_delete
            //   Some(true)  → force staging for all strategies
            //   Some(false) → disable (WARNING: merge strategies will fail)
            //
            // When auto, staging is REQUIRED for upsert/insert_ignore/merge_delete
            // in tiberius mode — MERGE needs a source table.  Previously this was
            // gated on non-identity alignment, which was a bug: when columns matched
            // perfectly, staging was skipped and bulk INSERT would fail on duplicate
            // keys or silently skip conflict handling.
            if !staging {
                let should_stage = match self.staging_table_opt {
                    Some(true)  => true,
                    Some(false) => {
                        if matches!(self.cfg.write_strategy, WriteStrategy::Upsert | WriteStrategy::MergeDelete | WriteStrategy::InsertIgnore) {
                            tracing::warn!(
                                table = %self.cfg.table,
                                "staging_table: false with upsert/insert_ignore/merge_delete — \
                                 conflict handling will NOT work correctly"
                            );
                        }
                        false
                    }
                    None => matches!(self.cfg.write_strategy, WriteStrategy::Upsert | WriteStrategy::MergeDelete | WriteStrategy::InsertIgnore),
                };
                if should_stage {
                    self.use_staging = true;
                    staging = true;
                }
            }
        }

        // ── Align + convert to TokenRows ──────────────────────────────────
        let batch = self.align_batch(batch)?;
        let schema = batch.schema();

        let staging_ddl = if first && staging { Some(create_staging_ddl(&schema)) } else { None };
        let token_rows = tokio::task::spawn_blocking(move || {
            if staging { batch_to_staging_rows(&batch) } else { batch_to_direct_rows(&batch) }
        }).await?;

        let client = self.client.as_mut().unwrap();
        if let Some(ref ddl) = staging_ddl {
            client.simple_query(ddl).await?.into_results().await?;
        }
        if first { self.saved_schema = Some(Arc::clone(&schema)); }

        let dest = if staging { "#etl_bulk_stage" } else { full_table.as_str() };
        self.row_buffer.extend(token_rows);
        self.staging_rows += num_rows;

        if self.row_buffer.len() >= self.bulk_threshold {
            let rows   = std::mem::take(&mut self.row_buffer);
            let client = self.client.as_mut().unwrap();
            let is_partial = !staging && (
                self.alignment.as_ref().map(|a| !a.is_identity()).unwrap_or(false)
                || self.saved_schema.as_ref().map(|s| schema_has_reserved_column(s)).unwrap_or(false)
            );
            if is_partial {
                if let Some(ref schema) = self.saved_schema {
                    do_bulk_insert_partial(client, dest, schema, rows).await?;
                } else {
                    do_bulk_insert(client, dest, rows).await?;
                }
            } else {
                do_bulk_insert(client, dest, rows).await?;
            }
        }

        tracing::debug!(table = %self.cfg.table, rows = num_rows, "MSSQL batch written");
        Ok(num_rows)
    }

    // ── ODBC write/flush ─────────────────────────────────────────────────────

    #[cfg(feature = "odbc")]
    async fn write_impl_odbc(&mut self, batch: RecordBatch, first: bool, table_prepared: bool) -> anyhow::Result<usize> {
        use crate::odbc::{self as odbc_mod, SendWriter};

        let _num_rows = batch.num_rows();
        let schema   = batch.schema();
        let ddl_schema = self.cfg.ddl_schema.clone().unwrap_or_else(|| schema.clone());
        let full_table = format!("[{}].[{}]", self.cfg.schema_name, self.cfg.table);
        let table       = self.cfg.table.clone();
        let schema_name = self.cfg.schema_name.clone();
        let is_create_if_not_exists = matches!(self.cfg.table_mode, TableMode::CreateIfNotExists);
        let is_drop_and_replace     = matches!(self.cfg.table_mode, TableMode::DropAndReplace);
        let is_clear_and_insert     = matches!(self.cfg.write_strategy, WriteStrategy::Truncate);

        // ── Lazy-init ODBC connection ─────────────────────────────────────
        if self.odbc_writer.is_none() {
            let params = self.params.clone();
            let writer = tokio::task::spawn_blocking(move || {
                odbc_mod::OdbcWriter::connect(&params)
            }).await??;
            self.odbc_writer = Some(writer);
        }

        // ── DDL + TRUNCATE (first batch) ──────────────────────────────────
        if !table_prepared {
            let did_drop_and_create = is_drop_and_replace;

            if is_drop_and_replace {
                let sql = format!("DROP TABLE IF EXISTS {full_table}");
                let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                tokio::task::spawn_blocking(move || {
                    let w = unsafe { sw.as_ref() };
                    w.execute_sql(&sql)
                }).await??;

            }
            let table_already_exists = if is_create_if_not_exists && !is_drop_and_replace {
                let sn = schema_name.clone();
                let tn = table.clone();
                let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                let cols = tokio::task::spawn_blocking(move || {
                    let w = unsafe { sw.as_ref() };
                    w.introspect_table_columns(&sn, &tn)
                }).await??;
                cols.is_some()
            } else {
                false
            };
            if (is_drop_and_replace || is_create_if_not_exists) && !table_already_exists {
                let db_config = self.cfg.database_schema_config.as_ref();
                let stmts = generate_ddl_with_schema(&table, Some(&schema_name), &ddl_schema, SqlDialect::Mssql, None, db_config, DdlOptions::default());
                for pre_stmt in &stmts.pre_create {
                    let sql = pre_stmt.to_string();
                    let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                    tokio::task::spawn_blocking(move || {
                        let w = unsafe { sw.as_ref() };
                        w.execute_sql(&sql)
                    }).await??;
                }
                {
                    let ddl = stmts.create_table;
                    let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                    tokio::task::spawn_blocking(move || {
                        let w = unsafe { sw.as_ref() };
                        w.execute_sql(&ddl)
                    }).await??;
                }
                for stmt in &stmts.post_create {
                    for batch_sql in stmt.split("\nGO\n").map(|s| s.trim()).filter(|s| !s.is_empty()) {
                        tracing::trace!(table = %self.cfg.table, "MSSQL post-create DDL (odbc):\n{batch_sql}");
                        let sql = batch_sql.to_string();
                        let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                        let result = tokio::task::spawn_blocking(move || {
                            let w = unsafe { sw.as_ref() };
                            w.execute_sql(&sql)
                        }).await?;
                        if let Err(e) = result {
                            tracing::warn!(table = %self.cfg.table, "MSSQL post-create DDL ignored (odbc): {e:#}");
                        }
                    }
                }
                if is_drop_and_replace {
                    tracing::info!(table = %self.cfg.table, "MSSQL DROP + CREATE TABLE applied (odbc)");
                } else {
                    tracing::info!(table = %self.cfg.table, "MSSQL CREATE TABLE IF NOT EXISTS applied (odbc)");
                }
            } else if table_already_exists {
                tracing::info!(table = %self.cfg.table, "MSSQL table exists — skipping DDL (odbc)");
            }

            // Compute column alignment via ODBC introspection
            if !did_drop_and_create {
                if self.alignment.is_none() {
                    let sn = self.cfg.schema_name.clone();
                    let tn = self.cfg.table.clone();
                    let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                    let table_cols = tokio::task::spawn_blocking(move || {
                        let w = unsafe { sw.as_ref() };
                        w.introspect_table_columns(&sn, &tn)
                    }).await??;
                    if let Some(cols) = table_cols {
                        use potato_etl_common::db::common::alignment::{
                            self as align, MissingColumnBehavior, TargetColumn,
                        };
                        let target_cols: Vec<TargetColumn> = cols.into_iter()
                            .map(|c| TargetColumn {
                                name: c.name, data_type: c.data_type,
                                nullable: c.nullable, has_default: c.has_default,
                            })
                            .collect();
                        let table_display = format!("{}.{}", self.cfg.schema_name, self.cfg.table);
                        let batch_schema_transformed = if let Some(case) = self.cfg.identifier_case {
                            use arrow::datatypes::{Field, Schema};
                            let fields: Vec<Field> = schema.fields().iter()
                                .map(|f| Field::new(case.transform(f.name()), f.data_type().clone(), f.is_nullable()).with_metadata(f.metadata().clone()))
                                .collect();
                            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
                        } else {
                            schema.clone()
                        };
                        let result = align::compute_alignment(&batch_schema_transformed, &target_cols, MissingColumnBehavior::Skip, &table_display)?;
                        self.alignment = Some(result);
                        self.target_columns = Some(target_cols);
                    }
                }
            }

            // Determine staging
            if !self.use_staging {
                let should_stage = match self.staging_table_opt {
                    Some(true)  => true,
                    Some(false) => {
                        if matches!(self.cfg.write_strategy, WriteStrategy::Upsert | WriteStrategy::MergeDelete | WriteStrategy::InsertIgnore) {
                            tracing::warn!(
                                table = %self.cfg.table,
                                "staging_table: false with upsert/insert_ignore/merge_delete — \
                                 conflict handling will NOT work correctly"
                            );
                        }
                        false
                    }
                    None => matches!(self.cfg.write_strategy, WriteStrategy::Upsert | WriteStrategy::MergeDelete | WriteStrategy::InsertIgnore),
                };
                if should_stage {
                    self.use_staging = true;
                }
            }

            if is_clear_and_insert && !is_drop_and_replace {
                let sql = format!("TRUNCATE TABLE {full_table}");
                let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                tokio::task::spawn_blocking(move || {
                    let w = unsafe { sw.as_ref() };
                    w.execute_sql(&sql)
                }).await??;
                tracing::info!(table = %self.cfg.table, "MSSQL TRUNCATE applied (odbc)");
            }
        }

        // ── BEGIN TRANSACTION ─────────────────────────────────────────────
        if !self.in_transaction {
            let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
            tokio::task::spawn_blocking(move || {
                let w = unsafe { sw.as_ref() };
                w.begin_transaction()
            }).await??;
            self.in_transaction = true;
        }

        // ── Align batch ──────────────────────────────────────────────────
        let batch = self.align_batch(batch)?;
        let schema = batch.schema();

        // ── Create staging table on first batch ──────────────────────────
        if first && self.use_staging {
            let ddl = odbc_mod::odbc_staging_ddl(&schema);
            let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
            tokio::task::spawn_blocking(move || {
                let w = unsafe { sw.as_ref() };
                w.execute_sql(&ddl)
            }).await??;
        }

        if first { self.saved_schema = Some(Arc::clone(&schema)); }

        // ── Bulk insert via ODBC ─────────────────────────────────────────
        let dest = if self.use_staging {
            "#etl_odbc_stage".to_string()
        } else {
            full_table.clone()
        };
        let bulk_threshold = self.bulk_threshold;
        let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
        let inserted = tokio::task::spawn_blocking(move || {
            let w = unsafe { sw.as_ref() };
            w.bulk_insert_batch(&dest, &batch, bulk_threshold)
        }).await??;

        self.staging_rows += inserted;
        tracing::debug!(table = %self.cfg.table, rows = inserted, "MSSQL batch written (odbc)");
        Ok(inserted)
    }

    #[cfg(feature = "odbc")]
    async fn flush_odbc(&mut self) -> anyhow::Result<()> {
        use crate::odbc::{self as odbc_mod, SendWriter};

        if self.odbc_writer.is_none() {
            self.reset_state();
            return Ok(());
        }

        if !self.in_transaction {
            self.reset_state();
            return Ok(());
        }

        let full_table = format!("[{}].[{}]", self.cfg.schema_name, self.cfg.table);

        if self.use_staging {
            if let Some(ref schema) = self.saved_schema {
                let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
                let pk_cols = potato_etl_common::db::pk_columns(schema);

                // Create PK index on staging table for MERGE performance
                if !pk_cols.is_empty() && matches!(
                    &self.cfg.write_strategy,
                    WriteStrategy::Upsert | WriteStrategy::MergeDelete | WriteStrategy::InsertIgnore
                ) {
                    let idx_sql = odbc_mod::odbc_staging_pk_index_sql(&pk_cols);
                    let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                    let idx_result = tokio::task::spawn_blocking(move || {
                        let w = unsafe { sw.as_ref() };
                        w.execute_sql(&idx_sql)
                    }).await?;
                    if let Err(e) = idx_result {
                        let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                        let _ = tokio::task::spawn_blocking(move || {
                            let w = unsafe { sw.as_ref() };
                            w.rollback()
                        }).await;
                        self.reset_state();
                        return Err(anyhow::anyhow!("ODBC staging index: {e}"));
                    }
                }

                // Run MERGE/INSERT SELECT
                let flush_sql = match &self.cfg.write_strategy {
                    WriteStrategy::InsertIgnore => odbc_mod::odbc_merge_sql(&full_table, &col_names, &pk_cols, false, true),
                    WriteStrategy::Upsert       => odbc_mod::odbc_merge_sql(&full_table, &col_names, &pk_cols, false, false),
                    WriteStrategy::MergeDelete  => odbc_mod::odbc_merge_sql(&full_table, &col_names, &pk_cols, true, false),
                    _                           => odbc_mod::odbc_insert_select_sql(&full_table, &col_names),
                };
                let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                let flush_result = tokio::task::spawn_blocking(move || {
                    let w = unsafe { sw.as_ref() };
                    w.execute_sql(&flush_sql)
                }).await?;
                if let Err(e) = flush_result {
                    let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
                    let _ = tokio::task::spawn_blocking(move || {
                        let w = unsafe { sw.as_ref() };
                        w.rollback()
                    }).await;
                    self.reset_state();
                    return Err(anyhow::anyhow!("ODBC flush: {e}"));
                }
            }
        }

        // COMMIT
        let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
        let commit_result = tokio::task::spawn_blocking(move || {
            let w = unsafe { sw.as_ref() };
            w.commit()
        }).await?;
        if let Err(e) = commit_result {
            let sw = SendWriter::new(self.odbc_writer.as_ref().unwrap());
            let _ = tokio::task::spawn_blocking(move || {
                let w = unsafe { sw.as_ref() };
                w.rollback()
            }).await;
            self.reset_state();
            return Err(anyhow::anyhow!("ODBC COMMIT: {e}"));
        }

        tracing::info!(table = %self.cfg.table, rows = self.staging_rows, "MSSQL flush complete (odbc)");
        self.reset_state();
        Ok(())
    }

    // ── Error handling helpers ────────────────────────────────────────────────

    async fn rollback_and_reset(&mut self) {
        if self.in_transaction { self.do_rollback().await; }
        #[cfg(feature = "bcp")]
        {
            if let Some(proc) = self.bcp_process.take() { proc.abort(); }
            self.drop_bcp_staging().await;
        }
        self.reset_state();
    }

    async fn do_rollback(&mut self) {
        #[cfg(feature = "odbc")]
        if let Some(ref writer) = self.odbc_writer {
            use crate::odbc::SendWriter;
            let sw = SendWriter::new(writer);
            let _ = tokio::task::spawn_blocking(move || {
                let w = unsafe { sw.as_ref() };
                w.rollback()
            }).await;
        }
        if let Some(ref mut c) = self.client {
            if let Ok(s) = c.simple_query("ROLLBACK TRANSACTION").await {
                let _ = s.into_results().await;
            }
        }
        self.in_transaction = false;
    }

    #[cfg(feature = "bcp")]
    async fn drop_bcp_staging(&mut self) {
        if let Some(ref staging_full) = self.bcp_staging_full {
            if let Some(ref mut c) = self.client {
                let sql = format!("DROP TABLE IF EXISTS {staging_full}");
                match c.simple_query(sql.as_str()).await {
                    Err(e) => tracing::warn!(staging = %staging_full, error = %e, "drop staging failed"),
                    Ok(s) => { let _ = s.into_results().await; }
                }
            }
        }
        self.bcp_staging_full = None;
    }

    fn reset_state(&mut self) {
        self.client             = None;
        self.first_batch        = true;
        self.saved_schema       = None;
        self.staging_rows       = 0;
        self.row_buffer.clear();
        self.cfg.table_prepared = false;
        self.in_transaction     = false;
        #[cfg(feature = "bcp")]
        { self.bcp_process = None; }
        self.bcp_staging_full   = None;
        #[cfg(feature = "odbc")]
        { self.odbc_writer = None; }
        self.alignment          = None;
    }
}

// ── SinkBuilder trait ────────────────────────────────────────────────────────

impl SinkBuilder for MssqlWriteDB {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.table(t); self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.schema(s); self }
    fn use_existing(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.use_existing(); self }
    fn create_if_not_exists(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.create_if_not_exists(); self }
    fn drop_and_replace(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.drop_and_replace(); self }
    fn insert(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.insert(); self }
    fn insert_ignore(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.insert_ignore(); self }
    fn upsert(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.upsert(); self }
    fn merge_delete(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.merge_delete(); self }
    fn clear_and_insert(mut self: Box<Self>) -> Box<dyn SinkBuilder> { self.cfg = self.cfg.clear_and_insert(); self }
    fn with_driver_options(self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SinkBuilder> {
        Box::new((*self).apply_driver_options(opts))
    }
    fn set_database_schema_config(&mut self, config: DatabaseSchemaConfig) { self.cfg.database_schema_config = Some(config); }
    fn set_ddl_schema(&mut self, schema: SchemaRef) { self.cfg.ddl_schema = Some(schema); }

    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<usize>> + Send + 'a>> {
        Box::pin(self.write_batch(batch))
    }

    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(self.flush_all())
    }
}

// `mssql_create_table_sql`, `mssql_create_if_not_exists_sql`, and
// `normalise_mssql_type` have been removed — DDL is now generated by
// `generate_ddl_with_schema` from the common crate, which correctly includes
// PRIMARY KEY, UNIQUE, FOREIGN KEY, indexes, DEFAULT, CHECK, and all other
// constraints. MSSQL TIME normalisation is handled by `resolve_sql_type`.

// ── bulk_insert helpers ───────────────────────────────────────────────────────

async fn do_bulk_insert(
    client: &mut MssqlClient,
    table:  &str,
    rows:   Vec<TokenRow<'static>>,
) -> anyhow::Result<()> {
    if rows.is_empty() { return Ok(()); }
    let mut req = client.bulk_insert(table).await?;
    for row in rows { req.send(row).await?; }
    req.finalize().await?;
    Ok(())
}

async fn do_bulk_insert_partial(
    client: &mut MssqlClient,
    table:  &str,
    schema: &SchemaRef,
    rows:   Vec<TokenRow<'static>>,
) -> anyhow::Result<()> {
    if rows.is_empty() { return Ok(()); }
    const STAGE: &str = "#etl_partial_stage";
    let cols: Vec<String> = schema.fields().iter().map(|f| {
        format!("    [{}] {} NULL", f.name(), staging_sql_type(f.data_type()))
    }).collect();
    let create_ddl = format!("CREATE TABLE {STAGE} (\n{}\n)", cols.join(",\n"));
    client.simple_query(&create_ddl).await?.into_results().await?;
    let mut req = client.bulk_insert(STAGE).await?;
    for row in rows { req.send(row).await?; }
    req.finalize().await?;
    let col_list = schema.fields().iter().map(|f| format!("[{}]", f.name())).collect::<Vec<_>>().join(", ");
    let insert_select = format!("INSERT INTO {table} ({col_list}) SELECT {col_list} FROM {STAGE}");
    client.simple_query(&insert_select).await?.into_results().await?;
    client.simple_query(&format!("DROP TABLE {STAGE}")).await?.into_results().await?;
    Ok(())
}