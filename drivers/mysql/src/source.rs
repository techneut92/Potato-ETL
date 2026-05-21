//! MySQL read source — `MySqlReadDB`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    LargeBinaryBuilder, StringBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use sqlx::mysql::MySqlRow;
use sqlx::{Column, Row, TypeInfo};

use crate::util::{backtick, unix_epoch};
use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::common::field_meta;
use potato_etl_common::db::traits::SourceBuilder;

// ── MySqlReadDB ───────────────────────────────────────────────────────────────

pub struct MySqlReadDB {
    conn_str:     String,
    table:        String,
    schema_name:  String,
    custom_query: Option<String>,
    cursor_col:   Option<String>,
    batch_size:   usize,
    /// SQL statements executed on every new connection in the pool.
    init_sql:     Vec<String>,
}

impl MySqlReadDB {
    pub fn new(conn_str: &str) -> Self {
        Self {
            conn_str:     conn_str.to_string(),
            table:        String::new(),
            schema_name:  String::new(),
            custom_query: None,
            cursor_col:   None,
            batch_size:   1_000,
            init_sql:     Vec::new(),
        }
    }
}

impl SourceBuilder for MySqlReadDB {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn SourceBuilder> { self.table = t; self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn SourceBuilder> { self.schema_name = s; self }
    fn query(mut self: Box<Self>, q: String) -> Box<dyn SourceBuilder> { self.custom_query = Some(q); self }
    fn cursor(mut self: Box<Self>, c: String) -> Box<dyn SourceBuilder> { self.cursor_col = Some(c); self }
    fn batch_size(mut self: Box<Self>, n: usize) -> Box<dyn SourceBuilder> { self.batch_size = n; self }
    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SourceBuilder> {
        if let Some(ref my) = opts.mysql {
            if !my.init_sql.is_empty() {
                self.init_sql = my.init_sql.clone();
            }
        }
        self
    }

    fn read_schema<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<SchemaRef>> + Send + 'a>> {
        Box::pin(async move {
            let pool = crate::util::mysql_pool_with_init_sql(&self.conn_str, 1, &self.init_sql).await?;
            let sql = "SELECT
                    COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                    CAST(CHARACTER_MAXIMUM_LENGTH AS SIGNED) AS char_max_len,
                    CAST(NUMERIC_PRECISION        AS SIGNED) AS numeric_prec,
                    CAST(NUMERIC_SCALE            AS SIGNED) AS numeric_scl,
                    CAST(DATETIME_PRECISION       AS SIGNED) AS dt_prec
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = COALESCE(NULLIF(?, ''), DATABASE())
                  AND TABLE_NAME   = ?
                ORDER BY ORDINAL_POSITION";
            let rows = sqlx::query(sql)
                .bind(&self.schema_name)
                .bind(&self.table)
                .fetch_all(&pool).await?;
            let fields: Vec<Field> = rows.iter().map(|row| {
                let name:      String      = row.get("COLUMN_NAME");
                let data_type: String      = row.get("DATA_TYPE");
                let nullable:  String      = row.get("IS_NULLABLE");
                let char_len:  Option<i64> = row.get("char_max_len");
                let num_prec:  Option<i64> = row.get("numeric_prec");
                let num_scale: Option<i64> = row.get("numeric_scl");
                let dt_prec:   Option<i64> = row.get("dt_prec");
                let precision  = num_prec.or(dt_prec).map(|p| p as i32);
                let scale      = num_scale.map(|s| s as i32);
                let meta       = field_meta::make_with_source(&data_type, precision, scale, char_len, "mysql");
                let arrow_dt   = mysql_type_to_arrow(&data_type);
                Field::new(&name, arrow_dt, nullable.eq_ignore_ascii_case("YES")).with_metadata(meta)
            }).collect();
            Ok(Arc::new(Schema::new(fields)))
        })
    }

    fn exec(self: Box<Self>) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
        let Self { conn_str, table, schema_name, custom_query, cursor_col, batch_size, init_sql } = *self;
        Box::pin(async_stream::try_stream! {
            let pool = crate::util::mysql_pool_with_init_sql(&conn_str, 2, &init_sql).await?;
            if cursor_col.is_some() {
                let mut last_cursor: Option<String> = None;
                loop {
                    let sql = mysql_build_query(&table, &schema_name, custom_query.as_deref(), cursor_col.as_deref(), last_cursor.as_deref(), batch_size);
                    tracing::debug!(sql = %sql, "MySqlReadDB keyset poll");
                    let rows: Vec<MySqlRow> = sqlx::query(&sql).fetch_all(&pool).await?;
                    if rows.is_empty() { break; }
                    let done = rows.len() < batch_size;
                    if let Some(col) = &cursor_col {
                        if let Some(last) = rows.last() {
                            if let Ok(Some(v)) = last.try_get::<Option<i64>, _>(col.as_str()) {
                                last_cursor = Some(v.to_string());
                            } else if let Ok(Some(v)) = last.try_get::<Option<i32>, _>(col.as_str()) {
                                last_cursor = Some(v.to_string());
                            } else if let Ok(Some(v)) = last.try_get::<Option<String>, _>(col.as_str()) {
                                last_cursor = Some(v);
                            }
                        }
                    }
                    yield mysql_rows_to_record_batch(&rows)?;
                    if done { break; }
                }
            } else {
                let full_sql = match custom_query.as_deref() {
                    Some(q) => q.to_string(),
                    None => if schema_name.is_empty() {
                        format!("SELECT * FROM {}", backtick(&table))
                    } else {
                        format!("SELECT * FROM {}.{}", backtick(&schema_name), backtick(&table))
                    },
                };
                tracing::debug!(sql = %full_sql, "MySqlReadDB full-scan stream");
                use futures::TryStreamExt as _;
                // Use sqlx::raw_sql (text protocol) instead of sqlx::query
                // (binary/prepared protocol). The binary BinaryRow decoder
                // in sqlx-mysql panics on certain wide-column packets
                // ("cannot advance past `remaining`"). The text protocol
                // ships values as strings and never enters that decoder.
                let mut row_stream = sqlx::raw_sql(&full_sql).fetch(&pool);
                let mut buf: Vec<MySqlRow> = Vec::with_capacity(batch_size);
                while let Some(row) = row_stream.try_next().await? {
                    buf.push(row);
                    if buf.len() >= batch_size {
                        yield mysql_rows_to_record_batch(&buf)?;
                        buf.clear();
                    }
                }
                if !buf.is_empty() {
                    yield mysql_rows_to_record_batch(&buf)?;
                }
            }
        })
    }

    fn try_clone(&self) -> Option<Box<dyn SourceBuilder>> { None }
}

// ── Row → RecordBatch ─────────────────────────────────────────────────────────

fn mysql_rows_to_record_batch(rows: &[MySqlRow]) -> anyhow::Result<RecordBatch> {
    if rows.is_empty() { return Ok(RecordBatch::new_empty(Arc::new(Schema::empty()))); }
    let cols = rows[0].columns();
    let n    = rows.len();
    let mut fields: Vec<Field>    = Vec::with_capacity(cols.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
    for col in cols {
        let type_name = col.type_info().name();
        let (dt, arr) = build_mysql_column(rows, col.ordinal(), type_name, n)?;
        let type_lower = type_name.to_lowercase();
        let mut meta = field_meta::type_only(&type_lower);
        field_meta::stamp_logical(&mut meta, &type_lower, "mysql");
        fields.push(Field::new(col.name(), dt, true).with_metadata(meta));
        arrays.push(arr);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| anyhow::anyhow!("MySQL RecordBatch construction failed: {e}"))
}

fn build_mysql_column(rows: &[MySqlRow], col_idx: usize, type_name: &str, n: usize) -> anyhow::Result<(DataType, ArrayRef)> {
    macro_rules! typed_col {
        ($Rust:ty, $Builder:ty, $DT:expr) => {{
            let mut b = <$Builder>::with_capacity(n);
            for row in rows {
                match row.try_get::<Option<$Rust>, _>(col_idx) {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            Ok(($DT, Arc::new(b.finish()) as ArrayRef))
        }};
    }
    match type_name {
        "TINYINT"              => typed_col!(i8,  Int8Builder,   DataType::Int8),
        "SMALLINT" | "YEAR"    => typed_col!(i16, Int16Builder,  DataType::Int16),
        "MEDIUMINT" | "INT"    => typed_col!(i32, Int32Builder,  DataType::Int32),
        "BIGINT"               => typed_col!(i64, Int64Builder,  DataType::Int64),
        "FLOAT"                => typed_col!(f32, Float32Builder, DataType::Float32),
        "DOUBLE" | "DECIMAL" | "NUMERIC" => typed_col!(f64, Float64Builder, DataType::Float64),
        "BIT"                  => typed_col!(bool, BooleanBuilder, DataType::Boolean),
        "DATE" => {
            let epoch = unix_epoch();
            let mut b = Date32Builder::with_capacity(n);
            for row in rows {
                match row.try_get::<Option<chrono::NaiveDate>, _>(col_idx) {
                    Ok(Some(d)) => b.append_value(d.signed_duration_since(epoch).num_days() as i32),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            Ok((DataType::Date32, Arc::new(b.finish()) as ArrayRef))
        }
        "DATETIME" | "TIMESTAMP" => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n);
            for row in rows {
                match row.try_get::<Option<chrono::NaiveDateTime>, _>(col_idx) {
                    Ok(Some(dt)) => b.append_value(dt.and_utc().timestamp_micros()),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            Ok((DataType::Timestamp(TimeUnit::Microsecond, None), Arc::new(b.finish()) as ArrayRef))
        }
        "TIME" => {
            use chrono::Timelike;
            let mut b = Time64MicrosecondBuilder::with_capacity(n);
            for row in rows {
                match row.try_get::<Option<chrono::NaiveTime>, _>(col_idx) {
                    Ok(Some(t)) => b.append_value(
                        t.num_seconds_from_midnight() as i64 * 1_000_000 + t.nanosecond() as i64 / 1_000,
                    ),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            Ok((DataType::Time64(TimeUnit::Microsecond), Arc::new(b.finish()) as ArrayRef))
        }
        "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" => {
            let mut b = LargeBinaryBuilder::with_capacity(n, n * 64);
            for row in rows {
                match row.try_get::<Option<Vec<u8>>, _>(col_idx) {
                    Ok(Some(v)) => b.append_value(&v),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            Ok((DataType::LargeBinary, Arc::new(b.finish()) as ArrayRef))
        }
        _ => {
            let mut b = StringBuilder::with_capacity(n, n * 16);
            for row in rows {
                match row.try_get::<Option<String>, _>(col_idx) {
                    Ok(Some(s)) => b.append_value(&s),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            Ok((DataType::Utf8, Arc::new(b.finish()) as ArrayRef))
        }
    }
}

// ── Query builder ─────────────────────────────────────────────────────────────

fn mysql_build_query(table: &str, schema: &str, custom_q: Option<&str>, cursor_col: Option<&str>, last_cursor: Option<&str>, batch_size: usize) -> String {
    let base = match custom_q {
        Some(q) => format!("SELECT * FROM ({q}) AS _etl_q"),
        None => if schema.is_empty() {
            format!("SELECT * FROM {}", backtick(table))
        } else {
            format!("SELECT * FROM {}.{}", backtick(schema), backtick(table))
        },
    };
    match (cursor_col, last_cursor) {
        (Some(col), Some(val)) => {
            let safe = val.replace('\'', "''");
            format!("SELECT * FROM ({base}) AS _q WHERE {} > '{safe}' ORDER BY {} ASC LIMIT {batch_size}", backtick(col), backtick(col))
        }
        (Some(col), None) => format!("SELECT * FROM ({base}) AS _q ORDER BY {} ASC LIMIT {batch_size}", backtick(col)),
        _ => format!("SELECT * FROM ({base}) AS _q LIMIT {batch_size}"),
    }
}

fn mysql_type_to_arrow(data_type: &str) -> DataType {
    match data_type {
        "tinyint"                         => DataType::Int8,
        "smallint" | "year"               => DataType::Int16,
        "mediumint" | "int" | "integer"   => DataType::Int32,
        "bigint"                          => DataType::Int64,
        "float"                           => DataType::Float32,
        "double" | "decimal" | "numeric"  => DataType::Float64,
        "bit"                             => DataType::Boolean,
        "date"                            => DataType::Date32,
        "datetime" | "timestamp"          => DataType::Timestamp(TimeUnit::Microsecond, None),
        "time"                            => DataType::Time64(TimeUnit::Microsecond),
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => DataType::LargeBinary,
        _                                 => DataType::Utf8,
    }
}

pub fn mysql_type_map() -> HashMap<&'static str, DataType> {
    [
        ("tinyint", DataType::Int8), ("smallint", DataType::Int16), ("mediumint", DataType::Int32),
        ("int", DataType::Int32), ("integer", DataType::Int32), ("bigint", DataType::Int64),
        ("float", DataType::Float32), ("double", DataType::Float64), ("decimal", DataType::Float64),
        ("numeric", DataType::Float64), ("bit", DataType::Boolean), ("date", DataType::Date32),
        ("datetime", DataType::Timestamp(TimeUnit::Microsecond, None)),
        ("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None)),
        ("time", DataType::Time64(TimeUnit::Microsecond)),
        ("binary", DataType::LargeBinary), ("varbinary", DataType::LargeBinary),
        ("blob", DataType::LargeBinary), ("tinyblob", DataType::LargeBinary),
        ("mediumblob", DataType::LargeBinary), ("longblob", DataType::LargeBinary),
        ("varchar", DataType::Utf8), ("char", DataType::Utf8), ("text", DataType::Utf8),
        ("tinytext", DataType::Utf8), ("mediumtext", DataType::Utf8), ("longtext", DataType::Utf8),
        ("json", DataType::Utf8), ("enum", DataType::Utf8), ("set", DataType::Utf8),
    ].into()
}