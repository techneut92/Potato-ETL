//! Oracle source — streaming `RecordBatch` reader via the `oracle` crate (ODPI-C).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder,
    LargeBinaryBuilder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use oracle::sql_type::Timestamp as OracleTimestamp;

use crate::util::{OracleConn, oracle_build_query};
use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::common::field_meta;
use potato_etl_common::db::traits::SourceBuilder;

// ── Type mapping ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub enum ColKind { Int64, Float64, Bool, Timestamp, TimestampTz, Bytes, Str }

pub fn oracle_col_kind_and_meta(col: &oracle::ColumnInfo) -> (ColKind, std::collections::HashMap<String, String>) {
    use oracle::sql_type::OracleType::*;
    let ot = col.oracle_type();
    let (kind, db_type, precision, scale, length) = match ot {
        Number(0, 0)            => (ColKind::Float64, "number", None, None, None),
        Number(_, -127)         => (ColKind::Float64, "number", None, None, None),
        Number(p, s) if *s > 0 => (ColKind::Float64, "number", Some(*p as i32), Some(*s as i32), None),
        Number(p, _)            => (ColKind::Int64,   "number", Some(*p as i32), None, None),
        BinaryDouble | Float(_) => (ColKind::Float64, "binary_double", None, None, None),
        BinaryFloat             => (ColKind::Float64, "binary_float", None, None, None),
        Boolean                 => (ColKind::Bool,    "boolean", None, None, None),
        Date                    => (ColKind::Timestamp, "date", None, None, None),
        Timestamp(p)            => (ColKind::Timestamp, "timestamp", Some(*p as i32), None, None),
        TimestampTZ(p)          => (ColKind::TimestampTz, "timestamp with time zone", Some(*p as i32), None, None),
        TimestampLTZ(p)         => (ColKind::TimestampTz, "timestamp with local time zone", Some(*p as i32), None, None),
        Raw(len)                => (ColKind::Bytes, "raw", None, None, Some(*len as i64)),
        LongRaw                 => (ColKind::Bytes, "long raw", None, None, None),
        Varchar2(len) | NVarchar2(len) => {
            let t = if matches!(ot, NVarchar2(_)) { "nvarchar2" } else { "varchar2" };
            (ColKind::Str, t, None, None, Some(*len as i64))
        }
        Char(len) | NChar(len) => {
            let t = if matches!(ot, NChar(_)) { "nchar" } else { "char" };
            (ColKind::Str, t, None, None, Some(*len as i64))
        }
        CLOB => (ColKind::Str, "clob", None, None, None),
        BLOB => (ColKind::Bytes, "blob", None, None, None),
        _    => (ColKind::Str, "unknown", None, None, None),
    };
    let mut meta = field_meta::make(db_type, precision, scale, length);
    field_meta::stamp_logical(&mut meta, db_type, "oracle");
    (kind, meta)
}

pub fn col_kind_to_datatype(kind: &ColKind) -> DataType {
    match kind {
        ColKind::Int64       => DataType::Int64,
        ColKind::Float64     => DataType::Float64,
        ColKind::Bool        => DataType::Boolean,
        ColKind::Timestamp   => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColKind::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        ColKind::Bytes       => DataType::LargeBinary,
        ColKind::Str         => DataType::Utf8,
    }
}

pub enum ColBuilder {
    Int64(Int64Builder), Float64(Float64Builder), Bool(BooleanBuilder),
    Timestamp(TimestampMicrosecondBuilder), TimestampTz(TimestampMicrosecondBuilder),
    Bytes(LargeBinaryBuilder), Str(StringBuilder),
}

impl ColBuilder {
    pub fn new(kind: &ColKind) -> Self {
        match kind {
            ColKind::Int64       => Self::Int64(Int64Builder::new()),
            ColKind::Float64     => Self::Float64(Float64Builder::new()),
            ColKind::Bool        => Self::Bool(BooleanBuilder::new()),
            ColKind::Timestamp   => Self::Timestamp(TimestampMicrosecondBuilder::new()),
            ColKind::TimestampTz => Self::TimestampTz(TimestampMicrosecondBuilder::new()),
            ColKind::Bytes       => Self::Bytes(LargeBinaryBuilder::new()),
            ColKind::Str         => Self::Str(StringBuilder::new()),
        }
    }
    pub fn append(&mut self, row: &oracle::Row, col_idx: usize) {
        match self {
            Self::Int64(b) => match row.get::<usize, Option<i64>>(col_idx) {
                Ok(Some(v)) => b.append_value(v), Ok(None) => b.append_null(),
                Err(_) => match row.get::<usize, Option<f64>>(col_idx).ok().flatten() {
                    Some(f) => b.append_value(f as i64), None => b.append_null(),
                },
            },
            Self::Float64(b) => match row.get::<usize, Option<f64>>(col_idx) {
                Ok(Some(v)) => b.append_value(v), _ => b.append_null(),
            },
            Self::Bool(b) => match row.get::<usize, Option<bool>>(col_idx) {
                Ok(Some(v)) => b.append_value(v), _ => b.append_null(),
            },
            Self::Timestamp(b) => match row.get::<usize, Option<OracleTimestamp>>(col_idx) {
                Ok(Some(ts)) => b.append_value(oracle_ts_to_micros(&ts)), _ => b.append_null(),
            },
            Self::TimestampTz(b) => match row.get::<usize, Option<OracleTimestamp>>(col_idx) {
                Ok(Some(ts)) => b.append_value(oracle_tstz_to_utc_micros(&ts)), _ => b.append_null(),
            },
            Self::Bytes(b) => match row.get::<usize, Option<Vec<u8>>>(col_idx) {
                Ok(Some(v)) => b.append_value(&v), _ => b.append_null(),
            },
            Self::Str(b) => {
                let val: Option<String> = row.get::<usize, Option<String>>(col_idx).ok().flatten()
                    .or_else(|| row.get::<usize, Option<i64>>(col_idx).ok().flatten().map(|n| n.to_string()))
                    .or_else(|| row.get::<usize, Option<f64>>(col_idx).ok().flatten().map(|f| f.to_string()));
                match val { Some(s) => b.append_value(&s), None => b.append_null() }
            }
        }
    }
    pub fn finish(self) -> ArrayRef {
        match self {
            Self::Int64(mut b)       => Arc::new(b.finish()),
            Self::Float64(mut b)     => Arc::new(b.finish()),
            Self::Bool(mut b)        => Arc::new(b.finish()),
            Self::Timestamp(mut b)   => Arc::new(b.finish()),
            Self::TimestampTz(mut b) => Arc::new(b.finish()),
            Self::Bytes(mut b)       => Arc::new(b.finish()),
            Self::Str(mut b)         => Arc::new(b.finish()),
        }
    }
}

fn oracle_ts_to_micros(ts: &OracleTimestamp) -> i64 {
    let dt = chrono::NaiveDateTime::new(
        chrono::NaiveDate::from_ymd_opt(ts.year(), ts.month(), ts.day())
            .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
        chrono::NaiveTime::from_hms_nano_opt(ts.hour(), ts.minute(), ts.second(), ts.nanosecond()).unwrap_or_default(),
    );
    dt.and_utc().timestamp_micros()
}

fn oracle_tstz_to_utc_micros(ts: &OracleTimestamp) -> i64 {
    let naive = chrono::NaiveDateTime::new(
        chrono::NaiveDate::from_ymd_opt(ts.year(), ts.month(), ts.day())
            .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
        chrono::NaiveTime::from_hms_nano_opt(ts.hour(), ts.minute(), ts.second(), ts.nanosecond()).unwrap_or_default(),
    );
    let offset_secs = ts.tz_hour_offset() as i64 * 3_600 + ts.tz_minute_offset() as i64 * 60;
    naive.and_utc().timestamp_micros() - offset_secs * 1_000_000
}

// ── OracleReadDB ─────────────────────────────────────────────────────────────

pub struct OracleReadDB {
    conn:         OracleConn,
    table:        String,
    schema_name:  String,
    custom_query: Option<String>,
    cursor_col:   Option<String>,
    batch_size:   usize,
    prefetch_rows: Option<u32>,
}

impl OracleReadDB {
    pub fn new(conn_str: &str) -> anyhow::Result<Self> {
        Ok(Self {
            conn: OracleConn::parse(conn_str)?, table: String::new(), schema_name: String::new(),
            custom_query: None, cursor_col: None, batch_size: 1_000, prefetch_rows: None,
        })
    }
}

impl SourceBuilder for OracleReadDB {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn SourceBuilder> { self.table = t; self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn SourceBuilder> { self.schema_name = s; self }
    fn query(mut self: Box<Self>, q: String) -> Box<dyn SourceBuilder> { self.custom_query = Some(q); self }
    fn cursor(mut self: Box<Self>, c: String) -> Box<dyn SourceBuilder> { self.cursor_col = Some(c); self }
    fn batch_size(mut self: Box<Self>, n: usize) -> Box<dyn SourceBuilder> { self.batch_size = n; self }
    fn with_driver_options(self: Box<Self>, _opts: &StepDriverOptions) -> Box<dyn SourceBuilder> { self }

    fn read_schema<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<SchemaRef>> + Send + 'a>> {
        Box::pin(async move {
            let conn  = self.conn.clone();
            let owner = if self.schema_name.is_empty() { String::new() } else { self.schema_name.to_uppercase() };
            let table = self.table.to_uppercase();
            tokio::task::spawn_blocking(move || {
                let oci = conn.open()?;
                let sql = "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_PRECISION, DATA_SCALE, CHAR_LENGTH \
                           FROM ALL_TAB_COLUMNS WHERE OWNER = COALESCE(NULLIF(:1, ''), USER) AND TABLE_NAME = :2 \
                           ORDER BY COLUMN_ID";
                let rows: Vec<oracle::Row> = oci.query_as::<oracle::Row>(sql, &[&owner, &table])?.collect::<oracle::Result<Vec<_>>>()?;
                let fields: Vec<Field> = rows.iter().map(|row| -> anyhow::Result<Field> {
                    let name: String = row.get(0)?;
                    let data_type: String = row.get(1)?;
                    let nullable: String = row.get(2)?;
                    let precision: Option<i32> = row.get::<usize, Option<f64>>(3)?.map(|v| v as i32);
                    let scale: Option<i32> = row.get::<usize, Option<f64>>(4)?.map(|v| v as i32);
                    let char_len: Option<i64> = row.get::<usize, Option<f64>>(5)?.map(|v| v as i64);
                    let dt_lower = data_type.to_lowercase();
                    let arrow_dt = oracle_data_type_to_arrow(&data_type);
                    let meta = field_meta::make_with_source(&dt_lower, precision, scale, char_len, "oracle");
                    Ok(Field::new(name.to_lowercase(), arrow_dt, nullable.eq_ignore_ascii_case("Y")).with_metadata(meta))
                }).collect::<anyhow::Result<Vec<_>>>()?;
                Ok(Arc::new(Schema::new(fields)) as SchemaRef)
            }).await.map_err(|e| anyhow::anyhow!("Oracle read_schema panicked: {e}"))?
        })
    }

    fn exec(self: Box<Self>) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<anyhow::Result<RecordBatch>>(4);
        let Self { conn, table, schema_name, custom_query, cursor_col, batch_size, prefetch_rows, .. } = *self;
        std::thread::spawn(move || {
            let result: anyhow::Result<()> = (|| {
                let oci_conn = conn.open()?;
                if cursor_col.is_some() {
                    let mut last_cursor: Option<String> = None;
                    loop {
                        let sql = oracle_build_query(&table, &schema_name, custom_query.as_deref(), cursor_col.as_deref(), last_cursor.as_deref(), batch_size);
                        let mut sb = oci_conn.statement(&sql);
                        if let Some(n) = prefetch_rows { sb.prefetch_rows(n); }
                        let mut stmt = sb.build()?;
                        let rows_iter = stmt.query(&[])?;
                        let mut schema: Option<Arc<Schema>> = None;
                        let mut builders: Vec<ColBuilder> = Vec::new();
                        let mut row_count = 0usize;
                        let mut new_cursor: Option<String> = None;
                        for row_result in rows_iter {
                            let row: oracle::Row = row_result?;
                            if schema.is_none() {
                                let ci = row.column_info();
                                let (kinds, metas): (Vec<ColKind>, Vec<_>) = ci.iter().map(oracle_col_kind_and_meta).unzip();
                                let fields: Vec<Field> = ci.iter().zip(kinds.iter()).zip(metas.iter())
                                    .map(|((c, k), m)| Field::new(c.name().to_lowercase(), col_kind_to_datatype(k), true).with_metadata(m.clone())).collect();
                                schema = Some(Arc::new(Schema::new(fields)));
                                builders = kinds.iter().map(ColBuilder::new).collect();
                            }
                            for (i, b) in builders.iter_mut().enumerate() { b.append(&row, i); }
                            if let Some(col) = &cursor_col {
                                let val = row.get::<&str, Option<i64>>(col.as_str()).ok().flatten().map(|n| n.to_string())
                                    .or_else(|| row.get::<&str, Option<String>>(col.as_str()).ok().flatten());
                                if val.is_some() { new_cursor = val; }
                            }
                            row_count += 1;
                        }
                        if row_count == 0 { break; }
                        let done = row_count < batch_size;
                        let s = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
                        let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();
                        let batch = RecordBatch::try_new(s, arrays)?;
                        if tx.blocking_send(Ok(batch)).is_err() { break; }
                        last_cursor = new_cursor;
                        if done { break; }
                    }
                } else {
                    let sql = oracle_build_query(&table, &schema_name, custom_query.as_deref(), None, None, batch_size);
                    let mut sb = oci_conn.statement(&sql);
                    if let Some(n) = prefetch_rows { sb.prefetch_rows(n); }
                    let mut stmt = sb.build()?;
                    let rows_iter = stmt.query(&[])?;
                    let mut schema_arc: Option<Arc<Schema>> = None;
                    let mut kinds_cache: Vec<ColKind> = Vec::new();
                    let mut builders: Vec<ColBuilder> = Vec::new();
                    let mut row_count = 0usize;
                    for row_result in rows_iter {
                        let row: oracle::Row = row_result?;
                        if schema_arc.is_none() {
                            let ci = row.column_info();
                            let (kinds, metas): (Vec<ColKind>, Vec<_>) = ci.iter().map(oracle_col_kind_and_meta).unzip();
                            let fields: Vec<Field> = ci.iter().zip(kinds.iter()).zip(metas.iter())
                                .map(|((c, k), m)| Field::new(c.name().to_lowercase(), col_kind_to_datatype(k), true).with_metadata(m.clone())).collect();
                            schema_arc = Some(Arc::new(Schema::new(fields)));
                            builders = kinds.iter().map(ColBuilder::new).collect();
                            kinds_cache = kinds;
                        } else if builders.is_empty() {
                            builders = kinds_cache.iter().map(ColBuilder::new).collect();
                        }
                        for (i, b) in builders.iter_mut().enumerate() { b.append(&row, i); }
                        row_count += 1;
                        if row_count >= batch_size {
                            let s = schema_arc.clone().unwrap();
                            let arrays: Vec<ArrayRef> = std::mem::take(&mut builders).into_iter().map(|b| b.finish()).collect();
                            let batch = RecordBatch::try_new(s, arrays)?;
                            if tx.blocking_send(Ok(batch)).is_err() { return Ok(()); }
                            row_count = 0;
                        }
                    }
                    if row_count > 0 {
                        let s = schema_arc.unwrap_or_else(|| Arc::new(Schema::empty()));
                        let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();
                        let batch = RecordBatch::try_new(s, arrays)?;
                        tx.blocking_send(Ok(batch)).ok();
                    }
                }
                Ok(())
            })();
            if let Err(e) = result { let _ = tx.blocking_send(Err(e)); }
        });
        Box::pin(async_stream::stream! { while let Some(item) = rx.recv().await { yield item; } })
    }

    fn try_clone(&self) -> Option<Box<dyn SourceBuilder>> { None }
}

fn oracle_data_type_to_arrow(data_type: &str) -> DataType {
    let dt = data_type.to_uppercase();
    if dt == "DATE" || (dt.starts_with("TIMESTAMP") && !dt.contains("TIME ZONE")) {
        return DataType::Timestamp(TimeUnit::Microsecond, None);
    }
    if dt.starts_with("TIMESTAMP") && dt.contains("TIME ZONE") {
        return DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")));
    }
    match dt.as_str() {
        "NUMBER" | "FLOAT" | "BINARY_FLOAT" | "BINARY_DOUBLE" => DataType::Float64,
        "BOOLEAN"          => DataType::Boolean,
        "RAW" | "LONG RAW" | "BLOB" => DataType::LargeBinary,
        _                  => DataType::Utf8,
    }
}