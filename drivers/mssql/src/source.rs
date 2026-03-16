//! MSSQL source — streaming `RecordBatch` reader via tiberius.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder,
    LargeBinaryBuilder, StringBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use tiberius::Row;

use crate::util::{MssqlConnParams, mssql_build_query, mssql_build_offset_query};
use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::common::field_meta;
use potato_etl_common::db::traits::SourceBuilder;

// ── Type mapping ─────────────────────────────────────────────────────────────

enum ColKind { Int64, Float64, Bool, Date32, Timestamp, TimestampTz, Time64, Bytes, Str }

fn mssql_col_kind(col: &tiberius::Column) -> ColKind {
    use tiberius::ColumnType as CT;
    match col.column_type() {
        CT::Int1 | CT::Int2 | CT::Int4 | CT::Int8 | CT::Intn => ColKind::Int64,
        CT::Float4 | CT::Float8 | CT::Floatn                 => ColKind::Float64,
        CT::Bit | CT::Bitn                                   => ColKind::Bool,
        CT::Daten                                            => ColKind::Date32,
        CT::Datetime | CT::Datetime2 | CT::Datetimen         => ColKind::Timestamp,
        CT::DatetimeOffsetn                                  => ColKind::TimestampTz,
        CT::Timen                                            => ColKind::Time64,
        CT::BigBinary | CT::BigVarBin | CT::Image            => ColKind::Bytes,
        _                                                    => ColKind::Str,
    }
}

fn col_kind_to_datatype(kind: &ColKind) -> DataType {
    match kind {
        ColKind::Int64       => DataType::Int64,
        ColKind::Float64     => DataType::Float64,
        ColKind::Bool        => DataType::Boolean,
        ColKind::Date32      => DataType::Date32,
        ColKind::Timestamp   => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColKind::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        ColKind::Time64      => DataType::Time64(TimeUnit::Microsecond),
        ColKind::Bytes       => DataType::LargeBinary,
        ColKind::Str         => DataType::Utf8,
    }
}

fn mssql_type_name(ct: tiberius::ColumnType) -> &'static str {
    use tiberius::ColumnType as CT;
    match ct {
        CT::Int1            => "tinyint",
        CT::Int2            => "smallint",
        CT::Int4            => "int",
        CT::Int8            => "bigint",
        CT::Intn            => "int",
        CT::Float4          => "real",
        CT::Float8          => "float",
        CT::Floatn          => "float",
        CT::Bit | CT::Bitn  => "bit",
        CT::Daten           => "date",
        CT::Datetime        => "datetime",
        CT::Datetime2       => "datetime2",
        CT::Datetimen       => "datetime",
        CT::DatetimeOffsetn => "datetimeoffset",
        CT::Timen           => "time",
        CT::BigBinary       => "binary",
        CT::BigVarBin       => "varbinary",
        CT::Image           => "image",
        CT::NVarchar        => "nvarchar",
        CT::BigVarChar      => "varchar",
        CT::BigChar         => "char",
        CT::NChar           => "nchar",
        CT::Text            => "text",
        CT::NText           => "ntext",
        CT::Decimaln        => "decimal",
        CT::Numericn        => "numeric",
        CT::Money           => "money",
        CT::Money4          => "smallmoney",
        CT::Xml             => "xml",
        CT::Udt             => "udt",
        _                   => "unknown",
    }
}

fn mssql_type_to_arrow(data_type: &str) -> DataType {
    match data_type {
        "tinyint" | "smallint" | "int" | "bigint" => DataType::Int64,
        "real" | "float"                           => DataType::Float64,
        "bit"                                      => DataType::Boolean,
        "date"                                     => DataType::Date32,
        "datetime" | "datetime2" | "smalldatetime" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "datetimeoffset"                           => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        "time"                                     => DataType::Time64(TimeUnit::Microsecond),
        "binary" | "varbinary" | "image"           => DataType::LargeBinary,
        _                                          => DataType::Utf8,
    }
}

enum ColBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Bool(BooleanBuilder),
    Date32(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    TimestampTz(TimestampMicrosecondBuilder),
    Time64(Time64MicrosecondBuilder),
    Bytes(LargeBinaryBuilder),
    Str(StringBuilder),
}

impl ColBuilder {
    fn new(kind: &ColKind) -> Self {
        match kind {
            ColKind::Int64       => Self::Int64(Int64Builder::new()),
            ColKind::Float64     => Self::Float64(Float64Builder::new()),
            ColKind::Bool        => Self::Bool(BooleanBuilder::new()),
            ColKind::Date32      => Self::Date32(Date32Builder::new()),
            ColKind::Timestamp   => Self::Timestamp(TimestampMicrosecondBuilder::new()),
            ColKind::TimestampTz => Self::TimestampTz(TimestampMicrosecondBuilder::new()),
            ColKind::Time64      => Self::Time64(Time64MicrosecondBuilder::new()),
            ColKind::Bytes       => Self::Bytes(LargeBinaryBuilder::new()),
            ColKind::Str         => Self::Str(StringBuilder::new()),
        }
    }

    fn append(&mut self, row: &Row, col_idx: usize) {
        match self {
            Self::Int64(b) => {
                let v: Option<i64> =
                    row.try_get::<i64, _>(col_idx).ok().flatten()
                    .or_else(|| row.try_get::<i32, _>(col_idx).ok().flatten().map(|n| n as i64))
                    .or_else(|| row.try_get::<i16, _>(col_idx).ok().flatten().map(|n| n as i64))
                    .or_else(|| row.try_get::<u8,  _>(col_idx).ok().flatten().map(|n| n as i64));
                match v { Some(n) => b.append_value(n), None => b.append_null() }
            }
            Self::Float64(b) => {
                let v: Option<f64> =
                    row.try_get::<f64, _>(col_idx).ok().flatten()
                    .or_else(|| row.try_get::<f32, _>(col_idx).ok().flatten().map(|f| f as f64));
                match v { Some(f) => b.append_value(f), None => b.append_null() }
            }
            Self::Bool(b) => {
                let v = row.try_get::<bool, _>(col_idx).ok().flatten();
                match v { Some(v) => b.append_value(v), None => b.append_null() }
            }
            Self::Date32(b) => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                match row.try_get::<chrono::NaiveDate, _>(col_idx).ok().flatten() {
                    Some(d) => b.append_value(d.signed_duration_since(epoch).num_days() as i32),
                    None    => b.append_null(),
                }
            }
            Self::Timestamp(b) => {
                match row.try_get::<chrono::NaiveDateTime, _>(col_idx).ok().flatten() {
                    Some(dt) => b.append_value(dt.and_utc().timestamp_micros()),
                    None     => b.append_null(),
                }
            }
            Self::TimestampTz(b) => {
                match row.try_get::<chrono::DateTime<chrono::FixedOffset>, _>(col_idx).ok().flatten() {
                    Some(dt) => b.append_value(dt.to_utc().timestamp_micros()),
                    None     => b.append_null(),
                }
            }
            Self::Time64(b) => {
                use chrono::Timelike;
                match row.try_get::<chrono::NaiveTime, _>(col_idx).ok().flatten() {
                    Some(t) => b.append_value(
                        t.num_seconds_from_midnight() as i64 * 1_000_000
                        + t.nanosecond() as i64 / 1_000,
                    ),
                    None => b.append_null(),
                }
            }
            Self::Bytes(b) => {
                match row.try_get::<&[u8], _>(col_idx).ok().flatten() {
                    Some(v) => b.append_value(v),
                    None    => b.append_null(),
                }
            }
            Self::Str(b) => {
                let v: Option<String> =
                    row.try_get::<&str, _>(col_idx).ok().flatten().map(|s| s.to_string())
                    .or_else(|| row.try_get::<rust_decimal::Decimal, _>(col_idx).ok().flatten().map(|d| d.to_string()))
                    .or_else(|| row.try_get::<chrono::DateTime<chrono::FixedOffset>, _>(col_idx).ok().flatten().map(|d| d.to_rfc3339()))
                    .or_else(|| row.try_get::<chrono::NaiveDateTime, _>(col_idx).ok().flatten().map(|d| d.to_string()))
                    .or_else(|| row.try_get::<chrono::NaiveDate, _>(col_idx).ok().flatten().map(|d| d.to_string()))
                    .or_else(|| row.try_get::<chrono::NaiveTime, _>(col_idx).ok().flatten().map(|t| t.to_string()))
                    .or_else(|| row.try_get::<i64,  _>(col_idx).ok().flatten().map(|n| n.to_string()))
                    .or_else(|| row.try_get::<f64,  _>(col_idx).ok().flatten().map(|f| f.to_string()))
                    .or_else(|| row.try_get::<bool, _>(col_idx).ok().flatten().map(|v| v.to_string()));
                match v { Some(s) => b.append_value(&s), None => b.append_null() }
            }
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Int64(mut b)       => Arc::new(b.finish()),
            Self::Float64(mut b)     => Arc::new(b.finish()),
            Self::Bool(mut b)        => Arc::new(b.finish()),
            Self::Date32(mut b)      => Arc::new(b.finish()),
            Self::Timestamp(mut b)   => Arc::new(b.finish()),
            Self::TimestampTz(mut b) => Arc::new(b.finish()),
            Self::Time64(mut b)      => Arc::new(b.finish()),
            Self::Bytes(mut b)       => Arc::new(b.finish()),
            Self::Str(mut b)         => Arc::new(b.finish()),
        }
    }
}

// ── MssqlReadDB ───────────────────────────────────────────────────────────────

pub struct MssqlReadDB {
    params:       MssqlConnParams,
    table:        String,
    schema_name:  String,
    custom_query: Option<String>,
    cursor_col:   Option<String>,
    batch_size:   usize,
}

impl MssqlReadDB {
    pub fn new(conn_str: &str) -> anyhow::Result<Self> {
        Ok(Self {
            params:       MssqlConnParams::parse(conn_str)?,
            table:        String::new(),
            schema_name:  "dbo".into(),
            custom_query: None,
            cursor_col:   None,
            batch_size:   1_000,
        })
    }
}

impl SourceBuilder for MssqlReadDB {
    fn table(mut self: Box<Self>, table: String) -> Box<dyn SourceBuilder> { self.table = table; self }
    fn schema(mut self: Box<Self>, schema: String) -> Box<dyn SourceBuilder> { self.schema_name = schema; self }
    fn query(mut self: Box<Self>, query: String) -> Box<dyn SourceBuilder> { self.custom_query = Some(query); self }
    fn cursor(mut self: Box<Self>, col: String) -> Box<dyn SourceBuilder> { self.cursor_col = Some(col); self }
    fn batch_size(mut self: Box<Self>, n: usize) -> Box<dyn SourceBuilder> { self.batch_size = n; self }
    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SourceBuilder> {
        if let Some(ref ms) = opts.mssql {
            if !ms.init_sql.is_empty() {
                self.params.init_sql = ms.init_sql.clone();
            }
        }
        self
    }

    fn read_schema<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<SchemaRef>> + Send + 'a>> {
        Box::pin(async move {
            let mut client = self.params.connect().await?;

            let sql = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                    CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION,
                    NUMERIC_SCALE, DATETIME_PRECISION
               FROM INFORMATION_SCHEMA.COLUMNS
              WHERE TABLE_SCHEMA = @P1 AND TABLE_NAME = @P2
              ORDER BY ORDINAL_POSITION";

            let rows = client.query(sql, &[
                &self.schema_name.as_str(),
                &self.table.as_str(),
            ]).await?.into_first_result().await?;

            let fields: Vec<Field> = rows.iter().map(|row| {
                let name:      &str         = row.get("COLUMN_NAME").unwrap_or("");
                let data_type: &str         = row.get("DATA_TYPE").unwrap_or("");
                let nullable:  &str         = row.get("IS_NULLABLE").unwrap_or("YES");
                let char_len:  Option<i32>  = row.get("CHARACTER_MAXIMUM_LENGTH");
                let num_prec:  Option<u8>   = row.get("NUMERIC_PRECISION");
                let num_scale: Option<i32>  = row.get("NUMERIC_SCALE");
                let dt_prec:   Option<i16>  = row.get("DATETIME_PRECISION");

                let precision = num_prec.map(|p| p as i32).or_else(|| dt_prec.map(|p| p as i32));
                let length    = char_len.map(|l| l as i64);
                let meta      = field_meta::make_with_source(data_type, precision, num_scale, length, "mssql");
                let arrow_dt  = mssql_type_to_arrow(data_type);

                Field::new(name, arrow_dt, nullable.eq_ignore_ascii_case("YES"))
                    .with_metadata(meta)
            }).collect();

            Ok(Arc::new(Schema::new(fields)))
        })
    }

    fn exec(self: Box<Self>) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
        let Self { params, table, schema_name, custom_query, cursor_col, batch_size, .. } = *self;

        Box::pin(async_stream::try_stream! {
            let mut client = params.connect().await?;

            if cursor_col.is_some() {
                let mut last_cursor: Option<String> = None;

                loop {
                    let sql = mssql_build_query(
                        &table, &schema_name,
                        custom_query.as_deref(),
                        cursor_col.as_deref(),
                        last_cursor.as_deref(),
                        batch_size,
                    );
                    tracing::debug!(sql = %sql, "MssqlReadDB keyset poll");

                    let tib_rows = client.query(&sql, &[]).await?
                        .into_first_result().await?;

                    if tib_rows.is_empty() { break; }
                    let done = tib_rows.len() < batch_size;

                    if let Some(col) = &cursor_col {
                        if let Some(last) = tib_rows.last() {
                            if let Ok(Some(v)) = last.try_get::<i64, _>(col.as_str()) {
                                last_cursor = Some(v.to_string());
                            } else if let Ok(Some(v)) = last.try_get::<&str, _>(col.as_str()) {
                                last_cursor = Some(v.to_string());
                            }
                        }
                    }

                    yield tib_rows_to_batch(&tib_rows)
                        .map_err(|e| anyhow::anyhow!("MSSQL keyset batch: {e}"))?;

                    if done { break; }
                }
            } else {
                let mut offset = 0usize;

                loop {
                    let sql = mssql_build_offset_query(
                        &table, &schema_name,
                        custom_query.as_deref(),
                        offset, batch_size,
                    );
                    tracing::debug!(sql = %sql, offset, "MssqlReadDB full-scan page");

                    let tib_rows = client.query(&sql, &[]).await?
                        .into_first_result().await?;

                    if tib_rows.is_empty() { break; }
                    let done = tib_rows.len() < batch_size;

                    offset += tib_rows.len();

                    yield tib_rows_to_batch(&tib_rows)
                        .map_err(|e| anyhow::anyhow!("MSSQL full-scan batch: {e}"))?;

                    if done { break; }
                }
            }
        })
    }

    fn try_clone(&self) -> Option<Box<dyn SourceBuilder>> {
        // MssqlReadDB doesn't implement Clone (params contain sensitive data).
        // Return None — the DAG executor will create a fresh instance if needed.
        None
    }
}

// ── Batch builder ─────────────────────────────────────────────────────────────

fn tib_rows_to_batch(tib_rows: &[tiberius::Row]) -> anyhow::Result<RecordBatch> {
    let cols   = tib_rows[0].columns();
    let kinds: Vec<ColKind> = cols.iter().map(mssql_col_kind).collect();
    let fields: Vec<Field> = cols.iter().zip(kinds.iter())
        .map(|(c, k)| {
            let type_name = mssql_type_name(c.column_type());
            let mut meta = field_meta::type_only(type_name);
            field_meta::stamp_logical(&mut meta, type_name, "mssql");
            Field::new(c.name(), col_kind_to_datatype(k), true).with_metadata(meta)
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let mut builders: Vec<ColBuilder> = kinds.iter().map(ColBuilder::new).collect();
    for row in tib_rows {
        for (ci, builder) in builders.iter_mut().enumerate() {
            builder.append(row, ci);
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|b| b.finish()).collect();
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| anyhow::anyhow!("MSSQL RecordBatch build: {e}"))
}