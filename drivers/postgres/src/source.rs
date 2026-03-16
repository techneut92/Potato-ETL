//! Postgres source — streaming `RecordBatch` reader.
//!
//! ## Pagination modes
//!
//! ### Keyset pagination (`cursor: <col>` set)
//! Issues repeated `SELECT … WHERE col > ? ORDER BY col LIMIT n` queries.
//!
//! ### Server-side cursor (`cursor:` absent)
//! Opens a single PostgreSQL portal via DECLARE CURSOR + FETCH FORWARD.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use sqlx::postgres::{PgPool, PgRow};
use sqlx::{Column, Row, TypeInfo};

use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::common::field_meta;
use potato_etl_common::db::traits::SourceBuilder;
use potato_etl_common::schema::constants::{META_DESCRIPTION, META_FOREIGN_KEY, META_PRIMARY_KEY};

use crate::util::pg_pool_with_init_sql;

// ── PgReadDB ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct PgReadDB {
    pub conn_str:     String,
    pub table:        String,
    pub schema_name:  String,
    pub custom_query: Option<String>,
    pub cursor_col:   Option<String>,
    pub batch_size:   usize,
    /// SQL statements executed on every new connection in the pool.
    pub init_sql:     Vec<String>,
}

impl PgReadDB {
    pub fn new(conn_str: impl Into<String>) -> Self {
        Self {
            conn_str:     conn_str.into(),
            table:        String::new(),
            schema_name:  "public".into(),
            custom_query: None,
            cursor_col:   None,
            batch_size:   1_000,
            init_sql:     Vec::new(),
        }
    }

    /// Builds a keyset-pagination SQL query.
    fn build_keyset_query(&self, last_cursor: Option<&str>) -> String {
        let base = match &self.custom_query {
            Some(q) => format!("SELECT * FROM ({q}) AS _etl_q"),
            None    => format!(
                "SELECT * FROM \"{}\".\"{}\""  ,
                self.schema_name.replace('"', "\"\""),
                self.table.replace('"', "\"\""),
            ),
        };
        match (&self.cursor_col, last_cursor) {
            (Some(col), Some(val)) => {
                let qcol = quote_ident(col);
                let safe = val.replace('\'', "''");
                format!("{base} WHERE {qcol} > '{safe}' ORDER BY {qcol} ASC LIMIT {}", self.batch_size)
            }
            (Some(col), None) => {
                let qcol = quote_ident(col);
                format!("{base} ORDER BY {qcol} ASC LIMIT {}", self.batch_size)
            }
            _ => format!("{base} LIMIT {}", self.batch_size),
        }
    }
}

// ── SourceBuilder trait implementation ────────────────────────────────────────

impl SourceBuilder for PgReadDB {
    fn table(mut self: Box<Self>, table: String) -> Box<dyn SourceBuilder> {
        self.table = table;
        self
    }

    fn schema(mut self: Box<Self>, schema: String) -> Box<dyn SourceBuilder> {
        self.schema_name = schema;
        self
    }

    fn query(mut self: Box<Self>, query: String) -> Box<dyn SourceBuilder> {
        self.custom_query = Some(query);
        self
    }

    fn cursor(mut self: Box<Self>, col: String) -> Box<dyn SourceBuilder> {
        self.cursor_col = Some(col);
        self
    }

    fn batch_size(mut self: Box<Self>, n: usize) -> Box<dyn SourceBuilder> {
        self.batch_size = n;
        self
    }

    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SourceBuilder> {
        if let Some(ref pg) = opts.postgres {
            if !pg.init_sql.is_empty() {
                self.init_sql = pg.init_sql.clone();
            }
        }
        self
    }

    fn read_schema<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<SchemaRef>> + Send + 'a>> {
        Box::pin(async move {
            let pool = pg_pool_with_init_sql(&self.conn_str, 1, &self.init_sql).await?;

            // ── Column metadata ──────────────────────────────────────────────
            let rows = sqlx::query(
                "SELECT column_name,
                        udt_name,
                        data_type,
                        is_nullable,
                        numeric_precision,
                        numeric_scale,
                        character_maximum_length,
                        datetime_precision
                   FROM information_schema.columns
                  WHERE table_schema = $1 AND table_name = $2
                  ORDER BY ordinal_position",
            )
            .bind(&self.schema_name)
            .bind(&self.table)
            .fetch_all(&pool)
            .await?;

            // ── Primary-key column names ─────────────────────────────────────
            let pk_rows = sqlx::query(
                "SELECT kcu.column_name
                   FROM information_schema.table_constraints  AS tc
                   JOIN information_schema.key_column_usage   AS kcu
                     ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema    = kcu.table_schema
                    AND tc.table_name      = kcu.table_name
                 WHERE tc.constraint_type = 'PRIMARY KEY'
                   AND tc.table_schema    = $1
                   AND tc.table_name      = $2
                 ORDER BY kcu.ordinal_position",
            )
            .bind(&self.schema_name)
            .bind(&self.table)
            .fetch_all(&pool)
            .await
            .unwrap_or_default();

            let pk_cols: std::collections::HashSet<String> = pk_rows
                .iter()
                .map(|r| r.get::<String, _>("column_name"))
                .collect();

            // ── Foreign-key column map ───────────────────────────────────────
            let fk_map: HashMap<String, String> = sqlx::query(
                "SELECT kcu.column_name,
                        ccu.table_schema  AS fk_schema,
                        ccu.table_name    AS fk_table,
                        ccu.column_name   AS fk_column
                   FROM information_schema.table_constraints        AS tc
                   JOIN information_schema.key_column_usage         AS kcu
                     ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema    = kcu.table_schema
                    AND tc.table_name      = kcu.table_name
                   JOIN information_schema.constraint_column_usage  AS ccu
                     ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema    = tc.table_schema
                 WHERE tc.constraint_type = 'FOREIGN KEY'
                   AND tc.table_schema    = $1
                   AND tc.table_name      = $2",
            )
            .bind(&self.schema_name)
            .bind(&self.table)
            .fetch_all(&pool)
            .await
            .unwrap_or_default()
            .iter()
            .map(|r| {
                let col:       String = r.get("column_name");
                let fk_schema: String = r.get("fk_schema");
                let fk_table:  String = r.get("fk_table");
                let fk_col:    String = r.get("fk_column");
                let json = make_fk_json(&fk_schema, &fk_table, &fk_col, &self.schema_name);
                (col, json)
            })
            .collect();

            // ── Column descriptions ──────────────────────────────────────────
            let desc_map: HashMap<String, String> = sqlx::query(
                "SELECT a.attname                             AS column_name,
                        col_description(a.attrelid, a.attnum) AS description
                   FROM pg_catalog.pg_attribute   a
                   JOIN pg_catalog.pg_class       c ON c.oid = a.attrelid
                   JOIN pg_catalog.pg_namespace   n ON n.oid = c.relnamespace
                  WHERE n.nspname      = $1
                    AND c.relname      = $2
                    AND a.attnum       > 0
                    AND NOT a.attisdropped",
            )
            .bind(&self.schema_name)
            .bind(&self.table)
            .fetch_all(&pool)
            .await
            .unwrap_or_default()
            .iter()
            .filter_map(|r| {
                let col:  String         = r.get("column_name");
                let desc: Option<String> = r.get("description");
                desc.map(|d| (col, d))
            })
            .collect();

            // ── Build Arrow fields ───────────────────────────────────────────
            let fields: Vec<Field> = rows.iter().map(|row| {
                let name:      String      = row.get("column_name");
                let udt:       String      = row.get("udt_name");
                let nullable:  String      = row.get("is_nullable");
                let num_prec:  Option<i32> = row.get("numeric_precision");
                let num_scale: Option<i32> = row.get("numeric_scale");
                let char_len:  Option<i32> = row.get("character_maximum_length");
                let dt_prec:   Option<i32> = row.get("datetime_precision");

                let precision  = num_prec.or(dt_prec);
                let length     = char_len.map(|l| l as i64);
                let is_user_defined = {
                    let data_type: String = row.get("data_type");
                    data_type.eq_ignore_ascii_case("USER-DEFINED")
                };
                let mut meta = field_meta::make_with_source(&udt, precision, num_scale, length, "postgres");
                if is_user_defined
                    && !meta.contains_key(potato_etl_common::schema::constants::META_LOGICAL_TYPE)
                {
                    meta.insert(
                        potato_etl_common::schema::constants::META_LOGICAL_TYPE.to_string(),
                        potato_etl_common::schema::field::LogicalType::Enum.as_str().to_string(),
                    );
                }
                let is_pk = pk_cols.contains(&name);

                if is_pk {
                    meta.insert(META_PRIMARY_KEY.to_string(), "true".to_string());
                }
                if let Some(fk_json) = fk_map.get(&name) {
                    meta.insert(META_FOREIGN_KEY.to_string(), fk_json.clone());
                }
                if let Some(desc) = desc_map.get(&name) {
                    meta.insert(META_DESCRIPTION.to_string(), desc.clone());
                }

                let is_nullable = !is_pk && nullable == "YES";
                let arrow_dt    = pg_udt_to_arrow(&udt);

                Field::new(&name, arrow_dt, is_nullable).with_metadata(meta)
            }).collect();

            Ok(Arc::new(Schema::new(fields)))
        })
    }

    fn exec(self: Box<Self>) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<anyhow::Result<RecordBatch>>(4);
        let err_tx   = tx.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("PgReadDB: failed to build fetch thread runtime");

            if let Err(e) = rt.block_on(exec_fetch(*self, tx)) {
                let _ = err_tx.blocking_send(Err(e));
            }
        });

        Box::pin(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }))
    }

    fn try_clone(&self) -> Option<Box<dyn SourceBuilder>> {
        Some(Box::new(self.clone()))
    }
}

// ── exec_fetch — the actual I/O loop ─────────────────────────────────────────

async fn exec_fetch(
    source: PgReadDB,
    tx:     tokio::sync::mpsc::Sender<anyhow::Result<RecordBatch>>,
) -> anyhow::Result<()> {
    let pool     = pg_pool_with_init_sql(&source.conn_str, 2, &source.init_sql).await?;
    let col_meta = query_column_meta(&pool, &source.schema_name, &source.table).await;

    if source.cursor_col.is_some() {
        // ── Keyset pagination ──────────────────────────────────────────────
        let mut conn        = pool.acquire().await?;
        let mut last_cursor: Option<String> = None;

        loop {
            let sql = source.build_keyset_query(last_cursor.as_deref());
            tracing::debug!(sql = %sql, "PgReadDB poll");

            let pg_rows = sqlx::raw_sql(&sql).fetch_all(&mut *conn).await?;
            if pg_rows.is_empty() { break; }
            let done = pg_rows.len() < source.batch_size;

            if let Some(col) = &source.cursor_col {
                if let Some(last) = pg_rows.last() {
                    if let Ok(v) = last.try_get::<i64, _>(col.as_str()) {
                        last_cursor = Some(v.to_string());
                    } else if let Ok(v) = last.try_get::<i32, _>(col.as_str()) {
                        last_cursor = Some(v.to_string());
                    } else if let Ok(v) = last.try_get::<String, _>(col.as_str()) {
                        last_cursor = Some(v);
                    }
                }
            }

            tx.send(Ok(pg_rows_to_record_batch(&pg_rows, &col_meta))).await
                .map_err(|_| anyhow::anyhow!("PgReadDB: downstream consumer dropped"))?;

            if done { break; }
        }
    } else {
        // ── Server-side cursor ─────────────────────────────────────────────
        let select = match &source.custom_query {
            Some(q) => q.clone(),
            None    => format!(
                "SELECT * FROM \"{}\".\"{}\""  ,
                source.schema_name.replace('"', "\"\""),
                source.table.replace('"', "\"\""),
            ),
        };
        let declare = format!("DECLARE _etl_cursor NO SCROLL CURSOR FOR {select}");
        tracing::debug!(sql = %select, "PgReadDB open server-side cursor");

        let mut conn = pool.acquire().await?;
        sqlx::raw_sql("BEGIN").execute(&mut *conn).await?;
        sqlx::raw_sql(&declare).execute(&mut *conn).await?;

        loop {
            let fetch_sql = format!("FETCH FORWARD {} FROM _etl_cursor", source.batch_size);
            tracing::debug!(sql = %fetch_sql, "PgReadDB fetch");

            let pg_rows = sqlx::raw_sql(&fetch_sql).fetch_all(&mut *conn).await?;
            if pg_rows.is_empty() { break; }
            let done = pg_rows.len() < source.batch_size;

            tx.send(Ok(pg_rows_to_record_batch(&pg_rows, &col_meta))).await
                .map_err(|_| anyhow::anyhow!("PgReadDB: downstream consumer dropped"))?;

            if done { break; }
        }

        sqlx::raw_sql("COMMIT").execute(&mut *conn).await?;
    }

    Ok(())
}

// ── Column metadata pre-fetch ─────────────────────────────────────────────────

struct ColumnMeta {
    nullable:        bool,
    is_pk:           bool,
    is_user_defined: bool,
    udt_name:        String,
    precision:       Option<i32>,
    scale:           Option<i32>,
    length:          Option<i64>,
    foreign_key:     Option<String>,
    description:     Option<String>,
}

async fn query_column_meta(
    pool:        &PgPool,
    schema_name: &str,
    table:       &str,
) -> HashMap<String, ColumnMeta> {
    if table.is_empty() {
        return HashMap::new();
    }

    let result = sqlx::query(
        "SELECT column_name,
                udt_name,
                data_type,
                is_nullable,
                numeric_precision,
                numeric_scale,
                character_maximum_length,
                datetime_precision
           FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = $2
          ORDER BY ordinal_position",
    )
    .bind(schema_name)
    .bind(table)
    .fetch_all(pool)
    .await;

    let column_rows = match result {
        Err(e) => {
            tracing::warn!(
                error = %e,
                table = %table,
                "PgReadDB: column metadata query failed — falling back to type-only metadata"
            );
            return HashMap::new();
        }
        Ok(rows) => rows,
    };

    // ── Primary-key lookup ──────────────────────────────────────────────────
    let pk_set: std::collections::HashSet<String> = sqlx::query(
        "SELECT kcu.column_name
           FROM information_schema.table_constraints  AS tc
           JOIN information_schema.key_column_usage   AS kcu
             ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema    = kcu.table_schema
            AND tc.table_name      = kcu.table_name
          WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema    = $1
            AND tc.table_name      = $2
          ORDER BY kcu.ordinal_position",
    )
    .bind(schema_name)
    .bind(table)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .iter()
    .map(|r| r.get::<String, _>("column_name"))
    .collect();

    // ── Foreign-key lookup ───────────────────────────────────────────────────
    let fk_map: HashMap<String, String> = sqlx::query(
        "SELECT kcu.column_name,
                ccu.table_schema  AS fk_schema,
                ccu.table_name    AS fk_table,
                ccu.column_name   AS fk_column
           FROM information_schema.table_constraints        AS tc
           JOIN information_schema.key_column_usage         AS kcu
             ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema    = kcu.table_schema
            AND tc.table_name      = kcu.table_name
           JOIN information_schema.constraint_column_usage  AS ccu
             ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema    = tc.table_schema
         WHERE tc.constraint_type = 'FOREIGN KEY'
           AND tc.table_schema    = $1
           AND tc.table_name      = $2",
    )
    .bind(schema_name)
    .bind(table)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .iter()
    .map(|r| {
        let col:       String = r.get("column_name");
        let fk_schema: String = r.get("fk_schema");
        let fk_table:  String = r.get("fk_table");
        let fk_col:    String = r.get("fk_column");
        let json = make_fk_json(&fk_schema, &fk_table, &fk_col, schema_name);
        (col, json)
    })
    .collect();

    // ── Description lookup ──────────────────────────────────────────────────
    let desc_map: HashMap<String, String> = sqlx::query(
        "SELECT a.attname                             AS column_name,
                col_description(a.attrelid, a.attnum) AS description
           FROM pg_catalog.pg_attribute   a
           JOIN pg_catalog.pg_class       c ON c.oid = a.attrelid
           JOIN pg_catalog.pg_namespace   n ON n.oid = c.relnamespace
          WHERE n.nspname      = $1
            AND c.relname      = $2
            AND a.attnum       > 0
            AND NOT a.attisdropped",
    )
    .bind(schema_name)
    .bind(table)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .iter()
    .filter_map(|r| {
        let col:  String         = r.get("column_name");
        let desc: Option<String> = r.get("description");
        desc.map(|d| (col, d))
    })
    .collect();

    column_rows.into_iter().map(|row| {
        let name:      String      = row.get("column_name");
        let udt:       String      = row.get("udt_name");
        let data_type: String      = row.get("data_type");
        let nullable:  String      = row.get("is_nullable");
        let num_prec:  Option<i32> = row.get("numeric_precision");
        let num_scl:   Option<i32> = row.get("numeric_scale");
        let char_len:  Option<i32> = row.get("character_maximum_length");
        let dt_prec:   Option<i32> = row.get("datetime_precision");

        let precision       = num_prec.or(dt_prec);
        let length          = char_len.map(|l| l as i64);
        let is_pk           = pk_set.contains(&name);
        let is_user_defined = data_type.eq_ignore_ascii_case("USER-DEFINED");
        let fk              = fk_map.get(&name).cloned();
        let desc            = desc_map.get(&name).cloned();

        (name, ColumnMeta {
            nullable:        !is_pk && nullable == "YES",
            is_pk,
            is_user_defined,
            udt_name:        udt.to_lowercase(),
            precision,
            scale:           num_scl,
            length,
            foreign_key:     fk,
            description:     desc,
        })
    }).collect()
}

// ── Direct Arrow building from PgRow ─────────────────────────────────────────

fn pg_rows_to_record_batch(
    rows:     &[PgRow],
    col_meta: &HashMap<String, ColumnMeta>,
) -> RecordBatch {
    if rows.is_empty() {
        return RecordBatch::new_empty(Arc::new(Schema::empty()));
    }

    let cols = rows[0].columns();
    let n    = rows.len();

    let mut fields: Vec<Field>    = Vec::with_capacity(cols.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());

    for col in cols {
        let type_name = col.type_info().name();
        let (dt, arr) = build_pg_column(rows, col.ordinal(), type_name, n);

        let (meta, nullable) = match col_meta.get(col.name()) {
            Some(cm) => {
                let mut m = field_meta::make_with_source(
                    &cm.udt_name,
                    cm.precision,
                    cm.scale,
                    cm.length,
                    "postgres",
                );
                if cm.is_user_defined
                    && !m.contains_key(potato_etl_common::schema::constants::META_LOGICAL_TYPE)
                {
                    m.insert(
                        potato_etl_common::schema::constants::META_LOGICAL_TYPE.to_string(),
                        potato_etl_common::schema::field::LogicalType::Enum.as_str().to_string(),
                    );
                }
                if cm.is_pk {
                    m.insert(META_PRIMARY_KEY.to_string(), "true".to_string());
                }
                if let Some(fk_json) = &cm.foreign_key {
                    m.insert(META_FOREIGN_KEY.to_string(), fk_json.clone());
                }
                if let Some(desc) = &cm.description {
                    m.insert(META_DESCRIPTION.to_string(), desc.clone());
                }
                (m, cm.nullable)
            }
            None => {
                let type_lower = type_name.to_lowercase();
                let mut m = field_meta::type_only(&type_lower);
                field_meta::stamp_logical(&mut m, &type_lower, "postgres");
                (m, true)
            }
        };

        fields.push(Field::new(col.name(), dt, nullable).with_metadata(meta));
        arrays.push(arr);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .expect("schema/arrays mismatch in pg_rows_to_record_batch")
}

fn build_pg_column(
    rows:      &[PgRow],
    col_idx:   usize,
    type_name: &str,
    n:         usize,
) -> (DataType, ArrayRef) {
    macro_rules! typed_col {
        ($Rust:ty, $Builder:ty, $DT:expr) => {{
            let mut b = <$Builder>::with_capacity(n);
            for row in rows {
                match row.try_get::<Option<$Rust>, _>(col_idx) {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None)    => b.append_null(),
                    Err(_)      => b.append_null(),
                }
            }
            ($DT, Arc::new(b.finish()) as ArrayRef)
        }};
    }

    match type_name {
        "INT2" | "SMALLINT" | "SMALLSERIAL"  => typed_col!(i16, Int16Builder,   DataType::Int16),
        "INT4" | "INT"      | "SERIAL"       => typed_col!(i32, Int32Builder,   DataType::Int32),
        "INT8" | "BIGINT"   | "BIGSERIAL"    => typed_col!(i64, Int64Builder,   DataType::Int64),
        "FLOAT4" | "REAL"                    => typed_col!(f32, Float32Builder, DataType::Float32),
        "FLOAT8" | "DOUBLE PRECISION"        => typed_col!(f64, Float64Builder, DataType::Float64),
        "BOOL"   | "BOOLEAN"                 => typed_col!(bool, BooleanBuilder, DataType::Boolean),

        "MONEY" => {
            let mut b = Decimal128Builder::with_capacity(n);
            for row in rows {
                match row.try_get_unchecked::<Option<String>, _>(col_idx) {
                    Ok(Some(s)) => match parse_pg_money(&s) {
                        Some(v) => b.append_value(v),
                        None    => {
                            tracing::warn!(value = %s, "PgReadDB: failed to parse money value — storing as NULL");
                            b.append_null();
                        }
                    },
                    Ok(None) => b.append_null(),
                    Err(_)   => b.append_null(),
                }
            }
            let arr = b
                .finish()
                .with_precision_and_scale(19, 4)
                .expect("Decimal128(19,4) is valid");
            (DataType::Decimal128(19, 4), Arc::new(arr) as ArrayRef)
        }

        "DATE" => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let mut b = Date32Builder::with_capacity(n);
            for row in rows {
                match row.try_get::<Option<chrono::NaiveDate>, _>(col_idx) {
                    Ok(Some(d)) => b.append_value(d.signed_duration_since(epoch).num_days() as i32),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            (DataType::Date32, Arc::new(b.finish()) as ArrayRef)
        }

        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n);
            for row in rows {
                match row.try_get::<Option<chrono::NaiveDateTime>, _>(col_idx) {
                    Ok(Some(dt)) => b.append_value(dt.and_utc().timestamp_micros()),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            (DataType::Timestamp(TimeUnit::Microsecond, None), Arc::new(b.finish()) as ArrayRef)
        }

        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n);
            for row in rows {
                match row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(col_idx) {
                    Ok(Some(dt)) => b.append_value(dt.timestamp_micros()),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            let arr = Arc::new(b.finish().with_timezone("UTC")) as ArrayRef;
            (DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), arr)
        }

        "BYTEA" => {
            let mut b = LargeBinaryBuilder::with_capacity(n, n * 64);
            for row in rows {
                match row.try_get::<Option<Vec<u8>>, _>(col_idx) {
                    Ok(Some(v)) => b.append_value(&v),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            (DataType::LargeBinary, Arc::new(b.finish()) as ArrayRef)
        }

        "TIME" | "TIME WITHOUT TIME ZONE" => {
            use chrono::Timelike;
            let mut b = Time64MicrosecondBuilder::with_capacity(n);
            for row in rows {
                match row.try_get::<Option<chrono::NaiveTime>, _>(col_idx) {
                    Ok(Some(t)) => b.append_value(
                        t.num_seconds_from_midnight() as i64 * 1_000_000
                        + t.nanosecond() as i64 / 1_000,
                    ),
                    Ok(None) | Err(_) => b.append_null(),
                }
            }
            (DataType::Time64(TimeUnit::Microsecond), Arc::new(b.finish()) as ArrayRef)
        }

        _ => {
            let mut b = StringBuilder::with_capacity(n, n * 16);
            for row in rows {
                match row.try_get_unchecked::<Option<String>, _>(col_idx) {
                    Ok(Some(s)) => b.append_value(&s),
                    Ok(None)    => b.append_null(),
                    Err(_)      => b.append_null(),
                }
            }
            (DataType::Utf8, Arc::new(b.finish()) as ArrayRef)
        }
    }
}

// ── Type helpers ──────────────────────────────────────────────────────────────

fn pg_udt_to_arrow(udt: &str) -> DataType {
    match udt {
        "int2"        => DataType::Int16,
        "int4"        => DataType::Int32,
        "int8"        => DataType::Int64,
        "float4"      => DataType::Float32,
        "float8"      => DataType::Float64,
        "bool"        => DataType::Boolean,
        "date"        => DataType::Date32,
        "time"        => DataType::Time64(TimeUnit::Microsecond),
        "timestamp"   => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
        "bytea"       => DataType::LargeBinary,
        "money"       => DataType::Decimal128(19, 4),
        _             => DataType::Utf8,
    }
}

fn quote_ident(s: &str) -> String { format!("\"{}\"", s.replace('"', "\"\"")) }

fn make_fk_json(fk_schema: &str, fk_table: &str, fk_col: &str, current_schema: &str) -> String {
    let mut json = serde_json::Map::new();
    json.insert("table".to_string(), fk_table.to_string().into());
    json.insert("column".to_string(), fk_col.to_string().into());
    if fk_schema != current_schema {
        json.insert("schema".to_string(), fk_schema.to_string().into());
    }
    serde_json::to_string(&json).expect("failed to serialise foreign-key JSON")
}

fn parse_pg_money(s: &str) -> Option<i128> {
    let is_negative = s.contains('(') || s.starts_with('-');

    let numeric: String = s
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == ',')
        .collect();

    if numeric.is_empty() {
        return None;
    }

    let last_dot   = numeric.rfind('.');
    let last_comma = numeric.rfind(',');

    let (int_str, frac_str) = match (last_dot, last_comma) {
        (Some(d), Some(c)) => {
            if d > c {
                let int_part: String = numeric[..d]
                    .chars()
                    .filter(|ch| ch.is_ascii_digit())
                    .collect();
                (int_part, &numeric[d + 1..])
            } else {
                let int_part: String = numeric[..c]
                    .chars()
                    .filter(|ch| ch.is_ascii_digit())
                    .collect();
                (int_part, &numeric[c + 1..])
            }
        }
        (Some(d), None) => {
            (numeric[..d].to_string(), &numeric[d + 1..])
        }
        (None, Some(c)) => {
            (numeric[..c].to_string(), &numeric[c + 1..])
        }
        (None, None) => {
            (numeric, "")
        }
    };

    let int_val: i128 = if int_str.is_empty() { 0 } else {
        int_str.parse().ok()?
    };

    let frac_val: i128 = if frac_str.is_empty() {
        0
    } else {
        let mut frac = String::with_capacity(4);
        for (i, ch) in frac_str.chars().enumerate() {
            if i >= 4 { break; }
            if ch.is_ascii_digit() {
                frac.push(ch);
            }
        }
        while frac.len() < 4 {
            frac.push('0');
        }
        frac.parse().ok()?
    };

    let unscaled = int_val * 10_000 + frac_val;
    Some(if is_negative { -unscaled } else { unscaled })
}