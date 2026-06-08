//! Postgres write sink — `PgWriteDB`.
//!
//! ## Write modes
//!
//! | Mode            | Default mechanism                               | With `staging_table: true`                               |
//! |-----------------|-------------------------------------------------|----------------------------------------------------------|
//! | `append`        | `COPY FROM STDIN (FORMAT BINARY)`               | (same)                                                   |
//! | `insert_ignore` | Chunked parameterized `INSERT … ON CONFLICT`    | Staging table + COPY + `INSERT … ON CONFLICT DO NOTHING` |
//! | `upsert`        | Chunked parameterized `INSERT … ON CONFLICT`    | Staging table + COPY + `INSERT … ON CONFLICT DO UPDATE`  |
//! | `merge_delete`  | Chunked parameterized `INSERT` + `DELETE`        | Staging table + COPY + upsert + `DELETE WHERE pk NOT IN` |
//! | `truncate`      | `TRUNCATE` then `COPY FROM STDIN (FORMAT BINARY)`| (same)                                                  |
//!
//! ## Performance notes
//!
//! - Append mode uses a **persistent COPY stream**: the COPY BINARY header is
//!   sent once on the first batch, row data is streamed per batch, and the
//!   trailer is sent on flush.  This eliminates per-batch COPY open/close
//!   overhead (~2-4ms saved per batch).
//! - Truncate/DropAndReplace uses per-batch COPY within a transaction (the
//!   writer borrows from the transaction and can't be stored persistently).
//! - COPY BINARY serialisation is offloaded to `spawn_blocking` to avoid
//!   stalling the tokio async runtime on CPU-intensive row encoding.
//! - Buffer pre-allocation uses actual string column data sizes to minimise
//!   reallocations for string-heavy schemas.
//! - The optional staging table path (`staging_table: true`) uses COPY BINARY
//!   via a temp table for upsert/insert_ignore, giving ~5-10× throughput
//!   improvement over the default chunked parameterized INSERTs.
//!
//! ## Configuring the staging table fast path
//!
//! ```yaml
//! write_db:
//!   options:
//!     postgres:
//!       staging_table: true      # opt-in: COPY BINARY via temp table
//!       max_connections: 5       # pool size (default: 5)
//! ```

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray,
    FixedSizeBinaryArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
    LargeBinaryArray, LargeStringArray, StringArray,
    Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray,
    TimestampSecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, IntervalUnit, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use sqlx::postgres::{PgCopyIn, PgPoolCopyExt};
use sqlx::postgres::types::PgInterval;
use sqlx::pool::PoolConnection;
use sqlx::{PgPool, Postgres};

use potato_etl_common::config::{DatabaseSchemaConfig, StepDriverOptions};
use potato_etl_common::db::{TableMode, WriteStrategy};
use potato_etl_common::db::common::SinkConfig;
use potato_etl_common::db::traits::SinkBuilder;
use potato_etl_common::schema::ddl::{DdlOptions, generate_ddl_with_schema, SqlDialect, arrow_type_to_sql_dialect};
// `record_batch_to_string_rows` is no longer used — native typed bindings
// eliminate the string roundtrip.  Kept as a comment for historical context.

// ── PgWriteDB ─────────────────────────────────────────────────────────────────

pub struct PgWriteDB {
    pub conn_str: String,
    pub cfg:      SinkConfig,
    pool: Option<PgPool>,
    /// Long-lived transaction for DropAndReplace and ClearAndInsert.
    txn:  Option<sqlx::Transaction<'static, sqlx::Postgres>>,
    /// PK values seen in `merge_delete` mode.
    seen_pk_rows: Vec<Vec<Option<String>>>,
    /// PK column names for `merge_delete` mode.
    seen_pk_col_names: Vec<String>,
    /// Cached column alignment.
    alignment: Option<potato_etl_common::db::common::alignment::ColumnAlignment>,
    /// Target columns for type coercion.
    target_columns: Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>,
    /// Pre-compiled coercion plan.
    coercion_plan: Option<potato_etl_common::db::common::type_coercion::CoercionPlan>,
    /// Whether coercion plan has been compiled.
    coercion_compiled: bool,
    /// Maximum pool connections (configurable via driver options).
    max_connections: u32,
    /// Use a temp staging table for upsert/insert_ignore (opt-in).
    /// When false, uses the default chunked parameterized INSERT path.
    use_staging_table: bool,
    /// Whether the temp staging table has been created this session.
    /// The staging table uses `ON COMMIT DELETE ROWS` so it persists across
    /// batches but its contents are automatically cleared on each COMMIT.
    staging_table_created: bool,
    /// Persistent COPY stream for Append mode (non-transactional).
    ///
    /// When active, the COPY header is sent once on the first batch, row data
    /// is sent per batch, and the trailer + finish are sent in `flush_impl`.
    /// This eliminates per-batch COPY open/close overhead (~2-4ms per batch).
    copy_writer: Option<PgCopyIn<PoolConnection<Postgres>>>,
    /// Cached target SQL types for the batch columns (after alignment).
    /// Used to pass to `copy_binary_rows` for JSONB detection in cross-database flows.
    target_sql_types: Option<Vec<String>>,
    /// Compiled INSERT plan — caches all per-run constants for the parameterized
    /// INSERT path (table name, columns SQL, suffix SQL, chunk size, SQL template).
    /// Compiled on the first batch, reused for all subsequent batches.
    insert_plan: Option<InsertPlan>,
    /// SQL statements executed on every new connection in the pool.
    init_sql: Vec<String>,
}

/// Pre-compiled constants for the parameterized INSERT path.
///
/// All fields are constant for the lifetime of a pipeline run (the schema and
/// write strategy don't change between batches).  Compiled once on the first
/// batch via `InsertPlan::compile()`, then reused for every subsequent batch.
struct InsertPlan {
    /// Fully-quoted table name: `"schema"."table"`.
    full_table: String,
    /// Comma-separated quoted column list: `"col_a", "col_b", ...`.
    cols_sql: String,
    /// ON CONFLICT suffix (empty for plain INSERT, contains DO NOTHING/DO UPDATE SET).
    suffix_sql: String,
    /// Max rows per INSERT chunk: `60_000 / num_cols`, minimum 1.
    chunk_size: usize,
    /// Pre-built SQL for a full chunk (when `rows == chunk_size`).
    /// `None` until the first full chunk is formatted.
    full_chunk_sql: Option<String>,
}

impl InsertPlan {
    /// Compile a new plan from the batch schema and write strategy.
    fn compile(
        full_table: &str,
        schema: &arrow::datatypes::Schema,
        write_strategy: &WriteStrategy,
    ) -> Self {
        let cols_sql = schema.fields().iter()
            .map(|f| format!("\"{}\"", f.name().replace('"', "\"\"")))
            .collect::<Vec<_>>().join(", ");
        let num_cols = schema.fields().len().max(1);
        let chunk_size = (60_000 / num_cols).max(1);

        let schema_ref: arrow::datatypes::SchemaRef = std::sync::Arc::new(schema.clone());
        let suffix_sql = match write_strategy {
            WriteStrategy::InsertIgnore => {
                let pk_cols = potato_etl_common::db::pk_columns(&schema_ref);
                if pk_cols.is_empty() {
                    String::new()
                } else {
                    let conflict = pk_cols.iter()
                        .map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");
                    format!(" ON CONFLICT ({conflict}) DO NOTHING")
                }
            }
            WriteStrategy::Upsert | WriteStrategy::MergeDelete => {
                let pk_cols = potato_etl_common::db::pk_columns(&schema_ref);
                if pk_cols.is_empty() {
                    String::new()
                } else {
                    let conflict = pk_cols.iter()
                        .map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");
                    let updates: Vec<String> = schema.fields().iter()
                        .filter(|f| !pk_cols.contains(f.name()))
                        .map(|f| format!("\"{}\" = EXCLUDED.\"{}\"", f.name(), f.name()))
                        .collect();
                    if !updates.is_empty() {
                        format!(" ON CONFLICT ({conflict}) DO UPDATE SET {}", updates.join(", "))
                    } else {
                        format!(" ON CONFLICT ({conflict}) DO NOTHING")
                    }
                }
            }
            _ => String::new(),
        };

        Self {
            full_table: full_table.to_string(),
            cols_sql,
            suffix_sql,
            chunk_size,
            full_chunk_sql: None,
        }
    }

    /// Build or return cached SQL for the given row count.
    fn sql_for_rows(&mut self, typed_cols: &[TypedColumn<'_>], rows: usize) -> String {
        if rows == self.chunk_size {
            if let Some(ref cached) = self.full_chunk_sql {
                return cached.clone();
            }
            let sql = build_insert_sql_planned(
                &self.full_table, &self.cols_sql, &self.suffix_sql, typed_cols, rows,
            );
            self.full_chunk_sql = Some(sql.clone());
            sql
        } else {
            build_insert_sql_planned(
                &self.full_table, &self.cols_sql, &self.suffix_sql, typed_cols, rows,
            )
        }
    }
}

impl PgWriteDB {
    pub fn new(conn_str: impl Into<String>) -> Self {
        Self {
            conn_str: conn_str.into(),
            cfg:      SinkConfig::new("public"),
            pool:     None,
            txn:      None,
            seen_pk_rows: Vec::new(),
            seen_pk_col_names: Vec::new(),
            alignment: None,
            target_columns: None,
            coercion_plan: None,
            coercion_compiled: false,
            max_connections: 5,
            use_staging_table: false,
            staging_table_created: false,
            copy_writer: None,
            target_sql_types: None,
            insert_plan: None,
            init_sql: Vec::new(),
        }
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    async fn ensure_pool(&mut self) -> anyhow::Result<()> {
        if self.pool.is_none() {
            self.pool = Some(
                crate::util::pg_pool_with_init_sql(
                    &self.conn_str,
                    self.max_connections,
                    &self.init_sql,
                ).await?
            );
        }
        Ok(())
    }

    async fn ensure_table(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
        if self.cfg.table_prepared { return Ok(()); }
        self.cfg.table_prepared = true;

        let pool       = self.pool.as_ref().unwrap();
        let full_table = pg_full_table(&self.cfg.schema_name, &self.cfg.table);
        let ddl_schema = self.cfg.ddl_schema.clone().unwrap_or_else(|| batch.schema());

        let pg_major = pg_major_version(pool).await.unwrap_or(0);
        let ddl_opts = DdlOptions { pg_major_version: Some(pg_major), ..Default::default() };

        match &self.cfg.table_mode {
            TableMode::UseExisting => { /* nothing */ }

            TableMode::CreateIfNotExists => {
                let db_config = self.cfg.database_schema_config.as_ref();
                let stmts = generate_ddl_with_schema(
                    &self.cfg.table,
                    Some(&self.cfg.schema_name),
                    &ddl_schema,
                    SqlDialect::Postgres,
                    None,
                    db_config,
                    ddl_opts,
                );
                tracing::debug!(table = %self.cfg.table, "Postgres DDL:\n{}", stmts.create_table);
                for stmt in &stmts.pre_create {
                    tracing::trace!(table = %self.cfg.table, "Postgres pre-create DDL:\n{}", stmt);
                    sqlx::query(stmt).execute(pool).await?;
                }
                sqlx::query(&stmts.create_table).execute(pool).await?;
                for stmt in &stmts.post_create {
                    tracing::trace!(table = %self.cfg.table, "Postgres post-create DDL:\n{}", stmt);
                    sqlx::query(stmt).execute(pool).await?;
                }
                tracing::info!(table = %self.cfg.table, "Postgres CREATE TABLE IF NOT EXISTS applied");
            }

            TableMode::DropAndReplace => {
                let txn = pool.begin().await?;
                self.txn = Some(txn);
                let txn = self.txn.as_mut().unwrap();
                sqlx::query(&format!("DROP TABLE IF EXISTS {full_table}"))
                    .execute(&mut **txn).await?;
                let db_config = self.cfg.database_schema_config.as_ref();
                let stmts = generate_ddl_with_schema(
                    &self.cfg.table,
                    Some(&self.cfg.schema_name),
                    &ddl_schema,
                    SqlDialect::Postgres,
                    None,
                    db_config,
                    ddl_opts,
                );
                tracing::debug!(table = %self.cfg.table, "Postgres DDL:\n{}", stmts.create_table);
                for stmt in &stmts.pre_create {
                    tracing::trace!(table = %self.cfg.table, "Postgres pre-create DDL:\n{}", stmt);
                    sqlx::query(stmt).execute(&mut **txn).await?;
                }
                sqlx::query(&stmts.create_table).execute(&mut **txn).await?;
                for stmt in &stmts.post_create {
                    tracing::trace!(table = %self.cfg.table, "Postgres post-create DDL:\n{}", stmt);
                    sqlx::query(stmt).execute(&mut **txn).await?;
                }
                tracing::info!(table = %self.cfg.table, "Postgres DROP + CREATE TABLE applied");
            }
        }

        if matches!(self.cfg.write_strategy, WriteStrategy::Truncate)
            && !matches!(self.cfg.table_mode, TableMode::DropAndReplace)
        {
            let txn = pool.begin().await?;
            self.txn = Some(txn);
            let txn = self.txn.as_mut().unwrap();
            sqlx::query(&format!("TRUNCATE TABLE {full_table}"))
                .execute(&mut **txn).await?;
            tracing::info!(table = %self.cfg.table, "Postgres TRUNCATE applied");
        }

        Ok(())
    }

    async fn ensure_alignment(&mut self, batch_schema: &SchemaRef) -> anyhow::Result<()> {
        use potato_etl_common::db::common::alignment::{self as align};

        if self.alignment.is_some() { return Ok(()); }

        let did_create = matches!(self.cfg.table_mode, TableMode::DropAndReplace);
        if did_create { return Ok(()); }

        let target_cols = if let Some(txn) = self.txn.as_mut() {
            crate::util::pg_introspect_table_columns(
                &mut **txn, &self.cfg.schema_name, &self.cfg.table,
            ).await?
        } else {
            let pool = self.pool.as_ref().unwrap();
            crate::util::pg_introspect_table_columns(
                pool, &self.cfg.schema_name, &self.cfg.table,
            ).await?
        };

        let target_cols = match target_cols {
            Some(cols) => cols,
            None => return Ok(()),
        };

        let table_display = format!("{}.{}", self.cfg.schema_name, self.cfg.table);
        let result = align::compute_alignment(
            batch_schema,
            &target_cols,
            self.cfg.missing_column_behavior,
            &self.cfg.rename_targets(),
            &table_display,
        )?;

        self.target_columns = Some(target_cols);
        self.alignment = Some(result);
        Ok(())
    }

    fn align_batch(&mut self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        let aligned = match &self.alignment {
            Some(a) => potato_etl_common::db::common::alignment::apply_alignment(batch, a)?,
            None    => batch,
        };

        // Cache target SQL types for JSONB detection in copy_binary_rows.
        if self.target_sql_types.is_none() {
            if let Some(tc) = &self.target_columns {
                let tc_map: std::collections::HashMap<&str, &str> = tc.iter()
                    .map(|c| (c.name.as_str(), c.data_type.as_str()))
                    .collect();
                self.target_sql_types = Some(
                    aligned.schema().fields().iter()
                        .map(|f| tc_map.get(f.name().as_str())
                            .unwrap_or(&"")
                            .to_lowercase())
                        .collect()
                );
            }
        }

        // Coerce integer source columns to Boolean when the target column is
        // BOOL/BOOLEAN — MySQL `TINYINT(1)` flags surface as ints but Postgres
        // rejects them in an INSERT against a BOOLEAN column. Arrow's cast
        // turns 0→false, non-zero→true (matches pandas semantics).
        let aligned = if let Some(types) = &self.target_sql_types {
            use arrow::datatypes::{DataType, Field, Schema};
            let schema = aligned.schema();
            let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
            let mut new_cols: Vec<arrow::array::ArrayRef> = Vec::with_capacity(schema.fields().len());
            let mut any_cast = false;
            for (idx, field) in schema.fields().iter().enumerate() {
                let target = types.get(idx).map(|s| s.as_str()).unwrap_or("");
                let src_dt = field.data_type();
                let needs_bool = matches!(target, "bool" | "boolean")
                    && matches!(
                        src_dt,
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                      | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64,
                    );
                if needs_bool {
                    let cast = arrow::compute::cast(aligned.column(idx).as_ref(), &DataType::Boolean)?;
                    new_fields.push(Field::new(field.name(), DataType::Boolean, field.is_nullable())
                        .with_metadata(field.metadata().clone()));
                    new_cols.push(cast);
                    any_cast = true;
                } else {
                    new_fields.push((**field).clone());
                    new_cols.push(arrow::array::ArrayRef::clone(aligned.column(idx)));
                }
            }
            if any_cast {
                let new_schema = std::sync::Arc::new(Schema::new_with_metadata(new_fields, schema.metadata().clone()));
                RecordBatch::try_new(new_schema, new_cols)?
            } else {
                aligned
            }
        } else {
            aligned
        };

        Ok(aligned)
    }

    /// **DEPRECATED**: Type coercion is now handled inline by `BinaryCol`
    /// (COPY BINARY path) and `TypedColumn` (parameterized INSERT path).
    /// This method is no longer called from `write_impl`.
    #[allow(dead_code)]
    fn coerce_batch(&mut self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        use potato_etl_common::db::common::type_coercion::{
            compile_coercion_plan, apply_coercion_plan, TargetColumn,
        };
        use crate::type_registry::PostgresTypeRegistry;

        if self.coercion_compiled {
            return match &self.coercion_plan {
                Some(plan) => apply_coercion_plan(batch, plan),
                None       => Ok(batch),
            };
        }

        self.coercion_compiled = true;

        let target_cols_adapted: Option<Vec<TargetColumn>> = self.target_columns.as_ref().map(|cols| {
            cols.iter()
                .map(|c| TargetColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                })
                .collect()
        });

        self.coercion_plan = compile_coercion_plan(
            &batch.schema(),
            target_cols_adapted.as_deref(),
            &PostgresTypeRegistry,
        )?;

        match &self.coercion_plan {
            Some(plan) => apply_coercion_plan(batch, plan),
            None       => Ok(batch),
        }
    }

    // ── write / flush (called by SinkBuilder trait) ──────────────────────────

    async fn write_impl(&mut self, batch: RecordBatch) -> anyhow::Result<usize> {
        if batch.num_rows() == 0 { return Ok(0); }

        let is_first_batch = !self.cfg.table_prepared;

        self.ensure_pool().await?;
        self.ensure_table(&batch).await?;
        self.ensure_alignment(&batch.schema()).await?;

        let batch = self.align_batch(batch)?;
        // NOTE: coercion is fully eliminated.  Both paths handle type
        // conversions inline:
        //   - COPY BINARY: `BinaryCol` target-aware variants (UUID parse,
        //     integer downcast, timestamp unit conversion, decimal rescale)
        //   - Parameterized INSERT: `TypedColumn` target-aware variants
        //     (tz strip/add, UUID parse, integer widening)
        // This eliminates all intermediate array allocations from
        // `arrow::compute::cast()` that coercion previously required.

        let full_table = pg_full_table(&self.cfg.schema_name, &self.cfg.table);
        let schema     = batch.schema();

        // ── First-row trace logging ──────────────────────────────────────────
        if is_first_batch && batch.num_rows() > 0 {
            tracing::trace!(
                table = %self.cfg.table,
                "Postgres first row (after alignment):"
            );
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let value_str = if col.is_null(0) {
                    "NULL".to_string()
                } else {
                    use arrow::array::*;
                    match col.data_type() {
                        DataType::Utf8 => {
                            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                            format!("Utf8({:?})", arr.value(0))
                        }
                        DataType::LargeUtf8 => {
                            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                            format!("LargeUtf8({:?})", arr.value(0))
                        }
                        DataType::Int8 => {
                            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
                            format!("Int8({})", arr.value(0))
                        }
                        DataType::Int16 => {
                            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                            format!("Int16({})", arr.value(0))
                        }
                        DataType::Int32 => {
                            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                            format!("Int32({})", arr.value(0))
                        }
                        DataType::Int64 => {
                            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                            format!("Int64({})", arr.value(0))
                        }
                        DataType::UInt8 => {
                            let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                            format!("UInt8({})", arr.value(0))
                        }
                        DataType::UInt16 => {
                            let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                            format!("UInt16({})", arr.value(0))
                        }
                        DataType::UInt32 => {
                            let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                            format!("UInt32({})", arr.value(0))
                        }
                        DataType::UInt64 => {
                            let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                            format!("UInt64({})", arr.value(0))
                        }
                        DataType::Float32 => {
                            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                            format!("Float32({})", arr.value(0))
                        }
                        DataType::Float64 => {
                            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                            format!("Float64({})", arr.value(0))
                        }
                        DataType::Boolean => {
                            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                            format!("Boolean({})", arr.value(0))
                        }
                        DataType::Timestamp(unit, tz) => {
                            let raw = match unit {
                                TimeUnit::Second =>
                                    col.as_any().downcast_ref::<TimestampSecondArray>()
                                        .map(|a| a.value(0).to_string()),
                                TimeUnit::Millisecond =>
                                    col.as_any().downcast_ref::<TimestampMillisecondArray>()
                                        .map(|a| a.value(0).to_string()),
                                TimeUnit::Microsecond =>
                                    col.as_any().downcast_ref::<TimestampMicrosecondArray>()
                                        .map(|a| a.value(0).to_string()),
                                TimeUnit::Nanosecond =>
                                    col.as_any().downcast_ref::<TimestampNanosecondArray>()
                                        .map(|a| a.value(0).to_string()),
                            };
                            format!("Timestamp({:?}, {:?}, raw_value={})", unit, tz,
                                raw.unwrap_or_else(|| "<error>".to_string()))
                        }
                        DataType::Date32 => {
                            let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                            format!("Date32(days={})", arr.value(0))
                        }
                        DataType::Time64(unit) => {
                            format!("Time64({:?}, raw_value={})", unit,
                                col.as_any().downcast_ref::<Time64MicrosecondArray>()
                                    .map(|a| a.value(0).to_string())
                                    .unwrap_or_else(|| "<error>".to_string()))
                        }
                        DataType::Decimal128(_precision, scale) => {
                            let dec = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                            let raw = dec.value(0);
                            format!("Decimal128(raw={}, scale={})", raw, scale)
                        }
                        DataType::Interval(unit) => {
                            format!("Interval({:?})", unit)
                        }
                        DataType::Duration(unit) => {
                            format!("Duration({:?})", unit)
                        }
                        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                            let len = col.as_any().downcast_ref::<BinaryArray>()
                                .map(|a| a.value(0).len())
                                .or_else(|| col.as_any().downcast_ref::<LargeBinaryArray>().map(|a| a.value(0).len()))
                                .or_else(|| col.as_any().downcast_ref::<FixedSizeBinaryArray>().map(|a| a.value(0).len()))
                                .unwrap_or(0);
                            format!("Binary({} bytes)", len)
                        }
                        _ => format!("{:?}", col.data_type()),
                    }
                };
                tracing::trace!(
                    table = %self.cfg.table,
                    col_idx,
                    col_name = %field.name(),
                    value = %value_str,
                    "  column value"
                );
            }
        }

        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let cols_sql = col_names.iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>().join(", ");
        let num_rows = batch.num_rows();

        match &self.cfg.write_strategy.clone() {
            WriteStrategy::Append | WriteStrategy::Truncate => {
                // COPY BINARY path — BinaryCol handles type conversions inline
                // (UUID parse, integer downcast, timestamp unit conversion,
                // decimal rescaling).  No coercion step needed.
                let copy_sql = format!(
                    "COPY {full_table} ({cols_sql}) FROM STDIN (FORMAT BINARY)"
                );
                match self.txn.as_mut() {
                    Some(txn) => {
                        // Transaction path (Truncate / DropAndReplace): per-batch COPY.
                        // We can't store the writer because it borrows from the
                        // transaction which is owned by self (self-referential).
                        let tt = self.target_sql_types.clone();
                        let data = tokio::task::spawn_blocking(move || {
                            let tt_refs: Option<Vec<&str>> = tt.as_ref()
                                .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>());
                            record_batch_to_copy_binary_with_types(&batch, tt_refs.as_deref())
                        }).await?;
                        let mut writer = (&mut **txn).copy_in_raw(&copy_sql).await?;
                        writer.send(data).await?;
                        writer.finish().await?;
                    }
                    None => {
                        // ── Persistent COPY stream (Append mode) ─────────────
                        //
                        // The COPY writer is opened once on the first batch and
                        // kept alive across all subsequent batches.  This saves
                        // ~2-4ms of COPY open/close overhead per batch.
                        //
                        // Protocol: header (1×) → rows (per batch) → trailer (flush)
                        let is_first = self.copy_writer.is_none();
                        if is_first {
                            let pool = self.pool.as_ref().unwrap();
                            let mut writer = pool.copy_in_raw(&copy_sql).await?;
                            // Send the 19-byte COPY BINARY header once.
                            writer.send(copy_binary_header()).await?;
                            self.copy_writer = Some(writer);
                            tracing::debug!(
                                table = %self.cfg.table,
                                "Postgres persistent COPY stream opened"
                            );
                        }
                        // Serialise only row data (no header/trailer) on a
                        // blocking thread, then send via the persistent writer.
                        let tt = self.target_sql_types.clone();
                        let rows_data = tokio::task::spawn_blocking(move || {
                            let tt_refs: Option<Vec<&str>> = tt.as_ref()
                                .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>());
                            copy_binary_rows(&batch, tt_refs.as_deref())
                        }).await?;
                        // Take the writer out to avoid borrow conflicts in
                        // error handling — if send() fails we need to abort
                        // the writer (consuming it) and return the error.
                        let mut writer = self.copy_writer.take().unwrap();
                        match writer.send(rows_data).await {
                            Ok(_) => {
                                // Put the writer back for the next batch.
                                self.copy_writer = Some(writer);
                            }
                            Err(e) => {
                                // Abort the broken writer — the connection is
                                // returned to the pool and the COPY is cancelled.
                                writer.abort("send error").await.ok();
                                return Err(e.into());
                            }
                        }
                    }
                }
            }

            WriteStrategy::InsertIgnore => {
                let pk_cols = potato_etl_common::db::pk_columns(&schema);
                anyhow::ensure!(!pk_cols.is_empty(),
                    "mode=insert_ignore requires at least one column marked \
                     `primary_key: true` in database_columns");
                let conflict_cols = pk_cols.iter()
                    .map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");

                if self.use_staging_table {
                    // ── Staging table + COPY BINARY (fast path) ──────────────
                    // BinaryCol handles type conversions inline — no coercion needed.
                    let pool = self.pool.as_ref().unwrap();
                    let staging = "_etl_staging";
                    let staging_ddl = build_staging_ddl(staging, &schema, self.target_columns.as_deref());
                    let mut txn = pool.begin().await?;

                    if !self.staging_table_created {
                        sqlx::query(&format!("DROP TABLE IF EXISTS {staging}"))
                            .execute(&mut *txn).await?;
                        sqlx::query(&staging_ddl).execute(&mut *txn).await?;
                        self.staging_table_created = true;
                    }

                    let staging_cols_sql = cols_sql.clone();
                    let copy_sql = format!(
                        "COPY {staging} ({staging_cols_sql}) FROM STDIN (FORMAT BINARY)"
                    );
                    let tt = self.target_sql_types.clone();
                    let data = tokio::task::spawn_blocking(move || {
                        let tt_refs: Option<Vec<&str>> = tt.as_ref()
                            .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>());
                        record_batch_to_copy_binary_with_types(&batch, tt_refs.as_deref())
                    }).await?;
                    let mut writer = (&mut *txn).copy_in_raw(&copy_sql).await?;
                    writer.send(data).await?;
                    writer.finish().await?;

                    let insert_sql = format!(
                        "INSERT INTO {full_table} ({cols_sql}) \
                         SELECT {cols_sql} FROM {staging} \
                         ON CONFLICT ({conflict_cols}) DO NOTHING"
                    );
                    sqlx::query(&insert_sql).execute(&mut *txn).await?;

                    txn.commit().await?;
                } else {
                    // ── Default path: compiled parameterized INSERT ───────────
                    // Uses InsertPlan to cache SQL template + suffix across batches.
                    self.execute_insert_plan(&batch, &full_table, &schema).await?;
                }
            }

            WriteStrategy::Upsert | WriteStrategy::MergeDelete => {
                let pk_cols = potato_etl_common::db::pk_columns(&schema);
                anyhow::ensure!(!pk_cols.is_empty(),
                    "mode=upsert/merge_delete requires at least one column marked \
                     `primary_key: true` in database_columns");
                let conflict_cols = pk_cols.iter()
                    .map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");
                let updates: Vec<String> = schema.fields().iter()
                    .filter(|f| !pk_cols.contains(f.name()))
                    .map(|f| format!("\"{}\" = EXCLUDED.\"{}\"", f.name(), f.name()))
                    .collect();

                if self.use_staging_table {
                    // ── Staging table + COPY BINARY (fast path) ──────────────
                    // BinaryCol handles type conversions inline — no coercion needed.
                    let pool = self.pool.as_ref().unwrap();
                    let staging = "_etl_staging";
                    let staging_ddl = build_staging_ddl(staging, &schema, self.target_columns.as_deref());
                    let mut txn = pool.begin().await?;

                    if !self.staging_table_created {
                        sqlx::query(&format!("DROP TABLE IF EXISTS {staging}"))
                            .execute(&mut *txn).await?;
                        sqlx::query(&staging_ddl).execute(&mut *txn).await?;
                        self.staging_table_created = true;
                    }

                    let staging_cols_sql = cols_sql.clone();
                    let copy_sql = format!(
                        "COPY {staging} ({staging_cols_sql}) FROM STDIN (FORMAT BINARY)"
                    );
                    let batch_clone = if matches!(self.cfg.write_strategy, WriteStrategy::MergeDelete) {
                        Some(batch.clone())
                    } else {
                        None
                    };
                    let tt = self.target_sql_types.clone();
                    let data = tokio::task::spawn_blocking(move || {
                        let tt_refs: Option<Vec<&str>> = tt.as_ref()
                            .map(|v| v.iter().map(|s| s.as_str()).collect::<Vec<_>>());
                        record_batch_to_copy_binary_with_types(&batch, tt_refs.as_deref())
                    }).await?;
                    let mut writer = (&mut *txn).copy_in_raw(&copy_sql).await?;
                    writer.send(data).await?;
                    writer.finish().await?;

                    let upsert_sql = if !updates.is_empty() {
                        format!(
                            "INSERT INTO {full_table} ({cols_sql}) \
                             SELECT {cols_sql} FROM {staging} \
                             ON CONFLICT ({conflict_cols}) DO UPDATE SET {}",
                            updates.join(", ")
                        )
                    } else {
                        format!(
                            "INSERT INTO {full_table} ({cols_sql}) \
                             SELECT {cols_sql} FROM {staging} \
                             ON CONFLICT ({conflict_cols}) DO NOTHING"
                        )
                    };
                    sqlx::query(&upsert_sql).execute(&mut *txn).await?;

                    txn.commit().await?;

                    if matches!(self.cfg.write_strategy, WriteStrategy::MergeDelete) {
                        if self.seen_pk_col_names.is_empty() {
                            self.seen_pk_col_names = pk_cols.clone();
                        }
                        if let Some(batch) = batch_clone {
                            let pk_rows = extract_pk_strings(&batch, &pk_cols, &schema);
                            self.seen_pk_rows.extend(pk_rows);
                        }
                    }
                } else {
                    // ── Default path: compiled parameterized INSERT ───────────
                    // Uses InsertPlan to cache SQL template + suffix across batches.
                    self.execute_insert_plan(&batch, &full_table, &schema).await?;

                    if matches!(self.cfg.write_strategy, WriteStrategy::MergeDelete) {
                        if self.seen_pk_col_names.is_empty() {
                            self.seen_pk_col_names = pk_cols.clone();
                        }
                        let pk_rows = extract_pk_strings(&batch, &pk_cols, &schema);
                        self.seen_pk_rows.extend(pk_rows);
                    }
                }
            }
        }

        tracing::debug!(table = %self.cfg.table, rows = num_rows, "Postgres batch written");
        Ok(num_rows)
    }

    async fn flush_impl(&mut self) -> anyhow::Result<()> {
        // ── Finish persistent COPY stream (Append mode) ──────────────────
        // Must happen BEFORE pool.close() since the writer holds a connection.
        if let Some(mut writer) = self.copy_writer.take() {
            // Send the 2-byte COPY BINARY trailer to signal end-of-data.
            writer.send(copy_binary_trailer()).await?;
            writer.finish().await?;
            tracing::debug!(table = %self.cfg.table, "Postgres persistent COPY stream closed");
        }

        if matches!(self.cfg.write_strategy, WriteStrategy::MergeDelete)
            && !self.seen_pk_rows.is_empty()
        {
            let pool       = self.pool.as_ref();
            let schema_str = &self.cfg.schema_name;
            let table_str  = &self.cfg.table;
            if let Some(pool) = pool {
                let full_table  = pg_full_table(schema_str, table_str);
                let pk_count = self.seen_pk_rows.first().map(|r| r.len()).unwrap_or(1);
                let values: Vec<String> = self.seen_pk_rows.iter().map(|pk_vals| {
                    let inner: Vec<String> = pk_vals.iter().map(|v| match v {
                        Some(s) => format!("'{}'", s.replace('\'', "''")),
                        None    => "NULL".into(),
                    }).collect();
                    if pk_count == 1 { inner[0].clone() } else { format!("({})", inner.join(", ")) }
                }).collect();
                let pk_col_names_sql = self.seen_pk_col_names.iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>().join(", ");
                let not_in_list = values.join(", ");
                let delete_sql = format!(
                    "DELETE FROM {full_table} WHERE ({pk_col_names_sql}) NOT IN ({not_in_list})"
                );
                tracing::debug!(table = %table_str, "PgWriteDB: MergeDelete — deleting absent rows");
                sqlx::query(&delete_sql).execute(pool).await?;
            }
            self.seen_pk_rows.clear();
            self.seen_pk_col_names.clear();
        }

        if let Some(txn) = self.txn.take() {
            txn.commit().await?;
        }
        if let Some(pool) = self.pool.take() { pool.close().await; }
        tracing::info!(table = %self.cfg.table, "Postgres flush complete");
        self.cfg.table_prepared = false;
        self.alignment = None;
        self.coercion_plan = None;
        self.coercion_compiled = false;
        self.staging_table_created = false;
        self.insert_plan = None;
        Ok(())
    }

    /// Execute the chunked parameterized INSERT loop using the cached `InsertPlan`.
    ///
    /// Compiles the plan on the first call, then reuses it for subsequent batches.
    /// The plan caches: `full_table`, `cols_sql`, `suffix_sql`, `chunk_size`, and
    /// the full-chunk SQL template.
    async fn execute_insert_plan(
        &mut self,
        batch: &RecordBatch,
        full_table: &str,
        schema: &arrow::datatypes::Schema,
    ) -> anyhow::Result<()> {
        // Compile the plan on first batch (or if reset after flush).
        if self.insert_plan.is_none() {
            self.insert_plan = Some(InsertPlan::compile(
                full_table, schema, &self.cfg.write_strategy,
            ));
        }

        // Resolve typed columns first (immutable borrow of target_sql_types).
        let typed_cols = resolve_typed_columns(batch, self.target_sql_types.as_deref());
        let total_rows = batch.num_rows();

        // Begin transaction (immutable borrow of pool).
        let pool   = self.pool.as_ref().unwrap();
        let mut txn = pool.begin().await?;

        // Mutable borrow of insert_plan for SQL caching.
        let plan = self.insert_plan.as_mut().unwrap();
        let chunk_size = plan.chunk_size;

        for start in (0..total_rows).step_by(chunk_size) {
            let end = (start + chunk_size).min(total_rows);
            let rows_in_chunk = end - start;
            let sql = plan.sql_for_rows(&typed_cols, rows_in_chunk);
            execute_planned_insert(&typed_cols, start..end, &sql, &mut txn).await?;
        }
        txn.commit().await?;
        Ok(())
    }
}

// ── SinkBuilder trait ────────────────────────────────────────────────────────

impl SinkBuilder for PgWriteDB {
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
    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SinkBuilder> {
        if let Some(b) = opts.on_missing_column { self.cfg.missing_column_behavior = b; }
        // Extract Postgres-specific options from connection-level config.
        if let Some(ref pg) = opts.postgres {
            if let Some(mc) = pg.max_connections {
                self.max_connections = mc;
            }
            if pg.staging_table {
                self.use_staging_table = true;
            }
            if !pg.init_sql.is_empty() {
                self.init_sql = pg.init_sql.clone();
            }
        }
        self
    }
    fn set_database_schema_config(&mut self, config: DatabaseSchemaConfig) { self.cfg.database_schema_config = Some(config); }
    fn set_ddl_schema(&mut self, schema: SchemaRef) { self.cfg.ddl_schema = Some(schema); }

    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<usize>> + Send + 'a>> {
        Box::pin(self.write_impl(batch))
    }

    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(self.flush_impl())
    }
}

// ── COPY BINARY serialisation ────────────────────────────────────────────────

/// Postgres epoch: 2000-01-01 00:00:00 UTC, expressed as microseconds since
/// Unix epoch (1970-01-01).
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

/// Postgres epoch for Date32: days between 1970-01-01 and 2000-01-01.
const PG_EPOCH_OFFSET_DAYS: i32 = 10_957;

enum BinaryCol<'a> {
    I8  (&'a Int8Array),
    I16 (&'a Int16Array),
    I32 (&'a Int32Array),
    I64 (&'a Int64Array),
    U8  (&'a UInt8Array),
    U16 (&'a UInt16Array),
    U32 (&'a UInt32Array),
    U64 (&'a UInt64Array),
    F32 (&'a Float32Array),
    F64 (&'a Float64Array),
    Bool(&'a BooleanArray),
    Str (&'a StringArray),
    LargeStr(&'a LargeStringArray),
    /// Arrow Binary / LargeBinary / FixedSizeBinary → Postgres BYTEA.
    Bin(&'a BinaryArray),
    LargeBin(&'a LargeBinaryArray),
    FixedBin(&'a FixedSizeBinaryArray),
    TsMicro(&'a TimestampMicrosecondArray),
    TsSecond(&'a TimestampSecondArray),
    TsMilli(&'a TimestampMillisecondArray),
    TsNano(&'a TimestampNanosecondArray),
    Date32(&'a Date32Array),
    Time64Micro(&'a Time64MicrosecondArray),
    Time64Nano(&'a Time64NanosecondArray),
    /// Arrow Interval → Postgres INTERVAL (16-byte binary: i64 µs + i32 days + i32 months).
    IntervalYM(&'a IntervalYearMonthArray),
    IntervalDT(&'a IntervalDayTimeArray),
    IntervalMDN(&'a IntervalMonthDayNanoArray),
    /// Arrow Duration → Postgres INTERVAL (only microseconds component).
    DurSec(&'a DurationSecondArray),
    DurMilli(&'a DurationMillisecondArray),
    DurMicro(&'a DurationMicrosecondArray),
    DurNano(&'a DurationNanosecondArray),
    /// Arrow Utf8 → Postgres JSONB binary (version byte 0x01 + JSON text).
    Jsonb(&'a StringArray),
    JsonbLarge(&'a LargeStringArray),
    /// Arrow Decimal128 → Postgres NUMERIC (binary format).
    Decimal128 { arr: &'a Decimal128Array, scale: i8 },
    /// Arrow Decimal128 → Postgres NUMERIC with rescaled precision/scale.
    Decimal128Rescaled { arr: &'a Decimal128Array, source_scale: i8, target_scale: i8 },
    /// Arrow Utf8 → Postgres UUID (16 raw bytes) — inline parse, no coercion array.
    UuidFromUtf8(&'a StringArray),
    /// Arrow LargeUtf8 → Postgres UUID (16 raw bytes).
    UuidFromLargeUtf8(&'a LargeStringArray),
    /// Arrow Int64 → Postgres INT4 (4-byte binary) — inline downcast.
    I64AsI32(&'a Int64Array),
    /// Arrow Int64 → Postgres INT2 (2-byte binary) — inline downcast.
    I64AsI16(&'a Int64Array),
    /// Arrow Int32 → Postgres INT2 (2-byte binary) — inline downcast.
    I32AsI16(&'a Int32Array),
    Generic(&'a dyn Array),
}

impl<'a> BinaryCol<'a> {
    fn from_array(arr: &'a dyn Array, effective_type: Option<&str>) -> Self {
        if let Some(tt) = effective_type {
            let lower = tt.to_lowercase();
            let base = lower.split('(').next().unwrap_or(&lower);

            // ── JSONB target ─────────────────────────────────────────────
            if base == "jsonb" {
                match arr.data_type() {
                    DataType::Utf8 => {
                        if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                            return Self::Jsonb(a);
                        }
                    }
                    DataType::LargeUtf8 => {
                        if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
                            return Self::JsonbLarge(a);
                        }
                    }
                    _ => {}
                }
            }

            // ── UUID target (inline parse — no coercion array) ───────────
            if base == "uuid" {
                match arr.data_type() {
                    DataType::Utf8 => {
                        if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                            return Self::UuidFromUtf8(a);
                        }
                    }
                    DataType::LargeUtf8 => {
                        if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
                            return Self::UuidFromLargeUtf8(a);
                        }
                    }
                    // FixedSizeBinary(16) → handled by from_array_inner as FixedBin
                    _ => {}
                }
            }

            // ── Integer downcasting (binary format size must match target) ─
            match (arr.data_type(), base) {
                (DataType::Int64, "integer" | "int" | "int4") => {
                    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                        return Self::I64AsI32(a);
                    }
                }
                (DataType::Int64, "smallint" | "int2") => {
                    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                        return Self::I64AsI16(a);
                    }
                }
                (DataType::Int32, "smallint" | "int2") => {
                    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
                        return Self::I32AsI16(a);
                    }
                }
                // ── Decimal rescaling ────────────────────────────────────
                (DataType::Decimal128(_, src_scale), "numeric" | "decimal") => {
                    if let Some(params) = lower.split('(').nth(1) {
                        let params = params.trim_end_matches(')');
                        let parts: Vec<&str> = params.split(',').collect();
                        if parts.len() == 2 {
                            if let (Ok(_), Ok(tgt_s)) = (
                                parts[0].trim().parse::<u8>(),
                                parts[1].trim().parse::<i8>(),
                            ) {
                                if *src_scale != tgt_s {
                                    if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
                                        return Self::Decimal128Rescaled {
                                            arr: a,
                                            source_scale: *src_scale,
                                            target_scale: tgt_s,
                                        };
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Self::from_array_inner(arr)
    }

    fn from_array_inner(arr: &'a dyn Array) -> Self {
        match arr.data_type() {
            DataType::Int8    => arr.as_any().downcast_ref::<Int8Array>().map_or(Self::Generic(arr), Self::I8),
            DataType::Int16   => arr.as_any().downcast_ref::<Int16Array>().map_or(Self::Generic(arr), Self::I16),
            DataType::Int32   => arr.as_any().downcast_ref::<Int32Array>().map_or(Self::Generic(arr), Self::I32),
            DataType::Int64   => arr.as_any().downcast_ref::<Int64Array>().map_or(Self::Generic(arr), Self::I64),
            DataType::UInt8   => arr.as_any().downcast_ref::<UInt8Array>().map_or(Self::Generic(arr), Self::U8),
            DataType::UInt16  => arr.as_any().downcast_ref::<UInt16Array>().map_or(Self::Generic(arr), Self::U16),
            DataType::UInt32  => arr.as_any().downcast_ref::<UInt32Array>().map_or(Self::Generic(arr), Self::U32),
            DataType::UInt64  => arr.as_any().downcast_ref::<UInt64Array>().map_or(Self::Generic(arr), Self::U64),
            DataType::Float32 => arr.as_any().downcast_ref::<Float32Array>().map_or(Self::Generic(arr), Self::F32),
            DataType::Float64 => arr.as_any().downcast_ref::<Float64Array>().map_or(Self::Generic(arr), Self::F64),
            DataType::Boolean => arr.as_any().downcast_ref::<BooleanArray>().map_or(Self::Generic(arr), Self::Bool),
            DataType::Utf8    => arr.as_any().downcast_ref::<StringArray>().map_or(Self::Generic(arr), Self::Str),
            DataType::LargeUtf8 => arr.as_any().downcast_ref::<LargeStringArray>().map_or(Self::Generic(arr), Self::LargeStr),
            DataType::Binary => arr.as_any().downcast_ref::<BinaryArray>().map_or(Self::Generic(arr), Self::Bin),
            DataType::LargeBinary => arr.as_any().downcast_ref::<LargeBinaryArray>().map_or(Self::Generic(arr), Self::LargeBin),
            DataType::FixedSizeBinary(_) => arr.as_any().downcast_ref::<FixedSizeBinaryArray>().map_or(Self::Generic(arr), Self::FixedBin),
            DataType::Timestamp(TimeUnit::Microsecond, _) =>
                arr.as_any().downcast_ref::<TimestampMicrosecondArray>().map_or(Self::Generic(arr), Self::TsMicro),
            DataType::Timestamp(TimeUnit::Second, _) =>
                arr.as_any().downcast_ref::<TimestampSecondArray>().map_or(Self::Generic(arr), Self::TsSecond),
            DataType::Timestamp(TimeUnit::Millisecond, _) =>
                arr.as_any().downcast_ref::<TimestampMillisecondArray>().map_or(Self::Generic(arr), Self::TsMilli),
            DataType::Timestamp(TimeUnit::Nanosecond, _) =>
                arr.as_any().downcast_ref::<TimestampNanosecondArray>().map_or(Self::Generic(arr), Self::TsNano),
            DataType::Date32  => arr.as_any().downcast_ref::<Date32Array>().map_or(Self::Generic(arr), Self::Date32),
            DataType::Time64(TimeUnit::Microsecond) =>
                arr.as_any().downcast_ref::<Time64MicrosecondArray>().map_or(Self::Generic(arr), Self::Time64Micro),
            DataType::Time64(TimeUnit::Nanosecond) =>
                arr.as_any().downcast_ref::<Time64NanosecondArray>().map_or(Self::Generic(arr), Self::Time64Nano),
            DataType::Interval(IntervalUnit::YearMonth) =>
                arr.as_any().downcast_ref::<IntervalYearMonthArray>().map_or(Self::Generic(arr), Self::IntervalYM),
            DataType::Interval(IntervalUnit::DayTime) =>
                arr.as_any().downcast_ref::<IntervalDayTimeArray>().map_or(Self::Generic(arr), Self::IntervalDT),
            DataType::Interval(IntervalUnit::MonthDayNano) =>
                arr.as_any().downcast_ref::<IntervalMonthDayNanoArray>().map_or(Self::Generic(arr), Self::IntervalMDN),
            DataType::Duration(TimeUnit::Second) =>
                arr.as_any().downcast_ref::<DurationSecondArray>().map_or(Self::Generic(arr), Self::DurSec),
            DataType::Duration(TimeUnit::Millisecond) =>
                arr.as_any().downcast_ref::<DurationMillisecondArray>().map_or(Self::Generic(arr), Self::DurMilli),
            DataType::Duration(TimeUnit::Microsecond) =>
                arr.as_any().downcast_ref::<DurationMicrosecondArray>().map_or(Self::Generic(arr), Self::DurMicro),
            DataType::Duration(TimeUnit::Nanosecond) =>
                arr.as_any().downcast_ref::<DurationNanosecondArray>().map_or(Self::Generic(arr), Self::DurNano),
            DataType::Decimal128(_precision, scale) => {
                let arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
                Self::Decimal128 { arr, scale: *scale }
            }
            _ => Self::Generic(arr),
        }
    }

    #[inline]
    fn is_null(&self, idx: usize) -> bool {
        match self {
            Self::I8(a)          => a.is_null(idx),
            Self::I16(a)         => a.is_null(idx),
            Self::I32(a)         => a.is_null(idx),
            Self::I64(a)         => a.is_null(idx),
            Self::U8(a)          => a.is_null(idx),
            Self::U16(a)         => a.is_null(idx),
            Self::U32(a)         => a.is_null(idx),
            Self::U64(a)         => a.is_null(idx),
            Self::F32(a)         => a.is_null(idx),
            Self::F64(a)         => a.is_null(idx),
            Self::Bool(a)        => a.is_null(idx),
            Self::Str(a)         => a.is_null(idx),
            Self::LargeStr(a)    => a.is_null(idx),
            Self::Jsonb(a)       => a.is_null(idx),
            Self::JsonbLarge(a)  => a.is_null(idx),
            Self::Bin(a)         => a.is_null(idx),
            Self::LargeBin(a)    => a.is_null(idx),
            Self::FixedBin(a)    => a.is_null(idx),
            Self::TsMicro(a)     => a.is_null(idx),
            Self::TsSecond(a)    => a.is_null(idx),
            Self::TsMilli(a)     => a.is_null(idx),
            Self::TsNano(a)      => a.is_null(idx),
            Self::Date32(a)      => a.is_null(idx),
            Self::Time64Micro(a) => a.is_null(idx),
            Self::Time64Nano(a)  => a.is_null(idx),
            Self::IntervalYM(a)  => a.is_null(idx),
            Self::IntervalDT(a)  => a.is_null(idx),
            Self::IntervalMDN(a) => a.is_null(idx),
            Self::DurSec(a)      => a.is_null(idx),
            Self::DurMilli(a)    => a.is_null(idx),
            Self::DurMicro(a)    => a.is_null(idx),
            Self::DurNano(a)     => a.is_null(idx),
            Self::Decimal128 { arr, .. } => arr.is_null(idx),
            Self::Decimal128Rescaled { arr, .. } => arr.is_null(idx),
            Self::UuidFromUtf8(a)    => a.is_null(idx),
            Self::UuidFromLargeUtf8(a) => a.is_null(idx),
            Self::I64AsI32(a)        => a.is_null(idx),
            Self::I64AsI16(a)        => a.is_null(idx),
            Self::I32AsI16(a)        => a.is_null(idx),
            Self::Generic(a)     => a.is_null(idx),
        }
    }

    #[inline]
    fn write_binary_value(&self, idx: usize, out: &mut Vec<u8>) {
        match self {
            Self::I8(a) => {
                out.extend_from_slice(&2_i32.to_be_bytes());
                out.extend_from_slice(&(a.value(idx) as i16).to_be_bytes());
            }
            Self::I16(a) => {
                out.extend_from_slice(&2_i32.to_be_bytes());
                out.extend_from_slice(&a.value(idx).to_be_bytes());
            }
            Self::I32(a) => {
                out.extend_from_slice(&4_i32.to_be_bytes());
                out.extend_from_slice(&a.value(idx).to_be_bytes());
            }
            Self::I64(a) => {
                out.extend_from_slice(&8_i32.to_be_bytes());
                out.extend_from_slice(&a.value(idx).to_be_bytes());
            }
            Self::U8(a) => {
                out.extend_from_slice(&2_i32.to_be_bytes());
                out.extend_from_slice(&(a.value(idx) as i16).to_be_bytes());
            }
            Self::U16(a) => {
                out.extend_from_slice(&4_i32.to_be_bytes());
                out.extend_from_slice(&(a.value(idx) as i32).to_be_bytes());
            }
            Self::U32(a) => {
                out.extend_from_slice(&8_i32.to_be_bytes());
                out.extend_from_slice(&(a.value(idx) as i64).to_be_bytes());
            }
            Self::U64(a) => {
                let s = a.value(idx).to_string();
                let bytes = s.as_bytes();
                out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                out.extend_from_slice(bytes);
            }
            Self::F32(a) => {
                out.extend_from_slice(&4_i32.to_be_bytes());
                out.extend_from_slice(&a.value(idx).to_be_bytes());
            }
            Self::F64(a) => {
                out.extend_from_slice(&8_i32.to_be_bytes());
                out.extend_from_slice(&a.value(idx).to_be_bytes());
            }
            Self::Bool(a) => {
                out.extend_from_slice(&1_i32.to_be_bytes());
                out.push(if a.value(idx) { 1 } else { 0 });
            }
            Self::Str(a) => {
                let bytes = a.value(idx).as_bytes();
                out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                out.extend_from_slice(bytes);
            }
            Self::LargeStr(a) => {
                let bytes = a.value(idx).as_bytes();
                out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                out.extend_from_slice(bytes);
            }
            // JSONB binary format: 1-byte version header (0x01) + JSON text.
            Self::Jsonb(a) => {
                let text = a.value(idx).as_bytes();
                out.extend_from_slice(&((text.len() + 1) as i32).to_be_bytes());
                out.push(0x01); // JSONB version byte
                out.extend_from_slice(text);
            }
            Self::JsonbLarge(a) => {
                let text = a.value(idx).as_bytes();
                out.extend_from_slice(&((text.len() + 1) as i32).to_be_bytes());
                out.push(0x01);
                out.extend_from_slice(text);
            }
            Self::Bin(a) => {
                let bytes = a.value(idx);
                out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                out.extend_from_slice(bytes);
            }
            Self::LargeBin(a) => {
                let bytes = a.value(idx);
                out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                out.extend_from_slice(bytes);
            }
            Self::FixedBin(a) => {
                let bytes = a.value(idx);
                out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                out.extend_from_slice(bytes);
            }
            Self::TsMicro(a) => {
                let arrow_us = a.value(idx);
                let pg_us = arrow_us - PG_EPOCH_OFFSET_US;
                out.extend_from_slice(&8_i32.to_be_bytes());
                out.extend_from_slice(&pg_us.to_be_bytes());
            }
            Self::TsSecond(a) => {
                let arrow_us = a.value(idx) * 1_000_000;
                let pg_us = arrow_us - PG_EPOCH_OFFSET_US;
                out.extend_from_slice(&8_i32.to_be_bytes());
                out.extend_from_slice(&pg_us.to_be_bytes());
            }
            Self::TsMilli(a) => {
                let arrow_us = a.value(idx) * 1_000;
                let pg_us = arrow_us - PG_EPOCH_OFFSET_US;
                out.extend_from_slice(&8_i32.to_be_bytes());
                out.extend_from_slice(&pg_us.to_be_bytes());
            }
            Self::TsNano(a) => {
                let arrow_us = a.value(idx) / 1_000;
                let pg_us = arrow_us - PG_EPOCH_OFFSET_US;
                out.extend_from_slice(&8_i32.to_be_bytes());
                out.extend_from_slice(&pg_us.to_be_bytes());
            }
            Self::Date32(a) => {
                let arrow_days = a.value(idx);
                let pg_days = arrow_days - PG_EPOCH_OFFSET_DAYS;
                out.extend_from_slice(&4_i32.to_be_bytes());
                out.extend_from_slice(&pg_days.to_be_bytes());
            }
            Self::Time64Micro(a) => {
                out.extend_from_slice(&8_i32.to_be_bytes());
                out.extend_from_slice(&a.value(idx).to_be_bytes());
            }
            Self::Time64Nano(a) => {
                // Postgres TIME is microseconds; convert ns → µs.
                let us = a.value(idx) / 1_000;
                out.extend_from_slice(&8_i32.to_be_bytes());
                out.extend_from_slice(&us.to_be_bytes());
            }
            // ── Interval → Postgres INTERVAL binary (16 bytes) ────────────
            // Layout: i64 microseconds, i32 days, i32 months
            Self::IntervalYM(a) => {
                let months = a.value(idx); // i32
                out.extend_from_slice(&16_i32.to_be_bytes());
                out.extend_from_slice(&0_i64.to_be_bytes());    // microseconds
                out.extend_from_slice(&0_i32.to_be_bytes());    // days
                out.extend_from_slice(&months.to_be_bytes());   // months
            }
            Self::IntervalDT(a) => {
                // Arrow IntervalDayTime struct: { days: i32, milliseconds: i32 }
                let val = a.value(idx);
                let days = val.days;
                let ms   = val.milliseconds;
                let us   = ms as i64 * 1_000;
                out.extend_from_slice(&16_i32.to_be_bytes());
                out.extend_from_slice(&us.to_be_bytes());       // microseconds
                out.extend_from_slice(&days.to_be_bytes());     // days
                out.extend_from_slice(&0_i32.to_be_bytes());    // months
            }
            Self::IntervalMDN(a) => {
                // Arrow IntervalMonthDayNano struct: { months: i32, days: i32, nanoseconds: i64 }
                let val = a.value(idx);
                let months = val.months;
                let days   = val.days;
                let ns     = val.nanoseconds;
                let us     = ns / 1_000;
                out.extend_from_slice(&16_i32.to_be_bytes());
                out.extend_from_slice(&us.to_be_bytes());       // microseconds
                out.extend_from_slice(&days.to_be_bytes());     // days
                out.extend_from_slice(&months.to_be_bytes());   // months
            }
            // ── Duration → Postgres INTERVAL binary (16 bytes) ───────────
            // Duration has only a time component — days and months are 0.
            Self::DurSec(a) => {
                let us = a.value(idx) * 1_000_000;
                out.extend_from_slice(&16_i32.to_be_bytes());
                out.extend_from_slice(&us.to_be_bytes());       // microseconds
                out.extend_from_slice(&0_i32.to_be_bytes());    // days
                out.extend_from_slice(&0_i32.to_be_bytes());    // months
            }
            Self::DurMilli(a) => {
                let us = a.value(idx) * 1_000;
                out.extend_from_slice(&16_i32.to_be_bytes());
                out.extend_from_slice(&us.to_be_bytes());
                out.extend_from_slice(&0_i32.to_be_bytes());
                out.extend_from_slice(&0_i32.to_be_bytes());
            }
            Self::DurMicro(a) => {
                out.extend_from_slice(&16_i32.to_be_bytes());
                out.extend_from_slice(&a.value(idx).to_be_bytes());
                out.extend_from_slice(&0_i32.to_be_bytes());
                out.extend_from_slice(&0_i32.to_be_bytes());
            }
            Self::DurNano(a) => {
                let us = a.value(idx) / 1_000;
                out.extend_from_slice(&16_i32.to_be_bytes());
                out.extend_from_slice(&us.to_be_bytes());
                out.extend_from_slice(&0_i32.to_be_bytes());
                out.extend_from_slice(&0_i32.to_be_bytes());
            }
            Self::Decimal128 { arr, scale } => {
                write_pg_numeric(arr.value(idx), *scale, out);
            }
            Self::Decimal128Rescaled { arr, source_scale, target_scale } => {
                let raw = arr.value(idx);
                let rescaled = rescale_decimal128_raw(raw, *source_scale, *target_scale);
                write_pg_numeric(rescaled, *target_scale, out);
            }
            // UUID from text: parse inline, write 16 raw bytes.
            // Eliminates the CoercionOperation::ParseUuid array allocation.
            Self::UuidFromUtf8(a) => {
                let text = a.value(idx);
                match uuid::Uuid::parse_str(text) {
                    Ok(u) => {
                        out.extend_from_slice(&16_i32.to_be_bytes());
                        out.extend_from_slice(u.as_bytes());
                    }
                    Err(_) => {
                        // Fallback: write as text, Postgres will reject but won't crash.
                        let bytes = text.as_bytes();
                        out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        out.extend_from_slice(bytes);
                    }
                }
            }
            Self::UuidFromLargeUtf8(a) => {
                let text = a.value(idx);
                match uuid::Uuid::parse_str(text) {
                    Ok(u) => {
                        out.extend_from_slice(&16_i32.to_be_bytes());
                        out.extend_from_slice(u.as_bytes());
                    }
                    Err(_) => {
                        let bytes = text.as_bytes();
                        out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        out.extend_from_slice(bytes);
                    }
                }
            }
            // Integer downcasting: write narrower binary format.
            Self::I64AsI32(a) => {
                out.extend_from_slice(&4_i32.to_be_bytes());
                out.extend_from_slice(&(a.value(idx) as i32).to_be_bytes());
            }
            Self::I64AsI16(a) => {
                out.extend_from_slice(&2_i32.to_be_bytes());
                out.extend_from_slice(&(a.value(idx) as i16).to_be_bytes());
            }
            Self::I32AsI16(a) => {
                out.extend_from_slice(&2_i32.to_be_bytes());
                out.extend_from_slice(&(a.value(idx) as i16).to_be_bytes());
            }
            Self::Generic(a) => {
                match arrow::util::display::array_value_to_string(*a, idx) {
                    Ok(s) => {
                        let bytes = s.as_bytes();
                        out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        out.extend_from_slice(bytes);
                    }
                    Err(_) => {
                        out.extend_from_slice(&0_i32.to_be_bytes());
                    }
                }
            }
        }
    }
}

// ── COPY BINARY building blocks ──────────────────────────────────────────────
//
// These three functions split the COPY BINARY format into composable parts:
//   1. `copy_binary_header()`  — 19-byte fixed header
//   2. `copy_binary_rows()`    — row data only (no header/trailer)
//   3. `copy_binary_trailer()` — 2-byte trailer (-1 as i16)
//
// This enables a **persistent COPY stream** where the header is sent once,
// rows are streamed per batch, and the trailer is sent on flush.

/// Returns the 19-byte COPY BINARY header.
#[inline]
pub fn copy_binary_header() -> Vec<u8> {
    let mut out = Vec::with_capacity(19);
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0_u32.to_be_bytes()); // flags
    out.extend_from_slice(&0_u32.to_be_bytes()); // header extension area length
    out
}

/// Returns the 2-byte COPY BINARY trailer.
#[inline]
pub fn copy_binary_trailer() -> Vec<u8> {
    (-1_i16).to_be_bytes().to_vec()
}

/// Serialises only the row data of a `RecordBatch` in COPY BINARY format.
///
/// Does **not** include the header or trailer — use `copy_binary_header()`
/// and `copy_binary_trailer()` to wrap the output for a complete stream.
/// `target_types` is an optional slice of target SQL types (lowercase) from
/// table introspection.  When provided, it is used to detect JSONB columns
/// even when `source_db_type` metadata doesn't indicate `jsonb` (e.g.,
/// cross-database flows from MSSQL → Postgres).  Pass `None` for the
/// common Postgres → Postgres case where `source_db_type` is sufficient.
pub fn copy_binary_rows(batch: &RecordBatch, target_types: Option<&[&str]>) -> Vec<u8> {
    let n_rows = batch.num_rows();
    let n_cols = batch.num_columns();

    // Estimate output size to avoid reallocations.
    let mut estimated: usize = 0;
    for ci in 0..n_cols {
        let col = batch.column(ci);
        estimated += match col.data_type() {
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>();
                arr.map_or(n_rows * 20, |a| n_rows * 4 + a.values().len())
            }
            DataType::LargeUtf8 => {
                let arr = col.as_any().downcast_ref::<LargeStringArray>();
                arr.map_or(n_rows * 20, |a| n_rows * 4 + a.values().len())
            }
            DataType::Binary => {
                let arr = col.as_any().downcast_ref::<BinaryArray>();
                arr.map_or(n_rows * 20, |a| n_rows * 4 + a.values().len())
            }
            DataType::LargeBinary => {
                let arr = col.as_any().downcast_ref::<LargeBinaryArray>();
                arr.map_or(n_rows * 20, |a| n_rows * 4 + a.values().len())
            }
            DataType::FixedSizeBinary(sz) => n_rows * (4 + *sz as usize),
            DataType::Int8 | DataType::UInt8 => n_rows * 6,
            DataType::Int16 | DataType::UInt16 => n_rows * 6,
            DataType::Int32 | DataType::UInt32 | DataType::Float32 => n_rows * 8,
            DataType::Int64 | DataType::UInt64 | DataType::Float64
                | DataType::Timestamp(..) | DataType::Time64(_) => n_rows * 12,
            DataType::Boolean => n_rows * 5,
            DataType::Date32 => n_rows * 8,
            // Interval/Duration: 4 (field_len) + 16 (µs + days + months) = 20
            DataType::Interval(_) | DataType::Duration(_) => n_rows * 20,
            // Decimal128: 4 (field_len) + 8 (header) + ~6*2 (avg digit groups) ≈ 24
            DataType::Decimal128(..) => n_rows * 24,
            _ => n_rows * 12,
        };
    }
    estimated += n_rows * (2 + n_cols * 4);
    let mut out: Vec<u8> = Vec::with_capacity(estimated);

    let schema = batch.schema();
    let cols: Vec<BinaryCol<'_>> = (0..n_cols)
        .map(|ci| {
            // Prefer target type (from introspection) over source_db_type metadata.
            // This ensures JSONB is correctly detected in cross-database flows.
            let effective_type = target_types
                .and_then(|tt| tt.get(ci).copied())
                .or_else(|| schema.field(ci).metadata()
                    .get("source_db_type").map(|s| s.as_str()));
            BinaryCol::from_array(batch.column(ci).as_ref(), effective_type)
        })
        .collect();

    let has_nulls: Vec<bool> = (0..n_cols)
        .map(|ci| batch.column(ci).null_count() > 0)
        .collect();

    let field_count = n_cols as i16;

    for row_idx in 0..n_rows {
        out.extend_from_slice(&field_count.to_be_bytes());
        for (col_idx, col) in cols.iter().enumerate() {
            if has_nulls[col_idx] && col.is_null(row_idx) {
                out.extend_from_slice(&(-1_i32).to_be_bytes());
            } else {
                col.write_binary_value(row_idx, &mut out);
            }
        }
    }

    out
}

/// Serialises a `RecordBatch` as a complete PostgreSQL COPY BINARY payload
/// (header + rows + trailer).
pub fn record_batch_to_copy_binary(batch: &RecordBatch) -> Vec<u8> {
    record_batch_to_copy_binary_with_types(batch, None)
}

/// Like `record_batch_to_copy_binary` but accepts target SQL types for JSONB detection.
pub fn record_batch_to_copy_binary_with_types(batch: &RecordBatch, target_types: Option<&[&str]>) -> Vec<u8> {
    let mut out = copy_binary_header();
    out.extend(copy_binary_rows(batch, target_types));
    out.extend(copy_binary_trailer());
    out
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Returns a fully-qualified, double-quoted Postgres table name.
pub fn pg_full_table(schema: &str, table: &str) -> String {
    format!(
        "\"{}\".\"{}\""  ,
        schema.replace('"', "\"\""),
        table.replace('"', "\"\""),
    )
}

/// Returns `true` if a Postgres target type is text-like and therefore does
/// not need an explicit `::type` cast when bound as a text parameter.
#[allow(dead_code)]
fn is_text_like_type(t: &str) -> bool {
    let lower = t.to_lowercase();
    lower.is_empty()
        || lower == "text"
        || lower == "varchar"
        || lower.starts_with("character varying")
        || lower.starts_with("varchar(")
        || lower == "char"
        || lower.starts_with("character(")
        || lower.starts_with("char(")
        || lower == "name"
        || lower == "citext"
}

/// Builds a parameterized INSERT with explicit `::type` casts on each
/// placeholder so that text-bound parameters are coerced to the target column
/// types.  Without this, Postgres rejects `$1` (TEXT) for integer/uuid/etc.
///
/// Generates SQL like:
///   INSERT INTO "t" ("a", "b") VALUES ($1::integer, $2::uuid), ($3::integer, $4::uuid)
///
/// Falls back to un-cast placeholders when `target_types` is `None` or when
/// the target type is text-like.
#[allow(dead_code)]
fn push_typed_values(
    qb: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>,
    rows: &[Vec<Option<String>>],
    target_types: Option<&[String]>,
) {
    qb.push(" VALUES ");
    for (row_idx, row) in rows.iter().enumerate() {
        if row_idx > 0 { qb.push(", "); }
        qb.push("(");
        for (col_idx, val) in row.iter().enumerate() {
            if col_idx > 0 { qb.push(", "); }
            qb.push_bind(val.clone());
            // Append ::type cast if we know the target type and it's not text-like.
            if let Some(types) = target_types {
                if let Some(t) = types.get(col_idx) {
                    if !is_text_like_type(t) {
                        qb.push(format!("::{t}"));
                    }
                }
            }
        }
        qb.push(")");
    }
}

/// Queries `server_version_num` and returns the major version.
async fn pg_major_version(pool: &PgPool) -> anyhow::Result<u32> {
    let row: (String,) = sqlx::query_as("SHOW server_version_num")
        .fetch_one(pool)
        .await?;
    let num: u32 = row.0.trim().parse().unwrap_or(0);
    Ok(num / 10_000)
}

/// Builds a CREATE TABLE DDL for a staging table.
///
/// When `target_columns` is provided, types like UUID and JSONB are preserved
/// from the target table.  Without this, `FixedSizeBinary(16)` would become
/// `BYTEA` (Postgres can't implicitly cast BYTEA→UUID) and `Utf8` would
/// become `TEXT` (TEXT→JSONB is implicit but slightly slower).
fn build_staging_ddl(
    table: &str,
    schema: &SchemaRef,
    target_columns: Option<&[potato_etl_common::db::common::alignment::TargetColumn]>,
) -> String {
    // Build a lookup map from column name → target SQL type.
    let target_map: std::collections::HashMap<&str, &str> = target_columns
        .map(|cols| cols.iter().map(|c| (c.name.as_str(), c.data_type.as_str())).collect())
        .unwrap_or_default();

    let mut cols_ddl = Vec::new();
    for field in schema.fields() {
        let sql_type = if let Some(&target_type) = target_map.get(field.name().as_str()) {
            let lower = target_type.to_lowercase();
            // Use target type directly for types that Postgres can't implicitly cast.
            if lower == "uuid" || lower == "jsonb" || lower == "json" {
                target_type.to_uppercase()
            } else {
                arrow_type_to_sql_dialect(field.data_type(), SqlDialect::Postgres)
            }
        } else {
            arrow_type_to_sql_dialect(field.data_type(), SqlDialect::Postgres)
        };
        cols_ddl.push(format!("\"{}\" {}", field.name(), sql_type));
    }
    format!(
        "CREATE TEMP TABLE {table} ({}) ON COMMIT DELETE ROWS",
        cols_ddl.join(", ")
    )
}

/// Writes an Arrow Decimal128 value as a Postgres NUMERIC in COPY BINARY format.
///
/// Postgres NUMERIC binary layout:
/// ```text
/// ndigits : i16  — number of base-10000 digit groups
/// weight  : i16  — exponent of the first digit group (0 = ones position)
/// sign    : u16  — 0x0000 = positive, 0x4000 = negative
/// dscale  : i16  — number of digits after the decimal point
/// digits  : [i16; ndigits] — base-10000 groups, MSB first
/// ```
///
/// The field-length prefix (required by COPY BINARY) is `8 + ndigits * 2`.
fn write_pg_numeric(value: i128, scale: i8, out: &mut Vec<u8>) {
    let dscale = scale.max(0) as u32;

    // Handle zero early.
    if value == 0 {
        let field_len = 8_i32;
        out.extend_from_slice(&field_len.to_be_bytes());
        out.extend_from_slice(&0_i16.to_be_bytes());  // ndigits
        out.extend_from_slice(&0_i16.to_be_bytes());  // weight
        out.extend_from_slice(&0x0000_u16.to_be_bytes()); // sign: positive
        out.extend_from_slice(&(dscale as i16).to_be_bytes()); // dscale
        return;
    }

    let negative = value < 0;
    let mut abs_val: u128 = value.unsigned_abs();

    // Pad the unscaled integer so that `dscale` aligns to a multiple of 4.
    // This makes the fractional part cleanly split into base-10000 groups.
    let padding = (4 - (dscale % 4)) % 4;
    for _ in 0..padding {
        abs_val *= 10;
    }
    let frac_groups = ((dscale + padding) / 4) as usize;

    // Decompose into base-10000 groups (LSB first).
    let mut groups: Vec<i16> = Vec::with_capacity(12);
    let mut remaining = abs_val;
    while remaining > 0 {
        groups.push((remaining % 10000) as i16);
        remaining /= 10000;
    }
    // Reverse to MSB-first order.
    groups.reverse();

    let total_groups = groups.len();
    // Weight = position of first group.  Integer groups are at positions
    // (total_groups - frac_groups - 1) .. 0, fractional at -1 .. -frac_groups.
    let weight = (total_groups as i16) - (frac_groups as i16) - 1;

    // Strip trailing zero groups (cosmetic — Postgres accepts them but they
    // waste bytes on the wire).
    while groups.last() == Some(&0) {
        groups.pop();
    }

    let ndigits = groups.len() as i16;
    let field_len = (8 + ndigits as i32 * 2) as i32;
    let sign: u16 = if negative { 0x4000 } else { 0x0000 };

    out.extend_from_slice(&field_len.to_be_bytes());
    out.extend_from_slice(&ndigits.to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&(dscale as i16).to_be_bytes());
    for &d in &groups {
        out.extend_from_slice(&d.to_be_bytes());
    }
}

// ── Native Arrow-typed parameterized INSERT ──────────────────────────────────
//
// Eliminates the string-roundtrip overhead of `record_batch_to_string_rows`:
//
//   Before: Arrow Int32 → String "42" → TEXT param → Postgres parses "42"::int
//   After:  Arrow Int32 → i32 → INT4 param → Postgres stores directly
//
// Each column value is bound with its native Rust type via `sqlx::Query::bind`.
// For types without native sqlx support (Decimal128), a string fallback with
// explicit `::type` cast is used.

/// Returns the SQL cast suffix needed for a `$N` placeholder, if any.
/// Most types bind natively (correct Postgres OID via sqlx), so they return "".
///
/// **DEPRECATED**: Superseded by [`TypedColumn::placeholder_suffix`].
#[allow(dead_code)]
fn native_placeholder_suffix(data_type: &DataType, target_type: Option<&str>) -> &'static str {
    // JSONB: we bind as String, Postgres needs the cast.
    if matches!(target_type, Some(t) if t.eq_ignore_ascii_case("jsonb")) {
        return "::jsonb";
    }
    match data_type {
        DataType::Decimal128(..) => "::numeric",
        _ => "",
    }
}

/// Binds a single Arrow column value at `row` to a sqlx query.
///
/// **DEPRECATED**: Superseded by [`TypedColumn::bind_value`] which avoids
/// per-cell downcast and target_type string comparison.
///
/// The returned query has one additional bind parameter.  The bound type
/// matches the Postgres column type (i32 for INT4, bool for BOOL, etc.),
/// eliminating text-to-type parsing overhead on the server.
#[allow(dead_code)]
fn bind_column_value<'q>(
    query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
    col: &dyn Array,
    row: usize,
    data_type: &DataType,
    target_type: Option<&str>,
) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
    // ── NULL fast path ───────────────────────────────────────────────────
    if col.is_null(row) {
        return match data_type {
            DataType::Int8                                   => query.bind(None::<i16>),
            DataType::Int16                                  => query.bind(None::<i16>),
            DataType::Int32                                  => query.bind(None::<i32>),
            DataType::Int64                                  => query.bind(None::<i64>),
            DataType::UInt8                                  => query.bind(None::<i16>),
            DataType::UInt16                                 => query.bind(None::<i32>),
            DataType::UInt32                                 => query.bind(None::<i64>),
            DataType::UInt64                                 => query.bind(None::<i64>),
            DataType::Float32                                => query.bind(None::<f32>),
            DataType::Float64                                => query.bind(None::<f64>),
            DataType::Boolean                                => query.bind(None::<bool>),
            DataType::Timestamp(..)                          => query.bind(None::<chrono::NaiveDateTime>),
            DataType::Date32                                 => query.bind(None::<chrono::NaiveDate>),
            DataType::Time64(..)                             => query.bind(None::<chrono::NaiveTime>),
            DataType::Interval(..) | DataType::Duration(..)  => query.bind(None::<PgInterval>),
            DataType::Binary | DataType::LargeBinary
                | DataType::FixedSizeBinary(..)              => {
                if matches!(target_type, Some(t) if t.eq_ignore_ascii_case("uuid")) {
                    query.bind(None::<uuid::Uuid>)
                } else {
                    query.bind(None::<Vec<u8>>)
                }
            }
            _                                                => query.bind(None::<String>),
        };
    }

    // ── UUID target type (from FixedSizeBinary or Utf8) ──────────────────
    if matches!(target_type, Some(t) if t.eq_ignore_ascii_case("uuid")) {
        match data_type {
            DataType::FixedSizeBinary(16) => {
                let arr = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                if let Ok(u) = uuid::Uuid::from_slice(arr.value(row)) {
                    return query.bind(u);
                }
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                if let Ok(u) = uuid::Uuid::parse_str(arr.value(row)) {
                    return query.bind(u);
                }
            }
            DataType::LargeUtf8 => {
                let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                if let Ok(u) = uuid::Uuid::parse_str(arr.value(row)) {
                    return query.bind(u);
                }
            }
            _ => {}
        }
        // fallback: bind as string, Postgres will cast
    }

    // ── JSONB target type (bind as String, cast via ::jsonb in SQL) ───────
    if matches!(target_type, Some(t) if t.eq_ignore_ascii_case("jsonb")) {
        return match data_type {
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                query.bind(arr.value(row).to_owned())
            }
            DataType::LargeUtf8 => {
                let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                query.bind(arr.value(row).to_owned())
            }
            _ => query.bind(None::<String>),
        };
    }

    // ── Standard type dispatch ───────────────────────────────────────────
    match data_type {
        // Integer types — Postgres has no unsigned, so widen as needed.
        DataType::Int8 => {
            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
            query.bind(arr.value(row) as i16)
        }
        DataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            query.bind(arr.value(row))
        }
        DataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            query.bind(arr.value(row))
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            query.bind(arr.value(row))
        }
        DataType::UInt8 => {
            let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
            query.bind(arr.value(row) as i16)
        }
        DataType::UInt16 => {
            let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
            query.bind(arr.value(row) as i32)
        }
        DataType::UInt32 => {
            let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            query.bind(arr.value(row) as i64)
        }
        DataType::UInt64 => {
            let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            query.bind(arr.value(row) as i64)
        }

        // Floating point
        DataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            query.bind(arr.value(row))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            query.bind(arr.value(row))
        }

        // Boolean
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            query.bind(arr.value(row))
        }

        // Strings
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            query.bind(arr.value(row).to_owned())
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            query.bind(arr.value(row).to_owned())
        }

        // Binary
        DataType::Binary => {
            let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
            query.bind(arr.value(row).to_vec())
        }
        DataType::LargeBinary => {
            let arr = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            query.bind(arr.value(row).to_vec())
        }
        DataType::FixedSizeBinary(_) => {
            let arr = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            query.bind(arr.value(row).to_vec())
        }

        // Timestamps → chrono types
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let arr = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            let v = arr.value(row);
            if tz.is_some() {
                query.bind(chrono::DateTime::from_timestamp(v, 0))
            } else {
                query.bind(chrono::DateTime::from_timestamp(v, 0).map(|dt| dt.naive_utc()))
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let arr = col.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
            let v = arr.value(row);
            if tz.is_some() {
                query.bind(chrono::DateTime::from_timestamp_millis(v))
            } else {
                query.bind(chrono::DateTime::from_timestamp_millis(v).map(|dt| dt.naive_utc()))
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let v = arr.value(row);
            if tz.is_some() {
                query.bind(chrono::DateTime::from_timestamp_micros(v))
            } else {
                query.bind(chrono::DateTime::from_timestamp_micros(v).map(|dt| dt.naive_utc()))
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let arr = col.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            let v = arr.value(row);
            let secs = v.div_euclid(1_000_000_000);
            let nsec = v.rem_euclid(1_000_000_000) as u32;
            if tz.is_some() {
                query.bind(chrono::DateTime::from_timestamp(secs, nsec))
            } else {
                query.bind(chrono::DateTime::from_timestamp(secs, nsec).map(|dt| dt.naive_utc()))
            }
        }

        // Date32 → NaiveDate (days since 1970-01-01)
        DataType::Date32 => {
            let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row);
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            query.bind(epoch + chrono::Duration::days(days as i64))
        }

        // Time64 → NaiveTime
        DataType::Time64(TimeUnit::Microsecond) => {
            let arr = col.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap();
            let v = arr.value(row);
            let secs = (v / 1_000_000) as u32;
            let nano = ((v % 1_000_000) * 1_000) as u32;
            query.bind(chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nano))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let arr = col.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
            let v = arr.value(row);
            let secs = (v / 1_000_000_000) as u32;
            let nano = (v % 1_000_000_000) as u32;
            query.bind(chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nano))
        }

        // Interval → PgInterval
        DataType::Interval(IntervalUnit::YearMonth) => {
            let arr = col.as_any().downcast_ref::<IntervalYearMonthArray>().unwrap();
            query.bind(PgInterval { months: arr.value(row), days: 0, microseconds: 0 })
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let arr = col.as_any().downcast_ref::<IntervalDayTimeArray>().unwrap();
            let v = arr.value(row);
            query.bind(PgInterval {
                months: 0,
                days: v.days,
                microseconds: v.milliseconds as i64 * 1_000,
            })
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let arr = col.as_any().downcast_ref::<IntervalMonthDayNanoArray>().unwrap();
            let v = arr.value(row);
            query.bind(PgInterval {
                months: v.months,
                days: v.days,
                microseconds: v.nanoseconds / 1_000,
            })
        }

        // Duration → PgInterval (microseconds only)
        DataType::Duration(TimeUnit::Second) => {
            let arr = col.as_any().downcast_ref::<DurationSecondArray>().unwrap();
            query.bind(PgInterval { months: 0, days: 0, microseconds: arr.value(row) * 1_000_000 })
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            let arr = col.as_any().downcast_ref::<DurationMillisecondArray>().unwrap();
            query.bind(PgInterval { months: 0, days: 0, microseconds: arr.value(row) * 1_000 })
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            let arr = col.as_any().downcast_ref::<DurationMicrosecondArray>().unwrap();
            query.bind(PgInterval { months: 0, days: 0, microseconds: arr.value(row) })
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            let arr = col.as_any().downcast_ref::<DurationNanosecondArray>().unwrap();
            query.bind(PgInterval { months: 0, days: 0, microseconds: arr.value(row) / 1_000 })
        }

        // Decimal128 → String (bound with ::numeric cast in SQL)
        DataType::Decimal128(_, scale) => {
            let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let raw = arr.value(row);
            let s = format_decimal128(raw, *scale);
            query.bind(s)
        }

        // Fallback: use Arrow's display format and bind as String.
        // The SQL placeholder has no cast, so this relies on Postgres
        // implicit TEXT coercion — same as the old approach.
        _ => {
            let arr = col;
            let formatter = arrow::util::display::ArrayFormatter::try_new(
                arr, &arrow::util::display::FormatOptions::default()
            );
            let s = match formatter {
                Ok(f) => f.value(row).to_string(),
                Err(_) => format!("{:?}", arr.data_type()),
            };
            query.bind(s)
        }
    }
}

/// Rescale a Decimal128 raw value from source_scale to target_scale.
///
/// If target has more decimal digits, multiply.  If fewer, divide (truncate).
#[inline]
fn rescale_decimal128_raw(raw: i128, source_scale: i8, target_scale: i8) -> i128 {
    let diff = target_scale as i32 - source_scale as i32;
    if diff == 0 {
        raw
    } else if diff > 0 {
        raw * 10_i128.pow(diff as u32)
    } else {
        raw / 10_i128.pow((-diff) as u32)
    }
}

/// Formats a Decimal128 raw value with the given scale as a decimal string.
fn format_decimal128(raw: i128, scale: i8) -> String {
    if scale <= 0 {
        // No fractional part; multiply by 10^(-scale).
        let factor: i128 = 10_i128.pow((-scale) as u32);
        return (raw * factor).to_string();
    }
    let scale = scale as u32;
    let negative = raw < 0;
    let abs = raw.unsigned_abs();
    let divisor = 10_u128.pow(scale);
    let int_part = abs / divisor;
    let frac_part = abs % divisor;
    let sign = if negative { "-" } else { "" };
    format!("{sign}{int_part}.{frac_part:0>width$}", width = scale as usize)
}

// ── Compiled INSERT plan ─────────────────────────────────────────────────────
//
// Phase 1 of the pipeline "compilation" optimisation.
//
// Instead of per-cell runtime dispatch (30-arm DataType match + target_type
// string comparison + `as_any().downcast_ref()` vtable call), we:
//
//   1. **Resolve** each column once per batch into a `TypedColumn` enum variant
//      that holds the pre-downcast concrete array reference.
//   2. **Bind** values via `TypedColumn::bind_value()` — a flat enum match with
//      no nested decisions, no downcasts, no string comparisons.
//   3. **Cache** the INSERT SQL template across same-sized chunks so that string
//      formatting overhead is paid only once per chunk size.
//
// Savings for a 10K-row × 20-column batch (200K cells):
//   - 199K `as_any().downcast_ref()` vtable calls eliminated (1 per column now)
//   - 200K `target_type` string comparisons eliminated
//   - SQL template built once instead of per-chunk

/// A pre-downcast, pre-resolved column encoder for the parameterized INSERT path.
///
/// Created once per column per batch via [`resolve_typed_columns`].  Each variant
/// holds a concrete typed reference to the Arrow array, so the per-row bind loop
/// avoids `as_any().downcast_ref()` calls and `DataType` / `target_type` matching.
enum TypedColumn<'a> {
    // ── Integers ─────────────────────────────────────────────────────────────
    I8AsI16(&'a Int8Array),         // Pg has no int1 → widen to INT2
    I16(&'a Int16Array),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
    U8AsI16(&'a UInt8Array),
    U16AsI32(&'a UInt16Array),
    U32AsI64(&'a UInt32Array),
    U64AsI64(&'a UInt64Array),

    // ── Floats ───────────────────────────────────────────────────────────────
    F32(&'a Float32Array),
    F64(&'a Float64Array),

    // ── Boolean ──────────────────────────────────────────────────────────────
    Bool(&'a BooleanArray),

    // ── Strings ──────────────────────────────────────────────────────────────
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),

    // ── Binary ───────────────────────────────────────────────────────────────
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    FixedBinary(&'a FixedSizeBinaryArray),

    // ── UUID target (pre-resolved — no per-cell target_type check) ───────────
    UuidFromFsb16(&'a FixedSizeBinaryArray),
    UuidFromUtf8(&'a StringArray),
    UuidFromLargeUtf8(&'a LargeStringArray),

    // ── JSONB target (cast suffix in SQL, bind as String) ────────────────────
    JsonbUtf8(&'a StringArray),
    JsonbLargeUtf8(&'a LargeStringArray),

    // ── Timestamps ───────────────────────────────────────────────────────────
    TsSecTz(&'a TimestampSecondArray),
    TsSecNaive(&'a TimestampSecondArray),
    TsMilliTz(&'a TimestampMillisecondArray),
    TsMilliNaive(&'a TimestampMillisecondArray),
    TsMicroTz(&'a TimestampMicrosecondArray),
    TsMicroNaive(&'a TimestampMicrosecondArray),
    TsNanoTz(&'a TimestampNanosecondArray),
    TsNanoNaive(&'a TimestampNanosecondArray),

    // ── Date / Time ──────────────────────────────────────────────────────────
    Date32Val(&'a Date32Array),
    Time64Micro(&'a Time64MicrosecondArray),
    Time64Nano(&'a Time64NanosecondArray),

    // ── Intervals → PgInterval ───────────────────────────────────────────────
    IntervalYM(&'a IntervalYearMonthArray),
    IntervalDT(&'a IntervalDayTimeArray),
    IntervalMDN(&'a IntervalMonthDayNanoArray),

    // ── Durations → PgInterval ───────────────────────────────────────────────
    DurSec(&'a DurationSecondArray),
    DurMilli(&'a DurationMillisecondArray),
    DurMicro(&'a DurationMicrosecondArray),
    DurNano(&'a DurationNanosecondArray),

    // ── Decimal128 → String + ::numeric cast ─────────────────────────────────
    Decimal128Val { arr: &'a Decimal128Array, scale: i8 },

    // ── Fallback: Arrow display formatter ────────────────────────────────────
    Fallback(&'a dyn Array),
}

impl<'a> TypedColumn<'a> {
    /// SQL placeholder suffix for this column (e.g. `"::jsonb"`, `"::numeric"`, or `""`).
    fn placeholder_suffix(&self) -> &'static str {
        match self {
            Self::JsonbUtf8(_) | Self::JsonbLargeUtf8(_) => "::jsonb",
            Self::Decimal128Val { .. } => "::numeric",
            _ => "",
        }
    }

    /// Bind the value at `row` to the sqlx query.  NULL handling is inlined
    /// per variant — no separate NULL dispatch match.
    #[inline]
    fn bind_value<'q>(
        &self,
        query: sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments>,
        row: usize,
    ) -> sqlx::query::Query<'q, Postgres, sqlx::postgres::PgArguments> {
        match self {
            // ── Integers ─────────────────────────────────────────────────────
            Self::I8AsI16(arr) => {
                if arr.is_null(row) { query.bind(None::<i16>) }
                else { query.bind(arr.value(row) as i16) }
            }
            Self::I16(arr) => {
                if arr.is_null(row) { query.bind(None::<i16>) }
                else { query.bind(arr.value(row)) }
            }
            Self::I32(arr) => {
                if arr.is_null(row) { query.bind(None::<i32>) }
                else { query.bind(arr.value(row)) }
            }
            Self::I64(arr) => {
                if arr.is_null(row) { query.bind(None::<i64>) }
                else { query.bind(arr.value(row)) }
            }
            Self::U8AsI16(arr) => {
                if arr.is_null(row) { query.bind(None::<i16>) }
                else { query.bind(arr.value(row) as i16) }
            }
            Self::U16AsI32(arr) => {
                if arr.is_null(row) { query.bind(None::<i32>) }
                else { query.bind(arr.value(row) as i32) }
            }
            Self::U32AsI64(arr) => {
                if arr.is_null(row) { query.bind(None::<i64>) }
                else { query.bind(arr.value(row) as i64) }
            }
            Self::U64AsI64(arr) => {
                if arr.is_null(row) { query.bind(None::<i64>) }
                else { query.bind(arr.value(row) as i64) }
            }

            // ── Floats ───────────────────────────────────────────────────────
            Self::F32(arr) => {
                if arr.is_null(row) { query.bind(None::<f32>) }
                else { query.bind(arr.value(row)) }
            }
            Self::F64(arr) => {
                if arr.is_null(row) { query.bind(None::<f64>) }
                else { query.bind(arr.value(row)) }
            }

            // ── Boolean ──────────────────────────────────────────────────────
            Self::Bool(arr) => {
                if arr.is_null(row) { query.bind(None::<bool>) }
                else { query.bind(arr.value(row)) }
            }

            // ── Strings ──────────────────────────────────────────────────────
            Self::Utf8(arr) | Self::JsonbUtf8(arr) => {
                if arr.is_null(row) { query.bind(None::<String>) }
                else { query.bind(arr.value(row).to_owned()) }
            }
            Self::LargeUtf8(arr) | Self::JsonbLargeUtf8(arr) => {
                if arr.is_null(row) { query.bind(None::<String>) }
                else { query.bind(arr.value(row).to_owned()) }
            }

            // ── Binary ───────────────────────────────────────────────────────
            Self::Binary(arr) => {
                if arr.is_null(row) { query.bind(None::<Vec<u8>>) }
                else { query.bind(arr.value(row).to_vec()) }
            }
            Self::LargeBinary(arr) => {
                if arr.is_null(row) { query.bind(None::<Vec<u8>>) }
                else { query.bind(arr.value(row).to_vec()) }
            }
            Self::FixedBinary(arr) => {
                if arr.is_null(row) { query.bind(None::<Vec<u8>>) }
                else { query.bind(arr.value(row).to_vec()) }
            }

            // ── UUID (pre-resolved target — no per-cell string compare) ──────
            Self::UuidFromFsb16(arr) => {
                if arr.is_null(row) { query.bind(None::<uuid::Uuid>) }
                else {
                    query.bind(
                        uuid::Uuid::from_slice(arr.value(row)).unwrap_or_default()
                    )
                }
            }
            Self::UuidFromUtf8(arr) => {
                if arr.is_null(row) { query.bind(None::<uuid::Uuid>) }
                else {
                    match uuid::Uuid::parse_str(arr.value(row)) {
                        Ok(u) => query.bind(u),
                        Err(_) => query.bind(arr.value(row).to_owned()),
                    }
                }
            }
            Self::UuidFromLargeUtf8(arr) => {
                if arr.is_null(row) { query.bind(None::<uuid::Uuid>) }
                else {
                    match uuid::Uuid::parse_str(arr.value(row)) {
                        Ok(u) => query.bind(u),
                        Err(_) => query.bind(arr.value(row).to_owned()),
                    }
                }
            }

            // ── Timestamps ───────────────────────────────────────────────────
            Self::TsSecTz(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::DateTime<chrono::Utc>>) }
                else { query.bind(chrono::DateTime::from_timestamp(arr.value(row), 0)) }
            }
            Self::TsSecNaive(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::NaiveDateTime>) }
                else { query.bind(chrono::DateTime::from_timestamp(arr.value(row), 0).map(|dt| dt.naive_utc())) }
            }
            Self::TsMilliTz(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::DateTime<chrono::Utc>>) }
                else { query.bind(chrono::DateTime::from_timestamp_millis(arr.value(row))) }
            }
            Self::TsMilliNaive(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::NaiveDateTime>) }
                else { query.bind(chrono::DateTime::from_timestamp_millis(arr.value(row)).map(|dt| dt.naive_utc())) }
            }
            Self::TsMicroTz(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::DateTime<chrono::Utc>>) }
                else { query.bind(chrono::DateTime::from_timestamp_micros(arr.value(row))) }
            }
            Self::TsMicroNaive(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::NaiveDateTime>) }
                else { query.bind(chrono::DateTime::from_timestamp_micros(arr.value(row)).map(|dt| dt.naive_utc())) }
            }
            Self::TsNanoTz(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::DateTime<chrono::Utc>>) }
                else {
                    let v = arr.value(row);
                    let secs = v.div_euclid(1_000_000_000);
                    let nsec = v.rem_euclid(1_000_000_000) as u32;
                    query.bind(chrono::DateTime::from_timestamp(secs, nsec))
                }
            }
            Self::TsNanoNaive(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::NaiveDateTime>) }
                else {
                    let v = arr.value(row);
                    let secs = v.div_euclid(1_000_000_000);
                    let nsec = v.rem_euclid(1_000_000_000) as u32;
                    query.bind(chrono::DateTime::from_timestamp(secs, nsec).map(|dt| dt.naive_utc()))
                }
            }

            // ── Date / Time ──────────────────────────────────────────────────
            Self::Date32Val(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::NaiveDate>) }
                else {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    query.bind(epoch + chrono::Duration::days(arr.value(row) as i64))
                }
            }
            Self::Time64Micro(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::NaiveTime>) }
                else {
                    let v = arr.value(row);
                    query.bind(chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                        (v / 1_000_000) as u32,
                        ((v % 1_000_000) * 1_000) as u32,
                    ))
                }
            }
            Self::Time64Nano(arr) => {
                if arr.is_null(row) { query.bind(None::<chrono::NaiveTime>) }
                else {
                    let v = arr.value(row);
                    query.bind(chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                        (v / 1_000_000_000) as u32,
                        (v % 1_000_000_000) as u32,
                    ))
                }
            }

            // ── Intervals ────────────────────────────────────────────────────
            Self::IntervalYM(arr) => {
                if arr.is_null(row) { query.bind(None::<PgInterval>) }
                else { query.bind(PgInterval { months: arr.value(row), days: 0, microseconds: 0 }) }
            }
            Self::IntervalDT(arr) => {
                if arr.is_null(row) { query.bind(None::<PgInterval>) }
                else {
                    let v = arr.value(row);
                    query.bind(PgInterval { months: 0, days: v.days, microseconds: v.milliseconds as i64 * 1_000 })
                }
            }
            Self::IntervalMDN(arr) => {
                if arr.is_null(row) { query.bind(None::<PgInterval>) }
                else {
                    let v = arr.value(row);
                    query.bind(PgInterval { months: v.months, days: v.days, microseconds: v.nanoseconds / 1_000 })
                }
            }

            // ── Durations ────────────────────────────────────────────────────
            Self::DurSec(arr) => {
                if arr.is_null(row) { query.bind(None::<PgInterval>) }
                else { query.bind(PgInterval { months: 0, days: 0, microseconds: arr.value(row) * 1_000_000 }) }
            }
            Self::DurMilli(arr) => {
                if arr.is_null(row) { query.bind(None::<PgInterval>) }
                else { query.bind(PgInterval { months: 0, days: 0, microseconds: arr.value(row) * 1_000 }) }
            }
            Self::DurMicro(arr) => {
                if arr.is_null(row) { query.bind(None::<PgInterval>) }
                else { query.bind(PgInterval { months: 0, days: 0, microseconds: arr.value(row) }) }
            }
            Self::DurNano(arr) => {
                if arr.is_null(row) { query.bind(None::<PgInterval>) }
                else { query.bind(PgInterval { months: 0, days: 0, microseconds: arr.value(row) / 1_000 }) }
            }

            // ── Decimal128 ───────────────────────────────────────────────────
            Self::Decimal128Val { arr, scale } => {
                if arr.is_null(row) { query.bind(None::<String>) }
                else { query.bind(format_decimal128(arr.value(row), *scale)) }
            }

            // ── Fallback ─────────────────────────────────────────────────────
            Self::Fallback(arr) => {
                if arr.is_null(row) { return query.bind(None::<String>); }
                let formatter = arrow::util::display::ArrayFormatter::try_new(
                    *arr, &arrow::util::display::FormatOptions::default(),
                );
                let s = match formatter {
                    Ok(f) => f.value(row).to_string(),
                    Err(_) => format!("{:?}", arr.data_type()),
                };
                query.bind(s)
            }
        }
    }
}

/// Resolve all columns in a batch to pre-downcast [`TypedColumn`] variants.
///
/// This is the "compile" step: each column is inspected once for its Arrow
/// `DataType` and optional target SQL type.  The resulting `Vec<TypedColumn>`
/// is then used in the bind loop with zero per-cell overhead for type
/// resolution.
fn resolve_typed_columns<'a>(
    batch: &'a RecordBatch,
    target_types: Option<&[String]>,
) -> Vec<TypedColumn<'a>> {
    let schema = batch.schema();
    (0..schema.fields().len())
        .map(|col_idx| {
            let col = batch.column(col_idx);
            let dt = schema.field(col_idx).data_type();
            let tt = target_types.and_then(|t| t.get(col_idx)).map(|s| s.as_str());
            resolve_single_column(col.as_ref(), dt, tt)
        })
        .collect()
}

/// Does the target SQL type indicate a timezone-aware timestamp?
///
/// Returns `Some(true)` for TIMESTAMPTZ / TIMESTAMP WITH TIME ZONE,
/// `Some(false)` for TIMESTAMP / TIMESTAMP WITHOUT TIME ZONE,
/// `None` if the target type is not a timestamp (let caller decide).
#[inline]
fn target_wants_tz(target_type: Option<&str>) -> Option<bool> {
    target_type.and_then(|tt| {
        // target_sql_types is already lowercased in align_batch().
        if tt.starts_with("timestamptz") || tt.contains("with time zone") {
            Some(true)
        } else if tt.starts_with("timestamp") {
            // "timestamp" or "timestamp without time zone" → no tz
            Some(false)
        } else {
            None
        }
    })
}

/// Resolve a single column to its [`TypedColumn`] variant.
fn resolve_single_column<'a>(
    col: &'a dyn Array,
    data_type: &DataType,
    target_type: Option<&str>,
) -> TypedColumn<'a> {
    // ── UUID target? (pre-resolve to avoid per-cell string compare) ───────
    if matches!(target_type, Some(t) if t.eq_ignore_ascii_case("uuid")) {
        match data_type {
            DataType::FixedSizeBinary(16) => {
                if let Some(a) = col.as_any().downcast_ref::<FixedSizeBinaryArray>() {
                    return TypedColumn::UuidFromFsb16(a);
                }
            }
            DataType::Utf8 => {
                if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                    return TypedColumn::UuidFromUtf8(a);
                }
            }
            DataType::LargeUtf8 => {
                if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
                    return TypedColumn::UuidFromLargeUtf8(a);
                }
            }
            _ => {}
        }
    }

    // ── JSONB target? ────────────────────────────────────────────────────
    if matches!(target_type, Some(t) if t.eq_ignore_ascii_case("jsonb")) {
        match data_type {
            DataType::Utf8 => {
                if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                    return TypedColumn::JsonbUtf8(a);
                }
            }
            DataType::LargeUtf8 => {
                if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
                    return TypedColumn::JsonbLargeUtf8(a);
                }
            }
            _ => {}
        }
    }

    // ── Standard type dispatch (one downcast per column, not per cell) ────
    match data_type {
        DataType::Int8    => col.as_any().downcast_ref::<Int8Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::I8AsI16),
        DataType::Int16   => col.as_any().downcast_ref::<Int16Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::I16),
        DataType::Int32   => col.as_any().downcast_ref::<Int32Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::I32),
        DataType::Int64   => col.as_any().downcast_ref::<Int64Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::I64),
        DataType::UInt8   => col.as_any().downcast_ref::<UInt8Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::U8AsI16),
        DataType::UInt16  => col.as_any().downcast_ref::<UInt16Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::U16AsI32),
        DataType::UInt32  => col.as_any().downcast_ref::<UInt32Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::U32AsI64),
        DataType::UInt64  => col.as_any().downcast_ref::<UInt64Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::U64AsI64),
        DataType::Float32 => col.as_any().downcast_ref::<Float32Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::F32),
        DataType::Float64 => col.as_any().downcast_ref::<Float64Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::F64),
        DataType::Boolean => col.as_any().downcast_ref::<BooleanArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::Bool),
        DataType::Utf8    => col.as_any().downcast_ref::<StringArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::Utf8),
        DataType::LargeUtf8 => col.as_any().downcast_ref::<LargeStringArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::LargeUtf8),
        DataType::Binary  => col.as_any().downcast_ref::<BinaryArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::Binary),
        DataType::LargeBinary => col.as_any().downcast_ref::<LargeBinaryArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::LargeBinary),
        DataType::FixedSizeBinary(_) => col.as_any().downcast_ref::<FixedSizeBinaryArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::FixedBinary),

        // ── Timestamps: target-aware Tz/Naive resolution ───────────────────
        // When a target SQL type is known, it overrides the Arrow field's
        // timezone metadata.  This allows skipping `coerce_batch()` entirely
        // for the parameterized INSERT path — the TypedColumn variant handles
        // the tz strip/add at bind time.
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let use_tz = target_wants_tz(target_type).unwrap_or_else(|| tz.is_some());
            match col.as_any().downcast_ref::<TimestampSecondArray>() {
                Some(a) if use_tz => TypedColumn::TsSecTz(a),
                Some(a)           => TypedColumn::TsSecNaive(a),
                None              => TypedColumn::Fallback(col),
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let use_tz = target_wants_tz(target_type).unwrap_or_else(|| tz.is_some());
            match col.as_any().downcast_ref::<TimestampMillisecondArray>() {
                Some(a) if use_tz => TypedColumn::TsMilliTz(a),
                Some(a)           => TypedColumn::TsMilliNaive(a),
                None              => TypedColumn::Fallback(col),
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let use_tz = target_wants_tz(target_type).unwrap_or_else(|| tz.is_some());
            match col.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                Some(a) if use_tz => TypedColumn::TsMicroTz(a),
                Some(a)           => TypedColumn::TsMicroNaive(a),
                None              => TypedColumn::Fallback(col),
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let use_tz = target_wants_tz(target_type).unwrap_or_else(|| tz.is_some());
            match col.as_any().downcast_ref::<TimestampNanosecondArray>() {
                Some(a) if use_tz => TypedColumn::TsNanoTz(a),
                Some(a)           => TypedColumn::TsNanoNaive(a),
                None              => TypedColumn::Fallback(col),
            }
        }

        DataType::Date32 => col.as_any().downcast_ref::<Date32Array>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::Date32Val),
        DataType::Time64(TimeUnit::Microsecond) => col.as_any().downcast_ref::<Time64MicrosecondArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::Time64Micro),
        DataType::Time64(TimeUnit::Nanosecond) => col.as_any().downcast_ref::<Time64NanosecondArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::Time64Nano),

        DataType::Interval(IntervalUnit::YearMonth) => col.as_any().downcast_ref::<IntervalYearMonthArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::IntervalYM),
        DataType::Interval(IntervalUnit::DayTime) => col.as_any().downcast_ref::<IntervalDayTimeArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::IntervalDT),
        DataType::Interval(IntervalUnit::MonthDayNano) => col.as_any().downcast_ref::<IntervalMonthDayNanoArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::IntervalMDN),

        DataType::Duration(TimeUnit::Second) => col.as_any().downcast_ref::<DurationSecondArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::DurSec),
        DataType::Duration(TimeUnit::Millisecond) => col.as_any().downcast_ref::<DurationMillisecondArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::DurMilli),
        DataType::Duration(TimeUnit::Microsecond) => col.as_any().downcast_ref::<DurationMicrosecondArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::DurMicro),
        DataType::Duration(TimeUnit::Nanosecond) => col.as_any().downcast_ref::<DurationNanosecondArray>().map_or_else(
            || TypedColumn::Fallback(col), TypedColumn::DurNano),

        DataType::Decimal128(_precision, scale) => {
            match col.as_any().downcast_ref::<Decimal128Array>() {
                Some(arr) => TypedColumn::Decimal128Val { arr, scale: *scale },
                None      => TypedColumn::Fallback(col),
            }
        }

        _ => TypedColumn::Fallback(col),
    }
}

/// Build the INSERT SQL string with `$N` placeholders and per-column cast
/// suffixes derived from pre-resolved [`TypedColumn`] variants.
fn build_insert_sql_planned(
    full_table: &str,
    cols_sql: &str,
    suffix: &str,
    typed_cols: &[TypedColumn<'_>],
    num_rows: usize,
) -> String {
    let num_cols = typed_cols.len();
    let suffixes: Vec<&str> = typed_cols.iter().map(|tc| tc.placeholder_suffix()).collect();

    let row_est = num_cols * 6;
    let mut sql = String::with_capacity(
        60 + full_table.len() + cols_sql.len() + num_rows * row_est + suffix.len(),
    );

    sql.push_str("INSERT INTO ");
    sql.push_str(full_table);
    sql.push_str(" (");
    sql.push_str(cols_sql);
    sql.push_str(") VALUES ");

    let mut param_idx: usize = 0;
    for row_offset in 0..num_rows {
        if row_offset > 0 { sql.push_str(", "); }
        sql.push('(');
        for col_idx in 0..num_cols {
            if col_idx > 0 { sql.push_str(", "); }
            param_idx += 1;
            sql.push('$');
            itoa_push(&mut sql, param_idx);
            sql.push_str(suffixes[col_idx]);
        }
        sql.push(')');
    }
    sql.push_str(suffix);
    sql
}

/// Append a usize as decimal digits without allocating.
#[inline]
fn itoa_push(s: &mut String, mut n: usize) {
    if n < 10 {
        s.push((b'0' + n as u8) as char);
        return;
    }
    let mut buf = [0u8; 20];
    let mut pos = 20;
    while n > 0 {
        pos -= 1;
        buf[pos] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    // SAFETY: digits are always valid ASCII/UTF-8.
    s.push_str(unsafe { std::str::from_utf8_unchecked(&buf[pos..]) });
}

/// Execute a planned INSERT for a chunk of rows using pre-resolved columns.
///
/// This replaces `execute_native_insert` with a "compiled" approach:
/// - No per-cell `DataType` match (decided at resolve time)
/// - No per-cell `as_any().downcast_ref()` (done once per column)
/// - No per-cell target_type string comparison (resolved to enum variant)
async fn execute_planned_insert(
    typed_cols: &[TypedColumn<'_>],
    row_range: std::ops::Range<usize>,
    sql: &str,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    let num_rows = row_range.len();
    if num_rows == 0 || typed_cols.is_empty() { return Ok(()); }

    let mut query = sqlx::query(sql);
    for row in row_range {
        for tc in typed_cols {
            query = tc.bind_value(query, row);
        }
    }
    query.execute(&mut **txn).await?;
    Ok(())
}

/// Executes a native-typed parameterized INSERT for a chunk of rows.
///
/// **DEPRECATED**: Prefer [`execute_planned_insert`] with pre-resolved
/// [`TypedColumn`] columns.  This function is retained as a fallback reference
/// but is no longer called from the main write path.
///
/// Builds SQL with `$N` placeholders (plus cast suffixes where needed) and
/// binds each Arrow column value with its native Rust type, eliminating the
/// `String` roundtrip entirely for int/float/bool/timestamp/date/uuid columns.
#[allow(dead_code)]
async fn execute_native_insert(
    batch: &RecordBatch,
    row_range: std::ops::Range<usize>,
    full_table: &str,
    cols_sql: &str,
    suffix: &str,
    target_types: Option<&[String]>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> anyhow::Result<()> {
    let schema = batch.schema();
    let num_cols = schema.fields().len();
    let num_rows = row_range.len();
    if num_rows == 0 || num_cols == 0 { return Ok(()); }

    // ── Build SQL with $N placeholders ───────────────────────────────────
    // Pre-compute per-column cast suffixes.
    let suffixes: Vec<&str> = (0..num_cols).map(|col_idx| {
        let dt = schema.field(col_idx).data_type();
        let tt = target_types.and_then(|t| t.get(col_idx)).map(|s| s.as_str());
        native_placeholder_suffix(dt, tt)
    }).collect();

    let mut sql = format!("INSERT INTO {full_table} ({cols_sql}) VALUES ");
    let mut param_idx: usize = 0;
    for row_offset in 0..num_rows {
        if row_offset > 0 { sql.push_str(", "); }
        sql.push('(');
        for col_idx in 0..num_cols {
            if col_idx > 0 { sql.push_str(", "); }
            param_idx += 1;
            sql.push('$');
            sql.push_str(&param_idx.to_string());
            sql.push_str(suffixes[col_idx]);
        }
        sql.push(')');
    }
    sql.push_str(suffix);

    // ── Bind all values ──────────────────────────────────────────────────
    let mut query = sqlx::query(&sql);
    for row in row_range {
        for col_idx in 0..num_cols {
            let col = batch.column(col_idx);
            let dt = schema.field(col_idx).data_type();
            let tt = target_types.and_then(|t| t.get(col_idx)).map(|s| s.as_str());
            query = bind_column_value(query, col.as_ref(), row, dt, tt);
        }
    }

    query.execute(&mut **txn).await?;
    Ok(())
}

/// Extracts PK column values as strings from a RecordBatch.
/// Only converts the PK columns, not the entire batch — far cheaper than
/// `record_batch_to_string_rows` for merge_delete PK tracking.
fn extract_pk_strings(
    batch: &RecordBatch,
    pk_cols: &[String],
    schema: &SchemaRef,
) -> Vec<Vec<Option<String>>> {
    let pk_indices: Vec<usize> = pk_cols.iter()
        .filter_map(|pk| schema.index_of(pk).ok())
        .collect();
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut pk_vals = Vec::with_capacity(pk_indices.len());
        for &col_idx in &pk_indices {
            let col = batch.column(col_idx);
            if col.is_null(row) {
                pk_vals.push(None);
            } else {
                let formatter = arrow::util::display::ArrayFormatter::try_new(
                    col.as_ref(),
                    &arrow::util::display::FormatOptions::default(),
                );
                let s = match formatter {
                    Ok(f) => Some(f.value(row).to_string()),
                    Err(_) => None,
                };
                pk_vals.push(s);
            }
        }
        rows.push(pk_vals);
    }
    rows
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::collections::HashMap;

    // ── JSONB COPY BINARY tests ──────────────────────────────────────────

    #[test]
    fn test_jsonb_copy_binary_has_version_byte() {
        let mut meta = HashMap::new();
        meta.insert("source_db_type".to_string(), "jsonb".to_string());
        let field = arrow::datatypes::Field::new("data", DataType::Utf8, true)
            .with_metadata(meta);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let arr = Arc::new(StringArray::from(vec![Some("{\"key\":\"value\"}")]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let out = copy_binary_rows(&batch, None);
        // Row format: 2 bytes field count + 4 bytes field len + 1 version + JSON text
        let field_count = i16::from_be_bytes([out[0], out[1]]);
        assert_eq!(field_count, 1);
        let field_len = i32::from_be_bytes([out[2], out[3], out[4], out[5]]);
        let json_text = b"{\"key\":\"value\"}";
        assert_eq!(field_len, (json_text.len() + 1) as i32);
        assert_eq!(out[6], 0x01); // JSONB version byte
        assert_eq!(&out[7..7 + json_text.len()], json_text);
    }

    #[test]
    fn test_plain_text_copy_binary_no_version_byte() {
        let field = arrow::datatypes::Field::new("name", DataType::Utf8, true);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let arr = Arc::new(StringArray::from(vec![Some("hello")]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let out = copy_binary_rows(&batch, None);
        let field_len = i32::from_be_bytes([out[2], out[3], out[4], out[5]]);
        assert_eq!(field_len, 5);
        assert_eq!(&out[6..11], b"hello");
    }

    #[test]
    fn test_jsonb_via_target_types() {
        // Cross-database: source_db_type absent, target type is jsonb.
        let field = arrow::datatypes::Field::new("data", DataType::Utf8, true);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let arr = Arc::new(StringArray::from(vec![Some("{\"a\":1}")]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let target_types: Vec<&str> = vec!["jsonb"];
        let out = copy_binary_rows(&batch, Some(&target_types));
        let field_len = i32::from_be_bytes([out[2], out[3], out[4], out[5]]);
        let json_text = b"{\"a\":1}";
        assert_eq!(field_len, (json_text.len() + 1) as i32);
        assert_eq!(out[6], 0x01);
    }

    #[test]
    fn test_uuid_copy_binary_16_bytes() {
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
            0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
        ];
        let arr = Arc::new(FixedSizeBinaryArray::from(
            vec![Some(uuid_bytes.as_slice())]
        ));
        let field = arrow::datatypes::Field::new("id", DataType::FixedSizeBinary(16), true);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let out = copy_binary_rows(&batch, None);
        let field_len = i32::from_be_bytes([out[2], out[3], out[4], out[5]]);
        assert_eq!(field_len, 16);
        assert_eq!(&out[6..22], &uuid_bytes);
    }

    #[test]
    fn test_staging_ddl_uses_target_uuid_and_jsonb() {
        use potato_etl_common::db::common::alignment::TargetColumn;

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::FixedSizeBinary(16), false),
            arrow::datatypes::Field::new("name", DataType::Utf8, true),
            arrow::datatypes::Field::new("data", DataType::Utf8, true),
            arrow::datatypes::Field::new("payload", DataType::Utf8, true),
        ]));

        let target_cols = vec![
            TargetColumn { name: "id".into(),      data_type: "uuid".into(),    nullable: false, has_default: false },
            TargetColumn { name: "name".into(),     data_type: "text".into(),    nullable: true,  has_default: false },
            TargetColumn { name: "data".into(),     data_type: "jsonb".into(),   nullable: true,  has_default: false },
            TargetColumn { name: "payload".into(),  data_type: "json".into(),    nullable: true,  has_default: false },
        ];

        let ddl = build_staging_ddl("_stg", &schema, Some(&target_cols));
        assert!(ddl.contains("\"id\" UUID"), "Expected UUID, got: {ddl}");
        assert!(ddl.contains("\"name\" TEXT"), "Expected TEXT, got: {ddl}");
        assert!(ddl.contains("\"data\" JSONB"), "Expected JSONB, got: {ddl}");
        assert!(ddl.contains("\"payload\" JSON"), "Expected JSON, got: {ddl}");
        assert!(ddl.contains("ON COMMIT DELETE ROWS"));
    }

    #[test]
    fn test_staging_ddl_without_target_columns() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::FixedSizeBinary(16), false),
            arrow::datatypes::Field::new("name", DataType::Utf8, true),
        ]));

        let ddl = build_staging_ddl("_stg", &schema, None);
        // Without target columns, FixedSizeBinary(16) maps to UUID via DDL dialect.
        assert!(ddl.contains("\"id\" UUID"), "Expected UUID, got: {ddl}");
        assert!(ddl.contains("\"name\" TEXT"), "Expected TEXT, got: {ddl}");
    }

    // ── JSON vs JSONB wire format tests ─────────────────────────────────────

    #[test]
    fn test_json_copy_binary_no_version_byte() {
        // Postgres JSON (not JSONB) uses raw text — no 0x01 prefix.
        let mut meta = HashMap::new();
        meta.insert("source_db_type".to_string(), "json".to_string());
        let field = arrow::datatypes::Field::new("data", DataType::Utf8, true)
            .with_metadata(meta);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let arr = Arc::new(StringArray::from(vec![Some("{\"key\":\"value\"}")]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let out = copy_binary_rows(&batch, None);
        let field_len = i32::from_be_bytes([out[2], out[3], out[4], out[5]]);
        let json_text = b"{\"key\":\"value\"}";
        assert_eq!(field_len, json_text.len() as i32, "JSON should NOT have version byte");
        assert_eq!(&out[6..6 + json_text.len()], json_text);
    }

    #[test]
    fn test_json_via_target_type_no_version_byte() {
        // Cross-database: target is json (not jsonb) — no version byte.
        let field = arrow::datatypes::Field::new("data", DataType::Utf8, true);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let arr = Arc::new(StringArray::from(vec![Some("[1,2,3]")]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let target_types: Vec<&str> = vec!["json"];
        let out = copy_binary_rows(&batch, Some(&target_types));
        let field_len = i32::from_be_bytes([out[2], out[3], out[4], out[5]]);
        assert_eq!(field_len, 7, "JSON should be 7 bytes (no version prefix)");
        assert_eq!(&out[6..13], b"[1,2,3]");
    }

    #[test]
    fn test_target_type_overrides_source_metadata() {
        // Target says jsonb, source says text — target wins.
        let mut meta = HashMap::new();
        meta.insert("source_db_type".to_string(), "text".to_string());
        let field = arrow::datatypes::Field::new("data", DataType::Utf8, true)
            .with_metadata(meta);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let arr = Arc::new(StringArray::from(vec![Some("{\"x\":1}")]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let target_types: Vec<&str> = vec!["jsonb"];
        let out = copy_binary_rows(&batch, Some(&target_types));
        let field_len = i32::from_be_bytes([out[2], out[3], out[4], out[5]]);
        let json_text = b"{\"x\":1}";
        assert_eq!(field_len, (json_text.len() + 1) as i32, "JSONB should add version byte");
        assert_eq!(out[6], 0x01);
    }

    #[test]
    fn test_large_utf8_jsonb_copy_binary() {
        // LargeUtf8 with JSONB target type.
        let field = arrow::datatypes::Field::new("data", DataType::LargeUtf8, true);
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let arr = Arc::new(LargeStringArray::from(vec![Some("{\"big\":true}")]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();

        let target_types: Vec<&str> = vec!["jsonb"];
        let out = copy_binary_rows(&batch, Some(&target_types));
        let field_len = i32::from_be_bytes([out[2], out[3], out[4], out[5]]);
        let json_text = b"{\"big\":true}";
        assert_eq!(field_len, (json_text.len() + 1) as i32);
        assert_eq!(out[6], 0x01);
        assert_eq!(&out[7..7 + json_text.len()], json_text);
    }
}