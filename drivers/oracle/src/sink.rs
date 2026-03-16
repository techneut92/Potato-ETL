//! Oracle write sink — `OracleWriteDB`.
//!
//! Persistent writer thread with OCI array binding. See core docs for full architecture.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array,
    UInt8Array, UInt16Array, UInt32Array,
    LargeBinaryArray, BinaryArray,
    StringArray, LargeStringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampSecondArray, TimestampNanosecondArray,
    Date32Array,
    Time64MicrosecondArray, Time64NanosecondArray,
    Time32MillisecondArray, Time32SecondArray,
    DurationMicrosecondArray, DurationMillisecondArray,
    DurationSecondArray, DurationNanosecondArray,
};
use arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Timelike};
use oracle::sql_type::Timestamp as OracleTimestamp;

use potato_etl_common::config::{DatabaseSchemaConfig, StepDriverOptions};
use potato_etl_common::db::{TableMode, WriteStrategy};
use potato_etl_common::db::common::SinkConfig;
use potato_etl_common::db::common::alignment::{self as align, ColumnAlignment, MissingColumnBehavior};
use potato_etl_common::db::traits::SinkBuilder;
use potato_etl_common::schema::ddl::{generate_ddl_with_schema, DdlOptions, SqlDialect};
use potato_etl_common::schema::constants::META_LOGICAL_TYPE;
use crate::util::{OracleConn, oracle_ident, oracle_qualified_table};

const DEFAULT_OCI_BATCH_SIZE: usize = 20_000;

// ── Writer-thread protocol ───────────────────────────────────────────────────

enum OracleCmd {
    WriteBatch { batch: RecordBatch, reply: tokio::sync::oneshot::Sender<anyhow::Result<usize>> },
    Commit { reply: tokio::sync::oneshot::Sender<anyhow::Result<()>> },
}

struct OracleWriterHandle {
    cmd_tx: tokio::sync::mpsc::Sender<OracleCmd>,
    _handle: Option<std::thread::JoinHandle<()>>,
}

struct WriterState {
    oci_conn: oracle::Connection,
    cfg: SinkConfig,
    direct_path: bool,
    parallel: Option<u32>,
    first_batch: bool,
    alignment: Option<ColumnAlignment>,
    alignment_resolved: bool,
    target_columns: Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>,
    ora_major_version: u32,
    oci_batch_size: usize,
    direct_path_committed_rows: usize,
}

impl WriterState {
    fn run(&mut self, mut cmd_rx: tokio::sync::mpsc::Receiver<OracleCmd>) {
        loop {
            match cmd_rx.blocking_recv() {
                Some(OracleCmd::WriteBatch { batch, reply }) => {
                    let result = std::panic::catch_unwind(
                        std::panic::AssertUnwindSafe(|| self.handle_write_batch(batch))
                    );
                    match result {
                        Ok(r) => { reply.send(r).ok(); }
                        Err(e) => { self.oci_conn.rollback().ok(); reply.send(Err(anyhow::anyhow!("panic: {:?}", e))).ok(); break; }
                    }
                }
                Some(OracleCmd::Commit { reply }) => { reply.send(self.do_commit()).ok(); }
                None => { self.oci_conn.rollback().ok(); break; }
            }
        }
    }

    fn do_commit(&self) -> anyhow::Result<()> {
        if self.direct_path && self.direct_path_committed_rows > 0 { return Ok(()); }
        self.oci_conn.commit().map_err(|e| anyhow::anyhow!("Oracle COMMIT failed: {e}"))?;
        Ok(())
    }

    fn handle_write_batch(&mut self, batch: RecordBatch) -> anyhow::Result<usize> {
        let num_rows = batch.num_rows();
        if num_rows == 0 { return Ok(0); }
        self.ensure_alignment(&batch.schema())?;
        let batch = self.align_batch(batch)?;
        let schema_name = &self.cfg.schema_name;
        let table = &self.cfg.table;
        let full_table = oracle_qualified_table(schema_name, table);
        let schema = batch.schema();
        let ddl_schema = self.cfg.ddl_schema.clone().unwrap_or_else(|| schema.clone());

        // ── DDL ──────────────────────────────────────────────────────────────
        if !self.cfg.table_prepared {
            let ddl_options = DdlOptions { ora_major_version: Some(self.ora_major_version), ..Default::default() };
            let db_config = self.cfg.database_schema_config.as_ref();
            match self.cfg.table_mode {
                TableMode::UseExisting => {}
                TableMode::CreateIfNotExists => {
                    let stmts = generate_ddl_with_schema(table, Some(schema_name), &ddl_schema, SqlDialect::Oracle, None, db_config, ddl_options);
                    for stmt in &stmts.pre_create {
                        tracing::trace!(table = %table, "Oracle pre-create DDL:\n{stmt}");
                        self.oci_conn.execute(stmt, &[])?;
                    }
                    tracing::debug!(table = %table, "Oracle DDL:\n{}", stmts.create_table);
                    self.oci_conn.execute(&stmts.create_table, &[])?;
                    for stmt in &stmts.post_create {
                        tracing::trace!(table = %table, "Oracle post-create DDL:\n{stmt}");
                        if let Err(e) = self.oci_conn.execute(stmt, &[]) { tracing::warn!(table = %table, "Oracle post-create DDL ignored: {e:#}"); }
                    }
                    tracing::info!(table = %table, "Oracle CREATE TABLE IF NOT EXISTS applied");
                }
                TableMode::DropAndReplace => {
                    let drop_plsql = format!("DECLARE e EXCEPTION; PRAGMA EXCEPTION_INIT(e,-942); BEGIN EXECUTE IMMEDIATE 'DROP TABLE {full_table} PURGE'; EXCEPTION WHEN e THEN NULL; END;");
                    self.oci_conn.execute(&drop_plsql, &[])?;
                    let stmts = generate_ddl_with_schema(table, Some(schema_name), &ddl_schema, SqlDialect::Oracle, None, db_config, ddl_options);
                    for stmt in &stmts.pre_create {
                        tracing::trace!(table = %table, "Oracle pre-create DDL:\n{stmt}");
                        self.oci_conn.execute(stmt, &[])?;
                    }
                    // stmts.create_table is wrapped in BEGIN/EXCEPTION for IF NOT EXISTS —
                    // harmless after a DROP since the CREATE will simply succeed.
                    tracing::debug!(table = %table, "Oracle DDL:\n{}", stmts.create_table);
                    self.oci_conn.execute(&stmts.create_table, &[])?;
                    for stmt in &stmts.post_create {
                        tracing::trace!(table = %table, "Oracle post-create DDL:\n{stmt}");
                        if let Err(e) = self.oci_conn.execute(stmt, &[]) { tracing::warn!(table = %table, "Oracle post-create DDL ignored: {e:#}"); }
                    }
                    tracing::info!(table = %table, "Oracle DROP + CREATE TABLE applied");
                }
            }
            self.cfg.table_prepared = true;
        }

        // ── TRUNCATE ─────────────────────────────────────────────────────────
        if self.first_batch && matches!(self.cfg.write_strategy, WriteStrategy::Truncate)
            && !matches!(self.cfg.table_mode, TableMode::DropAndReplace) {
            self.oci_conn.execute(&format!("TRUNCATE TABLE {full_table}"), &[])?;
            tracing::info!(table = %table, "Oracle TRUNCATE applied");
        }
        self.first_batch = false;

        // ── DML ──────────────────────────────────────────────────────────────
        let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let col_sql = col_names.iter().map(|c| oracle_ident(c)).collect::<Vec<_>>().join(", ");
        let n_cols = col_names.len();
        let bind_placeholders = (1..=n_cols).map(|i| format!(":{i}")).collect::<Vec<_>>().join(", ");

        match &self.cfg.write_strategy {
            WriteStrategy::Append | WriteStrategy::Truncate => {
                let hint = match (self.direct_path, self.parallel) {
                    (true, Some(deg))  => format!("/*+ APPEND_VALUES PARALLEL({table}, {deg}) */ "),
                    (false, Some(deg)) => format!("/*+ PARALLEL({table}, {deg}) */ "),
                    (true, None)       => "/*+ APPEND_VALUES */ ".to_string(),
                    (false, None)      => String::new(),
                };
                let sql = format!("INSERT {hint}INTO {full_table} ({col_sql}) VALUES ({bind_placeholders})");
                let typed_cols = extract_typed_columns(&batch)?;
                let mut refs_buf: Vec<&dyn oracle::sql_type::ToSql> = Vec::with_capacity(n_cols);
                for chunk_start in (0..num_rows).step_by(self.oci_batch_size) {
                    let chunk_end = (chunk_start + self.oci_batch_size).min(num_rows);
                    let chunk_len = chunk_end - chunk_start;
                    let mut oci_batch = self.oci_conn.batch(&sql, chunk_len).build()?;
                    for row_idx in chunk_start..chunk_end {
                        refs_buf.clear();
                        for col in &typed_cols { refs_buf.push(col.get_ref(row_idx)); }
                        oci_batch.append_row(&refs_buf)?;
                    }
                    oci_batch.execute()?;
                    if self.direct_path {
                        self.oci_conn.commit()?;
                        self.direct_path_committed_rows += chunk_len;
                    }
                }
            }
            WriteStrategy::InsertIgnore => {
                let typed_cols = extract_typed_columns(&batch)?;
                let pk_cols = potato_etl_common::db::pk_columns(&schema);
                anyhow::ensure!(!pk_cols.is_empty(), "insert_ignore requires primary_key");
                let on_clause = pk_cols.iter().map(|k| format!("T.{ci} = S.{ci}", ci = oracle_ident(k))).collect::<Vec<_>>().join(" AND ");
                let src_vals = col_names.iter().map(|c| format!("S.{}", oracle_ident(c))).collect::<Vec<_>>().join(", ");
                let using_select = col_names.iter().enumerate().map(|(i, c)| format!(":{} AS {}", i+1, oracle_ident(c))).collect::<Vec<_>>().join(", ");
                let sql = format!("MERGE INTO {full_table} T USING (SELECT {using_select} FROM DUAL) S ON ({on_clause}) WHEN NOT MATCHED THEN INSERT ({col_sql}) VALUES ({src_vals})");
                execute_typed_merge(&self.oci_conn, &sql, &typed_cols, num_rows, self.oci_batch_size)?;
            }
            WriteStrategy::Upsert | WriteStrategy::MergeDelete => {
                let typed_cols = extract_typed_columns(&batch)?;
                let pk_cols = potato_etl_common::db::pk_columns(&schema);
                anyhow::ensure!(!pk_cols.is_empty(), "upsert/merge_delete requires primary_key");
                let on_clause = pk_cols.iter().map(|k| format!("T.{ci} = S.{ci}", ci = oracle_ident(k))).collect::<Vec<_>>().join(" AND ");
                let update_set = col_names.iter().filter(|c| !pk_cols.contains(c)).map(|c| format!("T.{ci} = S.{ci}", ci = oracle_ident(c))).collect::<Vec<_>>().join(", ");
                let src_vals = col_names.iter().map(|c| format!("S.{}", oracle_ident(c))).collect::<Vec<_>>().join(", ");
                let using_select = col_names.iter().enumerate().map(|(i, c)| format!(":{} AS {}", i+1, oracle_ident(c))).collect::<Vec<_>>().join(", ");
                let sql = format!("MERGE INTO {full_table} T USING (SELECT {using_select} FROM DUAL) S ON ({on_clause}) WHEN MATCHED THEN UPDATE SET {update_set} WHEN NOT MATCHED THEN INSERT ({col_sql}) VALUES ({src_vals})");
                execute_typed_merge(&self.oci_conn, &sql, &typed_cols, num_rows, self.oci_batch_size)?;
                if matches!(self.cfg.write_strategy, WriteStrategy::MergeDelete) {
                    let pk_col = &pk_cols[0];
                    let pk_idx = col_names.iter().position(|c| c == pk_col).unwrap_or(0);
                    let pk_vals: Vec<Option<String>> = (0..num_rows).map(|i| {
                        match &typed_cols[pk_idx] {
                            TypedCol::I64(v) => v[i].map(|n| n.to_string()),
                            TypedCol::F64(v) => v[i].map(|n| n.to_string()),
                            TypedCol::Str(v) => v[i].clone(),
                            TypedCol::Ts(v) => v[i].as_ref().map(|t| format!("{t:?}")),
                            TypedCol::Bytes(v) => v[i].as_ref().map(|b| format!("[{}B]", b.len())),
                        }
                    }).collect();
                    for chunk in pk_vals.chunks(999) {
                        let placeholders = (1..=chunk.len()).map(|i| format!(":{i}")).collect::<Vec<_>>().join(", ");
                        let del_sql = format!("DELETE FROM {full_table} WHERE {} NOT IN ({placeholders})", oracle_ident(pk_col));
                        let bind_refs: Vec<&dyn oracle::sql_type::ToSql> = chunk.iter().map(|v| v as &dyn oracle::sql_type::ToSql).collect();
                        self.oci_conn.execute(&del_sql, &bind_refs)?;
                    }
                }
            }
        }
        tracing::debug!(table = %table, rows = num_rows, "Oracle batch written");
        Ok(num_rows)
    }

    fn ensure_alignment(&mut self, batch_schema: &SchemaRef) -> anyhow::Result<()> {
        if self.alignment_resolved { return Ok(()); }
        if matches!(self.cfg.table_mode, TableMode::DropAndReplace) { self.alignment_resolved = true; return Ok(()); }
        let target_cols = crate::util::oracle_introspect_table_columns(&self.oci_conn, &self.cfg.schema_name, &self.cfg.table)?;
        let target_cols = match target_cols { Some(c) => c, None => { self.alignment_resolved = true; return Ok(()); } };
        let table_display = format!("\"{}\".\"{}\""  , self.cfg.schema_name, self.cfg.table);
        let result = align::compute_alignment(batch_schema, &target_cols, MissingColumnBehavior::Skip, &table_display)?;
        self.target_columns = Some(target_cols);
        self.alignment = Some(result);
        self.alignment_resolved = true;
        Ok(())
    }

    fn align_batch(&self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        let batch = match &self.alignment { Some(a) => align::apply_alignment(batch, a)?, None => batch };
        use potato_etl_common::db::common::type_coercion::{coerce_batch_for_target, TargetColumn};
        use crate::type_registry::OracleTypeRegistry;
        let tc = self.target_columns.as_ref().map(|cols| cols.iter().map(|c| TargetColumn { name: c.name.clone(), data_type: c.data_type.clone() }).collect::<Vec<_>>());
        coerce_batch_for_target(batch, tc.as_deref(), &OracleTypeRegistry)
    }
}

// ── TypedCol (native OCI binding) ───────────────────────────────────────────

enum TypedCol { I64(Vec<Option<i64>>), F64(Vec<Option<f64>>), Str(Vec<Option<String>>), Ts(Vec<Option<OracleTimestamp>>), Bytes(Vec<Option<Vec<u8>>>) }

impl TypedCol {
    fn get_ref(&self, i: usize) -> &dyn oracle::sql_type::ToSql {
        match self { Self::I64(v) => &v[i], Self::F64(v) => &v[i], Self::Str(v) => &v[i], Self::Ts(v) => &v[i], Self::Bytes(v) => &v[i] }
    }
    #[allow(dead_code)]
    fn debug_value(&self, i: usize) -> String {
        match self { Self::I64(v) => v[i].map_or("NULL".into(), |v| v.to_string()), Self::F64(v) => v[i].map_or("NULL".into(), |v| v.to_string()), Self::Str(v) => v[i].as_ref().map_or("NULL".into(), |v| format!("'{v}'")), Self::Ts(v) => v[i].as_ref().map_or("NULL".into(), |v| format!("{v:?}")), Self::Bytes(v) => v[i].as_ref().map_or("NULL".into(), |v| format!("[{}B]", v.len())) }
    }
}

fn extract_typed_columns(batch: &RecordBatch) -> anyhow::Result<Vec<TypedCol>> {
    let n = batch.num_rows();
    batch.columns().iter().zip(batch.schema().fields().iter()).map(|(col, field)| extract_one_column(col, field, n)).collect()
}

fn extract_one_column(col: &Arc<dyn Array>, field: &Field, n: usize) -> anyhow::Result<TypedCol> {
    let dt = field.data_type();
    Ok(match dt {
        DataType::Int64  => { let a = col.as_any().downcast_ref::<Int64Array>().unwrap(); TypedCol::I64((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i)) }).collect()) }
        DataType::Int32  => { let a = col.as_any().downcast_ref::<Int32Array>().unwrap(); TypedCol::I64((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i) as i64) }).collect()) }
        DataType::Int16  => { let a = col.as_any().downcast_ref::<Int16Array>().unwrap(); TypedCol::I64((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i) as i64) }).collect()) }
        DataType::Int8   => { let a = col.as_any().downcast_ref::<Int8Array>().unwrap(); TypedCol::I64((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i) as i64) }).collect()) }
        DataType::UInt32 => { let a = col.as_any().downcast_ref::<UInt32Array>().unwrap(); TypedCol::I64((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i) as i64) }).collect()) }
        DataType::UInt16 => { let a = col.as_any().downcast_ref::<UInt16Array>().unwrap(); TypedCol::I64((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i) as i64) }).collect()) }
        DataType::UInt8  => { let a = col.as_any().downcast_ref::<UInt8Array>().unwrap(); TypedCol::I64((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i) as i64) }).collect()) }
        DataType::Float64 => { let a = col.as_any().downcast_ref::<Float64Array>().unwrap(); TypedCol::F64((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i)) }).collect()) }
        DataType::Float32 => { let a = col.as_any().downcast_ref::<Float32Array>().unwrap(); TypedCol::F64((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i) as f64) }).collect()) }
        DataType::Boolean => { let a = col.as_any().downcast_ref::<BooleanArray>().unwrap(); TypedCol::I64((0..n).map(|i| if a.is_null(i) { None } else { Some(if a.value(i) { 1 } else { 0 }) }).collect()) }
        DataType::Timestamp(TimeUnit::Microsecond, _) => { let a = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap(); TypedCol::Ts((0..n).map(|i| if a.is_null(i) { None } else { Some(micros_to_oracle_ts(a.value(i))) }).collect()) }
        DataType::Timestamp(TimeUnit::Millisecond, _) => { let a = col.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap(); TypedCol::Ts((0..n).map(|i| if a.is_null(i) { None } else { Some(micros_to_oracle_ts(a.value(i) * 1_000)) }).collect()) }
        DataType::Timestamp(TimeUnit::Second, _) => { let a = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap(); TypedCol::Ts((0..n).map(|i| if a.is_null(i) { None } else { Some(micros_to_oracle_ts(a.value(i) * 1_000_000)) }).collect()) }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => { let a = col.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap(); TypedCol::Ts((0..n).map(|i| if a.is_null(i) { None } else { Some(micros_to_oracle_ts(a.value(i) / 1_000)) }).collect()) }
        DataType::Date32 => { let a = col.as_any().downcast_ref::<Date32Array>().unwrap(); TypedCol::Ts((0..n).map(|i| if a.is_null(i) { None } else { let d = chrono::NaiveDate::from_num_days_from_ce_opt(a.value(i) + 719_163).unwrap_or(chrono::NaiveDate::from_ymd_opt(1970,1,1).unwrap()); Some(OracleTimestamp::new(d.year(), d.month(), d.day(), 0,0,0,0).expect("valid ts")) }).collect()) }
        DataType::Time64(TimeUnit::Microsecond) => { let a = col.as_any().downcast_ref::<Time64MicrosecondArray>().unwrap(); TypedCol::Str((0..n).map(|i| if a.is_null(i) { None } else { Some(micros_to_oracle_interval_str(a.value(i))) }).collect()) }
        DataType::Time64(TimeUnit::Nanosecond) => { let a = col.as_any().downcast_ref::<Time64NanosecondArray>().unwrap(); TypedCol::Str((0..n).map(|i| if a.is_null(i) { None } else { Some(micros_to_oracle_interval_str(a.value(i)/1_000)) }).collect()) }
        DataType::Time32(TimeUnit::Second) => { let a = col.as_any().downcast_ref::<Time32SecondArray>().unwrap(); TypedCol::Str((0..n).map(|i| if a.is_null(i) { None } else { Some(micros_to_oracle_interval_str(a.value(i) as i64 * 1_000_000)) }).collect()) }
        DataType::Time32(TimeUnit::Millisecond) => { let a = col.as_any().downcast_ref::<Time32MillisecondArray>().unwrap(); TypedCol::Str((0..n).map(|i| if a.is_null(i) { None } else { Some(micros_to_oracle_interval_str(a.value(i) as i64 * 1_000)) }).collect()) }
        DataType::Duration(TimeUnit::Microsecond) => { let a = col.as_any().downcast_ref::<DurationMicrosecondArray>().unwrap(); TypedCol::Str((0..n).map(|i| if a.is_null(i) { None } else { Some(duration_micros_to_oracle_interval_str(a.value(i))) }).collect()) }
        DataType::Duration(TimeUnit::Millisecond) => { let a = col.as_any().downcast_ref::<DurationMillisecondArray>().unwrap(); TypedCol::Str((0..n).map(|i| if a.is_null(i) { None } else { Some(duration_micros_to_oracle_interval_str(a.value(i)*1_000)) }).collect()) }
        DataType::Duration(TimeUnit::Second) => { let a = col.as_any().downcast_ref::<DurationSecondArray>().unwrap(); TypedCol::Str((0..n).map(|i| if a.is_null(i) { None } else { Some(duration_micros_to_oracle_interval_str(a.value(i)*1_000_000)) }).collect()) }
        DataType::Duration(TimeUnit::Nanosecond) => { let a = col.as_any().downcast_ref::<DurationNanosecondArray>().unwrap(); TypedCol::Str((0..n).map(|i| if a.is_null(i) { None } else { Some(duration_micros_to_oracle_interval_str(a.value(i)/1_000)) }).collect()) }
        DataType::Utf8 => { let a = col.as_any().downcast_ref::<StringArray>().unwrap(); let is_iv = is_interval_logical_type(field); TypedCol::Str((0..n).map(|i| if a.is_null(i) { None } else if is_iv { Some(pg_interval_to_oracle_dsinterval(a.value(i))) } else { Some(a.value(i).to_string()) }).collect()) }
        DataType::LargeUtf8 => { let a = col.as_any().downcast_ref::<LargeStringArray>().unwrap(); let is_iv = is_interval_logical_type(field); TypedCol::Str((0..n).map(|i| if a.is_null(i) { None } else if is_iv { Some(pg_interval_to_oracle_dsinterval(a.value(i))) } else { Some(a.value(i).to_string()) }).collect()) }
        DataType::LargeBinary => { let a = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap(); TypedCol::Bytes((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i).to_vec()) }).collect()) }
        DataType::Binary => { let a = col.as_any().downcast_ref::<BinaryArray>().unwrap(); TypedCol::Bytes((0..n).map(|i| if a.is_null(i) { None } else { Some(a.value(i).to_vec()) }).collect()) }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => { TypedCol::Str((0..n).map(|i| if col.is_null(i) { None } else { arrow::util::display::array_value_to_string(col.as_ref(), i).ok() }).collect()) }
        _ => anyhow::bail!("Oracle: unsupported Arrow type {:?} for column \"{}\"", dt, field.name()),
    })
}

fn micros_to_oracle_ts(micros: i64) -> OracleTimestamp {
    let secs = micros.div_euclid(1_000_000); let rem = micros.rem_euclid(1_000_000); let nanos = (rem * 1_000) as u32;
    let dt = chrono::DateTime::from_timestamp(secs, nanos).unwrap_or(chrono::DateTime::UNIX_EPOCH);
    OracleTimestamp::new(dt.year(), dt.month(), dt.day(), dt.hour(), dt.minute(), dt.second(), nanos).expect("valid ts")
}

fn micros_to_oracle_interval_str(micros: i64) -> String {
    let total_secs = micros / 1_000_000; let frac = (micros % 1_000_000).unsigned_abs();
    let h = total_secs / 3600; let m = (total_secs % 3600) / 60; let s = total_secs % 60;
    format!("+0 {:02}:{:02}:{:02}.{:06}", h, m, s, frac)
}

fn duration_micros_to_oracle_interval_str(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "+" }; let abs = micros.unsigned_abs();
    let total_secs = abs / 1_000_000; let frac = abs % 1_000_000;
    let days = total_secs / 86_400; let rem = total_secs % 86_400;
    let h = rem / 3_600; let m = (rem % 3_600) / 60; let s = rem % 60;
    format!("{sign}{days} {:02}:{:02}:{:02}.{:06}", h, m, s, frac)
}

fn is_interval_logical_type(field: &Field) -> bool {
    field.metadata().get(META_LOGICAL_TYPE).map_or(false, |lt| { let u = lt.to_ascii_uppercase(); u == "INTERVAL" || u == "TIME" })
}

fn pg_interval_to_oracle_dsinterval(pg: &str) -> String {
    let s = pg.trim(); let mut days: i64 = 0; let mut time_part = ""; let mut negative_time = false;
    let mut tokens = s.split_whitespace().peekable();
    while let Some(tok) = tokens.next() {
        if tok.contains(':') { if tok.starts_with('-') { negative_time = true; time_part = &tok[1..]; } else { time_part = tok; } break; }
        if let Ok(n) = tok.parse::<i64>() { if let Some(&unit) = tokens.peek() { let u = unit.to_ascii_lowercase(); if u.starts_with("year") { days += n*365; tokens.next(); } else if u.starts_with("mon") { days += n*30; tokens.next(); } else if u.starts_with("day") { days += n; tokens.next(); } } }
    }
    let (h, m, sec_whole, sec_frac) = parse_hms(time_part);
    let time_sign: i64 = if negative_time { -1 } else { 1 };
    let total_micros = days * 86_400_000_000 + time_sign * ((h as i64)*3_600_000_000 + (m as i64)*60_000_000 + (sec_whole as i64)*1_000_000 + sec_frac as i64);
    duration_micros_to_oracle_interval_str(total_micros)
}

fn parse_hms(s: &str) -> (u32, u32, u32, u32) {
    if s.is_empty() { return (0,0,0,0); }
    let parts: Vec<&str> = s.splitn(3, ':').collect();
    let h: u32 = parts.first().and_then(|p| p.parse().ok()).unwrap_or(0);
    let m: u32 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
    let (sw, sf) = if let Some(ss) = parts.get(2) {
        if let Some((whole, frac)) = ss.split_once('.') {
            let w: u32 = whole.parse().unwrap_or(0);
            let mut f_str = frac.to_string(); while f_str.len() < 6 { f_str.push('0'); } f_str.truncate(6);
            (w, f_str.parse().unwrap_or(0))
        } else { (ss.parse().unwrap_or(0), 0u32) }
    } else { (0,0) };
    (h, m, sw, sf)
}

// ── OracleWriteDB ─────────────────────────────────────────────────────────────

pub struct OracleWriteDB {
    conn: OracleConn,
    pub cfg: SinkConfig,
    direct_path: bool,
    parallel: Option<u32>,
    oci_batch_size: usize,
    writer: Option<OracleWriterHandle>,
}

impl OracleWriteDB {
    pub fn new(conn_str: &str) -> anyhow::Result<Self> {
        Ok(Self { conn: OracleConn::parse(conn_str)?, cfg: SinkConfig::new(""), direct_path: false, parallel: None, oci_batch_size: DEFAULT_OCI_BATCH_SIZE, writer: None })
    }

    fn ensure_writer(&mut self) -> anyhow::Result<&OracleWriterHandle> {
        if self.writer.is_some() { return Ok(self.writer.as_ref().unwrap()); }
        let conn = self.conn.clone();
        let mut cfg = self.cfg.clone();
        let direct_path = self.direct_path;
        let parallel = self.parallel;
        let oci_batch_size = self.oci_batch_size;
        if cfg.schema_name.is_empty() { cfg.schema_name = conn.user.to_uppercase(); }
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<OracleCmd>(4);
        let handle = std::thread::Builder::new().name(format!("oracle-writer-{}", cfg.table)).spawn(move || {
            let oci_conn = match conn.open() {
                Ok(c) => c,
                Err(e) => { let mut rx = cmd_rx; while let Some(cmd) = rx.blocking_recv() { match cmd { OracleCmd::WriteBatch { reply, .. } => { reply.send(Err(anyhow::anyhow!("conn failed: {e}"))).ok(); } OracleCmd::Commit { reply, .. } => { reply.send(Err(anyhow::anyhow!("conn failed: {e}"))).ok(); } } } return; }
            };
            let ora_major_version = crate::util::query_oracle_major_version(&oci_conn);
            if let Some(_deg) = parallel { let _ = oci_conn.execute("ALTER SESSION ENABLE PARALLEL DML", &[]); }
            let mut state = WriterState { oci_conn, cfg, direct_path, parallel, first_batch: true, alignment: None, alignment_resolved: false, target_columns: None, ora_major_version, oci_batch_size, direct_path_committed_rows: 0 };
            state.run(cmd_rx);
        })?;
        self.writer = Some(OracleWriterHandle { cmd_tx, _handle: Some(handle) });
        Ok(self.writer.as_ref().unwrap())
    }
}

impl SinkBuilder for OracleWriteDB {
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
        if let Some(dp) = opts.direct_path { self.direct_path = dp; }
        if let Some(p) = opts.parallel { self.parallel = Some(p); }
        if let Some(bs) = opts.oci_batch_size { self.oci_batch_size = bs; }
        self
    }
    fn set_database_schema_config(&mut self, config: DatabaseSchemaConfig) { self.cfg.database_schema_config = Some(config); }
    fn set_ddl_schema(&mut self, schema: SchemaRef) { self.cfg.ddl_schema = Some(schema); }

    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<usize>> + Send + 'a>> {
        Box::pin(async move {
            if batch.num_rows() == 0 { return Ok(0); }
            self.ensure_writer()?;
            let writer = self.writer.as_ref().unwrap();
            let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
            writer.cmd_tx.send(OracleCmd::WriteBatch { batch, reply: reply_tx }).await.map_err(|_| anyhow::anyhow!("Oracle writer thread exited"))?;
            reply_rx.await.map_err(|_| anyhow::anyhow!("Oracle writer dropped reply"))?
        })
    }

    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(writer) = self.writer.take() {
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                writer.cmd_tx.send(OracleCmd::Commit { reply: reply_tx }).await.map_err(|_| anyhow::anyhow!("Oracle writer exited"))?;
                reply_rx.await.map_err(|_| anyhow::anyhow!("Oracle writer dropped reply"))??;
                drop(writer);
            }
            tracing::info!(table = %self.cfg.table, "Oracle flush complete");
            self.cfg.table_prepared = false;
            Ok(())
        })
    }
}

fn execute_typed_merge(oci_conn: &oracle::Connection, sql: &str, typed_cols: &[TypedCol], num_rows: usize, oci_batch_size: usize) -> anyhow::Result<()> {
    for chunk_start in (0..num_rows).step_by(oci_batch_size) {
        let chunk_end = (chunk_start + oci_batch_size).min(num_rows);
        let chunk_len = chunk_end - chunk_start;
        let mut batch = oci_conn.batch(sql, chunk_len).build()?;
        for row_idx in chunk_start..chunk_end {
            let mut refs_buf: Vec<&dyn oracle::sql_type::ToSql> = Vec::with_capacity(typed_cols.len());
            for col in typed_cols { refs_buf.push(col.get_ref(row_idx)); }
            batch.append_row(&refs_buf)?;
        }
        batch.execute()?;
    }
    Ok(())
}

// `oracle_create_table_sql` has been removed — DDL is now generated by
// `generate_ddl_with_schema` from the common crate, which correctly includes
// PRIMARY KEY, UNIQUE, FOREIGN KEY, indexes, and all other constraints.