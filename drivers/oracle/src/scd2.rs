//! Oracle SCD Type 2 sink — `OracleScd2Sink`.

use std::collections::HashMap;
use std::collections::HashSet;

use arrow::record_batch::RecordBatch;

use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::{Scd2ColumnNames, Scd2Stats};
use potato_etl_common::db::common::{CurrentRows, Scd2Config, compute_scd2_decision};
use potato_etl_common::db::traits::Scd2Builder;
use potato_etl_common::util::arrow::extract_scd2_key_strings;
use crate::util::OracleConn;
use crate::util::{oracle_ident, oracle_qualified_table};

const DEFAULT_OCI_BATCH_SIZE: usize = 20_000;

pub struct OracleScd2Sink {
    conn: OracleConn,
    pub cfg: Scd2Config,
    oci_batch_size: usize,
    /// Keys seen across all batches — only populated when `close_missing` is true.
    seen_keys: HashSet<String>,
}

impl OracleScd2Sink {
    pub fn new(conn_str: &str) -> anyhow::Result<Self> {
        let mut cfg = Scd2Config::new("");
        cfg.chunk_size = DEFAULT_OCI_BATCH_SIZE;
        Ok(Self { conn: OracleConn::parse(conn_str)?, cfg, oci_batch_size: DEFAULT_OCI_BATCH_SIZE, seen_keys: HashSet::new() })
    }
}

impl Scd2Builder for OracleScd2Sink {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.table(t); self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.schema(s); self }
    fn key(mut self: Box<Self>, col: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.key(col); self }
    fn track(mut self: Box<Self>, cols: Vec<String>) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.track(cols); self }
    fn col_names(mut self: Box<Self>, n: Scd2ColumnNames) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.col_names(n); self }
    fn close_missing(mut self: Box<Self>, close: bool) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.close_missing(close); self }
    fn chunk_size(mut self: Box<Self>, n: usize) -> Box<dyn Scd2Builder> {
        self.cfg = self.cfg.chunk_size(n);
        self.oci_batch_size = self.cfg.chunk_size;
        self
    }
    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn Scd2Builder> {
        if let Some(bs) = opts.oci_batch_size {
            self.oci_batch_size = bs;
            self.cfg.chunk_size = bs;
        }
        self
    }

    fn ddl_params(&self) -> (String, Option<String>, String, Scd2ColumnNames, String) {
        (self.cfg.table.clone(), if self.cfg.schema_name.is_empty() { None } else { Some(self.cfg.schema_name.clone()) },
         self.cfg.key_col.clone(), self.cfg.col_names.clone(), format!("oracle://localhost/{}", self.cfg.table))
    }

    fn execute_ddl<'a>(&'a mut self, sql: &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let conn = self.conn.clone();
            let sql = sql.to_string();
            tokio::task::spawn_blocking(move || {
                let oci = conn.open()?;
                let stmt = sql.trim_end().trim_end_matches('/').trim();
                oci.execute(stmt, &[])?;
                oci.commit()?;
                Ok::<_, anyhow::Error>(())
            }).await??;
            Ok(())
        })
    }

    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Scd2Stats>> + Send + 'a>> {
        Box::pin(async move {
            if batch.num_rows() == 0 { return Ok(Scd2Stats::default()); }
            let schema = batch.schema();
            let key_idx = schema.index_of(&self.cfg.key_col)?;
            let key_vals = extract_scd2_key_strings(&batch, key_idx)?;

            // Track seen keys for close_missing mode.
            if self.cfg.close_missing {
                self.seen_keys.extend(key_vals.iter().cloned());
            }

            let conn = self.conn.clone();
            let cn = self.cfg.col_names.clone();
            let table = self.cfg.table.clone();
            let schema_name = self.cfg.schema_name.clone();
            let cfg_snapshot = self.cfg.clone();
            let oci_batch_size = self.oci_batch_size;

            tokio::task::spawn_blocking(move || -> anyhow::Result<Scd2Stats> {
                let oci_conn = conn.open()?;
                let full_table = oracle_qualified_table(&schema_name, &table);
                let existing = oracle_fetch_current(&oci_conn, &schema_name, &table, &cfg_snapshot.key_col, &key_vals, &cn.is_current)?;
                let decision = compute_scd2_decision(&batch, &cfg_snapshot, &existing)?;

                for chunk in decision.to_close.chunks(oci_batch_size) {
                    let sql = format!("UPDATE {full_table} SET {vt} = SYSTIMESTAMP, {ic} = 0 WHERE {ic} = 1 AND {kc} = :1",
                        vt = oracle_ident(&cn.valid_to), ic = oracle_ident(&cn.is_current), kc = oracle_ident(&cfg_snapshot.key_col));
                    let mut bs = oci_conn.batch(&sql, chunk.len()).build()?;
                    for key in chunk { let val: Option<&str> = Some(key.as_str()); bs.append_row(&[&val as &dyn oracle::sql_type::ToSql])?; }
                    bs.execute()?;
                }

                if !decision.to_insert.is_empty() {
                    let data_col_indices: Vec<usize> = decision.col_names_vec.iter().enumerate()
                        .filter(|(_, n)| !decision.scd_meta.iter().any(|m| m == n.as_str())).map(|(i, _)| i).collect();
                    let data_col_sql = data_col_indices.iter().map(|&i| oracle_ident(&decision.col_names_vec[i])).collect::<Vec<_>>().join(", ");
                    let n_data = data_col_indices.len();
                    let bind_ph = (1..=n_data).map(|i| format!(":{i}")).collect::<Vec<_>>().join(", ");
                    let sql = format!("INSERT INTO {full_table} ({data_col_sql}, {vf}, {vt}, {ic}) VALUES ({bind_ph}, SYSTIMESTAMP, NULL, 1)",
                        vf = oracle_ident(&cn.valid_from), vt = oracle_ident(&cn.valid_to), ic = oracle_ident(&cn.is_current));
                    for chunk in decision.to_insert.chunks(oci_batch_size) {
                        let mut bs = oci_conn.batch(&sql, chunk.len()).build()?;
                        for row in chunk {
                            let vals: Vec<Option<&str>> = data_col_indices.iter().map(|&i| row.get(i).and_then(|v| v.as_deref())).collect();
                            bs.append_row(&vals.iter().map(|v| v as &dyn oracle::sql_type::ToSql).collect::<Vec<_>>())?;
                        }
                        bs.execute()?;
                    }
                }
                oci_conn.commit()?;
                Ok(decision.stats)
            }).await?
        })
    }

    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // ── Close-missing: expire current rows whose key was never seen ──
            if self.cfg.close_missing && !self.seen_keys.is_empty() {
                let conn = self.conn.clone();
                let cfg = self.cfg.clone();
                let seen = std::mem::take(&mut self.seen_keys);

                tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    let oci_conn = conn.open()?;
                    let full_table = oracle_qualified_table(&cfg.schema_name, &cfg.table);
                    let cn = &cfg.col_names;

                    // 1. Fetch all current keys.
                    let all_current_keys = oracle_fetch_all_current_keys(
                        &oci_conn, &cfg.schema_name, &cfg.table, &cfg.key_col, &cn.is_current,
                    )?;

                    // 2. Compute missing keys.
                    let to_close: Vec<String> = all_current_keys.into_iter()
                        .filter(|k| !seen.contains(k))
                        .collect();

                    if !to_close.is_empty() {
                        tracing::info!(
                            table = %cfg.table,
                            count = to_close.len(),
                            "close_missing: expiring {} current rows not seen in incoming data",
                            to_close.len(),
                        );
                        let sql = format!(
                            "UPDATE {full_table} SET {vt} = SYSTIMESTAMP, {ic} = 0 WHERE {ic} = 1 AND {kc} = :1",
                            vt = oracle_ident(&cn.valid_to),
                            ic = oracle_ident(&cn.is_current),
                            kc = oracle_ident(&cfg.key_col),
                        );
                        for chunk in to_close.chunks(cfg.chunk_size) {
                            let mut bs = oci_conn.batch(&sql, chunk.len()).build()?;
                            for key in chunk {
                                let val: Option<&str> = Some(key.as_str());
                                bs.append_row(&[&val as &dyn oracle::sql_type::ToSql])?;
                            }
                            bs.execute()?;
                        }
                        oci_conn.commit()?;
                    }
                    Ok(())
                }).await??;
            }
            Ok(())
        })
    }
}

fn oracle_fetch_current(conn: &oracle::Connection, schema: &str, table: &str, key_col: &str, keys: &[String], is_current: &str) -> anyhow::Result<CurrentRows> {
    if keys.is_empty() { return Ok(HashMap::new()); }
    let full_table = oracle_qualified_table(schema, table);
    let mut result = HashMap::new();
    for chunk in keys.chunks(999) {
        let placeholders = (1..=chunk.len()).map(|i| format!(":{i}")).collect::<Vec<_>>().join(", ");
        let sql = format!("SELECT * FROM {full_table} WHERE {ic} = 1 AND {kc} IN ({placeholders})",
            ic = oracle_ident(is_current), kc = oracle_ident(key_col));
        let bind_refs: Vec<&dyn oracle::sql_type::ToSql> = chunk.iter().map(|k| k as &dyn oracle::sql_type::ToSql).collect();
        let rows = conn.query(&sql, &bind_refs)?;
        for row_result in rows {
            let row = row_result?;
            let col_infos = row.column_info();
            let key_str: String = row.get::<&str, Option<i64>>(key_col).ok().flatten().map(|v| v.to_string())
                .or_else(|| row.get::<&str, Option<String>>(key_col).ok().flatten()).unwrap_or_default();
            let col_map: HashMap<String, Option<String>> = col_infos.iter().enumerate().map(|(i, ci)| {
                let val = row.get::<usize, Option<i64>>(i).ok().flatten().map(|v| v.to_string())
                    .or_else(|| row.get::<usize, Option<f64>>(i).ok().flatten().map(|v| v.to_string()))
                    .or_else(|| row.get::<usize, Option<String>>(i).ok().flatten());
                (ci.name().to_lowercase(), val)
            }).collect();
            result.insert(key_str, col_map);
        }
    }
    Ok(result)
}

fn oracle_fetch_all_current_keys(conn: &oracle::Connection, schema: &str, table: &str, key_col: &str, is_current: &str) -> anyhow::Result<Vec<String>> {
    let full_table = oracle_qualified_table(schema, table);
    let sql = format!("SELECT {kc} FROM {full_table} WHERE {ic} = 1",
        kc = oracle_ident(key_col), ic = oracle_ident(is_current));
    let rows = conn.query(&sql, &[])?;
    let mut keys = Vec::new();
    for row_result in rows {
        let row = row_result?;
        let key = row.get::<&str, Option<i64>>(key_col).ok().flatten().map(|v| v.to_string())
            .or_else(|| row.get::<&str, Option<String>>(key_col).ok().flatten())
            .unwrap_or_default();
        keys.push(key);
    }
    Ok(keys)
}