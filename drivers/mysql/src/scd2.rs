//! MySQL SCD Type 2 sink — `MySqlScd2Sink`.

use std::collections::HashMap;
use std::collections::HashSet;

use arrow::record_batch::RecordBatch;
use sqlx::mysql::{MySqlPool, MySqlRow};
use sqlx::{Column, Row};

use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::{Scd2ColumnNames, Scd2Stats};
use potato_etl_common::db::common::{CurrentRows, Scd2Config, compute_scd2_decision};
use potato_etl_common::db::traits::Scd2Builder;
use potato_etl_common::util::arrow::extract_scd2_key_strings;
use crate::util::{backtick, mysql_full_table};

pub struct MySqlScd2Sink {
    conn_str: String,
    pub cfg:  Scd2Config,
    pool:     Option<MySqlPool>,
    /// SQL statements executed on every new connection in the pool.
    init_sql: Vec<String>,
    /// Keys seen across all batches — only populated when `close_missing` is true.
    seen_keys: HashSet<String>,
}

impl MySqlScd2Sink {
    pub fn new(conn_str: &str) -> Self {
        Self { conn_str: conn_str.to_string(), cfg: Scd2Config::new(""), pool: None, init_sql: Vec::new(), seen_keys: HashSet::new() }
    }

    async fn pool(&mut self) -> anyhow::Result<&MySqlPool> {
        if self.pool.is_none() {
            self.pool = Some(crate::util::mysql_pool_with_init_sql(&self.conn_str, 2, &self.init_sql).await?);
        }
        Ok(self.pool.as_ref().unwrap())
    }
}

impl Scd2Builder for MySqlScd2Sink {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.table(t); self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.schema(s); self }
    fn key(mut self: Box<Self>, col: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.key(col); self }
    fn track(mut self: Box<Self>, cols: Vec<String>) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.track(cols); self }
    fn col_names(mut self: Box<Self>, n: Scd2ColumnNames) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.col_names(n); self }
    fn close_missing(mut self: Box<Self>, close: bool) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.close_missing(close); self }
    fn chunk_size(mut self: Box<Self>, n: usize) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.chunk_size(n); self }
    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn Scd2Builder> {
        if let Some(ref my) = opts.mysql {
            if !my.init_sql.is_empty() {
                self.init_sql = my.init_sql.clone();
            }
        }
        self
    }

    fn ddl_params(&self) -> (String, Option<String>, String, Scd2ColumnNames, String) {
        (self.cfg.table.clone(), None, self.cfg.key_col.clone(), self.cfg.col_names.clone(), self.conn_str.clone())
    }

    fn execute_ddl<'a>(&'a mut self, sql: &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let pool = self.pool().await?;
            sqlx::query(sql).execute(pool).await.map_err(|e| anyhow::anyhow!("DDL error: {e:#}\nSQL: {sql}"))?;
            Ok(())
        })
    }

    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Scd2Stats>> + Send + 'a>> {
        Box::pin(async move {
            if batch.num_rows() == 0 { return Ok(Scd2Stats::default()); }
            let pool       = self.pool().await?.clone();
            let schema     = batch.schema();
            let key_idx    = schema.index_of(&self.cfg.key_col)?;
            let key_vals   = extract_scd2_key_strings(&batch, key_idx)?;
            let full_table = mysql_full_table(&self.cfg.schema_name, &self.cfg.table);
            let key_col    = self.cfg.key_col.clone();
            let cn         = &self.cfg.col_names;

            // Track seen keys for close_missing mode.
            if self.cfg.close_missing {
                self.seen_keys.extend(key_vals.iter().cloned());
            }

            let existing = mysql_fetch_current(&pool, &full_table, &key_col, &key_vals, cn.is_current.as_str()).await?;
            let decision = compute_scd2_decision(&batch, &self.cfg, &existing)?;

            for chunk in decision.to_close.chunks(self.cfg.chunk_size) {
                let mut qb = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
                    "UPDATE {full_table} SET {} = NOW(6), {} = 0 WHERE {} = 1 AND {} IN (",
                    backtick(cn.valid_to.as_str()), backtick(cn.is_current.as_str()),
                    backtick(cn.is_current.as_str()), backtick(&key_col)
                ));
                let mut sep = qb.separated(", ");
                for k in chunk { sep.push_bind(k.as_str()); }
                qb.push(")");
                qb.build().execute(&pool).await?;
            }

            if !decision.to_insert.is_empty() {
                let cols_with_scd = {
                    let mut parts: Vec<String> = decision.insert_cols.iter().map(|c| backtick(c)).collect();
                    parts.push(backtick(cn.valid_from.as_str()));
                    parts.push(backtick(cn.valid_to.as_str()));
                    parts.push(backtick(cn.is_current.as_str()));
                    parts.join(", ")
                };
                for chunk in decision.to_insert.chunks(self.cfg.chunk_size) {
                    let values: Vec<String> = chunk.iter().map(|row| {
                        let mut parts: Vec<String> = decision.col_names_vec.iter().zip(row.iter())
                            .filter(|(n, _)| !decision.scd_meta.iter().any(|m| m == n.as_str()))
                            .map(|(_, v)| match v.as_deref() {
                                None    => "NULL".to_string(),
                                Some(s) => format!("'{}'", s.replace('\'', "''")),
                            }).collect();
                        parts.push("NOW(6)".to_string());
                        parts.push("NULL".to_string());
                        parts.push("1".to_string());
                        format!("({})", parts.join(", "))
                    }).collect();
                    let sql = format!("INSERT INTO {full_table} ({cols_with_scd}) VALUES {}", values.join(", "));
                    sqlx::query(&sql).execute(&pool).await?;
                }
            }
            Ok(decision.stats)
        })
    }

    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // ── Close-missing: expire current rows whose key was never seen ──
            if self.cfg.close_missing && !self.seen_keys.is_empty() {
                let pool = self.pool().await?.clone();
                let full_table = mysql_full_table(&self.cfg.schema_name, &self.cfg.table);
                let cn = &self.cfg.col_names;

                let all_current_keys = mysql_fetch_all_current_keys(
                    &pool, &full_table, &self.cfg.key_col, &cn.is_current,
                ).await?;

                let to_close: Vec<String> = all_current_keys.into_iter()
                    .filter(|k| !self.seen_keys.contains(k))
                    .collect();

                if !to_close.is_empty() {
                    tracing::info!(
                        table = %self.cfg.table,
                        count = to_close.len(),
                        "close_missing: expiring {} current rows not seen in incoming data",
                        to_close.len(),
                    );
                    for chunk in to_close.chunks(self.cfg.chunk_size) {
                        let mut qb = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
                            "UPDATE {full_table} SET {} = NOW(6), {} = 0 WHERE {} = 1 AND {} IN (",
                            backtick(cn.valid_to.as_str()), backtick(cn.is_current.as_str()),
                            backtick(cn.is_current.as_str()), backtick(&self.cfg.key_col),
                        ));
                        let mut sep = qb.separated(", ");
                        for k in chunk { sep.push_bind(k.as_str()); }
                        qb.push(")");
                        qb.build().execute(&pool).await?;
                    }
                }
                self.seen_keys.clear();
            }

            if let Some(pool) = self.pool.take() { pool.close().await; }
            Ok(())
        })
    }
}

async fn mysql_fetch_current(pool: &MySqlPool, table: &str, key_col: &str, keys: &[String], is_current: &str) -> anyhow::Result<CurrentRows> {
    if keys.is_empty() { return Ok(HashMap::new()); }
    let mut result: HashMap<String, HashMap<String, Option<String>>> = HashMap::new();
    for chunk in keys.chunks(500) {
        let mut qb = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
            "SELECT * FROM {table} WHERE {} = 1 AND {} IN (", backtick(is_current), backtick(key_col)
        ));
        let mut sep = qb.separated(", ");
        for k in chunk { sep.push_bind(k.as_str()); }
        qb.push(")");
        let rows: Vec<MySqlRow> = qb.build().fetch_all(pool).await?;
        for row in &rows {
            let key_str = row.try_get::<i64, _>(key_col).ok().map(|v| v.to_string())
                .or_else(|| row.try_get::<i32, _>(key_col).ok().map(|v| v.to_string()))
                .or_else(|| row.try_get::<String, _>(key_col).ok())
                .unwrap_or_default();
            let mut col_map: HashMap<String, Option<String>> = HashMap::new();
            for col in row.columns() {
                let name = col.name().to_string();
                let val = row.try_get::<i64, _>(col.name()).ok().map(|v| v.to_string())
                    .or_else(|| row.try_get::<f64, _>(col.name()).ok().map(|v| v.to_string()))
                    .or_else(|| row.try_get::<bool, _>(col.name()).ok().map(|v| v.to_string()))
                    .or_else(|| row.try_get::<String, _>(col.name()).ok());
                col_map.insert(name, val);
            }
            result.insert(key_str, col_map);
        }
    }
    Ok(result)
}

async fn mysql_fetch_all_current_keys(pool: &MySqlPool, table: &str, key_col: &str, is_current: &str) -> anyhow::Result<Vec<String>> {
    let sql = format!("SELECT {} FROM {table} WHERE {} = 1", backtick(key_col), backtick(is_current));
    let rows: Vec<MySqlRow> = sqlx::query(&sql).fetch_all(pool).await?;
    let mut keys = Vec::with_capacity(rows.len());
    for row in &rows {
        let key = row.try_get::<i64, _>(key_col).ok().map(|v| v.to_string())
            .or_else(|| row.try_get::<i32, _>(key_col).ok().map(|v| v.to_string()))
            .or_else(|| row.try_get::<String, _>(key_col).ok())
            .unwrap_or_default();
        keys.push(key);
    }
    Ok(keys)
}