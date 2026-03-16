//! Postgres SCD Type 2 sink — `PgScd2Sink`.
//!
//! The SCD2 diff algorithm is delegated to
//! `potato_etl_common::db::common::compute_scd2_decision` — this module
//! only contains Postgres-specific SQL execution.

use std::collections::HashMap;
use std::collections::HashSet;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use sqlx::{Column, Row, TypeInfo};
use sqlx::PgPool;

use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::{Scd2ColumnNames, Scd2Stats};
use potato_etl_common::db::common::{CurrentRows, Scd2Config, compute_scd2_decision};
use potato_etl_common::db::traits::Scd2Builder;
use crate::util::pg_col_to_string_opt;

// ── PgScd2Sink ────────────────────────────────────────────────────────────────

pub struct PgScd2Sink {
    pub conn_str: String,
    pub cfg:      Scd2Config,
    pool: Option<PgPool>,
    /// SQL statements executed on every new connection in the pool.
    init_sql: Vec<String>,
    /// Keys seen across all batches — only populated when `close_missing` is true.
    seen_keys: HashSet<String>,
    /// Whether the key column uses an integer type in the Arrow schema.
    /// Cached on first write, used in flush() for close-missing queries.
    key_is_integer: Option<bool>,
}

impl PgScd2Sink {
    pub fn new(conn_str: impl Into<String>) -> Self {
        Self {
            conn_str: conn_str.into(),
            cfg:      Scd2Config::new("public"),
            pool:     None,
            init_sql: Vec::new(),
            seen_keys: HashSet::new(),
            key_is_integer: None,
        }
    }

    /// Ensure the connection pool is initialized.
    async fn ensure_pool(&mut self) -> anyhow::Result<&PgPool> {
        if self.pool.is_none() {
            self.pool = Some(
                crate::util::pg_pool_with_init_sql(&self.conn_str, 3, &self.init_sql).await?,
            );
        }
        Ok(self.pool.as_ref().unwrap())
    }
}

impl Scd2Builder for PgScd2Sink {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn Scd2Builder> {
        self.cfg = self.cfg.table(t); self
    }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn Scd2Builder> {
        self.cfg = self.cfg.schema(s); self
    }
    fn key(mut self: Box<Self>, col: String) -> Box<dyn Scd2Builder> {
        self.cfg = self.cfg.key(col); self
    }
    fn track(mut self: Box<Self>, cols: Vec<String>) -> Box<dyn Scd2Builder> {
        self.cfg = self.cfg.track(cols); self
    }
    fn col_names(mut self: Box<Self>, n: Scd2ColumnNames) -> Box<dyn Scd2Builder> {
        self.cfg = self.cfg.col_names(n); self
    }
    fn close_missing(mut self: Box<Self>, close: bool) -> Box<dyn Scd2Builder> {
        self.cfg = self.cfg.close_missing(close); self
    }
    fn chunk_size(mut self: Box<Self>, n: usize) -> Box<dyn Scd2Builder> {
        self.cfg = self.cfg.chunk_size(n); self
    }
    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn Scd2Builder> {
        if let Some(ref pg) = opts.postgres {
            if !pg.init_sql.is_empty() {
                self.init_sql = pg.init_sql.clone();
            }
        }
        self
    }

    fn ddl_params(&self) -> (String, Option<String>, String, Scd2ColumnNames, String) {
        (
            self.cfg.table.clone(),
            Some(self.cfg.schema_name.clone()),
            self.cfg.key_col.clone(),
            self.cfg.col_names.clone(),
            self.conn_str.clone(),
        )
    }

    fn execute_ddl<'a>(&'a mut self, sql: &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if self.pool.is_none() {
                self.pool = Some(
                    crate::util::pg_pool_with_init_sql(&self.conn_str, 2, &self.init_sql).await?,
                );
            }
            let pool = self.pool.as_ref().unwrap();
            sqlx::query(sql).execute(pool).await
                .map_err(|e| anyhow::anyhow!("DDL error: {e:#}\nSQL: {sql}"))?;
            Ok(())
        })
    }

    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Scd2Stats>> + Send + 'a>> {
        Box::pin(async move {
            if batch.num_rows() == 0 { return Ok(Scd2Stats::default()); }

            let pool = self.ensure_pool().await?.clone();
            let cn = &self.cfg.col_names;

            // Detect whether the key column is numeric in the Arrow schema.
            let key_idx = batch.schema().index_of(&self.cfg.key_col)?;
            let key_is_integer = matches!(
                batch.schema().field(key_idx).data_type(),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            );
            self.key_is_integer = Some(key_is_integer);

            // Fetch existing current rows from the database.
            let key_vals = potato_etl_common::util::arrow::extract_scd2_key_strings(
                &batch,
                key_idx,
            )?;

            // Track seen keys for close_missing mode.
            if self.cfg.close_missing {
                self.seen_keys.extend(key_vals.iter().cloned());
            }

            let mut txn = pool.begin().await?;
            let existing = fetch_current_rows(
                &mut txn, &self.cfg.schema_name, &self.cfg.table, &self.cfg.key_col,
                &key_vals, &cn.is_current, key_is_integer,
            ).await?;

            // Delegate the diff algorithm to the shared implementation.
            let decision = compute_scd2_decision(&batch, &self.cfg, &existing)?;

            // ── Close changed rows ───────────────────────────────────────────
            if !decision.to_close.is_empty() {
                for chunk in decision.to_close.chunks(self.cfg.chunk_size) {
                    let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(format!(
                        "UPDATE \"{}\".\"{}\"\
                         SET \"{}\" = now(), \"{}\" = false \
                         WHERE \"{}\" = true AND \"{}\" IN (",
                        self.cfg.schema_name, self.cfg.table,
                        cn.valid_to, cn.is_current, cn.is_current, self.cfg.key_col,
                    ));
                    let mut sep = qb.separated(", ");
                    for k in chunk {
                        if key_is_integer {
                            sep.push_bind(k.parse::<i64>().unwrap_or(0));
                        } else {
                            sep.push_bind(k.as_str());
                        }
                    }
                    qb.push(")");
                    qb.build().execute(&mut *txn).await?;
                }
            }

            // ── Insert new versions ──────────────────────────────────────────
            if !decision.to_insert.is_empty() {
                let cols_sql = decision.insert_cols.iter()
                    .map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
                let full_table = format!("\"{}\".\"{}\""  , self.cfg.schema_name, self.cfg.table);

                // Determine which column indices in col_names_vec correspond to
                // integer-typed key columns so we can bind them as i64.
                let key_col_positions: Vec<usize> = if key_is_integer {
                    decision.col_names_vec.iter().enumerate()
                        .filter(|(_, n)| *n == &self.cfg.key_col)
                        .filter(|(i, _)| {
                            // Only include if this position survives the non-scd filter
                            !decision.scd_meta.iter().any(|m| m == decision.col_names_vec[*i].as_str())
                        })
                        .map(|(i, _)| i)
                        .collect()
                } else {
                    vec![]
                };

                for chunk in decision.to_insert.chunks(500) {
                    let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(format!(
                        "INSERT INTO {full_table} ({cols_sql}, \"{}\", \"{}\", \"{}\") ",
                        cn.valid_from, cn.valid_to, cn.is_current,
                    ));
                    qb.push_values(chunk.iter(), |mut b, row| {
                        let non_scd: Vec<(usize, Option<&str>)> = decision.col_names_vec.iter()
                            .zip(row.iter())
                            .enumerate()
                            .filter(|(_, (n, _))| !decision.scd_meta.iter().any(|m| m == n.as_str()))
                            .map(|(orig_idx, (_, v))| (orig_idx, v.as_deref()))
                            .collect();
                        for (orig_idx, v) in non_scd {
                            if key_col_positions.contains(&orig_idx) {
                                // Bind integer keys as i64 so Postgres
                                // receives the correct wire type.
                                b.push_bind(v.and_then(|s| s.parse::<i64>().ok()));
                            } else {
                                b.push_bind(v);
                            }
                        }
                        b.push("now()");
                        b.push("NULL");
                        b.push_bind(true);
                    });
                    qb.build().execute(&mut *txn).await?;
                }
            }

            txn.commit().await?;
            tracing::debug!(
                table = %self.cfg.table,
                new = decision.stats.new_rows,
                updated = decision.stats.updated_rows,
                unchanged = decision.stats.unchanged_rows,
                "PgScd2Sink batch"
            );

            Ok(decision.stats)
        })
    }

    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // ── Close-missing: expire current rows whose key was never seen ──
            if self.cfg.close_missing && !self.seen_keys.is_empty() {
                let pool = self.ensure_pool().await?.clone();
                let cn = &self.cfg.col_names;
                let key_is_integer = self.key_is_integer.unwrap_or(false);

                // 1. Fetch ALL current keys from the database.
                let all_current_keys = fetch_all_current_keys(
                    &pool, &self.cfg.schema_name, &self.cfg.table,
                    &self.cfg.key_col, &cn.is_current,
                ).await?;

                // 2. Compute keys to close: current in DB but not seen in any batch.
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
                    // 3. Close in chunks.
                    for chunk in to_close.chunks(self.cfg.chunk_size) {
                        let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(format!(
                            "UPDATE \"{}\".\"{}\"\
                             SET \"{}\" = now(), \"{}\" = false \
                             WHERE \"{}\" = true AND \"{}\" IN (",
                            self.cfg.schema_name, self.cfg.table,
                            cn.valid_to, cn.is_current, cn.is_current, self.cfg.key_col,
                        ));
                        let mut sep = qb.separated(", ");
                        for k in chunk {
                            if key_is_integer {
                                sep.push_bind(k.parse::<i64>().unwrap_or(0));
                            } else {
                                sep.push_bind(k.as_str());
                            }
                        }
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

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn fetch_current_rows(
    txn:            &mut sqlx::Transaction<'static, sqlx::Postgres>,
    schema:         &str,
    table:          &str,
    key_col:        &str,
    keys:           &[String],
    is_current:     &str,
    key_is_integer: bool,
) -> anyhow::Result<CurrentRows> {
    if keys.is_empty() { return Ok(HashMap::new()); }
    let mut result = HashMap::new();

    for chunk in keys.chunks(500) {
        let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(format!(
            "SELECT * FROM \"{schema}\".\"{table}\" WHERE \"{key_col}\" IN ("
        ));
        let mut sep = qb.separated(", ");
        for k in chunk {
            if key_is_integer {
                sep.push_bind(k.parse::<i64>().unwrap_or(0));
            } else {
                sep.push_bind(k.as_str());
            }
        }
        qb.push(format!(") AND \"{}\" = true", is_current));

        let rows = qb.build().fetch_all(&mut **txn).await?;
        for row in &rows {
            let key_str = row.try_get::<i64, _>(key_col).map(|v| v.to_string())
                .or_else(|_| row.try_get::<i32, _>(key_col).map(|v| v.to_string()))
                .or_else(|_| row.try_get::<String, _>(key_col))?;

            let col_map: HashMap<String, Option<String>> = row.columns().iter()
                .map(|col| {
                    let val = pg_col_to_string_opt(row, col.ordinal(), col.type_info().name());
                    (col.name().to_string(), val)
                })
                .collect();
            result.insert(key_str, col_map);
        }
    }
    Ok(result)
}

/// Fetch all key values where `is_current = true` from the target table.
/// Used by close_missing to determine which keys exist in the DB but were
/// not seen in any incoming batch.
async fn fetch_all_current_keys(
    pool:       &PgPool,
    schema:     &str,
    table:      &str,
    key_col:    &str,
    is_current: &str,
) -> anyhow::Result<Vec<String>> {
    let sql = format!(
        "SELECT \"{key_col}\" FROM \"{schema}\".\"{table}\" WHERE \"{is_current}\" = true"
    );
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    let mut keys = Vec::with_capacity(rows.len());
    for row in &rows {
        let key = row.try_get::<i64, _>(key_col).map(|v| v.to_string())
            .or_else(|_| row.try_get::<i32, _>(key_col).map(|v| v.to_string()))
            .or_else(|_| row.try_get::<String, _>(key_col))?;
        keys.push(key);
    }
    Ok(keys)
}