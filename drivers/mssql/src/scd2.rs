//! MSSQL SCD Type 2 sink — `MssqlScd2Sink`.

use std::collections::HashMap;
use std::collections::HashSet;

use arrow::record_batch::RecordBatch;

use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::{Scd2ColumnNames, Scd2Stats};
use potato_etl_common::db::common::{Scd2Config, compute_scd2_decision};
use potato_etl_common::db::traits::Scd2Builder;
use potato_etl_common::util::arrow::extract_scd2_key_strings;
use crate::util::{MssqlClient, MssqlConnParams};

// ── MssqlScd2Sink ─────────────────────────────────────────────────────────────

pub struct MssqlScd2Sink {
    params:          MssqlConnParams,
    pub cfg:         Scd2Config,
    client:          Option<MssqlClient>,
    /// Keys seen across all batches — only populated when `close_missing` is true.
    seen_keys: HashSet<String>,
}

impl MssqlScd2Sink {
    pub fn new(conn_str: &str) -> anyhow::Result<Self> {
        Ok(Self {
            params:          MssqlConnParams::parse(conn_str)?,
            cfg:             Scd2Config::new("dbo"),
            client:          None,
            seen_keys:       HashSet::new(),
        })
    }

    async fn client(&mut self) -> anyhow::Result<&mut MssqlClient> {
        if self.client.is_none() {
            self.client = Some(self.params.connect().await?);
        }
        Ok(self.client.as_mut().unwrap())
    }

    async fn write_impl(&mut self, batch: RecordBatch) -> anyhow::Result<Scd2Stats> {
        let schema    = batch.schema();
        let key_idx   = schema.index_of(&self.cfg.key_col)?;
        let key_vals  = extract_scd2_key_strings(&batch, key_idx)?;

        // Track seen keys for close_missing mode.
        if self.cfg.close_missing {
            self.seen_keys.extend(key_vals.iter().cloned());
        }

        let full_table = format!("[{}].[{}]", self.cfg.schema_name, self.cfg.table);

        let key_col        = self.cfg.key_col.clone();
        let cn             = self.cfg.col_names.clone();
        let cfg_schema     = self.cfg.schema_name.clone();
        let cfg_table      = self.cfg.table.clone();
        let cfg            = self.cfg.clone();
        let chunk_size     = self.cfg.chunk_size;

        let client = self.client().await?;

        client.simple_query("BEGIN TRANSACTION").await?.into_results().await?;

        let existing = mssql_fetch_current(
            client, &cfg_schema, &cfg_table, &key_col, &key_vals, &cn.is_current,
        ).await?;

        let decision = compute_scd2_decision(&batch, &cfg, &existing)?;

        // Close changed rows.
        for chunk in decision.to_close.chunks(chunk_size) {
            let params: Vec<Option<&str>> = chunk.iter().map(|k| Some(k.as_str())).collect();
            let phs = (1..=chunk.len()).map(|i| format!("@P{i}")).collect::<Vec<_>>().join(", ");
            let sql = format!(
                "UPDATE {full_table} \
                 SET [{valid_to}] = SYSDATETIMEOFFSET(), [{is_current}] = 0 \
                 WHERE [{is_current}] = 1 AND [{key_col}] IN ({phs})",
                valid_to   = cn.valid_to,
                is_current = cn.is_current,
            );
            let refs: Vec<&dyn tiberius::ToSql> = params.iter()
                .map(|v| v as &dyn tiberius::ToSql).collect();
            client.execute(&sql, &refs).await?;
        }

        // Insert new versions.
        if !decision.to_insert.is_empty() {
            let cols_sql   = decision.insert_cols.iter().map(|c| format!("[{c}]")).collect::<Vec<_>>().join(", ");
            let chunk_size = (2_100 / (decision.insert_cols.len() + 1).max(1))
                .min(chunk_size)
                .max(1);

            for chunk in decision.to_insert.chunks(chunk_size) {
                let phs: String = chunk.iter().enumerate().map(|(ri, _)| {
                    let ps = (0..decision.insert_cols.len())
                        .map(|ci| format!("@P{}", ri * decision.insert_cols.len() + ci + 1))
                        .collect::<Vec<_>>().join(", ");
                    format!("({ps}, SYSDATETIMEOFFSET(), NULL, 1)")
                }).collect::<Vec<_>>().join(", ");

                let sql = format!(
                    "INSERT INTO {full_table} ({cols_sql}, [{valid_from}], [{valid_to}], [{is_current}]) VALUES {phs}",
                    valid_from = cn.valid_from,
                    valid_to   = cn.valid_to,
                    is_current = cn.is_current,
                );

                let flat: Vec<Option<&str>> = chunk.iter().flat_map(|row| {
                    decision.col_names_vec.iter().zip(row.iter())
                        .filter(|(n, _)| !decision.scd_meta.iter().any(|m| m == n.as_str()))
                        .map(|(_, v)| v.as_deref())
                }).collect();
                let refs: Vec<&dyn tiberius::ToSql> = flat.iter()
                    .map(|v| v as &dyn tiberius::ToSql).collect();
                client.execute(&sql, &refs).await?;
            }
        }

        client.simple_query("COMMIT").await?.into_results().await?;

        Ok(decision.stats)
    }
}

impl Scd2Builder for MssqlScd2Sink {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.table(t); self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.schema(s); self }
    fn key(mut self: Box<Self>, col: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.key(col); self }
    fn track(mut self: Box<Self>, cols: Vec<String>) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.track(cols); self }
    fn col_names(mut self: Box<Self>, n: Scd2ColumnNames) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.col_names(n); self }
    fn close_missing(mut self: Box<Self>, close: bool) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.close_missing(close); self }
    fn chunk_size(mut self: Box<Self>, n: usize) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.chunk_size(n); self }
    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn Scd2Builder> {
        if let Some(ref ms) = opts.mssql {
            if let Some(n) = ms.batch_size {
                self.cfg.chunk_size = n.max(1);
            }
            if !ms.init_sql.is_empty() {
                self.params.init_sql = ms.init_sql.clone();
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
            format!("mssql://localhost/{}", self.cfg.table),
        )
    }

    fn execute_ddl<'a>(&'a mut self, sql: &'a str) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let client = self.client().await?;
            client.simple_query(sql).await?.into_results().await
                .map_err(|e| anyhow::anyhow!("DDL error: {e:#}\nSQL: {sql}"))?;
            Ok(())
        })
    }

    fn write<'a>(&'a mut self, batch: RecordBatch) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Scd2Stats>> + Send + 'a>> {
        Box::pin(async move {
            if batch.num_rows() == 0 { return Ok(Scd2Stats::default()); }

            let result = self.write_impl(batch).await;
            if result.is_err() {
                tracing::warn!("MssqlScd2Sink: write error — rolling back transaction");
                if let Some(ref mut c) = self.client {
                    if let Ok(s) = c.simple_query("ROLLBACK TRANSACTION").await {
                        let _ = s.into_results().await;
                    }
                }
                self.client = None;
            }
            result
        })
    }

    fn flush<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // ── Close-missing: expire current rows whose key was never seen ──
            if self.cfg.close_missing && !self.seen_keys.is_empty() {
                let cfg_schema = self.cfg.schema_name.clone();
                let cfg_table  = self.cfg.table.clone();
                let key_col    = self.cfg.key_col.clone();
                let cn         = self.cfg.col_names.clone();
                let chunk_size = self.cfg.chunk_size;
                let full_table = format!("[{cfg_schema}].[{cfg_table}]");

                let client = self.client().await?;
                let all_current_keys = mssql_fetch_all_current_keys(
                    client, &cfg_schema, &cfg_table, &key_col, &cn.is_current,
                ).await?;

                let to_close: Vec<String> = all_current_keys.into_iter()
                    .filter(|k| !self.seen_keys.contains(k))
                    .collect();

                if !to_close.is_empty() {
                    tracing::info!(
                        table = %cfg_table,
                        count = to_close.len(),
                        "close_missing: expiring {} current rows not seen in incoming data",
                        to_close.len(),
                    );
                    let client = self.client().await?;
                    for chunk in to_close.chunks(chunk_size) {
                        let params: Vec<Option<&str>> = chunk.iter().map(|k| Some(k.as_str())).collect();
                        let phs = (1..=chunk.len()).map(|i| format!("@P{i}")).collect::<Vec<_>>().join(", ");
                        let sql = format!(
                            "UPDATE {full_table} \
                             SET [{valid_to}] = SYSDATETIMEOFFSET(), [{is_current}] = 0 \
                             WHERE [{is_current}] = 1 AND [{key_col}] IN ({phs})",
                            valid_to   = cn.valid_to,
                            is_current = cn.is_current,
                        );
                        let refs: Vec<&dyn tiberius::ToSql> = params.iter()
                            .map(|v| v as &dyn tiberius::ToSql).collect();
                        client.execute(&sql, &refs).await?;
                    }
                }
                self.seen_keys.clear();
            }

            self.client = None;
            Ok(())
        })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn mssql_fetch_current(
    client:     &mut MssqlClient,
    schema:     &str,
    table:      &str,
    key_col:    &str,
    keys:       &[String],
    is_current: &str,
) -> anyhow::Result<HashMap<String, HashMap<String, Option<String>>>> {
    if keys.is_empty() { return Ok(HashMap::new()); }

    let mut result: HashMap<String, HashMap<String, Option<String>>> = HashMap::new();

    for chunk in keys.chunks(500) {
        let params: Vec<Option<&str>> = chunk.iter().map(|k| Some(k.as_str())).collect();
        let phs = (1..=chunk.len()).map(|i| format!("@P{i}")).collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT * FROM [{schema}].[{table}] WHERE [{is_current}] = 1 AND [{key_col}] IN ({phs})"
        );
        let refs: Vec<&dyn tiberius::ToSql> = params.iter()
            .map(|v| v as &dyn tiberius::ToSql).collect();

        let rows = client.query(&sql, &refs).await?.into_first_result().await?;
        for row in &rows {
            let key_str = row.try_get::<&str, _>(key_col).ok().flatten().map(|v| v.to_string())
                .or_else(|| row.try_get::<i64, _>(key_col).ok().flatten().map(|v| v.to_string()))
                .or_else(|| row.try_get::<rust_decimal::Decimal, _>(key_col).ok().flatten().map(|d| d.to_string()))
                .unwrap_or_default();

            let mut col_map: HashMap<String, Option<String>> = HashMap::new();
            for col in row.columns() {
                let name = col.name().to_string();
                let val: Option<String> =
                    row.try_get::<&str, _>(col.name()).ok().flatten().map(|s| s.to_string())
                    .or_else(|| row.try_get::<rust_decimal::Decimal, _>(col.name()).ok().flatten().map(|d| d.to_string()))
                    .or_else(|| row.try_get::<chrono::DateTime<chrono::FixedOffset>, _>(col.name()).ok().flatten().map(|d| d.to_rfc3339()))
                    .or_else(|| row.try_get::<chrono::NaiveDateTime, _>(col.name()).ok().flatten().map(|d| d.to_string()))
                    .or_else(|| row.try_get::<chrono::NaiveDate, _>(col.name()).ok().flatten().map(|d| d.to_string()))
                    .or_else(|| row.try_get::<chrono::NaiveTime, _>(col.name()).ok().flatten().map(|t| t.to_string()))
                    .or_else(|| row.try_get::<i64,  _>(col.name()).ok().flatten().map(|v| v.to_string()))
                    .or_else(|| row.try_get::<f64,  _>(col.name()).ok().flatten().map(|v| v.to_string()))
                    .or_else(|| row.try_get::<bool, _>(col.name()).ok().flatten().map(|v| v.to_string()));
                col_map.insert(name, val);
            }
            result.insert(key_str, col_map);
        }
    }

    Ok(result)
}

async fn mssql_fetch_all_current_keys(
    client:     &mut MssqlClient,
    schema:     &str,
    table:      &str,
    key_col:    &str,
    is_current: &str,
) -> anyhow::Result<Vec<String>> {
    let sql = format!(
        "SELECT [{key_col}] FROM [{schema}].[{table}] WHERE [{is_current}] = 1"
    );
    let rows = client.simple_query(&sql).await?.into_first_result().await?;
    let mut keys = Vec::with_capacity(rows.len());
    for row in &rows {
        let key = row.try_get::<&str, _>(key_col).ok().flatten().map(|v| v.to_string())
            .or_else(|| row.try_get::<i64, _>(key_col).ok().flatten().map(|v| v.to_string()))
            .or_else(|| row.try_get::<rust_decimal::Decimal, _>(key_col).ok().flatten().map(|d| d.to_string()))
            .unwrap_or_default();
        keys.push(key);
    }
    Ok(keys)
}