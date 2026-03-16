//! Databricks REST API SCD Type 2 sink — Delta Lake UPDATE + INSERT via
//! SQL Statement Execution API.

use std::collections::HashMap;
use std::collections::HashSet;
use arrow::record_batch::RecordBatch;

use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::{Scd2ColumnNames, Scd2Stats};
use potato_etl_common::db::common::{CurrentRows, Scd2Config, compute_scd2_decision};
use potato_etl_common::db::traits::Scd2Builder;
use potato_etl_common::util::arrow::{extract_scd2_key_strings, record_batch_to_string_rows};
use crate::conn::{backtick, dbx_full_table, DatabricksConnParams};
use super::StatementClient;

pub struct DatabricksApiScd2 {
    api: StatementClient,
    pub cfg: Scd2Config,
    init_sql_done: bool,
    /// Keys seen across all batches — only populated when `close_missing` is true.
    seen_keys: HashSet<String>,
}

impl DatabricksApiScd2 {
    pub fn new(params: DatabricksConnParams) -> anyhow::Result<Self> {
        let api = StatementClient::new(params)?;
        Ok(Self { api, cfg: Scd2Config::new(""), init_sql_done: false, seen_keys: HashSet::new() })
    }

    async fn fetch_current(
        &self,
        table: &str,
        key_vals: &[String],
    ) -> anyhow::Result<CurrentRows> {
        if key_vals.is_empty() {
            return Ok(HashMap::new());
        }
        let mut result: HashMap<String, HashMap<String, Option<String>>> = HashMap::new();
        for chunk in key_vals.chunks(500) {
            let keys_sql = chunk
                .iter()
                .map(|k| format!("'{}'", k.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT * FROM {table} WHERE {} = true AND {} IN ({keys_sql})",
                backtick(self.cfg.col_names.is_current.as_str()),
                backtick(&self.cfg.key_col),
            );
            let batches = self.api.execute_statement(&sql).await?;
            for batch in batches {
                let key_idx = batch.schema().index_of(&self.cfg.key_col)?;
                let key_strings = extract_scd2_key_strings(&batch, key_idx)?;
                let rows = record_batch_to_string_rows(&batch);
                for (row_idx, row_vals) in rows.into_iter().enumerate() {
                    let mut col_map: HashMap<String, Option<String>> = HashMap::new();
                    for (field, val) in batch.schema().fields().iter().zip(row_vals.into_iter()) {
                        col_map.insert(field.name().clone(), val);
                    }
                    result.insert(key_strings[row_idx].clone(), col_map);
                }
            }
        }
        Ok(result)
    }

    fn effective_full_table(&self) -> String {
        let eff = if !self.cfg.schema_name.is_empty() {
            &self.cfg.schema_name
        } else {
            self.api.params.schema.as_deref().unwrap_or("")
        };
        dbx_full_table(self.api.params.catalog.as_deref(), eff, &self.cfg.table)
    }
}

impl Scd2Builder for DatabricksApiScd2 {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.table(t); self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.schema(s); self }
    fn key(mut self: Box<Self>, col: String) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.key(col); self }
    fn track(mut self: Box<Self>, cols: Vec<String>) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.track(cols); self }
    fn col_names(mut self: Box<Self>, n: Scd2ColumnNames) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.col_names(n); self }
    fn close_missing(mut self: Box<Self>, close: bool) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.close_missing(close); self }
    fn chunk_size(mut self: Box<Self>, n: usize) -> Box<dyn Scd2Builder> { self.cfg = self.cfg.chunk_size(n); self }
    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn Scd2Builder> {
        if !opts.init_sql.is_empty() {
            self.api.params.init_sql = opts.init_sql.clone();
        }
        self
    }

    fn ddl_params(&self) -> (String, Option<String>, String, Scd2ColumnNames, String) {
        (
            self.cfg.table.clone(),
            if self.cfg.schema_name.is_empty() { None } else { Some(self.cfg.schema_name.clone()) },
            self.cfg.key_col.clone(),
            self.cfg.col_names.clone(),
            format!(
                "databricks://{}/sql/1.0/warehouses/{}",
                self.api.params.host, self.api.params.warehouse_id,
            ),
        )
    }

    fn execute_ddl<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move { self.api.execute_dml(sql).await })
    }

    fn write<'a>(
        &'a mut self,
        batch: RecordBatch,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Scd2Stats>> + Send + 'a>> {
        Box::pin(async move {
            if batch.num_rows() == 0 {
                return Ok(Scd2Stats::default());
            }

            // Execute init_sql on first write.
            if !self.init_sql_done {
                self.init_sql_done = true;
                self.api.execute_init_sql().await?;
            }

            let schema = batch.schema();
            let key_idx = schema.index_of(&self.cfg.key_col)?;
            let key_vals = extract_scd2_key_strings(&batch, key_idx)?;

            // Track seen keys for close_missing mode.
            if self.cfg.close_missing {
                self.seen_keys.extend(key_vals.iter().cloned());
            }

            let full_table = self.effective_full_table();
            let cn = &self.cfg.col_names;
            let existing = self.fetch_current(&full_table, &key_vals).await?;
            let decision = compute_scd2_decision(&batch, &self.cfg, &existing)?;

            // Close existing current rows that are being superseded.
            for chunk in decision.to_close.chunks(self.cfg.chunk_size) {
                let keys_sql = chunk
                    .iter()
                    .map(|k| format!("'{}'", k.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "UPDATE {full_table} SET {} = current_timestamp(), {} = false \
                     WHERE {} = true AND {} IN ({keys_sql})",
                    backtick(cn.valid_to.as_str()),
                    backtick(cn.is_current.as_str()),
                    backtick(cn.is_current.as_str()),
                    backtick(&self.cfg.key_col),
                );
                self.api.execute_dml(&sql).await?;
            }

            // Insert new current rows.
            if !decision.to_insert.is_empty() {
                let cols_with_scd = {
                    let mut v: Vec<String> = decision.insert_cols.iter().map(|c| backtick(c)).collect();
                    v.push(backtick(cn.valid_from.as_str()));
                    v.push(backtick(cn.valid_to.as_str()));
                    v.push(backtick(cn.is_current.as_str()));
                    v.join(", ")
                };
                for chunk in decision.to_insert.chunks(self.cfg.chunk_size) {
                    let values: Vec<String> = chunk.iter().map(|row| {
                        let mut vals: Vec<String> = decision.col_names_vec.iter().zip(row.iter())
                            .filter(|(n, _)| !decision.scd_meta.iter().any(|m| m == n.as_str()))
                            .map(|(_, v)| format!("'{}'", v.as_deref().unwrap_or("").replace('\'', "''")))
                            .collect();
                        vals.push("current_timestamp()".into());
                        vals.push("NULL".into());
                        vals.push("true".into());
                        format!("({})", vals.join(", "))
                    }).collect();
                    self.api.execute_dml(&format!(
                        "INSERT INTO {full_table} ({cols_with_scd}) VALUES {}",
                        values.join(", ")
                    )).await?;
                }
            }
            Ok(decision.stats)
        })
    }

    fn flush<'a>(
        &'a mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // ── Close-missing: expire current rows whose key was never seen ──
            if self.cfg.close_missing && !self.seen_keys.is_empty() {
                let full_table = self.effective_full_table();
                let cn = &self.cfg.col_names;

                // 1. Fetch all current keys.
                let sql = format!(
                    "SELECT {} FROM {full_table} WHERE {} = true",
                    backtick(&self.cfg.key_col),
                    backtick(cn.is_current.as_str()),
                );
                let batches = self.api.execute_statement(&sql).await?;
                let mut all_current_keys = Vec::new();
                for batch in batches {
                    let key_idx = batch.schema().index_of(&self.cfg.key_col)?;
                    let keys = extract_scd2_key_strings(&batch, key_idx)?;
                    all_current_keys.extend(keys);
                }

                // 2. Compute missing keys.
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
                        let keys_sql = chunk.iter()
                            .map(|k| format!("'{}'", k.replace('\'', "''")))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = format!(
                            "UPDATE {full_table} SET {} = current_timestamp(), {} = false \
                             WHERE {} = true AND {} IN ({keys_sql})",
                            backtick(cn.valid_to.as_str()),
                            backtick(cn.is_current.as_str()),
                            backtick(cn.is_current.as_str()),
                            backtick(&self.cfg.key_col),
                        );
                        self.api.execute_dml(&sql).await?;
                    }
                }

                self.seen_keys.clear();
            }
            Ok(())
        })
    }
}