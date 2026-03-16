//! Shared state and pure logic used by every database backend.
//!
//! Backends (`postgres`, `mssql`, `oracle`, `mysql`, `databricks`) each have
//! their own connection type, SQL dialect, and parameter-binding API — but the
//! write-mode/strategy state machine and the SCD2 diff algorithm are identical
//! across all of them.  This module centralises those pieces so they exist
//! exactly once.
//!
//! ## Consumers
//!
//! - **`SinkConfig`** — embed with `cfg: SinkConfig` in every `*WriteDB` struct.
//!   Delegate all builder calls (`table`, `schema`, `use_existing`, …) to `cfg`.
//!
//! - **`Scd2Config`** — embed with `cfg: Scd2Config` in every `*Scd2Sink` struct.
//!   Delegate all builder calls (`table`, `schema`, `key`, `track`, `col_names`)
//!   to `cfg`.
//!
//! - **`compute_scd2_decision`** — call after fetching current rows from the
//!   database.  Returns the `to_close` keys and `to_insert` rows that each
//!   backend must then write using its own SQL dialect.

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::{CreateTableMode, IdentifierCase};
use crate::config::DatabaseSchemaConfig;
use crate::db::{Scd2ColumnNames, Scd2Stats, TableMode, WriteStrategy};
use crate::util::arrow::{extract_scd2_key_strings, record_batch_to_string_rows};

pub mod alignment;
pub mod field_meta;
pub mod type_coercion;

// ── CurrentRows ───────────────────────────────────────────────────────────────

/// The in-memory snapshot of the current (is_current = true) SCD2 rows,
/// keyed by the natural business key value.
///
/// Each value is a `col_name → string_value` map; `None` represents SQL NULL.
pub type CurrentRows = HashMap<String, HashMap<String, Option<String>>>;

// ── SinkConfig ────────────────────────────────────────────────────────────────

/// Shared configuration for every database write sink.
///
/// Embed as `cfg: SinkConfig` in each backend's write struct.  All builder
/// methods return `SinkConfig` by value so callers can chain.
#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub table:          String,
    pub schema_name:    String,
    pub table_mode:     TableMode,
    pub write_strategy: WriteStrategy,
    /// Set to `true` after the first write so DDL is only applied once.
    pub table_prepared: bool,
    /// Column name case transformation.
    pub identifier_case: Option<IdentifierCase>,
    /// Optional DDL schema that includes DDL-only columns.
    pub ddl_schema:     Option<SchemaRef>,
    /// Optional unified database schema config with named indexes and constraints.
    pub database_schema_config: Option<DatabaseSchemaConfig>,
}

impl SinkConfig {
    /// Creates a new config with `table = ""` and the given default schema.
    pub fn new(default_schema: impl Into<String>) -> Self {
        Self {
            table:          String::new(),
            schema_name:    default_schema.into(),
            table_mode:     TableMode::UseExisting,
            write_strategy: WriteStrategy::Append,
            table_prepared: false,
            identifier_case: None,
            ddl_schema:     None,
            database_schema_config: None,
        }
    }

    pub fn table(mut self, t: impl Into<String>) -> Self {
        self.table = t.into();
        self
    }

    pub fn schema(mut self, s: impl Into<String>) -> Self {
        self.schema_name = s.into();
        self
    }

    // ── DDL mode ──────────────────────────────────────────────────────────────

    pub fn use_existing(mut self) -> Self {
        self.table_mode = TableMode::UseExisting;
        self
    }

    pub fn create_if_not_exists(mut self) -> Self {
        self.table_mode = TableMode::CreateIfNotExists;
        self
    }

    pub fn drop_and_replace(mut self) -> Self {
        self.table_mode = TableMode::DropAndReplace;
        self
    }

    /// Applies a [`CreateTableMode`] from the pipeline config.
    pub fn create_mode(self, mode: CreateTableMode) -> Self {
        match mode {
            CreateTableMode::Never       => self,
            CreateTableMode::IfNotExists => self.create_if_not_exists(),
            CreateTableMode::Replace     => self.drop_and_replace(),
        }
    }

    // ── Write strategy ────────────────────────────────────────────────────────

    pub fn insert(mut self) -> Self {
        self.write_strategy = WriteStrategy::Append;
        self
    }

    pub fn insert_ignore(mut self) -> Self {
        self.write_strategy = WriteStrategy::InsertIgnore;
        self
    }

    pub fn upsert(mut self) -> Self {
        self.write_strategy = WriteStrategy::Upsert;
        self
    }

    pub fn merge_delete(mut self) -> Self {
        self.write_strategy = WriteStrategy::MergeDelete;
        self
    }

    pub fn clear_and_insert(mut self) -> Self {
        self.write_strategy = WriteStrategy::Truncate;
        self
    }

    /// Generates post-create DDL statements for named indexes and constraints.
    pub fn named_ddl_post_create(&self, dialect: crate::schema::SqlDialect) -> Vec<String> {
        let Some(db_config) = &self.database_schema_config else { return vec![] };
        if db_config.indexes.is_empty() && db_config.constraints.is_empty() {
            return vec![];
        }

        let q = |ident: &str| dialect.quote(ident);
        let qualified = if self.schema_name.is_empty() {
            q(&self.table)
        } else {
            format!("{}.{}", q(&self.schema_name), q(&self.table))
        };

        let mut stmts = Vec::new();

        // ── Named indexes ─────────────────────────────────────────────────
        for (idx_name, idx_def) in &db_config.indexes {
            if idx_def.primary {
                let cols_quoted: Vec<String> = idx_def.columns.iter().map(|c| q(c)).collect();
                stmts.push(format!(
                    "ALTER TABLE {qualified} ADD CONSTRAINT {} PRIMARY KEY ({});",
                    q(idx_name),
                    cols_quoted.join(", "),
                ));
            } else {
                let col_refs: Vec<&str> = idx_def.columns.iter().map(|s| s.as_str()).collect();
                stmts.push(crate::schema::make_create_index(
                    idx_name,
                    &qualified,
                    &col_refs,
                    idx_def.unique,
                    dialect,
                ));
            }
        }

        // ── Named CHECK constraints ───────────────────────────────────────
        for (constraint_name, constraint_def) in &db_config.constraints {
            if let Some(check_expr) = &constraint_def.check {
                stmts.push(format!(
                    "ALTER TABLE {qualified} ADD CONSTRAINT {} CHECK ({});",
                    q(constraint_name),
                    check_expr,
                ));
            }
        }

        stmts
    }
}

// ── Scd2Config ────────────────────────────────────────────────────────────────

/// Shared configuration for every database SCD2 sink.
#[derive(Debug, Clone)]
pub struct Scd2Config {
    pub table:        String,
    pub schema_name:  String,
    pub key_col:      String,
    /// Columns to track for changes.  Empty = track ALL non-key, non-SCD columns.
    pub tracked_cols: Vec<String>,
    pub col_names:    Scd2ColumnNames,
    /// When `true`, keys that are present in the database (`is_current = true`)
    /// but **absent** from the incoming data are closed (`is_current = false`,
    /// `valid_to = NOW()`).
    ///
    /// Use this for **full-snapshot** ingestion (API returns all records, or a
    /// paginated full dataset).  Leave `false` (the default) for **delta /
    /// cherry-pick** ingestion where absence means "not fetched", not "deleted".
    pub close_missing: bool,
    /// Maximum number of keys per SQL `WHERE key IN (...)` chunk.
    /// Used in both write (close-changed) and flush (close-missing) paths.
    /// Defaults to 1000.  Set from the step / global `batch_size` at runtime.
    pub chunk_size: usize,
}

impl Scd2Config {
    pub fn new(default_schema: impl Into<String>) -> Self {
        Self {
            table:        String::new(),
            schema_name:  default_schema.into(),
            key_col:      "id".into(),
            tracked_cols: Vec::new(),
            col_names:    Scd2ColumnNames::default(),
            close_missing: false,
            chunk_size:    1000,
        }
    }

    pub fn table(mut self, t: impl Into<String>) -> Self {
        self.table = t.into();
        self
    }

    pub fn schema(mut self, s: impl Into<String>) -> Self {
        self.schema_name = s.into();
        self
    }

    pub fn key(mut self, col: impl Into<String>) -> Self {
        self.key_col = col.into();
        self
    }

    pub fn track(mut self, cols: Vec<String>) -> Self {
        self.tracked_cols = cols;
        self
    }

    pub fn col_names(mut self, n: Scd2ColumnNames) -> Self {
        self.col_names = n;
        self
    }

    pub fn close_missing(mut self, close: bool) -> Self {
        self.close_missing = close;
        self
    }

    pub fn chunk_size(mut self, n: usize) -> Self {
        self.chunk_size = n.max(1);
        self
    }
}

// ── Scd2Decision ─────────────────────────────────────────────────────────────

/// The output of [`compute_scd2_decision`].
pub struct Scd2Decision {
    /// Natural-key values of rows that must be expired.
    pub to_close:      Vec<String>,
    /// Full rows to insert as new current versions.
    pub to_insert:     Vec<Vec<Option<String>>>,
    /// Column names (no SCD system columns).
    pub insert_cols:   Vec<String>,
    /// All column names from the incoming batch schema.
    pub col_names_vec: Vec<String>,
    /// The four SCD system column names.
    pub scd_meta:      [String; 4],
    /// Summary statistics.
    pub stats:         Scd2Stats,
}

// ── compute_scd2_decision ─────────────────────────────────────────────────────

/// Pure SCD2 diff algorithm — no I/O, no SQL, no connection.
pub fn compute_scd2_decision(
    batch:    &RecordBatch,
    cfg:      &Scd2Config,
    existing: &CurrentRows,
) -> anyhow::Result<Scd2Decision> {
    let schema = batch.schema();
    let key_idx = schema.index_of(&cfg.key_col)?;

    let cn = &cfg.col_names;
    let scd_meta = [
        cn.scd_id.clone(),
        cn.valid_from.clone(),
        cn.valid_to.clone(),
        cn.is_current.clone(),
    ];
    let scd_meta_strs: [&str; 4] = [
        cn.scd_id.as_str(),
        cn.valid_from.as_str(),
        cn.valid_to.as_str(),
        cn.is_current.as_str(),
    ];

    let col_names_vec: Vec<String> = schema.fields()
        .iter().map(|f| f.name().clone()).collect();

    let insert_cols: Vec<String> = col_names_vec.iter()
        .filter(|c| !scd_meta_strs.contains(&c.as_str()))
        .cloned()
        .collect();

    let tracked: Vec<String> = if cfg.tracked_cols.is_empty() {
        col_names_vec.iter()
            .filter(|c| *c != &cfg.key_col && !scd_meta_strs.contains(&c.as_str()))
            .cloned()
            .collect()
    } else {
        cfg.tracked_cols.clone()
    };

    let key_vals = extract_scd2_key_strings(batch, key_idx)?;
    let all_rows = record_batch_to_string_rows(batch);

    let mut to_close:  Vec<String>              = Vec::new();
    let mut to_insert: Vec<Vec<Option<String>>> = Vec::new();
    let mut unchanged = 0usize;

    for (row_idx, row_vals) in all_rows.iter().enumerate() {
        let key_val = &key_vals[row_idx];

        let incoming: HashMap<&str, Option<&str>> = col_names_vec.iter()
            .enumerate()
            .map(|(i, n)| (n.as_str(), row_vals[i].as_deref()))
            .collect();

        match existing.get(key_val) {
            None => to_insert.push(row_vals.clone()),
            Some(existing_row) => {
                let changed = tracked.iter().any(|col| {
                    incoming.get(col.as_str()).copied().flatten()
                        != existing_row.get(col).and_then(|v| v.as_deref())
                });
                if changed {
                    to_close.push(key_val.clone());
                    to_insert.push(row_vals.clone());
                } else {
                    unchanged += 1;
                }
            }
        }
    }

    // NOTE: close_missing is handled at flush() time by each driver,
    // not here — because `existing` only contains rows for keys in the
    // current batch, so we cannot detect globally-missing keys per-batch.

    let updated_rows = to_close.len();
    let new_rows     = to_insert.len() - updated_rows;

    Ok(Scd2Decision {
        to_close,
        to_insert,
        insert_cols,
        col_names_vec,
        scd_meta,
        stats: Scd2Stats { new_rows, updated_rows, unchanged_rows: unchanged },
    })
}