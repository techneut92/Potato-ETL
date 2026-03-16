//! Database common layer and driver traits.
//!
//! This module provides:
//! - **Shared types** used by every database driver: `TableMode`, `WriteStrategy`,
//!   `Scd2ColumnNames`, `Scd2Stats`, `pk_columns()`.
//! - **Common logic** reused across backends: `SinkConfig`, `Scd2Config`,
//!   `compute_scd2_decision`, type coercion, alignment, field metadata.
//! - **Driver traits**: `SourceBuilder`, `SinkBuilder`, `Scd2Builder` — the
//!   contracts that each driver crate implements.
//!
//! The *dispatch layer* (`ReadDB`, `WriteDB`, `Scd2Sink` enums) lives in
//! `potato-etl-runtime`, not here.

use arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};

pub mod common;
pub mod traits;

// ── Shared operation types ───────────────────────────────────────────────────

/// Controls DDL executed against the target table before the first write.
#[derive(Debug, Clone, PartialEq)]
pub enum TableMode {
    /// Table must already exist.  No DDL is executed. *(default)*
    UseExisting,
    /// `CREATE TABLE IF NOT EXISTS` derived from the incoming Arrow schema.
    CreateIfNotExists,
    /// `DROP TABLE IF EXISTS` + `CREATE TABLE` derived from the Arrow schema.
    DropAndReplace,
}

/// Controls how rows are written to the target table.
///
/// The merge key for `Upsert`, `InsertIgnore`, and `MergeDelete` is always
/// derived from the Arrow schema at write time by reading the
/// `etl.primary_key = "true"` field metadata.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteStrategy {
    /// Plain `INSERT`.  Errors on duplicate PK / unique constraint. *(default)*
    Append,
    /// `INSERT … ON CONFLICT DO NOTHING` or equivalent.
    InsertIgnore,
    /// `MERGE` / `INSERT … ON CONFLICT DO UPDATE`.
    Upsert,
    /// Upsert + DELETE absent target rows.
    MergeDelete,
    /// `TRUNCATE` on the first batch, then plain `INSERT`.
    Truncate,
}

/// Returns all column names marked `etl.primary_key = "true"` in `schema`.
pub fn pk_columns(schema: &SchemaRef) -> Vec<String> {
    schema.fields().iter()
        .filter(|f| f.metadata()
            .get(crate::schema::META_PRIMARY_KEY)
            .map(|v| v == "true")
            .unwrap_or(false))
        .map(|f| f.name().clone())
        .collect()
}

/// Statistics returned by every SCD2 write call.
#[derive(Debug, Default)]
pub struct Scd2Stats {
    pub new_rows:       usize,
    pub updated_rows:   usize,
    pub unchanged_rows: usize,
}

/// Custom names for the four system columns added by the SCD2 sink.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Scd2ColumnNames {
    #[serde(default = "scd2_col_valid_from")]
    pub valid_from: String,
    #[serde(default = "scd2_col_valid_to")]
    pub valid_to:   String,
    #[serde(default = "scd2_col_is_current")]
    pub is_current: String,
    #[serde(default = "scd2_col_scd_id")]
    pub scd_id:     String,
}

fn scd2_col_valid_from() -> String { "valid_from".into() }
fn scd2_col_valid_to()   -> String { "valid_to".into()   }
fn scd2_col_is_current() -> String { "is_current".into() }
fn scd2_col_scd_id()     -> String { "scd_id".into()     }

impl Default for Scd2ColumnNames {
    fn default() -> Self {
        Self {
            valid_from: scd2_col_valid_from(),
            valid_to:   scd2_col_valid_to(),
            is_current: scd2_col_is_current(),
            scd_id:     scd2_col_scd_id(),
        }
    }
}