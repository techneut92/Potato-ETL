//! Column-rename transform.

use indexmap::IndexMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use super::EtlTransform;

// ── Free function (used by the DAG executor) ──────────────────────────────────

/// Renames columns in a `RecordBatch` according to `columns`.
///
/// Keys in `columns` that do not match any field name are silently ignored.
/// Column data, Arrow types, nullability, and **all Arrow field metadata**
/// (including `etl.*` schema metadata) are preserved on renamed fields.
///
/// ## Schema propagation
///
/// Because Arrow field metadata travels with each field, renaming a column
/// with `primary_key`, `foreign_key`, `db_type`, or `description` metadata
/// keeps that metadata attached under the new name.  This means the full
/// schema survives a rename step intact — `generate_ddl()` downstream will
/// see the correct type/constraint information regardless of the column names
/// in the original source.
pub fn apply_rename(
    batch:   RecordBatch,
    columns: &IndexMap<String, String>,
) -> anyhow::Result<RecordBatch> {
    let fields: Vec<Field> = batch.schema().fields().iter().map(|f| {
        let new_name = columns.get(f.name()).map(|n| n.as_str()).unwrap_or(f.name());
        // Preserve data type, nullable flag, AND all metadata.
        Field::new(new_name, f.data_type().clone(), f.is_nullable())
            .with_metadata(f.metadata().clone())
    }).collect();

    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())?)
}

// ── RenameTransform ───────────────────────────────────────────────────────────

/// Renames a single column from `from` to `to`.
pub struct RenameTransform {
    from: String,
    to:   String,
}

impl RenameTransform {
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self { from: from.into(), to: to.into() }
    }
}

impl EtlTransform for RenameTransform {
    fn transform(&self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        let mut columns = IndexMap::new();
        columns.insert(self.from.clone(), self.to.clone());
        apply_rename(batch, &columns)
    }
}