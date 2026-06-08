//! Column alignment between incoming Arrow batches and existing target tables.
//!
//! When a sink writes to a table that already exists (i.e. not freshly
//! `DROP + CREATE`d), the column order, count, or types may differ from the
//! incoming `RecordBatch` schema.  This module provides a single, shared
//! alignment pass that:
//!
//! 1. **Detects column order mismatches** and computes a reorder mapping.
//! 2. **Detects extra batch columns** (present in batch, absent from table)
//!    and either drops them or errors depending on policy.
//! 3. **Detects missing batch columns** (present in table, absent from batch)
//!    and either fills NULL, skips, or errors depending on the backend's
//!    [`MissingColumnBehavior`].
//! 4. **Applies the mapping** to each `RecordBatch` via [`apply_alignment`] —
//!    a zero-copy column reorder + select.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

// ── TargetColumn ─────────────────────────────────────────────────────────────

/// Metadata about a single column in the target table.
#[derive(Debug, Clone)]
pub struct TargetColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub has_default: bool,
}

// ── MissingColumnBehavior ────────────────────────────────────────────────────

/// What to do when the target table has columns that are NOT in the batch.
///
/// Configurable per write step via `options.on_missing_column` (`error` | `skip`).
/// Defaults to [`MissingColumnBehavior::Skip`] — the database supplies NULL / the
/// column's DEFAULT, and the DB itself enforces NOT NULL. `Error` is opt-in strict
/// mode and only fails for a NOT NULL, no-default column (see `compute_alignment`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingColumnBehavior {
    Error,
    Skip,
}

// ── AlignedColumn ────────────────────────────────────────────────────────────

/// One entry in the alignment mapping.
#[derive(Debug, Clone)]
pub struct AlignedColumn {
    pub batch_index: usize,
    #[allow(dead_code)]
    pub target_name: String,
    #[allow(dead_code)]
    pub target_type: String,
    #[allow(dead_code)]
    pub target_ordinal: Option<usize>,
}

// ── ColumnAlignment ──────────────────────────────────────────────────────────

/// The result of [`compute_alignment`].
#[derive(Debug, Clone)]
pub struct ColumnAlignment {
    pub mapping: Vec<AlignedColumn>,
    #[allow(dead_code)]
    pub dropped_columns: Vec<String>,
    #[allow(dead_code)]
    pub missing_columns: Vec<String>,
    identity: bool,
}

impl ColumnAlignment {
    #[inline]
    pub fn is_identity(&self) -> bool {
        self.identity
    }
}

// ── compute_alignment ────────────────────────────────────────────────────────

/// Computes column alignment between an Arrow batch schema and a target table.
pub fn compute_alignment(
    batch_schema: &SchemaRef,
    target_columns: &[TargetColumn],
    missing_behavior: MissingColumnBehavior,
    rename_targets: &[String],
    table_display: &str,
) -> anyhow::Result<ColumnAlignment> {
    anyhow::ensure!(
        !target_columns.is_empty(),
        "compute_alignment: target table '{table_display}' has no columns"
    );

    let batch_names: HashMap<String, usize> = batch_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().to_lowercase(), i))
        .collect();

    let mut mapping: Vec<AlignedColumn> = Vec::with_capacity(target_columns.len());
    let mut missing_columns: Vec<String> = Vec::new();
    let mut needs_reorder = false;

    for (tbl_idx, tc) in target_columns.iter().enumerate() {
        let key = tc.name.to_lowercase();
        if let Some(&batch_idx) = batch_names.get(&key) {
            mapping.push(AlignedColumn {
                batch_index: batch_idx,
                target_name: tc.name.clone(),
                target_type: tc.data_type.clone(),
                target_ordinal: Some(tbl_idx + 1),
            });
            if batch_idx != tbl_idx {
                needs_reorder = true;
            }
        } else {
            match missing_behavior {
                // `Error` only fails for a column the database CANNOT fill on its
                // own — NOT NULL with no default. A nullable or defaulted column
                // is safe to omit: the server inserts NULL / its DEFAULT, so we
                // treat it as skipped rather than erroring.
                MissingColumnBehavior::Error if !tc.nullable && !tc.has_default => {
                    anyhow::bail!(
                        "column '{}' in target table [{}] (position {}) is NOT NULL with no \
                         default, but is not present in the incoming data — there is no value \
                         to insert. Provide it (e.g. via a map step or a matching `rename_to`), \
                         give the column a database default, or set \
                         `options.on_missing_column: skip` to insert anyway.",
                        tc.name, table_display, tbl_idx + 1
                    );
                }
                // `Skip`, or `Error` on a nullable/defaulted column: omit it and
                // let the database supply NULL / the column's DEFAULT.
                MissingColumnBehavior::Error | MissingColumnBehavior::Skip => {
                    missing_columns.push(tc.name.clone());
                }
            }
        }
    }

    let table_names: std::collections::HashSet<String> = target_columns
        .iter()
        .map(|c| c.name.to_lowercase())
        .collect();

    let dropped_columns: Vec<String> = batch_schema
        .fields()
        .iter()
        .filter(|f| !table_names.contains(&f.name().to_lowercase()))
        .map(|f| f.name().clone())
        .collect();

    // A `rename_to` target that ended up in the batch but matches no table
    // column means the rename silently discarded its data (e.g. `rename_to:
    // ATTRIBUTE_KEY` when the column is really `KEY`). This is always a mistake
    // — a dead rename whose source column was never present wouldn't appear in
    // the batch, so it can't reach `dropped_columns`.
    if !rename_targets.is_empty() && !dropped_columns.is_empty() {
        let rt_lower: std::collections::HashSet<String> =
            rename_targets.iter().map(|s| s.to_lowercase()).collect();
        if let Some(bad) = dropped_columns.iter().find(|c| rt_lower.contains(&c.to_lowercase())) {
            anyhow::bail!(
                "rename_to target '{}' matches no column in target table [{}] — the rename \
                 produced a column the table doesn't have, so its data would be silently \
                 discarded. Fix the target name to match an existing column (case-insensitive), \
                 or remove the rename.",
                bad, table_display
            );
        }
    }

    let has_drops = !dropped_columns.is_empty();
    let has_missing = !missing_columns.is_empty();

    let identity = !needs_reorder
        && !has_drops
        && !has_missing
        && mapping.len() == batch_schema.fields().len();

    if needs_reorder {
        let table_order: Vec<&str> = target_columns.iter().map(|c| c.name.as_str()).collect();
        let batch_order: Vec<&str> = batch_schema.fields().iter().map(|f| f.name().as_str()).collect();
        tracing::debug!(
            table       = %table_display,
            table_order = ?table_order,
            batch_order = ?batch_order,
            "column alignment: reordering batch columns to match table"
        );
    }

    if has_drops {
        tracing::debug!(
            table   = %table_display,
            columns = ?dropped_columns,
            "column alignment: {} batch column(s) not in target table — dropped",
            dropped_columns.len()
        );
    }

    if has_missing {
        tracing::debug!(
            table   = %table_display,
            columns = ?missing_columns,
            "column alignment: {} table column(s) not in batch — will receive DEFAULT/NULL",
            missing_columns.len()
        );
    }

    if identity {
        tracing::debug!(
            table   = %table_display,
            columns = mapping.len(),
            "column alignment: batch schema matches target table — no reorder needed"
        );
    }

    Ok(ColumnAlignment {
        mapping,
        dropped_columns,
        missing_columns,
        identity,
    })
}

// ── apply_alignment ──────────────────────────────────────────────────────────

/// Reorders/selects columns to match the target table layout.
/// Zero-copy O(columns) operation.
pub fn apply_alignment(
    batch: RecordBatch,
    alignment: &ColumnAlignment,
) -> anyhow::Result<RecordBatch> {
    if alignment.is_identity() {
        return Ok(batch);
    }

    let schema = batch.schema();
    let new_fields: Vec<_> = alignment
        .mapping
        .iter()
        .map(|ac| schema.field(ac.batch_index).clone())
        .collect();
    let new_columns: Vec<_> = alignment
        .mapping
        .iter()
        .map(|ac| Arc::clone(batch.column(ac.batch_index)))
        .collect();
    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    Ok(RecordBatch::try_new(new_schema, new_columns)?)
}


