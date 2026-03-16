//! Row-filter transform.
//!
//! Two modes:
//!
//! | YAML key | Behaviour |
//! |---|---|
//! | `column:` + `value:` | Keep rows where `column == value` (string comparison after cast). Legacy form. |
//! | `condition:` | Keep rows where the expression evaluates to `true`. Full expression DSL. |
//!
//! ## Examples
//!
//! ```yaml
//! # Legacy form
//! - id: active_only
//!   type: filter
//!   column: status
//!   value: active
//!
//! # Expression form
//! - id: active_high_value
//!   type: filter
//!   condition: "status == \"active\" and total >= 1000"
//! ```

use arrow::array::{BooleanArray, StringArray};
use arrow::compute::{cast, filter_record_batch};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use super::EtlTransform;
use super::expr::apply_condition;

// ── Free functions ────────────────────────────────────────────────────────────

/// Keep only rows where `col` equals `value` (string comparison after cast to Utf8).
pub fn apply_filter(
    batch: &RecordBatch,
    col:   &str,
    value: &str,
) -> anyhow::Result<RecordBatch> {
    let col_arr = batch.column_by_name(col)
        .ok_or_else(|| anyhow::anyhow!("filter: column '{col}' not found in batch"))?;

    let str_arr = cast(col_arr, &DataType::Utf8)?;
    let str_arr = str_arr.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("filter: cannot treat column '{col}' as text after cast"))?;

    let mask: BooleanArray = str_arr.iter()
        .map(|v| v.map(|s| s == value))
        .collect();

    Ok(filter_record_batch(batch, &mask)?)
}

/// Keep only rows where `condition` evaluates to `true`.
///
/// Uses the full expression DSL — see [`crate::transform::expr`] for syntax.
pub fn apply_filter_expr(batch: RecordBatch, condition: &str) -> anyhow::Result<RecordBatch> {
    apply_condition(batch, condition)
}

// ── FilterTransform (OO wrapper) ──────────────────────────────────────────────

/// Keeps rows where `field == value` (string comparison).
pub struct FilterTransform {
    field: String,
    value: String,
}

impl FilterTransform {
    pub fn new(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self { field: field.into(), value: value.into() }
    }
}

impl EtlTransform for FilterTransform {
    fn transform(&self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        apply_filter(&batch, &self.field, &self.value)
    }
}

/// Keeps rows where `condition` expression is true.
pub struct FilterExprTransform {
    condition: String,
}

impl FilterExprTransform {
    pub fn new(condition: impl Into<String>) -> Self {
        Self { condition: condition.into() }
    }
}

impl EtlTransform for FilterExprTransform {
    fn transform(&self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        apply_filter_expr(batch, &self.condition)
    }
}
