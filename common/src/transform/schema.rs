//! Schema-level transforms (rename all columns by case, drop/add columns, cast types).

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use anyhow::{Context, Result};

use crate::config::IdentifierCase;
use super::EtlTransform;

// ── Free functions (used by the DAG executor) ─────────────────────────────────

/// Renames **all** columns in a `RecordBatch` according to a case transformation strategy.
///
/// This is a zero-copy schema-only transformation: the underlying Arrow arrays
/// are not modified, only the field names in the schema metadata.
///
/// ## Example
///
/// ```rust
/// use arrow::record_batch::RecordBatch;
/// use arrow::array::Int32Array;
/// use arrow::datatypes::{Schema, Field, DataType};
/// use std::sync::Arc;
/// use potato_etl_runtime::config::IdentifierCase;
/// use potato_etl_runtime::transform::schema::apply_rename_all;
///
/// let schema = Schema::new(vec![
///     Field::new("employee_id", DataType::Int32, false),
///     Field::new("first_name", DataType::Utf8, true),
/// ]);
/// let batch = RecordBatch::try_new(
///     Arc::new(schema),
///     vec![
///         Arc::new(Int32Array::from(vec![1, 2, 3])),
///         Arc::new(arrow::array::StringArray::from(vec!["Alice", "Bob", "Charlie"])),
///     ],
/// ).unwrap();
///
/// let uppercased = apply_rename_all(batch, IdentifierCase::Upper).unwrap();
/// assert_eq!(uppercased.schema().field(0).name(), "EMPLOYEE_ID");
/// assert_eq!(uppercased.schema().field(1).name(), "FIRST_NAME");
/// ```
pub fn apply_rename_all(
    batch: RecordBatch,
    case: IdentifierCase,
) -> Result<RecordBatch> {
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            let new_name = case.transform(f.name());
            // Preserve data type, nullable flag, AND all metadata (etl.*, source_db, etc.).
            Field::new(&new_name, f.data_type().clone(), f.is_nullable())
                .with_metadata(f.metadata().clone())
        })
        .collect();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())
        .context("Failed to rebuild RecordBatch with renamed schema")
}

// ── RenameAllTransform ────────────────────────────────────────────────────────

/// Transform that renames all columns according to a case transformation strategy.
///
/// This is typically used before a database sink to match the target schema's
/// identifier conventions (e.g., Oracle's implicit uppercase, PostgreSQL's
/// lowercase, MSSQL's PascalCase or UPPERCASE).
///
/// ## Example
///
/// ```yaml
/// steps:
///   - id: source
///     type: read_db
///     from:
///       connection: mysql
///       table: employees
///
///   - id: uppercase_columns
///     type: transform_schema
///     input: source
///     transforms:
///       - type: rename_all
///         case: upper
///
///   - id: sink
///     type: write_db
///     input: uppercase_columns
///     target:
///       connection: oracle
///       table: EMPLOYEES
///     mode: truncate
/// ```
#[derive(Debug, Clone)]
pub struct RenameAllTransform {
    case: IdentifierCase,
}

impl RenameAllTransform {
    pub fn new(case: IdentifierCase) -> Self {
        Self { case }
    }
}

impl EtlTransform for RenameAllTransform {
    fn transform(&self, batch: RecordBatch) -> Result<RecordBatch> {
        apply_rename_all(batch, self.case)
    }
}