//! Applies per-column Arrow overrides and metadata to `RecordBatch`es.
//!
//! ## Functions
//!
//! | Function | Purpose | Used by |
//! |---|---|------|
//! | [`apply_arrow_overrides`] | Arrow type casting (col → arrow_type) | all steps (source + sink) |
//! | [`apply_column_options`]  | Stamp DDL constraints + db_type       | `write_db`, `scd2_sink` sinks |
//! | [`apply_rename`]          | Rename columns (preserves metadata)   | `rename` step |
//!
//! ## Compile-once plans
//!
//! For hot-path batch loops the DAG executor uses *compiled plans* that
//! precompute all HashMap lookups, type parsing, and schema construction
//! on the **first** batch and then apply cheaply to every subsequent batch:
//!
//! | Plan struct | Compiled from | Per-batch cost |
//! |---|---|---|
//! | [`ArrowOverridesPlan`] | `arrow_overrides` map | `arrow::compute::cast` only for columns that differ |
//! | [`MetadataStampPlan`]  | `column_options` (incl. `db_type`) | Schema swap (Arc clone) — zero data copies |
//!
//! ## Case sensitivity
//!
//! All column name matching is **case-insensitive** — Oracle uppercase column
//! names (`EMPLOYEE_ID`) match lower-cased keys (`employee_id`).

use std::sync::Arc;
use std::collections::HashMap;

use indexmap::IndexMap;

use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::schema::constants::{
    META_CHECK_EXPR, META_DEFAULT_EXPR, META_DESCRIPTION,
    META_FOREIGN_KEY, META_INDEX, META_NULLABLE,
    META_ON_UPDATE_EXPR, META_PRIMARY_KEY, META_UNIQUE,
    META_DB_TYPE, META_ENUM_VALUES,
};
use crate::util::schema::parse_arrow_type;
use crate::transform::expr::lookup_env_var;

// ── apply_arrow_overrides ────────────────────────────────────────────────────

/// Applies per-column Arrow type casts to a batch.
///
/// Keys are column names (case-insensitive).  Values are Arrow type strings
/// (`"utf8"`, `"int64"`, `"timestamp"`, etc.).
///
/// ## Example
///
/// ```yaml
/// arrow_overrides:
///   employee_id: utf8
///   salary: float64
/// ```
pub fn apply_arrow_overrides(
    batch:     RecordBatch,
    overrides: &HashMap<String, String>,
) -> anyhow::Result<RecordBatch> {
    if overrides.is_empty() {
        return Ok(batch);
    }

    let schema   = batch.schema();
    let mut cols = batch.columns().to_vec();
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_lower = field.name().to_lowercase();

        let type_str = overrides
            .iter()
            .find(|(k, _)| k.to_lowercase() == col_lower)
            .map(|(_, v)| v.as_str());

        let Some(type_str) = type_str else { continue };

        let target = parse_arrow_type(type_str)?;
        if target != *field.data_type() {
            let casted = cast(&*cols[idx], &target).map_err(|e| anyhow::anyhow!(
                "arrow_overrides cast for '{}' → '{}': {e}", field.name(), type_str
            ))?;
            cols[idx] = casted;
        }
        fields[idx] = Field::new(field.name(), target, field.is_nullable())
            .with_metadata(field.metadata().clone());

        tracing::debug!(
            column     = field.name(),
            arrow_type = type_str,
            "arrow_overrides: cast column"
        );
    }

    Ok(RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), cols)?)
}

// ── ArrowOverridesPlan (compile-once) ───────────────────────────────────────

/// Per-column action in a compiled [`SchemaOverridePlan`].
enum OverrideAction {
    /// No change needed — passthrough.
    None,
    /// Raw Arrow cast to the target type.
    Cast(DataType),
}

/// Precomputed arrow-overrides plan.
///
/// Built once from the first batch's schema + the `arrow_overrides` map.
/// Subsequent batches call [`apply_arrow_overrides_plan`] which skips all
/// HashMap lookups, `parse_arrow_type` calls, and schema construction.
pub struct ArrowOverridesPlan {
    /// Per-column action.
    actions: Vec<OverrideAction>,
    /// Fully-constructed output schema (reused by every batch).
    output_schema: SchemaRef,
}

/// Compile an [`ArrowOverridesPlan`] from the first batch's schema.
///
/// Returns `None` when `overrides` is empty (no work needed — pass batches through).
pub fn compile_arrow_overrides(
    schema:    &SchemaRef,
    overrides: &HashMap<String, String>,
) -> anyhow::Result<Option<ArrowOverridesPlan>> {
    if overrides.is_empty() {
        return Ok(None);
    }

    let mut actions: Vec<OverrideAction> = Vec::with_capacity(schema.fields().len());
    let mut fields:  Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut any_work = false;

    for field in schema.fields().iter() {
        let col_lower = field.name().to_lowercase();

        let type_str = overrides
            .iter()
            .find(|(k, _)| k.to_lowercase() == col_lower)
            .map(|(_, v)| v.as_str());

        if let Some(type_str) = type_str {
            let target = parse_arrow_type(type_str)?;
            let needs_cast = target != *field.data_type();
            if needs_cast { any_work = true; }
            actions.push(if needs_cast { OverrideAction::Cast(target.clone()) } else { OverrideAction::None });
            fields.push(
                Field::new(field.name(), target, field.is_nullable())
                    .with_metadata(field.metadata().clone()),
            );
            tracing::debug!(
                column     = field.name(),
                arrow_type = type_str,
                "arrow_overrides: compiled cast"
            );
        } else {
            actions.push(OverrideAction::None);
            fields.push((**field).clone());
        }
    }

    if !any_work && fields.iter().zip(schema.fields().iter()).all(|(a, b)| a == b.as_ref()) {
        return Ok(None);
    }

    Ok(Some(ArrowOverridesPlan {
        actions,
        output_schema: Arc::new(ArrowSchema::new(fields)),
    }))
}

/// Apply a precomputed [`ArrowOverridesPlan`] to a batch.
///
/// Only columns with an action are touched; the output schema is an Arc
/// clone — no HashMap lookups, no type-string parsing.
pub fn apply_arrow_overrides_plan(
    batch: RecordBatch,
    plan:  &ArrowOverridesPlan,
) -> anyhow::Result<RecordBatch> {
    let mut cols = batch.columns().to_vec();
    for (idx, action) in plan.actions.iter().enumerate() {
        match action {
            OverrideAction::None => {}
            OverrideAction::Cast(dt) => {
                let casted = cast(&*cols[idx], dt).map_err(|e| anyhow::anyhow!(
                    "arrow_overrides cast for '{}' → '{:?}': {e}",
                    plan.output_schema.field(idx).name(), dt
                ))?;
                cols[idx] = casted;
            }
        }
    }
    Ok(RecordBatch::try_new(plan.output_schema.clone(), cols)?)
}

// ── apply_rename ─────────────────────────────────────────────────────────────

/// Renames columns according to `columns` (old_name → new_name).
///
/// Column metadata is fully preserved.  Columns not listed in the map are
/// passed through unchanged.  Matching is case-insensitive.
///
/// The batch data arrays are never cloned — only the schema is rebuilt.
pub fn apply_rename(
    batch:   RecordBatch,
    columns: &IndexMap<String, String>,
) -> anyhow::Result<RecordBatch> {
    if columns.is_empty() {
        return Ok(batch);
    }

    let schema = batch.schema();
    let cols   = batch.columns().to_vec();
    let fields: Vec<Field> = schema.fields().iter().map(|f| {
        let col_lower = f.name().to_lowercase();
        let new_name = columns
            .iter()
            .find(|(k, _)| k.to_lowercase() == col_lower)
            .map(|(_, v)| v.as_str());

        if let Some(name) = new_name {
            Field::new(name, f.data_type().clone(), f.is_nullable())
                .with_metadata(f.metadata().clone())
        } else {
            (**f).clone()
        }
    }).collect();

    Ok(RecordBatch::try_new(ArrowSchema::new(fields).into(), cols)?)
}

// ── MetadataStampPlan (compile-once) ─────────────────────────────────────────

/// Precomputed plan for [`apply_column_options`].
///
/// Both `db_type` and DDL hints only modify Arrow **schema metadata** — no data
/// arrays are touched.  Since all batches from the same source share the same
/// schema, we precompute the stamped output schema once and swap it in per batch
/// (one `Arc::clone` + `RecordBatch::try_new`).
pub struct MetadataStampPlan {
    /// The output schema with `etl.db_type`, `etl.primary_key`, etc. already stamped.
    output_schema: SchemaRef,
}

/// Compile a [`MetadataStampPlan`] from the first batch's schema.
///
/// Applies `column_options` (including `db_type`) and caches the resulting
/// schema.  The `db_type` field in each [`ColumnOption`] is stamped as
/// `etl.db_type` metadata — the highest-priority SQL type for DDL generation.
///
/// Returns `None` when `column_options` is empty (no work needed).
pub fn compile_metadata_stamps(
    schema:         &SchemaRef,
    column_options: &HashMap<String, crate::schema::field::ColumnOption>,
) -> anyhow::Result<Option<MetadataStampPlan>> {
    if column_options.is_empty() {
        return Ok(None);
    }

    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_lower = field.name().to_lowercase();
        let opt = column_options
            .iter()
            .find(|(k, _)| k.to_lowercase() == col_lower)
            .map(|(_, v)| v);

        let Some(co) = opt else { continue };

        let mut meta = fields[idx].metadata().clone();
        let mut nullable = fields[idx].is_nullable();

        // db_type → etl.db_type (tier-1 DDL resolution)
        if let Some(db_type) = &co.db_type  { meta.insert(META_DB_TYPE.into(), db_type.clone()); }
        if co.primary_key { meta.insert(META_PRIMARY_KEY.into(), "true".into()); }
        if co.unique      { meta.insert(META_UNIQUE.into(),      "true".into()); }
        if co.index       { meta.insert(META_INDEX.into(),       "true".into()); }
        if let Some(n)       = co.nullable     { meta.insert(META_NULLABLE.into(),    n.to_string()); nullable = n; }
        if let Some(check)   = &co.check_expr  { meta.insert(META_CHECK_EXPR.into(),  check.clone()); }
        if let Some(default) = &co.default_expr { meta.insert(META_DEFAULT_EXPR.into(), default.clone()); }
        if let Some(fk)      = &co.foreign_key {
            let fk_json = serde_json::to_string(fk)
                .map_err(|e| anyhow::anyhow!("FK serialisation error: {e}"))?;
            meta.insert(META_FOREIGN_KEY.into(), fk_json);
        }
        if let Some(desc) = &co.description { meta.insert(META_DESCRIPTION.into(), desc.clone()); }
        if let Some(on_update) = &co.on_update_expr { meta.insert(META_ON_UPDATE_EXPR.into(), on_update.clone()); }
        if !co.enum_values.is_empty() {
            let json = serde_json::to_string(&co.enum_values)
                .map_err(|e| anyhow::anyhow!("enum_values serialisation error: {e}"))?;
            meta.insert(META_ENUM_VALUES.into(), json);
        }

        fields[idx] = Field::new(fields[idx].name(), fields[idx].data_type().clone(), nullable)
            .with_metadata(meta);

        tracing::debug!(
            column      = field.name(),
            primary_key = co.primary_key,
            unique      = co.unique,
            index       = co.index,
            db_type     = ?co.db_type,
            "metadata_stamp: compiled DDL hints"
        );
    }

    Ok(Some(MetadataStampPlan {
        output_schema: Arc::new(ArrowSchema::new(fields)),
    }))
}

/// Apply a precomputed [`MetadataStampPlan`] to a batch.
///
/// Swaps the schema in constant time — no HashMap lookups, no metadata
/// construction, no data copies.
pub fn apply_metadata_stamp_plan(
    batch: RecordBatch,
    plan:  &MetadataStampPlan,
) -> anyhow::Result<RecordBatch> {
    Ok(RecordBatch::try_new(plan.output_schema.clone(), batch.columns().to_vec())?)
}

// ── apply_column_options ─────────────────────────────────────────────────────

/// Stamps DDL-relevant `etl.*` metadata from a [`ColumnOptionsMap`][crate::schema::ColumnOptionsMap]
/// onto a batch's schema **without touching the data**.
///
/// Writes `db_type`, `primary_key`, `unique`, `index`, `nullable`, `check_expr`,
/// `default_expr`, `foreign_key`, `on_update_expr`, and `description` into
/// Arrow field metadata.
///
/// The `db_type` field is stamped as `etl.db_type` — the highest-priority SQL
/// type override for DDL generation.
///
/// Keys are column names (case-insensitive).  The batch data arrays are never
/// cloned — only the schema is rebuilt.
pub fn apply_column_options(
    batch:          RecordBatch,
    column_options: &HashMap<String, crate::schema::field::ColumnOption>,
) -> anyhow::Result<RecordBatch> {
    if column_options.is_empty() {
        return Ok(batch);
    }

    let schema  = batch.schema();
    let cols    = batch.columns().to_vec();
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_lower = field.name().to_lowercase();

        let opt = column_options
            .iter()
            .find(|(k, _)| k.to_lowercase() == col_lower)
            .map(|(_, v)| v);

        let Some(co) = opt else { continue };

        let mut meta = field.metadata().clone();
        let mut nullable = field.is_nullable();

        if let Some(db_type) = &co.db_type {
            meta.insert(META_DB_TYPE.into(), db_type.clone());
        }
        if co.primary_key {
            meta.insert(META_PRIMARY_KEY.into(), "true".into());
        }
        if co.unique {
            meta.insert(META_UNIQUE.into(), "true".into());
        }
        if co.index {
            meta.insert(META_INDEX.into(), "true".into());
        }
        if let Some(n) = co.nullable {
            meta.insert(META_NULLABLE.into(), n.to_string());
            nullable = n;
        }
        if let Some(check) = &co.check_expr {
            meta.insert(META_CHECK_EXPR.into(), check.clone());
        }
        if let Some(default) = &co.default_expr {
            meta.insert(META_DEFAULT_EXPR.into(), default.clone());
        }
        if let Some(fk) = &co.foreign_key {
            let fk_json = serde_json::to_string(fk)
                .map_err(|e| anyhow::anyhow!("FK serialisation error: {e}"))?;
            meta.insert(META_FOREIGN_KEY.into(), fk_json);
        }
        if let Some(desc) = &co.description {
            meta.insert(META_DESCRIPTION.into(), desc.clone());
        }
        if let Some(on_update) = &co.on_update_expr {
            meta.insert(META_ON_UPDATE_EXPR.into(), on_update.clone());
        }
        if !co.enum_values.is_empty() {
            let json = serde_json::to_string(&co.enum_values)
                .map_err(|e| anyhow::anyhow!("enum_values serialisation error: {e}"))?;
            meta.insert(META_ENUM_VALUES.into(), json);
        }

        fields[idx] = Field::new(field.name(), field.data_type().clone(), nullable)
            .with_metadata(meta);

        tracing::debug!(
            column = field.name(),
            primary_key = co.primary_key,
            unique = co.unique,
            index = co.index,
            db_type = ?co.db_type,
            "column_options: stamped DDL hints"
        );
    }

    Ok(RecordBatch::try_new(ArrowSchema::new(fields).into(), cols)?)
}

// ── apply_value_injections ──────────────────────────────────────────────────

/// Applies value injections from `schema.arrow.columns` entries that have
/// a `value:` field set.
///
/// For each entry in `injections`:
/// - The `value` string is resolved:
///   - `$name` → environment variable (broadcast to batch length)
///   - `name` (no `$`) → copy from an existing batch column
/// - If the target column already exists in the batch, its data is replaced.
/// - If it does not exist, a new column is appended.
/// - If `arrow_type` is set on the definition, the resolved data is cast
///   to that type after injection.
/// - If `logical_type` is set, `etl.logical_type` metadata is stamped.
/// - If `nullable` is set, the field's nullable flag is overridden.
///
/// Columns without a matching injection entry pass through unchanged.
///
/// ## Example
///
/// ```yaml
/// schema:
///   arrow:
///     columns:
///       inserted_at:
///         value: $insert_at_var
///         type: "timestamp[us, UTC]"
///         logical_type: timestamp
/// ```
pub fn apply_value_injections(
    batch: RecordBatch,
    injections: &HashMap<String, crate::config::ArrowColumnDef>,
) -> anyhow::Result<RecordBatch> {
    use crate::transform::expr::broadcast_array;
    use crate::schema::constants::META_LOGICAL_TYPE;

    if injections.is_empty() {
        return Ok(batch);
    }

    let num_rows   = batch.num_rows();
    let old_schema = batch.schema();

    // Start with a copy of all existing fields + arrays.
    let mut fields: Vec<Field>    = old_schema.fields().iter().map(|f| (**f).clone()).collect();
    let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();

    // Case-insensitive lookup for existing batch columns.
    let existing_lower: HashMap<String, usize> = old_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| (f.name().to_lowercase(), idx))
        .collect();

    for (target_name, col_def) in injections.iter() {
        let value_ref = match &col_def.value {
            Some(v) => v.as_str(),
            None    => continue, // should not happen (filtered by caller)
        };

        // ── Resolve the source data ──────────────────────────────────────────
        let source_name = value_ref.strip_prefix('$').unwrap_or(value_ref);
        let source_lower = source_name.to_lowercase();

        let resolved: ArrayRef = if source_lower == "null" || source_lower == "none" {
            // Literal null injection — create a null-filled column.
            // When an arrow_type is specified, use it; otherwise default to Utf8.
            let dt = col_def.arrow_type.as_ref()
                .map(|t| parse_arrow_type(t))
                .transpose()?
                .unwrap_or(arrow::datatypes::DataType::Utf8);
            arrow::array::new_null_array(&dt, num_rows)
        } else if let Some(&src_idx) = existing_lower.get(&source_lower) {
            // Copy from existing batch column.
            arrays[src_idx].clone()
        } else if let Some(env_array) = lookup_env_var(source_name) {
            broadcast_array(&env_array, num_rows)
                .map_err(|e| anyhow::anyhow!(
                    "value_injection: failed to broadcast env var '{}': {}",
                    source_name, e
                ))?
        } else {
            anyhow::bail!(
                "value_injection: source '{}' for column '{}' not found in \
                 batch (available: {}) or environment variables",
                source_name,
                target_name,
                existing_lower.keys().cloned().collect::<Vec<_>>().join(", ")
            );
        };

        // ── Optional type cast ───────────────────────────────────────────────
        let resolved = if let Some(type_str) = &col_def.arrow_type {
            let target_type = parse_arrow_type(type_str)?;
            if *resolved.data_type() != target_type {
                cast(&*resolved, &target_type).map_err(|e| anyhow::anyhow!(
                    "value_injection: cast '{}' → '{}' for column '{}': {e}",
                    resolved.data_type(), type_str, target_name
                ))?
            } else {
                resolved
            }
        } else {
            resolved
        };

        // ── Build field metadata ─────────────────────────────────────────────
        let nullable = col_def.nullable.unwrap_or(resolved.null_count() > 0);
        let mut meta = std::collections::HashMap::new();
        if let Some(lt) = &col_def.logical_type {
            meta.insert(META_LOGICAL_TYPE.to_string(), lt.clone());
        }
        let field = Field::new(target_name.clone(), resolved.data_type().clone(), nullable)
            .with_metadata(meta);

        // ── Insert or append ─────────────────────────────────────────────────
        let target_lower = target_name.to_lowercase();
        if let Some(&idx) = existing_lower.get(&target_lower) {
            // Replace existing column.
            fields[idx] = field;
            arrays[idx] = resolved;
            tracing::debug!(
                target = target_name,
                source = source_name,
                "value_injection: replaced existing column"
            );
        } else {
            // Append new column.
            fields.push(field);
            arrays.push(resolved);
            tracing::debug!(
                target = target_name,
                source = source_name,
                "value_injection: appended new column"
            );
        }
    }

    let new_schema = Arc::new(ArrowSchema::new(fields));
    Ok(RecordBatch::try_new(new_schema, arrays)?)
}

// ── apply_exclude_columns ───────────────────────────────────────────────────

/// Drops the specified columns from a batch.
///
/// Keys in `exclude` are column names (case-insensitive). Columns not listed
/// are passed through unchanged. If a column in `exclude` does not exist, it
/// is silently ignored (no error).
///
/// This is useful for:
/// - Dropping large BLOB columns early to reduce memory usage
/// - Excluding columns with incompatible types before sinking
/// - Removing sensitive columns from the pipeline
///
/// ## Example
///
/// ```yaml
/// - id: source
///   type: read_db
///   from:
///     connection: pg
///     table: user_details
///   exclude:
///     - system_presence    # Drop this column
///     - large_blob
/// ```
pub fn apply_exclude_columns(
    batch: RecordBatch,
    exclude: &[String],
) -> anyhow::Result<RecordBatch> {
    if exclude.is_empty() {
        return Ok(batch);
    }

    let schema = batch.schema();

    // Build lowercase set for case-insensitive matching
    let exclude_lower: std::collections::HashSet<String> = exclude
        .iter()
        .map(|s| s.to_lowercase())
        .collect();

    // Collect fields and arrays that are NOT in the exclude list
    let mut new_fields: Vec<Field> = Vec::new();
    let mut new_arrays: Vec<arrow::array::ArrayRef> = Vec::new();
    let mut excluded_count = 0;

    for (idx, field) in schema.fields().iter().enumerate() {
        let name_lower = field.name().to_lowercase();

        if exclude_lower.contains(&name_lower) {
            excluded_count += 1;
            tracing::debug!(
                column = field.name(),
                "exclude_columns: dropped column"
            );
        } else {
            new_fields.push((**field).clone());
            new_arrays.push(batch.column(idx).clone());
        }
    }

    if excluded_count > 0 {
        tracing::debug!(
            excluded = excluded_count,
            remaining = new_fields.len(),
            "exclude_columns: dropped {excluded_count} column(s)"
        );
    }

    let new_schema = Arc::new(ArrowSchema::new(new_fields));
    Ok(RecordBatch::try_new(new_schema, new_arrays)?)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use std::collections::HashMap;

    /// Helper: create a simple 2-column batch for testing
    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef;

        RecordBatch::try_new(schema, vec![id_array, name_array])
            .expect("failed to create test batch")
    }

    #[test]
    fn test_apply_exclude_columns() {
        let batch = make_test_batch();
        let exclude = vec!["name".to_string()];

        let result = apply_exclude_columns(batch, &exclude).unwrap();

        assert_eq!(result.num_columns(), 1); // only 'id' remains
        assert!(result.schema().field_with_name("id").is_ok());
        assert!(result.schema().field_with_name("name").is_err());
    }

    #[test]
    fn test_apply_exclude_columns_case_insensitive() {
        let batch = make_test_batch();
        let exclude = vec!["NAME".to_string()]; // uppercase

        let result = apply_exclude_columns(batch, &exclude).unwrap();

        assert_eq!(result.num_columns(), 1); // 'name' was dropped despite case difference
        assert!(result.schema().field_with_name("id").is_ok());
    }

    #[test]
    fn test_apply_value_injection_with_existing_column() {
        use crate::config::ArrowColumnDef;

        let batch = make_test_batch();
        let mut injections = HashMap::new();
        injections.insert("user_id".to_string(), ArrowColumnDef {
            value: Some("id".to_string()),
            ..Default::default()
        });

        let result = apply_value_injections(batch, &injections).unwrap();

        // id and name stay, user_id appended (id not consumed — still present)
        assert_eq!(result.num_columns(), 3);
        assert!(result.schema().field_with_name("user_id").is_ok());
        assert!(result.schema().field_with_name("name").is_ok());
    }

    #[test]
    fn test_apply_value_injection_with_env_var() {
        use crate::config::ArrowColumnDef;
        use crate::transform::expr::set_env_vars;

        // Setup: create an environment variable
        let mut env = HashMap::new();
        let timestamp_arr = Arc::new(StringArray::from(vec!["2026-03-11 12:00:00"])) as ArrayRef;
        env.insert("insert_ts".to_string(), timestamp_arr);
        set_env_vars(env);

        let batch = make_test_batch();
        let mut injections = HashMap::new();
        injections.insert("created_at".to_string(), ArrowColumnDef {
            value: Some("$insert_ts".to_string()),
            ..Default::default()
        });

        let result = apply_value_injections(batch, &injections).unwrap();

        assert_eq!(result.num_columns(), 3); // id, name, created_at
        let result_schema = result.schema();
        let created_field = result_schema.field_with_name("created_at").unwrap();
        assert_eq!(created_field.data_type(), &DataType::Utf8);

        let created_col = result.column_by_name("created_at").unwrap();
        let created_arr = created_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(created_arr.value(0), "2026-03-11 12:00:00");
        assert_eq!(created_arr.value(1), "2026-03-11 12:00:00");
        assert_eq!(created_arr.value(2), "2026-03-11 12:00:00");
    }

    #[test]
    fn test_apply_value_injection_with_null() {
        use crate::config::ArrowColumnDef;

        let batch = make_test_batch();
        let mut injections = HashMap::new();
        injections.insert("created_at".to_string(), ArrowColumnDef {
            value: Some("null".to_string()),
            arrow_type: Some("timestamp[us, UTC]".to_string()),
            ..Default::default()
        });

        let result = apply_value_injections(batch, &injections).unwrap();

        assert_eq!(result.num_columns(), 3); // id, name, created_at
        let result_schema = result.schema();
        let created_field = result_schema.field_with_name("created_at").unwrap();
        assert_eq!(created_field.data_type(), &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(std::sync::Arc::from("UTC"))));

        let created_col = result.column_by_name("created_at").unwrap();
        let created_arr = created_col.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();
        assert!(created_arr.is_null(0));
        assert!(created_arr.is_null(1));
        assert!(created_arr.is_null(2));
    }
}