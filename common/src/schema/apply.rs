//! Applies per-column Arrow overrides and metadata to `RecordBatch`es.
//!
//! ## Functions
//!
//! | Function | Purpose | Used by |
//! |---|---|------|
//! | [`apply_arrow_type_overrides`] | Arrow type casting (col → arrow_type) | all steps (source + sink) |
//! | [`apply_database_columns`]  | Stamp DDL constraints + db_type       | `write_db`, `scd2_sink` sinks |
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
//! | [`ArrowTypeOverridesPlan`] | `arrow_type_overrides` map | `arrow::compute::cast` only for columns that differ |
//! | [`MetadataStampPlan`]  | `database_columns` (incl. `db_type`) | Schema swap (Arc clone) — zero data copies |
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

// ── apply_arrow_type_overrides ────────────────────────────────────────────────────

/// Applies per-column Arrow type casts to a batch.
///
/// Keys are column names (case-insensitive).  Values are Arrow type strings
/// (`"utf8"`, `"int64"`, `"timestamp"`, etc.).
///
/// ## Example
///
/// ```yaml
/// arrow_type_overrides:
///   employee_id: utf8
///   salary: float64
/// ```
pub fn apply_arrow_type_overrides(
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
                "arrow_type_overrides cast for '{}' → '{}': {e}", field.name(), type_str
            ))?;
            cols[idx] = casted;
        }
        fields[idx] = Field::new(field.name(), target, field.is_nullable())
            .with_metadata(field.metadata().clone());

        tracing::debug!(
            column     = field.name(),
            arrow_type = type_str,
            "arrow_type_overrides: cast column"
        );
    }

    Ok(RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), cols)?)
}

// ── ArrowTypeOverridesPlan (compile-once) ───────────────────────────────────────

/// Per-column action in a compiled [`SchemaOverridePlan`].
enum OverrideAction {
    /// No change needed — passthrough.
    None,
    /// Raw Arrow cast to the target type.
    Cast(DataType),
}

/// Precomputed arrow-overrides plan.
///
/// Built once from the first batch's schema + the `arrow_type_overrides` map.
/// Subsequent batches call [`apply_arrow_type_overrides_plan`] which skips all
/// HashMap lookups, `parse_arrow_type` calls, and schema construction.
pub struct ArrowTypeOverridesPlan {
    /// Per-column action.
    actions: Vec<OverrideAction>,
    /// Fully-constructed output schema (reused by every batch).
    output_schema: SchemaRef,
}

/// Compile an [`ArrowTypeOverridesPlan`] from the first batch's schema.
///
/// Returns `None` when `overrides` is empty (no work needed — pass batches through).
pub fn compile_arrow_type_overrides(
    schema:    &SchemaRef,
    overrides: &HashMap<String, String>,
) -> anyhow::Result<Option<ArrowTypeOverridesPlan>> {
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
                "arrow_type_overrides: compiled cast"
            );
        } else {
            actions.push(OverrideAction::None);
            fields.push((**field).clone());
        }
    }

    if !any_work && fields.iter().zip(schema.fields().iter()).all(|(a, b)| a == b.as_ref()) {
        return Ok(None);
    }

    Ok(Some(ArrowTypeOverridesPlan {
        actions,
        output_schema: Arc::new(ArrowSchema::new(fields)),
    }))
}

/// Apply a precomputed [`ArrowTypeOverridesPlan`] to a batch.
///
/// Only columns with an action are touched; the output schema is an Arc
/// clone — no HashMap lookups, no type-string parsing.
pub fn apply_arrow_type_overrides_plan(
    batch: RecordBatch,
    plan:  &ArrowTypeOverridesPlan,
) -> anyhow::Result<RecordBatch> {
    let mut cols = batch.columns().to_vec();
    for (idx, action) in plan.actions.iter().enumerate() {
        match action {
            OverrideAction::None => {}
            OverrideAction::Cast(dt) => {
                let casted = cast(&*cols[idx], dt).map_err(|e| anyhow::anyhow!(
                    "arrow_type_overrides cast for '{}' → '{:?}': {e}",
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

/// Precomputed plan for [`apply_database_columns`].
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
/// Applies `database_columns` (including `db_type`) and caches the resulting
/// schema.  The `db_type` field in each [`ColumnOption`] is stamped as
/// `etl.db_type` metadata — the highest-priority SQL type for DDL generation.
///
/// Returns `None` when `database_columns` is empty (no work needed).
pub fn compile_metadata_stamps(
    schema:         &SchemaRef,
    database_columns: &HashMap<String, crate::schema::field::ColumnOption>,
) -> anyhow::Result<Option<MetadataStampPlan>> {
    if database_columns.is_empty() {
        return Ok(None);
    }

    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_lower = field.name().to_lowercase();
        let opt = database_columns
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

// ── apply_database_columns ─────────────────────────────────────────────────────

/// Stamps DDL-relevant `etl.*` metadata from a [`DatabaseColumnsMap`][crate::schema::DatabaseColumnsMap]
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
pub fn apply_database_columns(
    batch:          RecordBatch,
    database_columns: &HashMap<String, crate::schema::field::ColumnOption>,
) -> anyhow::Result<RecordBatch> {
    if database_columns.is_empty() {
        return Ok(batch);
    }

    let schema  = batch.schema();
    let cols    = batch.columns().to_vec();
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();

    for (idx, field) in schema.fields().iter().enumerate() {
        let col_lower = field.name().to_lowercase();

        let opt = database_columns
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
            "database_columns: stamped DDL hints"
        );
    }

    Ok(RecordBatch::try_new(ArrowSchema::new(fields).into(), cols)?)
}

// ── apply_value_injections ──────────────────────────────────────────────────

/// Applies value injections from `schema.arrow.columns` entries that have
/// a `value:` field set.
///
/// **Top-level dispatch** (the value string, exactly as written in YAML):
/// - `null` (case-insensitive) → null-fill column.
/// - `$name` (no dot) → broadcast a pipeline environment variable.
/// - `$step.col` (dot form) → copy from that batch column (case-insensitive).
///   `$source` is the conventional `step` when there's a single input.
/// - bare identifier / quoted string / int / float / bool → **broadcast as a literal**.
///   `value: 'xyz'` injects the string `xyz` into every row.
///   `value: 42` injects the integer 42.
/// - anything else (function calls, arithmetic, casts, …) → evaluate
///   the full expression against the pre-injection batch snapshot, e.g.
///   `truncate($source.old_value, 4000)`, `coalesce($source.a, $source.b)`.
///
/// If the target column already exists in the batch, its data is replaced.
/// If it does not exist, a new column is appended.
/// If `arrow_type` is set on the definition, the resolved data is cast
/// to that type after injection.
/// If `logical_type` is set, `etl.logical_type` metadata is stamped.
/// If `nullable` is set, the field's nullable flag is overridden.
///
/// Columns without a matching injection entry pass through unchanged.
///
/// ## Examples
///
/// ```yaml
/// schema:
///   arrow:
///     columns:
///       # env-var broadcast
///       inserted_at:
///         value: $insert_at_var
///         type: "timestamp[us, UTC]"
///         logical_type: timestamp
///       # literal string broadcast to every row
///       source_system:
///         value: 'genesys'
///       # in-place transform: truncate to 4000 chars for an NVARCHAR(4000) target
///       old_value:
///         value: truncate($source.old_value, 4000)
/// ```
pub fn apply_value_injections(
    batch: RecordBatch,
    injections: &HashMap<String, crate::config::ArrowColumnDef>,
) -> anyhow::Result<RecordBatch> {
    use crate::transform::expr::{broadcast_array, parse as parse_expr, eval as eval_expr, Expr};
    use crate::schema::constants::META_LOGICAL_TYPE;

    if injections.is_empty() {
        return Ok(batch);
    }

    let num_rows   = batch.num_rows();
    let old_schema = batch.schema();
    // Snapshot of the pre-injection batch — expression evaluation always sees
    // the original columns, never freshly-injected ones (matches the historical
    // bare-identifier / env-var behaviour).
    let eval_batch = batch.clone();

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
        // Top-level dispatch:
        //   Null              → null-fill
        //   EnvVar            → broadcast env var
        //   ColumnRef         → case-insensitive batch column copy
        //   Literals + bare   → broadcast as a literal value
        //   Function/binop/…  → eval against the pre-injection batch snapshot
        let null_dt = || -> anyhow::Result<arrow::datatypes::DataType> {
            Ok(col_def.arrow_type.as_ref()
                .map(|t| parse_arrow_type(t)).transpose()?
                .unwrap_or(arrow::datatypes::DataType::Utf8))
        };

        let lookup_col = |name: &str| -> anyhow::Result<ArrayRef> {
            let lower = name.to_lowercase();
            if let Some(&src_idx) = existing_lower.get(&lower) {
                Ok(arrays[src_idx].clone())
            } else {
                anyhow::bail!(
                    "value_injection: column '{}' for target '{}' not in batch \
                     (available: {})",
                    name, target_name,
                    existing_lower.keys().cloned().collect::<Vec<_>>().join(", ")
                );
            }
        };

        let resolved: ArrayRef = match parse_expr(value_ref) {
            Ok(Expr::Null) => arrow::array::new_null_array(&null_dt()?, num_rows),
            Ok(Expr::EnvVar(name)) => {
                if let Some(env_array) = lookup_env_var(&name) {
                    broadcast_array(&env_array, num_rows).map_err(|e| anyhow::anyhow!(
                        "value_injection: failed to broadcast env var '${}' for target '{}': {e}",
                        name, target_name
                    ))?
                } else {
                    anyhow::bail!(
                        "value_injection: env var '${}' for target '{}' is not defined",
                        name, target_name
                    );
                }
            }
            Ok(Expr::ColumnRef { step: _, col }) => lookup_col(&col)?,
            // ── Top-level literals ───────────────────────────────────────────
            // Bare identifiers, quoted strings, numbers, and bools are all
            // broadcast as constants. To reference a batch column, use
            // `$source.col` explicitly.
            Ok(Expr::Str(s))   => crate::transform::expr::broadcast_array(
                &(std::sync::Arc::new(arrow::array::StringArray::from(vec![s])) as ArrayRef),
                num_rows,
            )?,
            Ok(Expr::Column(name)) => crate::transform::expr::broadcast_array(
                &(std::sync::Arc::new(arrow::array::StringArray::from(vec![name])) as ArrayRef),
                num_rows,
            )?,
            Ok(Expr::Int(v))   => crate::transform::expr::broadcast_array(
                &(std::sync::Arc::new(arrow::array::Int64Array::from(vec![v])) as ArrayRef),
                num_rows,
            )?,
            Ok(Expr::Float(v)) => crate::transform::expr::broadcast_array(
                &(std::sync::Arc::new(arrow::array::Float64Array::from(vec![v])) as ArrayRef),
                num_rows,
            )?,
            Ok(Expr::Bool(b))  => crate::transform::expr::broadcast_array(
                &(std::sync::Arc::new(arrow::array::BooleanArray::from(vec![b])) as ArrayRef),
                num_rows,
            )?,
            // Complex expressions: function calls, arithmetic, casts, etc.
            Ok(expr) => eval_expr(&expr, &eval_batch).map_err(|e| anyhow::anyhow!(
                "value_injection: evaluating '{}' for target '{}': {e}",
                value_ref, target_name
            ))?,
            Err(parse_err) => anyhow::bail!(
                "value_injection: failed to parse '{}' for target '{}': {parse_err}",
                value_ref, target_name
            ),
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
                source = value_ref,
                "value_injection: replaced existing column"
            );
        } else {
            // Append new column.
            fields.push(field);
            arrays.push(resolved);
            tracing::debug!(
                target = target_name,
                source = value_ref,
                "value_injection: appended new column"
            );
        }
    }

    let new_schema = Arc::new(ArrowSchema::new(fields));
    Ok(RecordBatch::try_new(new_schema, arrays)?)
}

// ── apply_database_structural ───────────────────────────────────────────────

/// Applies the structural fields of a `DatabaseColumnsMap` — `rename_to`
/// and `drop` — to a batch. Runs **after** [`apply_database_columns`] so
/// that metadata (PK / type / default_expr / …) is stamped on the field
/// before it gets renamed; metadata follows the rename because Arrow's
/// `Field::with_metadata` is preserved across the rebuild.
///
/// Source-name match (the map key) is case-insensitive. The drop step
/// removes the column entirely (data + field). The rename step writes
/// the target name verbatim — case is preserved (no lowercasing).
///
/// Columns without a `rename_to` and without `drop: true` pass through
/// unchanged.
///
/// The data arrays are never cloned — only the schema is rebuilt.
pub fn apply_database_structural(
    batch: RecordBatch,
    database_columns: &HashMap<String, crate::schema::field::ColumnOption>,
) -> anyhow::Result<RecordBatch> {
    // Build case-insensitive lookup tables.
    let mut drops: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut renames: HashMap<String, String> = HashMap::new();
    for (src, opt) in database_columns.iter() {
        let lower = src.to_lowercase();
        if opt.drop {
            drops.insert(lower.clone());
        }
        if let Some(target) = &opt.rename_to {
            renames.insert(lower, target.clone());
        }
    }

    if drops.is_empty() && renames.is_empty() {
        return Ok(batch);
    }

    let schema = batch.schema();
    let cols   = batch.columns().to_vec();
    let mut new_fields: Vec<Field>    = Vec::with_capacity(schema.fields().len());
    let mut new_arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());

    for (idx, field) in schema.fields().iter().enumerate() {
        let lower = field.name().to_lowercase();
        if drops.contains(&lower) {
            tracing::debug!(column = field.name(), "database_structural: dropped");
            continue;
        }
        let new_name = renames.get(&lower).cloned().unwrap_or_else(|| field.name().clone());
        if new_name != *field.name() {
            tracing::debug!(from = field.name(), to = new_name, "database_structural: renamed");
        }
        new_fields.push(
            Field::new(new_name, field.data_type().clone(), field.is_nullable())
                .with_metadata(field.metadata().clone()),
        );
        new_arrays.push(cols[idx].clone());
    }

    Ok(RecordBatch::try_new(Arc::new(ArrowSchema::new(new_fields)), new_arrays)?)
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
    fn test_apply_value_injection_column_ref() {
        use crate::config::ArrowColumnDef;

        let batch = make_test_batch();
        let mut injections = HashMap::new();
        // `$source.id` → column reference (case-insensitive lookup).
        injections.insert("user_id".to_string(), ArrowColumnDef {
            value: Some("$source.id".to_string()),
            ..Default::default()
        });

        let result = apply_value_injections(batch, &injections).unwrap();

        // id and name stay, user_id appended (id not consumed — still present)
        assert_eq!(result.num_columns(), 3);
        assert!(result.schema().field_with_name("user_id").is_ok());
        assert!(result.schema().field_with_name("name").is_ok());
        // user_id carries the Int32 data from id (not a literal string).
        let uid = result.column_by_name("user_id").unwrap();
        assert_eq!(uid.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_apply_value_injection_literal_string() {
        use crate::config::ArrowColumnDef;

        let batch = make_test_batch();
        let mut injections = HashMap::new();
        // Bare scalar without `$` is a literal — broadcast to every row.
        injections.insert("source_system".to_string(), ArrowColumnDef {
            value: Some("'genesys'".to_string()),
            ..Default::default()
        });

        let result = apply_value_injections(batch, &injections).unwrap();

        assert_eq!(result.num_columns(), 3);
        let col = result.column_by_name("source_system").unwrap();
        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "genesys");
        assert_eq!(arr.value(1), "genesys");
        assert_eq!(arr.value(2), "genesys");
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