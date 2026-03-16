//! Step-level debug logging: schema display, data head, per-step tracing.
//!
//! Called from `run_component_inner` when `ETLConfig.log_level >= LogLevel::Debug`.

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use tracing::debug;

use crate::db::common::field_meta::{SOURCE_DB_TYPE, SOURCE_LENGTH, SOURCE_PRECISION, SOURCE_SCALE};
use crate::schema::{
    ForeignKey,
    META_DB_TYPE, META_DESCRIPTION, META_FOREIGN_KEY, META_PRIMARY_KEY,
};

/// Number of rows shown in the data preview.
pub const HEAD_ROWS: usize = 10;

// ── Public logging entry-points ───────────────────────────────────────────────

pub fn debug_input(step_id: &str, label: &str, batches: &[RecordBatch]) {
    if !tracing::enabled!(tracing::Level::DEBUG) { return; }
    if batches.is_empty() {
        debug!("[{step_id}] {label} input: (empty)");
        return;
    }
    let schema_str = format_schema(batches[0].schema_ref(), batches.iter().map(|b| b.num_rows()).sum());
    let head_str   = format_head(batches);
    debug!("[{step_id}] {label} input schema:\n{schema_str}");
    debug!("[{step_id}] {label} input head (≤{HEAD_ROWS} rows):\n{head_str}");
}

pub fn debug_output(step_id: &str, batches: &[RecordBatch]) {
    if !tracing::enabled!(tracing::Level::DEBUG) { return; }
    if batches.is_empty() {
        debug!("[{step_id}] output: (empty — sink or empty result)");
        return;
    }
    let schema_str = format_schema(batches[0].schema_ref(), batches.iter().map(|b| b.num_rows()).sum());
    let head_str   = format_head(batches);
    debug!("[{step_id}] output schema:\n{schema_str}");
    debug!("[{step_id}] output head (≤{HEAD_ROWS} rows):\n{head_str}");
}

// ── Schema formatter ──────────────────────────────────────────────────────────

pub fn format_schema(schema: &SchemaRef, total_rows: usize) -> String {
    let fields = schema.fields();
    if fields.is_empty() { return "  (no fields)".to_string(); }

    const H_NAME:    &str = "name";
    const H_TYPE:    &str = "arrow type";
    const H_SRC:     &str = "source type";
    const H_PK:      &str = "pk";
    const H_NN:      &str = "nn";
    const H_DBTYPE:  &str = "db type";
    const H_FK:      &str = "foreign key";
    const H_DESC:    &str = "description";

    let mut w_name   = H_NAME.len();
    let mut w_type   = H_TYPE.len();
    let mut w_src    = H_SRC.len();
    let mut w_dbtype = H_DBTYPE.len();
    let mut w_fk     = H_FK.len();
    let mut w_desc   = H_DESC.len();

    struct Row {
        name: String, typ: String, src_type: String,
        pk: &'static str, nn: &'static str,
        db_type: String, time_mismatch: bool,
        fk: String, desc: String,
    }

    let rows: Vec<Row> = fields.iter().map(|f| {
        let meta = f.metadata();
        let src_type = format_source_type(meta);
        let raw_db_type = meta.get(META_DB_TYPE).cloned().unwrap_or_default();
        let time_mismatch = {
            let upper = raw_db_type.trim().to_ascii_uppercase();
            (upper == "TIME" || (upper.starts_with("TIME(") && upper.ends_with(')')))
                && upper != "TIME(7)"
        };
        let db_type = if time_mismatch { format!("{} ⚑", raw_db_type) } else { raw_db_type };
        let fk = meta.get(META_FOREIGN_KEY)
            .and_then(|s| serde_json::from_str::<ForeignKey>(s).ok())
            .map(|fk| match &fk.schema {
                Some(s) => format!("→{s}.{}.{}", fk.table, fk.column),
                None    => format!("→{}.{}", fk.table, fk.column),
            })
            .unwrap_or_default();
        let desc = meta.get(META_DESCRIPTION)
            .map(|d| if d.chars().count() > 50 {
                format!("{}…", d.chars().take(49).collect::<String>())
            } else { d.clone() })
            .unwrap_or_default();
        let typ = format!("{:?}", f.data_type());
        Row {
            name: f.name().clone(), typ, src_type,
            pk: if meta.get(META_PRIMARY_KEY).map(|v| v == "true").unwrap_or(false) { "✓" } else { "" },
            nn: if !f.is_nullable() { "✓" } else { "" },
            db_type, time_mismatch, fk, desc,
        }
    }).collect();

    let has_time_mismatch = rows.iter().any(|r| r.time_mismatch);

    for r in &rows {
        w_name   = w_name.max(r.name.len());
        w_type   = w_type.max(r.typ.len());
        w_src    = w_src.max(r.src_type.len());
        w_dbtype = w_dbtype.max(r.db_type.len());
        w_fk     = w_fk.max(r.fk.len());
        w_desc   = w_desc.max(r.desc.len());
    }
    w_desc = w_desc.min(52);
    let dbt_w = w_dbtype.max(H_DBTYPE.len());

    let sep = format!(
        "  {}  {}  {}  {}  {}  {}  {}  {}",
        "─".repeat(w_name), "─".repeat(w_type),
        "─".repeat(w_src.max(H_SRC.len())),
        "─".repeat(H_PK.len()), "─".repeat(H_NN.len()),
        "─".repeat(dbt_w), "─".repeat(w_fk), "─".repeat(w_desc),
    );

    let mut lines = Vec::with_capacity(rows.len() + 3);
    lines.push(format!("  ({} field{}, {total_rows} rows total)",
        fields.len(), if fields.len() == 1 { "" } else { "s" }));
    lines.push(format!(
        "  {H_NAME:<wn$}  {H_TYPE:<wt$}  {H_SRC:<ws$}  {H_PK:<wp$}  {H_NN:<wnn$}  {H_DBTYPE:<wd$}  {H_FK:<wf$}  {H_DESC}",
        wn = w_name, wt = w_type, ws = w_src.max(H_SRC.len()),
        wp = H_PK.len(), wnn = H_NN.len(), wd = dbt_w, wf = w_fk,
    ));
    lines.push(sep);

    for r in &rows {
        lines.push(format!(
            "  {name:<wn$}  {typ:<wt$}  {src:<ws$}  {pk:<wp$}  {nn:<wnn$}  {db:<wd$}  {fk:<wf$}  {desc}",
            name = r.name, typ = r.typ, src = r.src_type,
            pk = r.pk, nn = r.nn, db = r.db_type, fk = r.fk, desc = r.desc,
            wn = w_name, wt = w_type, ws = w_src.max(H_SRC.len()),
            wp = H_PK.len(), wnn = H_NN.len(), wd = dbt_w, wf = w_fk,
        ));
    }

    if has_time_mismatch {
        lines.push(String::new());
        lines.push("  ⚑ TIME(n) with n≠7 — promoted to TIME(7) in the DDL (full 100 ns precision).".to_string());
    }

    lines.join("\n")
}

// ── source type formatter ─────────────────────────────────────────────────────

fn format_source_type(meta: &std::collections::HashMap<String, String>) -> String {
    let base = match meta.get(SOURCE_DB_TYPE) {
        Some(s) if !s.is_empty() => s.as_str(),
        _ => return String::new(),
    };
    let precision = meta.get(SOURCE_PRECISION).and_then(|v| v.parse::<i32>().ok());
    let scale     = meta.get(SOURCE_SCALE).and_then(|v| v.parse::<i32>().ok());
    let length    = meta.get(SOURCE_LENGTH).and_then(|v| v.parse::<i64>().ok());

    match (precision, scale, length) {
        (Some(p), Some(s), _) if s > 0 => format!("{base}({p},{s})"),
        (_, _, Some(-1))               => format!("{base}(MAX)"),
        (_, _, Some(n))                => format!("{base}({n})"),
        (Some(p), _, _)                => format!("{base}({p})"),
        _                              => base.to_string(),
    }
}

// ── Data head formatter ────────────────────────────────────────────────────────

pub fn format_head(batches: &[RecordBatch]) -> String {
    let head = take_head(batches, HEAD_ROWS);
    if head.is_empty() { return "  (empty)".to_string(); }
    let display: Vec<RecordBatch> = head.iter().map(strip_ts_timezone).collect();
    match pretty_format_batches(&display) {
        Ok(table) => table.to_string().lines()
            .map(|l| format!("  {l}"))
            .collect::<Vec<_>>()
            .join("\n"),
        Err(e) => format!("  (pretty-format error: {e})"),
    }
}

fn take_head(batches: &[RecordBatch], n: usize) -> Vec<RecordBatch> {
    let mut result    = Vec::new();
    let mut remaining = n;
    for batch in batches {
        if remaining == 0 { break; }
        let take = remaining.min(batch.num_rows());
        result.push(batch.slice(0, take));
        remaining -= take;
    }
    result
}

fn strip_ts_timezone(batch: &RecordBatch) -> RecordBatch {
    let schema = batch.schema();
    let needs_change = schema.fields().iter().any(|f| {
        matches!(f.data_type(), DataType::Timestamp(_, Some(_)))
    });
    if !needs_change { return batch.clone(); }

    let mut new_fields: Vec<Field>  = Vec::with_capacity(schema.fields().len());
    let mut new_cols:   Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for (field, col) in schema.fields().iter().zip(batch.columns()) {
        match field.data_type() {
            DataType::Timestamp(unit, Some(_)) => {
                let no_tz = DataType::Timestamp(*unit, None);
                let (actual_dt, actual_col) =
                    arrow::compute::cast(col.as_ref(), &no_tz)
                        .map(|c| (no_tz.clone(), c))
                        .unwrap_or_else(|_| {
                            arrow::compute::cast(col.as_ref(), &DataType::Utf8)
                                .map(|c| (DataType::Utf8, c))
                                .unwrap_or_else(|_| (field.data_type().clone(), col.clone()))
                        });
                new_fields.push(
                    Field::new(field.name(), actual_dt, field.is_nullable())
                        .with_metadata(field.metadata().clone())
                );
                new_cols.push(actual_col);
            }
            _ => {
                new_fields.push(field.as_ref().clone());
                new_cols.push(col.clone());
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(new_fields)), new_cols)
        .unwrap_or_else(|_| batch.clone())
}
