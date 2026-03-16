//! DAG-based pipeline orchestrator.
//!
//! ## Execution model
//!
//! Each component runs as an independent `tokio::spawn`-ed task.  Tasks are
//! connected by **bounded `mpsc` channels** (`channel_capacity` batches deep).
//! This gives two important properties:
//!
//! * **Constant memory** — at most `channel_capacity × batch_size` rows exist
//!   between any two adjacent stages at any one time.  A 500 M-row table
//!   never fully resides in memory.
//! * **Automatic back-pressure** — a slow sink stalls its upstream transform,
//!   which in turn stalls the source.  No explicit throttling is needed.
//!
//! ## Linear pipeline
//! ```text
//! Source ─► Filter ─► PythonTransform ─► Sink
//! ```
//! Each stage processes one `RecordBatch` and immediately forwards it.
//! Peak memory ≈ `channel_capacity × batch_size × row_size × num_stages`.
//!
//! ## Fan-out (one source, multiple sinks)
//! ```text
//!              ┌─► FilterEU ─► Sink (eu_orders)
//! Source ──────┤
//!              └─► FilterUS ─► Sink (us_orders)
//! ```
//! The source clones each batch (Arrow columns are `Arc`-backed; clone is
//! O(num_columns)) and sends to each consumer in sequence.  Both downstream
//! tasks run concurrently; the channel buffer decouples them.
//!
//! ## Fan-in (join)
//! ```text
//! Source A ──┐
//!            ├─► Join ─► PythonTransform ─► Sink
//! Source B ──┘
//! ```
//! Both sources run concurrently.  The join task **materialises the right
//! (build) side** into a hash map — unavoidable for a hash join.  The left
//! (probe) side is then streamed one batch at a time.
//! Put the smaller table on the right.

pub mod json;
pub mod yaml;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use futures::StreamExt;
use indexmap::IndexMap;
use tracing::{debug, info};

use potato_etl_common::file_transport::GlobSortOrder;
use crate::config::{CreateTableMode, ETLConfig, JoinHow, LogLevel, ReadOptions, SinkMode, SinkSchemaConfig, SourceSchemaConfig, StepDriverOptions, WriteOptions};
use crate::config::ConnParams;
use crate::db::{ReadDB, Scd2ColumnNames, WriteDB};
use crate::http::{fetch_rest_api, send_to_rest_api, RestApiOptions, RestApiSinkOptions};
use crate::schema::{
    apply_rename, apply_value_injections, apply_exclude_columns,
    ArrowOverridesPlan, compile_arrow_overrides, apply_arrow_overrides_plan,
    MetadataStampPlan, compile_metadata_stamps, apply_metadata_stamp_plan,
};
use crate::schema::field::ColumnOptionsMap;
use crate::transform::expr::{EvalContext, set_eval_context, set_env_vars, eval, parse};
use crate::transform::flatten::apply_flatten;
use crate::transform::unnest::{apply_unnest, UnnestConfig};
use crate::transform::objects::build_objects;
use crate::transform::map::apply_map;
use crate::transform::aggregate::apply_aggregate;
use crate::util::debug::{debug_input, debug_output, format_head};
use crate::util::schema::{apply_normalize_columns, should_normalize};

// ── TransformFn ───────────────────────────────────────────────────────────────

/// A synchronous transform function that runs in a `spawn_blocking` context.
///
/// Supports Rust closures and Python callbacks (via PyO3 + GIL).
pub type TransformFn = Arc<dyn Fn(RecordBatch) -> anyhow::Result<RecordBatch> + Send + Sync>;

// ── ComponentId ─────────────────────────────────────────────────────────────

pub type ComponentId = String;

// ── ComponentSpec ─────────────────────────────────────────────────────────────

pub(crate) enum ComponentSpec {
    Source        { conn_str: String, opts: ReadOptions },
    /// REST API source.
    RestApiSource { url: String, opts: RestApiOptions,
                    source_schema: SourceSchemaConfig },
    /// JSON file source (local or remote via file connection).
    JsonFileSource { path: String, data_path: Option<String>,
                     source_schema: SourceSchemaConfig,
                     batch_size: Option<usize>,
                     /// File connection (Local for legacy `path:` mode).
                     conn: ConnParams,
                     /// Sort order for glob-matched files (default: ascending name).
                     sort_glob: GlobSortOrder },
    /// One-to-one transform.
    ///
    /// `label` carries the original YAML step type for CLI introspection
    /// (e.g. `"filter"`, `"map"`).  `None` for transforms registered
    /// programmatically via [`Dag::add_transform`].
    Transform     { input: ComponentId, func: Option<TransformFn>, func_name: Option<String>,
                    /// Original step type string (e.g. `"filter"`, `"map"`, `"python_transform"`).
                    label: Option<&'static str> },
    /// Rename columns.
    ///
    /// `columns` is a simple input→output rename map.
    Rename        { input: ComponentId, columns: IndexMap<String, String> },
    /// Two-to-one join.
    Join          { left: ComponentId, right: ComponentId, on: String, how: JoinHow },
    /// Struct-flatten: extract nested fields from `StructArray` columns.
    Flatten       { input: ComponentId, select: IndexMap<String, String> },
    /// Array-unnest: explode a List/JSON-array column into one row per element.
    Unnest        { input: ComponentId, config: UnnestConfig },
    Sink          { input: ComponentId, conn_str: String, opts: WriteOptions },
    Scd2Sink      { input: ComponentId, conn_str: String, table: String, db_schema: Option<String>,
                    key_col: String, tracked: Vec<String>, col_names: Scd2ColumnNames,
                    create_table: CreateTableMode,
                    /// Shared sink-side schema settings.
                    sink_schema: SinkSchemaConfig,
                    /// Per-sink write batch size override (from YAML/JSON `batch_size:`).
                    /// Falls back to the global pipeline batch_size when `None`.
                    batch_size: Option<usize>,
                    /// Per-step driver-specific options (from YAML/JSON `options:`).
                    options: StepDriverOptions,
                    /// When `true`, keys present in the DB but absent from ALL
                    /// incoming batches are expired at flush time.
                    close_missing: bool },
    /// Group-by + metric aggregation.  Materialises ALL incoming batches before computing.
    Aggregate     { input: ComponentId,
                    group_by:     Vec<String>,
                    metrics:      IndexMap<String, String> },
    RestApiSink   { input: ComponentId, url: String, opts: RestApiSinkOptions },
    /// CSV file source (local or remote via file connection).
    CsvFileSource  { path: String, delimiter: u8, has_header: bool,
                     source_schema: SourceSchemaConfig,
                     batch_size: Option<usize>,
                     /// File connection (Local for legacy `path:` mode).
                     conn: ConnParams,
                     /// Sort order for glob-matched files (default: ascending name).
                     sort_glob: GlobSortOrder },
    /// CSV file sink (local or remote via file connection).
    CsvFileSink    { input: ComponentId, path: String, delimiter: u8, has_header: bool,
                     /// File connection (Local for legacy `path:` mode).
                     conn: ConnParams },
    /// JSON file sink (local or remote via file connection).
    JsonFileSink   { input: ComponentId, path: String, pretty: bool, wrap_key: Option<String>,
                     /// File connection (Local for legacy `path:` mode).
                     conn: ConnParams },
    /// Parquet file source (local or remote via file connection).
    ParquetFileSource { path: String, columns: Option<Vec<String>>,
                        source_schema: SourceSchemaConfig,
                        batch_size: Option<usize>,
                        conn: ConnParams,
                        /// Sort order for glob-matched files (default: ascending name).
                        sort_glob: GlobSortOrder },
    /// Parquet file sink (local or remote via file connection).
    ParquetFileSink   { input: ComponentId, path: String,
                        compression: String,
                        conn: ConnParams },
}

impl ComponentSpec {
    pub fn inputs(&self) -> Vec<&str> {
        match self {
            Self::Source        { .. }              => vec![],
            Self::RestApiSource { .. }              => vec![],
            Self::JsonFileSource { .. }             => vec![],
            Self::CsvFileSource { .. }              => vec![],
            Self::Transform     { input, .. }       => vec![input.as_str()],
            Self::Rename        { input, .. }       => vec![input.as_str()],
            Self::Flatten       { input, .. }       => vec![input.as_str()],
            Self::Unnest        { input, .. }       => vec![input.as_str()],
            Self::Join          { left, right, .. } => vec![left.as_str(), right.as_str()],
            Self::Sink          { input, .. }       => vec![input.as_str()],
            Self::Scd2Sink      { input, .. }       => vec![input.as_str()],
            Self::Aggregate     { input, .. }       => vec![input.as_str()],
            Self::RestApiSink   { input, .. }       => vec![input.as_str()],
            Self::CsvFileSink   { input, .. }       => vec![input.as_str()],
            Self::JsonFileSink  { input, .. }       => vec![input.as_str()],
            Self::ParquetFileSource { .. }           => vec![],
            Self::ParquetFileSink { input, .. }      => vec![input.as_str()],
        }
    }
}

// ── SpecKind (internal) ───────────────────────────────────────────────────────

#[derive(Copy, Clone)]
enum SpecKind {
    /// Output rows count as `rows_read` (sources).
    Source,
    /// Output rows count as `rows_written` (sinks).
    Sink,
    /// No contribution to top-level statistics (transforms, joins, renames).
    Transform,
}

// ── RunReport / ComponentStats ────────────────────────────────────────────────

/// Statistics returned after a successful `Dag::run()` call.
///
/// ## Quick display
///
/// ```no_run
/// let report = dag.run().await?;
/// println!("{report}");           // pretty-printed summary table
/// println!("{report:#?}");        // Rust debug dump
/// ```
///
/// The `Display` impl prints a formatted summary — total duration, rows,
/// throughput, and a per-component breakdown table.
#[derive(Debug, Default)]
pub struct RunReport {
    pub rows_read:       usize,
    pub rows_written:    usize,
    /// Number of data iterations that flowed through the pipeline.  Computed as
    /// the maximum iteration count across all components — NOT the sum, because
    /// every component processes the same data batches.
    pub iterations:      usize,
    pub per_component:   HashMap<ComponentId, ComponentStats>,
    /// Wall-clock duration of the entire pipeline (from first wave start to
    /// last wave end, excluding subscriber initialisation and validation).
    pub duration:        Duration,
    /// Time spent before any data row was produced by a source component.
    /// Includes connection setup, DDL checks, introspection, query planning,
    /// and similar one-time overhead.  `Duration::ZERO` when not measurable.
    pub startup_time:    Duration,
    /// Wall-clock time during which data was actively flowing through the
    /// pipeline.  Computed as `duration - startup_time`.
    pub active_duration: Duration,
    /// Rows processed per second during the **active** processing window.
    /// Computed as `rows_read / active_duration` (or `rows_written` when no
    /// source is present).  `0.0` when active_duration is zero.
    pub rows_per_second: f64,
    /// Component IDs in execution order (wave-by-wave, then insertion order
    /// within a wave).  Used by `Display` to print the breakdown in pipeline
    /// order rather than hash-map order.
    pub component_order: Vec<ComponentId>,
    /// Arrow schemas of each component's first output batch, captured during
    /// [`Dag::run_dry`].  Empty when the pipeline is executed with [`Dag::run`].
    pub schemas: IndexMap<ComponentId, arrow::datatypes::SchemaRef>,
    /// Pipeline environment variables that were active during the run.
    /// Keys are variable names; values are the string representation of the
    /// evaluated expression result.  Empty when no `environment:` block was defined.
    pub environment: IndexMap<String, String>,
}

impl std::fmt::Display for RunReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const SEP: &str = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";

        let dur_secs = self.duration.as_secs_f64();
        let dur_str  = if dur_secs < 1.0 {
            format!("{} ms", self.duration.as_millis())
        } else if dur_secs < 60.0 {
            format!("{:.3} s", dur_secs)
        } else {
            let m = self.duration.as_secs() / 60;
            let s = dur_secs - (m as f64 * 60.0);
            format!("{}m {:06.3}s", m, s)
        };

        let startup_secs = self.startup_time.as_secs_f64();
        let startup_str = if startup_secs < 1.0 {
            format!("{} ms", self.startup_time.as_millis())
        } else {
            format!("{:.3} s", startup_secs)
        };

        let active_secs = self.active_duration.as_secs_f64();
        let active_str = if active_secs < 1.0 {
            format!("{} ms", self.active_duration.as_millis())
        } else if active_secs < 60.0 {
            format!("{:.3} s", active_secs)
        } else {
            let m = self.active_duration.as_secs() / 60;
            let s = active_secs - (m as f64 * 60.0);
            format!("{}m {:06.3}s", m, s)
        };

        let tput_str = if self.rows_per_second > 0.0 && self.rows_per_second.is_finite() {
            format!("{} rows/s", fmt_num(self.rows_per_second as usize))
        } else {
            "—".to_string()
        };

        writeln!(f, "{SEP}")?;
        writeln!(f, "  {:<16} {}", "Duration",     dur_str)?;
        writeln!(f, "  {:<16} {} (startup) + {} (active)",
            "Breakdown", startup_str, active_str)?;
        writeln!(f, "  {:<16} {}", "Rows read",    fmt_num(self.rows_read))?;
        writeln!(f, "  {:<16} {}", "Rows written", fmt_num(self.rows_written))?;
        writeln!(f, "  {:<16} {}", "Iterations",   fmt_num(self.iterations))?;
        writeln!(f, "  {:<16} {}", "Throughput",   tput_str)?;
        writeln!(f, "{SEP}")?;
        writeln!(f, "  {:<24}  {:>9}  {:>9}  {:>7}  {:>7}",
            "Component", "rows in", "rows out", "iters", "ms")?;
        writeln!(f, "  {}", "─".repeat(58))?;

        // Print in pipeline execution order.
        let ids: Vec<&str> = if !self.component_order.is_empty() {
            self.component_order.iter().map(|s| s.as_str()).collect()
        } else {
            let mut v: Vec<&str> = self.per_component.keys().map(|s| s.as_str()).collect();
            v.sort_unstable();
            v
        };

        for id in ids {
            if let Some(s) = self.per_component.get(id) {
                let ri = if s.rows_in  == 0 { "—".to_string() } else { fmt_num(s.rows_in) };
                let ro = if s.rows_out == 0 { "—".to_string() } else { fmt_num(s.rows_out) };
                writeln!(f, "  {:<24}  {:>9}  {:>9}  {:>7}  {:>7}",
                    id, ri, ro, fmt_num(s.batches), s.duration_ms)?;
            }
        }

        // Print environment variables (if any).
        if !self.environment.is_empty() {
            writeln!(f, "{SEP}")?;
            writeln!(f, "  Environment variables:")?;
            for (name, val) in &self.environment {
                let display = if val.len() > 60 {
                    // Truncate at a char boundary to avoid panicking on multi-byte UTF-8.
                    let truncated: String = val.chars().take(57).collect();
                    format!("{truncated}…")
                } else {
                    val.clone()
                };
                writeln!(f, "    ${:<20} = {}", name, display)?;
            }
        }

        write!(f, "{SEP}")
    }
}

/// Formats `n` with thousands separators (e.g. `1234567` → `"1,234,567"`).
fn fmt_num(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { out.push(','); }
        out.push(c);
    }
    out.chars().rev().collect()
}

#[derive(Debug, Default)]
pub struct ComponentStats {
    pub rows_in:     usize,
    pub rows_out:    usize,
    /// Number of iterations (channel receives) this component processed.
    pub batches:     usize,
    /// Wall-clock time spent inside this component, in milliseconds.
    pub duration_ms: u64,
    /// Duration since pipeline start when this component first produced or
    /// consumed a data row.  `None` if the component never saw any data.
    pub first_data_at: Option<Duration>,
    /// Row count at which the last progress INFO was emitted.
    /// Used to trigger progress at fixed row-count intervals (every 500K)
    /// instead of batch-count intervals, so reporting density stays
    /// consistent regardless of batch size.
    last_progress_rows: usize,
}

impl ComponentStats {
    /// Increment the batch counter and, on the very first batch, record the
    /// time relative to pipeline start.  This gives us the startup overhead
    /// per component without touching every data-path call site individually.
    #[inline]
    fn record_batch(&mut self, pipeline_started: Instant) {
        self.batches += 1;
        if self.batches == 1 {
            self.first_data_at = Some(pipeline_started.elapsed());
        }
    }
}

// ── StepKind / StepSummary ────────────────────────────────────────────────────

/// The broad category of a pipeline step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepKind {
    Source,
    Transform,
    Sink,
}

impl std::fmt::Display for StepKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source    => write!(f, "source"),
            Self::Transform => write!(f, "transform"),
            Self::Sink      => write!(f, "sink"),
        }
    }
}

/// Static description of a single pipeline step.
///
/// Returned by [`Dag::steps`] and [`Dag::step_info`].
#[derive(Debug, Clone)]
pub struct StepSummary {
    /// The step ID as declared in the config.
    pub id:        String,
    /// Broad category: source / transform / sink.
    pub kind:      StepKind,
    /// Fine-grained type: `"read_db"`, `"rest_api_source"`, `"write_db"`,
    /// `"scd2_sink"`, `"rest_api_sink"`, `"transform"`, `"python_transform"`,
    /// `"rename"`, `"join"`, `"flatten"`, `"aggregate"`.
    pub step_type: &'static str,
    /// IDs of this step's upstream inputs (empty for sources).
    pub inputs:    Vec<String>,
}

// ── SchemaCollector (dry-run only) ────────────────────────────────────────────

/// Shared map populated by `run_dry` with the Arrow schema of each step's
/// first output batch.  Uses `DashMap` for lock-free concurrent schema capture
/// across parallel component tasks.  Each component writes exactly once (first
/// batch), so contention is minimal, but DashMap avoids blocking entirely.
type SchemaCollector = Arc<DashMap<ComponentId, arrow::datatypes::SchemaRef>>;

// ── Dag ───────────────────────────────────────────────────────────────────────

/// The pipeline definition: a directed acyclic graph of ETL components.
pub struct Dag {
    pub(crate) config:           ETLConfig,
    pub(crate) order:            Vec<ComponentId>,
    pub(crate) specs:            IndexMap<ComponentId, ComponentSpec>,
    pub(crate) named_transforms: HashMap<String, TransformFn>,
    id_counter: usize,
    /// Inline Python code blocks loaded from YAML/JSON config.
    ///
    /// Key   = internal transform name (`"__inline_{step_id}"`).
    /// Value = Python source string that will be `exec()`-ed per batch.
    ///
    /// These are populated by the JSON/YAML loaders when a `python_transform` step
    /// contains a `code` field.  The Python wheel auto-registers them on `from_yaml()`
    /// and `from_json()`.  In pure-Rust contexts, call `dag.inline_python_codes()` and
    /// register an executor manually before calling `dag.run()`.
    pub inline_python_codes: HashMap<String, String>,
    /// Universal per-step column value mapping.
    ///
    /// Populated by the JSON/YAML loaders when a step contains a `values:` block.
    /// Applied as a post-processing step after each component processes its batch,
    /// **before** sending to downstream consumers.
    /// Pipeline run start time (microseconds since Unix epoch, UTC).
    /// Captured once at the start of `run_inner()` and used by `run_ts()`
    /// expressions so that every batch sees the same timestamp.
    run_start_us: std::sync::Arc<std::sync::atomic::AtomicI64>,
    /// Pipeline environment variables — raw expression strings evaluated once
    /// at the start of `run_inner()`.  Results are stored as 1-element Arrow
    /// arrays and made available to step expressions via `$name` / `env("name")`.
    pub environment: IndexMap<String, String>,
    /// Pre-evaluated environment values.  Populated by `run_inner()` after
    /// evaluating the `environment` expressions.  Shared with transform
    /// closures via `Arc`.
    env_values: std::sync::Arc<std::sync::RwLock<HashMap<String, arrow::array::ArrayRef>>>,
}

impl Dag {
    // ── Constructors ─────────────────────────────────────────────────────────

    pub fn new(config: ETLConfig) -> Self {
        Self {
            config,
            order:               Vec::new(),
            specs:               IndexMap::new(),
            named_transforms:    HashMap::new(),
            id_counter:          0,
            inline_python_codes: HashMap::new(),
            run_start_us:        std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0)),
            environment:         IndexMap::new(),
            env_values:          std::sync::Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }

    pub fn next_id(&mut self, prefix: &str) -> ComponentId {
        self.id_counter += 1;
        format!("{prefix}_{}", self.id_counter)
    }

    /// Returns the number of components registered in this pipeline.
    pub fn len(&self) -> usize { self.order.len() }

    /// Returns `true` if no components have been registered yet.
    pub fn is_empty(&self) -> bool { self.order.is_empty() }

    /// Set a pipeline environment variable (expression string evaluated at run start).
    ///
    /// ```rust,no_run
    /// # use potato_etl_runtime::{Dag, config::ETLConfig};
    /// let mut dag = Dag::new(ETLConfig::default());
    /// dag.set_env("load_ts", "now()");
    /// dag.set_env("label", r#""nightly_sync""#);
    /// ```
    pub fn set_env(&mut self, name: impl Into<String>, expr: impl Into<String>) {
        self.environment.insert(name.into(), expr.into());
    }

    // ── Component registration ────────────────────────────────────────────────

    pub fn add_source(
        &mut self,
        id:       impl Into<ComponentId>,
        conn_str: impl Into<String>,
        opts:     ReadOptions,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::Source { conn_str: conn_str.into(), opts });
        self.order.push(id);
    }

    pub fn add_filter(
        &mut self,
        id:        impl Into<ComponentId>,
        input:     impl Into<ComponentId>,
        column:    Option<String>,
        value:     Option<String>,
        condition: Option<String>,
    ) {
        let id   = id.into();
        let run_start = self.run_start_us.clone();
        let env_vals = self.env_values.clone();
        let func: TransformFn = match condition {
            Some(cond) => Arc::new(move |batch| {
                set_eval_context(EvalContext {
                    run_start_us: run_start.load(std::sync::atomic::Ordering::Relaxed),
                });
                ensure_env_vars_set(&env_vals);
                crate::transform::filter::apply_filter_expr(batch, &cond)
            }),
            None => {
                let col = column.unwrap_or_default();
                let val = value.unwrap_or_default();
                Arc::new(move |batch| {
                    crate::transform::filter::apply_filter(&batch, &col, &val)
                })
            }
        };
        self.specs.insert(id.clone(), ComponentSpec::Transform {
            input: input.into(), func: Some(func), func_name: None,
            label: Some("filter"),
        });
        self.order.push(id);
    }

    /// Add a `map` step: compute/add/rename columns via the expression DSL.
    pub fn add_map(
        &mut self,
        id:          impl Into<ComponentId>,
        input:       impl Into<ComponentId>,
        columns:     IndexMap<String, String>,
        select_only: bool,
    ) {
        let id   = id.into();
        let run_start = self.run_start_us.clone();
        let env_vals = self.env_values.clone();
        let func: TransformFn = Arc::new(move |batch| {
            set_eval_context(EvalContext {
                run_start_us: run_start.load(std::sync::atomic::Ordering::Relaxed),
            });
            ensure_env_vars_set(&env_vals);
            apply_map(batch, &columns, select_only)
        });
        self.specs.insert(id.clone(), ComponentSpec::Transform {
            input: input.into(), func: Some(func), func_name: None,
            label: Some("map"),
        });
        self.order.push(id);
    }

    /// Add a group-by + aggregate step.
    pub fn add_aggregate(
        &mut self,
        id:       impl Into<ComponentId>,
        input:    impl Into<ComponentId>,
        group_by: Vec<String>,
        metrics:  IndexMap<String, String>,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::Aggregate {
            input: input.into(), group_by, metrics,
        });
        self.order.push(id);
    }

    pub fn add_transform(
        &mut self,
        id:    impl Into<ComponentId>,
        input: impl Into<ComponentId>,
        func:  TransformFn,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::Transform {
            input: input.into(), func: Some(func), func_name: None,
            label: None,
        });
        self.order.push(id);
    }

    /// Adds a named Python transform (for JSON config; name is resolved at run time).
    pub fn add_named_transform(
        &mut self,
        id:        impl Into<ComponentId>,
        input:     impl Into<ComponentId>,
        func_name: impl Into<String>,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::Transform {
            input: input.into(), func: None, func_name: Some(func_name.into()),
            label: Some("python_transform"),
        });
        self.order.push(id);
    }

    pub fn add_rename(
        &mut self,
        id:      impl Into<ComponentId>,
        input:   impl Into<ComponentId>,
        columns: IndexMap<String, String>,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::Rename {
            input: input.into(), columns,
        });
        self.order.push(id);
    }

    pub fn add_join(
        &mut self,
        id:    impl Into<ComponentId>,
        left:  impl Into<ComponentId>,
        right: impl Into<ComponentId>,
        on:    impl Into<String>,
        how:   JoinHow,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::Join {
            left: left.into(), right: right.into(), on: on.into(), how,
        });
        self.order.push(id);
    }

    /// Add a struct-flatten step.
    ///
    /// `select` maps output column names to dot-notation paths
    /// (e.g. `"city" → "address.city"`).  Pass an empty map to auto-expand
    /// all `StructArray` columns one level.
    pub fn add_flatten(
        &mut self,
        id:     impl Into<ComponentId>,
        input:  impl Into<ComponentId>,
        select: IndexMap<String, String>,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::Flatten {
            input: input.into(), select,
        });
        self.order.push(id);
    }

    /// Add an array-unnest step.
    ///
    /// Explodes a List/LargeList/JSON-array column into one row per element,
    /// optionally extracting sub-fields and carrying forward parent columns.
    pub fn add_unnest(
        &mut self,
        id:            impl Into<ComponentId>,
        input:         impl Into<ComponentId>,
        column:        String,
        fields:        IndexMap<String, String>,
        parent_fields: IndexMap<String, String>,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::Unnest {
            input: input.into(),
            config: UnnestConfig { column, fields, parent_fields },
        });
        self.order.push(id);
    }

    pub fn add_sink(
        &mut self,
        id:       impl Into<ComponentId>,
        input:    impl Into<ComponentId>,
        conn_str: impl Into<String>,
        opts:     WriteOptions,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::Sink {
            input: input.into(), conn_str: conn_str.into(), opts,
        });
        self.order.push(id);
    }

    pub fn add_scd2_sink(
        &mut self,
        id:           impl Into<ComponentId>,
        input:        impl Into<ComponentId>,
        conn_str:     impl Into<String>,
        table:        impl Into<String>,
        db_schema:    Option<String>,
        key_col:      impl Into<String>,
        tracked:      Vec<String>,
        col_names:    Scd2ColumnNames,
        create_table: CreateTableMode,
        sink_schema:  SinkSchemaConfig,
        batch_size:   Option<usize>,
        options:      StepDriverOptions,
        close_missing: bool,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::Scd2Sink {
            input: input.into(), conn_str: conn_str.into(), table: table.into(),
            db_schema, key_col: key_col.into(), tracked, col_names, create_table,
            sink_schema,
            batch_size,
            options,
            close_missing,
        });
        self.order.push(id);
    }

    pub fn add_rest_api(
        &mut self,
        id:              impl Into<ComponentId>,
        url:             impl Into<String>,
        opts:            RestApiOptions,
        source_schema:   SourceSchemaConfig,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::RestApiSource {
            url: url.into(), opts, source_schema,
        });
        self.order.push(id);
    }

    pub fn add_json_source(
        &mut self,
        id:            impl Into<ComponentId>,
        path:          impl Into<String>,
        data_path:     Option<String>,
        source_schema: SourceSchemaConfig,
        batch_size:    Option<usize>,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::JsonFileSource {
            path: path.into(), data_path, source_schema, batch_size,
            conn: ConnParams::Local { base_path: String::new() },
            sort_glob: GlobSortOrder::default(),
        });
        self.order.push(id);
    }

    /// Add a JSON file source using a resolved file connection.
    pub fn add_json_source_from(
        &mut self,
        id:            impl Into<ComponentId>,
        conn:          ConnParams,
        path:          impl Into<String>,
        data_path:     Option<String>,
        source_schema: SourceSchemaConfig,
        batch_size:    Option<usize>,
        sort_glob:     GlobSortOrder,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::JsonFileSource {
            path: path.into(), data_path, source_schema, batch_size, conn, sort_glob,
        });
        self.order.push(id);
    }

    pub fn add_csv_source(
        &mut self,
        id:            impl Into<ComponentId>,
        path:          impl Into<String>,
        delimiter:     String,
        has_header:    bool,
        source_schema: SourceSchemaConfig,
        batch_size:    Option<usize>,
    ) {
        let id = id.into();
        let delim_byte = delimiter.as_bytes().first().copied().unwrap_or(b',');
        self.specs.insert(id.clone(), ComponentSpec::CsvFileSource {
            path: path.into(), delimiter: delim_byte, has_header, source_schema, batch_size,
            conn: ConnParams::Local { base_path: String::new() },
            sort_glob: GlobSortOrder::default(),
        });
        self.order.push(id);
    }

    /// Add a CSV file source using a resolved file connection.
    pub fn add_csv_source_from(
        &mut self,
        id:            impl Into<ComponentId>,
        conn:          ConnParams,
        path:          impl Into<String>,
        delimiter:     String,
        has_header:    bool,
        source_schema: SourceSchemaConfig,
        batch_size:    Option<usize>,
        sort_glob:     GlobSortOrder,
    ) {
        let id = id.into();
        let delim_byte = delimiter.as_bytes().first().copied().unwrap_or(b',');
        self.specs.insert(id.clone(), ComponentSpec::CsvFileSource {
            path: path.into(), delimiter: delim_byte, has_header, source_schema, batch_size, conn, sort_glob,
        });
        self.order.push(id);
    }

    pub fn add_csv_sink(
        &mut self,
        id:         impl Into<ComponentId>,
        input:      impl Into<ComponentId>,
        path:       impl Into<String>,
        delimiter:  String,
        has_header: bool,
    ) {
        let id = id.into();
        let delim_byte = delimiter.as_bytes().first().copied().unwrap_or(b',');
        self.specs.insert(id.clone(), ComponentSpec::CsvFileSink {
            input: input.into(), path: path.into(), delimiter: delim_byte, has_header,
            conn: ConnParams::Local { base_path: String::new() },
        });
        self.order.push(id);
    }

    /// Add a CSV file sink using a resolved file connection.
    pub fn add_csv_sink_from(
        &mut self,
        id:         impl Into<ComponentId>,
        input:      impl Into<ComponentId>,
        conn:       ConnParams,
        path:       impl Into<String>,
        delimiter:  String,
        has_header: bool,
    ) {
        let id = id.into();
        let delim_byte = delimiter.as_bytes().first().copied().unwrap_or(b',');
        self.specs.insert(id.clone(), ComponentSpec::CsvFileSink {
            input: input.into(), path: path.into(), delimiter: delim_byte, has_header, conn,
        });
        self.order.push(id);
    }

    pub fn add_json_sink(
        &mut self,
        id:       impl Into<ComponentId>,
        input:    impl Into<ComponentId>,
        path:     impl Into<String>,
        pretty:   bool,
        wrap_key: Option<String>,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::JsonFileSink {
            input: input.into(), path: path.into(), pretty, wrap_key,
            conn: ConnParams::Local { base_path: String::new() },
        });
        self.order.push(id);
    }

    /// Add a JSON file sink using a resolved file connection.
    pub fn add_json_sink_from(
        &mut self,
        id:       impl Into<ComponentId>,
        input:    impl Into<ComponentId>,
        conn:     ConnParams,
        path:     impl Into<String>,
        pretty:   bool,
        wrap_key: Option<String>,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::JsonFileSink {
            input: input.into(), path: path.into(), pretty, wrap_key, conn,
        });
        self.order.push(id);
    }

    /// Add a Parquet file source using a resolved file connection.
    pub fn add_parquet_source_from(
        &mut self,
        id:            impl Into<ComponentId>,
        conn:          ConnParams,
        path:          impl Into<String>,
        columns:       Option<Vec<String>>,
        source_schema: SourceSchemaConfig,
        batch_size:    Option<usize>,
        sort_glob:     GlobSortOrder,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::ParquetFileSource {
            path: path.into(), columns, source_schema, batch_size, conn, sort_glob,
        });
        self.order.push(id);
    }

    /// Add a Parquet file sink using a resolved file connection.
    pub fn add_parquet_sink_from(
        &mut self,
        id:          impl Into<ComponentId>,
        input:       impl Into<ComponentId>,
        conn:        ConnParams,
        path:        impl Into<String>,
        compression: impl Into<String>,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::ParquetFileSink {
            input: input.into(), path: path.into(), compression: compression.into(), conn,
        });
        self.order.push(id);
    }

    pub fn add_rest_api_sink(
        &mut self,
        id:    impl Into<ComponentId>,
        input: impl Into<ComponentId>,
        url:   impl Into<String>,
        opts:  RestApiSinkOptions,
    ) {
        let id = id.into();
        self.specs.insert(id.clone(), ComponentSpec::RestApiSink {
            input: input.into(), url: url.into(), opts,
        });
        self.order.push(id);
    }

    pub fn add_object_builder(
        &mut self,
        id:         impl Into<ComponentId>,
        input:      impl Into<ComponentId>,
        field_map:  HashMap<String, String>,
        output_col: String,
    ) {
        let func: TransformFn = Arc::new(move |batch| {
            build_objects(batch, &field_map, &output_col)
        });
        self.add_transform(id, input, func);
    }

    pub fn register_transform(&mut self, name: impl Into<String>, func: TransformFn) {
        self.named_transforms.insert(name.into(), func);
    }

    pub fn add_lowercase_columns(
        &mut self,
        id:    impl Into<ComponentId>,
        input: impl Into<ComponentId>,
    ) {
        let func: TransformFn = Arc::new(|batch| apply_normalize_columns(batch));
        self.add_transform(id, input, func);
    }

    // ── Inline Python code accessor ───────────────────────────────────────────

    /// Returns inline Python code blocks stored by the JSON/YAML loader.
    ///
    /// Key   = internal transform name (e.g. `"__inline_add_bonus"`).
    /// Value = Python source string.
    ///
    /// The Python wheel auto-registers these; pure-Rust callers can iterate
    /// this map and register a subprocess-based executor before calling `run()`.
    pub fn inline_python_codes(&self) -> &HashMap<String, String> {
        &self.inline_python_codes
    }

    // ── Post-construction patching ────────────────────────────────────────────

    /// Attach (or replace) `column_options` for an already-registered sink step.
    /// Only `Sink` and `Scd2Sink` accept column options; other step types
    /// are silently ignored.
    ///
    /// A no-op when `column_options` is empty or `id` is not found.
    pub fn patch_column_options(&mut self, id: &str, column_options: ColumnOptionsMap) {
        if column_options.is_empty() { return; }
        let Some(spec) = self.specs.get_mut(id) else { return };
        // Convert ColumnOptionsMap → DatabaseSchemaConfig.columns
        let db_columns: indexmap::IndexMap<String, crate::config::DatabaseColumnDef> = column_options.into_iter()
            .map(|(name, co)| (name, crate::config::DatabaseColumnDef {
                db_type:        co.db_type,
                primary_key:    co.primary_key,
                nullable:       co.nullable,
                unique:         co.unique,
                generated:      false,
                default_expr:   co.default_expr,
                on_update_expr: co.on_update_expr,
                check_expr:     co.check_expr,
                foreign_key:    co.foreign_key,
                description:    co.description,
                enum_values:    None,
            }))
            .collect();
        let db_config = crate::config::DatabaseSchemaConfig {
            columns: db_columns,
            ..Default::default()
        };
        let patch_schema = |s: &mut SinkSchemaConfig| {
            let db = s.schema.database.get_or_insert_with(Default::default);
            db.columns = db_config.columns.clone();
        };
        match spec {
            ComponentSpec::Sink { opts, .. } => patch_schema(&mut opts.sink_schema),
            ComponentSpec::Scd2Sink { sink_schema, .. } => patch_schema(sink_schema),
            _ => {} // silently ignore — column_options only applies to sinks
        }
    }

    // ── Runtime overrides ────────────────────────────────────────────────────

    /// Override the global batch size for all sources in this pipeline.
    ///
    /// Per-step `batch_size` values in the config take precedence over this.
    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.config.batch_size = batch_size;
    }

    /// Override the log level for this pipeline.
    ///
    /// Useful for CLI flags (`-v` / `--trace`) that bump verbosity without
    /// re-parsing the config file.
    pub fn set_log_level(&mut self, level: LogLevel) {
        self.config.log_level = level;
    }

    /// Returns the current batch size setting.
    pub fn batch_size(&self) -> usize { self.config.batch_size }

    // ── Introspection ───────────────────────────���─────────────────────────────

    /// Returns a [`StepSummary`] for every step in pipeline order.
    ///
    /// This is a purely static operation — no database connection is made.
    pub fn steps(&self) -> Vec<StepSummary> {
        self.order.iter()
            .filter_map(|id| self.step_info(id))
            .collect()
    }

    /// Returns the [`StepSummary`] for a single step by ID, or `None` if the
    /// ID is not found in this pipeline.
    pub fn step_info(&self, id: &str) -> Option<StepSummary> {
        let spec = self.specs.get(id)?;
        Some(StepSummary {
            id:        id.to_string(),
            kind:      spec_kind(spec),
            step_type: spec_type(spec),
            inputs:    spec.inputs().iter().map(|s| s.to_string()).collect(),
        })
    }

    // ── Validation ────────────────────────────────────────────────────────────

    /// Validates the DAG's wiring without connecting to any database.
    ///
    /// Checks that every input reference resolves to a step that has already
    /// been declared (topological ordering) and that no cycles exist.
    /// Returns an error describing the first violation found.
    pub fn validate(&self) -> anyhow::Result<()> {
        // ── Duplicate ID check ────────────────────────────────────────────
        let mut defined = std::collections::HashSet::new();
        for id in &self.order {
            anyhow::ensure!(
                defined.insert(id.as_str()),
                "Duplicate step id '{id}'. Every step must have a unique `id`. \
                 Rename one of the duplicate '{id}' steps \
                 (e.g. '{id}_pg' and '{id}_oracle')."
            );
        }

        // ── Input reference check ────────────────────────────────────────
        let mut seen = std::collections::HashSet::new();
        for id in &self.order {
            let spec = &self.specs[id];
            for input in spec.inputs() {
                anyhow::ensure!(
                    seen.contains(input),
                    "Component '{id}' references '{input}' which has not been defined yet. \
                     Add '{input}' before '{id}'."
                );
            }
            seen.insert(id.as_str());
        }
        Ok(())
    }

    // ── Execution ─────────────────────────────────────────────────────────────

    /// Runs the pipeline and returns a [`RunReport`].
    ///
    /// ## Memory model
    ///
    /// Each component runs as an independent `tokio` task connected to its
    /// neighbours by a bounded `mpsc` channel (`channel_capacity` batches).
    /// Peak memory per pipeline edge ≈ `channel_capacity × batch_size × row_size`.
    /// At the default (4 × 1 000 × ~200 B) that is roughly **800 kB per
    /// edge** — constant regardless of total table size.
    ///
    /// ## Logging
    ///
    /// On first call, attempts to initialise a `tracing-subscriber` at the
    /// level set in `ETLConfig.log_level`.  If your application has already
    /// called `tracing_subscriber::fmt::init()` (or similar), this is a
    /// silent no-op.
    pub async fn run(self) -> anyhow::Result<RunReport> {
        self.run_inner(None, false).await
    }

    /// **Dry-run**: execute the pipeline without writing to any sink.
    ///
    /// All `write_db`, `scd2_sink`, and `rest_api_sink` steps are replaced
    /// with **no-op drains** before execution begins — **no data is committed**
    /// to any database, table, or external API, regardless of the pipeline
    /// config.  This guarantee holds even if `run_dry` is called on a
    /// production pipeline config.
    ///
    /// Sources are limited to `batch_limit` batches (use `1` to sample a
    /// single batch per source — the typical use-case for schema inspection).
    ///
    /// The returned [`RunReport`] includes the Arrow schema of each step's
    /// first output batch in [`RunReport::schemas`], keyed by step ID.
    pub async fn run_dry(mut self, batch_limit: usize) -> anyhow::Result<RunReport> {
        // ── Convert all sinks to no-op pass-through transforms ────────────────
        //
        // Each sink spec is replaced with an identity Transform that reads
        // incoming batches, counts them, and drops them (since terminal steps
        // have no downstream consumers, `send_to_all` with an empty `out_txs`
        // is a no-op — the batch is silently dropped).
        //
        // This is done BEFORE `run_inner` is called, so the actual DB/API
        // write code paths are never entered.
        for id in self.order.clone() {
            let input_id_opt: Option<ComponentId> = match &self.specs[&id] {
                ComponentSpec::Sink        { input, .. } => Some(input.clone()),
                ComponentSpec::Scd2Sink    { input, .. } => Some(input.clone()),
                ComponentSpec::RestApiSink { input, .. } => Some(input.clone()),
                ComponentSpec::CsvFileSink { input, .. } => Some(input.clone()),
                ComponentSpec::JsonFileSink    { input, .. } => Some(input.clone()),
                ComponentSpec::ParquetFileSink { input, .. } => Some(input.clone()),
                _ => None,
            };
            if let Some(input_id) = input_id_opt {
                self.specs.insert(id.clone(), ComponentSpec::Transform {
                    input:        input_id,
                    func:         Some(Arc::new(|batch| Ok(batch))),
                    func_name:    None,
                    label:        Some("dry_run_sink"),
                });
            }
        }
        self.run_inner(Some(batch_limit), true).await
    }

    async fn run_inner(mut self, batch_limit: Option<usize>, collect_schemas: bool)
        -> anyhow::Result<RunReport>
    {
        // ── Validate DAG before execution ────────────────────────────────────
        self.validate()?;

        // Capture the pipeline start time for run_ts() expressions.
        let run_start = chrono::Utc::now().timestamp_micros();
        self.run_start_us.store(
            run_start,
            std::sync::atomic::Ordering::Relaxed,
        );

        // Evaluate pipeline environment expressions once.
        if !self.environment.is_empty() {
            // Set run context so environment expressions like now() work.
            set_eval_context(EvalContext { run_start_us: run_start });
            // Create a 1-row dummy batch for evaluating scalar expressions.
            // We need exactly 1 row so functions like now() produce a 1-element
            // array that can be stored and later broadcast to match batch sizes.
            let dummy_field = arrow::datatypes::Field::new("__dummy", arrow::datatypes::DataType::Int32, true);
            let dummy_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![dummy_field]));
            let dummy_col: arrow::array::ArrayRef = std::sync::Arc::new(
                arrow::array::Int32Array::from(vec![None::<i32>])
            );
            let dummy_batch = RecordBatch::try_new(dummy_schema, vec![dummy_col])
                .expect("1-row dummy batch creation should never fail");
            let mut env_map = HashMap::new();
            for (name, expr_str) in &self.environment {
                let expr = parse(expr_str).map_err(|e| {
                    anyhow::anyhow!("environment variable '{name}': parse error in '{expr_str}': {e}")
                })?;
                let arr = eval(&expr, &dummy_batch).map_err(|e| {
                    anyhow::anyhow!("environment variable '{name}': eval error in '{expr_str}': {e}")
                })?;
                env_map.insert(name.clone(), arr);
            }
            *self.env_values.write().unwrap() = env_map;
        }

        // Initialise a fallback subscriber if none is already active.
        //
        // The base level comes from `RUST_LOG` (if set) or `config.log_level`.
        // On top of that we pin noisy third-party crates one level below `info`
        // so their per-column / per-row diagnostics don't flood the output at
        // the default log level.  Users can still see them via explicit
        // directives: `RUST_LOG=arrow_odbc=info,debug`.
        let base_level = self.config.log_level.as_filter_str();
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(format!(
                "{base_level},\
                 arrow_odbc=warn,\
                 hyper_util=warn,\
                 hyper_rustls=warn,\
                 rustls=warn,\
                 h2=warn,\
                 tower=warn"
            )));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init();

        // Note: validate() was already called at the top of run_inner().
        // No need to call it again — nothing changes between the two calls.
        let batch_size = self.config.batch_size;
        let channel_cap = self.config.channel_capacity.max(1); // at least 1
        let debug_step = self.config.log_level >= LogLevel::Debug;

        info!(
            batch_size,
            channel_capacity = channel_cap,
            "Pipeline config: batch_size={batch_size}, channel_capacity={channel_cap} \
             (producer may be at most {} batch(es) ahead of consumer)",
            channel_cap.saturating_sub(1),
        );

        // Schema collector — only allocated for dry-run.
        let schema_collector: Option<SchemaCollector> = if collect_schemas {
            Some(Arc::new(DashMap::new()))
        } else {
            None
        };

        // ── Dependency analysis ──────────────��────────────────────────────────
        let mut consumer_counts: HashMap<String, usize> = HashMap::new();
        let mut dependents:      HashMap<String, Vec<String>> = HashMap::new();

        for id in &self.order {
            for inp in self.specs[id].inputs() {
                *consumer_counts.entry(inp.to_string()).or_insert(0) += 1;
                dependents.entry(inp.to_string()).or_default().push(id.clone());
            }
        }
        let _ = consumer_counts; // used only for channel sizing above

        // ── Wire one channel per directed edge ────────────────────────────────
        //
        // Each component receives:
        //   primary_rx  — its single input (all non-source components)
        //   right_rx    — the build-side input (Join only)
        //   out_txs     — one Tx per downstream consumer (fan-out = N Txs)
        let mut primary_rx_map: HashMap<ComponentId, Rx> = HashMap::new();
        let mut right_rx_map:   HashMap<ComponentId, Rx> = HashMap::new();
        let mut output_txs_map: HashMap<ComponentId, Vec<Tx>> = HashMap::new();

        for id in &self.order {
            if let Some(consumers) = dependents.get(id.as_str()) {
                let mut txs: Vec<Tx> = Vec::with_capacity(consumers.len());
                for consumer_id in consumers {
                    let (tx, rx) = mpsc::channel::<anyhow::Result<RecordBatch>>(channel_cap);
                    txs.push(tx);
                    match &self.specs[consumer_id] {
                        ComponentSpec::Join { right, .. } if right.as_str() == id.as_str() => {
                            debug!("Channel: '{id}' → right of '{consumer_id}'");
                            right_rx_map.insert(consumer_id.clone(), rx);
                        }
                        _ => {
                            debug!("Channel: '{id}' → '{consumer_id}'");
                            primary_rx_map.insert(consumer_id.clone(), rx);
                        }
                    }
                }
                output_txs_map.insert(id.clone(), txs);
            }
        }

        let named_transforms: Arc<HashMap<String, TransformFn>> =
            Arc::new(std::mem::take(&mut self.named_transforms));

        // Stats: each task sends its row before exiting.
        let (stats_tx, mut stats_rx) =
            mpsc::channel::<(usize, ComponentId, ComponentStats, SpecKind)>(
                self.order.len().max(1)
            );

        // ── Pipeline timer ────────────────────────────────────────────────────
        let pipeline_started = Instant::now();

        // ── Spawn one task per component ──────────────────────────────────────
        let mut handles = Vec::with_capacity(self.order.len());

        for (order_idx, id) in self.order.iter().enumerate() {
            let spec     = self.specs.shift_remove(id).expect("validated");
            let out_txs  = output_txs_map.remove(id.as_str()).unwrap_or_default();
            let in_rx    = primary_rx_map.remove(id.as_str());
            let right_rx = right_rx_map.remove(id.as_str());

            handles.push(tokio::spawn(run_component(
                id.clone(), spec, in_rx, right_rx, out_txs,
                named_transforms.clone(), batch_size, batch_limit, debug_step,
                stats_tx.clone(), schema_collector.clone(), order_idx,
                self.env_values.clone(),  // pass environment variables
                pipeline_started,
            )));
        }

        // Drop our sender so the stats channel closes when all tasks finish.
        drop(stats_tx);

        // Await all tasks; propagate the root-cause error.
        //
        // When a sink fails (e.g. Oracle bind error), upstream tasks see
        // "downstream channel closed" — a symptom, not the cause.  We
        // collect all errors and report the first non-channel error (which
        // is typically the sink's original error).  If every error is a
        // channel error, we fall back to the first one.
        let results = futures::future::join_all(handles).await;
        let mut first_error: Option<anyhow::Error> = None;
        let mut channel_error: Option<anyhow::Error> = None;
        for r in results {
            match r {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    let msg = e.to_string();
                    if msg.contains("downstream channel closed") {
                        if channel_error.is_none() {
                            channel_error = Some(e);
                        }
                    } else if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(join_err) => {
                    if first_error.is_none() {
                        first_error = Some(anyhow::anyhow!("Task panicked: {join_err}"));
                    }
                }
            }
        }
        if let Some(e) = first_error {
            return Err(e);
        }
        if let Some(e) = channel_error {
            return Err(e);
        }

        // ── Collect stats ─────────────────────────────────────────────────────
        let mut report  = RunReport::default();
        let mut ordered: Vec<(usize, ComponentId, ComponentStats, SpecKind)> = Vec::new();
        while let Some(entry) = stats_rx.recv().await {
            ordered.push(entry);
        }
        ordered.sort_by_key(|(idx, ..)| *idx);

        for (_, id, stats, kind) in ordered {
            match kind {
                SpecKind::Source    => report.rows_read    += stats.rows_out,
                SpecKind::Sink      => report.rows_written += stats.rows_out,
                SpecKind::Transform => {}
            }
            report.iterations = report.iterations.max(stats.batches);
            report.per_component.insert(id.clone(), stats);
            report.component_order.push(id);
        }

        report.duration = pipeline_started.elapsed();

        // Startup time = earliest first_data_at across all components.
        // This is the overhead before any data row was produced (connection
        // setup, DDL checks, query planning, etc.).  The minimum across all
        // components captures the moment the first source emitted its first
        // batch — everything before that is startup overhead.
        let source_first_data: Option<Duration> = report.per_component.values()
            .filter_map(|s| s.first_data_at)
            .min();
        report.startup_time = source_first_data.unwrap_or(Duration::ZERO);
        report.active_duration = report.duration.saturating_sub(report.startup_time);

        // Throughput is computed over the active processing window, not the
        // total wall-clock time.  This gives users the true rows/s once
        // data is flowing, without startup overhead diluting the number.
        let ref_rows = if report.rows_read > 0 { report.rows_read }
                       else { report.rows_written };
        let active_secs = report.active_duration.as_secs_f64();
        report.rows_per_second = if active_secs > 0.0 { ref_rows as f64 / active_secs } else { 0.0 };

        // Drain schema collector (dry-run only).
        if let Some(collector) = schema_collector {
            // DashMap::into_read_only() converts to an immutable HashMap-like view,
            // but we need an IndexMap for the report.  Extract via iteration.
            if let Ok(dashmap) = Arc::try_unwrap(collector) {
                let map: IndexMap<ComponentId, arrow::datatypes::SchemaRef> = dashmap
                    .into_iter()
                    .collect();
                report.schemas = map;
            }
        }

        // Attach evaluated environment variables to the report for audit logging.
        if !self.environment.is_empty() {
            let ev = self.env_values.read().unwrap();
            for (name, _expr) in &self.environment {
                if let Some(arr) = ev.get(name) {
                    let display = array_first_to_string(arr);
                    report.environment.insert(name.clone(), display);
                }
            }
        }

        Ok(report)
    }
}

// ── spec_kind / spec_type helpers ─────────────────────────────────────────────

fn spec_kind(spec: &ComponentSpec) -> StepKind {
    match spec {
        ComponentSpec::Source { .. } | ComponentSpec::RestApiSource { .. }
        | ComponentSpec::JsonFileSource { .. }
        | ComponentSpec::CsvFileSource { .. }
        | ComponentSpec::ParquetFileSource { .. }                          => StepKind::Source,
        ComponentSpec::Sink { .. }
        | ComponentSpec::Scd2Sink { .. }
        | ComponentSpec::RestApiSink { .. }
        | ComponentSpec::CsvFileSink { .. }
        | ComponentSpec::JsonFileSink { .. }
        | ComponentSpec::ParquetFileSink { .. }                            => StepKind::Sink,
        _                                                                   => StepKind::Transform,
    }
}

fn spec_type(spec: &ComponentSpec) -> &'static str {
    match spec {
        ComponentSpec::Source { opts, .. } => {
            if opts.query.is_some() { "query_source" } else { "read_db" }
        }
        ComponentSpec::RestApiSource { .. }            => "rest_api_source",
        ComponentSpec::JsonFileSource { .. }           => "read_json",
        // Use the original YAML label when available, fall back to generic names.
        ComponentSpec::Transform { label: Some(lbl), .. }   => lbl,
        ComponentSpec::Transform { func_name: Some(_), .. } => "python_transform",
        ComponentSpec::Transform { .. }                     => "transform",
        ComponentSpec::Rename { .. }                   => "rename",
        ComponentSpec::Flatten { .. }                  => "flatten",
        ComponentSpec::Unnest  { .. }                  => "unnest",
        ComponentSpec::Join { .. }                     => "join",
        ComponentSpec::Aggregate { .. }                => "aggregate",
        ComponentSpec::Sink { .. }                     => "write_db",
        ComponentSpec::Scd2Sink { .. }                 => "scd2_sink",
        ComponentSpec::RestApiSink { .. }              => "rest_api_sink",
        ComponentSpec::CsvFileSource { .. }            => "read_csv",
        ComponentSpec::CsvFileSink { .. }              => "write_csv",
        ComponentSpec::JsonFileSink { .. }             => "write_json",
        ComponentSpec::ParquetFileSource { .. }        => "read_parquet",
        ComponentSpec::ParquetFileSink { .. }          => "write_parquet",
    }
}

/// Record the Arrow schema of `batch` into the collector the first time this
/// component ID is seen.  No-op when the collector is `None` (normal run).
fn capture_schema(
    collector: &Option<SchemaCollector>,
    id:        &str,
    batch:     &RecordBatch,
) {
    if let Some(c) = collector {
        // DashMap::entry() uses internal sharding for lock-free insertion.
        // Only the first batch from each component will trigger this insert;
        // subsequent batches are no-ops (entry already exists).
        c.entry(id.to_string()).or_insert_with(|| batch.schema());
    }
}

/// Extract the first element of a 1-element `ArrayRef` as a human-readable string.
///
/// Used to serialize evaluated environment variable values into the [`RunReport`].
fn array_first_to_string(arr: &arrow::array::ArrayRef) -> String {
    use arrow::array::Array;
    if arr.is_empty() { return String::new(); }
    if arr.is_null(0) { return "null".to_string(); }

    use arrow::datatypes::DataType;
    match arr.data_type() {
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            a.value(0).to_string()
        }
        DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
            a.value(0).to_string()
        }
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
            a.value(0).to_string()
        }
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
            a.value(0).to_string()
        }
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
            a.value(0).to_string()
        }
        DataType::Timestamp(unit, _) => {
            use arrow::datatypes::TimeUnit;
            // Convert to microseconds regardless of the stored unit, avoiding
            // a panic when the array is not TimestampMicrosecondArray.
            let us: Option<i64> = match unit {
                TimeUnit::Second => arr.as_any()
                    .downcast_ref::<arrow::array::TimestampSecondArray>()
                    .map(|a| a.value(0).checked_mul(1_000_000)).flatten(),
                TimeUnit::Millisecond => arr.as_any()
                    .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                    .map(|a| a.value(0).checked_mul(1_000)).flatten(),
                TimeUnit::Microsecond => arr.as_any()
                    .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                    .map(|a| a.value(0)),
                TimeUnit::Nanosecond => arr.as_any()
                    .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                    .map(|a| a.value(0) / 1_000),
            };
            match us {
                Some(micros) => chrono::DateTime::from_timestamp_micros(micros)
                    .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
                    .unwrap_or_else(|| micros.to_string()),
                None => format!("{:?}", arr),
            }
        }
        _ => format!("{:?}", arr),
    }
}

// ── Channel constants & helpers ───────────────────────────────────────────────

/// Fallback channel capacity when no config override is set.
///
/// In practice `run_inner` reads `ETLConfig::channel_capacity` and passes it
/// to channel creation.  This constant is only used as a compile-time default
/// for tests or places that cannot access the config.
///
/// Peak memory per edge = `channel_capacity × batch_size × avg_row_bytes`.
/// At the defaults (4 × 1 000 × ~200 B) ≈ **800 kB per edge**.
#[allow(dead_code)] // Kept as documented default; run_inner reads ETLConfig.
const DEFAULT_CHANNEL_CAP: usize = 4;

type Tx = mpsc::Sender<anyhow::Result<RecordBatch>>;
type Rx = mpsc::Receiver<anyhow::Result<RecordBatch>>;

/// Sends `batch` to every downstream channel (fan-out).
///
/// Clones for all but the last consumer — Arrow columns are `Arc`-backed so
/// a clone costs O(num_columns), not O(num_rows × row_size).
async fn send_to_all(txs: &[Tx], batch: RecordBatch) -> anyhow::Result<()> {
    match txs.len() {
        0 => {}
        1 => txs[0].send(Ok(batch)).await
                .map_err(|_| anyhow::anyhow!("downstream channel closed unexpectedly"))?,
        n => {
            for tx in &txs[..n - 1] {
                tx.send(Ok(batch.clone())).await
                    .map_err(|_| anyhow::anyhow!("downstream channel closed unexpectedly"))?;
            }
            txs[n - 1].send(Ok(batch)).await
                .map_err(|_| anyhow::anyhow!("downstream channel closed unexpectedly"))?;
        }
    }
    Ok(())
}

// ── Helper: ensure environment variables are set ──────────────────────────────

/// Set thread-local environment variables from the shared pipeline environment map.
///
/// This helper centralizes the pattern used in filter/map/sink components to make
/// environment variables available to expression evaluation and column mapping.
///
/// Uses a thread-local pointer cache to avoid re-cloning the HashMap on every
/// batch.  Environment variables are immutable after pipeline start, so a single
/// clone per `Arc` identity per thread is sufficient.  If a new pipeline runs
/// with different env vars (different `Arc`), the cache is invalidated.
#[inline]
fn ensure_env_vars_set(env_values: &Arc<std::sync::RwLock<HashMap<String, ArrayRef>>>) {
    std::thread_local! {
        /// Stores the `Arc` data pointer of the last `env_values` that was
        /// cloned into thread-local storage.  When the pointer matches, we
        /// skip the clone.
        static LAST_PTR: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }
    let ptr = Arc::as_ptr(env_values) as usize;
    LAST_PTR.with(|cached| {
        if cached.get() != ptr {
            let ev = env_values.read().unwrap().clone();
            if !ev.is_empty() {
                set_env_vars(ev);
            }
            cached.set(ptr);
        }
    });
}

// ── Shared sub-chunk write helpers ────────────────────────────────────────────

/// Sub-chunk a `RecordBatch` into slices of at most `chunk_size` rows and
/// write each slice via `WriteDB::write`.  When `write_chunk` is `None` the
/// entire batch is written in a single call.
///
/// This centralises the chunked-write + error-reporting logic that is shared
/// between the `Sink` and any future DB-writing component.
async fn write_batch_chunked(
    id:               &str,
    sink:             &mut crate::db::WriteDB,
    batch:            RecordBatch,
    write_chunk:      Option<usize>,
    stats:            &mut ComponentStats,
    pipeline_started: Instant,
) -> anyhow::Result<()> {
    if let Some(chunk_size) = write_chunk {
        let total  = batch.num_rows();
        let mut offset = 0usize;
        while offset < total {
            let len = chunk_size.min(total - offset);
            let sub = batch.slice(offset, len);
            // No clone needed: RecordBatch::slice() returns a cheap zero-copy view.
            // The debug log only fires on single-row error, so we re-slice there.
            let written = match sink.write(sub).await {
                Ok(n) => n,
                Err(e) => {
                    if len == 1 {
                        let err_sub = batch.slice(offset, 1);
                        debug!(
                            "[{id}] write error on single row:\n{}",
                            format_head(std::slice::from_ref(&err_sub))
                        );
                    }
                    return Err(e);
                }
            };
            stats.rows_out += written;
            stats.record_batch(pipeline_started);
            offset += len;
        }
    } else {
        let written = match sink.write(batch).await {
            Ok(n) => n,
            Err(e) => {
                // batch was moved into write(); for single-row debug logging
                // the error message itself is sufficient.
                return Err(e);
            }
        };
        stats.rows_out += written;
        stats.record_batch(pipeline_started);
    }
    Ok(())
}

/// Sub-chunk a `RecordBatch` into slices of at most `chunk_size` rows and
/// write each slice via `Scd2Sink::write`.  When `write_chunk` is `None` the
/// entire batch is written in a single call.
///
/// Same pattern as [`write_batch_chunked`] but for the SCD2 writer, which
/// returns [`Scd2Stats`] instead of a plain row count.
async fn write_scd2_batch_chunked(
    id:               &str,
    scd2:             &mut crate::db::Scd2Sink,
    batch:            RecordBatch,
    write_chunk:      Option<usize>,
    stats:            &mut ComponentStats,
    pipeline_started: Instant,
) -> anyhow::Result<()> {
    if let Some(chunk_size) = write_chunk {
        let total  = batch.num_rows();
        let mut offset = 0usize;
        while offset < total {
            let len = chunk_size.min(total - offset);
            let sub = batch.slice(offset, len);
            let s = match scd2.write(sub).await {
                Ok(s) => s,
                Err(e) => {
                    if len == 1 {
                        let err_sub = batch.slice(offset, 1);
                        debug!(
                            "[{id}] scd2 write error on single row:\n{}",
                            format_head(std::slice::from_ref(&err_sub))
                        );
                    }
                    return Err(e);
                }
            };
            stats.rows_out += s.new_rows + s.updated_rows;
            stats.record_batch(pipeline_started);
            offset += len;
        }
    } else {
        let s = match scd2.write(batch).await {
            Ok(s) => s,
            Err(e) => {
                return Err(e);
            }
        };
        stats.rows_out += s.new_rows + s.updated_rows;
        stats.record_batch(pipeline_started);
    }
    Ok(())
}

// ── Shared source-side schema application ─────────────────────────────────────

/// Stateful helper for applying source-side schema settings to each batch.
///
/// Handles `exclude`, `arrow_overrides`, value injections (from
/// `schema.arrow.columns` entries with `value:`), `database.columns` metadata
/// stamps (primary_key, db_type, etc.), and `normalize_columns`
/// from [`SourceSchemaConfig`] with a compile-once / apply-many pattern.
struct SourceSchemaApplicator {
    source_schema: SourceSchemaConfig,
    arrow_overrides_cache: HashMap<String, String>,
    /// Cached value injection columns (extracted once from `schema.arrow`).
    value_injections_cache: HashMap<String, crate::config::ArrowColumnDef>,
    /// Cached database column options (extracted once from `schema.database`).
    /// Stamped as `etl.*` Arrow field metadata so downstream sinks can use
    /// them for DDL generation (e.g. primary keys on Databricks sources).
    column_options_cache: ColumnOptionsMap,
    normalize: bool,
    override_plan: Option<ArrowOverridesPlan>,
    plan_compiled: bool,
    stamp_plan: Option<MetadataStampPlan>,
    stamp_compiled: bool,
}

impl SourceSchemaApplicator {
    fn new(source_schema: SourceSchemaConfig, normalize: bool) -> Self {
        let arrow_overrides_cache = source_schema.arrow_overrides();
        let value_injections_cache: HashMap<String, crate::config::ArrowColumnDef> =
            source_schema.schema.value_injection_columns()
                .into_iter()
                .map(|(k, v)| (k, v.clone()))
                .collect();
        let column_options_cache = source_schema.schema.column_options_map();
        Self { source_schema, arrow_overrides_cache, value_injections_cache,
               column_options_cache, normalize, override_plan: None, plan_compiled: false,
               stamp_plan: None, stamp_compiled: false }
    }

    /// Apply exclude → arrow_overrides → value_injections → metadata_stamps → normalize_columns to a batch.
    fn apply(&mut self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        // 1. Exclude columns
        let batch = if !self.source_schema.exclude.is_empty() {
            apply_exclude_columns(batch, &self.source_schema.exclude)?
        } else {
            batch
        };

        // 2. Arrow overrides (compile-once)
        let batch = if !self.plan_compiled {
            self.plan_compiled = true;
            self.override_plan = compile_arrow_overrides(
                &batch.schema(), &self.arrow_overrides_cache,
            )?;
            match &self.override_plan {
                Some(plan) => apply_arrow_overrides_plan(batch, plan)?,
                None       => batch,
            }
        } else {
            match &self.override_plan {
                Some(plan) => apply_arrow_overrides_plan(batch, plan)?,
                None       => batch,
            }
        };

        // 3. Value injections (from schema.arrow.columns with value:)
        let batch = if !self.value_injections_cache.is_empty() {
            apply_value_injections(batch, &self.value_injections_cache)?
        } else {
            batch
        };

        // 4. Metadata stamps from schema.database.columns (primary_key, db_type, etc.)
        //    These propagate downstream via Arrow field metadata so sinks can use
        //    them for DDL generation — essential for sources like Databricks that
        //    don't expose PK metadata through their catalog.
        let batch = if !self.stamp_compiled {
            self.stamp_compiled = true;
            self.stamp_plan = compile_metadata_stamps(
                &batch.schema(),
                &self.column_options_cache,
            )?;
            match &self.stamp_plan {
                Some(plan) => apply_metadata_stamp_plan(batch, plan)?,
                None       => batch,
            }
        } else {
            match &self.stamp_plan {
                Some(plan) => apply_metadata_stamp_plan(batch, plan)?,
                None       => batch,
            }
        };

        // 5. Normalize column names
        let batch = if self.normalize { apply_normalize_columns(batch)? } else { batch };
        Ok(batch)
    }
}

// ── Shared sink-side schema application ───────────────────────────────────────

/// Stateful helper for applying sink-side schema settings to each batch.
///
/// Handles `arrow_overrides` (Arrow casts), value injections (from
/// `schema.arrow.columns` entries with `value:`), and `column_options`
/// (DDL hints incl. `db_type`) from [`SinkSchemaConfig`]
/// with a compile-once / apply-many pattern.
struct SinkSchemaApplicator {
    sink_schema: SinkSchemaConfig,
    /// Cached arrow overrides map (extracted once from `schema.arrow`).
    arrow_overrides_cache: HashMap<String, String>,
    /// Cached value injection columns (extracted once from `schema.arrow`).
    value_injections_cache: HashMap<String, crate::config::ArrowColumnDef>,
    /// Cached column options map (extracted once from `schema.database`).
    column_options_cache: HashMap<String, crate::schema::field::ColumnOption>,
    /// Column names in YAML-defined order (from `schema.database.columns`
    /// IndexMap).  Used by `compute_ddl_schema` to preserve user-specified
    /// column order when appending DDL-only columns.
    ordered_column_names: Vec<String>,
    stamp_plan: Option<MetadataStampPlan>,
    stamp_compiled: bool,
    override_plan: Option<ArrowOverridesPlan>,
    override_compiled: bool,
}

impl SinkSchemaApplicator {
    fn new(sink_schema: SinkSchemaConfig) -> Self {
        let arrow_overrides_cache = sink_schema.arrow_overrides();
        let value_injections_cache: HashMap<String, crate::config::ArrowColumnDef> =
            sink_schema.schema.value_injection_columns()
                .into_iter()
                .map(|(k, v)| (k, v.clone()))
                .collect();
        let column_options_cache = sink_schema.column_options();
        let ordered_column_names: Vec<String> = sink_schema.schema.database
            .as_ref()
            .map(|db| db.columns.keys().cloned().collect())
            .unwrap_or_default();
        Self { sink_schema, arrow_overrides_cache, value_injections_cache,
               column_options_cache, ordered_column_names,
               stamp_plan: None, stamp_compiled: false,
               override_plan: None, override_compiled: false }
    }

    /// Apply arrow_overrides → value_injections → column_options to a batch.
    fn apply(&mut self, batch: RecordBatch) -> anyhow::Result<RecordBatch> {
        // 1. Arrow overrides (Arrow type casts) — compile once
        let batch = if !self.override_compiled {
            self.override_compiled = true;
            self.override_plan = compile_arrow_overrides(
                &batch.schema(),
                &self.arrow_overrides_cache,
            )?;
            match &self.override_plan {
                Some(plan) => apply_arrow_overrides_plan(batch, plan)?,
                None       => batch,
            }
        } else {
            match &self.override_plan {
                Some(plan) => apply_arrow_overrides_plan(batch, plan)?,
                None       => batch,
            }
        };

        // 2. Value injections (from schema.arrow.columns with value:)
        let batch = if !self.value_injections_cache.is_empty() {
            apply_value_injections(batch, &self.value_injections_cache)?
        } else {
            batch
        };

        // 3. Metadata stamps (column_options incl. db_type) — compile once
        let batch = if !self.stamp_compiled {
            self.stamp_compiled = true;
            self.stamp_plan = compile_metadata_stamps(
                &batch.schema(),
                &self.column_options_cache,
            )?;
            match &self.stamp_plan {
                Some(plan) => apply_metadata_stamp_plan(batch, plan)?,
                None       => batch,
            }
        } else {
            match &self.stamp_plan {
                Some(plan) => apply_metadata_stamp_plan(batch, plan)?,
                None       => batch,
            }
        };

        Ok(batch)
    }

    /// Compute a DDL schema that includes DDL-only columns.
    ///
    /// DDL-only columns are entries in `column_options` whose name does **not**
    /// appear in the batch schema.  They are added to the DDL schema with
    /// appropriate `etl.*` metadata so that `generate_ddl()` /
    /// `mssql_create_table_sql()` / etc. emit them in the `CREATE TABLE`
    /// statement, while the data path never touches them.
    ///
    /// Returns `None` when there are no DDL-only columns (i.e. every
    /// `column_options` key already exists in the batch).
    fn compute_ddl_schema(
        &self,
        batch_schema: &arrow::datatypes::SchemaRef,
    ) -> Option<arrow::datatypes::SchemaRef> {
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use crate::schema::constants::*;

        let batch_names: std::collections::HashSet<String> = batch_schema
            .fields()
            .iter()
            .map(|f| f.name().to_lowercase())
            .collect();

        // Collect DDL-only column names from column_options that are NOT in the batch.
        // Iterate `ordered_column_names` (YAML insertion order) so the DDL
        // appends extra columns in the same order the user defined them.
        let mut ddl_only_names: Vec<String> = Vec::new();
        for key in &self.ordered_column_names {
            if !batch_names.contains(&key.to_lowercase()) {
                ddl_only_names.push(key.clone());
            }
        }

        if ddl_only_names.is_empty() {
            return None;
        }

        // Start with the batch schema fields (already stamped by apply()).
        let mut fields: Vec<Field> = batch_schema
            .fields()
            .iter()
            .map(|f| (**f).clone())
            .collect();

        // Append DDL-only fields.
        for col_name in &ddl_only_names {
            let mut meta = std::collections::HashMap::new();

            // Stamp column_options (incl. db_type).
            let mut nullable = true;
            if let Some(co) = self.column_options_cache
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(col_name))
                .map(|(_, v)| v)
            {
                if let Some(db_type) = &co.db_type  { meta.insert(META_DB_TYPE.into(), db_type.clone()); }
                if co.primary_key { meta.insert(META_PRIMARY_KEY.into(), "true".into()); }
                if co.unique      { meta.insert(META_UNIQUE.into(),      "true".into()); }
                if co.index       { meta.insert(META_INDEX.into(),       "true".into()); }
                if let Some(n)       = co.nullable      { meta.insert(META_NULLABLE.into(), n.to_string()); nullable = n; }
                if let Some(check)   = &co.check_expr   { meta.insert(META_CHECK_EXPR.into(),  check.clone()); }
                if let Some(default) = &co.default_expr  { meta.insert(META_DEFAULT_EXPR.into(), default.clone()); }
                if let Some(on_upd)  = &co.on_update_expr{ meta.insert(META_ON_UPDATE_EXPR.into(), on_upd.clone()); }
                if let Some(fk) = &co.foreign_key {
                    if let Ok(fk_json) = serde_json::to_string(fk) {
                        meta.insert(META_FOREIGN_KEY.into(), fk_json);
                    }
                }
                if let Some(desc) = &co.description { meta.insert(META_DESCRIPTION.into(), desc.clone()); }
            }

            // Infer a sensible Arrow placeholder type for the DDL column.
            // When default_expr or on_update_expr contains a now()-like
            // expression and no explicit db_type is set, use Timestamp
            // so the DDL generates TIMESTAMPTZ instead of TEXT.
            let inferred_type = if meta.contains_key(META_DB_TYPE) {
                // User specified an explicit db_type — use Utf8 placeholder;
                // the DDL resolver will pick up the db_type metadata.
                DataType::Utf8
            } else {
                let has_ts_expr = [META_DEFAULT_EXPR, META_ON_UPDATE_EXPR]
                    .iter()
                    .any(|key| {
                        meta.get(*key)
                            .map(|v| {
                                let lc = v.trim().to_ascii_lowercase();
                                lc.contains("now()")
                                    || lc.contains("current_timestamp")
                                    || lc.contains("getdate()")
                                    || lc.contains("sysdate")
                                    || lc.contains("sysdatetime()")
                            })
                            .unwrap_or(false)
                    });
                if has_ts_expr {
                    DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Microsecond,
                        Some("UTC".into()),
                    )
                } else {
                    DataType::Utf8
                }
            };
            let field = Field::new(col_name, inferred_type, nullable)
                .with_metadata(meta);
            fields.push(field);

            tracing::debug!(
                column = %col_name,
                "SinkSchemaApplicator: added DDL-only column"
            );
        }

        Some(std::sync::Arc::new(ArrowSchema::new(fields)))
    }
}

// ── run_component ───────────────────────────────────────────────────────────

async fn run_component(
    id:               ComponentId,
    spec:             ComponentSpec,
    in_rx:            Option<Rx>,
    right_rx:         Option<Rx>,
    out_txs:          Vec<Tx>,
    named_transforms: Arc<HashMap<String, TransformFn>>,
    batch_size:       usize,
    batch_limit:      Option<usize>,
    debug_step:       bool,
    stats_tx:         mpsc::Sender<(usize, ComponentId, ComponentStats, SpecKind)>,
    schema_collector: Option<SchemaCollector>,
    order_idx:        usize,
    env_values:       Arc<std::sync::RwLock<HashMap<String, ArrayRef>>>,
    pipeline_started: Instant,
) -> anyhow::Result<()> {
    let comp_started = Instant::now();
    let mut stats    = ComponentStats::default();

    let kind: anyhow::Result<SpecKind> = run_component_inner(
        &id, spec, in_rx, right_rx, &out_txs,
        &named_transforms, batch_size, batch_limit, debug_step,
        &schema_collector, &mut stats, &env_values, pipeline_started,
    ).await;

    // ── Forward errors to downstream channels ────────────────────────────
    //
    // When a component (especially a source) fails, its `out_txs` senders
    // are about to be dropped.  Downstream consumers (sinks) interpret a
    // closed channel as "upstream is done" and call `flush()` — committing
    // partial data.  To prevent this, we send the error through the channel
    // so the downstream task sees `Err(...)` and aborts instead of
    // committing.
    if let Err(ref err) = kind {
        let err_msg = format!("{err:#}");
        for tx in &out_txs {
            // Best-effort: the receiver may already be gone.
            let _ = tx.send(Err(anyhow::anyhow!(
                "upstream component '{id}' failed: {err_msg}"
            ))).await;
        }
    }

    stats.duration_ms = comp_started.elapsed().as_millis() as u64;
    let reported_kind = match &kind { Ok(k) => *k, Err(_) => SpecKind::Transform };
    // Always send stats; ignore if the receiver was already dropped.
    let _ = stats_tx.send((order_idx, id.clone(), stats, reported_kind)).await;
    kind.map(|_| ()).map_err(|e| anyhow::anyhow!("[{id}] {e:#}"))
}

async fn run_component_inner(
    id:               &str,
    spec:             ComponentSpec,
    in_rx:            Option<Rx>,
    right_rx:         Option<Rx>,
    out_txs:          &[Tx],
    named_transforms: &Arc<HashMap<String, TransformFn>>,
    batch_size:       usize,
    batch_limit:      Option<usize>,
    debug_step:       bool,
    schema_collector: &Option<SchemaCollector>,
    stats:            &mut ComponentStats,
    env_values:       &Arc<std::sync::RwLock<HashMap<String, ArrayRef>>>,
    pipeline_started: Instant,
) -> anyhow::Result<SpecKind> {
    match spec {

        // ── DB source ─────────────────────────────────────────────────────────
        ComponentSpec::Source { conn_str, opts } => {
            let normalize = should_normalize(&conn_str, opts.source_schema.normalize_columns);
            let mut source = ReadDB::new(&conn_str)?;
            if let Some(t) = &opts.table  { source = source.table(t); }
            if let Some(s) = &opts.db_schema { source = source.schema(s); }
            if let Some(q) = &opts.query  { source = source.query(q); }
            if let Some(c) = &opts.cursor { source = source.cursor(c); }
            // Per-component batch_size overrides the global pipeline batch_size.
            let effective_batch = opts.batch_size.unwrap_or(batch_size);
            source = source.batch_size(effective_batch);
            // Apply per-step driver options (e.g. Oracle prefetch_rows).
            // No-op when the `options:` block is absent or empty.
            if !opts.options.is_empty() {
                source = source.with_driver_options(&opts.options);
            }

            let target = opts.query.as_deref()
                .map(|_| "<custom query>")
                .or_else(|| opts.table.as_deref())
                .unwrap_or("?");
            if opts.batch_size.is_some() {
                info!("[{id}] ▸ source  `{target}` (batch_size={effective_batch} [component override])");
            } else {
                info!("[{id}] ▸ source  `{target}` (batch_size={effective_batch})");
            }

            let mut stream          = source.exec();
            let mut first_seen      = false;
            let mut schema_captured = false;
            let mut applicator      = SourceSchemaApplicator::new(opts.source_schema, normalize);
            while let Some(result) = stream.next().await {
                // Env vars must be set before applicator.apply() so that
                // value injections referencing $env_var can resolve them.
                ensure_env_vars_set(env_values);
                let batch = applicator.apply(result?)?;
                // Capture schema from first batch only — independent of debug flag.
                if !schema_captured {
                    capture_schema(schema_collector, id, &batch);
                    schema_captured = true;
                }
                // Schema + head(10) on first batch only.
                if debug_step && !first_seen {
                    debug_output(id, std::slice::from_ref(&batch));
                    first_seen = true;
                }
                stats.rows_out += batch.num_rows();
                stats.record_batch(pipeline_started);
                // Honour dry-run batch limit.
                if let Some(limit) = batch_limit {
                    if stats.batches >= limit {
                        send_to_all(out_txs, batch).await?;
                        break;
                    }
                }
                // Progress every 500K rows so large tables stay visible.
                const PROGRESS_INTERVAL: usize = 500_000;
                if stats.rows_out / PROGRESS_INTERVAL > stats.last_progress_rows / PROGRESS_INTERVAL {
                    info!("[{id}]   … {} rows read", fmt_num(stats.rows_out));
                    stats.last_progress_rows = stats.rows_out;
                }
                send_to_all(out_txs, batch).await?;
            }
            info!("[{id}] ✓ source  {} rows, {} batches",
                fmt_num(stats.rows_out), fmt_num(stats.batches));
            Ok(SpecKind::Source)
        }

        // ── REST API source ───────────────────────────────────────────────────
        ComponentSpec::RestApiSource { url, opts, source_schema } => {
            info!("[{id}] ▸ REST API source  {url}");
            // REST API sources don't have a conn_str, so normalize_columns
            // defaults to false unless explicitly set.
            let normalize = source_schema.normalize_columns.unwrap_or(false);
            let raw_batches      = fetch_rest_api(&url, &opts, batch_size).await?;
            let mut first_seen   = false;
            let mut schema_captured = false;
            let mut applicator   = SourceSchemaApplicator::new(source_schema, normalize);
            for batch in raw_batches {
                ensure_env_vars_set(env_values);
                let batch = applicator.apply(batch)?;
                if !schema_captured {
                    capture_schema(schema_collector, id, &batch);
                    schema_captured = true;
                }
                if debug_step && !first_seen {
                    debug_output(id, std::slice::from_ref(&batch));
                    first_seen = true;
                }
                stats.rows_out += batch.num_rows();
                stats.record_batch(pipeline_started);
                if let Some(limit) = batch_limit {
                    if stats.batches >= limit { break; }
                }
                send_to_all(out_txs, batch).await?;
            }
            info!("[{id}] ✓ REST API source  {} rows", fmt_num(stats.rows_out));
            Ok(SpecKind::Source)
        }

        // ── JSON file source ──────────────────────────────────────────────────
        ComponentSpec::JsonFileSource { path, data_path, source_schema, batch_size: step_bs, conn: file_conn, sort_glob } => {
            let transport = potato_etl_common::file_transport::create_transport(&file_conn)?;
            let resolved_paths = potato_etl_common::file_transport::resolve_glob(transport.as_ref(), &path, sort_glob).await?;
            info!("[{id}] ▸ read_json  {path}  ({}, {} file(s))", transport.describe(), resolved_paths.len());
            let effective_bs = step_bs.unwrap_or(batch_size);
            let normalize = source_schema.normalize_columns.unwrap_or(false);
            let mut first_seen   = false;
            let mut schema_captured = false;
            let mut applicator   = SourceSchemaApplicator::new(source_schema, normalize);
            'json_outer: for file_path in &resolved_paths {
                let bytes = transport.read_bytes(file_path).await?;
                let raw_batches = potato_etl_common::json_file::read_json_bytes(
                    &bytes, data_path.as_deref(), effective_bs,
                )?;
                for batch in raw_batches {
                    ensure_env_vars_set(env_values);
                    let batch = applicator.apply(batch)?;
                    if !schema_captured {
                        capture_schema(schema_collector, id, &batch);
                        schema_captured = true;
                    }
                    if debug_step && !first_seen {
                        debug_output(id, std::slice::from_ref(&batch));
                        first_seen = true;
                    }
                    stats.rows_out += batch.num_rows();
                    stats.record_batch(pipeline_started);
                    if let Some(limit) = batch_limit {
                        if stats.batches >= limit { break 'json_outer; }
                    }
                    send_to_all(out_txs, batch).await?;
                }
            }
            info!("[{id}] ✓ read_json  {} rows from {path}", fmt_num(stats.rows_out));
            Ok(SpecKind::Source)
        }

        // ── CSV file source ───────────────────────────────────────────────────
        ComponentSpec::CsvFileSource { path, delimiter, has_header, source_schema, batch_size: step_bs, conn: file_conn, sort_glob } => {
            let transport = potato_etl_common::file_transport::create_transport(&file_conn)?;
            let resolved_paths = potato_etl_common::file_transport::resolve_glob(transport.as_ref(), &path, sort_glob).await?;
            info!("[{id}] ▸ read_csv  {path}  ({}, {} file(s))", transport.describe(), resolved_paths.len());
            let effective_bs = step_bs.unwrap_or(batch_size);
            let normalize = source_schema.normalize_columns.unwrap_or(false);
            let mut first_seen   = false;
            let mut schema_captured = false;
            let mut applicator   = SourceSchemaApplicator::new(source_schema, normalize);
            'csv_outer: for file_path in &resolved_paths {
                let bytes = transport.read_bytes(file_path).await?;
                let raw_batches = potato_etl_common::csv_file::read_csv_bytes(
                    &bytes, delimiter, has_header, effective_bs,
                )?;
                for batch in raw_batches {
                    ensure_env_vars_set(env_values);
                    let batch = applicator.apply(batch)?;
                    if !schema_captured {
                        capture_schema(schema_collector, id, &batch);
                        schema_captured = true;
                    }
                    if debug_step && !first_seen {
                        debug_output(id, std::slice::from_ref(&batch));
                        first_seen = true;
                    }
                    stats.rows_out += batch.num_rows();
                    stats.record_batch(pipeline_started);
                    if let Some(limit) = batch_limit {
                        if stats.batches >= limit { break 'csv_outer; }
                    }
                    send_to_all(out_txs, batch).await?;
                }
            }
            info!("[{id}] ✓ read_csv  {} rows from {path}", fmt_num(stats.rows_out));
            Ok(SpecKind::Source)
        }

        // ── Transform ─────────────────────────────────────────────────────────
        ComponentSpec::Transform { input: _, func, func_name, label: _ } => {
            let func: TransformFn = match (func, func_name) {
                (Some(f), _)       => f,
                (None, Some(name)) => named_transforms.get(&name)
                    .ok_or_else(|| anyhow::anyhow!(
                        "Transform '{name}' is not registered. \
                         Call `pipeline.register_transform(\"{name}\", ...)` before `pipeline.run()`."
                    ))?.clone(),
                (None, None) => anyhow::bail!(
                    "Transform '{id}' has no function and no name — this is a bug"
                ),
            };
            info!("[{id}] ▸ transform  starting");
            let mut rx         = in_rx.ok_or_else(|| anyhow::anyhow!(
                "Transform '{id}' has no input channel — wiring bug"
            ))?;
            let mut first_seen = false;
            while let Some(result) = rx.recv().await {
                let batch = result?;
                stats.rows_in += batch.num_rows();
                if debug_step && !first_seen {
                    debug_input(id, "input", std::slice::from_ref(&batch));
                }
                let f      = func.clone();
                let result = tokio::task::spawn_blocking(move || f(batch)).await??;
                if result.num_rows() > 0 {
                    // Schema capture is independent of debug logging.
                    if !first_seen {
                        capture_schema(schema_collector, id, &result);
                    }
                    if debug_step && !first_seen {
                        debug_output(id, std::slice::from_ref(&result));
                    }
                    if !first_seen { first_seen = true; }
                    stats.rows_out += result.num_rows();
                    stats.record_batch(pipeline_started);
                    send_to_all(out_txs, result).await?;
                }
            }
            info!("[{id}] ✓ transform  {} → {} rows",
                fmt_num(stats.rows_in), fmt_num(stats.rows_out));
            Ok(SpecKind::Transform)
        }

        // ── Rename ────────────────────────────────────────────────────────────
        ComponentSpec::Rename { input: _, columns } => {
            info!("[{id}] ▸ rename  {} column mappings", columns.len());
            let mut rx         = in_rx.ok_or_else(|| anyhow::anyhow!(
                "Rename '{id}' has no input channel — wiring bug"
            ))?;
            let columns = Arc::new(columns);
            let mut first_seen = false;
            while let Some(result) = rx.recv().await {
                let batch  = result?;
                stats.rows_in += batch.num_rows();
                let cols_c = Arc::clone(&columns);
                let result = tokio::task::spawn_blocking(move || {
                    apply_rename(batch, &cols_c)
                }).await??;
                if !first_seen {
                    capture_schema(schema_collector, id, &result);
                    if debug_step {
                        debug_output(id, std::slice::from_ref(&result));
                    }
                    first_seen = true;
                }
                stats.rows_out += result.num_rows();
                stats.record_batch(pipeline_started);
                send_to_all(out_txs, result).await?;
            }
            info!("[{id}] ✓ rename  {} rows", fmt_num(stats.rows_out));
            Ok(SpecKind::Transform)
        }

        // ── Flatten ───────────────────────────────────────────────────────────
        ComponentSpec::Flatten { input: _, select } => {
            info!("[{id}] ▸ flatten  starting");
            let mut rx         = in_rx.ok_or_else(|| anyhow::anyhow!(
                "Flatten '{id}' has no input channel — wiring bug"
            ))?;
            let mut first_seen = false;
            while let Some(result) = rx.recv().await {
                let batch  = result?;
                stats.rows_in += batch.num_rows();
                let result = apply_flatten(batch, &select)?;
                if !first_seen {
                    capture_schema(schema_collector, id, &result);
                    if debug_step {
                        debug_output(id, std::slice::from_ref(&result));
                    }
                    first_seen = true;
                }
                stats.rows_out += result.num_rows();
                stats.record_batch(pipeline_started);
                send_to_all(out_txs, result).await?;
            }
            info!("[{id}] ✓ flatten  {} rows", fmt_num(stats.rows_out));
            Ok(SpecKind::Transform)
        }

        // ── Unnest ───────────────────────────────────────────────────────────
        ComponentSpec::Unnest { input: _, config } => {
            info!("[{id}] ▸ unnest  column={} starting", config.column);
            let mut rx         = in_rx.ok_or_else(|| anyhow::anyhow!(
                "Unnest '{id}' has no input channel — wiring bug"
            ))?;
            let mut first_seen = false;
            while let Some(result) = rx.recv().await {
                let batch  = result?;
                stats.rows_in += batch.num_rows();
                let result = apply_unnest(batch, &config)?;
                if result.num_rows() == 0 { continue; }
                if !first_seen {
                    capture_schema(schema_collector, id, &result);
                    if debug_step {
                        debug_output(id, std::slice::from_ref(&result));
                    }
                    first_seen = true;
                }
                stats.rows_out += result.num_rows();
                stats.record_batch(pipeline_started);
                send_to_all(out_txs, result).await?;
            }
            info!("[{id}] ✓ unnest  {} rows in → {} rows out", fmt_num(stats.rows_in), fmt_num(stats.rows_out));
            Ok(SpecKind::Transform)
        }

        // ── Join ──────────────────────────────────────────────────────────────
        //
        // Memory: right (build) side is fully materialised; left is streamed.
        // Put the SMALLER table on the right.
        ComponentSpec::Join { left: _, right: _, on, how } => {
            let mut left_rx = in_rx.ok_or_else(|| anyhow::anyhow!(
                "Join '{id}' has no left-side channel — wiring bug"
            ))?;
            let mut right_rx = right_rx.ok_or_else(|| anyhow::anyhow!(
                "Join '{id}' has no right-side channel — wiring bug"
            ))?;

            info!("[{id}] ▸ join  materialising right (build) side on key `{on}`");

            // Phase 1: materialise the right (build) side.
            let mut right_batches: Vec<RecordBatch> = Vec::new();
            while let Some(result) = right_rx.recv().await {
                right_batches.push(result?);
            }
            let right_rows: usize = right_batches.iter().map(|b| b.num_rows()).sum();
            info!("[{id}]   right side ready: {} rows", fmt_num(right_rows));

            if right_batches.is_empty() && how == JoinHow::Inner {
                while left_rx.recv().await.is_some() {}
                info!("[{id}] ✓ join  right side empty — inner join produced 0 rows");
            } else {
                // Phase 2: build hash map.
                let on_clone  = on.clone();
                let how_clone = how.clone();
                let right_map = tokio::task::spawn_blocking(move || {
                    crate::transform::join::build_right_hash_map(&right_batches, &on_clone, &how_clone)
                }).await??;
                let right_map  = Arc::new(right_map);
                let mut first_seen = false;
                // Track the output schema for FULL join unmatched-right emission.
                let mut output_schema: Option<arrow::datatypes::SchemaRef> = None;

                // Phase 3: stream left side, probe per batch.
                while let Some(result) = left_rx.recv().await {
                    let batch = result?;
                    stats.rows_in += batch.num_rows();
                    if debug_step && !first_seen {
                        debug_input(id, "left", std::slice::from_ref(&batch));
                    }
                    let rm    = Arc::clone(&right_map);
                    let how_c = how.clone();
                    let out   = tokio::task::spawn_blocking(move || {
                        crate::transform::join::probe_left_batch(batch, &rm, &how_c)
                    }).await??;
                    if let Some(out_batch) = out {
                        if !first_seen {
                            capture_schema(schema_collector, id, &out_batch);
                            output_schema = Some(out_batch.schema());
                            if debug_step {
                                debug_output(id, std::slice::from_ref(&out_batch));
                            }
                            first_seen = true;
                        }
                        stats.rows_out += out_batch.num_rows();
                        stats.record_batch(pipeline_started);
                        send_to_all(out_txs, out_batch).await?;
                    }
                }
                // Phase 4 (FULL join only): emit unmatched right-side rows.
                if how == JoinHow::Full {
                    if let Some(out_schema) = output_schema {
                        let rm = Arc::clone(&right_map);
                        let on_c = on.clone();
                        let unmatched = tokio::task::spawn_blocking(move || {
                            crate::transform::join::emit_unmatched_right(&rm, &out_schema, &on_c)
                        }).await??;
                        if let Some(unmatched_batch) = unmatched {
                            stats.rows_out += unmatched_batch.num_rows();
                            stats.record_batch(pipeline_started);
                            send_to_all(out_txs, unmatched_batch).await?;
                        }
                    }
                }

                info!("[{id}] ✓ join  {} left + {} right → {} output rows",
                    fmt_num(stats.rows_in), fmt_num(right_rows), fmt_num(stats.rows_out));
            }
            Ok(SpecKind::Transform)
        }

        // ── DB sink ───────────────────────────────────────────────────────────
        ComponentSpec::Sink { input: _, conn_str, opts } => {
            let mut rx = in_rx.ok_or_else(|| anyhow::anyhow!(
                "Sink '{id}' has no input channel — wiring bug"
            ))?;
            let mut sink = WriteDB::new(&conn_str)?;
            if !opts.table.is_empty() { sink = sink.table(&opts.table); }
            if let Some(s) = &opts.db_schema { sink = sink.schema(s); }
            sink = match &opts.mode {
                SinkMode::Append        => sink.insert(),
                SinkMode::InsertIgnore  => sink.insert_ignore(),
                SinkMode::Upsert        => sink.upsert(),
                SinkMode::MergeDelete   => sink.merge_delete(),
                SinkMode::Truncate      => sink.clear_and_insert(),
            };
            if opts.create_table != CreateTableMode::Never {
                sink = sink.create_mode(opts.create_table);
            }
            // Apply per-step driver options (e.g. MSSQL mode, batch_size).
            // No-op when the `options:` block is absent or empty.
            if !opts.options.is_empty() {
                sink = sink.with_driver_options(&opts.options);
            }

            // Per-component write batch size: if set, incoming RecordBatches
            // are sub-chunked before each write() call so the sink never
            // receives more than `write_chunk` rows at once.
            //
            // Example: source batch_size=50 000, sink batch_size=20 000.
            // A 50 000-row batch becomes three write() calls: 20 000 + 20 000 + 10 000.
            // The final remainder (10 000) is handled by the last sub-chunk.
            // flush() is called once after all sub-batches are written.
            let write_chunk = opts.batch_size;
            if let Some(n) = write_chunk {
                info!("[{id}] ▸ sink  `{}` mode={:?} (write_batch_size={n} [component override])",
                    opts.table, opts.mode);
            } else {
                info!("[{id}] ▸ sink  `{}` mode={:?}", opts.table, opts.mode);
            }

            let mut first_seen = false;
            let mut ddl_schema_set = false;
            let mut applicator = SinkSchemaApplicator::new(opts.sink_schema);
            while let Some(result) = rx.recv().await {
                let batch = result?;
                stats.rows_in += batch.num_rows();
                if debug_step && !first_seen {
                    debug_input(id, "input", std::slice::from_ref(&batch));
                    first_seen = true;
                }
                ensure_env_vars_set(env_values);
                let batch = applicator.apply(batch)?;

                // On the first batch, compute DDL schema (batch fields +
                // DDL-only columns from column_options) and push it to
                // the sink so CREATE TABLE includes them.
                if !ddl_schema_set {
                    ddl_schema_set = true;
                    if let Some(ddl_schema) = applicator.compute_ddl_schema(&batch.schema()) {
                        sink.set_ddl_schema(ddl_schema);
                    }
                    // Pass named indexes and constraints from schema.database
                    // to the sink for post-create DDL generation.
                    if let Some(db_config) = applicator.sink_schema.database_schema_config() {
                        sink.set_database_schema_config(db_config.clone());
                    }
                }

                write_batch_chunked(
                    id, &mut sink, batch, write_chunk, stats, pipeline_started,
                ).await?;
            }
            let flush_start = Instant::now();
            sink.flush().await?;
            let flush_ms = flush_start.elapsed().as_millis();
            info!("[{id}] ✓ sink  {} rows written  (flush: {flush_ms}ms)",
                fmt_num(stats.rows_out));
            Ok(SpecKind::Sink)
        }

        // ── SCD2 sink ─────────────────────────────────────────────────────────
        ComponentSpec::Scd2Sink { input: _, conn_str, table, db_schema, key_col, tracked,
                                   col_names, create_table, sink_schema,
                                   batch_size: sink_batch_size, options, close_missing } => {
            let mut rx = in_rx.ok_or_else(|| anyhow::anyhow!(
                "Scd2Sink '{id}' has no input channel — wiring bug"
            ))?;
            let mut scd2 = crate::db::Scd2Sink::new(&conn_str)?;
            scd2 = scd2.table(&table).key(&key_col).col_names(col_names);
            if let Some(s) = &db_schema { scd2 = scd2.schema(s); }
            if !tracked.is_empty()   { scd2 = scd2.track(tracked); }
            scd2 = scd2.create_table(create_table);
            scd2 = scd2.close_missing(close_missing);
            // Set chunk_size from the step-level batch_size (or global).
            let effective_chunk = sink_batch_size.unwrap_or(batch_size);
            scd2 = scd2.chunk_size(effective_chunk);
            // Apply per-step driver options (no-op when the block is absent or empty).
            if !options.is_empty() {
                scd2 = scd2.with_driver_options(&options);
            }
            if close_missing {
                info!("[{id}] ▸ scd2  `{table}` key=`{key_col}` close_missing=true");
            } else if let Some(n) = sink_batch_size {
                info!("[{id}] ▸ scd2  `{table}` key=`{key_col}` (write_batch_size={n} [component override])");
            } else {
                info!("[{id}] ▸ scd2  `{table}` key=`{key_col}`");
            }
            let mut first_seen = false;
            let mut ddl_schema_set = false;
            let mut applicator = SinkSchemaApplicator::new(sink_schema);
            while let Some(result) = rx.recv().await {
                let batch = result?;
                stats.rows_in += batch.num_rows();
                if debug_step && !first_seen {
                    debug_input(id, "input", std::slice::from_ref(&batch));
                    first_seen = true;
                }
                ensure_env_vars_set(env_values);
                let batch = applicator.apply(batch)?;

                // On the first batch, compute DDL schema and push to scd2 sink.
                if !ddl_schema_set {
                    ddl_schema_set = true;
                    if let Some(ddl_schema) = applicator.compute_ddl_schema(&batch.schema()) {
                        scd2.set_ddl_schema(ddl_schema);
                    }
                    // Pass named indexes and constraints from schema.database
                    // to the scd2 sink for post-create DDL generation.
                    if let Some(db_config) = applicator.sink_schema.database_schema_config() {
                        scd2.set_database_schema_config(db_config.clone());
                    }
                }

                write_scd2_batch_chunked(
                    id, &mut scd2, batch, sink_batch_size, stats, pipeline_started,
                ).await?;
            }
            scd2.flush().await?;
            info!("[{id}] ✓ scd2  {} rows new/updated", fmt_num(stats.rows_out));
            Ok(SpecKind::Sink)
        }

        // ── Aggregate ─────────────────────────────────────────────────────────
        //
        // Materialises ALL incoming batches — unavoidable for group-by.
        ComponentSpec::Aggregate { input: _, group_by, metrics } => {
            let mut rx = in_rx.ok_or_else(|| anyhow::anyhow!(
                "Aggregate '{id}' has no input channel — wiring bug"
            ))?;
            info!("[{id}] ▸ aggregate  group_by=[{}]  metrics=[{}]",
                group_by.join(", "),
                metrics.keys().cloned().collect::<Vec<_>>().join(", "));

            let mut batches = Vec::new();
            while let Some(result) = rx.recv().await {
                let batch = result?;
                stats.rows_in += batch.num_rows();
                batches.push(batch);
            }

            if batches.is_empty() {
                info!("[{id}] ✓ aggregate  0 rows in — skipping");
                return Ok(SpecKind::Transform);
            }

            // CPU-bound — run off the async executor
            let gb = group_by.clone();
            let m  = metrics.clone();
            let result = tokio::task::spawn_blocking(move || {
                apply_aggregate(batches, &gb, &m)
            }).await??;

            capture_schema(schema_collector, id, &result);
            if debug_step {
                debug_output(id, std::slice::from_ref(&result));
            }

            stats.rows_out += result.num_rows();
            stats.record_batch(pipeline_started);

            info!("[{id}] ✓ aggregate  {} groups from {} rows",
                fmt_num(stats.rows_out), fmt_num(stats.rows_in));

            send_to_all(out_txs, result).await?;
            Ok(SpecKind::Transform)
        }

        // ── REST API sink ─────────────────────────────────────────────────────
        ComponentSpec::RestApiSink { input: _, url, opts } => {
            info!("[{id}] ▸ REST API sink  {url}");
            let mut rx         = in_rx.ok_or_else(|| anyhow::anyhow!(
                "RestApiSink '{id}' has no input channel — wiring bug"
            ))?;
            let mut first_seen = false;
            while let Some(result) = rx.recv().await {
                let batch = result?;
                stats.rows_in += batch.num_rows();
                if debug_step && !first_seen {
                    debug_input(id, "input", std::slice::from_ref(&batch));
                    first_seen = true;
                }
                let sent = send_to_rest_api(std::slice::from_ref(&batch), &url, &opts).await?;
                stats.rows_out += sent;
                stats.record_batch(pipeline_started);
            }
            info!("[{id}] ✓ REST API sink  {} rows sent", fmt_num(stats.rows_out));
            Ok(SpecKind::Sink)
        }

        // ── CSV file sink ──────────────────────────────────────────────────────
        ComponentSpec::CsvFileSink { input: _, path, delimiter, has_header, conn: file_conn } => {
            let transport = potato_etl_common::file_transport::create_transport(&file_conn)?;
            info!("[{id}] ▸ write_csv  {path}  ({})", transport.describe());
            let mut rx         = in_rx.ok_or_else(|| anyhow::anyhow!(
                "CsvFileSink '{id}' has no input channel — wiring bug"
            ))?;
            let mut all_batches: Vec<RecordBatch> = Vec::new();
            let mut first_seen = false;
            while let Some(result) = rx.recv().await {
                let batch = result?;
                stats.rows_in += batch.num_rows();
                if debug_step && !first_seen {
                    debug_input(id, "input", std::slice::from_ref(&batch));
                    first_seen = true;
                }
                all_batches.push(batch);
            }
            let bytes = potato_etl_common::csv_file::write_csv_to_bytes(
                &all_batches, delimiter, has_header,
            )?;
            let written = all_batches.iter().map(|b| b.num_rows()).sum::<usize>();
            transport.write_bytes(&path, &bytes).await?;
            stats.rows_out += written;
            stats.record_batch(pipeline_started);
            info!("[{id}] ✓ write_csv  {} rows to {path}", fmt_num(stats.rows_out));
            Ok(SpecKind::Sink)
        }

        // ── JSON file sink ─────────────────────────────────────────────────────
        ComponentSpec::JsonFileSink { input: _, path, pretty, wrap_key, conn: file_conn } => {
            let transport = potato_etl_common::file_transport::create_transport(&file_conn)?;
            info!("[{id}] ▸ write_json  {path}  ({})", transport.describe());
            let mut rx         = in_rx.ok_or_else(|| anyhow::anyhow!(
                "JsonFileSink '{id}' has no input channel — wiring bug"
            ))?;
            let mut all_batches: Vec<RecordBatch> = Vec::new();
            let mut first_seen = false;
            while let Some(result) = rx.recv().await {
                let batch = result?;
                stats.rows_in += batch.num_rows();
                if debug_step && !first_seen {
                    debug_input(id, "input", std::slice::from_ref(&batch));
                    first_seen = true;
                }
                all_batches.push(batch);
            }
            let bytes = potato_etl_common::json_file::write_json_to_bytes(
                &all_batches, pretty, wrap_key.as_deref(),
            )?;
            let written = all_batches.iter().map(|b| b.num_rows()).sum::<usize>();
            transport.write_bytes(&path, &bytes).await?;
            stats.rows_out += written;
            stats.record_batch(pipeline_started);
            info!("[{id}] ✓ write_json  {} rows to {path}", fmt_num(stats.rows_out));
            Ok(SpecKind::Sink)
        }

        // ── Parquet file source ──────────────────────────────────────────────
        ComponentSpec::ParquetFileSource { path, columns, source_schema, batch_size: step_bs, conn: file_conn, sort_glob } => {
            let transport = potato_etl_common::file_transport::create_transport(&file_conn)?;
            let resolved_paths = potato_etl_common::file_transport::resolve_glob(transport.as_ref(), &path, sort_glob).await?;
            info!("[{id}] ▸ read_parquet  {path}  ({}, {} file(s))", transport.describe(), resolved_paths.len());
            let effective_bs = step_bs.unwrap_or(batch_size);
            let normalize = source_schema.normalize_columns.unwrap_or(false);
            let mut first_seen      = false;
            let mut schema_captured = false;
            let mut applicator      = SourceSchemaApplicator::new(source_schema, normalize);
            'parquet_outer: for file_path in &resolved_paths {
                let bytes = transport.read_bytes(file_path).await?;
                let raw_batches = potato_etl_common::parquet_file::read_parquet_bytes(
                    &bytes, effective_bs, columns.as_deref(),
                )?;
                for batch in raw_batches {
                    ensure_env_vars_set(env_values);
                    let batch = applicator.apply(batch)?;
                    if !schema_captured {
                        capture_schema(schema_collector, id, &batch);
                        schema_captured = true;
                    }
                    if debug_step && !first_seen {
                        debug_output(id, std::slice::from_ref(&batch));
                        first_seen = true;
                    }
                    stats.rows_out += batch.num_rows();
                    stats.record_batch(pipeline_started);
                    if let Some(limit) = batch_limit {
                        if stats.batches >= limit { break 'parquet_outer; }
                    }
                    send_to_all(out_txs, batch).await?;
                }
            }
            info!("[{id}] ✓ read_parquet  {} rows from {path}", fmt_num(stats.rows_out));
            Ok(SpecKind::Source)
        }

        // ── Parquet file sink ────────────────────────────────────────────────
        ComponentSpec::ParquetFileSink { input: _, path, compression, conn: file_conn } => {
            let transport = potato_etl_common::file_transport::create_transport(&file_conn)?;
            info!("[{id}] ▸ write_parquet  {path}  ({})", transport.describe());
            let mut rx         = in_rx.ok_or_else(|| anyhow::anyhow!(
                "ParquetFileSink '{id}' has no input channel -- wiring bug"
            ))?;
            let mut all_batches: Vec<RecordBatch> = Vec::new();
            let mut first_seen = false;
            while let Some(result) = rx.recv().await {
                let batch = result?;
                stats.rows_in += batch.num_rows();
                if debug_step && !first_seen {
                    debug_input(id, "input", std::slice::from_ref(&batch));
                    first_seen = true;
                }
                all_batches.push(batch);
            }
            let codec = potato_etl_common::parquet_file::ParquetCompression::from_str_loose(&compression)?;
            let bytes = potato_etl_common::parquet_file::write_parquet_to_bytes(
                &all_batches, codec,
            )?;
            let written = all_batches.iter().map(|b| b.num_rows()).sum::<usize>();
            transport.write_bytes(&path, &bytes).await?;
            stats.rows_out += written;
            stats.record_batch(pipeline_started);
            info!("[{id}] ✓ write_parquet  {} rows ({} bytes) to {path}",
                fmt_num(stats.rows_out), fmt_num(bytes.len()));
            Ok(SpecKind::Sink)
        }
    }
}