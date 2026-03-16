//! `potato_etl` - command-line interface for the PotatoFlow ETL library.
//!
//! ## Usage
//!
//! ```text
//! potato_etl <COMMAND> --config <FILE> [OPTIONS]
//!
//! Commands:
//!   run          Run the full pipeline (reads sources, applies transforms, writes sinks)
//!   dry-run      Execute 1 batch per source without writing anything - safe inspection mode
//!   schema       Show the Arrow schema of a step's output (requires a live connection)
//!   validate     Validate config YAML/JSON structure without connecting to any database
//!   list-steps   List every step ID in the pipeline in execution order
//!   step-info    Show detailed information for a single step
//!   explain      Describe what the pipeline would do (static analysis, no connection)
//!   to-json      Convert a YAML pipeline config to JSON
//! ```

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};

use potato_etl_runtime::{Dag, StepKind};

// -- CLI arguments -------------------------------------------------------------

#[derive(Parser)]
#[command(
    name    = "potato_etl",
    version,
    about   = "PotatoFlow ETL - run, inspect, and validate data pipelines",
    long_about = None,
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Clone, ValueEnum, PartialEq, Eq)]
enum OutputFormat {
    /// Human-readable aligned text (default).
    Pretty,
    /// Machine-readable JSON.
    Json,
    /// Compact ASCII table.
    Table,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the full pipeline.
    ///
    /// Reads every source, applies all transforms, and commits every sink.
    /// Prints a run-report summary on completion.
    Run {
        /// Path to the pipeline config file (.yaml or .json).
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,

        /// Override the global batch_size from the config.
        #[arg(long, value_name = "N")]
        batch_size: Option<usize>,

        /// Enable verbose (debug) logging.
        #[arg(short, long)]
        verbose: bool,

        /// Enable trace logging (very noisy).
        #[arg(long)]
        trace: bool,
    },

    /// Execute 1 batch per source, NO writes committed anywhere.
    ///
    /// All write_db, scd2_sink, and rest_api_sink steps are replaced with
    /// no-op drains before execution.  This is a hard guarantee - the actual
    /// write code paths are never entered regardless of the pipeline config.
    ///
    /// Useful for testing transformations, validating schemas, and previewing
    /// what the pipeline would produce on real data.
    #[command(name = "dry-run")]
    DryRun {
        /// Path to the pipeline config file (.yaml or .json).
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,

        /// Number of batches to read from each source (default: 1).
        #[arg(long, default_value = "1", value_name = "N")]
        batches: usize,

        /// Override the global batch_size from the config.
        #[arg(long, value_name = "N")]
        batch_size: Option<usize>,

        /// Output format.
        #[arg(long, default_value = "pretty", value_name = "FORMAT")]
        format: OutputFormat,

        /// Enable verbose (debug) logging.
        #[arg(short, long)]
        verbose: bool,
    },

    /// Show the Arrow schema of a step's output.
    ///
    /// Executes a 1-batch dry-run (no writes) and prints the inferred Arrow
    /// schema for the requested step.  Requires a live database connection
    /// to any source that feeds into the target step.
    Schema {
        /// Path to the pipeline config file (.yaml or .json).
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,

        /// The step ID whose output schema should be displayed.
        #[arg(long, value_name = "STEP_ID")]
        step: String,

        /// Output format.
        #[arg(long, default_value = "table", value_name = "FORMAT")]
        format: OutputFormat,
    },

    /// Validate the config file structure without connecting to any database.
    ///
    /// Checks YAML/JSON syntax, required fields, and DAG wiring (no dangling
    /// input references, topological ordering).  Exits 0 on success, 1 on failure.
    Validate {
        /// Path to the pipeline config file (.yaml or .json).
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
    },

    /// List all step IDs in the pipeline in execution order.
    #[command(name = "list-steps")]
    ListSteps {
        /// Path to the pipeline config file (.yaml or .json).
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,

        /// Output format.
        #[arg(long, default_value = "table", value_name = "FORMAT")]
        format: OutputFormat,
    },

    /// Show detailed information for a single step.
    #[command(name = "step-info")]
    StepInfo {
        /// Path to the pipeline config file (.yaml or .json).
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,

        /// The step ID to inspect.
        #[arg(long, value_name = "STEP_ID")]
        step: String,

        /// Output format.
        #[arg(long, default_value = "pretty", value_name = "FORMAT")]
        format: OutputFormat,
    },

    /// Describe what the pipeline would do (static analysis, no connection).
    ///
    /// Prints each step in order with its type, kind, and inputs.  No database
    /// connection is made.
    Explain {
        /// Path to the pipeline config file (.yaml or .json).
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,

        /// Output format.
        #[arg(long, default_value = "pretty", value_name = "FORMAT")]
        format: OutputFormat,
    },

    /// Convert a YAML pipeline config to JSON.
    ///
    /// Reads a YAML (.yaml / .yml) pipeline file, validates the structure,
    /// and prints the equivalent JSON to stdout.  Useful for normalizing
    /// configs before storing them in a database or sending over the wire.
    ///
    /// The output is pretty-printed by default.  Use `--compact` for a
    /// single-line JSON string suitable for embedding.
    #[command(name = "to-json")]
    ToJson {
        /// Path to the YAML pipeline config file.
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,

        /// Emit compact single-line JSON instead of pretty-printed.
        #[arg(long)]
        compact: bool,
    },
}

// -- Entry point ---------------------------------------------------------------

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("\n{} {e:#}\n", red(&"X  Error:"));
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize transport factories for all enabled feature flags.
    potato_etl_runtime::init();

    match cli.command {
        Commands::Run { config, batch_size, verbose, trace } => {
            cmd_run(config, batch_size, verbose, trace).await
        }
        Commands::DryRun { config, batches, batch_size, format, verbose } => {
            cmd_dry_run(config, batches, batch_size, format, verbose).await
        }
        Commands::Schema { config, step, format } => {
            cmd_schema(config, step, format).await
        }
        Commands::Validate { config } => {
            cmd_validate(config).await
        }
        Commands::ListSteps { config, format } => {
            cmd_list_steps(config, format).await
        }
        Commands::StepInfo { config, step, format } => {
            cmd_step_info(config, step, format).await
        }
        Commands::Explain { config, format } => {
            cmd_explain(config, format).await
        }
        Commands::ToJson { config, compact } => {
            cmd_to_json(config, compact)
        }
    }
}

// -- Config loading ------------------------------------------------------------

/// Load a `Dag` from a YAML or JSON config file.
///
/// Detection order:
/// 1. `.json` extension -> parsed as JSON
/// 2. Everything else    -> parsed as YAML (YAML is a superset of JSON so a
///    `.yaml` or `.yml` file works, as does a bare string path without
///    extension)
async fn load_dag(path: &PathBuf, batch_size_override: Option<usize>) -> anyhow::Result<Dag> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("cannot read config file: {}", path.display()))?;

    let is_json = path.extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    let has_secrets = raw.contains("secret::");

    let mut dag = if is_json {
        if has_secrets {
            Dag::from_json_async(&raw).await
        } else {
            Dag::from_json(&raw)
        }.with_context(|| format!("failed to parse JSON config: {}", path.display()))?
    } else {
        if has_secrets {
            Dag::from_yaml_async(&raw).await
        } else {
            Dag::from_yaml(&raw)
        }.with_context(|| format!("failed to parse YAML config: {}", path.display()))?
    };

    if let Some(bs) = batch_size_override {
        dag.set_batch_size(bs);
    }

    Ok(dag)
}

// -- Commands ------------------------------------------------------------------

async fn cmd_run(
    config:     PathBuf,
    batch_size: Option<usize>,
    verbose:    bool,
    trace:      bool,
) -> anyhow::Result<()> {
    use potato_etl_runtime::LogLevel;

    let mut dag = load_dag(&config, batch_size).await?;
    if trace        { dag.set_log_level(LogLevel::Trace); }
    else if verbose { dag.set_log_level(LogLevel::Debug); }

    println!("{} Running pipeline: {}", bold(&">"), config.display());
    println!();

    let report = dag.run().await
        .context("pipeline execution failed")?;

    println!();
    println!("{report}");
    Ok(())
}

async fn cmd_dry_run(
    config:     PathBuf,
    batches:    usize,
    batch_size: Option<usize>,
    format:     OutputFormat,
    verbose:    bool,
) -> anyhow::Result<()> {
    use potato_etl_runtime::LogLevel;

    let mut dag = load_dag(&config, batch_size).await?;
    if verbose { dag.set_log_level(LogLevel::Debug); }

    let step_summaries = dag.steps();

    // Banner - make the dry-run nature unmistakable.
    eprintln!();
    eprintln!("  {} DRY RUN - reading {} batch(es) per source",
        yellow(&"!"),
        batches);
    eprintln!("  {} NO DATA WILL BE WRITTEN to any database or API",
        yellow(&"!"));
    eprintln!();

    let report = dag.run_dry(batches).await
        .context("dry-run execution failed")?;

    match format {
        OutputFormat::Json => {
            // Build a JSON object with stats + schemas.
            let mut obj = serde_json::Map::new();
            obj.insert("dry_run".into(), serde_json::Value::Bool(true));
            obj.insert("batches_per_source".into(), serde_json::json!(batches));
            obj.insert("rows_read".into(),     serde_json::json!(report.rows_read));
            obj.insert("rows_written".into(),  serde_json::json!(0));  // always 0
            obj.insert("duration_ms".into(),   serde_json::json!(report.duration.as_millis()));

            let schemas_json: serde_json::Map<String, serde_json::Value> = report.schemas
                .iter()
                .map(|(id, schema)| {
                    let fields: Vec<_> = schema.fields().iter().map(|f| {
                        serde_json::json!({
                            "name":     f.name(),
                            "type":     format!("{:?}", f.data_type()),
                            "nullable": f.is_nullable(),
                            "metadata": f.metadata(),
                        })
                    }).collect();
                    (id.clone(), serde_json::Value::Array(fields))
                })
                .collect();
            obj.insert("schemas".into(), serde_json::Value::Object(schemas_json));
            println!("{}", serde_json::to_string_pretty(&obj)?);
        }

        OutputFormat::Pretty | OutputFormat::Table => {
            println!("{}", dim(&"=".repeat(63)));
            println!("  {:<18} {}",
                bold(&"DRY RUN"),
                dim(&"(no writes committed)"));
            println!("  {:<18} {}",  bold(&"Config"),    config.display());
            println!("  {:<18} {} batch(es) per source", bold(&"Sample"),  batches);
            println!("  {:<18} {}", bold(&"Duration"),
                fmt_duration(report.duration));
            println!("  {:<18} {}", bold(&"Rows read"),  fmt_num(report.rows_read));
            println!("  {:<18} {} {}",  bold(&"Rows written"),
                fmt_num(report.rows_written),
                dim(&"<- always 0 in dry-run"));
            println!("{}", dim(&"=".repeat(63)));

            // Per-component breakdown.
            println!();
            println!("  {:<24}  {:>9}  {:>9}  {:<11}",
                bold(&"Step"), bold(&"rows in"), bold(&"rows out"), bold(&"type"));
            println!("  {}", "-".repeat(58));
            for s in &step_summaries {
                let stats = report.per_component.get(&s.id);
                let ri = stats.map(|st| fmt_num(st.rows_in)).unwrap_or_else(|| "-".into());
                let ro = stats.map(|st| fmt_num(st.rows_out)).unwrap_or_else(|| "-".into());
                let sink_note = if s.kind == StepKind::Sink {
                    dim(&" [suppressed]")
                } else {
                    String::new()
                };
                println!("  {:<24}  {:>9}  {:>9}  {}{}",
                    s.id, ri, ro, s.step_type, sink_note);
            }

            // Schemas sampled.
            if !report.schemas.is_empty() {
                println!();
                println!("{}", dim(&"=".repeat(63)));
                println!("  {}", bold(&"Sampled schemas"));
                println!("{}", dim(&"=".repeat(63)));
                for id in &report.component_order {
                    if let Some(schema) = report.schemas.get(id) {
                        println!();
                        println!("  {} {}", bold(id), dim(&format!("({} columns)", schema.fields().len())));
                        print_schema_table(schema, 4);
                    }
                }
            }
        }
    }

    Ok(())
}

async fn cmd_schema(
    config: PathBuf,
    step:   String,
    format: OutputFormat,
) -> anyhow::Result<()> {
    let dag = load_dag(&config, None).await?;

    // Verify the step exists before running.
    let summary = dag.step_info(&step)
        .ok_or_else(|| anyhow::anyhow!("step '{}' not found in pipeline", step))?;

    // Run a 1-batch dry-run to capture live schemas.
    eprintln!();
    eprintln!("  {} Connecting to sources for 1-batch dry-run...", dim(&"i"));
    eprintln!("  {} NO DATA WILL BE WRITTEN", yellow(&"!"));
    eprintln!();

    let report = dag.run_dry(1).await
        .context("dry-run for schema discovery failed")?;

    let schema = report.schemas.get(&step)
        .ok_or_else(|| anyhow::anyhow!(
            "no schema captured for step '{}' - the step may produce 0 rows \
             or the source returned no data", step
        ))?;

    match format {
        OutputFormat::Json => {
            let fields: Vec<_> = schema.fields().iter().map(|f| {
                serde_json::json!({
                    "name":     f.name(),
                    "type":     format!("{:?}", f.data_type()),
                    "nullable": f.is_nullable(),
                    "metadata": f.metadata(),
                })
            }).collect();
            let out = serde_json::json!({
                "step":      &step,
                "step_type": summary.step_type,
                "kind":      summary.kind.to_string(),
                "fields":    fields,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }

        OutputFormat::Pretty | OutputFormat::Table => {
            println!();
            println!("  {} {}", bold(&"Schema for step:"), green(&step));
            println!("  {} {}  |  {} {}",
                dim(&"type:"), summary.step_type,
                dim(&"kind:"), summary.kind);
            println!();
            print_schema_table(schema, 2);
        }
    }

    Ok(())
}

async fn cmd_validate(config: PathBuf) -> anyhow::Result<()> {
    let dag = load_dag(&config, None).await
        .with_context(|| "config parse failed")?;

    dag.validate()
        .with_context(|| "DAG wiring validation failed")?;

    let steps = dag.steps();
    let sources = steps.iter().filter(|s| s.kind == StepKind::Source).count();
    let sinks   = steps.iter().filter(|s| s.kind == StepKind::Sink).count();

    println!();
    println!("  {} Config is valid", green(&"OK"));
    println!();
    println!("  {:<12} {}", bold(&"File"),     config.display());
    println!("  {:<12} {}", bold(&"Steps"),    steps.len());
    println!("  {:<12} {}", bold(&"Sources"),  sources);
    println!("  {:<12} {}", bold(&"Sinks"),    sinks);
    println!("  {:<12} {}", bold(&"Transforms"), steps.len() - sources - sinks);
    println!();

    Ok(())
}

async fn cmd_list_steps(config: PathBuf, format: OutputFormat) -> anyhow::Result<()> {
    let dag   = load_dag(&config, None).await?;
    let steps = dag.steps();

    match format {
        OutputFormat::Json => {
            let arr: Vec<_> = steps.iter().map(|s| serde_json::json!({
                "id":        &s.id,
                "kind":      s.kind.to_string(),
                "step_type": s.step_type,
                "inputs":    &s.inputs,
            })).collect();
            println!("{}", serde_json::to_string_pretty(&arr)?);
        }

        OutputFormat::Pretty | OutputFormat::Table => {
            println!();
            println!("  {:<24}  {:<16}  {:<12}  {}",
                bold(&"ID"), bold(&"TYPE"), bold(&"KIND"), bold(&"INPUTS"));
            println!("  {}", "-".repeat(72));
            for (i, s) in steps.iter().enumerate() {
                let inputs = if s.inputs.is_empty() {
                    dim(&"-")
                } else {
                    s.inputs.join(", ")
                };
                let kind_col = match s.kind {
                    StepKind::Source    => green(&"source"),
                    StepKind::Sink      => yellow(&"sink"),
                    StepKind::Transform => blue(&"transform"),
                };
                println!("  {:<3} {:<24}  {:<16}  {:<12}  {}",
                    dim(&format!("[{}]", i + 1)),
                    s.id, s.step_type, kind_col, inputs);
            }
            println!();
        }
    }

    Ok(())
}

async fn cmd_step_info(config: PathBuf, step: String, format: OutputFormat) -> anyhow::Result<()> {
    let dag = load_dag(&config, None).await?;

    let summary = dag.step_info(&step)
        .ok_or_else(|| anyhow::anyhow!("step '{}' not found in pipeline", step))?;

    match format {
        OutputFormat::Json => {
            let out = serde_json::json!({
                "id":        &summary.id,
                "kind":      summary.kind.to_string(),
                "step_type": summary.step_type,
                "inputs":    &summary.inputs,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }

        OutputFormat::Pretty | OutputFormat::Table => {
            println!();
            println!("  {}", bold(&format!("Step: {}", summary.id)));
            println!();
            println!("  {:<12} {}", bold(&"Type"),   summary.step_type);
            println!("  {:<12} {}", bold(&"Kind"),   summary.kind);
            if summary.inputs.is_empty() {
                println!("  {:<12} {}", bold(&"Inputs"),  dim(&"(source - no upstream inputs)"));
            } else {
                println!("  {:<12} {}", bold(&"Inputs"),  summary.inputs.join(", "));
            }
            println!();
            println!("  {} Use `schema --step {}` to see live Arrow schema (requires a connection).",
                dim(&"i"), summary.id);
            println!();
        }
    }

    Ok(())
}

async fn cmd_explain(config: PathBuf, format: OutputFormat) -> anyhow::Result<()> {
    let dag   = load_dag(&config, None).await?;
    let steps = dag.steps();

    match format {
        OutputFormat::Json => {
            let arr: Vec<_> = steps.iter().enumerate().map(|(i, s)| serde_json::json!({
                "index":     i + 1,
                "id":        &s.id,
                "kind":      s.kind.to_string(),
                "step_type": s.step_type,
                "inputs":    &s.inputs,
            })).collect();
            let out = serde_json::json!({
                "config": config.display().to_string(),
                "steps":  arr,
            });
            println!("{}", serde_json::to_string_pretty(&out)?);
        }

        OutputFormat::Pretty | OutputFormat::Table => {
            println!();
            println!("  {} Pipeline: {}",
                bold(&">"), config.display());
            println!("  {} {} steps total", dim(&"i"), steps.len());
            println!();
            println!("  {}", "-".repeat(72));

            for (i, s) in steps.iter().enumerate() {
                let idx    = dim(&format!("[{:>2}]", i + 1));
                let kind_badge = match s.kind {
                    StepKind::Source    => format!("{}  ", green(&"SOURCE   ")),
                    StepKind::Sink      => format!("{}",   yellow(&"SINK     ")),
                    StepKind::Transform => format!("{}",   blue(&"TRANSFORM")),
                };
                let arrow_in = if s.inputs.is_empty() {
                    dim(&"         <-  (no input - reads from DB/API)")
                } else {
                    format!("         <-  {}", s.inputs.join(", "))
                };
                println!("  {}  {:<24}  {}  {}",
                    idx, bold(&s.id), kind_badge, dim(&format!("({})", s.step_type)));
                println!("  {}", arrow_in);
            }

            println!("  {}", "-".repeat(72));
            println!();
            println!("  {} No database connection needed for 'explain'.", dim(&"i"));
            println!("  {} Use 'dry-run' to execute with real data (no writes).", dim(&"i"));
            println!("  {} Use 'run' to execute the full pipeline.", dim(&"i"));
            println!();
        }
    }

    Ok(())
}

fn cmd_to_json(config: PathBuf, compact: bool) -> anyhow::Result<()> {
    let raw = std::fs::read_to_string(&config)
        .with_context(|| format!("cannot read config file: {}", config.display()))?;

    // Detect format: if the file is already JSON, inform the user.
    let is_json = config.extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("json"))
        .unwrap_or(false);

    if is_json {
        anyhow::bail!(
            "input file '{}' is already JSON. \
             This command converts YAML to JSON.",
            config.display()
        );
    }

    let json = if compact {
        Dag::yaml_to_json_compact(&raw)
            .with_context(|| format!("failed to convert YAML to JSON: {}", config.display()))?
    } else {
        Dag::yaml_to_json(&raw)
            .with_context(|| format!("failed to convert YAML to JSON: {}", config.display()))?
    };

    println!("{json}");
    Ok(())
}

// -- Schema table printer ------------------------------------------------------

fn print_schema_table(schema: &arrow::datatypes::Schema, indent: usize) {
    use potato_etl_runtime::{
        META_DB_TYPE, META_PRIMARY_KEY, META_DESCRIPTION,
    };

    let pad = " ".repeat(indent);
    let w_name  = schema.fields().iter().map(|f| f.name().len()).max().unwrap_or(8).max(8);
    let w_arrow = schema.fields().iter()
        .map(|f| format!("{:?}", f.data_type()).len())
        .max().unwrap_or(10).max(10);
    let w_db    = schema.fields().iter()
        .map(|f| f.metadata().get(META_DB_TYPE).map(|s| s.len()).unwrap_or(0))
        .max().unwrap_or(0).max(7);

    println!("{pad}{:<w_name$}  {:<w_arrow$}  {:<w_db$}  {:<5}  {:<5}  {}",
        bold(&"Column"), bold(&"Arrow type"), bold(&"DB type"),
        bold(&"PK"), bold(&"Null"), bold(&"Description"),
        w_name = w_name, w_arrow = w_arrow, w_db = w_db);
    println!("{pad}{}", "-".repeat(w_name + w_arrow + w_db + 28));

    for field in schema.fields() {
        let meta   = field.metadata();
        let db_t   = meta.get(META_DB_TYPE)    .map(|s| s.as_str()).unwrap_or("-");
        let pk     = meta.get(META_PRIMARY_KEY).map(|s| s == "true").unwrap_or(false);
        let desc   = meta.get(META_DESCRIPTION).map(|s| s.as_str()).unwrap_or("");
        let arrow  = format!("{:?}", field.data_type());
        let null_m = if field.is_nullable() { "Y" } else { " " };
        let pk_m   = if pk { green(&"Y") } else { dim(&" ") };

        println!("{pad}{:<w_name$}  {:<w_arrow$}  {:<w_db$}  {:<5}  {:<5}  {}",
            field.name(), arrow, db_t, pk_m, null_m, dim(&desc),
            w_name = w_name, w_arrow = w_arrow, w_db = w_db);
    }
}

// -- Formatting helpers --------------------------------------------------------

fn fmt_num(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { out.push(','); }
        out.push(c);
    }
    out.chars().rev().collect()
}

fn fmt_duration(d: std::time::Duration) -> String {
    let ms = d.as_millis();
    if ms < 1_000 {
        format!("{ms} ms")
    } else if ms < 60_000 {
        format!("{:.3} s", d.as_secs_f64())
    } else {
        let m = d.as_secs() / 60;
        let s = d.as_secs_f64() - m as f64 * 60.0;
        format!("{m}m {s:.1}s")
    }
}

// -- ANSI colour helpers -------------------------------------------------------
//
// Only applied when stdout is a terminal.  When piped or redirected the
// helpers return plain strings.
//
// `std::io::IsTerminal` is stable since Rust 1.70.  Edition 2024 requires
// Rust >= 1.85, so this is always available - no unsafe `isatty` needed.

fn is_tty() -> bool {
    use std::io::IsTerminal as _;
    std::io::stdout().is_terminal()
}

macro_rules! colour {
    ($name:ident, $code:expr) => {
        fn $name(s: &dyn std::fmt::Display) -> String {
            if is_tty() {
                format!("\x1b[{}m{}\x1b[0m", $code, s)
            } else {
                s.to_string()
            }
        }
    };
}

colour!(bold,   "1");
colour!(dim,    "2");
colour!(green,  "32");
colour!(yellow, "33");
colour!(blue,   "34");
colour!(red,    "31");