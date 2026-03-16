//! YAML pipeline configuration.
//!
//! YAML is treated as an alternative serialization format for the same pipeline
//! document model used by JSON.  Internally, YAML is parsed into the shared
//! `PipelineDoc` / `StepDef` types from `dag::json`, then processed by the same
//! `build_dag_from_doc()` function.  This means every feature available in the
//! JSON format is also available in YAML — they are equivalent representations.
//!
//! ## Connections
//!
//! Define a `connections` map at the top level; database steps reference entries
//! by name in their `from.connection` or `target.connection` field.  REST API
//! steps use `conn: <name>`.  Every connection is a structured `ConnParams`
//! object discriminated by the `driver` key.
//!
//! ```yaml
//! connections:
//!   pg_source:
//!     driver: postgres
//!     host: db.example.com
//!     database: mydb
//!     auth:
//!       type: user_pass
//!       username: etl_user
//!       password: "p@ss:w0rd"   # any character safe — encoded automatically
//!     options:
//!       ssl: require
//!
//!   github_api:
//!     driver: rest_api
//!     base_url: "https://api.github.com"
//!     auth:
//!       type: bearer
//!       token: "${GITHUB_TOKEN}"
//!     headers:
//!       Accept: "application/vnd.github.v3+json"
//!
//! steps:
//!   - id: employees
//!     type: read_db
//!     from:
//!       connection: pg_source
//!       table: employees
//!       cursor: id
//!
//!   - id: api_issues
//!     type: rest_api
//!     conn: github_api     # base_url + auth + headers from connection
//!     url: /repos/owner/repo/issues
//!     data_path: null      # top-level array
//! ```
//!
//! ## Minimal example
//!
//! ```yaml
//! config:
//!   batch_size: 500
//!   channel_capacity: 4   # max RecordBatches buffered between stages (default: 4)
//!
//! connections:
//!   pg:
//!     driver: postgres
//!     host: localhost
//!     database: mydb
//!     auth:
//!       type: user_pass
//!       username: user
//!       password: "pass"
//!
//! steps:
//!   - id: source_employees
//!     type: read_db
//!     from:
//!       connection: pg
//!       table: employees
//!       cursor: id
//!
//!   - id: active_only
//!     type: filter
//!     input: source_employees
//!     column: status
//!     value: active
//!
//!   - id: add_bonus
//!     type: python_transform
//!     input: active_only
//!     # Inline Python code — table (pyarrow.Table) and pa (pyarrow) are in scope.
//!     # Assign the result to `result`.
//!     code: |
//!       import pyarrow.compute as pc
//!       bonus = pc.multiply(table.column("salary"), 0.10)
//!       result = table.append_column("bonus", bonus.cast(pa.float64()))
//!
//!   - id: output
//!     type: write_db
//!     input: add_bonus
//!     target:
//!       connection: pg
//!       table: employees_enriched
//!     mode: truncate
//! ```
//!
//! ## Named Python transforms
//! ```yaml
//! - id: enrich
//!   type: python_transform
//!   input: source
//!   function: my_registered_function   # must call dag.register_transform() first
//! ```
//!
//! ## YAML → JSON (for database storage)
//! ```rust,no_run
//! let json = potato_etl_runtime::Dag::yaml_to_json(yaml_str)?;
//! // Store `json` in the database, load later with Dag::from_json(&json).
//! ```
//!
//! ## All supported step types
//!
//! | `type`             | Description                                                    |
//! |--------------------|----------------------------------------------------------------|
//! | `read_db`          | Read from database (Postgres / MSSQL / Oracle / MySQL)        |
//! | `rest_api`         | Read from REST API with pagination                            |
//! | `filter`           | Keep rows where `condition:` expr is true (or `column`/`value`) |
//! | `map`              | Add / compute / rename columns via expression DSL             |
//! | `aggregate`        | Group-by + metric aggregation (`sum`, `count`, `avg`, …)      |
//! | `rename`           | Rename columns (Arrow metadata preserved)                     |
//! | `flatten`          | Extract struct sub-fields or JSON paths into top-level columns |
//! | `join`             | Hash-join two branches on a key column                        |
//! | `python_transform` | Apply Python code per batch (sandboxed subprocess)            |
//! | `write_db`         | Write to database (auto-DDL, upsert, truncate, …)             |
//! | `scd2_sink`        | Slowly-changing dimension Type 2 sink                         |
//! | `rest_api_sink`    | POST/PUT batches to a REST API                                |
//!
//! ## File steps
//!
//! | `type`             | Description                                                    |
//! |--------------------|----------------------------------------------------------------|
//! | `read_json`        | Read JSON file(s), supports glob + recursive glob              |
//! | `read_csv`         | Read CSV file(s), supports glob + recursive glob               |
//! | `read_parquet`     | Read Parquet file(s), supports glob + recursive glob           |
//! | `write_json`       | Write to JSON file                                             |
//! | `write_csv`        | Write to CSV file                                              |
//! | `write_parquet`    | Write to Parquet file                                          |
//!
//! ### Glob patterns
//!
//! File sources support glob patterns in `path` (or `from.path`):
//!
//! ```yaml
//! # Single-level glob
//! - id: all_json
//!   type: read_json
//!   path: data/export_*.json
//!   sort_glob: name          # name (default) | name_desc
//!
//! # Recursive glob — search subdirectories
//! - id: deep_csvs
//!   type: read_csv
//!   path: incoming/**/report_*.csv
//!
//! # Character classes and ranges
//! - id: q1_data
//!   type: read_parquet
//!   path: warehouse/events_[1-3].parquet
//! ```
//!
//! See the `json` module docs for the full pattern reference.
//!
//! ## Expression DSL (map / filter)
//!
//! ```yaml
//! # map: add computed columns
//! - id: enrich
//!   type: map
//!   input: source
//!   columns:
//!     load_ts:    now()
//!     revenue:    price * quantity
//!     order_year: year(order_date)
//!     uid:        json_get(payload, "user.id")
//!     tag_0:      json_get(payload, "tags[0]")
//!
//! # filter: keep rows where expression is true
//! - id: active_high_value
//!   type: filter
//!   input: enrich
//!   condition: "status == \"active\" and revenue >= 500"
//!
//! # aggregate: group-by + metrics
//! - id: by_customer
//!   type: aggregate
//!   input: active_high_value
//!   group_by: [customer_id]
//!   metrics:
//!     total_sales:  sum(revenue)
//!     order_count:  count()
//!     avg_revenue:  avg(revenue)
//!
//! # flatten: extract from StructArray or JSON string column
//! - id: flat
//!   type: flatten
//!   input: source
//!   select:
//!     user_id:  payload.user.id
//!     tag_0:    payload.tags[0]
//!     city:     address.city
//! ```
//!
//! ## Environment variables
//!
//! Define pipeline-level variables evaluated once at run start.  Reference them
//! in step expressions with `$name`.
//!
//! ```yaml
//! environment:
//!   load_ts: now()
//!   label: '"nightly_sync"'
//!
//! steps:
//!   - id: enrich
//!     type: map
//!     input: source
//!     columns:
//!       inserted_at: $load_ts
//!       pipeline:    $label
//! ```
use super::json::{build_dag_from_doc, PipelineDoc};
use super::Dag;

use potato_etl_common::secrets::resolve::{resolve_secrets_in_value, extract_secrets_config};

impl Dag {
    /// Loads a complete pipeline definition from a YAML string.
    ///
    /// Named connections in the top-level `connections` map are resolved at
    /// parse time via `from.connection` / `target.connection` references.
    ///
    /// ## Inline Python
    /// Steps with a `code` field have their source stored in
    /// `dag.inline_python_codes()`.  When loaded via the Python wheel (`from_yaml`),
    /// these are auto-registered.  In pure-Rust contexts, iterate
    /// `dag.inline_python_codes()` and register an executor manually before `run()`.
    ///
    /// ## Named Python transforms
    /// Steps with a `function` field reference a transform that must be registered
    /// via `dag.register_transform(name, fn)` before `dag.run()`.
    ///
    /// ## Example
    /// ```rust,no_run
    /// # use potato_etl_runtime::Dag;
    /// let yaml = r#"
    /// connections:
    ///   mydb:
    ///     driver: postgres
    ///     host: db.example.com
    ///     database: orders
    ///     auth:
    ///       type: user_pass
    ///       username: etl_user
    ///       password: "s3cr3t"
    ///
    /// steps:
    ///   - id: src
    ///     type: read_db
    ///     from:
    ///       connection: mydb
    ///       table: orders
    ///   - id: out
    ///     type: write_db
    ///     input: src
    ///     target:
    ///       connection: mydb
    ///       table: orders_copy
    /// "#;
    /// # tokio_test::block_on(async {
    /// let report = Dag::from_yaml(yaml)?.run().await?;
    /// println!("{} rows copied", report.rows_written);
    /// # anyhow::Ok(())
    /// # });
    /// ```
    pub fn from_yaml(yaml: &str) -> anyhow::Result<Self> {
        // Quick check: if the YAML contains secret references, the user must
        // use `from_yaml_async` instead (secret resolution requires async I/O).
        if yaml.contains("secret::") {
            anyhow::bail!(
                "Pipeline YAML contains `secret::` references. \
                 Use `Dag::from_yaml_async()` instead of `Dag::from_yaml()` \
                 to enable secret resolution."
            );
        }
        let doc: PipelineDoc = serde_yaml_ng::from_str(yaml)
            .map_err(|e| anyhow::anyhow!("YAML parse error: {e}"))?;
        build_dag_from_doc(doc)
    }

    /// Loads a pipeline definition from a YAML string, resolving secret references.
    ///
    /// This is the async version of [`from_yaml`] that supports `secret::*`
    /// references in connection strings and other config values.
    ///
    /// ## Secret reference format
    ///
    /// ```yaml
    /// secrets:
    ///   vault:
    ///     address: https://vault.internal:8200
    ///     auth:
    ///       method: token
    ///
    /// connections:
    ///   pg_prod:
    ///     driver: postgres
    ///     host: prod-db.internal
    ///     database: analytics
    ///     auth:
    ///       type: user_pass
    ///       username: "secret::vault/prod/pg#username"
    ///       password: "secret::vault/prod/pg#password"
    /// ```
    pub async fn from_yaml_async(yaml: &str) -> anyhow::Result<Self> {
        // 1. Parse YAML into a raw Value tree.
        let mut value: serde_yaml_ng::Value = serde_yaml_ng::from_str(yaml)
            .map_err(|e| anyhow::anyhow!("YAML parse error: {e}"))?;

        // 2. Extract secrets config and resolve secret references.
        let secrets_config = extract_secrets_config(&value)?;
        if !secrets_config.is_empty() || yaml.contains("secret::") {
            value = resolve_secrets_in_value(value, &secrets_config).await?;
        }

        // 3. Remove the `secrets:` key before deserializing into PipelineDoc
        //    (PipelineDoc doesn't have a `secrets` field).
        if let serde_yaml_ng::Value::Mapping(ref mut map) = value {
            map.remove(&serde_yaml_ng::Value::String("secrets".into()));
        }

        // 4. Deserialize resolved value into PipelineDoc.
        let doc: PipelineDoc = serde_yaml_ng::from_value(value)
            .map_err(|e| anyhow::anyhow!("YAML parse error (after secret resolution): {e}"))?;

        build_dag_from_doc(doc)
    }

    /// Converts a YAML pipeline definition to a canonical, pretty-printed JSON string.
    ///
    /// This is the recommended way to store a pipeline in the database: accept YAML
    /// from users (human-friendly, supports inline Python via multi-line strings),
    /// convert once to JSON, and persist the JSON.  Load it back with `Dag::from_json`.
    ///
    /// The round-trip is lossless: all fields — including inline `code` blocks and the
    /// `connections` map — are preserved in the JSON representation.
    ///
    /// ## Example
    /// ```rust,no_run
    /// # use potato_etl_runtime::Dag;
    /// let yaml = std::fs::read_to_string("pipeline.yaml")?;
    /// let json = Dag::yaml_to_json(&yaml)?;
    /// // INSERT INTO pipelines (graph_json) VALUES ($1)
    /// // Later: Dag::from_json(&row.graph_json)
    /// # anyhow::Ok(())
    /// ```
    pub fn yaml_to_json(yaml: &str) -> anyhow::Result<String> {
        let doc: PipelineDoc = serde_yaml_ng::from_str(yaml)
            .map_err(|e| anyhow::anyhow!("YAML parse error: {e}"))?;
        serde_json::to_string_pretty(&doc)
            .map_err(|e| anyhow::anyhow!("JSON serialization error: {e}"))
    }

    /// Converts a YAML pipeline definition to a compact (single-line) JSON string.
    ///
    /// Use `yaml_to_json` for human-readable storage; use this variant when the
    /// JSON will be embedded in another JSON document or sent over the wire.
    pub fn yaml_to_json_compact(yaml: &str) -> anyhow::Result<String> {
        let doc: PipelineDoc = serde_yaml_ng::from_str(yaml)
            .map_err(|e| anyhow::anyhow!("YAML parse error: {e}"))?;
        serde_json::to_string(&doc)
            .map_err(|e| anyhow::anyhow!("JSON serialization error: {e}"))
    }
}