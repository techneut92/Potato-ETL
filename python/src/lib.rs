//! Python bindings: `etl.ETL`, `etl.ComponentRef` + low-level batch helpers.
//!
//! ```python
//! import potato_etl
//!
//! pipeline = potato_etl.ETL(batch_size=500)
//!
//! # Linear pipeline
//! orders   = pipeline.read_db("postgresql://...", table="orders", cursor="id")
//! filtered = pipeline.filter(orders, "status", "active")
//! enriched = pipeline.python_transform(filtered, lambda b: b)
//! pipeline.write_db(enriched, "postgresql://...", table="output", mode="truncate")
//!
//! # Fan-out: same node as input for two sinks
//! source  = pipeline.read_db("postgresql://...", table="events", cursor="id")
//! sink_a  = pipeline.write_db(source, "postgresql://...", table="events_eu")
//! sink_b  = pipeline.write_db(source, "postgresql://...", table="events_us")
//!
//! # Fan-in: merge two sources via join
//! customers = pipeline.read_db("mssql://...", table="customers")
//! joined    = pipeline.join(orders, customers, on="customer_id")
//!
//! stats = pipeline.run()
//! print(stats)  # {"rows_read": ..., "rows_written": ..., "components": ...}
//!
//! # JSON config
//! pipeline2 = potato_etl.ETL.from_json(open("pipeline.json").read())
//! pipeline2.register_transform("add_bonus", add_bonus_fn)
//! stats2 = pipeline2.run()
//!
//! # Environment variables (evaluated once at pipeline start)
//! pipeline3 = potato_etl.ETL(batch_size=500)
//! pipeline3.set_env("load_ts", "now()")
//! pipeline3.set_env("label", '"nightly"')
//! src = pipeline3.read_db("postgresql://...", table="orders")
//! enriched = pipeline3.map(src, {"inserted_at": "$load_ts", "pipeline": "$label"})
//! pipeline3.write_db(enriched, "postgresql://...", table="output")
//! stats3 = pipeline3.run()
//! ```

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use indexmap::IndexMap;

use arrow::pyarrow::{PyArrowType, ToPyArrow};
use arrow::record_batch::RecordBatch;
use potato_etl_runtime::{
    AuthConfig, Dag, ETLConfig, HttpMethod, IdentifierCase, JoinHow, PaginationConfig, ReadDB,
    ReadOptions, RestApiOptions, RestApiSinkOptions, RunReport, Scd2Sink, SinkMode, SinkWriteMode,
    StepDriverOptions, TransformFn, WriteDB, WriteOptions,
    ComponentSchema, ArrowSchemaConfig, ArrowColumnDef, DatabaseSchemaConfig, DatabaseColumnDef,
};
use futures::StreamExt;
use pyo3::exceptions::{PyRuntimeError, PyStopIteration, PyValueError};
use pyo3::prelude::*;

/// PyO3 0.28 removed the `PyObject` type alias from prelude.
type PyObject = Py<PyAny>;

/// Release the GIL, run `f`, then re-acquire the GIL.
///
/// Equivalent to the old `py.allow_threads(f)` which was removed in PyO3 0.28.
/// Uses the CPython C API directly.
///
/// # Safety contract
/// The caller must ensure `f` does not interact with any Python objects.
/// The GIL is released for the duration of `f`.
fn allow_threads<F, R>(_py: Python<'_>, f: F) -> R
where
    F: FnOnce() -> R,
{
    unsafe {
        let save = pyo3::ffi::PyEval_SaveThread();
        let result = f();
        pyo3::ffi::PyEval_RestoreThread(save);
        result
    }
}

/// Acquire the GIL from any thread and run `f`.
///
/// Equivalent to the old `Python::with_gil(f)` which was removed in PyO3 0.28
/// for extension modules.  In an extension module the Python runtime is always
/// active, so `try_attach` always succeeds.
fn with_gil<F, R>(f: F) -> R
where
    F: for<'py> FnOnce(Python<'py>) -> R,
{
    Python::try_attach(f).expect("Python runtime must be active (extension module)")
}

// ── Tokio runtime ─────────────────────────────────────────────────────────────

fn rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime")
    })
}

// ── BatchIter ─────────────────────────────────────────────────────────────────

#[pyclass(unsendable)]
struct BatchIter {
    rx: std::sync::mpsc::Receiver<anyhow::Result<RecordBatch>>,
}

#[pymethods]
impl BatchIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> { slf }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let recv = allow_threads(py, || self.rx.recv());
        match recv {
            Err(_)        => Err(PyStopIteration::new_err(())),
            Ok(Err(e))    => Err(PyRuntimeError::new_err(e.to_string())),
            Ok(Ok(batch)) => Ok(batch.to_pyarrow(py)?.into()),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ComponentRef — handle to a node in the DAG
// ─────────────────────────────────────────────────────────────────────────────

/// Reference to a component in the ETL pipeline.
///
/// Returned by `ETL.read_db`, `ETL.filter`, `ETL.join`, etc.
/// Pass this object to the next step in the pipeline.
///
/// Supports the `>>` operator for fluent chaining::
///
///   orders >> filter_step >> sink
#[pyclass(name = "ComponentRef", skip_from_py_object)]
#[derive(Clone)]
struct PyComponentRef {
    id: String,
}

#[pymethods]
impl PyComponentRef {
    fn __repr__(&self) -> String {
        format!("ComponentRef('{}')", self.id)
    }

    /// Syntactic sugar: `a >> b` connects b downstream of a.
    /// `b` must be a `ComponentRef`.  For most cases `etl.filter(source, ...)` is clearer.
    fn __rshift__(&self, other: &PyComponentRef) -> PyComponentRef {
        // The connection is already established by the ETL builder methods;
        // this operator simply returns the right-hand side for chaining.
        other.clone()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ETL — factory + orchestrator
// ─────────────────────────────────────────────────────────────────────────────

/// DAG-based ETL pipeline builder.
///
/// **Usage**::
///
///   pipeline = etl.ETL(batch_size=1000)
///
///   orders   = pipeline.read_db("postgresql://...", table="orders", cursor="id")
///   filtered = pipeline.filter(orders, "status", "active")
///   joined   = pipeline.join(orders, customers, on="customer_id", how="inner")
///   enriched = pipeline.python_transform(filtered, my_func)
///   pipeline.write_db(enriched, "postgresql://...", table="output", mode="truncate")
///
///   stats = pipeline.run()
///
/// **JSON**::
///
///   pipeline = etl.ETL.from_json(open("pipeline.json").read())
///   pipeline.register_transform("my_func", my_func)
///   stats = pipeline.run()
#[pyclass(name = "ETL")]
struct PyETL {
    dag: Dag,
}

#[pymethods]
impl PyETL {
    // ── Constructors ──────────────────────────────────────────────────────────

    #[new]
    #[pyo3(signature = (batch_size = 1000))]
    fn new(batch_size: usize) -> Self {
        Self { dag: Dag::new(ETLConfig { batch_size, ..Default::default() }) }
    }

    /// Load a pipeline definition from a JSON string.
    ///
    /// Example JSON::
    ///
    ///   {
    ///     "config": { "batch_size": 500 },
    ///     "steps": [
    ///       { "id": "src",  "type": "read_db",  "from": { "connection": "postgresql://...", "table": "orders" } },
    ///       { "id": "xfrm","type": "python_transform", "input": "src", "function": "add_bonus" },
    ///       { "id": "dst",  "type": "write_db", "input": "xfrm", "target": { "connection": "...", "table": "out" } }
    ///     ]
    ///   }
    ///
    /// Inline Python code (``"code"`` field) is auto-registered.
    /// Named transforms (``"function"`` field) must be registered via
    /// ``pipeline.register_transform(name, fn)`` before ``run()``.
    #[staticmethod]
    fn from_json(json_str: &str) -> PyResult<Self> {
        let mut dag = Dag::from_json(json_str)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        // Auto-register any inline Python code blocks embedded in the JSON.
        register_inline_python_codes(&mut dag)?;
        Ok(Self { dag })
    }

    /// Load a pipeline definition from a YAML string.
    ///
    /// YAML is an alternative, human-friendly format for the same pipeline model.
    /// All JSON step types are supported.  Inline Python code is written as a
    /// YAML multi-line string (``|`` block scalar)::
    ///
    ///   config:
    ///     batch_size: 500
    ///
    ///   connections:
    ///     pg:
    ///       driver: postgres
    ///       host: db.example.com
    ///       database: mydb
    ///       auth:
    ///         type: user_pass
    ///         username: etl_user
    ///         password: "s3cr3t"
    ///
    ///   steps:
    ///     - id: source
    ///       type: read_db
    ///       from:
    ///         connection: pg
    ///         table: employees
    ///
    ///     - id: add_bonus
    ///       type: python_transform
    ///       input: source
    ///       code: |
    ///         import pyarrow.compute as pc
    ///         bonus = pc.multiply(table.column("salary"), 0.10)
    ///         result = table.append_column("bonus", bonus.cast(pa.float64()))
    ///
    ///     - id: output
    ///       type: write_db
    ///       input: add_bonus
    ///       target:
    ///         connection: pg
    ///         table: employees_enriched
    ///       mode: truncate
    ///
    /// **In-scope variables for inline code:**
    ///   - ``table`` — ``pyarrow.Table`` containing the current batch
    ///   - ``pa``    — the ``pyarrow`` module
    ///   - Assign the output to ``result`` (Table or RecordBatch)
    ///
    /// Named transforms (``function`` field) must still be registered via
    /// ``pipeline.register_transform(name, fn)``.
    #[staticmethod]
    fn from_yaml(yaml_str: &str) -> PyResult<Self> {
        let mut dag = Dag::from_yaml(yaml_str)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        // Auto-register inline Python code blocks.
        register_inline_python_codes(&mut dag)?;
        Ok(Self { dag })
    }

    /// Load a pipeline from YAML with secret manager resolution.
    ///
    /// Supports ``secret::`` references for HashiCorp Vault, Azure Key Vault,
    /// and Google Secret Manager.  Requires a ``secrets:`` config block in the
    /// YAML::
    ///
    ///   secrets:
    ///     vault:
    ///       address: https://vault.internal:8200
    ///       auth:
    ///         method: token
    ///
    ///   connections:
    ///     pg_prod:
    ///       driver: postgres
    ///       host: prod-db.internal
    ///       database: analytics
    ///       auth:
    ///         type: user_pass
    ///         username: "secret::vault/prod/pg#username"
    ///         password: "secret::vault/prod/pg#password"
    #[staticmethod]
    fn from_yaml_with_secrets(yaml_str: &str) -> PyResult<Self> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyValueError::new_err(format!("Failed to create async runtime: {e}")))?;
        let mut dag = rt.block_on(Dag::from_yaml_async(yaml_str))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        register_inline_python_codes(&mut dag)?;
        Ok(Self { dag })
    }

    /// Convert a YAML pipeline definition to canonical pretty-printed JSON.
    ///
    /// Use this to normalise a user-supplied YAML file before storing it in
    /// the database.  The resulting JSON string is accepted by ``from_json``::
    ///
    ///   json_str = etl.ETL.yaml_to_json(open("pipeline.yaml").read())
    ///   # store json_str in the database
    ///   pipeline = etl.ETL.from_json(json_str)
    ///
    /// The conversion is lossless: inline ``code`` blocks are preserved as-is
    /// in the JSON ``"code"`` field.
    #[staticmethod]
    fn yaml_to_json(yaml_str: &str) -> PyResult<String> {
        Dag::yaml_to_json(yaml_str)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Returns a dict of inline Python code blocks in this pipeline.
    ///
    /// Keys are internal transform names (e.g. ``"__inline_add_bonus"``).
    /// Values are the Python source strings.
    ///
    /// Useful for debugging or for implementing a custom executor in pure Python::
    ///
    ///   for name, code in pipeline.inline_python_codes().items():
    ///       print(f"{name}:\\n{code}")
    fn inline_python_codes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let d = pyo3::types::PyDict::new(py);
        for (k, v) in self.dag.inline_python_codes() {
            d.set_item(k, v)?;
        }
        Ok(d)
    }

    // ── Environment variables ──────────────────────────────────────────────────

    /// Set a pipeline environment variable (expression evaluated once at run start).
    ///
    /// Environment variables are referenced in step expressions with ``$name``
    /// or ``env("name")``.  They are evaluated once before any step runs and
    /// the result is frozen for the entire pipeline execution.
    ///
    /// ::
    ///
    ///   pipeline.set_env("load_ts", "now()")
    ///   pipeline.set_env("label", '"nightly_sync"')
    ///
    ///   enriched = pipeline.map(source, {"inserted_at": "$load_ts", "label": "$label"})
    fn set_env(&mut self, name: &str, expr: &str) {
        self.dag.set_env(name, expr);
    }

    // ── Transform registration ────────────────────────────────────────────────

    /// Register a Python function as a named transform (for JSON pipelines).
    ///
    ///   pipeline.register_transform("add_bonus", add_bonus_fn)
    ///
    /// The function receives a PyArrow RecordBatch and must return a RecordBatch.
    fn register_transform(&mut self, name: &str, func: PyObject) {
        let transform_fn = make_py_transform_fn(func);
        self.dag.register_transform(name, transform_fn);
    }

    // ── Source ────────────────────────────────────────────────────────────────

    /// Add a database source component.
    ///
    /// Parameters:
    ///   conn              : connection string (postgresql://, mssql://, oracle://)
    ///   table             : table name (omit when `query` is supplied)
    ///   schema            : database schema / owner (default: "public" / "dbo")
    ///   query             : custom SQL query
    ///   cursor            : column name for cursor-based pagination (e.g. "id")
    ///   normalize_columns : True  → lowercase all column names
    ///                       False → preserve original case
    ///                       None  → auto: True for oracle://, False for others
    ///   arrow_overrides   : dict { col: "arrow_type_str" }
    ///                       Per-column Arrow type overrides applied immediately after reading.
    ///   batch_size        : override the global batch_size for this source only
    ///   exclude           : list of column names to drop immediately after reading
    ///   options           : dict of driver-specific options (see StepDriverOptions)
    ///                       Keys: mode (Databricks: "api"|"odbc"),
    ///                       prefetch_rows (Oracle), fetch_array_size (Oracle),
    ///                       identifier_case ("as_is"|"upper"|"lower")
    #[pyo3(signature = (conn, *, table=None, schema=None, query=None, cursor=None, normalize_columns=None, arrow_overrides=None, batch_size=None, exclude=None, options=None))]
    fn read_db(
        &mut self,
        conn:              &str,
        table:             Option<String>,
        schema:            Option<String>,
        query:             Option<String>,
        cursor:            Option<String>,
        normalize_columns: Option<bool>,
        arrow_overrides:   Option<HashMap<String, String>>,
        batch_size:        Option<usize>,
        exclude:           Option<Vec<String>>,
        options:           Option<HashMap<String, PyObject>>,
        py:                Python<'_>,
    ) -> PyResult<PyComponentRef> {
        let driver_opts = parse_step_driver_options(options, py)?;
        let id = self.dag.next_id("read_db");
        let arrow_cfg = arrow_overrides.map(|ao| ArrowSchemaConfig {
            columns: ao.into_iter().map(|(k, v)| (k, ArrowColumnDef {
                arrow_type: Some(v), nullable: None, logical_type: None, value: None,
            })).collect(),
        });
        let comp_schema = ComponentSchema {
            database: None,
            arrow: arrow_cfg,
        };
        self.dag.add_source(id.clone(), conn, ReadOptions {
            table, db_schema: schema, query, cursor,
            source_schema: potato_etl_runtime::SourceSchemaConfig {
                schema: comp_schema,
                normalize_columns,
                exclude: exclude.unwrap_or_default(),
            },
            batch_size,
            options: driver_opts,
        });
        Ok(PyComponentRef { id })
    }

    // ── REST API source ───────────────────────────────────────────────────────

    /// Add a REST API source component.
    ///
    /// **Minimal example**::
    ///
    ///   api = pipeline.read_api("https://api.example.com/employees",
    ///                            data_path="data")
    ///
    /// **Full example**::
    ///
    ///   api = pipeline.read_api(
    ///       "https://api.example.com/employees",
    ///       method        = "GET",
    ///       headers       = {"X-Tenant": "acme", "Accept": "application/json"},
    ///       params        = {"active": "true", "format": "json"},
    ///       auth          = {"type": "bearer", "token": "eyJ..."},
    ///       data_path     = "data",          # or "results.items" for nested arrays
    ///       pagination    = "cursor",
    ///       cursor_path   = "meta.next_cursor",
    ///       cursor_param  = "cursor",
    ///       page_size     = 100,
    ///       rate_limit_rps = 5.0,
    ///       timeout_secs  = 30,
    ///   )
    ///
    /// **Auth types** (via `auth` dict):
    ///   - ``{"type": "bearer",  "token": "..."}``
    ///   - ``{"type": "basic",   "username": "...", "password": "..."}``
    ///   - ``{"type": "api_key", "header": "X-API-Key", "key": "..."}``
    ///
    /// **Pagination** (via `pagination` string):\n
    ///   - ``"none"``        — single request (default)\n
    ///   - ``"cursor"``      — cursor in response body; requires `cursor_path` + `cursor_param`\n
    ///   - ``"offset"``      — offset/limit parameters\n
    ///   - ``"page"``        — page number\n
    ///   - ``"link_header"`` — RFC 5988 ``Link: <url>; rel="next"`` header\n
    ///\n
    /// **Offset / Page pagination and insertion drift**::\n
    ///\n
    ///   Offset-based pagination is vulnerable to concurrent insertions on the\n
    ///   source API: new records shift the offset, causing duplicates or gaps.\n
    ///   Always set `dedup_key` when using ``pagination="offset"`` or\n
    ///   ``pagination="page"`` on a live source.  Example::\n
    ///\n
    ///     api = pipeline.read_api(\n
    ///         "https://api.example.com/employees",\n
    ///         pagination       = "offset",\n
    ///         offset_param     = "skip",\n
    ///         limit_param      = "take",\n
    ///         page_size        = 100,\n
    ///         total_count_path = "meta.total",   # optional: early stop\n
    ///         has_more_path    = "meta.has_more", # optional: reliable stop signal\n
    ///         dedup_key        = "id",            # required: drop offset-drift duplicates\n
    ///     )\n
    #[pyo3(signature = (
        url,
        *,
        method           = "GET",
        headers          = None,
        params           = None,
        body             = None,
        auth             = None,
        data_path        = None,
        pagination       = "none",
        cursor_path      = None,
        cursor_param     = None,
        offset_param     = None,
        limit_param      = None,
        page_param       = None,
        size_param       = None,
        page_size        = 100usize,
        first_page       = 1usize,
        total_count_path = None,
        has_more_path    = None,
        dedup_key        = None,
        rate_limit_rps   = None,
        timeout_secs     = None,
        allow_non_2xx    = false,
        arrow_overrides  = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn read_api(
        &mut self,
        url:              &str,
        method:           &str,
        headers:          Option<HashMap<String, String>>,
        params:           Option<HashMap<String, String>>,
        body:             Option<String>,
        auth:             Option<HashMap<String, String>>,
        data_path:        Option<String>,
        pagination:       &str,
        cursor_path:      Option<String>,
        cursor_param:     Option<String>,
        offset_param:     Option<String>,
        limit_param:      Option<String>,
        page_param:       Option<String>,
        size_param:       Option<String>,
        page_size:        usize,
        first_page:       usize,
        total_count_path: Option<String>,
        has_more_path:    Option<String>,
        dedup_key:        Option<String>,
        rate_limit_rps:   Option<f64>,
        timeout_secs:     Option<u64>,
        allow_non_2xx:    bool,
        arrow_overrides:  Option<HashMap<String, String>>,
    ) -> PyResult<PyComponentRef> {
        // HTTP method
        let http_method = match method.to_uppercase().as_str() {
            "POST"   => HttpMethod::Post,
            "PUT"    => HttpMethod::Put,
            "PATCH"  => HttpMethod::Patch,
            "DELETE" => HttpMethod::Delete,
            _        => HttpMethod::Get,
        };

        // Auth — reuse the shared helper instead of duplicating the match
        let auth_cfg = parse_auth_dict(auth)?;

        // Pagination
        let pagination_cfg = match pagination.to_lowercase().as_str() {
            "cursor" => PaginationConfig::Cursor {
                cursor_path:  cursor_path.ok_or_else(|| PyValueError::new_err(
                    "pagination='cursor' requires cursor_path=... (e.g. 'meta.next_cursor')"
                ))?,
                cursor_param: cursor_param.unwrap_or_else(|| "cursor".into()),
                page_size,
                size_param,
            },
            "offset" => PaginationConfig::Offset {
                offset_param:     offset_param.unwrap_or_else(|| "offset".into()),
                limit_param:      limit_param.unwrap_or_else(||  "limit".into()),
                page_size,
                total_count_path,
                has_more_path,
            },
            "page" => PaginationConfig::Page {
                page_param:       page_param.unwrap_or_else(|| "page".into()),
                size_param:       size_param.unwrap_or_else(||  "page_size".into()),
                page_size,
                first_page,
                total_count_path,
                has_more_path,
            },
            "link_header" | "link" => PaginationConfig::LinkHeader,
            _ => PaginationConfig::None,
        };

        // Build RestApiOptions
        let opts = RestApiOptions {
            method:         http_method,
            headers:        headers.unwrap_or_default(),
            params:         params.unwrap_or_default(),
            body,
            auth:           auth_cfg,
            data_path,
            pagination:     pagination_cfg,
            rate_limit_rps,
            timeout_secs,
            allow_non_2xx,
            dedup_key,
        };

        let id = self.dag.next_id("rest_api");
        let arrow_cfg = arrow_overrides.map(|ao| ArrowSchemaConfig {
            columns: ao.into_iter().map(|(k, v)| (k, ArrowColumnDef {
                arrow_type: Some(v), nullable: None, logical_type: None, value: None,
            })).collect(),
        });
        self.dag.add_rest_api(id.clone(), url, opts, potato_etl_runtime::SourceSchemaConfig {
            schema: ComponentSchema { database: None, arrow: arrow_cfg },
            ..Default::default()
        });
        Ok(PyComponentRef { id })
    }

    // ── Transforms ────────────────────────────────────────────────────────────

    /// Filter rows.
    ///
    /// Two forms:
    ///   active = pipeline.filter(source, column="status", value="active")
    ///   active = pipeline.filter(source, condition="status == 'active' and total >= 100")
    ///
    #[pyo3(signature = (input, column=None, value=None, condition=None))]
    fn filter(
        &mut self,
        input:        &PyComponentRef,
        column:       Option<&str>,
        value:        Option<&str>,
        condition:    Option<&str>,
    ) -> PyResult<PyComponentRef> {
        let id = self.dag.next_id("filter");
        self.dag.add_filter(
            id.clone(), &input.id,
            column.map(|s| s.to_string()),
            value.map(|s| s.to_string()),
            condition.map(|s| s.to_string()),
        );
        Ok(PyComponentRef { id })
    }

    /// Add/compute/rename columns using the expression DSL.
    ///
    ///   enrich = pipeline.map(source, {
    ///       "load_ts":    "now()",
    ///       "revenue":    "price * quantity",
    ///       "order_year": "year(order_date)",
    ///   })
    ///
    /// select_only: if True, only the listed columns appear in the output.
    #[pyo3(signature = (input, columns, select_only=false))]
    fn map(
        &mut self,
        input:        &PyComponentRef,
        columns:      HashMap<String, String>,
        select_only:  bool,
    ) -> PyResult<PyComponentRef> {
        let id = self.dag.next_id("map");
        // Convert HashMap → IndexMap.
        // Python 3.7+ dicts are ordered, but PyO3's HashMap extraction does not
        // preserve that order.  For guaranteed column ordering, use Dag::from_yaml.
        let ordered: IndexMap<String, String> = columns.into_iter().collect();
        self.dag.add_map(id.clone(), &input.id, ordered, select_only);
        Ok(PyComponentRef { id })
    }

    /// Group rows and compute aggregate metrics.
    ///
    ///   summary = pipeline.aggregate(enrich,
    ///       group_by=["customer_id"],
    ///       metrics={"total_sales": "sum(revenue)", "n": "count()"}
    ///   )
    #[pyo3(signature = (input, group_by=None, metrics=None))]
    fn aggregate(
        &mut self,
        input:        &PyComponentRef,
        group_by:     Option<Vec<String>>,
        metrics:      Option<HashMap<String, String>>,
    ) -> PyResult<PyComponentRef> {
        let id = self.dag.next_id("aggregate");
        let metrics_map: indexmap::IndexMap<String, String> = metrics.unwrap_or_default().into_iter().collect();
        self.dag.add_aggregate(
            id.clone(), &input.id,
            group_by.unwrap_or_default(),
            metrics_map,
        );
        Ok(PyComponentRef { id })
    }

    /// Rename columns.  Preserves ALL Arrow metadata under the new column names.
    ///
    ///   renamed = pipeline.rename(source, {"emp_id": "id", "emp_name": "name"})
    #[pyo3(signature = (input, columns))]
    fn rename(
        &mut self,
        input:        &PyComponentRef,
        columns:      HashMap<String, String>,
    ) -> PyResult<PyComponentRef> {
        let id = self.dag.next_id("rename");
        let columns_map: indexmap::IndexMap<String, String> = columns.into_iter().collect();
        self.dag.add_rename(id.clone(), &input.id, columns_map);
        Ok(PyComponentRef { id })
    }

    /// Lowercase all column names from this component.
    ///
    /// When to use:
    /// - After a join where the right side still has UPPERCASE/CamelCase column names
    /// - For Postgres or MSSQL sources you want to explicitly normalise
    ///   (Oracle does this automatically when normalize_columns=True)
    ///
    /// Example::
    ///
    ///   # Oracle column "EmployeeId" → "employeeid", "SALARY" → "salary"
    ///   lowered = pipeline.lowercase_columns(oracle_source)
    ///
    ///   # After a join: right side had CamelCase column names
    ///   joined  = pipeline.join(left, right, on="id")
    ///   lowered = pipeline.lowercase_columns(joined)
    fn lowercase_columns(&mut self, input: &PyComponentRef) -> PyComponentRef {
        let id = self.dag.next_id("lowercase_columns");
        self.dag.add_lowercase_columns(id.clone(), &input.id);
        PyComponentRef { id }
    }

    /// Apply a Python function to every batch from this component.
    ///
    /// **Signature**: `fn(batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch`
    ///
    /// Use this for simple single-stream transformations.
    /// To merge two streams, use `join()`.
    ///
    ///   def add_bonus(batch):
    ///       bonus = pc.multiply(batch.column("salary"), 0.10)
    ///       return batch.append_column("bonus", bonus.cast(pa.float64()))
    ///
    ///   enriched = pipeline.python_transform(source, add_bonus)
    ///
    /// Lambdas work too for simple cases::
    ///
    ///   upper = pipeline.python_transform(source,
    ///               lambda b: b.set_column(
    ///                   b.schema.get_field_index("name"),
    ///                   "name",
    ///                   pc.utf8_upper(b.column("name"))
    ///               ))
    fn python_transform(
        &mut self,
        input: &PyComponentRef,
        func:  PyObject,
    ) -> PyComponentRef {
        let id           = self.dag.next_id("python_transform");
        let transform_fn = make_py_transform_fn(func);
        self.dag.add_transform(id.clone(), &input.id, transform_fn);
        PyComponentRef { id }
    }

    /// Apply a Rust FilterTransform (internal, for run_pipeline compatibility).
    fn rust_filter(
        &mut self,
        input:  &PyComponentRef,
        column: String,
        value:  String,
    ) -> PyComponentRef {
        let id = self.dag.next_id("rust_filter");
        let func: TransformFn = Arc::new(move |batch| {
            potato_etl_runtime::transform::filter::apply_filter(&batch, &column, &value)
        });
        self.dag.add_transform(id.clone(), &input.id, func);
        PyComponentRef { id }
    }

    // ── Flatten ───────────────────────────────────────────────────────────────

    /// Extract and/or rename fields from Arrow StructArray columns.
    ///
    /// REST APIs often return nested JSON objects.  When parsed, the nested
    /// object becomes a StructArray column.  This step lets you extract
    /// specific sub-fields into top-level columns.
    ///
    /// **`select`** — dict mapping output column names to dot-notation paths::
    ///
    ///   flat = pipeline.flatten(source, select={
    ///       "employee_id": "EmId",          # rename a top-level column
    ///       "street":      "address.street", # extract a nested sub-field
    ///       "city":        "address.city",
    ///   })
    ///
    /// **Empty `select`** — auto-expand every StructArray column one level.
    /// Sub-fields become top-level columns named ``{parent}_{child}``::
    ///
    ///   flat = pipeline.flatten(source)  # expand all structs one level
    ///
    #[pyo3(signature = (input, select=None))]
    fn flatten(
        &mut self,
        input:        &PyComponentRef,
        select:       Option<HashMap<String, String>>,
    ) -> PyResult<PyComponentRef> {
        let id = self.dag.next_id("flatten");
        let select_map: indexmap::IndexMap<String, String> = select.unwrap_or_default().into_iter().collect();
        self.dag.add_flatten(id.clone(), &input.id, select_map);
        Ok(PyComponentRef { id })
    }

    // ── Join ──────────────────────────────────────────────────────────────────

    /// Combine two sources via a hash join.
    ///
    ///   joined = pipeline.join(orders, customers, on="customer_id", how="inner")
    ///
    /// how: "inner" (default) | "left" | "full"
    ///
    /// The RIGHT side is fully buffered in memory (expected to be the smaller table).
    ///
    #[pyo3(signature = (left, right, on, how = "inner"))]
    fn join(
        &mut self,
        left:         &PyComponentRef,
        right:        &PyComponentRef,
        on:           &str,
        how:          &str,
    ) -> PyResult<PyComponentRef> {
        let id = self.dag.next_id("join");
        let join_how = match how {
            "inner"                => JoinHow::Inner,
            "left" | "left_outer"  => JoinHow::Left,
            "full" | "full_outer"  => JoinHow::Full,
            other => return Err(PyValueError::new_err(format!(
                "Unknown join type '{other}'. Valid: inner, left, left_outer, full, full_outer"
            ))),
        };
        self.dag.add_join(id.clone(), &left.id, &right.id, on, join_how);
        Ok(PyComponentRef { id })
    }

    // ── Sinks ─────────────────────────────────────────────────────────────────

    /// Write to a database (no further steps after this).
    ///
    ///   pipeline.write_db(enriched, "postgresql://...", table="output", mode="truncate")
    ///
    /// Parameters:
    ///   mode         : "append" (default) | "insert_ignore" | "upsert" |
    ///                  "merge_delete" | "truncate"
    ///   create_table   : "never" (default) | "if_not_exists" | "replace"
    ///                    Creates the target table from the Arrow schema of the
    ///                    first batch.  Use `column_options` (incl. `db_type`) to
    ///                    control DDL output.
    ///   column_options : dict { col: { "primary_key": bool, "unique": bool,
    ///                    "index": bool, "nullable": bool, "db_type": "SQL_TYPE",
    ///                    "check_expr": "...", "default_expr": "...",
    ///                    "on_update_expr": "...", "description": "...",
    ///                    "foreign_key": { "table": "...", "column": "..." } } }
    ///                    Mark merge key columns with primary_key=True.
    ///                    The upsert/insert_ignore/merge_delete modes read the key
    ///                    from etl.primary_key metadata — no separate upsert_key needed.
    ///   values          : dict { target_col: "$source_col" | "null" } — value injection
    ///                    (converted to ArrowColumnDef entries with `value:` set)
    ///   arrow_overrides : dict { col: "arrow_type_str" } — per-column Arrow type casts
    ///   batch_size      : override the global batch_size for this sink only
    ///   options         : dict of driver-specific options (see StepDriverOptions)
    ///                     Keys: mode (MSSQL: "tiberius"|"bcp"|"odbc"),
    ///                     bcp_path (str), bcp_staging (bool),
    ///                     direct_path (bool), parallel (int),
    ///                     oci_batch_size (int),
    ///                     identifier_case ("as_is"|"upper"|"lower")
    #[pyo3(signature = (input, conn, *, table, schema=None, mode="append",
                        create_table="never", column_options=None,
                        values=None, arrow_overrides=None, batch_size=None, options=None))]
    fn write_db(
        &mut self,
        input:           &PyComponentRef,
        conn:            &str,
        table:           String,
        schema:          Option<String>,
        mode:            &str,
        create_table:    &str,
        column_options:  Option<HashMap<String, PyObject>>,
        values:          Option<HashMap<String, String>>,
        arrow_overrides: Option<HashMap<String, String>>,
        batch_size:      Option<usize>,
        options:         Option<HashMap<String, PyObject>>,
        py:              Python<'_>,
    ) -> PyResult<PyComponentRef> {
        let sink_mode = match mode {
            "append"        => SinkMode::Append,
            "insert_ignore" => SinkMode::InsertIgnore,
            "upsert"        => SinkMode::Upsert,
            "merge_delete"  => SinkMode::MergeDelete,
            "truncate"      => SinkMode::Truncate,
            other           => return Err(PyValueError::new_err(format!(
                "Unknown write mode '{other}'. Valid: append, insert_ignore, upsert, \
                 merge_delete, truncate. The merge key comes from primary_key=True \
                 in column_options — no upsert_key parameter needed."
            ))),
        };
        let ct = parse_create_table_mode(create_table)?;
        let co = parse_py_column_options(column_options, py)?;
        let driver_opts = parse_step_driver_options(options, py)?;
        let id = self.dag.next_id("write_db");
        let comp_schema = build_component_schema(arrow_overrides, co, values);
        self.dag.add_sink(id.clone(), &input.id, conn, WriteOptions {
            table, db_schema: schema, mode: sink_mode, create_table: ct,
            sink_schema: potato_etl_runtime::SinkSchemaConfig {
                schema: comp_schema,
            },
            batch_size,
            options: driver_opts,
        });
        Ok(PyComponentRef { id })
    }

    /// Write to an SCD Type 2 history table.
    ///
    ///   pipeline.scd2_sink(source, "postgresql://...", table="orders_hist", key="id")
    ///
    /// Parameters:
    ///   key          : natural business key column name
    ///   track        : list of columns to monitor for changes.  Omit to track ALL
    ///                  non-key columns (the default — recommended).
    ///   col_names    : dict to override SCD2 system column names:
    ///                    {"valid_from": "eff_from", "valid_to": "eff_to",
    ///                     "is_current": "is_latest", "scd_id": "sk"}
    ///   create_table : "never" (default) | "if_not_exists" | "replace"
    ///                  When set, creates the SCD2 target table automatically.
    ///                  SCD2 system columns (scd_id, valid_from, valid_to, is_current)
    ///                  are prepended in the correct dialect-specific syntax.
    ///                  A (key_col, is_current) covering index is created automatically.
    ///   close_missing : bool (default: False) — when True, keys that are present
    ///                  in the database (is_current = true) but absent from ALL
    ///                  incoming batches are expired at flush time.  Use for
    ///                  full-snapshot ingestion.  Leave False for delta/cherry-pick.
    ///   column_options : dict { col: { "primary_key": bool, "db_type": "SQL_TYPE", ... } }
    ///                    Applied BEFORE writing.  Metadata is baked into DDL when
    ///                    create_table is set.  Use `db_type` for per-column SQL type
    ///                    overrides (replaces the old `type_override` parameter).
    ///   values         : dict { target_col: "$source_col" | "null" } — value injection.
    ///                    Converted to ArrowColumnDef entries with `value:` set.
    ///                    Inject columns from other columns or NULL at the sink boundary
    ///                    before SCD2 processing.
    ///   arrow_overrides : dict { col: "arrow_type_str" } — per-column Arrow type casts
    ///
    /// Full example with auto-create::
    ///
    ///   history = pipeline.scd2_sink(
    ///       employees_mapped, PG,
    ///       table          = "employees_history",
    ///       schema         = "hr",
    ///       key            = "employee_id",
    ///       create_table   = "if_not_exists",
    ///       column_options = {
    ///           "employee_id": {"nullable": False, "db_type": "VARCHAR(50)",
    ///                           "description": "AFAS employee number"},
    ///           "salary":      {"db_type": "NUMERIC(10,2)", "check_expr": "salary >= 0"},
    ///           "email":       {"unique": True},
    ///       },
    ///   )
    ///
    /// Additional parameters:
    ///   batch_size     : override the global batch_size for this sink only
    ///   options        : dict of driver-specific options (see StepDriverOptions)
    ///                    Keys: mode (MSSQL: "tiberius"|"bcp"|"odbc"),
    ///                    bcp_path (str), bcp_staging (bool),
    ///                    direct_path (bool), parallel (int),
    ///                    oci_batch_size (int),
    ///                    identifier_case ("as_is"|"upper"|"lower")
    #[pyo3(signature = (input, conn, *, table, schema=None, key="id", track=None,
                        col_names=None, create_table="never", close_missing=false,
                        column_options=None, values=None, arrow_overrides=None,
                        batch_size=None, options=None))]
    fn scd2_sink(
        &mut self,
        input:           &PyComponentRef,
        conn:            &str,
        table:           String,
        schema:          Option<String>,
        key:             &str,
        track:           Option<Vec<String>>,
        col_names:       Option<std::collections::HashMap<String, String>>,
        create_table:    &str,
        close_missing:   bool,
        column_options:  Option<HashMap<String, PyObject>>,
        values:          Option<HashMap<String, String>>,
        arrow_overrides: Option<HashMap<String, String>>,
        batch_size:      Option<usize>,
        options:         Option<HashMap<String, PyObject>>,
        py:              Python<'_>,
    ) -> PyResult<PyComponentRef> {
        use potato_etl_runtime::Scd2ColumnNames;
        let mut cn = Scd2ColumnNames::default();
        if let Some(names) = col_names {
            for (k, v) in names {
                match k.as_str() {
                    "valid_from" => cn.valid_from = v,
                    "valid_to"   => cn.valid_to   = v,
                    "is_current" => cn.is_current = v,
                    "scd_id"     => cn.scd_id     = v,
                    other => return Err(PyValueError::new_err(format!(
                        "Unknown col_names key '{other}'. \
                         Valid keys: valid_from, valid_to, is_current, scd_id"
                    ))),
                }
            }
        }
        let ct = parse_create_table_mode(create_table)?;
        let co = parse_py_column_options(column_options, py)?;
        let driver_opts = parse_step_driver_options(options, py)?;
        let id = self.dag.next_id("scd2_sink");
        let comp_schema = build_component_schema(arrow_overrides, co, values);
        let sink_schema = potato_etl_runtime::config::SinkSchemaConfig {
            schema: comp_schema,
        };
        self.dag.add_scd2_sink(
            id.clone(), &input.id, conn, table, schema, key,
            track.unwrap_or_default(), cn, ct,
            sink_schema,
            batch_size,
            driver_opts,
            close_missing,
        );
        Ok(PyComponentRef { id })
    }

    // ── Object builder ────────────────────────────────────────────────────────

    /// Build nested JSON objects from each row using dot-notation field mapping.
    ///
    /// Appends a `_json` column to the existing columns.  The original columns
    /// remain available (useful for URL templates in `write_api`).
    ///
    /// **Empty `field_map`** → all columns as flat JSON keys:
    ///
    ///   built = pipeline.build_objects(source)
    ///   # Each row: {"employee_id": 42, "name": "Jan", "salary": 3500.0}
    ///
    /// **With `field_map`** → selective columns, nested structure via dot-notation:
    ///
    ///   built = pipeline.build_objects(source, field_map={
    ///       "employee_id": "KnEmployee.Element.Fields.EmId",
    ///       "first_name":  "KnEmployee.Element.Fields.FiNm",
    ///       "last_name":   "KnEmployee.Element.Fields.LaNm",
    ///       "salary":      "KnEmployee.Element.Fields.SaSa",
    ///   })
    ///   # Each row:
    ///   # { "KnEmployee": { "Element": { "Fields": {
    ///   #     "EmId": "42", "FiNm": "Jan", "LaNm": "Jansen", "SaSa": 3500.0
    ///   # }}}}
    ///
    /// **`output_col`** (default `"_json"`): name of the column containing the JSON object.
    ///
    /// Pass the output ref to `write_api(json_column="_json")`.
    #[pyo3(signature = (input, field_map=None, output_col="_json"))]
    fn build_objects(
        &mut self,
        input:      &PyComponentRef,
        field_map:  Option<HashMap<String, String>>,
        output_col: &str,
    ) -> PyComponentRef {
        let id = self.dag.next_id("build_objects");
        self.dag.add_object_builder(
            id.clone(),
            &input.id,
            field_map.unwrap_or_default(),
            output_col.to_string(),
        );
        PyComponentRef { id }
    }

    // ── REST API sink ─────────────────────────────────────────────────────────

    /// Send output to a REST API.
    ///
    /// **Minimal example** — flat JSON per row:
    ///
    ///   pipeline.write_api(source, "https://api.example.com/employees",
    ///                       method="POST", auth={"type": "bearer", "token": "..."})
    ///
    /// **Nested JSON via field_map**:
    ///
    ///   pipeline.write_api(
    ///       source,
    ///       "https://env.afas.online/profitrestservices/connectors/KnEmployee",
    ///       method    = "PUT",
    ///       headers   = {"Authorization": afas_authorization(TOKEN)},
    ///       field_map = {
    ///           "employee_id": "KnEmployee.Element.Fields.EmId",
    ///           "first_name":  "KnEmployee.Element.Fields.FiNm",
    ///       },
    ///       mode = "per_row",
    ///   )
    ///
    /// **Pre-built JSON** — use `build_objects()` first:
    ///
    ///   built = pipeline.build_objects(source, field_map={"employee_id": "data.id", ...})
    ///   pipeline.write_api(built, url, json_column="_json", mode="per_row")
    ///
    /// **Batch mode** — one request per batch (array of objects):
    ///
    ///   pipeline.write_api(source, "https://api.example.com/bulk",
    ///                       mode="batch", wrap_key="employees")
    ///   # Body: {"employees": [{...}, {...}, ...]}
    ///
    /// **URL template** — use column values in the URL:
    ///
    ///   pipeline.write_api(source, "https://api.example.com/employees/{employee_id}",
    ///                       method="PUT", mode="per_row")
    ///   # Each row: PUT .../employees/42
    ///
    /// **Auth types**:
    ///   - ``{"type": "bearer",  "token": "..."}``                  → Authorization: Bearer ...
    ///   - ``{"type": "basic",   "username": "...", "password": "..."}``
    ///   - ``{"type": "api_key", "header": "X-API-Key", "key": "..."}``
    ///   - Custom (e.g. AFAS): supply ``headers={"Authorization": "AfasToken ..."}``
    ///
    /// **`mode`**: ``"per_row"`` (default) | ``"batch"``
    #[pyo3(signature = (
        input,
        url,
        *,
        method         = "POST",
        headers        = None,
        auth           = None,
        mode           = "per_row",
        field_map      = None,
        json_column    = None,
        wrap_key       = None,
        rate_limit_rps = None,
        timeout_secs   = None,
        allow_non_2xx  = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn write_api(
        &mut self,
        input:          &PyComponentRef,
        url:            &str,
        method:         &str,
        headers:        Option<HashMap<String, String>>,
        auth:           Option<HashMap<String, String>>,
        mode:           &str,
        field_map:      Option<HashMap<String, String>>,
        json_column:    Option<String>,
        wrap_key:       Option<String>,
        rate_limit_rps: Option<f64>,
        timeout_secs:   Option<u64>,
        allow_non_2xx:  bool,
    ) -> PyResult<PyComponentRef> {
        let http_method = match method.to_uppercase().as_str() {
            "PUT"    => HttpMethod::Put,
            "PATCH"  => HttpMethod::Patch,
            "DELETE" => HttpMethod::Delete,
            "GET"    => HttpMethod::Get,
            _        => HttpMethod::Post,
        };

        let auth_cfg = parse_auth_dict(auth)?;

        let write_mode = match mode.to_lowercase().as_str() {
            "batch" => SinkWriteMode::Batch,
            _       => SinkWriteMode::PerRow,
        };

        let opts = RestApiSinkOptions {
            method:         http_method,
            headers:        headers.unwrap_or_default(),
            auth:           auth_cfg,
            mode:           write_mode,
            field_map:      field_map.unwrap_or_default(),
            json_column,
            wrap_key,
            url_template:   None,
            rate_limit_rps,
            timeout_secs,
            allow_non_2xx,
        };

        let id = self.dag.next_id("write_api");
        self.dag.add_rest_api_sink(id.clone(), &input.id, url, opts);
        Ok(PyComponentRef { id })
    }

    // ── Pipeline execution ────────────────────────────────────────────────────

    /// Execute the pipeline and return statistics.
    ///
    /// Returns a dict::
    ///
    ///   {
    ///     "rows_read":       int,
    ///     "rows_written":    int,
    ///     "batches":         int,
    ///     "duration_secs":   float,   # total wall-clock time
    ///     "rows_per_second": float,   # rows_read / duration (or rows_written if no source)
    ///     "components": {
    ///       "read_db_1": { "rows_in": 0,   "rows_out": 500, "batches": 5, "duration_ms": 823 },
    ///       "filter_2":  { "rows_in": 500, "rows_out": 320, "batches": 5, "duration_ms":  42 },
    ///       ...
    ///     }
    ///   }
    fn run(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        // Swap out the Dag (run() takes ownership).
        let dag = std::mem::replace(&mut self.dag, Dag::new(ETLConfig::default()));

        let report: RunReport = allow_threads(py, || {
            rt().block_on(dag.run())
        }).map_err(|e: anyhow::Error| PyRuntimeError::new_err(e.to_string()))?;

        let d = pyo3::types::PyDict::new(py);
        d.set_item("rows_read",       report.rows_read)?;
        d.set_item("rows_written",    report.rows_written)?;
        d.set_item("iterations",      report.iterations)?;
        d.set_item("duration_secs",   report.duration.as_secs_f64())?;
        d.set_item("rows_per_second", report.rows_per_second)?;

        let comps = pyo3::types::PyDict::new(py);
        for (comp_id, stats) in &report.per_component {
            let s = pyo3::types::PyDict::new(py);
            s.set_item("rows_in",     stats.rows_in)?;
            s.set_item("rows_out",    stats.rows_out)?;
            s.set_item("batches",     stats.batches)?;
            s.set_item("duration_ms", stats.duration_ms)?;
            comps.set_item(comp_id, s)?;
        }
        d.set_item("components", comps)?;

        // Environment variables (if any were defined).
        if !report.environment.is_empty() {
            let env = pyo3::types::PyDict::new(py);
            for (name, val) in &report.environment {
                env.set_item(name, val)?;
            }
            d.set_item("environment", env)?;
        }

        Ok(d.into())
    }

    // ── Debug ─────────────────────────────────────────────────────────────────

    fn __repr__(&self) -> String {
        format!("ETL(components={}, batch_size={})",
            self.dag.len(), self.dag.batch_size())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Low-level helpers (backwards compat + direct batch API)
// ─────────────────────────────────────────────────────────────────────────────

/// Low-level database reader.
/// Use `ETL.read_db` for pipeline use; this is for direct batch iteration.
#[pyclass(name = "ReadDB", unsendable)]
struct PyReadDB { inner: Option<ReadDB> }

#[pymethods]
impl PyReadDB {
    #[new]
    fn new(conn_str: &str) -> PyResult<Self> {
        ReadDB::new(conn_str).map(|r| Self { inner: Some(r) })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn table<'a>(mut slf: PyRefMut<'a, Self>, t: &str) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|r| r.table(t)); slf }
    fn schema<'a>(mut slf: PyRefMut<'a, Self>, s: &str) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|r| r.schema(s)); slf }
    fn query<'a>(mut slf: PyRefMut<'a, Self>, q: &str) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|r| r.query(q)); slf }
    fn cursor<'a>(mut slf: PyRefMut<'a, Self>, col: &str) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|r| r.cursor(col)); slf }
    fn batch_size<'a>(mut slf: PyRefMut<'a, Self>, n: usize) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|r| r.batch_size(n)); slf }

    fn exec(&mut self) -> PyResult<BatchIter> {
        let source = self.inner.take()
            .ok_or_else(|| PyRuntimeError::new_err("ReadDB already consumed"))?;
        let (tx, rx) = std::sync::mpsc::channel::<anyhow::Result<RecordBatch>>();
        rt().spawn(async move {
            let mut stream = source.exec();
            while let Some(r) = stream.next().await { if tx.send(r).is_err() { break; } }
        });
        Ok(BatchIter { rx })
    }
}

/// Low-level database writer.
#[pyclass(name = "WriteDB", unsendable)]
struct PyWriteDB { inner: Option<WriteDB> }

#[pymethods]
impl PyWriteDB {
    #[new]
    fn new(conn_str: &str) -> PyResult<Self> {
        WriteDB::new(conn_str).map(|w| Self { inner: Some(w) })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn table<'a>(mut slf: PyRefMut<'a, Self>, t: &str) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|w| w.table(t)); slf }
    fn schema<'a>(mut slf: PyRefMut<'a, Self>, s: &str) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|w| w.schema(s)); slf }

    // Table DDL mode
    fn use_existing<'a>(mut slf: PyRefMut<'a, Self>) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|w| w.use_existing()); slf }
    fn create_if_not_exists<'a>(mut slf: PyRefMut<'a, Self>) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|w| w.create_if_not_exists()); slf }
    fn drop_and_replace<'a>(mut slf: PyRefMut<'a, Self>) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|w| w.drop_and_replace()); slf }

    // Write strategy
    fn insert<'a>(mut slf: PyRefMut<'a, Self>) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|w| w.insert()); slf }
    fn upsert<'a>(mut slf: PyRefMut<'a, Self>) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|w| w.upsert()); slf }
    fn clear_and_insert<'a>(mut slf: PyRefMut<'a, Self>) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|w| w.clear_and_insert()); slf }

    fn write(&mut self, py: Python<'_>, batch: PyArrowType<RecordBatch>) -> PyResult<usize> {
        let rb  = batch.0;
        let mut s = self.inner.take().ok_or_else(|| PyRuntimeError::new_err("WriteDB already closed"))?;
        let (r, back) = allow_threads(py, move || { let res = rt().block_on(s.write(rb)); (res, s) });
        self.inner = Some(back);
        r.map_err(|e: anyhow::Error| PyRuntimeError::new_err(e.to_string()))
    }

    fn flush(&mut self, py: Python<'_>) -> PyResult<()> {
        let mut s = match self.inner.take() { Some(s) => s, None => return Ok(()) };
        let (r, back) = allow_threads(py, move || { let res = rt().block_on(s.flush()); (res, s) });
        self.inner = Some(back);
        r.map_err(|e: anyhow::Error| PyRuntimeError::new_err(e.to_string()))
    }
}

/// Low-level SCD2 sink.
#[pyclass(name = "Scd2Sink", unsendable)]
struct PyScd2Sink { inner: Option<Scd2Sink> }

#[pymethods]
impl PyScd2Sink {
    #[new]
    fn new(conn_str: &str) -> PyResult<Self> {
        Scd2Sink::new(conn_str).map(|s| Self { inner: Some(s) })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn table<'a>(mut slf: PyRefMut<'a, Self>, t: &str) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|s| s.table(t)); slf }
    fn schema<'a>(mut slf: PyRefMut<'a, Self>, schema_name: &str) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|s| s.schema(schema_name)); slf }
    fn key<'a>(mut slf: PyRefMut<'a, Self>, col: &str) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|s| s.key(col)); slf }
    fn track<'a>(mut slf: PyRefMut<'a, Self>, cols: Vec<String>) -> PyRefMut<'a, Self> { slf.inner = slf.inner.take().map(|s| s.track(cols)); slf }

    /// Override the names of the four SCD2 system columns.
    ///
    /// Pass a dict with any subset of the keys ``valid_from``, ``valid_to``,
    /// ``is_current``, ``scd_id``.  Omitted keys keep their default names.
    ///
    /// Example::
    ///
    ///     sink.col_names({"valid_from": "eff_from", "valid_to": "eff_to"})
    fn col_names<'a>(mut slf: PyRefMut<'a, Self>, names: std::collections::HashMap<String, String>) -> PyResult<PyRefMut<'a, Self>> {
        use potato_etl_runtime::Scd2ColumnNames;
        let mut cn = Scd2ColumnNames::default();
        for (k, v) in names {
            match k.as_str() {
                "valid_from" => cn.valid_from = v,
                "valid_to"   => cn.valid_to   = v,
                "is_current" => cn.is_current = v,
                "scd_id"     => cn.scd_id     = v,
                other => return Err(PyValueError::new_err(format!(
                    "Unknown scd2_columns key '{other}'. \
                     Valid keys: valid_from, valid_to, is_current, scd_id"
                ))),
            }
        }
        slf.inner = slf.inner.take().map(|s| s.col_names(cn));
        Ok(slf)
    }

    fn write(&mut self, py: Python<'_>, batch: PyArrowType<RecordBatch>) -> PyResult<Py<PyAny>> {
        let rb  = batch.0;
        let mut s = self.inner.take().ok_or_else(|| PyRuntimeError::new_err("Scd2Sink already closed"))?;
        let (r, back) = allow_threads(py, move || { let res = rt().block_on(s.write(rb)); (res, s) });
        self.inner = Some(back);
        let stats = r.map_err(|e: anyhow::Error| PyRuntimeError::new_err(e.to_string()))?;
        let d = pyo3::types::PyDict::new(py);
        d.set_item("new_rows",       stats.new_rows)?;
        d.set_item("updated_rows",   stats.updated_rows)?;
        d.set_item("unchanged_rows", stats.unchanged_rows)?;
        Ok(d.into())
    }

    fn flush(&mut self, py: Python<'_>) -> PyResult<()> {
        let mut s = match self.inner.take() { Some(s) => s, None => return Ok(()) };
        let (r, back) = allow_threads(py, move || { let res = rt().block_on(s.flush()); (res, s) });
        self.inner = Some(back);
        r.map_err(|e: anyhow::Error| PyRuntimeError::new_err(e.to_string()))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Parse a `create_table` string into a [`CreateTableMode`].
fn parse_create_table_mode(s: &str) -> PyResult<potato_etl_runtime::CreateTableMode> {
    use potato_etl_runtime::CreateTableMode;
    match s {
        "never" | ""    => Ok(CreateTableMode::Never),
        "if_not_exists" => Ok(CreateTableMode::IfNotExists),
        "replace"       => Ok(CreateTableMode::Replace),
        other => Err(PyValueError::new_err(format!(
            "Unknown create_table value '{other}'. Use: never | if_not_exists | replace"
        ))),
    }
}

/// Parse a Python `column_options` dict (col → attribute-dict) into a
/// `ColumnOptionsMap`.
///
/// Accepted keys per column: `primary_key`, `unique`, `index`, `nullable`,
/// `db_type`, `check_expr`, `default_expr`, `on_update_expr`, `description`,
/// `foreign_key`.
fn parse_py_column_options(
    raw: Option<HashMap<String, PyObject>>,
    py:  Python<'_>,
) -> PyResult<potato_etl_runtime::ColumnOptionsMap> {
    use pyo3::types::PyDict;
    use potato_etl_runtime::ColumnOption;

    let Some(raw) = raw else { return Ok(potato_etl_runtime::ColumnOptionsMap::new()); };
    let mut out = potato_etl_runtime::ColumnOptionsMap::with_capacity(raw.len());

    for (col, obj) in raw {
        let d = obj.bind(py).cast::<PyDict>().map_err(|_| PyValueError::new_err(
            format!("column_options['{col}']: expected a dict of column attributes")
        ))?;

        let mut co = ColumnOption::default();

        if let Some(v) = d.get_item("primary_key")?  { co.primary_key  = v.extract::<bool>()?; }
        if let Some(v) = d.get_item("unique")?       { co.unique       = v.extract::<bool>()?; }
        if let Some(v) = d.get_item("index")?        { co.index        = v.extract::<bool>()?; }
        if let Some(v) = d.get_item("nullable")?     { co.nullable     = Some(v.extract::<bool>()?); }
        if let Some(v) = d.get_item("db_type")?      { co.db_type      = Some(v.extract::<String>()?); }
        if let Some(v) = d.get_item("check_expr")?   { co.check_expr   = Some(v.extract::<String>()?); }
        if let Some(v) = d.get_item("default_expr")?   { co.default_expr   = Some(v.extract::<String>()?); }
        if let Some(v) = d.get_item("on_update_expr")? { co.on_update_expr = Some(v.extract::<String>()?); }
        if let Some(v) = d.get_item("description")?    { co.description    = Some(v.extract::<String>()?); }

        if let Some(v) = d.get_item("foreign_key")? {
            let fk = v.cast::<PyDict>().map_err(|_| PyValueError::new_err(
                format!("column_options['{col}']['foreign_key']: expected dict with 'table' and 'column'")
            ))?;
            co.foreign_key = Some(potato_etl_runtime::ForeignKey {
                table:  fk.get_item("table")?.ok_or_else(|| PyValueError::new_err(
                    format!("column_options['{col}']['foreign_key']: missing 'table'")
                ))?.extract::<String>()?,
                column: fk.get_item("column")?.ok_or_else(|| PyValueError::new_err(
                    format!("column_options['{col}']['foreign_key']: missing 'column'")
                ))?.extract::<String>()?,
                schema: fk.get_item("schema")?.map(|v| v.extract::<String>()).transpose()?,
            });
        }

        out.insert(col, co);
    }
    Ok(out)
}

/// Build a [`ComponentSchema`] from optional arrow_overrides, column_options,
/// and value injection maps.
///
/// Used by `write_db` and `scd2_sink` to convert legacy Python API parameters
/// into the unified `schema:` config.
///
/// `values` entries (`{ target_col: "$source" | "null" }`) are converted into
/// `ArrowColumnDef` entries with `value:` set, merged into the arrow schema
/// alongside any `arrow_overrides`.
fn build_component_schema(
    arrow_overrides: Option<HashMap<String, String>>,
    column_options:  potato_etl_runtime::ColumnOptionsMap,
    values:          Option<HashMap<String, String>>,
) -> ComponentSchema {
    // Merge arrow_overrides and values into a single ArrowColumnDef map.
    let mut arrow_columns: HashMap<String, ArrowColumnDef> = HashMap::new();

    // 1. Arrow overrides (type casts only, no value injection).
    if let Some(ao) = arrow_overrides {
        for (k, v) in ao {
            arrow_columns.insert(k, ArrowColumnDef {
                arrow_type: Some(v), nullable: None, logical_type: None, value: None,
            });
        }
    }

    // 2. Value injections — merge into existing entries or create new ones.
    if let Some(vals) = values {
        for (target, src) in vals {
            let value_str = {
                let lower = src.trim().to_lowercase();
                if lower == "null" || lower == "none" {
                    "null".to_string()
                } else {
                    src
                }
            };
            if let Some(existing) = arrow_columns.get_mut(&target) {
                // Arrow override already exists for this column — add value injection.
                existing.value = Some(value_str);
            } else {
                arrow_columns.insert(target, ArrowColumnDef {
                    arrow_type: None, nullable: None, logical_type: None,
                    value: Some(value_str),
                });
            }
        }
    }

    let arrow = if arrow_columns.is_empty() {
        None
    } else {
        Some(ArrowSchemaConfig { columns: arrow_columns })
    };

    let database = if column_options.is_empty() {
        None
    } else {
        let db_cols: indexmap::IndexMap<String, DatabaseColumnDef> = column_options.into_iter()
            .map(|(name, co)| (name, DatabaseColumnDef {
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
        Some(DatabaseSchemaConfig {
            columns: db_cols,
            ..Default::default()
        })
    };
    ComponentSchema { database, arrow }
}

/// Parse a Python `options` dict into a [`StepDriverOptions`].
///
/// Accepted keys:
///   - `mode`             : str     — driver mode (MSSQL: tiberius|bcp|odbc, Databricks: api|odbc)
///   - `bcp_path`         : str     — explicit path to bcp binary (MSSQL only)
///   - `bcp_staging`      : bool    — override bcp staging behaviour (MSSQL only)
///   - `prefetch_rows`    : int     — rows per OCI round-trip (Oracle only)
///   - `fetch_array_size` : int     — internal OCI array size (Oracle only)
///   - `direct_path`      : bool    — use APPEND_VALUES hint (Oracle write only)
///   - `parallel`         : int     — Oracle parallel DML degree
///   - `oci_batch_size`   : int     — max rows per OCI batch (Oracle only)
///   - `identifier_case`  : str     — "as_is" | "upper" | "lower"
fn parse_step_driver_options(
    raw: Option<HashMap<String, PyObject>>,
    py:  Python<'_>,
) -> PyResult<StepDriverOptions> {
    let Some(raw) = raw else { return Ok(StepDriverOptions::default()); };
    let mut opts = StepDriverOptions::default();

    for (key, val) in &raw {
        match key.as_str() {
            "mode"             => opts.mode             = Some(val.extract::<String>(py)?),
            "bcp_path"         => opts.bcp_path         = Some(val.extract::<String>(py)?),
            "bcp_staging"      => opts.bcp_staging       = Some(val.extract::<bool>(py)?),
            "prefetch_rows"    => opts.prefetch_rows     = Some(val.extract::<u32>(py)?),
            "fetch_array_size" => opts.fetch_array_size  = Some(val.extract::<u32>(py)?),
            "direct_path"      => opts.direct_path       = Some(val.extract::<bool>(py)?),
            "parallel"         => opts.parallel          = Some(val.extract::<u32>(py)?),
            "oci_batch_size"   => opts.oci_batch_size    = Some(val.extract::<usize>(py)?),
            "identifier_case"  => {
                let s = val.extract::<String>(py)?;
                opts.identifier_case = Some(match s.to_lowercase().as_str() {
                    "upper"            => IdentifierCase::Upper,
                    "lower"            => IdentifierCase::Lower,
                    "as_is" | "asis"   => IdentifierCase::AsIs,
                    other => return Err(PyValueError::new_err(format!(
                        "Unknown identifier_case '{other}'. Valid: as_is, upper, lower"
                    ))),
                });
            }
            other => return Err(PyValueError::new_err(format!(
                "Unknown driver option '{other}'. Valid keys: mode, bcp_path, \
                 bcp_staging, prefetch_rows, fetch_array_size, \
                 direct_path, parallel, oci_batch_size, identifier_case"
            ))),
        }
    }
    Ok(opts)
}

/// Parse a Python auth dict into an `AuthConfig`.
fn parse_auth_dict(auth: Option<HashMap<String, String>>) -> PyResult<Option<AuthConfig>> {
    match auth {
        None => Ok(None),
        Some(ref a) => {
            let t = a.get("type").map(|s| s.as_str()).unwrap_or("");
            Ok(Some(match t {
                "bearer" => AuthConfig::Bearer {
                    token: a.get("token")
                        .ok_or_else(|| PyValueError::new_err("auth type='bearer' requires 'token'"))?.clone(),
                },
                "basic" => AuthConfig::Basic {
                    username: a.get("username")
                        .ok_or_else(|| PyValueError::new_err("auth type='basic' requires 'username'"))?.clone(),
                    password: a.get("password")
                        .ok_or_else(|| PyValueError::new_err("auth type='basic' requires 'password'"))?.clone(),
                },
                "api_key" => AuthConfig::ApiKey {
                    header: a.get("header")
                        .ok_or_else(|| PyValueError::new_err("auth type='api_key' requires 'header'"))?.clone(),
                    key: a.get("key")
                        .ok_or_else(|| PyValueError::new_err("auth type='api_key' requires 'key'"))?.clone(),
                },
                _ => return Err(PyValueError::new_err(format!(
                    "Unknown auth type '{t}'. Use: bearer, basic, api_key. \
                     For custom schemes (AFAS, NTLM, …): supply the header value \
                     directly via headers={{\"Authorization\": \"...\"}}"
                ))),
            }))
        }
    }
}

/// Wrap a Python callable as a `TransformFn` that can be stored in the Dag.
///
/// The GIL is re-acquired inside the closure when calling the Python function.
/// Arrow IPC is used for zero-copy round-trip via the C Data Interface.
fn make_py_transform_fn(func: PyObject) -> TransformFn {
    Arc::new(move |batch: RecordBatch| -> anyhow::Result<RecordBatch> {
        with_gil(|py| {
            let py_batch = batch.to_pyarrow(py)
                .map_err(|e| anyhow::anyhow!("Arrow → Python conversion failed: {e}"))?;
            let result = func.call1(py, (&py_batch,))
                .map_err(|e| anyhow::anyhow!("Python transform raised an exception: {e}"))?;
            let out = result.extract::<PyArrowType<RecordBatch>>(py)
                .map_err(|e| anyhow::anyhow!("Python transform did not return a RecordBatch: {e}"))?;
            Ok(out.0)
        })
    })
}

/// Create a `TransformFn` that `exec()`s an inline Python code block per batch.
///
/// The code block runs with two names in scope:
/// - `table` — `pyarrow.Table` wrapping the current `RecordBatch`.
/// - `pa`    — the `pyarrow` module (no import needed inside the code).
///
/// The code must assign the transformed output to `result`.  Both
/// `pyarrow.Table` and `pyarrow.RecordBatch` are accepted; a Table is
/// automatically flattened to a single RecordBatch via `concat_batches`.
fn make_py_exec_transform_fn(code: String) -> TransformFn {
    Arc::new(move |batch: RecordBatch| -> anyhow::Result<RecordBatch> {
        with_gil(|py| {
            // Import pyarrow once per batch invocation.
            let pa = py.import("pyarrow")
                .map_err(|e| anyhow::anyhow!("Failed to import pyarrow: {e}"))?;

            // Convert RecordBatch → pyarrow.Table so the user gets the
            // familiar Table API (column accessors, filter, etc.).
            let py_rb = batch.to_pyarrow(py)
                .map_err(|e| anyhow::anyhow!("Arrow → pyarrow conversion failed: {e}"))?;
            let rb_list = pyo3::types::PyList::new(py, [&py_rb])
                .map_err(|e| anyhow::anyhow!("Failed to create PyList: {e}"))?;
            let table = pa
                .getattr("Table")
                .and_then(|t| t.call_method1("from_batches", (&rb_list,)))
                .map_err(|e| anyhow::anyhow!("pyarrow.Table.from_batches failed: {e}"))?;

            // Build the execution namespace.
            let globals = pyo3::types::PyDict::new(py);
            globals
                .set_item("table", &table)
                .map_err(|e| anyhow::anyhow!("Failed to set 'table': {e}"))?;
            globals
                .set_item("pa", &pa)
                .map_err(|e| anyhow::anyhow!("Failed to set 'pa': {e}"))?;

            // Execute the user's code block.
            let c_code = std::ffi::CString::new(code.as_str())
                .map_err(|e| anyhow::anyhow!("Python code contains interior NUL byte: {e}"))?;
            py.run(&c_code, Some(&globals), None)
                .map_err(|e| anyhow::anyhow!("Inline Python code raised an exception:\n{e}"))?;

            // Read `result` — must be assigned by the user's code.
            let result_obj = globals
                .get_item("result")
                .map_err(|e| anyhow::anyhow!("Error reading 'result' from namespace: {e}"))?
                .ok_or_else(|| anyhow::anyhow!(
                    "Inline Python transform must assign its output to 'result'.\n\
                     Example: result = table.filter(pc.equal(table.column('status'), 'active'))"
                ))?;

            // Accept pyarrow.RecordBatch directly (no conversion needed).
            if let Ok(rb) = result_obj.extract::<PyArrowType<RecordBatch>>() {
                return Ok(rb.0);
            }

            // Accept pyarrow.Table — convert to RecordBatch via to_batches() + concat.
            let batches_obj = result_obj
                .call_method0("to_batches")
                .map_err(|_| anyhow::anyhow!(
                    "'result' must be a pyarrow.RecordBatch or pyarrow.Table"
                ))?;
            let batch_list: Vec<PyArrowType<RecordBatch>> = batches_obj
                .extract()
                .map_err(|e| anyhow::anyhow!("Failed to extract batches from result Table: {e}"))?;

            if batch_list.is_empty() {
                // Preserve the input schema for empty results.
                return Ok(RecordBatch::new_empty(batch.schema()));
            }

            let rb_vec: Vec<RecordBatch> = batch_list.into_iter().map(|b| b.0).collect();
            arrow::compute::concat_batches(&rb_vec[0].schema(), &rb_vec)
                .map_err(|e| anyhow::anyhow!("Failed to concatenate result Table batches: {e}"))
        })
    })
}

/// Auto-register inline Python code blocks stored in a `Dag` by the JSON/YAML loader.
///
/// Called by `from_json` and `from_yaml` after parsing the pipeline definition.
/// Each `__inline_*` entry in `dag.inline_python_codes()` is registered as a
/// `make_py_exec_transform_fn`-backed `TransformFn`.
fn register_inline_python_codes(dag: &mut Dag) -> PyResult<()> {
    // Collect first to avoid borrowing `dag` immutably while mutating it.
    let codes: Vec<(String, String)> = dag
        .inline_python_codes()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    for (name, code) in codes {
        let transform_fn = make_py_exec_transform_fn(code);
        dag.register_transform(name, transform_fn);
    }
    Ok(())
}

// ── Module registration ───────────────────────────────────────────────────────

#[pymodule]
fn etl(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyETL>()?;
    m.add_class::<PyComponentRef>()?;
    m.add_class::<PyReadDB>()?;
    m.add_class::<PyWriteDB>()?;
    m.add_class::<PyScd2Sink>()?;
    m.add_class::<BatchIter>()?;
    Ok(())
}