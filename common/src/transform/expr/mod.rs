//! Expression DSL — parser and Arrow evaluator for `map`, `aggregate`, and `filter` steps.
//!
//! ## Syntax
//!
//! | Form | Example |
//! |---|---|
//! | Column reference | `price`, `order_date` |
//! | Integer literal  | `42`, `-7` |
//! | Float literal    | `3.14`, `-0.5` |
//! | String literal   | `"active"`, `'hello'` |
//! | Bool literal     | `true`, `false` |
//! | Arithmetic       | `price * quantity`, `total - discount` |
//! | Comparison       | `status == "active"`, `age >= 18` |
//! | Logical          | `a and b`, `not flag` |
//! | Function call    | `now()`, `year(order_date)`, `upper(name)`, `json_get(payload, "id")` |
//! | Null literal     | `null` |
//! | Environment variable | `$name` — reference to a pipeline environment variable. |
//!
//! ## Built-in functions
//!
//! | Function | Return type | Notes |
//! |---|---|---|
//! | `now()` | `Timestamp[us]` | current **local** wall-clock time, naive (no tz). Same value for all rows in batch. Mirrors Python's `datetime.now()`. |
//! | `utcnow()` | `Timestamp[us, UTC]` | current UTC time, tz-aware. |
//! | `run_ts()` | `Timestamp[us, UTC]` | pipeline start time — same across all batches |
//! | `run_ts_naive()` | `Timestamp[us]` | pipeline start time without timezone — same across all batches |
//! | `year(col)` | `Int32` | works on Date32, Timestamp, or `"YYYY-MM-DD"` strings |
//! | `month(col)` | `Int32` | |
//! | `day(col)` | `Int32` | |
//! | `hour(col)` | `Int32` | |
//! | `minute(col)` | `Int32` | |
//! | `second(col)` | `Int32` | |
//! | `upper(col)` | `Utf8` | |
//! | `lower(col)` | `Utf8` | |
//! | `trim(col)` | `Utf8` | strips ASCII whitespace |
//! | `length(col)` | `Int32` | UTF-8 character count |
//! | `truncate(col, n)` / `left(col, n)` | `Utf8` | first `n` characters (UTF-8 chars, not bytes); shorter strings pass through; `n` must be a non-negative integer literal |
//! | `json_get(col, "key")` | `Utf8` | extract JSON field; supports `"a.b.c"` dot paths |
//! | `json_get(col, "arr[0]")` | `Utf8` | extract JSON array element |
//! | `json_length(col)` | `Int32` | length of JSON array |
//! | `coalesce(col1, col2)` | same | first non-null value; preserves type when all args match, falls back to Utf8 for mixed types |
//! | `cast(col, "type_str")` | varies | Arrow type string e.g. `"int32"`, `"float64"` |
//! | `sum(col)` | `Float64` | for use inside `aggregate` metrics only |
//! | `count()` | `Int64` | row count — aggregate context only |
//! | `min(col)` | same as col | aggregate context only |
//! | `max(col)` | same as col | aggregate context only |
//! | `avg(col)` | `Float64` | aggregate context only |

mod tokens;
mod ast;
mod eval;
mod helpers;

// ── Public API ────────────────────────────────────────────────────────────────

pub use tokens::{Tok, tokenize};
pub use ast::{Expr, BinOp, UnaryOp, parse};
pub use eval::{eval, eval_bool_mask, apply_condition};
pub use helpers::{to_string_array, json_path_get, broadcast_array};

use std::collections::HashMap;
use arrow::array::ArrayRef;

// ── EvalContext ───────────────────────────────────────────────────────────────

/// Evaluation context threaded through all expression evaluation.
///
/// Carries pipeline-level values that are constant across batches — the
/// canonical example being the run start timestamp used by `run_ts()`.
#[derive(Debug, Clone, Copy)]
pub struct EvalContext {
    /// Pipeline run start time as microseconds since Unix epoch (UTC).
    /// Used by the `run_ts()` expression function.
    /// When 0, `run_ts()` falls back to `Utc::now()` (pre-run evaluation).
    pub run_start_us: i64,
}

impl Default for EvalContext {
    fn default() -> Self {
        Self { run_start_us: 0 }
    }
}

// ── Thread-local eval context ─────────────────────────────────────────────────

std::thread_local! {
    static EVAL_CTX: std::cell::Cell<EvalContext> = const { std::cell::Cell::new(EvalContext { run_start_us: 0 }) };
    /// Pipeline environment variables: pre-evaluated 1-element Arrow arrays.
    /// Set once at pipeline start via `set_env_vars`, broadcast to batch size in `eval`.
    static ENV_VARS: std::cell::RefCell<HashMap<String, ArrayRef>> = std::cell::RefCell::new(HashMap::new());
}

/// Set the evaluation context for the current thread.
///
/// Called by the DAG executor before invoking `apply_map` / `apply_filter_expr`
/// so that functions like `run_ts()` can read pipeline-level constants without
/// threading a context parameter through every `eval` call.
pub fn set_eval_context(ctx: EvalContext) {
    EVAL_CTX.set(ctx);
}

/// Read the current thread's evaluation context.
pub(crate) fn current_eval_context() -> EvalContext {
    EVAL_CTX.get()
}

/// Set the pipeline environment variables for the current thread.
///
/// Each value is a 1-element `ArrayRef` produced by evaluating the expression
/// from the `environment:` block once at pipeline start.  The `env("name")` /
/// `$name` expression function broadcasts this value to match the batch size.
pub fn set_env_vars(vars: HashMap<String, ArrayRef>) {
    ENV_VARS.with(|cell| *cell.borrow_mut() = vars);
}

/// Clear the pipeline environment variables for the current thread.
pub fn clear_env_vars() {
    ENV_VARS.with(|cell| cell.borrow_mut().clear());
}

/// Look up a pipeline environment variable by name.
///
/// Returns `Some(ArrayRef)` if the variable exists (a 1-element array), or `None` if undefined.
/// The returned array must be broadcast to batch size before use.
pub fn lookup_env_var(name: &str) -> Option<ArrayRef> {
    ENV_VARS.with(|cell| cell.borrow().get(name).cloned())
}
