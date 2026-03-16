//! Built-in ETL transform components.
//!
//! # Trait
//! [`EtlTransform`] — the common contract for all stateless, synchronous transforms.
//!
//! # Modules
//! | Module          | Contents                                                        |
//! |-----------------|---------------------------------------------------------------------|
//! | [`expr`]        | Expression DSL parser + Arrow evaluator used by `map`/`filter`  |
//! | [`map`]         | `apply_map` — add/compute/rename columns via expressions         |
//! | [`aggregate`]   | `apply_aggregate` — group-by + metrics aggregation               |
//! | [`filter`]      | `apply_filter` + `apply_filter_expr` + [`FilterTransform`]      |
//! | [`rename`]      | `apply_rename` + [`RenameTransform`]                            |
//! | [`schema`]      | `apply_rename_all` + [`RenameAllTransform`]                     |
//! | [`join`]        | `hash_join` — in-memory hash join over `RecordBatch`es           |
//! | [`objects`]     | `build_objects` + `resolve_url_template`                        |
//! | [`flatten`]     | `apply_flatten`                                                  |
//! | [`unnest`]      | `apply_unnest` — explode array columns into rows                 |

pub mod expr;
pub mod map;
pub mod aggregate;
pub mod filter;
pub mod rename;
pub mod schema;
pub mod join;
pub mod objects;
pub mod flatten;
pub mod unnest;

// NOTE: pipeline_tests.rs lives in runtime/tests/pipeline_tests.rs as an
// integration test because it depends on `Dag` which is a runtime-only type.

use arrow::record_batch::RecordBatch;

// ── EtlTransform ──────────────────────────────────────────────────────────────

/// Contract for transforms: receives a batch, returns a (potentially different) batch.
///
/// Implementors must be `Send + Sync`.
pub trait EtlTransform: Send + Sync {
    fn transform(&self, batch: RecordBatch) -> anyhow::Result<RecordBatch>;
}

// ── Re-exports ────────────────────────────────────────────────────────────────

pub use filter::FilterTransform;
pub use rename::RenameTransform;
pub use flatten::apply_flatten;
pub use unnest::{apply_unnest, UnnestConfig};
pub use map::apply_map;
pub use aggregate::apply_aggregate;
pub use schema::RenameAllTransform;