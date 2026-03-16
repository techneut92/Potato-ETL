//! Databricks ODBC transport (Simba driver).
//!
//! Requires the `odbc` Cargo feature and an installed Simba Spark /
//! Databricks ODBC driver on the host system.
//!
//! ## Prerequisites
//!
//! - **unixODBC** dev headers: `sudo apt install unixodbc-dev` (Debian/Ubuntu)
//! - **Databricks Simba ODBC Driver**: download from
//!   <https://www.databricks.com/spark/odbc-drivers-download>
//!
//! ## Module layout
//!
//! - `source.rs` — zero-copy Arrow reads via `arrow-odbc` server-side cursors
//! - `sink.rs`   — bulk INSERT via ODBC columnar parameter binding; DDL and
//!                 MERGE operations delegate to the REST API [`StatementClient`]
//! - `conn_str.rs` — shared ODBC connection string builder

pub mod source;
pub mod sink;
mod conn_str;

use odbc_api::Environment;
use std::sync::OnceLock;

// ── Process-wide ODBC Environment singleton ───────────────────────────────────
//
// The ODBC spec requires a single `Environment` handle per process.  Creating
// one per query works but is wasteful (driver manager re-init, handle churn).
// `OnceLock` gives us a lazy, thread-safe singleton with zero overhead after
// the first call.
static ODBC_ENV: OnceLock<Environment> = OnceLock::new();

pub(crate) fn odbc_env() -> &'static Environment {
    ODBC_ENV.get_or_init(|| {
        Environment::new().expect("ODBC Environment::new() must succeed")
    })
}
