//! Databricks SQL warehouse driver for potato-etl.
//!
//! Three transport modes controlled by `mode=` in the connection URL:
//!
//! - **`api`** (default) — REST SQL Statement Execution API.  Pure Rust, no
//!   system dependencies.
//! - **`odbc`** — Simba ODBC driver.  Requires the `odbc` feature and an
//!   installed driver.
//! - **`thrift`** — Hive / Spark Thrift binary protocol over HTTPS.
//!   Requires the `thrift` feature.
//!
//! ## Module layout
//!
//! ```text
//! conn.rs           — transport-agnostic identity, auth, shared helpers
//! type_registry.rs  — Databricks ↔ Arrow type coercion (shared)
//! api/              — REST SQL Statement API transport
//!   mod.rs          — StatementClient, response types, introspection
//!   source.rs       — DatabricksApiSource  (SourceBuilder)
//!   sink.rs         — DatabricksApiSink    (SinkBuilder)
//!   scd2.rs         — DatabricksApiScd2    (Scd2Builder)
//! odbc/             — ODBC transport (feature-gated)
//!   mod.rs          — ODBC env singleton, sub-module declarations
//!   conn_str.rs     — shared ODBC connection string builder
//!   source.rs       — DatabricksOdbcSource (arrow-odbc reads)
//!   sink.rs         — DatabricksOdbcSink   (ODBC bulk INSERT + REST API MERGE)
//! thrift/           — Thrift transport (feature-gated)
//!   mod.rs          — sub-module declarations
//!   protocol.rs     — Thrift Binary encoding reader / writer
//!   rpc.rs          — Databricks TCLIService RPC builders + parsers
//!   arrow_convert.rs — Thrift column data → Arrow RecordBatch
//!   client.rs       — Session-managed ThriftClient
//!   source.rs       — DatabricksThriftSource (SourceBuilder)
//!   sink.rs         — DatabricksThriftSink   (SinkBuilder)
//! ```

pub mod conn;
pub mod api;
pub mod type_registry;
pub(crate) mod sql_helpers;

#[cfg(feature = "odbc")]
pub mod odbc;

#[cfg(feature = "thrift")]
pub mod thrift;

use conn::DatabricksMode;
use potato_etl_common::db::traits::DriverRegistry;

/// Register the Databricks driver with the given registry.
///
/// All modes share the `databricks://` scheme.  The `mode=` query parameter
/// selects the transport (default: `api`).
pub fn register(registry: &mut DriverRegistry) {
    registry.register_source(
        &["databricks://"],
        Box::new(|conn_str| {
            let params = conn::DatabricksConnParams::parse(conn_str)?;
            match params.mode {
                DatabricksMode::Api => {
                    Ok(Box::new(api::source::DatabricksApiSource::new(params)))
                }
                DatabricksMode::Odbc => {
                    #[cfg(feature = "odbc")]
                    {
                        Ok(Box::new(odbc::source::DatabricksOdbcSource::new(params)))
                    }
                    #[cfg(not(feature = "odbc"))]
                    {
                        anyhow::bail!(
                            "Databricks ODBC mode requires the `odbc` feature. \
                             Rebuild with `--features odbc` or use mode=api (default)."
                        )
                    }
                }
                DatabricksMode::Thrift => {
                    #[cfg(feature = "thrift")]
                    {
                        Ok(Box::new(thrift::source::DatabricksThriftSource::new(params)))
                    }
                    #[cfg(not(feature = "thrift"))]
                    {
                        anyhow::bail!(
                            "Databricks Thrift mode requires the `thrift` feature. \
                             Rebuild with `--features thrift` or use mode=api (default)."
                        )
                    }
                }
            }
        }),
    );

    registry.register_sink(
        &["databricks://"],
        Box::new(|conn_str| {
            let params = conn::DatabricksConnParams::parse(conn_str)?;
            match params.mode {
                DatabricksMode::Api => {
                    Ok(Box::new(api::sink::DatabricksApiSink::new(params)?))
                }
                DatabricksMode::Odbc => {
                    #[cfg(feature = "odbc")]
                    {
                        Ok(Box::new(odbc::sink::DatabricksOdbcSink::new(params)?))
                    }
                    #[cfg(not(feature = "odbc"))]
                    {
                        anyhow::bail!(
                            "Databricks ODBC mode requires the `odbc` feature. \
                             Rebuild with `--features odbc` or use mode=api (default)."
                        )
                    }
                }
                DatabricksMode::Thrift => {
                    #[cfg(feature = "thrift")]
                    {
                        Ok(Box::new(thrift::sink::DatabricksThriftSink::new(params)?))
                    }
                    #[cfg(not(feature = "thrift"))]
                    {
                        anyhow::bail!(
                            "Databricks Thrift mode requires the `thrift` feature. \
                             Rebuild with `--features thrift` or use mode=api (default)."
                        )
                    }
                }
            }
        }),
    );

    registry.register_scd2(
        &["databricks://"],
        Box::new(|conn_str| {
            let params = conn::DatabricksConnParams::parse(conn_str)?;
            match params.mode {
                DatabricksMode::Api => {
                    Ok(Box::new(api::scd2::DatabricksApiScd2::new(params)?))
                }
                DatabricksMode::Odbc | DatabricksMode::Thrift => {
                    anyhow::bail!(
                        "Databricks SCD2 is only supported via the REST API (mode=api). \
                         Current mode: {:?}",
                        params.mode
                    )
                }
            }
        }),
    );
}
