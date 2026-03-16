//! SQL Server driver for potato-etl.
//!
//! Provides `MssqlReadDB`, `MssqlWriteDB`, and `MssqlScd2Sink` using `tiberius`.
//! Optional ODBC write path (`odbc` feature) and BCP CLI loader (`bcp` feature).

pub mod source;
pub mod sink;
pub mod scd2;
pub mod util;
pub mod type_registry;
#[cfg(feature = "bcp")]
pub mod bcp;
#[cfg(feature = "odbc")]
pub mod odbc;

use potato_etl_common::db::traits::DriverRegistry;

/// Register the MSSQL driver with the given registry.
///
/// Scheme: `mssql://`
pub fn register(registry: &mut DriverRegistry) {
    registry.register_source(
        &["mssql://"],
        Box::new(|conn_str| {
            Ok(Box::new(source::MssqlReadDB::new(conn_str)?))
        }),
    );
    registry.register_sink(
        &["mssql://"],
        Box::new(|conn_str| {
            Ok(Box::new(sink::MssqlWriteDB::new(conn_str)?))
        }),
    );
    registry.register_scd2(
        &["mssql://"],
        Box::new(|conn_str| {
            Ok(Box::new(scd2::MssqlScd2Sink::new(conn_str)?))
        }),
    );
}