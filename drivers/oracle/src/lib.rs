//! Oracle Database driver for potato-etl.
//!
//! Provides `OracleReadDB`, `OracleWriteDB`, and `OracleScd2Sink`.
//! Requires Oracle Instant Client installed on the system.

pub mod source;
pub mod sink;
pub mod scd2;
pub mod util;
pub mod type_registry;

use potato_etl_common::db::traits::DriverRegistry;

/// Register the Oracle driver with the given registry.
///
/// Scheme: `oracle://`
pub fn register(registry: &mut DriverRegistry) {
    registry.register_source(
        &["oracle://"],
        Box::new(|conn_str| {
            Ok(Box::new(source::OracleReadDB::new(conn_str)?))
        }),
    );
    registry.register_sink(
        &["oracle://"],
        Box::new(|conn_str| {
            Ok(Box::new(sink::OracleWriteDB::new(conn_str)?))
        }),
    );
    registry.register_scd2(
        &["oracle://"],
        Box::new(|conn_str| {
            Ok(Box::new(scd2::OracleScd2Sink::new(conn_str)?))
        }),
    );
}