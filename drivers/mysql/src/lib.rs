//! MySQL / Aurora / MariaDB driver for potato-etl.
//!
//! Provides `MySqlReadDB`, `MySqlWriteDB`, and `MySqlScd2Sink` using `sqlx`.

pub mod source;
pub mod sink;
pub mod scd2;
pub mod util;
pub mod type_registry;

use potato_etl_common::db::traits::DriverRegistry;

/// Register the MySQL driver with the given registry.
///
/// Schemes: `mysql://`, `mariadb://`
pub fn register(registry: &mut DriverRegistry) {
    registry.register_source(
        &["mysql://", "mariadb://"],
        Box::new(|conn_str| {
            Ok(Box::new(source::MySqlReadDB::new(conn_str)))
        }),
    );
    registry.register_sink(
        &["mysql://", "mariadb://"],
        Box::new(|conn_str| {
            Ok(Box::new(sink::MySqlWriteDB::new(conn_str)))
        }),
    );
    registry.register_scd2(
        &["mysql://", "mariadb://"],
        Box::new(|conn_str| {
            Ok(Box::new(scd2::MySqlScd2Sink::new(conn_str)))
        }),
    );
}