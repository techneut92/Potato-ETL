//! PostgreSQL driver for potato-etl.
//!
//! Provides `PgReadDB`, `PgWriteDB`, and `PgScd2Sink` implementations
//! using `sqlx` (pure Rust, no system libraries).

pub mod source;
pub mod sink;
pub mod scd2;
pub mod util;
pub mod type_registry;

use potato_etl_common::db::traits::DriverRegistry;

/// Register the PostgreSQL driver with the given registry.
///
/// Schemes: `postgresql://`, `postgres://`
pub fn register(registry: &mut DriverRegistry) {
    registry.register_source(
        &["postgresql://", "postgres://"],
        Box::new(|conn_str| {
            Ok(Box::new(source::PgReadDB::new(conn_str)))
        }),
    );
    registry.register_sink(
        &["postgresql://", "postgres://"],
        Box::new(|conn_str| {
            Ok(Box::new(sink::PgWriteDB::new(conn_str)))
        }),
    );
    registry.register_scd2(
        &["postgresql://", "postgres://"],
        Box::new(|conn_str| {
            Ok(Box::new(scd2::PgScd2Sink::new(conn_str)))
        }),
    );
}