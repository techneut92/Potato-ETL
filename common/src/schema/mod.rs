//! Schema model for the ETL pipeline.
//!
//! ## 1. Per-column DDL hints ([`field`])
//!
//! [`ColumnOption`] is the per-column DDL constraint descriptor for sinks.
//! [`DatabaseColumnsMap`] = `HashMap<String, ColumnOption>`.
//!
//! [`ForeignKey`] describes a foreign-key reference used by [`ColumnOption`].
//!
//! ## 2. Whole-table structure ([`table`])
//!
//! [`TableSchema`] represents the **declared or inferred structure** of a
//! complete table — its columns (in order), types, and primary key.
//!
//! ## 3. DDL generation + dialect mapping ([`ddl`])
//!
//! [`generate_ddl`] converts an Arrow schema enriched with `etl.*` metadata
//! into `CREATE TABLE` statements for all five supported SQL dialects.
//!
//! ## 4. Arrow metadata constants ([`constants`])
//!
//! [`META_NULLABLE`], [`META_PRIMARY_KEY`], … — the `etl.*` keys written by
//! [`apply_database_columns`] and read back by [`generate_ddl`].
//!
//! ## 5. Schema application ([`apply`])
//!
//! | Function | Purpose |
//! |---|---|
//! | [`apply_arrow_type_overrides`]  | Arrow type casting (source + sink) |
//! | [`apply_value_injections`] | Value injection from env vars / batch columns |
//! | [`apply_database_columns`]   | DDL constraints + db_type at sink |
//! | [`apply_rename`]           | Column renaming |

// ── Submodules ────────────────────────────────────────────────────────────────

/// Arrow field metadata key constants (`etl.*`).
pub mod constants;
/// Per-column types: [`ColumnOption`], [`ForeignKey`], [`LogicalType`].
pub mod field;
/// Whole-table structure: [`TableSchema`], [`ColumnDef`].
pub mod table;
/// Schema application functions.
pub mod apply;
/// DDL generation: [`generate_ddl`], [`SqlDialect`], dialect-aware type mapping.
pub mod ddl;

// ── Flat re-exports ───────────────────────────────────────────────────────────

pub use constants::{
    META_CHECK_EXPR, META_DB_TYPE, META_DEFAULT_EXPR, META_DESCRIPTION,
    META_ENUM_VALUES, META_FOREIGN_KEY, META_INDEX, META_LOGICAL_TYPE, META_NULLABLE,
    META_ON_UPDATE_EXPR, META_PRIMARY_KEY, META_SOURCE_DB, META_UNIQUE,
};

pub use field::{ColumnOption, DatabaseColumnsMap, ForeignKey, LogicalType, logical_type_for_source};

pub use table::{ColumnDef, TableSchema};

pub use apply::{
    apply_arrow_type_overrides, apply_rename, apply_database_columns,
    apply_database_structural, apply_value_injections, apply_exclude_columns,
    // compile-once plans
    ArrowTypeOverridesPlan, compile_arrow_type_overrides, apply_arrow_type_overrides_plan,
    MetadataStampPlan,  compile_metadata_stamps,  apply_metadata_stamp_plan,
};

pub use ddl::{
    arrow_type_to_sql_dialect,
    DdlOptions, DdlStatements, generate_ddl, generate_ddl_with_schema, generate_post_create,
    logical_type_to_target_sql,
    make_create_index, resolve_sql_type, Scd2DdlInfo, SqlDialect,
    source_type_to_target_sql,
};