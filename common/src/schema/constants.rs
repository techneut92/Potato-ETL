//! Arrow field metadata key constants (`etl.*`).
//!
//! These string keys are written by [`apply_arrow_type_overrides`][super::apply_arrow_type_overrides],
//! [`apply_database_columns`][super::apply_database_columns], and source connectors,
//! then read back by [`generate_ddl`][super::generate_ddl] and
//! [`table_def_from_arrow`][super::table_def_from_arrow].
//!
//! ## ETL policy keys (written by `apply_database_columns`)
//!
//! | Key                  | Value type        | Meaning                                |
//! |----------------------|-------------------|----------------------------------------|
//! | `etl.nullable`       | `"true"/"false"`  | Overrides Arrow's `is_nullable()`      |
//! | `etl.primary_key`    | `"true"`          | Column is part of the primary key      |
//! | `etl.db_type`        | SQL string        | Explicit DDL type override, e.g. `NUMERIC(12,2)` |
//! | `etl.foreign_key`    | JSON object       | `{"table":"…","column":"…"}`           |
//! | `etl.description`    | plain text        | Human-readable description             |
//! | `etl.default_expr`   | SQL expression    | DEFAULT expression, e.g. `now()`       |
//! | `etl.unique`         | `"true"`          | UNIQUE constraint on this column       |
//! | `etl.index`          | `"true"`          | CREATE INDEX on this column            |
//! | `etl.check_expr`     | SQL expression    | CHECK constraint, e.g. `salary > 0`   |
//!
//! ## Semantic type keys (written by source connectors)
//!
//! | Key                  | Value type        | Meaning                                |
//! |----------------------|-------------------|----------------------------------------|
//! | `etl.logical_type`   | [`LogicalType`] string | Semantic type (e.g. `"json"`, `"uuid"`, `"currency"`) |
//! | `etl.source_db`      | driver name string | Which DB produced this column (`"postgres"`, `"mssql"`, …) |
//!
//! `etl.logical_type` is the **key layer** for cross-database type resolution.
//! When a sink sees `logical_type = "json"` it knows to emit `NVARCHAR(MAX)` on
//! MSSQL, `JSONB` on Postgres, `JSON` on MySQL, and `CLOB` on Oracle — without
//! needing a full source-DB → target-DB mapping table for every pair.
//!
//! Resolution priority inside `resolve_sql_type()`:
//! 1. `etl.db_type` — explicit user override (verbatim SQL string)
//! 2. `etl.logical_type` → target dialect via `logical_type_to_target_sql()`
//! 3. `source_db_type` → target dialect via `source_type_to_target_sql()`
//! 4. Arrow `DataType` → target dialect fallback

pub const META_NULLABLE:      &str = "etl.nullable";
pub const META_PRIMARY_KEY:   &str = "etl.primary_key";
pub const META_DB_TYPE:       &str = "etl.db_type";
pub const META_FOREIGN_KEY:   &str = "etl.foreign_key";
pub const META_DESCRIPTION:   &str = "etl.description";
pub const META_DEFAULT_EXPR:  &str = "etl.default_expr";
pub const META_UNIQUE:        &str = "etl.unique";
pub const META_INDEX:         &str = "etl.index";
pub const META_CHECK_EXPR:    &str = "etl.check_expr";

/// `ON UPDATE` expression for automatic updates on row modification.
///
/// Only natively supported by MySQL (`ON UPDATE CURRENT_TIMESTAMP`).
/// For Postgres, MSSQL, and Oracle, the DDL generator emits a
/// `CREATE TRIGGER` statement in `post_create` that calls the expression
/// on each `UPDATE`.  Databricks does not support triggers — a warning
/// is logged and the key is ignored.
pub const META_ON_UPDATE_EXPR: &str = "etl.on_update_expr";

/// Allowed enum values for columns with `logical_type = "enum"`.
///
/// Stored as a JSON array string, e.g. `["draft","active","archived"]`.
///
/// When present:
/// - **Postgres**: DDL generates `CREATE TYPE <col>_enum AS ENUM (...)` and
///   uses that type instead of `TEXT`.
/// - **MySQL**: DDL generates `ENUM('draft','active','archived')`.
/// - **MSSQL / Oracle / Databricks**: DDL adds a `CHECK` constraint.
pub const META_ENUM_VALUES: &str = "etl.enum_values";

/// Semantic type carried through the pipeline.
///
/// Set by source connectors on every field whose source DB type maps to a
/// well-known semantic meaning.  Read by [`resolve_sql_type`][super::resolve_sql_type]
/// as tier 2 type resolution (highest priority: `etl.db_type` override).
///
/// See [`LogicalType`][super::field::LogicalType] for the full list of variants.
pub const META_LOGICAL_TYPE:  &str = "etl.logical_type";

/// Which database engine produced this column.
///
/// Set by source connectors.  Value is a lowercase driver name string:
/// `"postgres"`, `"mssql"`, `"oracle"`, `"mysql"`, `"databricks"`.
pub const META_SOURCE_DB:     &str = "etl.source_db";