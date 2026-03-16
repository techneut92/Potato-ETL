//! Unified schema configuration for source, transform, and sink components.

use std::collections::HashMap;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::schema::field::{ColumnOption, ForeignKey};

// ── SourceSchemaConfig ────────────────────────────────────────────────────────

/// Common source-side schema settings shared by all read steps.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceSchemaConfig {
    /// Unified schema configuration.
    #[serde(default, skip_serializing_if = "ComponentSchema::is_empty")]
    pub schema: ComponentSchema,

    /// Whether to lowercase all column names after reading.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalize_columns: Option<bool>,

    /// List of column names to exclude from the source.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
}

impl SourceSchemaConfig {
    /// Returns the effective Arrow overrides map from `schema.arrow.columns`.
    pub fn arrow_overrides(&self) -> HashMap<String, String> {
        self.schema.arrow_overrides_map()
    }
}

// ── SinkSchemaConfig ──────────────────────────────────────────────────────────

/// Common sink-side schema settings shared by `write_db` and `scd2_sink` steps.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SinkSchemaConfig {
    /// Unified schema configuration.
    #[serde(default, skip_serializing_if = "ComponentSchema::is_empty")]
    pub schema: ComponentSchema,
}

impl SinkSchemaConfig {
    /// Returns the effective Arrow overrides map from `schema.arrow.columns`.
    pub fn arrow_overrides(&self) -> HashMap<String, String> {
        self.schema.arrow_overrides_map()
    }

    /// Returns the effective column options map from `schema.database.columns`.
    pub fn column_options(&self) -> HashMap<String, ColumnOption> {
        self.schema.column_options_map()
    }

    /// Returns the `DatabaseSchemaConfig` if it contains named indexes or constraints.
    pub fn database_schema_config(&self) -> Option<&DatabaseSchemaConfig> {
        let db = self.schema.database.as_ref()?;
        if db.indexes.is_empty() && db.constraints.is_empty() {
            None
        } else {
            Some(db)
        }
    }
}

// ── ComponentSchema (unified schema config) ──────────────────────────────────

/// Unified schema configuration that works on **every** component.
///
/// A structured `schema:` block that cleanly separates database-side and
/// Arrow-side schema concerns.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ComponentSchema {
    /// Database-side schema: column types, indexes, and constraints for DDL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<DatabaseSchemaConfig>,

    /// Arrow-side schema: per-column Arrow type overrides and logical types.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow: Option<ArrowSchemaConfig>,
}

impl ComponentSchema {
    /// Returns `true` when both `database` and `arrow` are absent or empty.
    pub fn is_empty(&self) -> bool {
        self.database.as_ref().map_or(true, |d| d.is_empty())
            && self.arrow.as_ref().map_or(true, |a| a.is_empty())
    }

    /// Extracts a flat `HashMap<String, String>` of pure type-cast overrides.
    pub fn arrow_overrides_map(&self) -> HashMap<String, String> {
        match &self.arrow {
            Some(a) => a.columns.iter()
                .filter(|(_, v)| v.arrow_type.is_some() && v.value.is_none())
                .map(|(k, v)| (k.clone(), v.arrow_type.clone().unwrap()))
                .collect(),
            None => HashMap::new(),
        }
    }

    /// Returns all Arrow column definitions that have a `value` injection.
    pub fn value_injection_columns(&self) -> HashMap<String, &ArrowColumnDef> {
        match &self.arrow {
            Some(a) => a.columns.iter()
                .filter(|(_, v)| v.value.is_some())
                .map(|(k, v)| (k.clone(), v))
                .collect(),
            None => HashMap::new(),
        }
    }

    /// Extracts a flat `HashMap<String, ColumnOption>` for backward compat.
    pub fn column_options_map(&self) -> HashMap<String, ColumnOption> {
        match &self.database {
            Some(db) => db.to_column_options(),
            None => HashMap::new(),
        }
    }
}

// ── DatabaseSchemaConfig ──────────────────────────────────────────────────────

/// Database-side schema: column definitions, named indexes, and named constraints.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatabaseSchemaConfig {
    /// Per-column database type definitions and DDL hints.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, DatabaseColumnDef>,

    /// Named indexes.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub indexes: IndexMap<String, IndexDef>,

    /// Named CHECK constraints.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub constraints: IndexMap<String, ConstraintDef>,
}

impl DatabaseSchemaConfig {
    /// Returns `true` when all maps are empty.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty() && self.indexes.is_empty() && self.constraints.is_empty()
    }

    /// Converts to the legacy `ColumnOptionsMap` format.
    pub fn to_column_options(&self) -> HashMap<String, ColumnOption> {
        self.columns.iter().map(|(name, col)| {
            (name.clone(), ColumnOption {
                db_type:        col.db_type.clone(),
                primary_key:    col.primary_key,
                unique:         col.unique,
                index:          false, // handled via named indexes
                nullable:       col.nullable,
                check_expr:     col.check_expr.clone(),
                default_expr:   col.default_expr.clone(),
                on_update_expr: col.on_update_expr.clone(),
                foreign_key:    col.foreign_key.clone(),
                description:    col.description.clone(),
                enum_values:    col.enum_values.clone().unwrap_or_default(),
            })
        }).collect()
    }
}

// ── DatabaseColumnDef ─────────────────────────────────────────────────────────

/// Per-column database definition within [`DatabaseSchemaConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatabaseColumnDef {
    /// Explicit SQL type for DDL generation.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub db_type: Option<String>,

    /// Marks this column as part of the primary key.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub primary_key: bool,

    /// Whether the column may be NULL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,

    /// Adds a `UNIQUE` constraint on this column.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unique: bool,

    /// Column is auto-generated (IDENTITY / SERIAL / AUTO_INCREMENT).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub generated: bool,

    /// SQL DEFAULT expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_expr: Option<String>,

    /// SQL `ON UPDATE` expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_update_expr: Option<String>,

    /// Inline CHECK constraint expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_expr: Option<String>,

    /// Foreign-key reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foreign_key: Option<ForeignKey>,

    /// Human-readable description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Allowed enum values for this column.
    ///
    /// When set, the DDL generator creates dialect-appropriate enum constraints:
    /// - **Postgres**: `CREATE TYPE "<col>_enum" AS ENUM (...)` + uses that type
    /// - **MySQL**: inline `ENUM('draft', 'active', ...)`
    /// - **MSSQL / Oracle / Databricks**: `CHECK (<col> IN (...))` constraint
    ///
    /// ```yaml
    /// database:
    ///   columns:
    ///     status:
    ///       enum_values: [draft, active, archived, deleted]
    /// ```
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
}

// ── IndexDef ──────────────────────────────────────────────────────────────────

/// Named index definition within [`DatabaseSchemaConfig`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    /// Columns included in the index, in order.
    pub columns: Vec<String>,

    /// Whether this is a unique index.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unique: bool,

    /// Whether this index IS the primary key.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub primary: bool,
}

// ── ConstraintDef ─────────────────────────────────────────────────────────────

/// Named constraint definition within [`DatabaseSchemaConfig`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintDef {
    /// CHECK constraint expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check: Option<String>,
}

// ── ArrowSchemaConfig ─────────────────────────────────────────────────────────

/// Arrow-side schema configuration: per-column type overrides, semantic
/// annotations, and value injections.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ArrowSchemaConfig {
    /// Per-column Arrow type overrides and metadata.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub columns: HashMap<String, ArrowColumnDef>,
}

impl ArrowSchemaConfig {
    /// Returns `true` when no columns are configured.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

// ── ArrowColumnDef ────────────────────────────────────────────────────────────

/// Per-column Arrow type definition within [`ArrowSchemaConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ArrowColumnDef {
    /// Arrow type string, e.g. `"int64"`, `"utf8"`, `"timestamp[us, UTC]"`.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub arrow_type: Option<String>,

    /// Arrow-level nullable override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,

    /// Semantic / logical type annotation (e.g. `"json"`, `"uuid"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_type: Option<String>,

    /// Value injection: `$name` = env var, `name` = copy from batch column.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}