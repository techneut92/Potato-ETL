//! Per-column schema types for the ETL pipeline.
//!
//! ## Types
//!
//! | Type | Purpose |
//! |---|---|
//! | [`ColumnOption`] | Per-column DDL hints for sinks (PK, unique, index, etc.) |
//! | [`DatabaseColumnsMap`] | `HashMap<String, ColumnOption>` — canonical alias |
//! | [`ForeignKey`] | FK reference used by [`ColumnOption`] |
//! | [`LogicalType`] | Semantic type (JSON, UUID, Currency, …) for DDL resolution |

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ── LogicalType ──────────────────────────────────────────────────────────────

/// Semantic type of a column — the **middle layer** between the source DB type
/// and the Arrow physical type.
///
/// Resolution priority inside `resolve_sql_type()`:
/// 1. `etl.db_type` — explicit YAML override (highest)
/// 2. `etl.logical_type` ← **this enum**
/// 3. `source_db_type` raw cross-dialect mapping
/// 4. Arrow `DataType` fallback (lowest)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalType {
    Json, Currency, Uuid, Xml, Ip, MacAddr, Geometry,
    BitString, Range, Array, FullText, Interval, Hstore, Enum,
}

impl LogicalType {
    /// Returns the snake_case string representation, as stored in Arrow metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Json      => "json",
            Self::Currency  => "currency",
            Self::Uuid      => "uuid",
            Self::Xml       => "xml",
            Self::Ip        => "ip",
            Self::MacAddr   => "mac_addr",
            Self::Geometry  => "geometry",
            Self::BitString => "bit_string",
            Self::Range     => "range",
            Self::Array     => "array",
            Self::FullText  => "full_text",
            Self::Interval  => "interval",
            Self::Hstore    => "hstore",
            Self::Enum      => "enum",
        }
    }

    /// Parses from the string stored in Arrow metadata.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "json"       => Some(Self::Json),
            "currency"   => Some(Self::Currency),
            "uuid"       => Some(Self::Uuid),
            "xml"        => Some(Self::Xml),
            "ip"         => Some(Self::Ip),
            "mac_addr"   => Some(Self::MacAddr),
            "geometry"   => Some(Self::Geometry),
            "bit_string" => Some(Self::BitString),
            "range"      => Some(Self::Range),
            "array"      => Some(Self::Array),
            "full_text"  => Some(Self::FullText),
            "interval"   => Some(Self::Interval),
            "hstore"     => Some(Self::Hstore),
            "enum"       => Some(Self::Enum),
            _            => None,
        }
    }
}

/// Derives a [`LogicalType`] from a source DB type name and driver.
pub fn logical_type_for_source(source_db_type: &str, source_db: &str) -> Option<LogicalType> {
    let t = source_db_type.to_lowercase();
    let db = source_db.to_lowercase();

    if matches!(t.as_str(), "json" | "jsonb") { return Some(LogicalType::Json); }
    if matches!(t.as_str(), "uuid" | "uniqueidentifier") { return Some(LogicalType::Uuid); }
    if matches!(t.as_str(), "money" | "smallmoney") { return Some(LogicalType::Currency); }
    if matches!(t.as_str(), "xml" | "xmltype") { return Some(LogicalType::Xml); }
    if matches!(t.as_str(), "inet" | "cidr") { return Some(LogicalType::Ip); }
    if matches!(t.as_str(), "macaddr" | "macaddr8") { return Some(LogicalType::MacAddr); }
    if matches!(t.as_str(),
        "point" | "line" | "lseg" | "box" | "circle" | "path" | "polygon" |
        "geometry" | "geography" | "sdo_geometry"
    ) { return Some(LogicalType::Geometry); }
    if t == "varbit" || (t == "bit" && db == "postgres") { return Some(LogicalType::BitString); }
    if matches!(t.as_str(),
        "int4range" | "int8range" | "numrange" | "daterange" | "tsrange" | "tstzrange"
    ) { return Some(LogicalType::Range); }
    if db == "postgres" && t.starts_with('_') { return Some(LogicalType::Array); }
    if matches!(t.as_str(), "tsvector" | "tsquery") { return Some(LogicalType::FullText); }
    if t == "interval" { return Some(LogicalType::Interval); }
    if t == "hstore" { return Some(LogicalType::Hstore); }

    None
}

// ── ForeignKey ────────────────────────────────────────────────────────────────

/// Foreign-key reference used by [`ColumnOption`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

// ── ColumnOption ──────────────────────────────────────────────────────────────

/// Per-column DDL hint for sink steps.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnOption {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_type: Option<String>,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub index: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_expr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_expr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_update_expr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub foreign_key: Option<ForeignKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Allowed enum values (e.g. `["draft", "active", "archived"]`).
    ///
    /// When set, the DDL generator creates a native enum type (Postgres),
    /// `ENUM(...)` (MySQL), or a `CHECK` constraint (MSSQL/Oracle/Databricks).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enum_values: Vec<String>,
    /// Output name when this column is renamed during schema apply.
    /// The map key (this column's source-side name) is matched
    /// case-insensitively; the rename target is written verbatim.
    /// Field metadata follows the column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rename_to: Option<String>,
    /// Drop this column from the batch during schema apply.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub drop: bool,
}

/// Type alias for the per-column options map on sinks.
pub type DatabaseColumnsMap = HashMap<String, ColumnOption>;