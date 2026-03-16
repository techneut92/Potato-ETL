//! Declared or inferred whole-table schema.
//!
//! [`TableSchema`] represents **what a table actually is** — its columns,
//! types, and primary key, in declaration order.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::schema::constants::{
    META_DB_TYPE, META_DESCRIPTION, META_NULLABLE, META_PRIMARY_KEY,
};

// ── TableSchema ───────────────────────────────────────────────────────────────

/// Declared or inferred description of a complete table.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableSchema {
    /// Columns in declaration order.
    pub columns: IndexMap<String, ColumnDef>,

    /// Columns that together form the primary key, in key order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_key: Vec<String>,

    /// Table name, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Database schema the table lives in.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_schema: Option<String>,
}

// ── ColumnDef ─────────────────────────────────────────────────────────────────

/// Description of a single column within a [`TableSchema`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    #[serde(with = "arrow_type_serde")]
    pub arrow_type: DataType,

    #[serde(default = "default_nullable")]
    pub nullable: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_type: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(default)]
    pub cdc_key: bool,
}

fn default_nullable() -> bool { true }

// ── TableSchema impl ──────────────────────────────────────────────────────────

impl TableSchema {
    /// Constructs a `TableSchema` from an Arrow schema.
    pub fn from_arrow(
        schema:    &ArrowSchema,
        name:      Option<String>,
        db_schema: Option<String>,
    ) -> Self {
        let mut primary_key = Vec::new();
        let mut columns     = IndexMap::new();

        for field in schema.fields() {
            let meta = field.metadata();
            let nullable = meta.get(META_NULLABLE)
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(field.is_nullable());
            let is_pk = meta.get(META_PRIMARY_KEY)
                .map(|v| v == "true")
                .unwrap_or(false);
            if is_pk { primary_key.push(field.name().clone()); }

            columns.insert(field.name().clone(), ColumnDef {
                arrow_type:  field.data_type().clone(),
                nullable,
                db_type:     meta.get(META_DB_TYPE).cloned(),
                description: meta.get(META_DESCRIPTION).cloned(),
                cdc_key:     is_pk,
            });
        }

        Self { columns, primary_key, name, db_schema }
    }

    /// Converts this `TableSchema` to an Arrow `Schema`.
    pub fn to_arrow(&self) -> ArrowSchema {
        let pk_set: HashSet<&str> = self.primary_key.iter().map(String::as_str).collect();

        let fields: Vec<Field> = self.columns.iter().map(|(name, col)| {
            let mut meta = HashMap::new();
            meta.insert(META_NULLABLE.to_string(), col.nullable.to_string());
            if let Some(dt) = &col.db_type {
                meta.insert(META_DB_TYPE.to_string(), dt.clone());
            }
            if let Some(desc) = &col.description {
                meta.insert(META_DESCRIPTION.to_string(), desc.clone());
            }
            if pk_set.contains(name.as_str()) || col.cdc_key {
                meta.insert(META_PRIMARY_KEY.to_string(), "true".to_string());
            }
            Field::new(name.as_str(), col.arrow_type.clone(), col.nullable)
                .with_metadata(meta)
        }).collect();

        ArrowSchema::new_with_metadata(
            fields,
            self.name.as_ref().map(|n| {
                let mut m = HashMap::new();
                m.insert("etl.table".to_string(), n.clone());
                if let Some(s) = &self.db_schema {
                    m.insert("etl.schema".to_string(), s.clone());
                }
                m
            }).unwrap_or_default(),
        )
    }

    /// Returns `true` if every column listed in `primary_key` exists in `columns`.
    pub fn is_valid(&self) -> bool {
        self.primary_key.iter().all(|k| self.columns.contains_key(k))
    }

    /// Returns an Arrow `SchemaRef`.
    pub fn to_arrow_ref(&self) -> Arc<ArrowSchema> {
        Arc::new(self.to_arrow())
    }
}

// ── Custom serde for Arrow DataType ──────────────────────────────────────────

mod arrow_type_serde {
    use arrow::datatypes::{DataType, TimeUnit};
    use serde::{Deserializer, Serializer};
    use serde::de::Error as _;

    pub fn serialize<S: Serializer>(dt: &DataType, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&data_type_to_str(dt))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DataType, D::Error> {
        let raw = <String as serde::Deserialize>::deserialize(d)?;
        crate::util::schema::parse_arrow_type(&raw).map_err(D::Error::custom)
    }

    fn data_type_to_str(dt: &DataType) -> String {
        match dt {
            DataType::Boolean     => "boolean".into(),
            DataType::Int8        => "int8".into(),
            DataType::Int16       => "int16".into(),
            DataType::Int32       => "int32".into(),
            DataType::Int64       => "int64".into(),
            DataType::Float32     => "float32".into(),
            DataType::Float64     => "float64".into(),
            DataType::Utf8        => "utf8".into(),
            DataType::LargeUtf8   => "large_utf8".into(),
            DataType::Binary      => "binary".into(),
            DataType::LargeBinary => "large_binary".into(),
            DataType::Date32      => "date32".into(),
            DataType::Timestamp(u, None)     => format!("timestamp[{}]",     unit_str(u)),
            DataType::Timestamp(u, Some(tz)) => format!("timestamp[{}, {}]", unit_str(u), tz),
            DataType::Time32(u)              => format!("time32[{}]",        unit_str(u)),
            DataType::Time64(u)              => format!("time64[{}]",        unit_str(u)),
            DataType::Duration(u)            => format!("duration[{}]",      unit_str(u)),
            other => format!("{other:?}"),
        }
    }

    fn unit_str(u: &TimeUnit) -> &'static str {
        match u {
            TimeUnit::Second      => "s",
            TimeUnit::Millisecond => "ms",
            TimeUnit::Microsecond => "us",
            TimeUnit::Nanosecond  => "ns",
        }
    }
}
