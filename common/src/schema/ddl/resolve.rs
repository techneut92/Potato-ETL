//! SQL type resolution: `resolve_sql_type`, `logical_type_to_target_sql`,
//! `source_type_to_target_sql`.

use arrow::datatypes::Field as ArrowField;
use crate::db::common::field_meta;
use crate::schema::constants::{META_DB_TYPE, META_LOGICAL_TYPE, META_ENUM_VALUES};
use crate::schema::field::LogicalType;
use super::SqlDialect;
use super::dialect::arrow_type_to_sql_dialect;
use serde_json;

// ── resolve_sql_type ──────────────────────────────────────────────────────────

/// Four-tier SQL type resolution for a single Arrow field.
///
/// Priority (highest → lowest):
///
/// 1. **`etl.db_type`** — explicit user override; used verbatim.
/// 2. **`etl.logical_type`** — semantic type from source connectors.
/// 3. **`source_db_type`** — original DB type name, translated to target dialect.
/// 4. **Arrow `DataType`** — dialect-aware fallback.
pub fn resolve_sql_type(field: &ArrowField, dialect: SqlDialect) -> String {
    let meta = field.metadata();

    let resolved = if let Some(explicit) = meta.get(META_DB_TYPE) {
        explicit.clone()
    } else if let Some(lt_str) = meta.get(META_LOGICAL_TYPE) {
        if let Some(lt) = LogicalType::from_str(lt_str) {
            // Special handling for enums with declared values.
            if lt == LogicalType::Enum {
                if let Some(enum_values_json) = meta.get(META_ENUM_VALUES) {
                    if let Ok(values) = serde_json::from_str::<Vec<String>>(enum_values_json) {
                        if !values.is_empty() {
                            return resolve_enum_with_values(field.name(), &values, dialect);
                        }
                    }
                }
            }
            let precision = meta.get(field_meta::SOURCE_PRECISION)
                .and_then(|s| s.parse::<i32>().ok());
            let scale = meta.get(field_meta::SOURCE_SCALE)
                .and_then(|s| s.parse::<i32>().ok());
            logical_type_to_target_sql(lt, dialect, field.name(), precision, scale)
        } else {
            tracing::warn!(
                column       = field.name(),
                logical_type = lt_str,
                "etl.logical_type is set but not recognised; falling back to source_db_type / Arrow resolution"
            );
            resolve_from_source_or_arrow(field, meta, dialect)
        }
    } else {
        resolve_from_source_or_arrow(field, meta, dialect)
    };

    // ── MSSQL TIME normalisation ──────────────────────────────────────────────
    if dialect == SqlDialect::Mssql {
        let upper = resolved.trim().to_ascii_uppercase();
        if upper == "TIME" || (upper.starts_with("TIME(") && upper.ends_with(')')) {
            if upper != "TIME(7)" {
                tracing::warn!(
                    column   = field.name(),
                    from     = %resolved,
                    to       = "TIME(7)",
                    tier     = if meta.contains_key(META_DB_TYPE) { "etl.db_type override" } else { "Arrow fallback" },
                    "MSSQL TIME normalisation: TIME(n) promoted to TIME(7) for full 100 ns precision",
                );
            }
            return "TIME(7)".to_string();
        }
    }

    resolved
}

/// Tiers 3 + 4: `source_db_type` translation then Arrow fallback.
fn resolve_from_source_or_arrow(
    field:   &ArrowField,
    meta:    &std::collections::HashMap<String, String>,
    dialect: SqlDialect,
) -> String {
    if let Some(src_type) = meta.get(field_meta::SOURCE_DB_TYPE) {
        let precision = meta.get(field_meta::SOURCE_PRECISION)
            .and_then(|s| s.parse::<i32>().ok());
        let scale = meta.get(field_meta::SOURCE_SCALE)
            .and_then(|s| s.parse::<i32>().ok());
        let length = meta.get(field_meta::SOURCE_LENGTH)
            .and_then(|s| s.parse::<i64>().ok());

        if let Some(sql) = source_type_to_target_sql(
            src_type, precision, scale, length, dialect, field.name()
        ) {
            return sql;
        }
    }
    arrow_type_to_sql_dialect(field.data_type(), dialect)
}

// ── resolve_enum_with_values ────────────────────────────────────────────────

/// Resolves an enum type with declared values to a SQL type expression for the given target dialect.
///
/// For Postgres: returns the enum type name (`"<col>_enum"`). The caller
/// ([`generate_ddl`]) is responsible for emitting `CREATE TYPE`.
/// For MySQL: returns `ENUM('a','b','c')` inline.
/// For MSSQL/Oracle/Databricks: returns the fallback text type.  The caller
/// adds a `CHECK` constraint.
fn resolve_enum_with_values(
    col:       &str,
    values:    &[String],
    dialect:   SqlDialect,
) -> String {
    use SqlDialect::*;

    match dialect {
        Postgres => {
            // Type name is created by generate_ddl as: CREATE TYPE "<col>_enum" AS ENUM (...)
            format!("\"{}\"", enum_type_name(col))
        },
        Mysql => {
            let escaped: Vec<String> = values.iter()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .collect();
            format!("ENUM({})", escaped.join(", "))
        },
        // MSSQL/Oracle/Databricks: use text type, generate_ddl adds CHECK constraint.
        Mssql      => "NVARCHAR(255)".to_string(),
        Oracle     => "NVARCHAR2(255)".to_string(),
        Databricks => "STRING".to_string(),
    }
}

/// Returns the generated Postgres enum type name for a column.
pub(super) fn enum_type_name(col: &str) -> String {
    format!("{col}_enum")
}

// ── logical_type_to_target_sql ────────────────────────────────────────────────

/// Maps a [`LogicalType`] to a SQL type expression for the given target dialect.
pub fn logical_type_to_target_sql(
    lt:        LogicalType,
    dialect:   SqlDialect,
    col:       &str,
    precision: Option<i32>,
    _scale:    Option<i32>,
) -> String {
    use SqlDialect::*;

    macro_rules! lossy {
        ($sql:expr, $note:expr) => {{
            tracing::warn!(
                column       = col,
                logical_type = lt.as_str(),
                target_type  = $sql,
                note         = $note,
                "logical type has no native equivalent in target DB"
            );
            $sql.to_string()
        }};
    }

    match lt {
        LogicalType::Json => match dialect {
            Postgres   => "JSONB".to_string(),
            Mssql      => lossy!("NVARCHAR(MAX)", "MSSQL has no JSON column type"),
            Oracle     => lossy!("CLOB", "Oracle < 21c has no JSON column type"),
            Mysql      => "JSON".to_string(),
            Databricks => "STRING".to_string(),
        },
        LogicalType::Currency => match dialect {
            Postgres   => "NUMERIC(19,4)".to_string(),
            Mssql      => "MONEY".to_string(),
            Oracle     => "NUMBER(19,4)".to_string(),
            Mysql      => "DECIMAL(19,4)".to_string(),
            Databricks => "DECIMAL(19,4)".to_string(),
        },
        LogicalType::Uuid => match dialect {
            Postgres   => "UUID".to_string(),
            Mssql      => "UNIQUEIDENTIFIER".to_string(),
            Oracle     => "VARCHAR2(36)".to_string(),
            Mysql      => "VARCHAR(36)".to_string(),
            Databricks => "STRING".to_string(),
        },
        LogicalType::Xml => match dialect {
            Postgres   => "XML".to_string(),
            Mssql      => "XML".to_string(),
            Oracle     => lossy!("CLOB", "Oracle XMLTYPE requires XMLTYPE() constructor"),
            Mysql      => lossy!("LONGTEXT", "MySQL has no XML column type"),
            Databricks => lossy!("STRING", "Databricks has no XML column type"),
        },
        LogicalType::Ip => match dialect {
            Postgres   => "INET".to_string(),
            Mssql      => lossy!("NVARCHAR(45)", "MSSQL has no IP/INET type"),
            Oracle     => lossy!("VARCHAR2(45)", "Oracle has no IP/INET type"),
            Mysql      => lossy!("VARCHAR(45)", "MySQL has no IP/INET type"),
            Databricks => lossy!("STRING", "Databricks has no IP/INET type"),
        },
        LogicalType::MacAddr => match dialect {
            Postgres   => "MACADDR".to_string(),
            Mssql      => lossy!("NVARCHAR(23)", "MSSQL has no MAC address type"),
            Oracle     => lossy!("VARCHAR2(23)", "Oracle has no MAC address type"),
            Mysql      => lossy!("VARCHAR(23)", "MySQL has no MAC address type"),
            Databricks => lossy!("STRING", "Databricks has no MAC address type"),
        },
        LogicalType::Geometry => match dialect {
            Postgres   => "TEXT".to_string(),
            Mssql      => lossy!("NVARCHAR(MAX)", "MSSQL Spatial GEOMETRY not assumed; storing WKT"),
            Oracle     => lossy!("CLOB", "Oracle Spatial SDO_GEOMETRY not assumed; storing WKT"),
            Mysql      => lossy!("LONGTEXT", "MySQL geometry stored as WKT text"),
            Databricks => lossy!("STRING", "Databricks geometry stored as WKT string"),
        },
        LogicalType::BitString => match dialect {
            Postgres   => "TEXT".to_string(),
            Mssql      => lossy!("NVARCHAR(MAX)", "MSSQL has no variable bit-string type"),
            Oracle     => lossy!("VARCHAR2(4000)", "Oracle has no bit-string type"),
            Mysql      => lossy!("TEXT", "MySQL has no variable bit-string type"),
            Databricks => lossy!("STRING", "Databricks has no bit-string type"),
        },
        LogicalType::Range => match dialect {
            Postgres   => "TEXT".to_string(),
            Mssql      => lossy!("NVARCHAR(100)", "No native range type"),
            Oracle     => lossy!("VARCHAR2(100)", "No native range type"),
            Mysql      => lossy!("VARCHAR(100)", "No native range type"),
            Databricks => lossy!("STRING", "No native range type"),
        },
        LogicalType::Array => match dialect {
            Postgres   => "TEXT".to_string(),
            Mssql      => lossy!("NVARCHAR(MAX)", "No native array type"),
            Oracle     => lossy!("CLOB", "No native array type"),
            Mysql      => "JSON".to_string(),
            Databricks => "STRING".to_string(),
        },
        LogicalType::FullText => match dialect {
            Postgres   => "TEXT".to_string(),
            Mssql      => lossy!("NVARCHAR(MAX)", "MSSQL full-text is index-based"),
            Oracle     => lossy!("CLOB", "Oracle Text is index-based"),
            Mysql      => lossy!("LONGTEXT", "MySQL full-text is index-based"),
            Databricks => lossy!("STRING", "No tsvector equivalent in Databricks"),
        },
        LogicalType::Interval => match dialect {
            Postgres   => "INTERVAL".to_string(),
            Mssql      => lossy!("NVARCHAR(50)", "MSSQL has no INTERVAL column type"),
            Oracle     => {
                let p = precision.unwrap_or(6);
                format!("INTERVAL DAY(9) TO SECOND({p})")
            },
            Mysql      => lossy!("VARCHAR(50)", "MySQL has no INTERVAL column type"),
            Databricks => lossy!("STRING", "Databricks has no INTERVAL column type"),
        },
        LogicalType::Hstore => match dialect {
            Postgres   => "JSONB".to_string(),
            Mssql      => lossy!("NVARCHAR(MAX)", "hstore serialised as JSON text"),
            Oracle     => lossy!("CLOB", "hstore serialised as JSON text"),
            Mysql      => "JSON".to_string(),
            Databricks => "STRING".to_string(),
        },
        LogicalType::Enum => match dialect {
            Postgres   => "TEXT".to_string(),
            Mssql      => lossy!("NVARCHAR(255)", "Postgres enum stored as text"),
            Oracle     => lossy!("NVARCHAR2(255)", "Postgres enum stored as text"),
            Mysql      => lossy!("VARCHAR(255)", "MySQL has ENUM but values unknown; storing as VARCHAR"),
            Databricks => "STRING".to_string(),
        },
    }
}

// ── source_type_to_target_sql ─────────────────────────────────────────────────

/// Translates a `source_db_type` into a SQL type expression for the target dialect.
///
/// Returns `None` for unknown source types (caller falls back to Arrow mapping).
pub fn source_type_to_target_sql(
    source_type: &str,
    precision:   Option<i32>,
    scale:       Option<i32>,
    length:      Option<i64>,
    dialect:     SqlDialect,
    col:         &str,
) -> Option<String> {
    use SqlDialect::*;

    macro_rules! pick {
        ($pg:expr, $ms:expr, $ora:expr, $my:expr, $dbx:expr) => {
            match dialect {
                Postgres   => $pg.to_string(),
                Mssql      => $ms.to_string(),
                Oracle     => $ora.to_string(),
                Mysql      => $my.to_string(),
                Databricks => $dbx.to_string(),
            }
        };
    }

    macro_rules! lossy {
        ($sql:expr, $note:expr) => {{
            tracing::warn!(
                column = col, source_type,
                target_type = $sql, note = $note,
                "cross-dialect type mapping: no direct equivalent in target DB"
            );
            $sql.to_string()
        }};
    }

    macro_rules! num {
        ($pg:expr, $ms:expr, $ora:expr, $my:expr, $dbx:expr) => {{
            let base = match dialect {
                Postgres => $pg, Mssql => $ms, Oracle => $ora, Mysql => $my, Databricks => $dbx,
            };
            match (precision, scale) {
                (Some(p), Some(s)) if s > 0 => format!("{base}({p},{s})"),
                (Some(p), _)                => format!("{base}({p})"),
                _                           => base.to_string(),
            }
        }};
    }

    macro_rules! varlen {
        ($base:expr, $large:expr, $max:expr) => {{
            match length {
                Some(-1) | None           => $large.to_string(),
                Some(n) if n > $max       => $large.to_string(),
                Some(n)                   => format!("{}({n})", $base),
            }
        }};
    }

    let src = source_type.to_lowercase();

    Some(match src.as_str() {
        // ── Integers ─────────────────────────────────────────────────────────
        "int2" | "smallint"               => pick!("SMALLINT",   "SMALLINT",  "NUMBER(5)",  "SMALLINT",  "SMALLINT"),
        "int4" | "int" | "integer"        => pick!("INTEGER",    "INT",       "NUMBER(10)", "INT",       "INT"),
        "int8" | "bigint"                 => pick!("BIGINT",     "BIGINT",    "NUMBER(19)", "BIGINT",    "BIGINT"),
        "tinyint"                         => pick!("SMALLINT",   "TINYINT",   "NUMBER(3)",  "TINYINT",   "TINYINT"),

        // ── Floats ───────────────────────────────────────────────────────────
        "float4" | "real"                            => pick!("REAL",             "REAL",   "BINARY_FLOAT",  "FLOAT",   "FLOAT"),
        "float8" | "float" | "double" | "double precision"
                                                     => pick!("DOUBLE PRECISION", "FLOAT",  "BINARY_DOUBLE", "DOUBLE",  "DOUBLE"),
        "binary_float"                               => pick!("REAL",             "REAL",   "BINARY_FLOAT",  "FLOAT",   "FLOAT"),
        "binary_double"                              => pick!("DOUBLE PRECISION", "FLOAT",  "BINARY_DOUBLE", "DOUBLE",  "DOUBLE"),

        // ── Decimal / Numeric ────────────────────────────────────────────────
        "numeric" | "decimal"             => num!("NUMERIC",  "DECIMAL",  "NUMBER",  "DECIMAL",  "DECIMAL"),
        "number"                          => num!("NUMERIC",  "DECIMAL",  "NUMBER",  "DECIMAL",  "DECIMAL"),
        "money"                           => pick!("NUMERIC(19,4)",  "MONEY",       "NUMBER(19,4)",  "DECIMAL(19,4)",  "DECIMAL(19,4)"),
        "smallmoney"                      => pick!("NUMERIC(10,4)",  "SMALLMONEY",  "NUMBER(10,4)",  "DECIMAL(10,4)",  "DECIMAL(10,4)"),

        // ── Boolean ──────────────────────────────────────────────────────────
        "bool" | "boolean"                => pick!("BOOLEAN",  "BIT",  "NUMBER(1)",  "TINYINT(1)",  "BOOLEAN"),
        "bit" => match dialect {
            Mssql => "BIT".to_string(), Postgres => "BOOLEAN".to_string(),
            Oracle => "NUMBER(1)".to_string(), Mysql => "TINYINT(1)".to_string(),
            Databricks => "BOOLEAN".to_string(),
        },

        // ── Date ─────────────────────────────────────────────────────────────
        "date" => "DATE".to_string(),

        // ── Time ─────────────────────────────────────────────────────────────
        "time" => match dialect {
            Postgres   => precision.map(|p| format!("TIME({p})")).unwrap_or("TIME".to_string()),
            Mssql      => "TIME(7)".to_string(),
            Oracle     => { let p = precision.unwrap_or(6); lossy!(&format!("INTERVAL DAY(1) TO SECOND({p})"), "Oracle has no TIME type") }
            Mysql      => precision.map(|p| format!("TIME({p})")).unwrap_or("TIME(6)".to_string()),
            Databricks => lossy!("STRING", "Databricks has no TIME type"),
        },
        "timetz" | "time with time zone" => match dialect {
            Postgres   => "TIMETZ".to_string(),
            Mssql      => lossy!("NVARCHAR(50)",  "MSSQL has no TIMETZ"),
            Oracle     => lossy!("VARCHAR2(50)",  "Oracle has no TIMETZ"),
            Mysql      => lossy!("VARCHAR(50)",   "MySQL has no TIMETZ"),
            Databricks => lossy!("STRING",        "Databricks has no TIMETZ"),
        },

        // ── Timestamp (no timezone) ──────────────────────────────────────────
        "timestamp" | "timestamp without time zone" => match dialect {
            Postgres   => precision.map(|p| format!("TIMESTAMP({p})")).unwrap_or("TIMESTAMP".to_string()),
            Mssql      => precision.map(|p| format!("DATETIME2({p})")).unwrap_or("DATETIME2".to_string()),
            Oracle     => precision.map(|p| format!("TIMESTAMP({p})")).unwrap_or("TIMESTAMP".to_string()),
            Mysql      => precision.map(|p| format!("DATETIME({p})")).unwrap_or("DATETIME(6)".to_string()),
            Databricks => "TIMESTAMP".to_string(),
        },
        "datetime" | "datetime2" | "smalldatetime" => match dialect {
            Postgres => "TIMESTAMP".to_string(),
            Mssql    => precision.map(|p| format!("DATETIME2({p})")).unwrap_or("DATETIME2".to_string()),
            Oracle   => "TIMESTAMP".to_string(),
            Mysql    => "DATETIME(6)".to_string(),
            Databricks => "TIMESTAMP".to_string(),
        },

        // ── Timestamp WITH timezone ──────────────────────────────────────────
        "timestamptz" | "timestamp with time zone" => match dialect {
            Postgres   => precision.map(|p| format!("TIMESTAMPTZ({p})")).unwrap_or("TIMESTAMPTZ".to_string()),
            Mssql      => precision.map(|p| format!("DATETIMEOFFSET({p})")).unwrap_or("DATETIMEOFFSET".to_string()),
            Oracle     => precision.map(|p| format!("TIMESTAMP({p}) WITH TIME ZONE")).unwrap_or("TIMESTAMP WITH TIME ZONE".to_string()),
            Mysql      => { let t = precision.map(|p| format!("DATETIME({p})")).unwrap_or("DATETIME(6)".to_string()); lossy!(&t, "MySQL has no TIMESTAMP WITH TIME ZONE") }
            Databricks => "TIMESTAMP".to_string(),
        },
        "timestamp with local time zone" => match dialect {
            Postgres   => "TIMESTAMPTZ".to_string(),
            Mssql      => "DATETIMEOFFSET".to_string(),
            Oracle     => precision.map(|p| format!("TIMESTAMP({p}) WITH LOCAL TIME ZONE")).unwrap_or("TIMESTAMP WITH LOCAL TIME ZONE".to_string()),
            Mysql      => lossy!("DATETIME(6)", "MySQL has no TIMESTAMP WITH LOCAL TIME ZONE"),
            Databricks => "TIMESTAMP".to_string(),
        },
        "datetimeoffset" => match dialect {
            Postgres   => "TIMESTAMPTZ".to_string(),
            Mssql      => precision.map(|p| format!("DATETIMEOFFSET({p})")).unwrap_or("DATETIMEOFFSET".to_string()),
            Oracle     => "TIMESTAMP WITH TIME ZONE".to_string(),
            Mysql      => lossy!("DATETIME(6)", "MySQL has no DATETIMEOFFSET"),
            Databricks => "TIMESTAMP".to_string(),
        },

        // ── VARCHAR with length ───────────────────────────────────────────────
        "varchar" | "character varying" => match dialect {
            Postgres   => varlen!("VARCHAR",    "TEXT",          65_535),
            Mssql      => varlen!("NVARCHAR",   "NVARCHAR(MAX)", 4_000),
            Oracle     => varlen!("VARCHAR2",   "CLOB",          4_000),
            Mysql      => varlen!("VARCHAR",    "LONGTEXT",      16_383),
            Databricks => "STRING".to_string(),
        },
        "nvarchar" => match dialect {
            Postgres   => varlen!("VARCHAR",    "TEXT",          65_535),
            Mssql      => varlen!("NVARCHAR",   "NVARCHAR(MAX)", 4_000),
            Oracle     => varlen!("NVARCHAR2",  "NCLOB",         2_000),
            Mysql      => varlen!("VARCHAR",    "LONGTEXT",      16_383),
            Databricks => "STRING".to_string(),
        },
        "varchar2" => match dialect {
            Postgres   => varlen!("VARCHAR",    "TEXT",          65_535),
            Mssql      => varlen!("NVARCHAR",   "NVARCHAR(MAX)", 4_000),
            Oracle     => varlen!("VARCHAR2",   "CLOB",          4_000),
            Mysql      => varlen!("VARCHAR",    "LONGTEXT",      16_383),
            Databricks => "STRING".to_string(),
        },
        "nvarchar2" => match dialect {
            Postgres   => varlen!("VARCHAR",    "TEXT",          65_535),
            Mssql      => varlen!("NVARCHAR",   "NVARCHAR(MAX)", 4_000),
            Oracle     => varlen!("NVARCHAR2",  "NCLOB",         2_000),
            Mysql      => varlen!("VARCHAR",    "LONGTEXT",      16_383),
            Databricks => "STRING".to_string(),
        },

        // ── CHAR with length ─────────────────────────────────────────────────
        "char" | "bpchar" | "character" => match dialect {
            Postgres   => match length { Some(n) => format!("CHAR({n})"),  None => "TEXT".to_string() },
            Mssql      => match length { Some(n) => format!("NCHAR({n})"), None => "NVARCHAR(MAX)".to_string() },
            Oracle     => match length { Some(n) => format!("CHAR({n})"),  None => "CLOB".to_string() },
            Mysql      => match length { Some(n) => format!("CHAR({n})"),  None => "LONGTEXT".to_string() },
            Databricks => "STRING".to_string(),
        },
        "nchar" => match dialect {
            Postgres   => match length { Some(n) => format!("CHAR({n})"),  None => "TEXT".to_string() },
            Mssql      => match length { Some(n) => format!("NCHAR({n})"), None => "NVARCHAR(MAX)".to_string() },
            Oracle     => match length { Some(n) => format!("NCHAR({n})"), None => "NCLOB".to_string() },
            Mysql      => match length { Some(n) => format!("NCHAR({n})"), None => "LONGTEXT".to_string() },
            Databricks => "STRING".to_string(),
        },

        // ── Large text ───────────────────────────────────────────────────────
        "text" | "ntext" | "clob" | "nclob" | "long"
        | "mediumtext" | "longtext" | "tinytext"
                                          => pick!("TEXT", "NVARCHAR(MAX)", "CLOB", "LONGTEXT", "STRING"),

        // ── Binary ───────────────────────────────────────────────────────────
        "bytea" | "blob" | "longblob" | "tinyblob" | "mediumblob"
        | "image" | "long raw"            => pick!("BYTEA", "VARBINARY(MAX)", "BLOB", "LONGBLOB", "BINARY"),
        "binary" => match dialect {
            Mssql => match length { Some(-1) | None => "VARBINARY(MAX)".to_string(), Some(n) => format!("BINARY({n})") },
            _     => pick!("BYTEA", "BINARY", "RAW(2000)", "BINARY", "BINARY"),
        },
        "varbinary" | "raw" => match dialect {
            Mssql  => match length { Some(-1) | None => "VARBINARY(MAX)".to_string(), Some(n) => format!("VARBINARY({n})") },
            Oracle => match length { Some(-1) | None => "BLOB".to_string(), Some(n) => format!("RAW({n})") },
            _      => pick!("BYTEA", "VARBINARY(MAX)", "BLOB", "VARBINARY(MAX)", "BINARY"),
        },

        // ── UUID / GUID ──────────────────────────────────────────────────────
        "uuid" | "uniqueidentifier" => match dialect {
            Postgres   => "UUID".to_string(),
            Mssql      => "UNIQUEIDENTIFIER".to_string(),
            Oracle     => "VARCHAR2(36)".to_string(),
            Mysql      => "VARCHAR(36)".to_string(),
            Databricks => "STRING".to_string(),
        },

        // ── JSON ─────────────────────────────────────────────────────────────
        "json"  => pick!("JSON",  "NVARCHAR(MAX)", "CLOB", "JSON", "STRING"),
        "jsonb" => match dialect {
            Postgres   => "JSONB".to_string(),
            Mssql      => lossy!("NVARCHAR(MAX)", "MSSQL has no JSONB"),
            Oracle     => lossy!("CLOB",          "Oracle has no JSONB"),
            Mysql      => lossy!("JSON",           "MySQL JSON ≠ PostgreSQL JSONB"),
            Databricks => lossy!("STRING",         "Databricks has no JSONB"),
        },

        // ── XML ──────────────────────────────────────────────────────────────
        "xml" => match dialect {
            Postgres   => "TEXT".to_string(),
            Mssql      => "XML".to_string(),
            Oracle     => lossy!("XMLTYPE", "XMLTYPE DDL requires careful schema setup"),
            Mysql      => lossy!("LONGTEXT",  "MySQL has no XML type"),
            Databricks => lossy!("STRING",    "Databricks has no XML type"),
        },
        "xmltype" => match dialect {
            Postgres   => lossy!("TEXT",       "PostgreSQL has no XMLTYPE"),
            Mssql      => lossy!("XML",        "MSSQL XML ≠ Oracle XMLTYPE"),
            Oracle     => "XMLTYPE".to_string(),
            Mysql      => lossy!("LONGTEXT",   "MySQL has no XMLTYPE"),
            Databricks => lossy!("STRING",     "Databricks has no XMLTYPE"),
        },

        // ── Postgres network types ───────────────────────────────────────────
        "inet" | "cidr" => match dialect {
            Postgres   => src.to_uppercase(),
            Mssql      => lossy!("NVARCHAR(50)",  "MSSQL has no INET/CIDR type"),
            Oracle     => lossy!("VARCHAR2(50)",  "Oracle has no INET/CIDR type"),
            Mysql      => lossy!("VARCHAR(50)",   "MySQL has no INET/CIDR type"),
            Databricks => lossy!("STRING",        "Databricks has no INET/CIDR type"),
        },
        "macaddr" | "macaddr8" => match dialect {
            Postgres   => src.to_uppercase(),
            Mssql      => lossy!("NVARCHAR(25)",  "MSSQL has no MACADDR type"),
            Oracle     => lossy!("VARCHAR2(25)",  "Oracle has no MACADDR type"),
            Mysql      => lossy!("VARCHAR(25)",   "MySQL has no MACADDR type"),
            Databricks => lossy!("STRING",        "Databricks has no MACADDR type"),
        },

        // ── Unknown ──────────────────────────────────────────────────────────
        _ => return None,
    })
}