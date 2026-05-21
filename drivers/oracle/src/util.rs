//! Oracle connection params, SQL literal helpers, and shared query builder.

use potato_etl_common::config::pct_decode;
use potato_etl_common::schema::ddl::SqlDialect;
use std::sync::OnceLock;

static ORACLE_DRIVER_ERROR: OnceLock<String> = OnceLock::new();

/// Formats an Oracle identifier, quoting only when necessary.
///
/// Delegates to [`SqlDialect::Oracle.quote()`][SqlDialect::quote] which
/// implements smart quoting: simple identifiers are emitted **unquoted**
/// so Oracle uppercases them naturally, while complex or reserved
/// identifiers are double-quoted to preserve exact spelling.
#[inline]
pub fn oracle_ident(name: &str) -> String {
    SqlDialect::Oracle.quote(name)
}

/// Formats a qualified `schema.table` reference for Oracle.
///
/// An empty `schema_name` falls back to an unqualified `"TABLE"` reference —
/// Oracle then resolves the table against the connecting user's schema.
/// Emitting `""."TABLE"` would trigger `ORA-01741: illegal zero-length identifier`.
pub fn oracle_qualified_table(schema_name: &str, table: &str) -> String {
    if schema_name.is_empty() {
        oracle_ident(table)
    } else {
        format!("{}.{}", oracle_ident(schema_name), oracle_ident(table))
    }
}

#[derive(Clone)]
pub struct OracleConn {
    pub user:    String,
    pub pass:    String,
    pub connect: String,
}

impl OracleConn {
    pub fn parse(conn_str: &str) -> anyhow::Result<Self> {
        let s = conn_str.strip_prefix("oracle://")
            .ok_or_else(|| anyhow::anyhow!("Oracle connection string must start with 'oracle://'. Got: {conn_str}"))?;
        let (creds, rest) = s.split_once('@')
            .ok_or_else(|| anyhow::anyhow!("Missing '@' in Oracle connection string"))?;
        let (raw_user, raw_pass) = creds.split_once(':')
            .ok_or_else(|| anyhow::anyhow!("Missing ':' between user and password in Oracle connection string"))?;
        let user = pct_decode(raw_user)?;
        let pass = pct_decode(raw_pass)?;
        let connect = if rest.starts_with('(') {
            rest.to_string()
        } else if rest.contains('/') {
            format!("//{rest}")
        } else {
            rest.to_string()
        };
        Ok(Self { user, pass, connect })
    }

    pub fn open(&self) -> anyhow::Result<oracle::Connection> {
        if let Some(cached) = ORACLE_DRIVER_ERROR.get() {
            anyhow::bail!("{cached}");
        }
        oracle::Connection::connect(&self.user, &self.pass, &self.connect)
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("DPI-1047") {
                    let err_msg = ORACLE_DRIVER_ERROR.get_or_init(|| {
                        "Oracle Client library not found (DPI-1047). Install Oracle Instant Client.".to_string()
                    });
                    anyhow::anyhow!("{err_msg}")
                } else if msg.contains("DPI-1072") {
                    let err_msg = ORACLE_DRIVER_ERROR.get_or_init(|| {
                        format!("Oracle Client architecture mismatch (DPI-1072): {e}")
                    });
                    anyhow::anyhow!("{err_msg}")
                } else {
                    anyhow::anyhow!("Oracle connection failed: {e}")
                }
            })
    }
}

pub fn to_oracle_literal(val: &Option<String>) -> String {
    match val {
        None    => "NULL".to_string(),
        Some(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

pub fn oracle_build_query(table: &str, schema: &str, custom_q: Option<&str>, cursor_col: Option<&str>, last_cursor: Option<&str>, batch_size: usize) -> String {
    let base = match custom_q {
        Some(q) => format!("SELECT * FROM ({q}) etl_q"),
        None    => format!("SELECT * FROM {}", oracle_qualified_table(schema, table)),
    };
    match (cursor_col, last_cursor) {
        (Some(col), Some(val)) => {
            let lit = to_oracle_literal(&Some(val.to_string()));
            let col_ref = oracle_ident(col);
            format!("{base} WHERE {col_ref} > {lit} ORDER BY {col_ref} ASC FETCH FIRST {batch_size} ROWS ONLY")
        }
        (Some(col), None) => {
            let col_ref = oracle_ident(col);
            format!("{base} ORDER BY {col_ref} ASC FETCH FIRST {batch_size} ROWS ONLY")
        }
        _ => base,
    }
}

pub fn query_oracle_major_version(conn: &oracle::Connection) -> u32 {
    let version_str: Option<String> = conn
        .query_row_as::<String>("SELECT VERSION_FULL FROM V$INSTANCE", &[]).ok()
        .or_else(|| conn.query_row_as::<String>("SELECT VERSION FROM V$INSTANCE", &[]).ok());
    match version_str {
        Some(v) => v.split('.').next().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0),
        None => 0,
    }
}

pub fn oracle_introspect_table_columns(
    oci_conn: &oracle::Connection,
    schema_name: &str,
    table_name: &str,
) -> anyhow::Result<Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>> {
    use potato_etl_common::db::common::alignment::TargetColumn;
    let sql = "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT \
               FROM ALL_TAB_COLUMNS \
               WHERE OWNER = :1 AND TABLE_NAME = :2 \
               ORDER BY COLUMN_ID";
    let rows = oci_conn.query_as::<(String, String, String, Option<String>)>(
        sql, &[&schema_name.to_ascii_uppercase(), &table_name.to_ascii_uppercase()],
    )?;
    let mut cols: Vec<TargetColumn> = Vec::new();
    for row_result in rows {
        let (name, data_type, nullable, default) = row_result?;
        cols.push(TargetColumn {
            name, data_type: data_type.to_ascii_lowercase(), nullable: nullable == "Y", has_default: default.is_some(),
        });
    }
    if cols.is_empty() { Ok(None) } else { Ok(Some(cols)) }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_idents_not_quoted() {
        assert_eq!(oracle_ident("my_table"), "my_table");
        assert_eq!(oracle_ident("employee_id"), "employee_id");
        assert_eq!(oracle_ident("COL1"), "COL1");
        assert_eq!(oracle_ident("_private"), "_private");
        assert_eq!(oracle_ident("SYS$SESSION"), "SYS$SESSION");
        assert_eq!(oracle_ident("temp#data"), "temp#data");
    }

    #[test]
    fn reserved_words_quoted() {
        assert_eq!(oracle_ident("table"), "\"table\"");
        assert_eq!(oracle_ident("select"), "\"select\"");
        assert_eq!(oracle_ident("DATE"), "\"DATE\"");
        assert_eq!(oracle_ident("user"), "\"user\"");
        assert_eq!(oracle_ident("order"), "\"order\"");
    }

    #[test]
    fn special_chars_quoted() {
        assert_eq!(oracle_ident("my table"), "\"my table\"");
        assert_eq!(oracle_ident("col.name"), "\"col.name\"");
        assert_eq!(oracle_ident("1start"), "\"1start\"");
        assert_eq!(oracle_ident(""), "\"\"");
    }

    #[test]
    fn qualified_table_simple() {
        assert_eq!(
            oracle_qualified_table("HR", "employees"),
            "HR.employees"
        );
    }

    #[test]
    fn qualified_table_reserved() {
        assert_eq!(
            oracle_qualified_table("public", "order"),
            "\"public\".\"order\""
        );
    }

    #[test]
    fn qualified_table_empty_schema_unqualified() {
        // Empty schema → unqualified reference (Oracle resolves to user's schema).
        // `""."TABLE"` would raise ORA-01741.
        assert_eq!(
            oracle_qualified_table("", "EXT_GENESYS_AUDITS"),
            "EXT_GENESYS_AUDITS"
        );
    }
}