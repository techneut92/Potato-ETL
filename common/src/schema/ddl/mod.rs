//! DDL generation and SQL dialect mapping.
//!
//! [`generate_ddl`] converts an Arrow schema (enriched with `etl.*` metadata
//! by [`apply_database_columns`][super::apply_database_columns]) into `CREATE TABLE`
//! and post-create statements for the five supported SQL dialects.
//!
//! [`SqlDialect`] is auto-detected from connection URL prefixes via
//! [`SqlDialect::from_conn_str`].

mod dialect;
mod resolve;
mod helpers;

use arrow::datatypes::Schema as ArrowSchema;

use super::constants::{
    META_CHECK_EXPR, META_DEFAULT_EXPR, META_DESCRIPTION,
    META_FOREIGN_KEY, META_INDEX, META_NULLABLE,
    META_ON_UPDATE_EXPR, META_PRIMARY_KEY, META_UNIQUE,
    META_ENUM_VALUES,
};
use super::field::ForeignKey;

// ── Public re-exports ─────────────────────────────────────────────────────────

pub use dialect::arrow_type_to_sql_dialect;
pub use resolve::{
    resolve_sql_type, logical_type_to_target_sql, source_type_to_target_sql,
};
pub use helpers::make_create_index;
pub use helpers::normalise_now_expr;

use helpers::{
    make_on_update_trigger,
    scd2_system_column_defs, mssql_create_if_not_exists, oracle_create_if_not_exists,
};
use resolve::enum_type_name;

// ── SqlDialect ────────────────────────────────────────────────────────────────

use serde::{Deserialize, Serialize};

/// SQL dialect used by DDL generation.
///
/// Auto-detected from connection URLs via [`SqlDialect::from_conn_str`].
/// Supply explicitly when calling [`generate_ddl`] from code that does not
/// have a connection string at hand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqlDialect {
    /// PostgreSQL (the default).
    #[default]
    Postgres,
    /// Microsoft SQL Server (tiberius / TDS).
    Mssql,
    /// Oracle Database.
    Oracle,
    /// MySQL, Amazon Aurora (MySQL), and MariaDB.
    Mysql,
    /// Databricks SQL warehouse (Unity Catalog / Delta Lake).
    Databricks,
}

impl SqlDialect {
    /// Auto-detects the dialect from a connection URL prefix.
    pub fn from_conn_str(s: &str) -> Self {
        if s.starts_with("postgresql://") || s.starts_with("postgres://") {
            Self::Postgres
        } else if s.starts_with("mssql://") {
            Self::Mssql
        } else if s.starts_with("oracle://") {
            Self::Oracle
        } else if s.starts_with("mysql://")
            || s.starts_with("aurora://")
            || s.starts_with("mariadb://")
        {
            Self::Mysql
        } else if s.starts_with("databricks://") {
            Self::Databricks
        } else {
            Self::Postgres
        }
    }

    /// Returns the dialect-specific quoting character for identifiers.
    pub fn quote_char(self) -> char {
        match self {
            Self::Mssql => ']',
            Self::Mysql => '`',
            _           => '"',
        }
    }

    /// Quotes an identifier for this dialect.
    ///
    /// For Oracle, simple identifiers (letters, digits, `_`, `#`, `$`) that
    /// are **not** reserved words are returned **unquoted** so that Oracle
    /// automatically uppercases them (standard convention).  Complex or
    /// reserved identifiers are double-quoted to preserve exact spelling.
    ///
    /// All other dialects always quote (Postgres `"`, MSSQL `[]`, MySQL `` ` ``).
    pub fn quote(self, ident: &str) -> String {
        match self {
            Self::Mssql => format!("[{}]", ident.replace(']', "]]")),
            Self::Mysql => format!("`{}`", ident.replace('`', "``")),
            Self::Oracle => oracle_smart_quote(ident),
            _           => format!("\"{}\"", ident.replace('"', "\"\"")),
        }
    }

    /// Always-quote variant for contexts where quoting is mandatory
    /// regardless of dialect (e.g. inside dynamic SQL strings).
    pub fn force_quote(self, ident: &str) -> String {
        match self {
            Self::Mssql => format!("[{}]", ident.replace(']', "]]")),
            Self::Mysql => format!("`{}`", ident.replace('`', "``")),
            _           => format!("\"{}\"", ident.replace('"', "\"\"")),
        }
    }

    /// Returns `true` if `CREATE TABLE IF NOT EXISTS` is natively supported.
    pub fn supports_create_if_not_exists(self) -> bool {
        !matches!(self, Self::Mssql | Self::Oracle)
    }

    /// Returns `true` if `CREATE INDEX IF NOT EXISTS` is supported.
    pub fn supports_index_if_not_exists(self) -> bool {
        matches!(self, Self::Postgres | Self::Databricks)
    }

    /// Generates a `DROP TABLE IF EXISTS` (or dialect-equivalent) statement.
    pub fn drop_table_if_exists(self, table: &str, schema: Option<&str>) -> String {
        let qualified = match schema {
            Some(s) => format!("{}.{}", self.quote(s), self.quote(table)),
            None    => self.quote(table),
        };
        match self {
            Self::Oracle => format!(
                "BEGIN\n  EXECUTE IMMEDIATE 'DROP TABLE {qualified}';\n  \
                 EXCEPTION WHEN OTHERS THEN \
                 IF SQLCODE = -942 THEN NULL; ELSE RAISE; END IF;\nEND;"
            ),
            _ => format!("DROP TABLE IF EXISTS {qualified};"),
        }
    }
}

// ── DdlOptions ────────────────────────────────────────────────────────────────

/// Optional hints that influence DDL generation.
#[derive(Debug, Clone, Copy, Default)]
pub struct DdlOptions {
    /// PostgreSQL major version (e.g. `16` for PG 16.x).
    pub pg_major_version: Option<u32>,
    /// Oracle major version (e.g. `21` for 21c). Enables version-aware type
    /// upgrades: JSON column type (≥ 21c), native BOOLEAN (≥ 23ai).
    pub ora_major_version: Option<u32>,
}

// ── DdlStatements ─────────────────────────────────────────────────────────────

/// The result of [`generate_ddl`]: a `CREATE TABLE` and follow-up statements.
#[derive(Debug, Clone)]
pub struct DdlStatements {
    /// Statements to execute **before** the `CREATE TABLE` (e.g. `CREATE TYPE` for Postgres enums).
    pub pre_create: Vec<String>,
    /// The `CREATE TABLE (IF NOT EXISTS)` statement (or MSSQL/Oracle equivalent).
    pub create_table: String,
    /// Statements to execute after the table exists.
    pub post_create: Vec<String>,
}

/// Generates only the post-create statements for an Arrow schema with `etl.*` metadata.
pub fn generate_post_create(
    table:     &str,
    db_schema: Option<&str>,
    ar_schema: &ArrowSchema,
    dialect:   SqlDialect,
    options:   DdlOptions,
) -> Vec<String> {
    generate_ddl(table, db_schema, ar_schema, dialect, None, options).post_create
}

// ── Scd2DdlInfo ───────────────────────────────────────────────────────────────

/// SCD2-specific information required by [`generate_ddl`] to prepend system columns.
pub struct Scd2DdlInfo<'a> {
    pub col_names: &'a crate::db::Scd2ColumnNames,
    pub key_col: &'a str,
}

// ── generate_ddl ──────────────────────────────────────────────────────────────

/// Generates `CREATE TABLE` and post-create statements from an Arrow schema.
pub fn generate_ddl(
    table:     &str,
    db_schema: Option<&str>,
    ar_schema: &ArrowSchema,
    dialect:   SqlDialect,
    scd2:      Option<&Scd2DdlInfo>,
    options:   DdlOptions,
) -> DdlStatements {
    let q = |ident: &str| dialect.quote(ident);

    let qualified = match db_schema {
        Some(s) => format!("{}.{}", q(s), q(table)),
        None    => q(table),
    };

    let mut col_defs:       Vec<String> = Vec::new();
    let mut pk_cols:        Vec<String> = Vec::new();
    let mut unique_cols:    Vec<String> = Vec::new();
    let mut fk_constraints: Vec<String> = Vec::new();
    let mut post_create:    Vec<String> = Vec::new();
    let mut pre_create:     Vec<String> = Vec::new();

    let pk_col_names: Vec<&str> = ar_schema.fields().iter()
        .filter(|f| f.metadata().get(META_PRIMARY_KEY).map(|v| v == "true").unwrap_or(false))
        .map(|f| f.name().as_str())
        .collect();

    // ── SCD2 system columns (prepended) ────────────────────────────────────
    if let Some(scd2_info) = scd2 {
        let sys_cols = scd2_system_column_defs(scd2_info.col_names, dialect);
        col_defs.extend(sys_cols);
    }

    // ── User-defined data columns ─────────────────────────────────────────────
    for field in ar_schema.fields() {
        let meta = field.metadata();

        if let Some(scd2_info) = scd2 {
            let cn = scd2_info.col_names;
            let sys_names = [
                cn.scd_id.as_str(), cn.valid_from.as_str(),
                cn.valid_to.as_str(), cn.is_current.as_str(),
            ];
            if sys_names.contains(&field.name().as_str()) { continue; }
        }

        let nullable = meta.get(META_NULLABLE)
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(field.is_nullable());

        let sql_type = resolve_sql_type(field, dialect);

        // ── Oracle version-aware type upgrades ──────────────────────────────
        let sql_type = if dialect == SqlDialect::Oracle {
            if let Some(ora_major) = options.ora_major_version {
                let mut st = sql_type;
                if let Some(lt) = meta.get(super::constants::META_LOGICAL_TYPE) {
                    let u = lt.to_ascii_uppercase();
                    if u == "JSON" && ora_major >= 21 {
                        let s = st.trim().to_ascii_uppercase();
                        if s == "CLOB" || s == "NCLOB" { st = "JSON".to_string(); }
                    }
                    if u == "BOOLEAN" && ora_major >= 23 {
                        if st.trim().to_ascii_uppercase() == "NUMBER(1)" { st = "BOOLEAN".to_string(); }
                    }
                }
                st
            } else {
                sql_type
            }
        } else {
            sql_type
        };

        let null_clause    = if nullable { "" } else { " NOT NULL" };
        let default_clause = meta.get(META_DEFAULT_EXPR)
            .map(|d| {
                let normalised = normalise_now_expr(d, dialect);
                format!(" DEFAULT {normalised}")
            })
            .unwrap_or_default();

        let check_inline = meta.get(META_CHECK_EXPR)
            .map(|c| format!(" CHECK ({c})"))
            .unwrap_or_default();

        let on_update_inline = match dialect {
            SqlDialect::Mysql => meta.get(META_ON_UPDATE_EXPR)
                .map(|expr| format!(" ON UPDATE {}", normalise_now_expr(expr, dialect)))
                .unwrap_or_default(),
            _ => String::new(),
        };

        col_defs.push(format!(
            "    {} {}{}{}{}{}", q(field.name()), sql_type, null_clause, default_clause, on_update_inline, check_inline
        ));

        if meta.get(META_PRIMARY_KEY).map(|v| v == "true").unwrap_or(false) {
            pk_cols.push(q(field.name()));
        }

        if meta.get(META_UNIQUE).map(|v| v == "true").unwrap_or(false)
            && !meta.get(META_PRIMARY_KEY).map(|v| v == "true").unwrap_or(false)
        {
            unique_cols.push(q(field.name()));
        }

        if let Some(fk_str) = meta.get(META_FOREIGN_KEY) {
            if let Ok(fk) = serde_json::from_str::<ForeignKey>(fk_str) {
                let ref_table = match &fk.schema {
                    Some(s) => format!("{}.{}", q(s), q(&fk.table)),
                    None    => q(&fk.table),
                };
                fk_constraints.push(format!(
                    "    FOREIGN KEY ({}) REFERENCES {}({})",
                    q(field.name()), ref_table, q(&fk.column)
                ));
            }
        }

        if meta.get(META_INDEX).map(|v| v == "true").unwrap_or(false) {
            let idx_name = format!("idx_{table}_{}", field.name());
            let stmt = make_create_index(&idx_name, &qualified, &[field.name()], false, dialect);
            post_create.push(stmt);
        }

        if !matches!(dialect, SqlDialect::Mysql) {
            if let Some(on_update_expr) = meta.get(META_ON_UPDATE_EXPR) {
                let trigger_stmts = make_on_update_trigger(
                    table, db_schema, field.name(), on_update_expr, dialect,
                    &pk_col_names, options,
                );
                post_create.extend(trigger_stmts);
            }
        }

        if let Some(desc) = meta.get(META_DESCRIPTION) {
            let comment_stmt = match dialect {
                SqlDialect::Postgres | SqlDialect::Oracle => Some(format!(
                    "COMMENT ON COLUMN {}.{} IS '{}';",
                    qualified, q(field.name()), desc.replace('\'', "''")
                )),
                _ => None,
            };
            if let Some(s) = comment_stmt {
                post_create.push(s);
            }
        }

        if let Some(enum_values_json) = meta.get(META_ENUM_VALUES) {
            if let Ok(values) = serde_json::from_str::<Vec<String>>(enum_values_json) {
                if !values.is_empty() {
                    let escaped: Vec<String> = values.iter()
                        .map(|v| format!("'{}'", v.replace('\'', "''")))
                        .collect();

                    match dialect {
                        SqlDialect::Postgres => {
                            // CREATE TYPE idempotently (DO $$ ... EXCEPTION $$).
                            let type_name = enum_type_name(field.name());
                            pre_create.push(format!(
                                "DO $$ BEGIN\n  \
                                   CREATE TYPE \"{}\" AS ENUM ({});\n\
                                 EXCEPTION WHEN duplicate_object THEN NULL;\n\
                                 END $$;",
                                type_name, escaped.join(", ")
                            ));
                        }
                        SqlDialect::Mysql => {
                            // MySQL ENUM is inline — already handled by resolve_sql_type.
                        }
                        _ => {
                            // MSSQL / Oracle / Databricks — add CHECK constraint
                            // as post-create (ALTER TABLE avoids bloating the col def).
                            let check_vals = escaped.join(", ");
                            post_create.push(format!(
                                "ALTER TABLE {qualified} ADD CHECK ({} IN ({check_vals}));",
                                q(field.name())
                            ));
                        }
                    }
                }
            }
        }
    }

    // ── Table-level constraints ──────────────────────────────────────────────
    if !pk_cols.is_empty() {
        col_defs.push(format!("    PRIMARY KEY ({})", pk_cols.join(", ")));
    }
    for uc in &unique_cols {
        col_defs.push(format!("    UNIQUE ({uc})"));
    }
    col_defs.extend(fk_constraints);

    // ── SCD2 key + is_current index ───────────────────────────────────────────
    if let Some(scd2_info) = scd2 {
        let cn = scd2_info.col_names;
        let idx_name = format!("idx_{table}_{}_current", scd2_info.key_col);
        let idx_stmt = make_create_index(
            &idx_name, &qualified,
            &[scd2_info.key_col, cn.is_current.as_str()],
            false, dialect,
        );
        post_create.insert(0, idx_stmt);
    }

    // ── Assemble CREATE TABLE ─────────────────────────────────────────────────
    let create_table = match dialect {
        SqlDialect::Mssql  => mssql_create_if_not_exists(table, db_schema, &col_defs),
        SqlDialect::Oracle => oracle_create_if_not_exists(table, db_schema, &col_defs),
        _ => format!(
            "CREATE TABLE IF NOT EXISTS {qualified} (\n{}\n);\n",
            col_defs.join(",\n")
        ),
    };

    DdlStatements { pre_create, create_table, post_create }
}

// ── generate_ddl_with_schema ──────────────────────────────────────────────────

/// Extended DDL generation that also emits named indexes and constraints from
/// a [`DatabaseSchemaConfig`].
pub fn generate_ddl_with_schema(
    table:     &str,
    db_schema: Option<&str>,
    ar_schema: &ArrowSchema,
    dialect:   SqlDialect,
    scd2:      Option<&Scd2DdlInfo>,
    db_config: Option<&crate::config::DatabaseSchemaConfig>,
    options:   DdlOptions,
) -> DdlStatements {
    let mut stmts = generate_ddl(table, db_schema, ar_schema, dialect, scd2, options);

    let Some(db) = db_config else { return stmts; };

    let q = |ident: &str| dialect.quote(ident);
    let qualified = match db_schema {
        Some(s) => format!("{}.{}", q(s), q(table)),
        None    => q(table),
    };

    for (idx_name, idx_def) in &db.indexes {
        if idx_def.primary {
            let already_has_pk = stmts.create_table.contains("PRIMARY KEY");
            if !already_has_pk {
                let cols_quoted: Vec<String> = idx_def.columns.iter().map(|c| q(c)).collect();
                let stmt = format!(
                    "ALTER TABLE {qualified} ADD CONSTRAINT {} PRIMARY KEY ({});",
                    q(idx_name), cols_quoted.join(", "),
                );
                stmts.post_create.push(stmt);
            }
        } else {
            let stmt = make_create_index(
                idx_name, &qualified,
                &idx_def.columns.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                idx_def.unique, dialect,
            );
            stmts.post_create.push(stmt);
        }
    }

    for (constraint_name, constraint_def) in &db.constraints {
        if let Some(check_expr) = &constraint_def.check {
            let stmt = format!(
                "ALTER TABLE {qualified} ADD CONSTRAINT {} CHECK ({});",
                q(constraint_name), check_expr,
            );
            stmts.post_create.push(stmt);
        }
    }

    stmts
}

// ── oracle_smart_quote ────────────────────────────────────────────────────────

/// Oracle-specific smart quoting for identifiers.
///
/// Simple identifiers (letters, digits, `_`, `#`, `$`) that are **not**
/// reserved words are returned **unquoted** so that Oracle automatically
/// uppercases them (standard convention).  Complex or reserved identifiers
/// are double-quoted to preserve exact spelling.
fn oracle_smart_quote(ident: &str) -> String {
    if ident.is_empty() {
        return "\"\"".to_string();
    }

    // First char must be alphabetic or underscore.
    let first = ident.chars().next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return format!("\"{}\"", ident.replace('"', "\"\""));
    }

    // Remaining chars: alphanumeric, underscore, `#`, or `$`.
    if !ident[1..].chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '#' || c == '$') {
        return format!("\"{}\"", ident.replace('"', "\"\""));
    }

    // Check against Oracle reserved words (21c).
    // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Oracle-Reserved-Words.html
    if oracle_is_reserved(ident) {
        return format!("\"{}\"", ident.replace('"', "\"\""));
    }

    // Simple identifier — emit unquoted.
    ident.to_string()
}

/// Returns `true` if the identifier matches an Oracle reserved word (case-insensitive).
fn oracle_is_reserved(ident: &str) -> bool {
    // Sorted for readability; uses `matches!` for O(1)-ish branch table.
    matches!(
        ident.to_ascii_uppercase().as_str(),
        "ACCESS" | "ADD" | "ALL" | "ALTER" | "AND" | "ANY" | "AS" | "ASC"
        | "AUDIT" | "BETWEEN" | "BY" | "CHAR" | "CHECK" | "CLUSTER"
        | "COLUMN" | "COMMENT" | "COMPRESS" | "CONNECT" | "CREATE"
        | "CURRENT" | "DATE" | "DECIMAL" | "DEFAULT" | "DELETE" | "DESC"
        | "DISTINCT" | "DROP" | "ELSE" | "EXCLUSIVE" | "EXISTS" | "FILE"
        | "FLOAT" | "FOR" | "FROM" | "GRANT" | "GROUP" | "HAVING"
        | "IDENTIFIED" | "IMMEDIATE" | "IN" | "INCREMENT" | "INDEX"
        | "INITIAL" | "INSERT" | "INTEGER" | "INTERSECT" | "INTO" | "IS"
        | "LEVEL" | "LIKE" | "LOCK" | "LONG" | "MAXEXTENTS" | "MINUS"
        | "MLSLABEL" | "MODE" | "MODIFY" | "NOAUDIT" | "NOCOMPRESS"
        | "NOT" | "NOWAIT" | "NULL" | "NUMBER" | "OF" | "OFFLINE"
        | "ON" | "ONLINE" | "OPTION" | "OR" | "ORDER" | "PCTFREE"
        | "PRIOR" | "PUBLIC" | "RAW" | "RENAME" | "RESOURCE" | "REVOKE"
        | "ROW" | "ROWID" | "ROWNUM" | "ROWS" | "SELECT" | "SESSION"
        | "SET" | "SHARE" | "SIZE" | "SMALLINT" | "START" | "SUCCESSFUL"
        | "SYNONYM" | "SYSDATE" | "TABLE" | "THEN" | "TO" | "TRIGGER"
        | "UID" | "UNION" | "UNIQUE" | "UPDATE" | "USER" | "VALIDATE"
        | "VALUES" | "VARCHAR" | "VARCHAR2" | "VIEW" | "WHENEVER"
        | "WHERE" | "WITH"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_simple_idents_not_quoted() {
        let d = SqlDialect::Oracle;
        assert_eq!(d.quote("my_table"), "my_table");
        assert_eq!(d.quote("employee_id"), "employee_id");
        assert_eq!(d.quote("COL1"), "COL1");
        assert_eq!(d.quote("_private"), "_private");
        assert_eq!(d.quote("SYS$SESSION"), "SYS$SESSION");
        assert_eq!(d.quote("temp#data"), "temp#data");
    }

    #[test]
    fn oracle_reserved_words_quoted() {
        let d = SqlDialect::Oracle;
        assert_eq!(d.quote("table"), "\"table\"");
        assert_eq!(d.quote("select"), "\"select\"");
        assert_eq!(d.quote("DATE"), "\"DATE\"");
        assert_eq!(d.quote("user"), "\"user\"");
        assert_eq!(d.quote("order"), "\"order\"");
        assert_eq!(d.quote("NULL"), "\"NULL\"");
    }

    #[test]
    fn oracle_special_chars_quoted() {
        let d = SqlDialect::Oracle;
        assert_eq!(d.quote("my table"), "\"my table\"");
        assert_eq!(d.quote("col.name"), "\"col.name\"");
        assert_eq!(d.quote("1start"), "\"1start\"");
        assert_eq!(d.quote(""), "\"\"");
    }

    #[test]
    fn oracle_force_quote_always_quotes() {
        let d = SqlDialect::Oracle;
        // force_quote should always quote, even for simple identifiers.
        assert_eq!(d.force_quote("my_table"), "\"my_table\"");
        assert_eq!(d.force_quote("employee_id"), "\"employee_id\"");
    }

    #[test]
    fn postgres_always_quotes() {
        let d = SqlDialect::Postgres;
        assert_eq!(d.quote("my_table"), "\"my_table\"");
        assert_eq!(d.quote("employee_id"), "\"employee_id\"");
    }

    #[test]
    fn mssql_always_brackets() {
        let d = SqlDialect::Mssql;
        assert_eq!(d.quote("my_table"), "[my_table]");
    }

    #[test]
    fn mysql_always_backticks() {
        let d = SqlDialect::Mysql;
        assert_eq!(d.quote("my_table"), "`my_table`");
    }
}