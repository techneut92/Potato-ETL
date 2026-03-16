//! DDL helper functions: index creation, trigger generation, SCD2 columns,
//! dialect-specific CREATE TABLE wrappers, and expression normalisation.

use super::{SqlDialect, DdlOptions};

// ── CREATE INDEX helper ───────────────────────────────────────────────────────

pub fn make_create_index(
    index_name:      &str,
    qualified_table: &str,
    cols:            &[&str],
    unique:          bool,
    dialect:         SqlDialect,
) -> String {
    let q          = |s: &str| dialect.quote(s);
    let unique_kw  = if unique { "UNIQUE " } else { "" };
    let cols_sql   = cols.iter().map(|c| q(c)).collect::<Vec<_>>().join(", ");

    match dialect {
        SqlDialect::Postgres | SqlDialect::Databricks => format!(
            "CREATE {unique_kw}INDEX IF NOT EXISTS {} ON {qualified_table} ({cols_sql});",
            q(index_name)
        ),
        SqlDialect::Mssql => format!(
            "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{index_name}') \
             CREATE {unique_kw}INDEX [{index_name}] ON {qualified_table} ({cols_sql});"
        ),
        SqlDialect::Oracle => format!(
            "BEGIN\n  EXECUTE IMMEDIATE \
             'CREATE {unique_kw}INDEX \"{index_name}\" ON {qualified_table} ({cols_sql})';\n\
             EXCEPTION WHEN OTHERS THEN \
             IF SQLCODE = -1408 THEN NULL; ELSE RAISE; END IF;\nEND;"
        ),
        SqlDialect::Mysql => format!(
            "CREATE {unique_kw}INDEX `{index_name}` ON {qualified_table} ({cols_sql});"
        ),
    }
}

// ── ON UPDATE trigger helper ──────────────────────────────────────────────────

pub(super) fn make_on_update_trigger(
    table:        &str,
    db_schema:    Option<&str>,
    col_name:     &str,
    expr:         &str,
    dialect:      SqlDialect,
    pk_col_names: &[&str],
    options:      DdlOptions,
) -> Vec<String> {
    let trigger_name = format!("trg_{table}_{col_name}_on_update");
    let dialect_expr = normalise_now_expr(expr, dialect);

    match dialect {
        SqlDialect::Postgres => {
            let qualified = match db_schema {
                Some(s) => format!("\"{s}\".\"{table}\""),
                None    => format!("\"{table}\""),
            };
            let fn_name = format!("fn_{table}_{col_name}_on_update");
            let create_fn = format!(
                "CREATE OR REPLACE FUNCTION {fn_name}() RETURNS TRIGGER AS $$\n\
                 BEGIN\n  NEW.\"{col_name}\" = {dialect_expr};\n  RETURN NEW;\nEND;\n\
                 $$ LANGUAGE plpgsql"
            );

            let pg_major = options.pg_major_version.unwrap_or(0);
            if pg_major >= 14 {
                let create_trg = format!(
                    "CREATE OR REPLACE TRIGGER \"{trigger_name}\"\n  \
                       BEFORE UPDATE ON {qualified}\n  \
                       FOR EACH ROW\n  \
                       EXECUTE FUNCTION {fn_name}()"
                );
                vec![create_fn, create_trg]
            } else {
                let drop_trg = format!(
                    "DROP TRIGGER IF EXISTS \"{trigger_name}\" ON {qualified}"
                );
                let create_trg = format!(
                    "CREATE TRIGGER \"{trigger_name}\"\n  \
                       BEFORE UPDATE ON {qualified}\n  \
                       FOR EACH ROW\n  \
                       EXECUTE FUNCTION {fn_name}()"
                );
                vec![create_fn, drop_trg, create_trg]
            }
        }

        SqlDialect::Mssql => {
            let schema_str = db_schema.unwrap_or("dbo");
            let join_or_where = if pk_col_names.is_empty() {
                format!(
                    "FROM [{schema_str}].[{table}] t\n  \
                     WHERE EXISTS (SELECT 1 FROM inserted)"
                )
            } else {
                let join_conds: Vec<String> = pk_col_names.iter()
                    .map(|pk| format!("t.[{pk}] = i.[{pk}]"))
                    .collect();
                format!(
                    "FROM [{schema_str}].[{table}] t\n  \
                     INNER JOIN inserted i ON {}",
                    join_conds.join(" AND ")
                )
            };
            let drop_stmt = format!(
                "IF EXISTS (SELECT 1 FROM sys.triggers WHERE name = N'{trigger_name}')\n  \
                   DROP TRIGGER [{schema_str}].[{trigger_name}];"
            );
            let create_stmt = format!(
                "CREATE TRIGGER [{schema_str}].[{trigger_name}]\n  \
                   ON [{schema_str}].[{table}]\n  \
                   AFTER UPDATE\n\
                 AS\n\
                 BEGIN\n  SET NOCOUNT ON;\n  \
                   UPDATE t SET t.[{col_name}] = {dialect_expr}\n  \
                   {join_or_where};\n\
                 END;"
            );
            vec![drop_stmt, create_stmt]
        }

        SqlDialect::Oracle => {
            let owner = db_schema.map(|s| format!("{}.", dialect.quote(s))).unwrap_or_default();
            vec![format!(
                "CREATE OR REPLACE TRIGGER {owner}{trg}\n  \
                   BEFORE UPDATE ON {owner}{tbl}\n  \
                   FOR EACH ROW\n\
                 BEGIN\n  :NEW.{col} := {dialect_expr};\nEND;",
                trg = dialect.quote(&trigger_name),
                tbl = dialect.quote(table),
                col = dialect.quote(col_name),
            )]
        }

        SqlDialect::Databricks => {
            tracing::warn!(
                column = col_name,
                table  = table,
                "on_update_expr: Databricks does not support triggers — \
                 ON UPDATE for '{}' will be ignored. Consider handling this \
                 in a transform step instead.",
                col_name
            );
            vec![]
        }

        SqlDialect::Mysql => vec![],
    }
}

// ── normalise_now_expr ────────────────────────────────────────────────────────

pub fn normalise_now_expr(expr: &str, dialect: SqlDialect) -> String {
    let lower = expr.trim().to_lowercase();
    if lower == "now()" || lower == "current_timestamp" || lower == "current_timestamp()" {
        match dialect {
            SqlDialect::Postgres   => "now()".to_string(),
            SqlDialect::Mssql      => "SYSDATETIMEOFFSET()".to_string(),
            SqlDialect::Oracle     => "SYSTIMESTAMP".to_string(),
            SqlDialect::Mysql      => "CURRENT_TIMESTAMP".to_string(),
            SqlDialect::Databricks => "current_timestamp()".to_string(),
        }
    } else {
        expr.to_string()
    }
}

// ── SCD2 system column DDL per dialect ────────────────────────────────────────

pub(super) fn scd2_system_column_defs(
    cn:      &crate::db::Scd2ColumnNames,
    dialect: SqlDialect,
) -> Vec<String> {
    match dialect {
        SqlDialect::Postgres => vec![
            format!("    \"{}\" BIGSERIAL PRIMARY KEY", cn.scd_id),
            format!("    \"{}\" TIMESTAMPTZ NOT NULL DEFAULT now()", cn.valid_from),
            format!("    \"{}\" TIMESTAMPTZ", cn.valid_to),
            format!("    \"{}\" BOOLEAN NOT NULL DEFAULT true", cn.is_current),
        ],
        SqlDialect::Mssql => vec![
            format!("    [{}] BIGINT IDENTITY(1,1) PRIMARY KEY", cn.scd_id),
            format!("    [{}] DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET()", cn.valid_from),
            format!("    [{}] DATETIMEOFFSET", cn.valid_to),
            format!("    [{}] BIT NOT NULL DEFAULT 1", cn.is_current),
        ],
        SqlDialect::Oracle => vec![
            format!("    {} NUMBER(19) GENERATED ALWAYS AS IDENTITY PRIMARY KEY", dialect.quote(&cn.scd_id)),
            format!("    {} TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL", dialect.quote(&cn.valid_from)),
            format!("    {} TIMESTAMP WITH TIME ZONE", dialect.quote(&cn.valid_to)),
            format!("    {} NUMBER(1) DEFAULT 1 NOT NULL", dialect.quote(&cn.is_current)),
        ],
        SqlDialect::Mysql => vec![
            format!("    `{}` BIGINT AUTO_INCREMENT PRIMARY KEY", cn.scd_id),
            format!("    `{}` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)", cn.valid_from),
            format!("    `{}` DATETIME(6)", cn.valid_to),
            format!("    `{}` TINYINT(1) NOT NULL DEFAULT 1", cn.is_current),
        ],
        SqlDialect::Databricks => vec![
            format!("    `{}` BIGINT GENERATED ALWAYS AS IDENTITY", cn.scd_id),
            format!("    `{}` TIMESTAMP NOT NULL DEFAULT current_timestamp()", cn.valid_from),
            format!("    `{}` TIMESTAMP", cn.valid_to),
            format!("    `{}` BOOLEAN NOT NULL DEFAULT true", cn.is_current),
        ],
    }
}

// ── Dialect-specific CREATE TABLE wrappers ────────────────────────────────────

pub(super) fn mssql_create_if_not_exists(table: &str, schema: Option<&str>, col_defs: &[String]) -> String {
    let schema_str = schema.unwrap_or("dbo");
    let body = col_defs.join(",\n");
    format!(
        "IF NOT EXISTS (\
           SELECT 1 FROM sys.objects \
           WHERE object_id = OBJECT_ID(N'[{schema_str}].[{table}]') AND type = N'U'\
         )\nBEGIN\n  CREATE TABLE [{schema_str}].[{table}] (\n{body}\n  )\nEND;\n"
    )
}

pub(super) fn oracle_create_if_not_exists(table: &str, schema: Option<&str>, col_defs: &[String]) -> String {
    let dialect = SqlDialect::Oracle;
    let owner = schema.map(|s| format!("{}.", dialect.quote(s))).unwrap_or_default();
    let tbl   = dialect.quote(table);
    let body  = col_defs.join(",\n");
    // Escape single quotes inside the DDL body so it can be safely embedded
    // in an EXECUTE IMMEDIATE string literal (e.g. DEFAULT 'foo' → DEFAULT ''foo'').
    let escaped_body = body.replace('\'', "''");
    let escaped_owner = owner.replace('\'', "''");
    let escaped_tbl = tbl.replace('\'', "''");
    format!(
        "BEGIN\n  \
           EXECUTE IMMEDIATE 'CREATE TABLE {escaped_owner}{escaped_tbl} (\n{escaped_body}\n  )';\n\
         EXCEPTION\n  \
           WHEN OTHERS THEN\n    \
             IF SQLCODE = -955 THEN NULL; ELSE RAISE; END IF;\n\
         END;"
    )
}