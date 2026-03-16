//! MySQL shared helpers — identifier quoting, full-table name, and epoch.

use chrono::NaiveDate;

/// Creates a [`MySqlPool`] with optional `init_sql` statements executed on
/// every new connection via [`MySqlPoolOptions::after_connect`].
pub async fn mysql_pool_with_init_sql(
    conn_str:        &str,
    max_connections: u32,
    init_sql:        &[String],
) -> anyhow::Result<sqlx::MySqlPool> {
    use sqlx::mysql::MySqlPoolOptions;

    let mut opts = MySqlPoolOptions::new().max_connections(max_connections);

    if !init_sql.is_empty() {
        let stmts: Vec<String> = init_sql.to_vec();
        tracing::info!(
            count = stmts.len(),
            "mysql_pool_with_init_sql: registering {} init_sql statement(s)",
            stmts.len(),
        );
        opts = opts.after_connect(move |conn, _meta| {
            let stmts = stmts.clone();
            Box::pin(async move {
                for stmt in &stmts {
                    tracing::debug!(sql = %stmt, "init_sql: executing");
                    sqlx::query(stmt).execute(&mut *conn).await
                        .map_err(|e| sqlx::Error::Configuration(
                            format!("init_sql failed: {stmt}: {e}").into()
                        ))?;
                }
                Ok(())
            })
        });
    }

    let pool = opts.connect(conn_str).await?;
    Ok(pool)
}

/// Returns `` `schema`.`table` `` or `` `table` `` when schema is empty.
pub fn mysql_full_table(schema: &str, table: &str) -> String {
    if schema.is_empty() {
        backtick(table)
    } else {
        format!("{}.{}", backtick(schema), backtick(table))
    }
}

/// Backtick-quotes a MySQL identifier, doubling any embedded backticks.
pub fn backtick(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// UNIX epoch as `NaiveDate` — used for `Date32` conversion in the source.
pub fn unix_epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
}

// ── Table introspection ──────────────────────────────────────────────────────

pub async fn mysql_introspect_table_columns(
    pool: &sqlx::MySqlPool,
    schema_name: &str,
    table_name: &str,
) -> anyhow::Result<Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>> {
    use potato_etl_common::db::common::alignment::TargetColumn;

    let rows = if schema_name.is_empty() {
        sqlx::query_as::<_, (String, String, String, Option<String>)>(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION"
        )
        .bind(table_name)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as::<_, (String, String, String, Option<String>)>(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION"
        )
        .bind(schema_name)
        .bind(table_name)
        .fetch_all(pool)
        .await?
    };

    if rows.is_empty() {
        return Ok(None);
    }

    let cols: Vec<TargetColumn> = rows
        .into_iter()
        .map(|(name, data_type, nullable, default)| TargetColumn {
            name,
            data_type: data_type.to_ascii_lowercase(),
            nullable: nullable.eq_ignore_ascii_case("YES"),
            has_default: default.is_some(),
        })
        .collect();

    Ok(Some(cols))
}