//! Postgres-specific row → Rust value helpers.
//!
//! Shared between the Postgres source (`source.rs`) and SCD2 sink (`scd2.rs`).

use sqlx::Row;

/// Creates a [`PgPool`] with optional `init_sql` statements executed on every
/// new connection via [`PgPoolOptions::after_connect`].
///
/// When `init_sql` is empty, this is equivalent to a plain
/// `PgPoolOptions::new().max_connections(n).connect(url)`.
pub async fn pg_pool_with_init_sql(
    conn_str:        &str,
    max_connections: u32,
    init_sql:        &[String],
) -> anyhow::Result<sqlx::PgPool> {
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx::ConnectOptions;
    use tracing::log::LevelFilter;
    use std::time::Duration;

    // Silence sqlx's per-statement "slow statement" warnings — for any sizable
    // batched INSERT they fire at WARN with the entire bind-placeholder list,
    // which floods the log. Our driver layer emits its own per-batch stats.
    let connect_opts: PgConnectOptions = conn_str.parse::<PgConnectOptions>()?
        .log_slow_statements(LevelFilter::Off, Duration::from_secs(60))
        .log_statements(LevelFilter::Off);

    let mut opts = PgPoolOptions::new().max_connections(max_connections);

    if !init_sql.is_empty() {
        let stmts: Vec<String> = init_sql.to_vec();
        tracing::info!(
            count = stmts.len(),
            "pg_pool_with_init_sql: registering {} init_sql statement(s)",
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

    let pool = opts.connect_with(connect_opts).await?;
    Ok(pool)
}

/// Converts one Postgres cell to `Option<String>`.
///
/// All values are stored as text for easy equality comparison in the SCD2 logic.
pub fn pg_col_to_string_opt(
    row:       &sqlx::postgres::PgRow,
    idx:       usize,
    type_name: &str,
) -> Option<String> {
    macro_rules! try_str {
        ($t:ty) => {
            if let Ok(Some(v)) = row.try_get::<Option<$t>, _>(idx) {
                return Some(v.to_string());
            }
        };
    }

    match type_name {
        "INT2" | "SMALLINT" | "SMALLSERIAL"      => { try_str!(i16); }
        "INT4" | "INT"      | "SERIAL"            => { try_str!(i32); }
        "INT8" | "BIGINT"   | "BIGSERIAL"         => { try_str!(i64); }
        "FLOAT4" | "REAL"                         => { try_str!(f32); }
        "FLOAT8" | "DOUBLE PRECISION"             => { try_str!(f64); }
        "BOOL"   | "BOOLEAN"                      => { try_str!(bool); }
        _                                         => { try_str!(String); }
    }

    None
}

// ── Table introspection ──────────────────────────────────────────────────────

/// Queries `information_schema.columns` for the target table and returns
/// column metadata in `ordinal_position` order.
///
/// Returns `Ok(None)` when the table does not exist (empty result set).
pub async fn pg_introspect_table_columns<'e, E>(
    executor: E,
    schema_name: &str,
    table_name: &str,
) -> anyhow::Result<Option<Vec<potato_etl_common::db::common::alignment::TargetColumn>>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    use potato_etl_common::db::common::alignment::TargetColumn;

    let rows = sqlx::query_as::<_, (String, String, String, Option<String>)>(
        "SELECT column_name, data_type, is_nullable, column_default \
         FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 \
         ORDER BY ordinal_position"
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(executor)
    .await?;

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