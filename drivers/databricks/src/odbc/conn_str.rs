//! ODBC connection string builder for the Databricks Simba ODBC driver.
//!
//! Shared by both source and sink.

use crate::conn::{DatabricksAuth, DatabricksConnParams};

/// Builds an ODBC connection string for the Databricks Simba ODBC driver.
///
/// Key parameters:
/// - Driver:          the installed ODBC driver name (varies by OS/install)
/// - Host:            Databricks workspace hostname
/// - Port:            443 (always HTTPS)
/// - HTTPPath:        warehouse/cluster HTTP path
/// - ThriftTransport: 2 = HTTP transport
/// - SSL:             1 = enable TLS
/// - UseNativeQuery:  1 = pass SQL through without rewriting
///
/// Authentication:
///   AuthMech=3  → Personal Access Token (PAT)
///                  UID=token (literal), PWD=<pat>
///   AuthMech=11 → OAuth 2.0 Token Passthrough
///                  Auth_AccessToken=<bearer_token>
///                  Used for OAuth2 client-credentials flow: we exchange
///                  the client_id/secret for an access token via reqwest,
///                  then pass the resulting Bearer token to the driver.
///
/// When `odbc_driver_path` is set, use the direct path to the `.so` file
/// instead of the registered driver name.  This avoids the need to register
/// the driver in `odbcinst.ini`.
pub(crate) async fn build_odbc_connection_string(
    params: &DatabricksConnParams,
    batch_size: usize,
) -> anyhow::Result<String> {
    let driver_value = match &params.odbc_driver_path {
        Some(path) => path.clone(),
        None       => "{Databricks ODBC Driver}".to_string(),
    };

    let oc = &params.odbc_transport;

    let port             = oc.port.unwrap_or(443);
    let ssl              = if oc.ssl.unwrap_or(true) { "1" } else { "0" };
    let thrift_transport = oc.thrift_transport.unwrap_or(2);
    let use_native_query = if oc.use_native_query.unwrap_or(true) { "1" } else { "0" };

    let mut parts = vec![
        format!("Driver={driver_value}"),
        format!("Host={}", params.host),
        format!("Port={port}"),
        format!("HTTPPath={}", params.http_path),
        format!("ThriftTransport={thrift_transport}"),
        format!("SSL={ssl}"),
        format!("UseNativeQuery={use_native_query}"),
    ];

    // ── Simba string / buffer handling hints ────────────────────────────
    if let Some(len) = oc.string_column_length {
        parts.push(format!("StringColumnLength={len}"));
    }
    if oc.use_unicode_sql_character_types == Some(true) {
        parts.push("UseUnicodeSqlCharacterTypes=1".to_string());
    }
    if oc.use_long_varchar == Some(true) {
        parts.push("UseLongVarchar=1".to_string());
    }

    // ── Auth mechanism ──────────────────────────────────────────────────
    match &params.auth {
        DatabricksAuth::Pat(pat) => {
            parts.push("AuthMech=3".to_string());
            parts.push("UID=token".to_string());
            parts.push(format!("PWD={pat}"));
        }
        DatabricksAuth::OAuth2 { .. } => {
            // Exchange client credentials for an access token, then pass
            // the Bearer token to the driver via AuthMech=11.
            let http_client = reqwest::Client::builder()
                .https_only(true)
                .build()
                .map_err(|e| anyhow::anyhow!("HTTP client for OAuth2: {e}"))?;
            let bearer = params.auth_header(&http_client).await?;
            let access_token = bearer
                .strip_prefix("Bearer ")
                .unwrap_or(&bearer);
            parts.push("AuthMech=11".to_string());
            parts.push(format!("Auth_AccessToken={access_token}"));
        }
    }

    if let Some(cat) = &params.catalog {
        parts.push(format!("Catalog={cat}"));
    }
    if let Some(sch) = &params.schema {
        parts.push(format!("Schema={sch}"));
    }

    // Append user-supplied url_params verbatim.
    for p in &params.url_params {
        parts.push(p.clone());
    }

    // Default `RowsFetchedPerBlock` to the pipeline/step `batch_size` so the
    // ODBC driver's server-side cursor fetch size stays in sync.
    let user_set_rfpb = params.url_params.iter().any(|p| {
        p.split_once('=')
            .map_or(false, |(k, _)| k.eq_ignore_ascii_case("RowsFetchedPerBlock"))
    });
    if !user_set_rfpb {
        parts.push(format!("RowsFetchedPerBlock={batch_size}"));
    }

    let conn_str = parts.join(";");

    let redacted = redact_odbc_connection_string(&conn_str);
    tracing::debug!(
        connection_string = %redacted,
        "Databricks ODBC connection string built"
    );

    Ok(conn_str)
}

/// Redacts sensitive values from an ODBC connection string for safe logging.
fn redact_odbc_connection_string(conn_str: &str) -> String {
    conn_str
        .split(';')
        .map(|part| {
            let upper = part.to_uppercase();
            if upper.starts_with("PWD=") {
                "PWD=***REDACTED***"
            } else if upper.starts_with("AUTH_ACCESSTOKEN=") {
                "Auth_AccessToken=***REDACTED***"
            } else {
                return part;
            }
        })
        .collect::<Vec<_>>()
        .join(";")
}
