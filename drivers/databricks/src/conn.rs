//! Transport-agnostic Databricks connection identity, authentication, and shared helpers.
//!
//! This module owns [`DatabricksConnParams`] (parsing, auth, catalog/schema)
//! but contains **no** REST-statement-execution or ODBC logic.  Transport-
//! specific code lives in `api/`, `odbc/`, and (future) `thrift/`.

use std::sync::Arc;

use reqwest::Client;
use serde::Deserialize;
use tokio::sync::Mutex;

use potato_etl_common::config::pct_decode;

// ── Transport mode ────────────────────────────────────────────────────────────

/// Which wire protocol to use when talking to a Databricks SQL warehouse.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum DatabricksMode {
    /// REST SQL Statement Execution API (`/api/2.0/sql/statements`).
    /// Pure Rust, no system dependencies.
    #[default]
    Api,
    /// Simba ODBC driver — requires the `odbc` feature and an installed driver.
    Odbc,
    /// Hive / Spark Thrift binary protocol over HTTPS — requires the `thrift` feature.
    Thrift,
}

// ── Auth ──────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum DatabricksAuth {
    Pat(String),
    OAuth2 { client_id: String, client_secret: String },
}

#[derive(Clone, Debug)]
struct CachedToken {
    access_token: String,
    expires_at: std::time::Instant,
}

#[derive(Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    expires_in: u64,
}

// ── ODBC sub-config ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub struct OdbcTransportConfig {
    pub port: Option<u16>,
    pub ssl: Option<bool>,
    pub thrift_transport: Option<u8>,
    pub use_native_query: Option<bool>,
    pub string_column_length: Option<u32>,
    pub use_unicode_sql_character_types: Option<bool>,
    pub use_long_varchar: Option<bool>,
}

// ── Connection params ─────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct DatabricksConnParams {
    pub host: String,
    pub http_path: String,
    pub warehouse_id: String,
    pub auth: DatabricksAuth,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub mode: DatabricksMode,
    pub odbc_driver_path: Option<String>,
    pub odbc_transport: OdbcTransportConfig,
    pub url_params: Vec<String>,
    /// SQL statements executed at the start of each source/sink operation.
    pub init_sql: Vec<String>,
    token_cache: Arc<Mutex<Option<CachedToken>>>,
}

impl std::fmt::Debug for DatabricksConnParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabricksConnParams")
            .field("host", &self.host)
            .field("warehouse_id", &self.warehouse_id)
            .field("mode", &self.mode)
            .field("auth", &match &self.auth {
                DatabricksAuth::Pat(_) => "PAT(***)",
                DatabricksAuth::OAuth2 { .. } => "OAuth2(***)",
            })
            .field("catalog", &self.catalog)
            .field("schema", &self.schema)
            .finish()
    }
}

impl DatabricksConnParams {
    pub fn parse(conn_str: &str) -> anyhow::Result<Self> {
        let rest = conn_str
            .strip_prefix("databricks://")
            .ok_or_else(|| anyhow::anyhow!("Must start with 'databricks://'"))?;

        let (host, path_and_query) = rest
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("Must contain path after host"))?;

        let (path, query) = match path_and_query.split_once('?') {
            Some((p, q)) => (p, q),
            None => (path_and_query, ""),
        };

        let warehouse_id = path
            .split('/')
            .filter(|s| !s.is_empty())
            .last()
            .ok_or_else(|| anyhow::anyhow!("Cannot extract warehouse ID"))?
            .to_string();

        let mut token = None;
        let mut client_id = None;
        let mut client_secret = None;
        let mut catalog = None;
        let mut schema = None;
        let mut mode: Option<String> = None;
        let mut odbc_legacy = false;
        let mut odbc_driver_path = None;
        let mut odbc_transport = OdbcTransportConfig::default();
        let mut url_params: Vec<String> = Vec::new();

        for param in query.split('&').filter(|s| !s.is_empty()) {
            if let Some((k, v)) = param.split_once('=') {
                match k {
                    "token"          => token = Some(pct_decode(v)?),
                    "client_id"      => client_id = Some(pct_decode(v)?),
                    "client_secret"  => client_secret = Some(pct_decode(v)?),
                    "catalog"        => catalog = Some(v.to_string()),
                    "schema"         => schema = Some(v.to_string()),
                    "mode"           => mode = Some(v.to_string()),
                    "odbc"           => odbc_legacy = v.to_lowercase() == "true",
                    "odbc_driver_path" => odbc_driver_path = Some(pct_decode(v)?),
                    "url_param"      => url_params.push(pct_decode(v)?),
                    "odbc_port"      => odbc_transport.port = Some(v.parse()?),
                    "odbc_ssl"       => odbc_transport.ssl = Some(v.parse()?),
                    "odbc_thrift_transport"          => odbc_transport.thrift_transport = Some(v.parse()?),
                    "odbc_use_native_query"          => odbc_transport.use_native_query = Some(v.parse()?),
                    "odbc_string_column_length"      => odbc_transport.string_column_length = Some(v.parse()?),
                    "odbc_use_unicode_sql_char_types" => odbc_transport.use_unicode_sql_character_types = Some(v.parse()?),
                    "odbc_use_long_varchar"           => odbc_transport.use_long_varchar = Some(v.parse()?),
                    _ => {}
                }
            }
        }

        // Resolve mode — explicit `mode=` takes precedence over legacy `odbc=true`.
        let resolved_mode = match mode.as_deref() {
            Some("api")    => DatabricksMode::Api,
            Some("odbc")   => DatabricksMode::Odbc,
            Some("thrift") => DatabricksMode::Thrift,
            Some(other)    => anyhow::bail!(
                "Unknown Databricks mode '{other}'. Valid modes: api, odbc, thrift"
            ),
            None if odbc_legacy => DatabricksMode::Odbc,
            None => DatabricksMode::Api,
        };

        let auth = match (token, client_id, client_secret) {
            (Some(t), None, None) => DatabricksAuth::Pat(t),
            (None, Some(id), Some(sec)) => DatabricksAuth::OAuth2 {
                client_id: id,
                client_secret: sec,
            },
            (None, Some(_), None) => anyhow::bail!("`client_id` without `client_secret`"),
            (None, None, Some(_)) => anyhow::bail!("`client_secret` without `client_id`"),
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                anyhow::bail!("Provide `token` OR `client_id`+`client_secret`, not both")
            }
            (None, None, None) => {
                anyhow::bail!("Missing auth. Provide `token` or `client_id`+`client_secret`")
            }
        };

        Ok(Self {
            host: host.to_string(),
            http_path: format!("/{path}"),
            warehouse_id,
            auth,
            catalog,
            schema,
            mode: resolved_mode,
            odbc_driver_path,
            odbc_transport,
            url_params,
            init_sql: Vec::new(),
            token_cache: Arc::new(Mutex::new(None)),
        })
    }

    /// Base URL for all Databricks REST API calls.
    pub fn api_base(&self) -> String {
        format!("https://{}", self.host)
    }

    /// Returns a `Bearer <token>` header value, refreshing OAuth2 tokens as needed.
    pub async fn auth_header(&self, client: &Client) -> anyhow::Result<String> {
        match &self.auth {
            DatabricksAuth::Pat(token) => Ok(format!("Bearer {token}")),
            DatabricksAuth::OAuth2 { client_id, client_secret } => {
                // Check cache first.
                {
                    let guard = self.token_cache.lock().await;
                    if let Some(c) = guard.as_ref() {
                        if std::time::Instant::now() < c.expires_at {
                            return Ok(format!("Bearer {}", c.access_token));
                        }
                    }
                }
                let token_url = format!("{}/oidc/v1/token", self.api_base());
                let resp = client
                    .post(&token_url)
                    .form(&[
                        ("grant_type", "client_credentials"),
                        ("client_id", client_id.as_str()),
                        ("client_secret", client_secret.as_str()),
                        ("scope", "all-apis"),
                    ])
                    .send()
                    .await?;

                if !resp.status().is_success() {
                    let s = resp.status();
                    let b = resp.text().await.unwrap_or_default();
                    anyhow::bail!("OAuth2 token endpoint returned {s}: {b}");
                }

                let tr: OAuthTokenResponse = resp.json().await?;
                let margin = 60.min(tr.expires_in / 4);
                let expires_at = std::time::Instant::now()
                    + std::time::Duration::from_secs(tr.expires_in.saturating_sub(margin));
                let bearer = format!("Bearer {}", tr.access_token);
                {
                    let mut g = self.token_cache.lock().await;
                    *g = Some(CachedToken {
                        access_token: tr.access_token,
                        expires_at,
                    });
                }
                Ok(bearer)
            }
        }
    }

    /// Returns `true` if the legacy `odbc` field was set — kept for backward compat
    /// in `with_driver_options` until callers migrate to `mode`.
    pub fn is_odbc(&self) -> bool {
        self.mode == DatabricksMode::Odbc
    }
}

// ── Shared helpers ────────────────────────────────────────────────────────────

/// Three-part-name quoting with backticks for Databricks / Spark SQL.
pub fn dbx_full_table(catalog: Option<&str>, schema: &str, table: &str) -> String {
    match (catalog, schema.is_empty()) {
        (Some(cat), false) => format!("{}.{}.{}", backtick(cat), backtick(schema), backtick(table)),
        (Some(cat), true)  => format!("{}.{}", backtick(cat), backtick(table)),
        (None, false)      => format!("{}.{}", backtick(schema), backtick(table)),
        (None, true)       => backtick(table),
    }
}

/// Backtick-quote an identifier, escaping embedded backticks.
pub fn backtick(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// Build a shared reqwest client suitable for Databricks API calls.
pub fn build_client() -> anyhow::Result<Client> {
    Client::builder()
        .https_only(true)
        .pool_max_idle_per_host(4)
        .tcp_nodelay(true)
        .connect_timeout(std::time::Duration::from_secs(30))
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|e| anyhow::anyhow!("HTTP client: {e}"))
}