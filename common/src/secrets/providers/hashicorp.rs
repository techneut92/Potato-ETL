//! HashiCorp Vault KV v2 secret provider.
//!
//! Fetches secrets from a Vault KV v2 secrets engine via the HTTP API.
//!
//! ## Authentication methods
//!
//! | Method      | How it works                                        |
//! |-------------|-----------------------------------------------------|
//! | Token       | Uses a Vault token directly (`$VAULT_TOKEN` or config) |
//! | AppRole     | Exchanges `role_id` + `secret_id` for a token       |
//! | Kubernetes  | Uses a K8s service account JWT to obtain a token     |
//!
//! ## Secret path resolution
//!
//! The path from `secret::vault/<path>` is appended to the KV v2 data endpoint:
//!
//! ```text
//! GET {address}/v1/{mount}/data/{path}
//! ```
//!
//! The response JSON has the structure:
//! ```json
//! {
//!   "data": {
//!     "data": { "username": "admin", "password": "s3cret" },
//!     "metadata": { ... }
//!   }
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::RwLock;

use super::SecretProvider;
use crate::config::secrets::{VaultConfig, VaultAuth};

/// Safety margin: refresh tokens 5 minutes before they expire.
const TOKEN_EXPIRY_MARGIN_SECS: u64 = 300;

/// Cached token with expiry tracking.
struct CachedToken {
    token: String,
    /// When this token was obtained.
    obtained_at: Instant,
    /// Token lifetime in seconds (from the auth response `lease_duration`).
    expires_in_secs: u64,
}

impl CachedToken {
    fn is_valid(&self) -> bool {
        let elapsed = self.obtained_at.elapsed().as_secs();
        elapsed + TOKEN_EXPIRY_MARGIN_SECS < self.expires_in_secs
    }
}

/// HashiCorp Vault KV v2 provider.
pub struct HashiCorpVaultProvider {
    address: String,
    mount:   String,
    namespace: Option<String>,
    auth:    VaultAuth,
    client:  reqwest::Client,
    /// Cached token with expiry (obtained via AppRole/K8s auth, or directly from config).
    token:   Arc<RwLock<Option<CachedToken>>>,
    /// Per-path cache: avoid fetching the same secret twice in one resolution pass.
    cache:   Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl HashiCorpVaultProvider {
    /// Creates a new Vault provider from config.
    ///
    /// The `address` falls back to `$VAULT_ADDR` if not set in config.
    /// Token auth falls back to `$VAULT_TOKEN` if not set in config.
    pub fn from_config(config: &VaultConfig) -> anyhow::Result<Self> {
        let address = config.address.clone()
            .or_else(|| std::env::var("VAULT_ADDR").ok())
            .ok_or_else(|| anyhow::anyhow!(
                "Vault address not configured. Set `secrets.vault.address` in YAML \
                 or the `VAULT_ADDR` environment variable."
            ))?;

        // Strip trailing slash for clean URL construction.
        let address = address.trim_end_matches('/').to_string();

        // Pre-resolve token if using token auth.
        let initial_token = match &config.auth {
            VaultAuth::Token { token } => {
                token.clone()
                    .map(|t| resolve_env_ref(&t))
                    .or_else(|| std::env::var("VAULT_TOKEN").ok())
                    .map(|t| CachedToken {
                        token: t,
                        obtained_at: Instant::now(),
                        // Token auth tokens don't have a known expiry — use a long default.
                        // Users are expected to provide non-expiring tokens or rotate externally.
                        expires_in_secs: 86400 * 365, // ~1 year
                    })
            }
            _ => None,
        };

        Ok(Self {
            address,
            mount: config.mount.clone(),
            namespace: config.namespace.clone(),
            auth: config.auth.clone(),
            client: reqwest::Client::new(),
            token: Arc::new(RwLock::new(initial_token)),
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Ensures we have a valid Vault token, authenticating if necessary.
    async fn ensure_token(&self) -> anyhow::Result<String> {
        // Check if we already have a valid token.
        {
            let guard = self.token.read().await;
            if let Some(cached) = guard.as_ref() {
                if cached.is_valid() {
                    return Ok(cached.token.clone());
                }
                tracing::debug!("vault: cached token expired, re-authenticating");
            }
        }

        // Authenticate based on method.
        let (new_token, expires_in) = match &self.auth {
            VaultAuth::Token { token } => {
                let t = token.clone()
                    .map(|t| resolve_env_ref(&t))
                    .or_else(|| std::env::var("VAULT_TOKEN").ok())
                    .ok_or_else(|| anyhow::anyhow!(
                        "Vault token not available. Set `secrets.vault.auth.token` in YAML \
                         or the `VAULT_TOKEN` environment variable."
                    ))?;
                (t, 86400 * 365u64) // Token auth: no known expiry
            }
            VaultAuth::AppRole { role_id, secret_id } => {
                self.auth_approle(
                    &resolve_env_ref(role_id),
                    &resolve_env_ref(secret_id),
                ).await?
            }
            VaultAuth::Kubernetes { role, token_path, mount_path } => {
                self.auth_kubernetes(role, token_path, mount_path).await?
            }
        };

        let mut guard = self.token.write().await;
        *guard = Some(CachedToken {
            token: new_token.clone(),
            obtained_at: Instant::now(),
            expires_in_secs: expires_in,
        });
        Ok(new_token)
    }

    /// Authenticate via AppRole and return `(client_token, lease_duration_secs)`.
    async fn auth_approle(&self, role_id: &str, secret_id: &str) -> anyhow::Result<(String, u64)> {
        let url = format!("{}/v1/auth/approle/login", self.address);
        let body = serde_json::json!({
            "role_id": role_id,
            "secret_id": secret_id,
        });

        let mut req = self.client.post(&url).json(&body);
        if let Some(ns) = &self.namespace {
            req = req.header("X-Vault-Namespace", ns.as_str());
        }

        let resp = req.send().await
            .map_err(|e| anyhow::anyhow!("Vault AppRole auth request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Vault AppRole auth failed (HTTP {status}): {body}");
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| anyhow::anyhow!("Vault AppRole auth: invalid JSON response: {e}"))?;

        json["auth"]["client_token"]
            .as_str()
            .map(|s| {
                let lease = json["auth"]["lease_duration"]
                    .as_u64()
                    .unwrap_or(3600);
                (s.to_string(), lease)
            })
            .ok_or_else(|| anyhow::anyhow!("Vault AppRole auth: missing client_token in response"))
    }

    /// Authenticate via Kubernetes service account JWT.
    ///
    /// Returns `(client_token, lease_duration_secs)`.
    async fn auth_kubernetes(
        &self,
        role: &str,
        token_path: &str,
        mount_path: &str,
    ) -> anyhow::Result<(String, u64)> {
        let jwt = tokio::fs::read_to_string(token_path).await
            .map_err(|e| anyhow::anyhow!(
                "Failed to read Kubernetes service account token from '{token_path}': {e}"
            ))?;

        let url = format!("{}/v1/auth/{mount_path}/login", self.address);
        let body = serde_json::json!({
            "role": role,
            "jwt": jwt.trim(),
        });

        let mut req = self.client.post(&url).json(&body);
        if let Some(ns) = &self.namespace {
            req = req.header("X-Vault-Namespace", ns.as_str());
        }

        let resp = req.send().await
            .map_err(|e| anyhow::anyhow!("Vault Kubernetes auth request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Vault Kubernetes auth failed (HTTP {status}): {body}");
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| anyhow::anyhow!("Vault K8s auth: invalid JSON response: {e}"))?;

        json["auth"]["client_token"]
            .as_str()
            .map(|s| {
                let lease = json["auth"]["lease_duration"]
                    .as_u64()
                    .unwrap_or(3600);
                (s.to_string(), lease)
            })
            .ok_or_else(|| anyhow::anyhow!("Vault K8s auth: missing client_token in response"))
    }
}

#[async_trait::async_trait]
impl SecretProvider for HashiCorpVaultProvider {
    async fn get_secret(&self, path: &str) -> anyhow::Result<HashMap<String, String>> {
        // Check cache first.
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(path) {
                return Ok(cached.clone());
            }
        }

        let token = self.ensure_token().await?;
        let url = format!("{}/v1/{}/data/{}", self.address, self.mount, path);

        let mut req = self.client.get(&url)
            .header("X-Vault-Token", &token);
        if let Some(ns) = &self.namespace {
            req = req.header("X-Vault-Namespace", ns.as_str());
        }

        let resp = req.send().await
            .map_err(|e| anyhow::anyhow!("Vault GET {url}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Vault secret '{path}' fetch failed (HTTP {status}): {body}");
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| anyhow::anyhow!("Vault secret '{path}': invalid JSON response: {e}"))?;

        // KV v2 response: { "data": { "data": { ... }, "metadata": { ... } } }
        let data = &json["data"]["data"];

        let result: HashMap<String, String> = match data {
            serde_json::Value::Object(map) => {
                map.iter()
                    .map(|(k, v)| {
                        let val = match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        (k.clone(), val)
                    })
                    .collect()
            }
            serde_json::Value::String(s) => {
                // Single-value secret — try to parse as structured (JSON → YAML → plain).
                super::parse_structured_value(s)
            }
            _ => anyhow::bail!(
                "Vault secret '{path}': unexpected data format. \
                 Expected a JSON object or string, got: {data}"
            ),
        };

        tracing::debug!(
            path = path,
            fields = result.len(),
            "vault: fetched secret"
        );

        // Cache the result.
        {
            let mut cache = self.cache.write().await;
            cache.insert(path.to_string(), result.clone());
        }

        Ok(result)
    }

    fn provider_name(&self) -> &str { "HashiCorp Vault" }
}

/// Resolves `$ENV_VAR` references in a string value.
///
/// If the string starts with `$`, the remainder is treated as an environment
/// variable name.  Otherwise the string is returned as-is.
fn resolve_env_ref(s: &str) -> String {
    if let Some(var) = s.strip_prefix('$') {
        std::env::var(var).unwrap_or_else(|_| s.to_string())
    } else {
        s.to_string()
    }
}