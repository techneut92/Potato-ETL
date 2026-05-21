//! Azure Key Vault secret provider.
//!
//! Fetches secrets from Azure Key Vault via the REST API.
//!
//! ## Authentication — DefaultAzureCredential chain
//!
//! Follows the same resolution order as Python's `DefaultAzureCredential`
//! from the `azure-identity` SDK:
//!
//! | Priority | Method                | Source                                              |
//! |----------|-----------------------|-----------------------------------------------------|
//! | 1        | **Service Principal** | Config or `AZURE_TENANT_ID` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` |
//! | 2        | **Managed Identity**  | Azure IMDS endpoint (`169.254.169.254`)             |
//! | 3        | **Azure CLI**         | `az account get-access-token`                       |
//!
//! The first method that returns a valid token is cached.  On refresh the
//! same method is retried first; if it fails the chain restarts from the top.
//!
//! ## Secret path format
//!
//! ```text
//! secret::azure/<vault-name>/<secret-name>[#field]
//! ```
//!
//! The vault URL is constructed as `https://<vault-name>.vault.azure.net`.
//! When `vault_url` is set in config, it overrides the vault name from the path
//! (and the path is just `<secret-name>`).
//!
//! ## API
//!
//! ```text
//! GET https://<vault>.vault.azure.net/secrets/<secret-name>?api-version=7.4
//! Authorization: Bearer <token>
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::RwLock;

use super::SecretProvider;
use crate::config::secrets::AzureKeyVaultConfig;

const AKV_API_VERSION: &str = "7.4";
const AKV_SCOPE: &str = "https://vault.azure.net/.default";
/// Resource identifier used for managed identity and CLI token requests.
const AKV_RESOURCE: &str = "https://vault.azure.net";

/// Azure IMDS (Instance Metadata Service) endpoint for managed identity tokens.
const IMDS_TOKEN_URL: &str =
    "http://169.254.169.254/metadata/identity/oauth2/token";

/// Safety margin: refresh tokens 5 minutes before they expire.
const TOKEN_EXPIRY_MARGIN_SECS: u64 = 300;

/// IMDS probe timeout — short because the endpoint is local and instant on
/// Azure, but unreachable (and slow to time out) elsewhere.
const IMDS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Azure CLI timeout — `az` is a Python program that can be slow to start.
const CLI_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

// ── Auth method tracking ────────────────────────────────────────────────────

/// Which credential method produced the current token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthMethod {
    ServicePrincipal,
    ManagedIdentity,
    AzureCli,
}

/// Cached token with expiry and the method that produced it.
struct CachedToken {
    access_token: String,
    obtained_at: Instant,
    expires_in_secs: u64,
    method: AuthMethod,
}

impl CachedToken {
    fn is_valid(&self) -> bool {
        let elapsed = self.obtained_at.elapsed().as_secs();
        elapsed + TOKEN_EXPIRY_MARGIN_SECS < self.expires_in_secs
    }
}

// ── SPN credentials (optional) ──────────────────────────────────────────────

/// Service principal credentials resolved from config + env vars.
struct SpnCredentials {
    tenant_id:     String,
    client_id:     String,
    client_secret: String,
}

// ── Provider ────────────────────────────────────────────────────────────────

/// Azure Key Vault provider with `DefaultAzureCredential`-style auth chain.
pub struct AzureKeyVaultProvider {
    /// Override vault URL from config (if set, path contains only secret name).
    vault_url_override: Option<String>,
    /// SPN credentials (present only when all three fields are available).
    spn: Option<SpnCredentials>,
    /// User-assigned managed identity client ID (None = system-assigned).
    managed_identity_client_id: Option<String>,
    client: reqwest::Client,
    /// Cached OAuth2 access token with expiry + method.
    token:  Arc<RwLock<Option<CachedToken>>>,
    /// Per-path secret cache.
    cache:  Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

impl AzureKeyVaultProvider {
    /// Creates a new Azure Key Vault provider from config.
    ///
    /// **Never fails for missing credentials** — the credential chain is
    /// resolved lazily on the first `get_secret()` call.
    pub fn from_config(config: &AzureKeyVaultConfig) -> anyhow::Result<Self> {
        // Try to assemble SPN credentials from config + env vars.
        let tenant_id = config.tenant_id.as_deref()
            .map(resolve_env_ref)
            .or_else(|| std::env::var("AZURE_TENANT_ID").ok());
        let client_id = config.client_id.as_deref()
            .map(resolve_env_ref)
            .or_else(|| std::env::var("AZURE_CLIENT_ID").ok());
        let client_secret = config.client_secret.as_deref()
            .map(resolve_env_ref)
            .or_else(|| std::env::var("AZURE_CLIENT_SECRET").ok());

        let spn = match (tenant_id, client_id, client_secret) {
            (Some(t), Some(c), Some(s)) => {
                tracing::debug!("azure_kv: SPN credentials available (tenant={t})");
                Some(SpnCredentials {
                    tenant_id:     t,
                    client_id:     c,
                    client_secret: s,
                })
            }
            _ => {
                tracing::debug!(
                    "azure_kv: SPN credentials incomplete — will try managed identity / CLI"
                );
                None
            }
        };

        Ok(Self {
            vault_url_override: config.vault_url.clone(),
            spn,
            managed_identity_client_id: config.managed_identity_client_id.clone(),
            client: reqwest::Client::new(),
            token: Arc::new(RwLock::new(None)),
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    // ── Token acquisition ───────────────────────────────────────────────────

    /// Ensures a valid access token is available, refreshing if necessary.
    ///
    /// On refresh, the previously successful method is retried first.
    /// If that fails, the full chain is retried from the top.
    async fn ensure_token(&self) -> anyhow::Result<String> {
        // Fast path: cached token still valid.
        {
            let guard = self.token.read().await;
            if let Some(cached) = guard.as_ref() {
                if cached.is_valid() {
                    return Ok(cached.access_token.clone());
                }
                tracing::debug!(
                    method = ?cached.method,
                    "azure_kv: cached token expired, refreshing"
                );
            }
        }

        // Check if we have a previously successful method to try first.
        let previous_method = {
            let guard = self.token.read().await;
            guard.as_ref().map(|c| c.method)
        };

        // If previous method exists, try it first for a fast refresh.
        if let Some(method) = previous_method {
            if let Ok((token, expires_in)) = self.try_method(method).await {
                return self.store_token(token, expires_in, method).await;
            }
            tracing::debug!(
                method = ?method,
                "azure_kv: previous auth method failed on refresh, trying full chain"
            );
        }

        // Full credential chain.
        self.run_credential_chain().await
    }

    /// Runs the full credential chain: SPN → Managed Identity → Azure CLI.
    async fn run_credential_chain(&self) -> anyhow::Result<String> {
        let mut errors: Vec<String> = Vec::new();

        // ── 1. Service Principal ────────────────────────────────────────────
        if self.spn.is_some() {
            match self.token_from_spn().await {
                Ok((token, expires_in)) => {
                    tracing::info!("azure_kv: authenticated via Service Principal");
                    return self.store_token(token, expires_in, AuthMethod::ServicePrincipal).await;
                }
                Err(e) => {
                    tracing::debug!("azure_kv: SPN auth failed: {e}");
                    errors.push(format!("ServicePrincipal: {e}"));
                }
            }
        } else {
            // SPN credentials weren't assembled — tell the user *which* env
            // vars were missing so they can fix the launch (common with
            // container runtimes that don't forward host env by default).
            let missing: Vec<&str> = [
                ("AZURE_TENANT_ID",     std::env::var("AZURE_TENANT_ID").is_err()),
                ("AZURE_CLIENT_ID",     std::env::var("AZURE_CLIENT_ID").is_err()),
                ("AZURE_CLIENT_SECRET", std::env::var("AZURE_CLIENT_SECRET").is_err()),
            ].iter().filter_map(|(n, m)| if *m { Some(*n) } else { None }).collect();
            errors.push(format!(
                "ServicePrincipal: skipped (missing env vars: {})",
                if missing.is_empty() { "<none — config block also empty>".to_string() } else { missing.join(", ") },
            ));
        }

        // ── 2. Managed Identity (IMDS) ──────────────────────────────────────
        match self.token_from_managed_identity().await {
            Ok((token, expires_in)) => {
                tracing::info!("azure_kv: authenticated via Managed Identity");
                return self.store_token(token, expires_in, AuthMethod::ManagedIdentity).await;
            }
            Err(e) => {
                tracing::debug!("azure_kv: Managed Identity auth failed: {e}");
                errors.push(format!("ManagedIdentity: {e}"));
            }
        }

        // ── 3. Azure CLI ────────────────────────────────────────────────────
        match self.token_from_azure_cli().await {
            Ok((token, expires_in)) => {
                tracing::info!("azure_kv: authenticated via Azure CLI");
                return self.store_token(token, expires_in, AuthMethod::AzureCli).await;
            }
            Err(e) => {
                tracing::debug!("azure_kv: Azure CLI auth failed: {e}");
                errors.push(format!("AzureCli: {e}"));
            }
        }

        // All methods exhausted.
        anyhow::bail!(
            "Azure Key Vault: all authentication methods failed.\n\
             Tried (in order):\n  {}\n\n\
             Configure one of:\n  \
             - Service Principal: set AZURE_TENANT_ID + AZURE_CLIENT_ID + AZURE_CLIENT_SECRET\n  \
             - Managed Identity: run on Azure (AKS / App Service / VM)\n  \
             - Azure CLI: run `az login` first",
            errors.join("\n  ")
        )
    }

    /// Tries a specific auth method.  Used to fast-refresh with the last known method.
    async fn try_method(&self, method: AuthMethod) -> anyhow::Result<(String, u64)> {
        match method {
            AuthMethod::ServicePrincipal => self.token_from_spn().await,
            AuthMethod::ManagedIdentity  => self.token_from_managed_identity().await,
            AuthMethod::AzureCli         => self.token_from_azure_cli().await,
        }
    }

    /// Stores a token in the cache and returns it.
    async fn store_token(
        &self,
        access_token: String,
        expires_in: u64,
        method: AuthMethod,
    ) -> anyhow::Result<String> {
        let mut guard = self.token.write().await;
        *guard = Some(CachedToken {
            access_token: access_token.clone(),
            obtained_at: Instant::now(),
            expires_in_secs: expires_in,
            method,
        });
        Ok(access_token)
    }

    // ── Method 1: Service Principal (client credentials flow) ───────────────

    async fn token_from_spn(&self) -> anyhow::Result<(String, u64)> {
        let spn = self.spn.as_ref()
            .ok_or_else(|| anyhow::anyhow!("SPN credentials not available"))?;

        let url = format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            spn.tenant_id
        );

        let resp = self.client.post(&url)
            .form(&[
                ("grant_type",    "client_credentials"),
                ("client_id",     &spn.client_id),
                ("client_secret", &spn.client_secret),
                ("scope",         AKV_SCOPE),
            ])
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("SPN token request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("SPN token request failed (HTTP {status}): {body}");
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| anyhow::anyhow!("SPN token: invalid JSON: {e}"))?;

        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("SPN token: missing access_token"))?
            .to_string();

        let expires_in = json["expires_in"]
            .as_u64()
            .or_else(|| json["expires_in"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(3600);

        Ok((access_token, expires_in))
    }

    // ── Method 2: Managed Identity (IMDS) ───────────────────────────────────

    /// Obtains a token from the Azure Instance Metadata Service (IMDS).
    ///
    /// Works on:
    /// - Azure VMs
    /// - Azure Kubernetes Service (AKS) pods with managed identity
    /// - Azure App Service / Azure Functions
    /// - Azure Container Instances
    ///
    /// The IMDS endpoint is a link-local address (`169.254.169.254`) that is
    /// only reachable from within Azure infrastructure.  Outside Azure, the
    /// request times out quickly (2s) and we move on to the next method.
    async fn token_from_managed_identity(&self) -> anyhow::Result<(String, u64)> {
        let imds_client = reqwest::Client::builder()
            .timeout(IMDS_TIMEOUT)
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build IMDS client: {e}"))?;

        let mut url = format!(
            "{IMDS_TOKEN_URL}?api-version=2018-02-01&resource={AKV_RESOURCE}"
        );

        // User-assigned managed identity requires the client_id parameter.
        if let Some(mi_client_id) = &self.managed_identity_client_id {
            url.push_str(&format!("&client_id={mi_client_id}"));
        }

        let resp = imds_client.get(&url)
            .header("Metadata", "true")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!(
                "IMDS token request failed (not running on Azure?): {e}"
            ))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("IMDS token request failed (HTTP {status}): {body}");
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| anyhow::anyhow!("IMDS token: invalid JSON: {e}"))?;

        let access_token = json["access_token"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("IMDS token: missing access_token"))?
            .to_string();

        // IMDS returns `expires_in` as a string (seconds) or `expires_on` as epoch.
        let expires_in = json["expires_in"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| json["expires_in"].as_u64())
            .unwrap_or_else(|| {
                // Fallback: compute from `expires_on` epoch timestamp.
                json["expires_on"]
                    .as_str()
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(|epoch| {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        epoch.saturating_sub(now)
                    })
                    .unwrap_or(3600)
            });

        Ok((access_token, expires_in))
    }

    // ── Method 3: Azure CLI ─────────────────────────────────────────────────

    /// Obtains a token by shelling out to `az account get-access-token`.
    ///
    /// Requires:
    /// - Azure CLI (`az`) installed and on `$PATH`
    /// - User is logged in (`az login` completed previously)
    ///
    /// The CLI returns JSON:
    /// ```json
    /// {
    ///   "accessToken": "eyJ...",
    ///   "expiresOn": "2025-01-15 14:30:00.000000",
    ///   "tokenType": "Bearer"
    /// }
    /// ```
    async fn token_from_azure_cli(&self) -> anyhow::Result<(String, u64)> {
        let output = tokio::time::timeout(CLI_TIMEOUT, async {
            tokio::process::Command::new("az")
                .args(["account", "get-access-token", "--resource", AKV_RESOURCE, "--output", "json"])
                .output()
                .await
        })
        .await
        .map_err(|_| anyhow::anyhow!(
            "Azure CLI timed out after {}s — is `az` installed?",
            CLI_TIMEOUT.as_secs()
        ))?
        .map_err(|e| anyhow::anyhow!(
            "Azure CLI failed to execute: {e}. Is `az` installed and on $PATH?"
        ))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!(
                "Azure CLI returned exit code {}: {stderr}",
                output.status.code().unwrap_or(-1)
            );
        }

        let stdout = String::from_utf8(output.stdout)
            .map_err(|e| anyhow::anyhow!("Azure CLI: invalid UTF-8 output: {e}"))?;

        let json: serde_json::Value = serde_json::from_str(&stdout)
            .map_err(|e| anyhow::anyhow!("Azure CLI: invalid JSON output: {e}"))?;

        let access_token = json["accessToken"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Azure CLI: missing accessToken in output"))?
            .to_string();

        // `expiresOn` is a datetime string like "2025-01-15 14:30:00.000000".
        // Parse it to compute remaining seconds.
        let expires_in = json["expiresOn"]
            .as_str()
            .and_then(|s| {
                // Try ISO-like formats that `az` emits.
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok()
                    .or_else(|| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f").ok())
            })
            .map(|dt| {
                let now = chrono::Utc::now().naive_utc();
                let diff = dt.signed_duration_since(now).num_seconds();
                if diff > 0 { diff as u64 } else { 0 }
            })
            .unwrap_or(3600);

        Ok((access_token, expires_in))
    }

    // ── Path resolution ─────────────────────────────────────────────────────

    /// Resolves the vault URL and secret name from a path.
    ///
    /// - If `vault_url_override` is set: path is just the secret name.
    /// - Otherwise: path is `<vault-name>/<secret-name>`.
    fn resolve_vault_and_secret<'a>(&self, path: &'a str) -> anyhow::Result<(String, &'a str)> {
        if let Some(url) = &self.vault_url_override {
            let vault_url = url.trim_end_matches('/').to_string();
            Ok((vault_url, path))
        } else {
            let slash = path.find('/').ok_or_else(|| anyhow::anyhow!(
                "Azure Key Vault path must be '<vault-name>/<secret-name>', got: '{path}'"
            ))?;
            let vault_name = &path[..slash];
            let secret_name = &path[slash + 1..];
            if secret_name.is_empty() {
                anyhow::bail!("Azure Key Vault: empty secret name in path '{path}'");
            }
            Ok((format!("https://{vault_name}.vault.azure.net"), secret_name))
        }
    }
}

// ── SecretProvider impl ─────────────────────────────────────────────────────

#[async_trait::async_trait]
impl SecretProvider for AzureKeyVaultProvider {
    async fn get_secret(&self, path: &str) -> anyhow::Result<HashMap<String, String>> {
        // Check cache.
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(path) {
                tracing::debug!(
                    path = path,
                    fields = cached.len(),
                    "azure_kv: returning cached secret"
                );
                return Ok(cached.clone());
            }
        }

        let token = self.ensure_token().await?;
        let (vault_url, secret_name) = self.resolve_vault_and_secret(path)?;

        let url = format!(
            "{vault_url}/secrets/{secret_name}?api-version={AKV_API_VERSION}"
        );

        tracing::debug!(
            url = %url,
            path = path,
            vault_url = %vault_url,
            secret_name = secret_name,
            "azure_kv: fetching secret"
        );

        let resp = self.client.get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Azure Key Vault GET {url}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "Azure Key Vault secret '{path}' fetch failed (HTTP {status}): {body}"
            );
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| anyhow::anyhow!(
                "Azure Key Vault secret '{path}': invalid JSON response: {e}"
            ))?;

        // Azure KV response: { "value": "...", "id": "...", ... }
        let value_str = json["value"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!(
                "Azure Key Vault secret '{path}': missing 'value' field in response"
            ))?;

        tracing::debug!(
            path = path,
            value_len = value_str.len(),
            value_preview = %if value_str.len() > 120 {
                format!("{}...", &value_str[..120])
            } else {
                value_str.to_string()
            },
            "azure_kv: raw secret value retrieved"
        );

        // Parse value as structured (JSON -> YAML -> plain string).
        let result = super::parse_structured_value(value_str);

        tracing::debug!(
            path = path,
            fields = result.len(),
            field_keys = ?result.keys().collect::<Vec<_>>(),
            "azure_kv: parsed secret fields"
        );

        {
            let mut cache = self.cache.write().await;
            cache.insert(path.to_string(), result.clone());
        }

        Ok(result)
    }

    fn provider_name(&self) -> &str { "Azure Key Vault" }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn resolve_env_ref(s: &str) -> String {
    if let Some(var) = s.strip_prefix('$') {
        std::env::var(var).unwrap_or_else(|_| s.to_string())
    } else {
        s.to_string()
    }
}