//! Google Cloud Secret Manager provider.
//!
//! Fetches secrets from Google Cloud Secret Manager via the REST API.
//!
//! ## Authentication
//!
//! Uses a service account JSON key file to obtain an OAuth2 access token.
//! Falls back to `$GOOGLE_APPLICATION_CREDENTIALS` environment variable.
//!
//! In GCP-hosted environments (GKE, Cloud Run, Compute Engine), the metadata
//! server provides tokens automatically when no explicit credentials are configured.
//!
//! ## Secret path format
//!
//! ```text
//! secret::gcp/<project>/<secret-name>[#field]
//! ```
//!
//! ## API
//!
//! ```text
//! GET https://secretmanager.googleapis.com/v1/projects/<project>/secrets/<secret>/versions/latest:access
//! Authorization: Bearer <token>
//! ```
//!
//! The response contains the secret payload as base64-encoded data.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use base64::Engine;
use tokio::sync::RwLock;

use super::SecretProvider;
use crate::config::secrets::GcpSecretManagerConfig;

const GSM_BASE_URL: &str = "https://secretmanager.googleapis.com/v1";
const GCE_METADATA_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
const GSM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// Safety margin: refresh tokens 5 minutes before they expire.
const TOKEN_EXPIRY_MARGIN_SECS: u64 = 300;

/// Cached token with expiry tracking.
struct CachedToken {
    access_token: String,
    /// When this token was obtained.
    obtained_at: Instant,
    /// Token lifetime in seconds (from the provider response or JWT `exp`).
    expires_in_secs: u64,
}

impl CachedToken {
    /// Returns `true` if the token is still valid (with safety margin).
    fn is_valid(&self) -> bool {
        let elapsed = self.obtained_at.elapsed().as_secs();
        elapsed + TOKEN_EXPIRY_MARGIN_SECS < self.expires_in_secs
    }
}

/// Google Cloud Secret Manager provider.
pub struct GcpSecretManagerProvider {
    /// Default project (from config).
    default_project: Option<String>,
    /// Service account credentials (parsed from JSON key file).
    service_account: Option<ServiceAccountKey>,
    client: reqwest::Client,
    /// Cached access token with expiry.
    token:  Arc<RwLock<Option<CachedToken>>>,
    /// Per-path cache.
    cache:  Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
}

/// Minimal service account key fields needed for token exchange.
#[derive(Debug, Clone, serde::Deserialize)]
#[allow(dead_code)]
struct ServiceAccountKey {
    client_email:  String,
    private_key:   String,
    token_uri:     String,
}

impl GcpSecretManagerProvider {
    /// Creates a new GCP Secret Manager provider from config.
    pub fn from_config(config: &GcpSecretManagerConfig) -> anyhow::Result<Self> {
        let creds_path = config.credentials_file.clone()
            .or_else(|| std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok());

        let service_account = if let Some(path) = creds_path {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!(
                    "GCP: failed to read credentials file '{path}': {e}"
                ))?;
            let key: ServiceAccountKey = serde_json::from_str(&content)
                .map_err(|e| anyhow::anyhow!(
                    "GCP: failed to parse credentials file '{path}': {e}"
                ))?;
            Some(key)
        } else {
            None // Will try metadata server (GCE/GKE/Cloud Run).
        };

        Ok(Self {
            default_project: config.project.clone(),
            service_account,
            client: reqwest::Client::new(),
            token: Arc::new(RwLock::new(None)),
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Obtains an access token, refreshing if expired.
    ///
    /// Priority:
    /// 1. Cached token (if still valid)
    /// 2. Service account key → JWT → token exchange
    /// 3. GCE metadata server (for workloads running on GCP)
    async fn ensure_token(&self) -> anyhow::Result<String> {
        // Check cached token validity.
        {
            let guard = self.token.read().await;
            if let Some(cached) = guard.as_ref() {
                if cached.is_valid() {
                    return Ok(cached.access_token.clone());
                }
                tracing::debug!("gcp: cached token expired, refreshing");
            }
        }

        let (new_token, expires_in) = if let Some(sa) = &self.service_account {
            self.token_from_service_account(sa).await?
        } else {
            self.token_from_metadata_server().await?
        };

        let mut guard = self.token.write().await;
        *guard = Some(CachedToken {
            access_token: new_token.clone(),
            obtained_at: Instant::now(),
            expires_in_secs: expires_in,
        });
        Ok(new_token)
    }

    /// Exchange a service account key for an access token.
    ///
    /// Creates a self-signed RS256 JWT and exchanges it for an OAuth2 token
    /// via Google's token endpoint.
    ///
    /// Returns `(access_token, expires_in_seconds)`.
    async fn token_from_service_account(
        &self,
        sa: &ServiceAccountKey,
    ) -> anyhow::Result<(String, u64)> {
        let jwt = self.sign_jwt(sa)?;

        // Exchange JWT for access token.
        let resp = self.client.post(&sa.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion",  &jwt),
            ])
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("GCP token exchange failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("GCP token exchange failed (HTTP {status}): {body}");
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| anyhow::anyhow!("GCP token exchange: invalid JSON: {e}"))?;

        let access_token = json["access_token"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("GCP token exchange: missing access_token"))?;

        // Google typically returns `expires_in` in seconds (default: 3600).
        let expires_in = json["expires_in"]
            .as_u64()
            .unwrap_or(3600);

        Ok((access_token, expires_in))
    }

    /// Build and sign a JWT using RS256 for Google OAuth2.
    ///
    /// Uses the `jsonwebtoken` crate for proper RSA-PKCS1-SHA256 signing
    /// with the service account's private key.
    fn sign_jwt(&self, sa: &ServiceAccountKey) -> anyhow::Result<String> {
        let now = chrono::Utc::now().timestamp() as u64;

        let claims = GcpJwtClaims {
            iss: sa.client_email.clone(),
            scope: GSM_SCOPE.to_string(),
            aud: sa.token_uri.clone(),
            iat: now,
            exp: now + 3600,
        };

        let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(sa.private_key.as_bytes())
            .map_err(|e| anyhow::anyhow!(
                "GCP: failed to parse service account private key (RSA PEM): {e}. \
                 Ensure the credentials file contains a valid RSA private key."
            ))?;

        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);

        jsonwebtoken::encode(&header, &claims, &encoding_key)
            .map_err(|e| anyhow::anyhow!("GCP: JWT signing failed: {e}"))
    }

    /// Obtain a token from the GCE metadata server (for workloads on GCP).
    ///
    /// Returns `(access_token, expires_in_seconds)`.
    async fn token_from_metadata_server(&self) -> anyhow::Result<(String, u64)> {
        let resp = self.client.get(GCE_METADATA_URL)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!(
                "GCP metadata server token request failed: {e}. \
                 Are you running on GCP? If not, set `secrets.gcp.credentials_file` \
                 or `$GOOGLE_APPLICATION_CREDENTIALS`."
            ))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "GCP metadata server token request failed (HTTP {status}): {body}"
            );
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| anyhow::anyhow!("GCP metadata token: invalid JSON: {e}"))?;

        let access_token = json["access_token"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("GCP metadata token: missing access_token"))?;

        let expires_in = json["expires_in"]
            .as_u64()
            .unwrap_or(3600);

        Ok((access_token, expires_in))
    }

    /// Resolves project and secret name from a path.
    ///
    /// Path format: `<project>/<secret-name>` or just `<secret-name>` (uses default project).
    fn resolve_project_and_secret<'a>(&self, path: &'a str) -> anyhow::Result<(String, &'a str)> {
        if let Some(slash) = path.find('/') {
            let project = &path[..slash];
            let secret = &path[slash + 1..];
            if secret.is_empty() {
                anyhow::bail!("GCP Secret Manager: empty secret name in path '{path}'");
            }
            Ok((project.to_string(), secret))
        } else if let Some(project) = &self.default_project {
            Ok((project.clone(), path))
        } else {
            anyhow::bail!(
                "GCP Secret Manager: path '{path}' has no project prefix and no default \
                 project configured. Use `secret::gcp/<project>/<secret>` or set \
                 `secrets.gcp.project` in config."
            )
        }
    }
}

/// JWT claims for Google OAuth2 service account authentication.
#[derive(Debug, serde::Serialize)]
struct GcpJwtClaims {
    iss:   String,
    scope: String,
    aud:   String,
    iat:   u64,
    exp:   u64,
}

#[async_trait::async_trait]
impl SecretProvider for GcpSecretManagerProvider {
    async fn get_secret(&self, path: &str) -> anyhow::Result<HashMap<String, String>> {
        // Check cache.
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(path) {
                return Ok(cached.clone());
            }
        }

        let token = self.ensure_token().await?;
        let (project, secret_name) = self.resolve_project_and_secret(path)?;

        let url = format!(
            "{GSM_BASE_URL}/projects/{project}/secrets/{secret_name}/versions/latest:access"
        );

        let resp = self.client.get(&url)
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("GCP Secret Manager GET {url}: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!(
                "GCP Secret Manager secret '{path}' fetch failed (HTTP {status}): {body}"
            );
        }

        let json: serde_json::Value = resp.json().await
            .map_err(|e| anyhow::anyhow!(
                "GCP Secret Manager secret '{path}': invalid JSON response: {e}"
            ))?;

        // Response: { "payload": { "data": "<base64>" }, ... }
        let data_b64 = json["payload"]["data"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!(
                "GCP Secret Manager secret '{path}': missing payload.data in response"
            ))?;

        let decoded = base64::engine::general_purpose::STANDARD.decode(data_b64)
            .map_err(|e| anyhow::anyhow!(
                "GCP Secret Manager secret '{path}': base64 decode error: {e}"
            ))?;

        let value_str = String::from_utf8(decoded)
            .map_err(|e| anyhow::anyhow!(
                "GCP Secret Manager secret '{path}': UTF-8 decode error: {e}"
            ))?;

        // Parse value as structured (JSON → YAML → plain string).
        let result = super::parse_structured_value(&value_str);

        tracing::debug!(
            path = path,
            fields = result.len(),
            "gcp_sm: fetched secret"
        );

        {
            let mut cache = self.cache.write().await;
            cache.insert(path.to_string(), result.clone());
        }

        Ok(result)
    }

    fn provider_name(&self) -> &str { "Google Secret Manager" }
}