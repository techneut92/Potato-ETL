//! Configuration types for the top-level `secrets:` block.
//!
//! ## Example
//!
//! ```yaml
//! secrets:
//!   vault:
//!     address: https://vault.internal:8200
//!     auth:
//!       method: token
//!       token: $VAULT_TOKEN
//!     mount: secret          # KV v2 mount point (default: "secret")
//!     namespace: admin       # Vault Enterprise namespace (optional)
//!
//!   azure:
//!     # Minimal config — auth is auto-detected (SPN → Managed Identity → az CLI)
//!     vault_url: https://my-vault.vault.azure.net
//!
//!     # Explicit service principal (optional — skips auto-detection):
//!     # tenant_id: $AZURE_TENANT_ID
//!     # client_id: $AZURE_CLIENT_ID
//!     # client_secret: $AZURE_CLIENT_SECRET
//!
//!     # User-assigned managed identity (optional — default is system-assigned):
//!     # managed_identity_client_id: 12345678-1234-...
//!
//!   gcp:
//!     project: my-gcp-project
//!     # Authentication: Application Default Credentials by default.
//!     # Explicit service account key:
//!     credentials_file: /path/to/service-account.json
//! ```

use serde::{Deserialize, Serialize};

/// Top-level secret manager configuration.
///
/// Each field corresponds to a supported provider.  Providers that are `None`
/// are not used — secret references targeting an unconfigured provider will
/// produce an error at resolution time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecretsConfig {
    /// HashiCorp Vault configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<VaultConfig>,

    /// Azure Key Vault configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure: Option<AzureKeyVaultConfig>,

    /// Google Cloud Secret Manager configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gcp: Option<GcpSecretManagerConfig>,
}

impl SecretsConfig {
    pub fn is_empty(&self) -> bool {
        self.vault.is_none() && self.azure.is_none() && self.gcp.is_none()
    }
}

// ── HashiCorp Vault ──────────────────────────────────────────────────────────

/// HashiCorp Vault configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultConfig {
    /// Vault server address (e.g. `https://vault.internal:8200`).
    ///
    /// Falls back to `$VAULT_ADDR` environment variable if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,

    /// Authentication method.
    #[serde(default)]
    pub auth: VaultAuth,

    /// KV v2 secrets engine mount point.  Default: `"secret"`.
    #[serde(default = "default_vault_mount")]
    pub mount: String,

    /// Vault Enterprise namespace (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

fn default_vault_mount() -> String { "secret".into() }

/// Vault authentication method.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum VaultAuth {
    /// Token-based authentication.
    ///
    /// Falls back to `$VAULT_TOKEN` if `token` is not set.
    Token {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
    },

    /// AppRole authentication.
    AppRole {
        role_id: String,
        secret_id: String,
    },

    /// Kubernetes service account authentication.
    Kubernetes {
        role: String,
        /// Path to the mounted service account token.
        /// Default: `/var/run/secrets/kubernetes.io/serviceaccount/token`
        #[serde(default = "default_k8s_token_path")]
        token_path: String,
        /// Vault auth mount path.  Default: `"kubernetes"`.
        #[serde(default = "default_k8s_mount")]
        mount_path: String,
    },
}

impl Default for VaultAuth {
    fn default() -> Self {
        Self::Token { token: None }
    }
}

fn default_k8s_token_path() -> String {
    "/var/run/secrets/kubernetes.io/serviceaccount/token".into()
}
fn default_k8s_mount() -> String { "kubernetes".into() }

// ── Azure Key Vault ──────────────────────────────────────────────────────────

/// Azure Key Vault configuration.
///
/// Authentication follows a **credential chain** (similar to Python's
/// `DefaultAzureCredential`), tried in order:
///
/// 1. **Service Principal** — if `tenant_id` + `client_id` + `client_secret`
///    are available (from config or env vars `AZURE_TENANT_ID`,
///    `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`).
/// 2. **Managed Identity** — Azure IMDS endpoint (for AKS, App Service, VMs).
///    Set `managed_identity_client_id` for user-assigned identities.
/// 3. **Azure CLI** — shells out to `az account get-access-token`.
///
/// The first method that succeeds is cached for subsequent requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureKeyVaultConfig {
    /// Vault URL (e.g. `https://my-vault.vault.azure.net`).
    ///
    /// When using the `secret::azure/<vault-name>/<secret>` format,
    /// the vault URL is constructed automatically.  This field overrides
    /// the vault name from the path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault_url: Option<String>,

    /// Azure AD tenant ID.
    /// Falls back to `$AZURE_TENANT_ID`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,

    /// Azure AD client (application) ID.
    /// Falls back to `$AZURE_CLIENT_ID`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// Azure AD client secret.
    /// Falls back to `$AZURE_CLIENT_SECRET`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,

    /// Client ID for a **user-assigned** managed identity.
    ///
    /// Leave unset for system-assigned managed identity.
    /// Only used when the SPN credential chain step is skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_identity_client_id: Option<String>,
}

// ── Google Cloud Secret Manager ──────────────────────────────────────────────

/// Google Cloud Secret Manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcpSecretManagerConfig {
    /// GCP project ID.
    ///
    /// When using the `secret::gcp/<project>/<secret>` format, the project
    /// is taken from the path.  This field is a default when the path
    /// contains only a secret name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,

    /// Path to a service account JSON key file.
    /// Falls back to `$GOOGLE_APPLICATION_CREDENTIALS`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_file: Option<String>,
}