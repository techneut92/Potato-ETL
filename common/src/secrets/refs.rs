//! Secret reference parsing.
//!
//! A secret reference is a string with the format:
//!
//! ```text
//! secret::<provider>/<path>[#field]
//! ```
//!
//! Examples:
//!
//! | Reference                                | Provider   | Path                        | Field      |
//! |------------------------------------------|------------|-----------------------------|------------|
//! | `secret::vault/prod/pg#password`         | vault      | prod/pg                     | password   |
//! | `secret::vault/prod/pg_connection`       | vault      | prod/pg_connection          | (none)     |
//! | `secret::azure/my-vault/pg-password`     | azure      | my-vault/pg-password        | (none)     |
//! | `secret::azure/my-vault/pg-config#host`  | azure      | my-vault/pg-config          | host       |
//! | `secret::gcp/my-project/pg-password`     | gcp        | my-project/pg-password      | (none)     |
//! | `secret::gcp/my-project/pg-config#port`  | gcp        | my-project/pg-config        | port       |

use std::fmt;

/// Identifies which secret manager provider to use.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SecretProviderKind {
    /// HashiCorp Vault (KV v2).
    HashiCorpVault,
    /// Azure Key Vault.
    AzureKeyVault,
    /// Google Cloud Secret Manager.
    GoogleSecretManager,
}

impl fmt::Display for SecretProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HashiCorpVault      => write!(f, "vault"),
            Self::AzureKeyVault       => write!(f, "azure"),
            Self::GoogleSecretManager => write!(f, "gcp"),
        }
    }
}

/// A parsed secret reference.
///
/// Parsed from strings like `secret::vault/prod/pg#password`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SecretRef {
    /// Which secret manager to query.
    pub provider: SecretProviderKind,
    /// Provider-specific path to the secret.
    ///
    /// - **Vault**: KV v2 mount path (e.g. `prod/pg` → `GET /v1/secret/data/prod/pg`)
    /// - **Azure**: `<vault-name>/<secret-name>` (e.g. `my-vault/pg-password`)
    /// - **GCP**:   `<project>/<secret-name>` (e.g. `my-project/pg-password`)
    pub path: String,
    /// Optional field within the secret value.
    ///
    /// When set, the secret value is treated as a JSON object and this field
    /// is extracted.  When `None`, the secret value is used as-is (string).
    pub field: Option<String>,
}

impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "secret::{}/{}", self.provider, self.path)?;
        if let Some(field) = &self.field {
            write!(f, "#{field}")?;
        }
        Ok(())
    }
}

/// The prefix that identifies a secret reference string.
pub const SECRET_PREFIX: &str = "secret::";

/// Returns `true` if the string is a secret reference.
pub fn is_secret_ref(s: &str) -> bool {
    s.starts_with(SECRET_PREFIX)
}

/// Parses a secret reference string into a [`SecretRef`].
///
/// Returns `None` if the string does not start with `secret::` or is malformed.
///
/// ## Format
///
/// ```text
/// secret::<provider>/<path>[#field]
/// ```
///
/// Provider aliases:
/// - `vault`, `hashicorp` → [`SecretProviderKind::HashiCorpVault`]
/// - `azure`, `akv`       → [`SecretProviderKind::AzureKeyVault`]
/// - `gcp`, `gsm`, `google` → [`SecretProviderKind::GoogleSecretManager`]
pub fn parse_secret_ref(s: &str) -> Option<SecretRef> {
    let rest = s.strip_prefix(SECRET_PREFIX)?;

    // Split provider from path at the first '/'.
    let slash = rest.find('/')?;
    let provider_str = &rest[..slash];
    let path_and_field = &rest[slash + 1..];

    if path_and_field.is_empty() {
        return None;
    }

    let provider = match provider_str.to_lowercase().as_str() {
        "vault" | "hashicorp" => SecretProviderKind::HashiCorpVault,
        "azure" | "akv"       => SecretProviderKind::AzureKeyVault,
        "gcp"   | "gsm" | "google" => SecretProviderKind::GoogleSecretManager,
        _ => return None,
    };

    // Split path from optional field at '#'.
    let (path, field) = match path_and_field.rfind('#') {
        Some(hash_pos) => {
            let p = &path_and_field[..hash_pos];
            let f = &path_and_field[hash_pos + 1..];
            if p.is_empty() || f.is_empty() {
                return None;
            }
            (p.to_string(), Some(f.to_string()))
        }
        None => (path_and_field.to_string(), None),
    };

    Some(SecretRef { provider, path, field })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_vault_with_field() {
        let r = parse_secret_ref("secret::vault/prod/pg#password").unwrap();
        assert_eq!(r.provider, SecretProviderKind::HashiCorpVault);
        assert_eq!(r.path, "prod/pg");
        assert_eq!(r.field.as_deref(), Some("password"));
    }

    #[test]
    fn parse_vault_without_field() {
        let r = parse_secret_ref("secret::vault/prod/pg_connection").unwrap();
        assert_eq!(r.provider, SecretProviderKind::HashiCorpVault);
        assert_eq!(r.path, "prod/pg_connection");
        assert_eq!(r.field, None);
    }

    #[test]
    fn parse_hashicorp_alias() {
        let r = parse_secret_ref("secret::hashicorp/prod/db#user").unwrap();
        assert_eq!(r.provider, SecretProviderKind::HashiCorpVault);
        assert_eq!(r.path, "prod/db");
        assert_eq!(r.field.as_deref(), Some("user"));
    }

    #[test]
    fn parse_azure() {
        let r = parse_secret_ref("secret::azure/my-vault/pg-password").unwrap();
        assert_eq!(r.provider, SecretProviderKind::AzureKeyVault);
        assert_eq!(r.path, "my-vault/pg-password");
        assert_eq!(r.field, None);
    }

    #[test]
    fn parse_azure_with_field() {
        let r = parse_secret_ref("secret::akv/my-vault/pg-config#host").unwrap();
        assert_eq!(r.provider, SecretProviderKind::AzureKeyVault);
        assert_eq!(r.path, "my-vault/pg-config");
        assert_eq!(r.field.as_deref(), Some("host"));
    }

    #[test]
    fn parse_gcp() {
        let r = parse_secret_ref("secret::gcp/my-project/pg-password").unwrap();
        assert_eq!(r.provider, SecretProviderKind::GoogleSecretManager);
        assert_eq!(r.path, "my-project/pg-password");
        assert_eq!(r.field, None);
    }

    #[test]
    fn parse_google_alias() {
        let r = parse_secret_ref("secret::google/proj/secret#key").unwrap();
        assert_eq!(r.provider, SecretProviderKind::GoogleSecretManager);
        assert_eq!(r.path, "proj/secret");
        assert_eq!(r.field.as_deref(), Some("key"));
    }

    #[test]
    fn parse_gsm_alias() {
        let r = parse_secret_ref("secret::gsm/proj/secret").unwrap();
        assert_eq!(r.provider, SecretProviderKind::GoogleSecretManager);
    }

    #[test]
    fn invalid_no_prefix() {
        assert!(parse_secret_ref("vault/prod/pg#password").is_none());
    }

    #[test]
    fn invalid_no_path() {
        assert!(parse_secret_ref("secret::vault/").is_none());
    }

    #[test]
    fn invalid_no_slash() {
        assert!(parse_secret_ref("secret::vault").is_none());
    }

    #[test]
    fn invalid_unknown_provider() {
        assert!(parse_secret_ref("secret::aws/prod/pg").is_none());
    }

    #[test]
    fn invalid_empty_field() {
        assert!(parse_secret_ref("secret::vault/prod/pg#").is_none());
    }

    #[test]
    fn display_roundtrip() {
        let r = parse_secret_ref("secret::vault/prod/pg#password").unwrap();
        assert_eq!(r.to_string(), "secret::vault/prod/pg#password");
    }

    #[test]
    fn display_no_field() {
        let r = parse_secret_ref("secret::vault/prod/pg_conn").unwrap();
        assert_eq!(r.to_string(), "secret::vault/prod/pg_conn");
    }

    #[test]
    fn is_secret_ref_positive() {
        assert!(is_secret_ref("secret::vault/x"));
    }

    #[test]
    fn is_secret_ref_negative() {
        assert!(!is_secret_ref("not_a_secret"));
    }
}
