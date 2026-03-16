//! Secret manager integration.
//!
//! Resolves `secret::provider/path[#field]` references in pipeline YAML/JSON
//! before the config is deserialized into typed structs.  This makes secret
//! resolution **transparent** — existing types (`ConnParams`, `DbAuth`, etc.)
//! don't need any changes.
//!
//! ## Supported providers
//!
//! | Provider              | Prefix          | Aliases              | Auth chain                                      |
//! |-----------------------|-----------------|----------------------|-------------------------------------------------|
//! | HashiCorp Vault KV v2 | `secret::vault` | `secret::hashicorp`  | Token → AppRole → Kubernetes SA                 |
//! | Azure Key Vault       | `secret::azure` | `secret::akv`        | SPN → Managed Identity → Azure CLI              |
//! | Google Secret Manager | `secret::gcp`   | `secret::gsm`, `secret::google` | Service Account key → GCE metadata server |
//!
//! ## Secret value formats
//!
//! Secret values are auto-detected as **JSON**, **YAML**, or **plain string**
//! (tried in that order).  This means you can store structured secrets in any
//! format and reference individual fields with `#field`:
//!
//! ```yaml
//! # Secret stored as YAML in Azure Key Vault:
//! #   host: db.internal
//! #   port: 5432
//! #   password: hunter2
//!
//! connections:
//!   pg:
//!     host: "secret::azure/my-vault/db-config#host"
//!     port: "secret::azure/my-vault/db-config#port"
//!     password: "secret::azure/my-vault/db-config#password"
//! ```
//!
//! ## Usage patterns
//!
//! ```yaml
//! # Individual field references (recommended)
//! connections:
//!   pg_prod:
//!     driver: postgres
//!     host: prod-db.internal
//!     database: analytics
//!     auth:
//!       type: user_pass
//!       username: "secret::vault/prod/pg#username"
//!       password: "secret::vault/prod/pg#password"
//!
//! # Whole connection from vault (JSON object stored as secret value)
//! connections:
//!   pg_staging: "secret::vault/staging/pg_connection"
//! ```

pub mod refs;
pub mod providers;
pub mod resolve;

pub use refs::{SecretRef, SecretProviderKind, parse_secret_ref, is_secret_ref, SECRET_PREFIX};
pub use providers::SecretProvider;
pub use resolve::resolve_secrets_in_value;