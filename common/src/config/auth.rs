//! Authentication configuration types for HTTP and database connections.

use serde::{Deserialize, Serialize};

// ── AuthConfig ────────────────────────────────────────────────────────────────

/// HTTP authentication configuration.
///
/// Used in REST API connections and REST API steps.  Supply `None` (the
/// `Option` variant) to make unauthenticated requests.
///
/// ```yaml
/// auth:
///   type: bearer
///   token: "eyJ..."
///
/// auth:
///   type: basic
///   username: etl_user
///   password: "s3cr3t"
///
/// auth:
///   type: api_key
///   header: X-API-Key        # any header name
///   key: "sk-live-..."
///
/// # AFAS Profit — AfasToken scheme via api_key:
/// auth:
///   type: api_key
///   header: Authorization
///   key: "AfasToken <base64_encoded_token_here>"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    /// `Authorization: Bearer <token>` header.
    Bearer { token: String },
    /// `Authorization: Basic <base64(user:pass)>` header.
    Basic  { username: String, password: String },
    /// Arbitrary header with a static key value.
    /// Use this for `X-API-Key`, `AfasToken`, or any other custom scheme.
    ApiKey { header: String, key: String },
}

// ── DbAuth ────────────────────────────────────────────────────────────────────

/// Database authentication configuration.
///
/// Used in the `auth:` block of every database connection.  The `type` field
/// selects the authentication method; remaining fields are method-specific.
///
/// ## Supported types
///
/// | `type`                       | Fields                                      | Drivers                         |
/// |------------------------------|---------------------------------------------|---------------------------------|
/// | `user_pass`                  | `username`, `password`                      | All SQL databases               |
/// | `pat`                        | `token`                                     | Databricks                      |
/// | `oauth2_client_credentials`  | `client_id`, `client_secret`                | Databricks (+ future OAuth2)    |
/// | `kerberos`                   | `principal`, `keytab` (optional)            | MSSQL, Postgres, Oracle         |
/// | `windows_integrated`         | *(none)*                                    | MSSQL                           |
/// | `certificate`                | `cert_path`, `key_path`, `ca_path`          | Postgres, MySQL, Oracle         |
/// | `aws_iam`                    | `username`, `region`                        | Aurora/RDS (Postgres, MySQL)    |
/// | `none`                       | *(none)*                                    | Trusted / local connections     |
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DbAuth {
    /// Username + password.  The most common method for SQL databases.
    UserPass {
        username: String,
        password: String,
    },

    /// Personal Access Token (PAT).  Used by Databricks.
    Pat {
        token: String,
    },

    /// OAuth2 client credentials grant (M2M / service principal).
    ///
    /// The library automatically exchanges `client_id` + `client_secret` for
    /// an access token via the provider's OIDC endpoint and caches/refreshes it.
    #[serde(rename = "oauth2_client_credentials")]
    OAuth2ClientCredentials {
        client_id:     String,
        client_secret: String,
    },

    /// Kerberos / SPNEGO authentication.
    ///
    /// Uses the system Kerberos infrastructure (MIT krb5 or Heimdal).
    /// If `keytab` is provided, the library kinits automatically;
    /// otherwise it expects a valid ticket in the default credential cache.
    Kerberos {
        /// Kerberos principal, e.g. `etl_user@CORP.LOCAL`.
        principal: String,
        /// Path to a keytab file.  Optional — when absent, the library
        /// uses the default credential cache (`KRB5CCNAME`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        keytab: Option<String>,
    },

    /// Windows Integrated Authentication (SSPI / NTLM / Negotiate).
    ///
    /// Only supported on Windows or via GSSAPI on Linux.
    /// No credentials needed — uses the current OS session.
    WindowsIntegrated,

    /// mTLS / client certificate authentication.
    ///
    /// The client presents a TLS certificate to the server.
    Certificate {
        /// Path to the client certificate (PEM or DER).
        cert_path: String,
        /// Path to the client private key (PEM or DER).
        key_path:  String,
        /// Path to the CA certificate for server verification.  Optional.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_path:   Option<String>,
    },

    /// AWS IAM token authentication for RDS / Aurora.
    ///
    /// Generates a short-lived auth token using the caller's AWS credentials
    /// (from env vars, instance profile, or SSO).
    AwsIam {
        /// Database username that has been granted `rds_iam` role.
        username: String,
        /// AWS region of the RDS/Aurora cluster, e.g. `eu-west-1`.
        region:   String,
    },

    /// No authentication.
    ///
    /// Used for trusted local connections (e.g. Postgres `trust` auth,
    /// Unix socket, or SQLite).
    None,
}

// ── FileAuth ──────────────────────────────────────────────────────────────────

/// File transport authentication configuration.
///
/// Used in the `auth:` block of file-based connections (`sftp`, `s3`,
/// `azure_blob`, `gcs`, `sharepoint`, `ftp`, `smb`).  The `local` driver
/// does not require authentication.
///
/// ## Supported types
///
/// | `type`                      | Fields                                      | Drivers                         |
/// |-----------------------------|---------------------------------------------|---------------------------------|
/// | `user_pass`                 | `username`, `password`                      | SFTP, FTP, SMB                  |
/// | `key`                       | `username`, `private_key`, `passphrase`     | SFTP                            |
/// | `access_key`                | `access_key_id`, `secret_access_key`        | S3                              |
/// | `role_arn`                  | `role_arn`, `external_id`                   | S3 (STS AssumeRole)             |
/// | `client_credentials`        | `tenant_id`, `client_id`, `client_secret`   | SharePoint, Azure Blob          |
/// | `service_account`           | `credentials_file`                          | GCS                             |
/// | `connection_string`         | `connection_string`                         | Azure Blob                      |
/// | `sas_token`                 | `token`                                     | Azure Blob                      |
/// | `default_credentials`       | *(none)*                                    | S3, GCS, Azure Blob             |
/// | `none`                      | *(none)*                                    | Local, anonymous FTP            |
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FileAuth {
    /// Username + password.  Used for SFTP password auth, FTP, SMB.
    UserPass {
        username: String,
        password: String,
    },

    /// SSH private key authentication.  Used for SFTP.
    ///
    /// ```yaml
    /// auth:
    ///   type: key
    ///   username: deploy
    ///   private_key: "${SFTP_PRIVATE_KEY}"   # PEM-encoded key or file path
    ///   passphrase: "${KEY_PASSPHRASE}"      # optional
    /// ```
    Key {
        username:    String,
        /// PEM-encoded private key string, or filesystem path to the key file.
        private_key: String,
        /// Optional passphrase for encrypted private keys.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        passphrase:  Option<String>,
    },

    /// AWS access key credentials.  Used for S3 and S3-compatible storage.
    AccessKey {
        access_key_id:     String,
        secret_access_key: String,
        /// Optional session token for temporary credentials (STS).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_token:     Option<String>,
    },

    /// AWS IAM role assumption via STS AssumeRole.
    RoleArn {
        /// IAM role ARN to assume, e.g. `arn:aws:iam::123456789012:role/ETLRole`.
        role_arn:     String,
        /// Optional external ID for cross-account access.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        external_id:  Option<String>,
    },

    /// OAuth2 client credentials (service principal).
    /// Used for SharePoint (MS Graph API) and Azure Blob Storage.
    ///
    /// ```yaml
    /// auth:
    ///   type: client_credentials
    ///   tenant_id: "${AZURE_TENANT_ID}"
    ///   client_id: "${AZURE_CLIENT_ID}"
    ///   client_secret: "${AZURE_CLIENT_SECRET}"
    /// ```
    ClientCredentials {
        tenant_id:     String,
        client_id:     String,
        client_secret: String,
    },

    /// GCP service account key file.  Used for Google Cloud Storage.
    ServiceAccount {
        /// Path to the service account JSON key file, or the JSON content itself.
        credentials_file: String,
    },

    /// Azure Storage connection string.
    ConnectionString {
        connection_string: String,
    },

    /// Azure Shared Access Signature (SAS) token.
    SasToken {
        token: String,
    },

    /// Use the default credential chain of the cloud provider.
    ///
    /// - **S3**: env vars → instance profile → ECS task role → SSO
    /// - **GCS**: `GOOGLE_APPLICATION_CREDENTIALS` → GCE metadata → gcloud CLI
    /// - **Azure**: env vars → managed identity → Azure CLI
    DefaultCredentials,

    /// No authentication.  Used for local filesystem and anonymous FTP.
    None,
}