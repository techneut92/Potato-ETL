//! Structured, driver-specific connection parameters.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use super::auth::{AuthConfig, DbAuth};
use super::auth::FileAuth;
use super::encoding::pct_encode;
use super::serde_helpers::deserialize_null_as_default;
use super::driver_options::{
    is_default,
    PostgresOptions, MssqlOptions, OracleOptions, MySqlOptions,
    DatabricksOptions, DatabricksOdbcOptions,
    StepDriverOptions,
};

// ── ConnectionDef ─────────────────────────────────────────────────────────────

/// A named connection defined in the top-level `connections` map.
pub type ConnectionDef = ConnParams;

/// Structured, driver-specific connection parameters.
///
/// Discriminated by the `driver` field.  Passwords and usernames may contain
/// any characters; percent-encoding is handled automatically in `to_url()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "driver", rename_all = "snake_case")]
pub enum ConnParams {
    /// PostgreSQL connection.
    #[serde(alias = "postgresql")]
    Postgres {
        host:     String,
        #[serde(skip_serializing_if = "Option::is_none")]
        port:     Option<u16>,
        database: String,
        auth:     DbAuth,
        #[serde(default, deserialize_with = "deserialize_null_as_default", skip_serializing_if = "is_default")]
        options:  PostgresOptions,
    },

    /// Microsoft SQL Server connection.
    Mssql {
        host:     String,
        #[serde(skip_serializing_if = "Option::is_none")]
        port:     Option<u16>,
        database: String,
        auth:     DbAuth,
        #[serde(default, deserialize_with = "deserialize_null_as_default", skip_serializing_if = "is_default")]
        options:  MssqlOptions,
    },

    /// Oracle Database connection.
    Oracle {
        auth: DbAuth,
        #[serde(skip_serializing_if = "Option::is_none")]
        host: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        port: Option<u16>,
        #[serde(skip_serializing_if = "Option::is_none")]
        service: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        sid: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tns: Option<String>,
        #[serde(default, deserialize_with = "deserialize_null_as_default", skip_serializing_if = "is_default")]
        options: OracleOptions,
    },

    /// MySQL, Amazon Aurora (MySQL), and MariaDB.
    #[serde(alias = "aurora", alias = "mariadb")]
    Mysql {
        host:     String,
        #[serde(skip_serializing_if = "Option::is_none")]
        port:     Option<u16>,
        database: String,
        auth:     DbAuth,
        #[serde(default, deserialize_with = "deserialize_null_as_default", skip_serializing_if = "is_default")]
        options:  MySqlOptions,
    },

    /// Databricks SQL warehouse via the HTTP SQL Statement API.
    Databricks {
        host:      String,
        http_path: String,
        auth:      DbAuth,
        #[serde(default, deserialize_with = "deserialize_null_as_default", skip_serializing_if = "is_default")]
        options:   DatabricksOptions,
    },

    /// REST API endpoint.
    RestApi {
        base_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth: Option<AuthConfig>,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        headers: HashMap<String, String>,
        #[serde(default = "default_rest_timeout")]
        timeout_secs: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rate_limit_rps: Option<f64>,
    },

    // ── File transport connections ────────────────────────────────────────────

    /// Local filesystem.
    ///
    /// ```yaml
    /// connections:
    ///   local_data:
    ///     driver: local
    ///     base_path: /data/warehouse
    /// ```
    Local {
        /// Base directory.  Step paths are resolved relative to this.
        #[serde(default)]
        base_path: String,
    },

    /// SFTP (SSH File Transfer Protocol).
    ///
    /// ```yaml
    /// connections:
    ///   reports_sftp:
    ///     driver: sftp
    ///     host: sftp.example.com
    ///     port: 22
    ///     auth:
    ///       type: key
    ///       username: deploy
    ///       private_key: "${SFTP_KEY}"
    ///     base_path: /uploads/reports
    /// ```
    Sftp {
        host: String,
        #[serde(default = "default_sftp_port", skip_serializing_if = "is_default_sftp_port")]
        port: u16,
        auth: FileAuth,
        #[serde(default)]
        base_path: String,
        /// Known host key fingerprint for strict host key checking.
        /// When absent, host key verification is skipped (development only).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_key: Option<String>,
    },

    /// Amazon S3 or S3-compatible object storage (MinIO, R2, DigitalOcean Spaces).
    ///
    /// ```yaml
    /// connections:
    ///   data_lake:
    ///     driver: s3
    ///     bucket: my-data-lake
    ///     region: eu-west-1
    ///     auth:
    ///       type: access_key
    ///       access_key_id: "${AWS_ACCESS_KEY_ID}"
    ///       secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
    ///     base_path: etl/output
    /// ```
    S3 {
        bucket: String,
        #[serde(default = "default_s3_region")]
        region: String,
        #[serde(default)]
        auth: FileAuth,
        #[serde(default)]
        base_path: String,
        /// Custom endpoint URL for S3-compatible services (MinIO, R2, etc.).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        /// Force path-style addressing (`endpoint/bucket/key` instead of
        /// `bucket.endpoint/key`).  Required for most S3-compatible services.
        #[serde(default)]
        force_path_style: bool,
    },

    /// Azure Blob Storage.
    ///
    /// ```yaml
    /// connections:
    ///   azure_storage:
    ///     driver: azure_blob
    ///     account: mystorageaccount
    ///     container: etl-data
    ///     auth:
    ///       type: connection_string
    ///       connection_string: "${AZURE_STORAGE_CONN}"
    ///     base_path: raw/incoming
    /// ```
    AzureBlob {
        account:   String,
        container: String,
        auth:      FileAuth,
        #[serde(default)]
        base_path: String,
    },

    /// Google Cloud Storage.
    ///
    /// ```yaml
    /// connections:
    ///   gcs_bucket:
    ///     driver: gcs
    ///     bucket: my-etl-bucket
    ///     auth:
    ///       type: service_account
    ///       credentials_file: "${GCP_SA_KEY_FILE}"
    ///     base_path: output
    /// ```
    Gcs {
        bucket: String,
        auth:   FileAuth,
        #[serde(default)]
        base_path: String,
    },

    /// Microsoft SharePoint Online (via MS Graph API).
    ///
    /// ```yaml
    /// connections:
    ///   company_sp:
    ///     driver: sharepoint
    ///     site_url: "https://company.sharepoint.com/sites/DataTeam"
    ///     auth:
    ///       type: client_credentials
    ///       tenant_id: "${AZURE_TENANT_ID}"
    ///       client_id: "${AZURE_CLIENT_ID}"
    ///       client_secret: "${AZURE_CLIENT_SECRET}"
    ///     base_path: /Shared Documents/ETL
    /// ```
    #[serde(alias = "sharepoint_online")]
    Sharepoint {
        /// SharePoint site URL, e.g. `https://company.sharepoint.com/sites/DataTeam`.
        site_url: String,
        auth:     FileAuth,
        /// Document library path.  Default: `Shared Documents`.
        #[serde(default = "default_sharepoint_base_path")]
        base_path: String,
        /// Drive ID override (optional).  When absent, uses the site's default
        /// document library.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        drive_id: Option<String>,
    },

    /// FTP / FTPS (explicit TLS).
    ///
    /// ```yaml
    /// connections:
    ///   legacy_ftp:
    ///     driver: ftp
    ///     host: ftp.example.com
    ///     port: 21
    ///     auth:
    ///       type: user_pass
    ///       username: etl_user
    ///       password: "${FTP_PASSWORD}"
    ///     tls: true
    ///     base_path: /incoming
    /// ```
    #[serde(alias = "ftps")]
    Ftp {
        host: String,
        #[serde(default = "default_ftp_port", skip_serializing_if = "is_default_ftp_port")]
        port: u16,
        #[serde(default)]
        auth: FileAuth,
        #[serde(default)]
        base_path: String,
        /// Enable explicit FTPS (TLS).  Default: `false`.
        #[serde(default)]
        tls: bool,
        /// Passive mode.  Default: `true`.
        #[serde(default = "default_true")]
        passive: bool,
    },

    /// SMB / CIFS (Windows file shares).
    ///
    /// ```yaml
    /// connections:
    ///   file_share:
    ///     driver: smb
    ///     host: fileserver.corp.local
    ///     share: DataDrop
    ///     auth:
    ///       type: user_pass
    ///       username: "DOMAIN\\etl_user"
    ///       password: "${SMB_PASSWORD}"
    ///     base_path: /etl/incoming
    /// ```
    #[serde(alias = "cifs")]
    Smb {
        host:  String,
        share: String,
        #[serde(default = "default_smb_port", skip_serializing_if = "is_default_smb_port")]
        port:  u16,
        #[serde(default)]
        auth:  FileAuth,
        #[serde(default)]
        base_path: String,
    },
}

fn default_rest_timeout() -> u64 { 30 }
fn default_sftp_port() -> u16 { 22 }
fn default_ftp_port() -> u16 { 21 }
fn default_smb_port() -> u16 { 445 }
fn default_s3_region() -> String { "us-east-1".into() }
fn default_sharepoint_base_path() -> String { "Shared Documents".into() }
fn default_true() -> bool { true }
fn is_default_sftp_port(p: &u16) -> bool { *p == 22 }
fn is_default_ftp_port(p: &u16) -> bool { *p == 21 }
fn is_default_smb_port(p: &u16) -> bool { *p == 445 }

impl Default for FileAuth {
    fn default() -> Self { Self::None }
}

impl ConnParams {
    /// Returns `true` if this is a file-transport connection
    /// (local, sftp, s3, azure_blob, gcs, sharepoint, ftp, smb).
    pub fn is_file_connection(&self) -> bool {
        matches!(self,
            Self::Local { .. } | Self::Sftp { .. } | Self::S3 { .. }
            | Self::AzureBlob { .. } | Self::Gcs { .. } | Self::Sharepoint { .. }
            | Self::Ftp { .. } | Self::Smb { .. }
        )
    }

    /// Returns the `base_path` for file-transport connections, or `None` for
    /// database / REST API connections.
    pub fn file_base_path(&self) -> Option<&str> {
        match self {
            Self::Local      { base_path, .. } => Some(base_path),
            Self::Sftp       { base_path, .. } => Some(base_path),
            Self::S3         { base_path, .. } => Some(base_path),
            Self::AzureBlob  { base_path, .. } => Some(base_path),
            Self::Gcs        { base_path, .. } => Some(base_path),
            Self::Sharepoint { base_path, .. } => Some(base_path),
            Self::Ftp        { base_path, .. } => Some(base_path),
            Self::Smb        { base_path, .. } => Some(base_path),
            _ => None,
        }
    }

    /// Resolves a relative file path against this connection's `base_path`.
    ///
    /// Returns the full path suitable for the file transport layer.
    /// Errors if this connection is not a file-transport connection.
    pub fn resolve_file_path(&self, relative_path: &str) -> anyhow::Result<String> {
        let base = self.file_base_path().ok_or_else(|| anyhow::anyhow!(
            "Connection is not a file-transport connection (driver: {:?}). \
             File steps can only reference local, sftp, s3, azure_blob, gcs, \
             sharepoint, ftp, or smb connections.",
            self.driver_name()
        ))?;
        if base.is_empty() {
            Ok(relative_path.to_string())
        } else {
            let base = base.trim_end_matches('/');
            let rel  = relative_path.trim_start_matches('/');
            Ok(format!("{base}/{rel}"))
        }
    }

    /// Returns the driver name as a string.
    pub fn driver_name(&self) -> &'static str {
        match self {
            Self::Postgres { .. }   => "postgres",
            Self::Mssql { .. }      => "mssql",
            Self::Oracle { .. }     => "oracle",
            Self::Mysql { .. }      => "mysql",
            Self::Databricks { .. } => "databricks",
            Self::RestApi { .. }    => "rest_api",
            Self::Local { .. }      => "local",
            Self::Sftp { .. }       => "sftp",
            Self::S3 { .. }         => "s3",
            Self::AzureBlob { .. }  => "azure_blob",
            Self::Gcs { .. }        => "gcs",
            Self::Sharepoint { .. } => "sharepoint",
            Self::Ftp { .. }        => "ftp",
            Self::Smb { .. }        => "smb",
        }
    }

    /// Returns the resolved database connection URL.
    pub fn to_url(&self) -> anyhow::Result<String> {
        match self {
            Self::Postgres { host, port, database, auth, options } => {
                let port = port.unwrap_or(5432);
                let (u, p) = require_user_pass(auth, "postgres")?;
                let base = format!("postgresql://{u}:{p}@{host}:{port}/{database}");
                let mut qs: Vec<String> = Vec::new();
                encode_auth_qs(auth, &mut qs);
                if let Some(m) = &options.ssl { qs.push(format!("sslmode={}", pct_encode(m))); }
                if let Some(t) = options.connect_timeout { qs.push(format!("connect_timeout={t}")); }
                if let Some(n) = &options.application_name { qs.push(format!("application_name={}", pct_encode(n))); }
                if qs.is_empty() { Ok(base) } else { Ok(format!("{base}?{}", qs.join("&"))) }
            }

            Self::Mssql { host, port, database, auth, options } => {
                let port = port.unwrap_or(1433);
                let (u, p) = require_user_pass(auth, "mssql")?;
                let base = format!("mssql://{u}:{p}@{host}:{port}/{database}");
                let mut qs: Vec<String> = Vec::new();
                encode_auth_qs(auth, &mut qs);
                if let Some(n) = &options.application_name { qs.push(format!("application_name={}", pct_encode(n))); }
                if let Some(t) = options.login_timeout { qs.push(format!("login_timeout={t}")); }
                if options.trust_cert { qs.push("trust_cert=true".to_string()); }
                let eff = options.effective_mode();
                if eff != "tiberius" { qs.push(format!("mode={eff}")); }
                if let Some(ref path) = options.bcp_path { qs.push(format!("bcp_path={}", pct_encode(path))); }
                if let Some(n) = options.batch_size { qs.push(format!("batch_size={n}")); }
                if qs.is_empty() { Ok(base) } else { Ok(format!("{base}?{}", qs.join("&"))) }
            }

            Self::Oracle { auth, host, port, service, sid, tns, options: _ } => {
                let (u, p) = require_user_pass(auth, "oracle")?;
                if let Some(tns_str) = tns {
                    return Ok(format!("oracle://{u}:{p}@{tns_str}"));
                }
                let host_str = host.as_deref().ok_or_else(|| anyhow::anyhow!(
                    "Oracle connection: supply either `tns` or `host` (with `service` or `sid`)"
                ))?;
                let port_num = port.unwrap_or(1521);
                let target   = service.as_deref()
                    .or(sid.as_deref())
                    .ok_or_else(|| anyhow::anyhow!(
                        "Oracle connection: supply `service` or `sid` when using host-based mode"
                    ))?;
                Ok(format!("oracle://{u}:{p}@{host_str}:{port_num}/{target}"))
            }

            Self::Mysql { host, port, database, auth, options } => {
                let port = port.unwrap_or(3306);
                let (u, p) = require_user_pass(auth, "mysql")?;
                let base = format!("mysql://{u}:{p}@{host}:{port}/{database}");
                let mut qs: Vec<String> = Vec::new();
                encode_auth_qs(auth, &mut qs);
                if let Some(m) = &options.ssl_mode { qs.push(format!("ssl-mode={m}")); }
                if let Some(t) = options.connect_timeout { qs.push(format!("connect_timeout={t}")); }
                if let Some(c) = &options.charset { qs.push(format!("charset={c}")); }
                if qs.is_empty() { Ok(base) } else { Ok(format!("{base}?{}", qs.join("&"))) }
            }

            Self::Databricks { host, http_path, auth, options } => {
                let mut qs: Vec<String> = Vec::new();
                match auth {
                    DbAuth::Pat { token } =>
                        qs.push(format!("token={}", pct_encode(token))),
                    DbAuth::OAuth2ClientCredentials { client_id, client_secret } => {
                        qs.push(format!("client_id={}", pct_encode(client_id)));
                        qs.push(format!("client_secret={}", pct_encode(client_secret)));
                    }
                    other => anyhow::bail!(
                        "Databricks only supports `pat` or `oauth2_client_credentials` auth, got: {other:?}"
                    ),
                }
                if let Some(c) = &options.catalog { qs.push(format!("catalog={c}")); }
                if let Some(s) = &options.schema  { qs.push(format!("schema={s}")); }
                if let Some(t) = options.connect_timeout { qs.push(format!("connect_timeout={t}")); }
                let eff = options.effective_mode();
                if eff != "api" { qs.push(format!("mode={eff}")); }
                if let Some(p) = &options.odbc.driver_path { qs.push(format!("odbc_driver_path={}", pct_encode(p))); }
                if let Some(v) = options.odbc.port { qs.push(format!("odbc_port={v}")); }
                if let Some(v) = options.odbc.ssl { qs.push(format!("odbc_ssl={v}")); }
                if let Some(v) = options.odbc.thrift_transport { qs.push(format!("odbc_thrift_transport={v}")); }
                if let Some(v) = options.odbc.use_native_query { qs.push(format!("odbc_use_native_query={v}")); }
                if let Some(v) = options.odbc.string_column_length { qs.push(format!("odbc_string_column_length={v}")); }
                if let Some(v) = options.odbc.use_unicode_sql_character_types { qs.push(format!("odbc_use_unicode_sql_char_types={v}")); }
                if let Some(v) = options.odbc.use_long_varchar { qs.push(format!("odbc_use_long_varchar={v}")); }
                for p in &options.url_params { qs.push(format!("url_param={}", pct_encode(p))); }
                Ok(format!("databricks://{host}{http_path}?{}", qs.join("&")))
            }

            Self::RestApi { .. } => anyhow::bail!(
                "REST API connections cannot be used as database connections. \
                 Reference this connection from a `rest_api` or `rest_api_sink` step."
            ),

            // File-transport connections do not have database URLs.
            _ => anyhow::bail!(
                "File connection (driver: '{}') cannot be used as a database connection. \
                 Reference this connection from a file step (`read_csv`, `read_json`, \
                 `write_csv`, `write_json`) using `from.connection` or `target.connection`.",
                self.driver_name()
            ),
        }
    }

    /// Returns connection-level ODBC options if this is a Databricks connection.
    pub fn databricks_odbc_opts(&self) -> Option<&DatabricksOdbcOptions> {
        match self {
            Self::Databricks { options, .. } => Some(&options.odbc),
            _ => None,
        }
    }

    /// Extracts connection-level driver options as a `StepDriverOptions`.
    ///
    /// This lets connection-level defaults (e.g. `staging_table`, `max_connections`)
    /// propagate to sinks without repeating them on every step.  Step-level
    /// options override these defaults via [`StepDriverOptions::merge_from`].
    pub fn to_step_driver_options(&self) -> StepDriverOptions {
        match self {
            Self::Postgres { options, .. } => StepDriverOptions {
                postgres: Some(options.clone()),
                ..StepDriverOptions::default()
            },
            Self::Mssql { options, .. } => StepDriverOptions {
                mssql: Some(options.clone()),
                mode: Some(options.effective_mode().to_owned()),
                bcp_path: options.bcp_path.clone(),
                ..StepDriverOptions::default()
            },
            Self::Mysql { options, .. } => StepDriverOptions {
                mysql: Some(options.clone()),
                ..StepDriverOptions::default()
            },
            Self::Databricks { options, .. } => StepDriverOptions {
                // Propagate mode so step-level options inherit the
                // connection-level transport setting (api/odbc/thrift).
                mode: Some(options.effective_mode().to_owned()),
                init_sql: options.init_sql.clone(),
                ..StepDriverOptions::default()
            },
            Self::Oracle { options, .. } => StepDriverOptions {
                oracle: Some(options.clone()),
                ..StepDriverOptions::default()
            },
            _ => StepDriverOptions::default(),
        }
    }
}

// ── DbAuth helpers ────────────────────────────────────────────────────────────

/// Extracts percent-encoded `(username, password)` from a [`DbAuth`].
fn require_user_pass(auth: &DbAuth, driver: &str) -> anyhow::Result<(String, String)> {
    match auth {
        DbAuth::UserPass { username, password } =>
            Ok((pct_encode(username), pct_encode(password))),
        DbAuth::AwsIam { username, .. } =>
            Ok((pct_encode(username), String::new())),
        DbAuth::Kerberos { .. } | DbAuth::WindowsIntegrated | DbAuth::Certificate { .. } | DbAuth::None =>
            Ok((String::new(), String::new())),
        other => anyhow::bail!(
            "Auth type `{other:?}` is not supported for the `{driver}` driver."
        ),
    }
}

/// Encodes non-`user_pass` authentication information as URL query parameters.
fn encode_auth_qs(auth: &DbAuth, qs: &mut Vec<String>) {
    match auth {
        DbAuth::UserPass { .. } => {}
        DbAuth::Kerberos { principal, keytab } => {
            qs.push("auth_type=kerberos".to_string());
            qs.push(format!("principal={}", pct_encode(principal)));
            if let Some(kt) = keytab { qs.push(format!("keytab={}", pct_encode(kt))); }
        }
        DbAuth::WindowsIntegrated => { qs.push("auth_type=windows_integrated".to_string()); }
        DbAuth::Certificate { cert_path, key_path, ca_path } => {
            qs.push("auth_type=certificate".to_string());
            qs.push(format!("cert_path={}", pct_encode(cert_path)));
            qs.push(format!("key_path={}", pct_encode(key_path)));
            if let Some(ca) = ca_path { qs.push(format!("ca_path={}", pct_encode(ca))); }
        }
        DbAuth::AwsIam { region, .. } => {
            qs.push("auth_type=aws_iam".to_string());
            qs.push(format!("region={}", pct_encode(region)));
        }
        DbAuth::None => { qs.push("auth_type=none".to_string()); }
        _ => {}
    }
}