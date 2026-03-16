//! Cloud object storage file transport — S3, Azure Blob Storage, Google Cloud
//! Storage.
//!
//! Uses the [`object_store`](https://docs.rs/object_store) crate which provides
//! a unified API across all three cloud providers.
//!
//! ## Supported auth types
//!
//! | Driver | Auth types |
//! |--------|------------|
//! | S3     | `access_key`, `role_arn`, `default_credentials`, `none` |
//! | Azure  | `client_credentials`, `connection_string`, `sas_token`, `default_credentials` |
//! | GCS    | `service_account`, `default_credentials` |

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;

use potato_etl_common::config::{ConnParams, FileAuth};
use potato_etl_common::file_transport::FileTransport;

// ── ObjectStoreTransport ──────────────────────────────────────────────────────

/// Unified transport wrapping any `object_store::ObjectStore` implementation.
pub struct ObjectStoreTransport {
    store:    Arc<dyn ObjectStore>,
    describe: String,
}

#[async_trait::async_trait]
impl FileTransport for ObjectStoreTransport {
    async fn read_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let location = ObjectPath::from(path);
        let result = self.store.get(&location).await
            .map_err(|e| anyhow::anyhow!("{}: cannot read '{}': {e}", self.describe, path))?;
        let bytes = result.bytes().await
            .map_err(|e| anyhow::anyhow!("{}: error reading bytes from '{}': {e}", self.describe, path))?;
        Ok(bytes.to_vec())
    }

    async fn write_bytes(&self, path: &str, data: &[u8]) -> anyhow::Result<()> {
        let location = ObjectPath::from(path);
        self.store.put(&location, object_store::PutPayload::from(data.to_vec())).await
            .map_err(|e| anyhow::anyhow!("{}: cannot write '{}': {e}", self.describe, path))?;
        Ok(())
    }

    async fn exists(&self, path: &str) -> anyhow::Result<bool> {
        let location = ObjectPath::from(path);
        match self.store.head(&location).await {
            Ok(_)  => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(anyhow::anyhow!("{}: cannot check '{}': {e}", self.describe, path)),
        }
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        use futures::TryStreamExt;
        let prefix_path = ObjectPath::from(prefix);
        let mut entries = Vec::new();
        let mut stream = self.store.list(Some(&prefix_path));
        while let Some(meta) = stream.try_next().await
            .map_err(|e| anyhow::anyhow!("{}: cannot list '{}': {e}", self.describe, prefix))? {
            entries.push(meta.location.to_string());
        }
        Ok(entries)
    }

    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let location = ObjectPath::from(path);
        self.store.delete(&location).await
            .map_err(|e| anyhow::anyhow!("{}: cannot delete '{}': {e}", self.describe, path))?;
        Ok(())
    }

    fn describe(&self) -> String {
        self.describe.clone()
    }
}

// ── Factory functions ─────────────────────────────────────────────────────────

/// Create an S3 transport.
pub fn create_s3_transport(
    bucket:           &str,
    region:           &str,
    auth:             &FileAuth,
    endpoint:         Option<&str>,
    force_path_style: bool,
) -> anyhow::Result<Box<dyn FileTransport>> {
    use object_store::aws::AmazonS3Builder;

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region);

    match auth {
        FileAuth::AccessKey { access_key_id, secret_access_key, session_token } => {
            builder = builder
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);
            if let Some(token) = session_token {
                builder = builder.with_token(token);
            }
        }
        FileAuth::RoleArn { role_arn, external_id } => {
            tracing::warn!(
                "S3 role_arn auth: object_store will use the default credential \
                 chain which includes role assumption via instance profile or \
                 web identity. Explicit STS AssumeRole with role_arn='{}' \
                 (external_id={:?}) requires the AWS SDK credential chain.",
                role_arn, external_id
            );
            builder = builder.with_skip_signature(false);
        }
        FileAuth::DefaultCredentials => {}
        FileAuth::None => {
            builder = builder.with_allow_http(true).with_skip_signature(true);
        }
        other => anyhow::bail!(
            "S3: unsupported auth type '{}' -- use access_key, role_arn, \
             default_credentials, or none.",
            auth_type_name(other)
        ),
    }

    if let Some(ep) = endpoint {
        builder = builder.with_endpoint(ep).with_allow_http(ep.starts_with("http://"));
    }

    if force_path_style {
        builder = builder.with_virtual_hosted_style_request(false);
    }

    let store = builder.build()
        .map_err(|e| anyhow::anyhow!("S3: failed to build client for bucket '{bucket}': {e}"))?;

    let describe = match endpoint {
        Some(ep) => format!("S3-compatible ({ep}/{bucket})"),
        None     => format!("S3 (s3://{bucket}, {region})"),
    };

    Ok(Box::new(ObjectStoreTransport { store: Arc::new(store), describe }))
}

/// Create an Azure Blob Storage transport.
pub fn create_azure_transport(
    account:   &str,
    container: &str,
    auth:      &FileAuth,
) -> anyhow::Result<Box<dyn FileTransport>> {
    use object_store::azure::MicrosoftAzureBuilder;
    use object_store::azure::AzureConfigKey;

    let mut builder = MicrosoftAzureBuilder::new()
        .with_account(account)
        .with_container_name(container);

    match auth {
        FileAuth::ClientCredentials { tenant_id, client_id, client_secret } => {
            builder = builder
                .with_tenant_id(tenant_id)
                .with_client_id(client_id)
                .with_client_secret(client_secret);
        }
        FileAuth::ConnectionString { connection_string } => {
            // object_store 0.13 removed `with_connection_string`.
            // Parse the Azure connection string and set individual config keys.
            for part in connection_string.split(';') {
                let part = part.trim();
                if part.is_empty() { continue; }
                if let Some((key, value)) = part.split_once('=') {
                    match key {
                        "AccountName" => { builder = builder.with_account(value); }
                        "AccountKey"  => { builder = builder.with_access_key(value); }
                        "EndpointSuffix" => {
                            // Construct the full endpoint URL from protocol + suffix.
                            let protocol = connection_string
                                .split(';')
                                .find_map(|p| p.strip_prefix("DefaultEndpointsProtocol="))
                                .unwrap_or("https");
                            let endpoint = format!("{protocol}://{account}.blob.{value}");
                            builder = builder.with_config(AzureConfigKey::Endpoint, endpoint);
                        }
                        // DefaultEndpointsProtocol is handled via EndpointSuffix above.
                        // BlobEndpoint can override the endpoint directly.
                        "BlobEndpoint" => {
                            builder = builder.with_config(AzureConfigKey::Endpoint, value);
                        }
                        _ => {} // ignore other keys
                    }
                }
            }
        }
        FileAuth::SasToken { token } => {
            builder = builder.with_config(AzureConfigKey::SasKey, token);
        }
        FileAuth::DefaultCredentials => {
            builder = builder.with_use_fabric_endpoint(false);
        }
        other => anyhow::bail!(
            "Azure Blob: unsupported auth type '{}' -- use client_credentials, \
             connection_string, sas_token, or default_credentials.",
            auth_type_name(other)
        ),
    }

    let store = builder.build()
        .map_err(|e| anyhow::anyhow!(
            "Azure Blob: failed to build client for {account}/{container}: {e}"
        ))?;

    Ok(Box::new(ObjectStoreTransport {
        store: Arc::new(store),
        describe: format!("Azure Blob ({account}/{container})"),
    }))
}

/// Create a Google Cloud Storage transport.
pub fn create_gcs_transport(
    bucket: &str,
    auth:   &FileAuth,
) -> anyhow::Result<Box<dyn FileTransport>> {
    use object_store::gcp::GoogleCloudStorageBuilder;

    let mut builder = GoogleCloudStorageBuilder::new()
        .with_bucket_name(bucket);

    match auth {
        FileAuth::ServiceAccount { credentials_file } => {
            builder = builder.with_service_account_path(credentials_file);
        }
        FileAuth::DefaultCredentials => {}
        other => anyhow::bail!(
            "GCS: unsupported auth type '{}' -- use service_account or \
             default_credentials.",
            auth_type_name(other)
        ),
    }

    let store = builder.build()
        .map_err(|e| anyhow::anyhow!(
            "GCS: failed to build client for bucket '{bucket}': {e}"
        ))?;

    Ok(Box::new(ObjectStoreTransport {
        store: Arc::new(store),
        describe: format!("GCS (gs://{bucket})"),
    }))
}

/// Create from a `ConnParams` variant (S3, AzureBlob, or Gcs).
pub fn from_conn_params(conn: &ConnParams) -> anyhow::Result<Box<dyn FileTransport>> {
    match conn {
        ConnParams::S3 { bucket, region, auth, endpoint, force_path_style, .. } => {
            create_s3_transport(bucket, region, auth, endpoint.as_deref(), *force_path_style)
        }
        ConnParams::AzureBlob { account, container, auth, .. } => {
            create_azure_transport(account, container, auth)
        }
        ConnParams::Gcs { bucket, auth, .. } => {
            create_gcs_transport(bucket, auth)
        }
        other => anyhow::bail!(
            "cloud transport: expected S3, AzureBlob, or Gcs connection, got '{}'",
            other.driver_name()
        ),
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn auth_type_name(auth: &FileAuth) -> &'static str {
    match auth {
        FileAuth::UserPass { .. }          => "user_pass",
        FileAuth::Key { .. }               => "key",
        FileAuth::AccessKey { .. }         => "access_key",
        FileAuth::RoleArn { .. }           => "role_arn",
        FileAuth::ClientCredentials { .. } => "client_credentials",
        FileAuth::ServiceAccount { .. }    => "service_account",
        FileAuth::ConnectionString { .. }  => "connection_string",
        FileAuth::SasToken { .. }          => "sas_token",
        FileAuth::DefaultCredentials       => "default_credentials",
        FileAuth::None                     => "none",
    }
}