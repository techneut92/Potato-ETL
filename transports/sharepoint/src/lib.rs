//! SharePoint Online file transport — read/write files via MS Graph API.
//!
//! Uses [`reqwest`](https://docs.rs/reqwest) to call the Microsoft Graph REST
//! API for file operations on SharePoint document libraries.
//!
//! ## Auth
//!
//! Only `client_credentials` (Azure AD app registration) is supported.  The
//! transport requests an OAuth2 token via the client credentials flow and
//! caches it until expiry.
//!
//! ## API surface
//!
//! | Operation | Graph endpoint |
//! |-----------|----------------|
//! | read      | `GET /drives/{id}/root:/{path}:/content` |
//! | write     | `PUT /drives/{id}/root:/{path}:/content` |
//! | exists    | `GET /drives/{id}/root:/{path}` (metadata) |
//! | list      | `GET /drives/{id}/root:/{path}:/children` |
//! | delete    | `DELETE /drives/{id}/root:/{path}` |

use reqwest::Client;
use serde::Deserialize;
use tokio::sync::Mutex;

use potato_etl_common::config::FileAuth;
use potato_etl_common::file_transport::FileTransport;

// ── SharePointTransport ───────────────────────────────────────────────────────

/// SharePoint Online transport backed by MS Graph API.
pub struct SharePointTransport {
    client:        Client,
    token:         Mutex<Option<CachedToken>>,
    drive_url:     String,
    tenant_id:     String,
    client_id:     String,
    client_secret: String,
}

struct CachedToken {
    access_token: String,
    expires_at:   std::time::Instant,
}

impl SharePointTransport {
    pub fn new(
        drive_url:     String,
        tenant_id:     String,
        client_id:     String,
        client_secret: String,
    ) -> Self {
        Self {
            client: Client::new(),
            token:  Mutex::new(None),
            drive_url,
            tenant_id,
            client_id,
            client_secret,
        }
    }

    async fn get_token(&self) -> anyhow::Result<String> {
        let mut guard = self.token.lock().await;
        if let Some(cached) = guard.as_ref() {
            if cached.expires_at > std::time::Instant::now() {
                return Ok(cached.access_token.clone());
            }
        }

        let token_url = format!(
            "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
            self.tenant_id
        );

        let resp = self.client
            .post(&token_url)
            .form(&[
                ("grant_type",    "client_credentials"),
                ("client_id",     &self.client_id),
                ("client_secret", &self.client_secret),
                ("scope",         "https://graph.microsoft.com/.default"),
            ])
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("SharePoint: token request failed: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("SharePoint: token request returned {status}: {body}");
        }

        let token_resp: TokenResponse = resp.json().await
            .map_err(|e| anyhow::anyhow!("SharePoint: cannot parse token response: {e}"))?;

        let expires_at = std::time::Instant::now()
            + std::time::Duration::from_secs(token_resp.expires_in.saturating_sub(60));

        let access_token = token_resp.access_token.clone();
        *guard = Some(CachedToken { access_token: token_resp.access_token, expires_at });

        tracing::debug!("SharePoint: acquired new access token (expires in {}s)", token_resp.expires_in);
        Ok(access_token)
    }

    fn file_url(&self, path: &str) -> String {
        let clean = path.trim_start_matches('/');
        format!("{}/root:/{clean}", self.drive_url)
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in:   u64,
}

#[derive(Deserialize)]
struct DriveItemList {
    value: Vec<DriveItem>,
}

#[derive(Deserialize)]
struct DriveItem {
    name: String,
    #[serde(default)]
    file: Option<serde_json::Value>,
}

#[async_trait::async_trait]
impl FileTransport for SharePointTransport {
    async fn read_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let token = self.get_token().await?;
        let url = format!("{}:/content", self.file_url(path));

        let resp = self.client.get(&url)
            .bearer_auth(&token)
            .send().await
            .map_err(|e| anyhow::anyhow!("SharePoint: cannot read '{path}': {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("SharePoint: read '{path}' returned {status}: {body}");
        }

        let bytes = resp.bytes().await
            .map_err(|e| anyhow::anyhow!("SharePoint: error reading bytes from '{path}': {e}"))?;
        Ok(bytes.to_vec())
    }

    async fn write_bytes(&self, path: &str, data: &[u8]) -> anyhow::Result<()> {
        let token = self.get_token().await?;
        let url = format!("{}:/content", self.file_url(path));

        let resp = self.client.put(&url)
            .bearer_auth(&token)
            .header("Content-Type", "application/octet-stream")
            .body(data.to_vec())
            .send().await
            .map_err(|e| anyhow::anyhow!("SharePoint: cannot write '{path}': {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("SharePoint: write '{path}' returned {status}: {body}");
        }

        Ok(())
    }

    async fn exists(&self, path: &str) -> anyhow::Result<bool> {
        let token = self.get_token().await?;
        let url = self.file_url(path);

        let resp = self.client.get(&url)
            .bearer_auth(&token)
            .send().await
            .map_err(|e| anyhow::anyhow!("SharePoint: cannot check '{path}': {e}"))?;

        match resp.status().as_u16() {
            200..=299 => Ok(true),
            404       => Ok(false),
            status    => {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("SharePoint: exists check for '{path}' returned {status}: {body}");
            }
        }
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let token = self.get_token().await?;
        let url = format!("{}:/children", self.file_url(prefix));

        let resp = self.client.get(&url)
            .bearer_auth(&token)
            .send().await
            .map_err(|e| anyhow::anyhow!("SharePoint: cannot list '{prefix}': {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("SharePoint: list '{prefix}' returned {status}: {body}");
        }

        let items: DriveItemList = resp.json().await
            .map_err(|e| anyhow::anyhow!("SharePoint: cannot parse list response: {e}"))?;

        Ok(items.value.into_iter()
            .filter(|item| item.file.is_some())
            .map(|item| item.name)
            .collect())
    }

    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let token = self.get_token().await?;
        let url = self.file_url(path);

        let resp = self.client.delete(&url)
            .bearer_auth(&token)
            .send().await
            .map_err(|e| anyhow::anyhow!("SharePoint: cannot delete '{path}': {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("SharePoint: delete '{path}' returned {status}: {body}");
        }

        Ok(())
    }

    fn describe(&self) -> String {
        format!("SharePoint ({})", self.drive_url)
    }
}

// ── Factory ───────────────────────────────────────────────────────────────────

/// Create a SharePoint transport from connection parameters.
pub fn create_sharepoint_transport(
    site_url: &str,
    auth:     &FileAuth,
    drive_id: Option<&str>,
) -> anyhow::Result<Box<dyn FileTransport>> {
    let (tenant_id, client_id, client_secret) = match auth {
        FileAuth::ClientCredentials { tenant_id, client_id, client_secret } => {
            (tenant_id.clone(), client_id.clone(), client_secret.clone())
        }
        other => anyhow::bail!(
            "SharePoint: unsupported auth type -- only 'client_credentials' is supported, got '{}'",
            match other {
                FileAuth::UserPass { .. } => "user_pass",
                FileAuth::None => "none",
                _ => "unsupported",
            }
        ),
    };

    let drive_url = if let Some(id) = drive_id {
        format!("https://graph.microsoft.com/v1.0/drives/{id}")
    } else {
        let parsed = url::Url::parse(site_url)
            .map_err(|e| anyhow::anyhow!("SharePoint: invalid site_url '{site_url}': {e}"))?;
        let host = parsed.host_str()
            .ok_or_else(|| anyhow::anyhow!("SharePoint: site_url has no host"))?;
        let path = parsed.path().trim_end_matches('/');
        format!("https://graph.microsoft.com/v1.0/sites/{host}:{path}:/drive")
    };

    tracing::debug!("SharePoint: drive_url = {drive_url}");

    Ok(Box::new(SharePointTransport::new(
        drive_url, tenant_id, client_id, client_secret,
    )))
}
