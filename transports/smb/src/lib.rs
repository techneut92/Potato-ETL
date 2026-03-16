//! SMB / CIFS file transport — read/write files on Windows file shares.
//!
//! Uses the [`pavao`](https://docs.rs/pavao) crate (v0.2) for SMB client
//! operations via libsmbclient bindings.
//!
//! ## Connection model
//!
//! Lazy connection: the SMB session is established on first I/O operation.
//!
//! ## Auth types
//!
//! | Auth type   | Description |
//! |-------------|-------------|
//! | `user_pass` | NTLM authentication (`DOMAIN\user` + password) |
//! | `none`      | Guest / anonymous access |

use std::io::{Read, Write};

use pavao::{SmbClient, SmbCredentials, SmbDirentType, SmbMode, SmbOpenOptions, SmbOptions};
use tokio::sync::Mutex;

use potato_etl_common::config::FileAuth;
use potato_etl_common::file_transport::FileTransport;

// ── SmbTransport ──────────────────────────────────────────────────────────────

/// SMB/CIFS transport with lazy connection.
pub struct SmbTransport {
    client: Mutex<Option<SmbClient>>,
    host:   String,
    share:  String,
    port:   u16,
    auth:   FileAuth,
}

impl SmbTransport {
    /// Create a new SMB transport.  No network I/O until first use.
    pub fn new(host: String, share: String, port: u16, auth: FileAuth) -> Self {
        Self {
            client: Mutex::new(None),
            host,
            share,
            port,
            auth,
        }
    }

    fn smb_url(&self, path: &str) -> String {
        let clean = path.trim_start_matches('/');
        format!("smb://{}:{}/{}/{}", self.host, self.port, self.share, clean)
    }

    async fn ensure_connected(&self) -> anyhow::Result<tokio::sync::MutexGuard<'_, Option<SmbClient>>> {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            let client = connect_smb(&self.host, self.port, &self.share, &self.auth)?;
            *guard = Some(client);
        }
        Ok(guard)
    }
}

#[async_trait::async_trait]
impl FileTransport for SmbTransport {
    async fn read_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let guard = self.ensure_connected().await?;
        let client = guard.as_ref().unwrap();
        let smb_path = self.smb_url(path);

        let mut file = client.open_with(
            &smb_path,
            SmbOpenOptions::default().read(true),
        )
        .map_err(|e| anyhow::anyhow!("SMB ({}): cannot open '{}': {e}", self.host, path))?;

        let mut data = Vec::new();
        file.read_to_end(&mut data)
            .map_err(|e| anyhow::anyhow!("SMB ({}): cannot read '{}': {e}", self.host, path))?;
        Ok(data)
    }

    async fn write_bytes(&self, path: &str, data: &[u8]) -> anyhow::Result<()> {
        let guard = self.ensure_connected().await?;
        let client = guard.as_ref().unwrap();
        let smb_path = self.smb_url(path);

        // Ensure parent directory exists.
        if let Some(parent) = std::path::Path::new(path).parent() {
            let parent_str = parent.to_string_lossy();
            if !parent_str.is_empty() && parent_str != "/" && parent_str != "." {
                let mut current = String::new();
                for segment in parent_str.split('/') {
                    if segment.is_empty() { continue; }
                    if !current.is_empty() { current.push('/'); }
                    current.push_str(segment);
                    let dir_url = self.smb_url(&current);
                    let _ = client.mkdir(&dir_url, SmbMode::from(0o755));
                }
            }
        }

        let mut file = client.open_with(
            &smb_path,
            SmbOpenOptions::default()
                .write(true)
                .create(true)
                .truncate(true),
        )
        .map_err(|e| anyhow::anyhow!("SMB ({}): cannot create '{}': {e}", self.host, path))?;

        file.write_all(data)
            .map_err(|e| anyhow::anyhow!("SMB ({}): cannot write '{}': {e}", self.host, path))?;
        Ok(())
    }

    async fn exists(&self, path: &str) -> anyhow::Result<bool> {
        let guard = self.ensure_connected().await?;
        let client = guard.as_ref().unwrap();
        let smb_path = self.smb_url(path);

        match client.stat(&smb_path) {
            Ok(_)  => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let guard = self.ensure_connected().await?;
        let client = guard.as_ref().unwrap();
        let smb_path = self.smb_url(prefix);

        let entries = client.list_dir(&smb_path)
            .map_err(|e| anyhow::anyhow!("SMB ({}): cannot list '{}': {e}", self.host, prefix))?;

        Ok(entries.into_iter()
            .filter(|e| e.get_type() == SmbDirentType::File)
            .map(|e| e.name().to_string())
            .collect())
    }

    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let guard = self.ensure_connected().await?;
        let client = guard.as_ref().unwrap();
        let smb_path = self.smb_url(path);

        client.unlink(&smb_path)
            .map_err(|e| anyhow::anyhow!("SMB ({}): cannot delete '{}': {e}", self.host, path))?;
        Ok(())
    }

    fn describe(&self) -> String {
        format!("SMB (\\\\{}\\{})", self.host, self.share)
    }
}

// ── Internal: connect ─────────────────────────────────────────────────────────

fn connect_smb(
    host:  &str,
    port:  u16,
    share: &str,
    auth:  &FileAuth,
) -> anyhow::Result<SmbClient> {
    let (username, password, workgroup) = match auth {
        FileAuth::UserPass { username, password } => {
            let (wg, user) = if let Some(pos) = username.find('\\') {
                (&username[..pos], &username[pos + 1..])
            } else {
                ("WORKGROUP", username.as_str())
            };
            (user.to_string(), password.clone(), wg.to_string())
        }
        FileAuth::None => {
            ("guest".to_string(), String::new(), "WORKGROUP".to_string())
        }
        other => anyhow::bail!(
            "SMB: unsupported auth type -- supports 'user_pass' or 'none', got '{}'",
            match other {
                FileAuth::Key { .. } => "key",
                _ => "unsupported",
            }
        ),
    };

    let smb_url = format!("smb://{}:{}/{}", host, port, share);

    let credentials = SmbCredentials::default()
        .server(&smb_url)
        .share(share)
        .username(&username)
        .password(&password)
        .workgroup(&workgroup);

    let options = SmbOptions::default();

    let client = SmbClient::new(credentials, options)
        .map_err(|e| anyhow::anyhow!("SMB: cannot connect to {host}:{port}/{share}: {e}"))?;

    tracing::info!("SMB: connected to \\\\{host}\\{share} as {workgroup}\\{username}");
    Ok(client)
}
