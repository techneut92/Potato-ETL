//! SFTP file transport — read/write files via SSH File Transfer Protocol.
//!
//! Uses [`russh`](https://docs.rs/russh) + [`russh-sftp`](https://docs.rs/russh-sftp)
//! for a pure-Rust, async SSH implementation.
//!
//! ## Connection model
//!
//! Lazy connection: the SSH handshake and SFTP session setup happen on the
//! first I/O operation, not at construction time.
//!
//! ## Auth types
//!
//! | Auth type   | Description |
//! |-------------|-------------|
//! | `user_pass` | Password-based SSH authentication |
//! | `key`       | SSH private key (PEM string or file path) |

use std::sync::Arc;

use russh::client;
use russh_sftp::client::SftpSession;
use tokio::sync::Mutex;

use potato_etl_common::config::FileAuth;
use potato_etl_common::file_transport::FileTransport;

// ── SftpTransport ─────────────────────────────────────────────────────────────

/// SFTP transport with lazy SSH connection.
pub struct SftpTransport {
    session:  Mutex<Option<SftpSession>>,
    host:     String,
    port:     u16,
    auth:     FileAuth,
    host_key: Option<String>,
}

impl SftpTransport {
    /// Create a new SFTP transport.  No network I/O until first use.
    pub fn new(host: String, port: u16, auth: FileAuth, host_key: Option<String>) -> Self {
        Self {
            session: Mutex::new(None),
            host,
            port,
            auth,
            host_key,
        }
    }

    async fn ensure_connected(&self) -> anyhow::Result<tokio::sync::MutexGuard<'_, Option<SftpSession>>> {
        let mut guard = self.session.lock().await;
        if guard.is_none() {
            let sftp = connect_sftp(&self.host, self.port, &self.auth, self.host_key.as_deref()).await?;
            *guard = Some(sftp);
        }
        Ok(guard)
    }
}

#[async_trait::async_trait]
impl FileTransport for SftpTransport {
    async fn read_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let guard = self.ensure_connected().await?;
        let sftp = guard.as_ref().unwrap();
        sftp.read(path).await
            .map_err(|e| anyhow::anyhow!("SFTP ({}): cannot read '{}': {e}", self.host, path))
    }

    async fn write_bytes(&self, path: &str, data: &[u8]) -> anyhow::Result<()> {
        let guard = self.ensure_connected().await?;
        let sftp = guard.as_ref().unwrap();

        if let Some(parent) = std::path::Path::new(path).parent() {
            let parent_str = parent.to_string_lossy();
            if !parent_str.is_empty() && parent_str != "/" {
                let mut current = String::new();
                for segment in parent_str.split('/') {
                    if segment.is_empty() {
                        current.push('/');
                        continue;
                    }
                    if !current.is_empty() && !current.ends_with('/') {
                        current.push('/');
                    }
                    current.push_str(segment);
                    let _ = sftp.create_dir(&current).await;
                }
            }
        }

        sftp.write(path, data).await
            .map_err(|e| anyhow::anyhow!("SFTP ({}): cannot write '{}': {e}", self.host, path))
    }

    async fn exists(&self, path: &str) -> anyhow::Result<bool> {
        let guard = self.ensure_connected().await?;
        let sftp = guard.as_ref().unwrap();
        match sftp.metadata(path).await {
            Ok(_)  => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let guard = self.ensure_connected().await?;
        let sftp = guard.as_ref().unwrap();
        let entries = sftp.read_dir(prefix).await
            .map_err(|e| anyhow::anyhow!("SFTP ({}): cannot list '{}': {e}", self.host, prefix))?;
        Ok(entries.into_iter()
            .filter(|e| !e.file_type().is_dir())
            .map(|e| e.file_name())
            .collect())
    }

    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let guard = self.ensure_connected().await?;
        let sftp = guard.as_ref().unwrap();
        sftp.remove_file(path).await
            .map_err(|e| anyhow::anyhow!("SFTP ({}): cannot delete '{}': {e}", self.host, path))
    }

    fn describe(&self) -> String {
        format!("SFTP ({}:{})", self.host, self.port)
    }
}

// ── SSH client handler ────────────────────────────────────────────────────────

struct SshHandler;

impl client::Handler for SshHandler {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        // TODO: verify against `host_key` from connection config.
        Ok(true)
    }
}

// ── Internal ──────────────────────────────────────────────────────────────────

async fn connect_sftp(
    host:      &str,
    port:      u16,
    auth:      &FileAuth,
    _host_key: Option<&str>,
) -> anyhow::Result<SftpSession> {
    let config = Arc::new(client::Config::default());
    let handler = SshHandler;

    let mut session = client::connect(config, (host, port), handler).await
        .map_err(|e| anyhow::anyhow!("SFTP: cannot connect to {host}:{port}: {e}"))?;

    match auth {
        FileAuth::UserPass { username, password } => {
            let result = session.authenticate_password(username, password).await
                .map_err(|e| anyhow::anyhow!("SFTP: password auth failed for {username}@{host}: {e}"))?;
            anyhow::ensure!(result.success(), "SFTP: password auth rejected for {username}@{host}");
        }
        FileAuth::Key { username, private_key, passphrase } => {
            let key_pair = if private_key.contains("-----BEGIN") {
                russh::keys::decode_secret_key(private_key, passphrase.as_deref())
                    .map_err(|e| anyhow::anyhow!("SFTP: cannot decode private key: {e}"))?
            } else {
                russh::keys::load_secret_key(private_key, passphrase.as_deref())
                    .map_err(|e| anyhow::anyhow!("SFTP: cannot load key file '{private_key}': {e}"))?
            };
            let key_with_hash = russh::keys::PrivateKeyWithHashAlg::new(
                Arc::new(key_pair),
                None, // use default hash algorithm
            );
            let result = session.authenticate_publickey(username, key_with_hash).await
                .map_err(|e| anyhow::anyhow!("SFTP: key auth failed for {username}@{host}: {e}"))?;
            anyhow::ensure!(result.success(), "SFTP: key auth rejected for {username}@{host}");
        }
        other => anyhow::bail!(
            "SFTP: unsupported auth type -- requires 'user_pass' or 'key', got '{}'",
            match other { FileAuth::None => "none", _ => "unsupported" }
        ),
    }

    let channel = session.channel_open_session().await
        .map_err(|e| anyhow::anyhow!("SFTP: cannot open session channel: {e}"))?;
    channel.request_subsystem(true, "sftp").await
        .map_err(|e| anyhow::anyhow!("SFTP: cannot request sftp subsystem: {e}"))?;

    let sftp = SftpSession::new(channel.into_stream()).await
        .map_err(|e| anyhow::anyhow!("SFTP: cannot initialize SFTP session: {e}"))?;

    tracing::info!("SFTP: connected to {host}:{port}");
    Ok(sftp)
}