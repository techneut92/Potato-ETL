//! FTP / FTPS file transport — read/write files via FTP with optional TLS.
//!
//! Uses the [`suppaftp`](https://docs.rs/suppaftp) crate (v8).
//!
//! ## Connection model
//!
//! Lazy connection: the TCP handshake and authentication happen on the first
//! I/O operation.
//!
//! ## Auth types
//!
//! | Auth type   | Description |
//! |-------------|-------------|
//! | `user_pass` | Standard FTP `USER`/`PASS` authentication |
//! | `none`      | Anonymous FTP (`anonymous` / empty password) |

use std::io::Cursor;

use suppaftp::NativeTlsConnector;
use suppaftp::NativeTlsFtpStream;
use suppaftp::native_tls::TlsConnector;
use tokio::sync::Mutex;

use potato_etl_common::config::FileAuth;
use potato_etl_common::file_transport::FileTransport;

// ── FtpTransport ──────────────────────────────────────────────────────────────

/// FTP/FTPS transport with lazy connection.
pub struct FtpTransport {
    session: Mutex<Option<NativeTlsFtpStream>>,
    host:    String,
    port:    u16,
    auth:    FileAuth,
    tls:     bool,
    passive: bool,
}

impl FtpTransport {
    /// Create a new FTP transport.  No network I/O until first use.
    pub fn new(host: String, port: u16, auth: FileAuth, tls: bool, passive: bool) -> Self {
        Self {
            session: Mutex::new(None),
            host,
            port,
            auth,
            tls,
            passive,
        }
    }

    async fn ensure_connected(&self) -> anyhow::Result<tokio::sync::MutexGuard<'_, Option<NativeTlsFtpStream>>> {
        let mut guard = self.session.lock().await;
        if guard.is_none() {
            let stream = connect_ftp(
                &self.host, self.port, &self.auth, self.tls, self.passive,
            )?;
            *guard = Some(stream);
        }
        Ok(guard)
    }
}

#[async_trait::async_trait]
impl FileTransport for FtpTransport {
    async fn read_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let mut guard = self.ensure_connected().await?;
        let ftp = guard.as_mut().unwrap();
        let cursor = ftp.retr_as_buffer(path)
            .map_err(|e| anyhow::anyhow!("FTP ({}): cannot read '{}': {e}", self.host, path))?;
        Ok(cursor.into_inner())
    }

    async fn write_bytes(&self, path: &str, data: &[u8]) -> anyhow::Result<()> {
        let mut guard = self.ensure_connected().await?;
        let ftp = guard.as_mut().unwrap();

        // Ensure parent directory exists.
        if let Some(parent) = std::path::Path::new(path).parent() {
            let parent_str = parent.to_string_lossy();
            if !parent_str.is_empty() && parent_str != "/" && parent_str != "." {
                let mut current = String::new();
                for segment in parent_str.split('/') {
                    if segment.is_empty() { continue; }
                    if !current.is_empty() { current.push('/'); }
                    current.push_str(segment);
                    let _ = ftp.mkdir(&current);
                }
            }
        }

        let mut reader = Cursor::new(data);
        ftp.put_file(path, &mut reader)
            .map_err(|e| anyhow::anyhow!("FTP ({}): cannot write '{}': {e}", self.host, path))?;
        Ok(())
    }

    async fn exists(&self, path: &str) -> anyhow::Result<bool> {
        let mut guard = self.ensure_connected().await?;
        let ftp = guard.as_mut().unwrap();
        match ftp.size(path) {
            Ok(_)  => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let mut guard = self.ensure_connected().await?;
        let ftp = guard.as_mut().unwrap();
        let names = ftp.nlst(Some(prefix))
            .map_err(|e| anyhow::anyhow!("FTP ({}): cannot list '{}': {e}", self.host, prefix))?;
        Ok(names)
    }

    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let mut guard = self.ensure_connected().await?;
        let ftp = guard.as_mut().unwrap();
        ftp.rm(path)
            .map_err(|e| anyhow::anyhow!("FTP ({}): cannot delete '{}': {e}", self.host, path))?;
        Ok(())
    }

    fn describe(&self) -> String {
        let proto = if self.tls { "FTPS" } else { "FTP" };
        format!("{proto} ({}:{})", self.host, self.port)
    }
}

// ── Internal: connect ─────────────────────────────────────────────────────────

fn connect_ftp(
    host:    &str,
    port:    u16,
    auth:    &FileAuth,
    tls:     bool,
    passive: bool,
) -> anyhow::Result<NativeTlsFtpStream> {
    let addr = format!("{host}:{port}");

    let mut ftp = NativeTlsFtpStream::connect(&addr)
        .map_err(|e| anyhow::anyhow!("FTP: cannot connect to {addr}: {e}"))?;

    // Upgrade to TLS if requested (explicit FTPS — AUTH TLS).
    if tls {
        let tls_connector = TlsConnector::new()
            .map_err(|e| anyhow::anyhow!("FTP: cannot create TLS connector: {e}"))?;
        let connector = NativeTlsConnector::from(tls_connector);
        ftp = ftp.into_secure(connector, host)
            .map_err(|e| anyhow::anyhow!("FTP: TLS upgrade failed for {addr}: {e}"))?;
    }

    // Authenticate.
    match auth {
        FileAuth::UserPass { username, password } => {
            ftp.login(username, password)
                .map_err(|e| anyhow::anyhow!("FTP: login failed for {username}@{host}: {e}"))?;
        }
        FileAuth::None => {
            ftp.login("anonymous", "")
                .map_err(|e| anyhow::anyhow!("FTP: anonymous login failed for {host}: {e}"))?;
        }
        other => anyhow::bail!(
            "FTP: unsupported auth type -- supports 'user_pass' or 'none', got '{}'",
            match other {
                FileAuth::UserPass { .. } => "user_pass",
                FileAuth::Key { .. } => "key",
                _ => "unsupported",
            }
        ),
    }

    // Set binary transfer mode.
    ftp.transfer_type(suppaftp::types::FileType::Binary)
        .map_err(|e| anyhow::anyhow!("FTP: cannot set binary mode: {e}"))?;

    // Set passive mode if requested.
    if passive {
        // suppaftp uses passive mode by default for data connections,
        // no explicit call needed.
    }

    tracing::info!("FTP: connected to {addr} (tls={tls}, passive={passive})");
    Ok(ftp)
}
