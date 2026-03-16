//! Transport registration — wires up file transport crates into the
//! [`TransportRegistry`](potato_etl_common::file_transport) based on
//! which feature flags are enabled.
//!
//! Call [`register_all()`] once at startup (before running any pipeline).

use potato_etl_common::config::ConnParams;
use potato_etl_common::file_transport::{FileTransport, register_transport};

/// Register all transport factories for enabled feature flags.
///
/// This should be called once during application startup, before
/// `create_transport()` is used.  It's safe to call multiple times
/// (duplicate registrations are harmless — first match wins).
pub fn register_all() {
    #[cfg(feature = "transport-cloud")]
    register_transport(cloud_factory);

    #[cfg(feature = "transport-sftp")]
    register_transport(sftp_factory);

    #[cfg(feature = "transport-ftp")]
    register_transport(ftp_factory);

    #[cfg(feature = "transport-sharepoint")]
    register_transport(sharepoint_factory);

    #[cfg(feature = "transport-smb")]
    register_transport(smb_factory);
}

// ── Cloud (S3, Azure Blob, GCS) ──────────────────────────────────────────────

#[cfg(feature = "transport-cloud")]
fn cloud_factory(conn: &ConnParams) -> anyhow::Result<Option<Box<dyn FileTransport>>> {
    match conn {
        ConnParams::S3 { .. } | ConnParams::AzureBlob { .. } | ConnParams::Gcs { .. } => {
            let transport = potato_etl_transport_cloud::from_conn_params(conn)?;
            Ok(Some(transport))
        }
        _ => Ok(None),
    }
}

// ── SFTP ─────────────────────────────────────────────────────────────────────

#[cfg(feature = "transport-sftp")]
fn sftp_factory(conn: &ConnParams) -> anyhow::Result<Option<Box<dyn FileTransport>>> {
    match conn {
        ConnParams::Sftp { host, port, auth, host_key, .. } => {
            let transport = potato_etl_transport_sftp::SftpTransport::new(
                host.clone(), *port, auth.clone(), host_key.clone(),
            );
            Ok(Some(Box::new(transport)))
        }
        _ => Ok(None),
    }
}

// ── FTP / FTPS ───────────────────────────────────────────────────────────────

#[cfg(feature = "transport-ftp")]
fn ftp_factory(conn: &ConnParams) -> anyhow::Result<Option<Box<dyn FileTransport>>> {
    match conn {
        ConnParams::Ftp { host, port, auth, tls, passive, .. } => {
            let transport = potato_etl_transport_ftp::FtpTransport::new(
                host.clone(), *port, auth.clone(), *tls, *passive,
            );
            Ok(Some(Box::new(transport)))
        }
        _ => Ok(None),
    }
}

// ── SharePoint ───────────────────────────────────────────────────────────────

#[cfg(feature = "transport-sharepoint")]
fn sharepoint_factory(conn: &ConnParams) -> anyhow::Result<Option<Box<dyn FileTransport>>> {
    match conn {
        ConnParams::Sharepoint { site_url, auth, drive_id, .. } => {
            let transport = potato_etl_transport_sharepoint::create_sharepoint_transport(
                site_url, auth, drive_id.as_deref(),
            )?;
            Ok(Some(transport))
        }
        _ => Ok(None),
    }
}

// ── SMB / CIFS ───────────────────────────────────────────────────────────────

#[cfg(feature = "transport-smb")]
fn smb_factory(conn: &ConnParams) -> anyhow::Result<Option<Box<dyn FileTransport>>> {
    match conn {
        ConnParams::Smb { host, share, port, auth, .. } => {
            let transport = potato_etl_transport_smb::SmbTransport::new(
                host.clone(), share.clone(), *port, auth.clone(),
            );
            Ok(Some(Box::new(transport)))
        }
        _ => Ok(None),
    }
}
