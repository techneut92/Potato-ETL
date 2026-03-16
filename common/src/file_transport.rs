//! File transport abstraction — read/write bytes from/to any supported
//! storage backend.
//!
//! ## Architecture
//!
//! This module provides:
//! - The [`FileTransport`] trait
//! - [`LocalTransport`] — always-available local filesystem implementation
//! - [`TransportRegistry`] — runtime-configurable dispatch table for remote transports
//! - [`GlobSortOrder`] — sort order enum for glob-matched file lists
//! - [`resolve_glob`] — glob pattern resolution (flat + recursive `**`)
//! - [`is_glob_path`] — check whether a path contains glob meta-characters
//!
//! Remote transports (S3, SFTP, FTP, SharePoint, SMB) live in their own
//! crates under `/transports/`.  They register themselves into the
//! `TransportRegistry` at startup, and `create_transport()` dispatches to them.
//!
//! ```text
//! FileTransport trait (this crate)
//! ├── LocalTransport              — always available
//! └── (registered at runtime by transport crates)
//!     ├── ObjectStoreTransport    — transports/cloud
//!     ├── SftpTransport           — transports/sftp
//!     ├── FtpTransport            — transports/ftp
//!     ├── SharePointTransport     — transports/sharepoint
//!     └── SmbTransport            — transports/smb
//! ```

use std::sync::OnceLock;

use crate::config::ConnParams;

// ── GlobSortOrder ─────────────────────────────────────────────────────────────

/// Sort order for files resolved by a glob pattern.
///
/// Used by file sources (`read_json`, `read_csv`, `read_parquet`) to control
/// the order in which matched files are processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GlobSortOrder {
    /// Ascending alphabetical by full path (default).
    #[default]
    Name,
    /// Descending alphabetical by full path.
    NameDesc,
}

impl GlobSortOrder {
    /// Parse from a user-provided string (case-insensitive).
    ///
    /// Accepted values: `name`, `name_asc`, `name_desc`.
    /// Returns `None` for unrecognised values.
    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "name" | "name_asc" => Some(Self::Name),
            "name_desc"         => Some(Self::NameDesc),
            _                   => None,
        }
    }
}

// ── Trait ─────────────────────────────────────────────────────────────────────

/// Unified file transport abstraction.
///
/// Implementations are created from a [`ConnParams`] file connection variant.
/// All operations are async to support network-based transports.
#[async_trait::async_trait]
pub trait FileTransport: Send + Sync {
    /// Read the entire contents of a file at `path`.
    async fn read_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>>;

    /// Write `data` to a file at `path`, creating or overwriting.
    async fn write_bytes(&self, path: &str, data: &[u8]) -> anyhow::Result<()>;

    /// Check whether a file exists at `path`.
    async fn exists(&self, path: &str) -> anyhow::Result<bool>;

    /// List files matching a prefix (non-recursive by default).
    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>>;

    /// List subdirectory names under `prefix`.
    ///
    /// Default implementation returns an empty list (sufficient for transports
    /// that don't support directory enumeration or for flat storage like S3
    /// where [`list_recursive`](Self::list_recursive) can be overridden
    /// directly).
    async fn list_dirs(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let _ = prefix;
        Ok(vec![])
    }

    /// Recursively list all files under `prefix`, returning paths **relative
    /// to `prefix`**.
    ///
    /// Default implementation walks subdirectories using [`list`] and
    /// [`list_dirs`].  Transports with native recursive listing (e.g. S3
    /// prefix scan) should override this for efficiency.
    async fn list_recursive(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let mut result = Vec::new();
        let mut stack: Vec<String> = vec![String::new()];
        while let Some(rel_dir) = stack.pop() {
            let abs_dir = if rel_dir.is_empty() {
                prefix.to_string()
            } else {
                format!("{prefix}/{rel_dir}")
            };
            // Files in this directory.
            for name in self.list(&abs_dir).await? {
                if rel_dir.is_empty() {
                    result.push(name);
                } else {
                    result.push(format!("{rel_dir}/{name}"));
                }
            }
            // Subdirectories to recurse into.
            for sub in self.list_dirs(&abs_dir).await? {
                if rel_dir.is_empty() {
                    stack.push(sub);
                } else {
                    stack.push(format!("{rel_dir}/{sub}"));
                }
            }
        }
        Ok(result)
    }

    /// Delete a file at `path`.
    async fn delete(&self, path: &str) -> anyhow::Result<()>;

    /// Human-readable description for logging.
    fn describe(&self) -> String;
}

// ── LocalTransport ────────────────────────────────────────────────────────────

/// Local filesystem transport.  Always available — no feature flags needed.
pub struct LocalTransport;

#[async_trait::async_trait]
impl FileTransport for LocalTransport {
    async fn read_bytes(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("local: cannot read '{path}': {e}"))
    }

    async fn write_bytes(&self, path: &str, data: &[u8]) -> anyhow::Result<()> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| anyhow::anyhow!("local: cannot create directory for '{path}': {e}"))?;
        }
        std::fs::write(path, data)
            .map_err(|e| anyhow::anyhow!("local: cannot write '{path}': {e}"))
    }

    async fn exists(&self, path: &str) -> anyhow::Result<bool> {
        Ok(std::path::Path::new(path).exists())
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let dir = std::path::Path::new(prefix);
        if !dir.is_dir() {
            return Ok(vec![]);
        }
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(dir)
            .map_err(|e| anyhow::anyhow!("local: cannot list '{prefix}': {e}"))?
        {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    entries.push(name.to_string());
                }
            }
        }
        Ok(entries)
    }

    async fn list_dirs(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let dir = std::path::Path::new(prefix);
        if !dir.is_dir() {
            return Ok(vec![]);
        }
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(dir)
            .map_err(|e| anyhow::anyhow!("local: cannot list dirs '{prefix}': {e}"))?
        {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    entries.push(name.to_string());
                }
            }
        }
        Ok(entries)
    }

    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        std::fs::remove_file(path)
            .map_err(|e| anyhow::anyhow!("local: cannot delete '{path}': {e}"))
    }

    fn describe(&self) -> String {
        "local filesystem".to_string()
    }
}

// ── Transport Registry ───────────────────────────────────────────────────────

/// A function that creates a `Box<dyn FileTransport>` from a `ConnParams`.
///
/// Returns `Ok(Some(transport))` if this factory handles the given driver,
/// or `Ok(None)` if it doesn't (pass to next factory).
pub type TransportFactory =
    fn(&ConnParams) -> anyhow::Result<Option<Box<dyn FileTransport>>>;

/// Global registry of transport factories.
///
/// Transport crates register their factory functions at startup (e.g., from
/// the runtime crate's initialization code).  `create_transport()` iterates
/// through registered factories in order.
///
/// ## Usage from the runtime crate
///
/// ```rust,ignore
/// use potato_etl_common::file_transport::register_transport;
///
/// // At startup:
/// register_transport(potato_etl_transport_cloud::try_create);
/// register_transport(potato_etl_transport_sftp::try_create);
/// register_transport(potato_etl_transport_ftp::try_create);
/// register_transport(potato_etl_transport_sharepoint::try_create);
/// register_transport(potato_etl_transport_smb::try_create);
/// ```
static TRANSPORT_FACTORIES: OnceLock<std::sync::Mutex<Vec<TransportFactory>>> = OnceLock::new();

fn factories() -> &'static std::sync::Mutex<Vec<TransportFactory>> {
    TRANSPORT_FACTORIES.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

/// Register a transport factory.  Call this during runtime initialization.
pub fn register_transport(factory: TransportFactory) {
    factories().lock().unwrap().push(factory);
}

// ── create_transport ──────────────────────────────────────────────────────────

/// Create a [`FileTransport`] from a file connection definition.
///
/// 1. `Local` connections are handled directly.
/// 2. All other file drivers are dispatched to registered transport factories.
/// 3. Non-file connections (database, REST API) return an error.
///
/// # Errors
///
/// - If the connection driver is not a file transport type.
/// - If no registered factory handles the driver (transport crate not linked).
pub fn create_transport(conn: &ConnParams) -> anyhow::Result<Box<dyn FileTransport>> {
    // Local is always built-in.
    if matches!(conn, ConnParams::Local { .. }) {
        return Ok(Box::new(LocalTransport));
    }

    // Check if this is a file transport driver at all.
    let is_file_driver = matches!(
        conn,
        ConnParams::Local { .. }
        | ConnParams::Sftp { .. }
        | ConnParams::S3 { .. }
        | ConnParams::AzureBlob { .. }
        | ConnParams::Gcs { .. }
        | ConnParams::Sharepoint { .. }
        | ConnParams::Ftp { .. }
        | ConnParams::Smb { .. }
    );

    if !is_file_driver {
        anyhow::bail!(
            "Connection driver '{}' is not a file-transport driver. \
             File steps can only use: local, sftp, s3, azure_blob, gcs, \
             sharepoint, ftp, smb.",
            conn.driver_name()
        );
    }

    // Try registered factories.
    let facs = factories().lock().unwrap();
    for factory in facs.iter() {
        if let Some(transport) = factory(conn)? {
            return Ok(transport);
        }
    }

    // No factory handled it — the transport crate isn't linked.
    anyhow::bail!(
        "No transport registered for driver '{}'. \
         Make sure the corresponding transport crate is included as a \
         dependency (e.g., potato-etl-transport-cloud for S3/Azure/GCS, \
         potato-etl-transport-sftp for SFTP, etc.).",
        conn.driver_name()
    );
}

// ── Glob support ──────────────────────────────────────────────────────────────

/// Check whether a path contains glob meta-characters (`*`, `?`, `[`).
pub fn is_glob_path(path: &str) -> bool {
    path.contains('*') || path.contains('?') || path.contains('[')
}

/// Split a glob path into `(directory, filename_pattern)`.
///
/// ```text
/// "data/xyz_*.json"       → ("data",  "xyz_*.json")
/// "/opt/files/2024-??-01" → ("/opt/files", "2024-??-01")
/// "*.csv"                 → (".",     "*.csv")
/// ```
fn split_glob_path(path: &str) -> (&str, &str) {
    // Find first glob character.
    let glob_pos = match path.find(|c: char| c == '*' || c == '?' || c == '[') {
        Some(p) => p,
        None    => return (path, ""),
    };
    // Walk backwards to the nearest '/'.
    match path[..glob_pos].rfind('/') {
        Some(slash) => (&path[..slash], &path[slash + 1..]),
        None        => (".", path),
    }
}

/// Resolve a (potentially globbed) path to a list of concrete file paths.
///
/// ## Behaviour
///
/// | Path form | Action |
/// |-----------|--------|
/// | No glob characters | Returns `vec![path]` unchanged (no I/O) |
/// | Single-level glob (`data/xyz_*.json`) | Lists `data/`, filters by `xyz_*.json` |
/// | Recursive glob (`data/**/xyz_*.json`) | Recursively lists under `data/`, filters by `xyz_*.json` |
///
/// ## Sort order
///
/// Results are sorted according to `sort`.  The default (`GlobSortOrder::Name`)
/// gives ascending alphabetical order — deterministic and easy to reason about.
///
/// ## `**` (double-star / recursive)
///
/// A `**` segment in the path matches zero or more directory levels:
///
/// ```text
/// data/**/report_*.csv
///   → data/report_001.csv
///   → data/2024/report_002.csv
///   → data/2024/q1/report_003.csv
/// ```
///
/// Only one `**` segment is supported.  If more than one is present, the path
/// is treated as having a single recursive root at the **first** `**`.
///
/// # Errors
///
/// Returns an error if listing fails or if no files match the pattern.
pub async fn resolve_glob(
    transport: &dyn FileTransport,
    path: &str,
    sort: GlobSortOrder,
) -> anyhow::Result<Vec<String>> {
    if !is_glob_path(path) {
        return Ok(vec![path.to_string()]);
    }

    let mut matched = if path.contains("**/") || path.ends_with("**") {
        resolve_glob_recursive(transport, path).await?
    } else {
        resolve_glob_flat(transport, path).await?
    };

    // Apply sort order.
    match sort {
        GlobSortOrder::Name     => matched.sort(),
        GlobSortOrder::NameDesc => {
            matched.sort();
            matched.reverse();
        }
    }

    if matched.is_empty() {
        anyhow::bail!(
            "glob: no files matching pattern '{path}' ({})",
            transport.describe(),
        );
    }

    tracing::info!(
        "glob: pattern '{path}' matched {} file(s) ({})",
        matched.len(), transport.describe(),
    );

    Ok(matched)
}

/// Flat (single-directory) glob resolution.
async fn resolve_glob_flat(
    transport: &dyn FileTransport,
    path: &str,
) -> anyhow::Result<Vec<String>> {
    let (dir, pattern) = split_glob_path(path);
    let entries = transport.list(dir).await?;

    Ok(entries
        .into_iter()
        .filter(|name| glob_match(pattern, name))
        .map(|name| {
            if dir == "." { name } else { format!("{dir}/{name}") }
        })
        .collect())
}

/// Recursive (`**`) glob resolution.
///
/// Splits the pattern at the first `**/`, then recursively lists all files
/// under the prefix directory and matches each file's relative path against
/// the suffix pattern.
async fn resolve_glob_recursive(
    transport: &dyn FileTransport,
    path: &str,
) -> anyhow::Result<Vec<String>> {
    // Split at the first `**/`.
    let (prefix, suffix) = if let Some(pos) = path.find("**/") {
        let pfx = if pos == 0 { "." } else { &path[..pos.saturating_sub(1)] };
        let sfx = &path[pos + 3..]; // skip `**/`
        (pfx, sfx)
    } else if path.ends_with("**") {
        let pfx = if path.len() <= 2 { "." } else { &path[..path.len() - 3] };
        (pfx, "*")
    } else {
        // Shouldn't happen — caller checks for `**`.
        return resolve_glob_flat(transport, path).await;
    };

    let all_files = transport.list_recursive(prefix).await?;

    // Match each relative path against the suffix pattern.
    // The suffix may itself contain glob chars (e.g. `report_*.csv`).
    Ok(all_files
        .into_iter()
        .filter(|rel| {
            // Match the filename part against the suffix pattern.
            // For multi-segment suffixes like `subdir/*.csv`, match the
            // full relative path against the suffix.
            glob_match(suffix, rel)
                || rel.rsplit('/').next().is_some_and(|fname| glob_match(suffix, fname))
        })
        .map(|rel| {
            if prefix == "." { rel } else { format!("{prefix}/{rel}") }
        })
        .collect())
}

/// Simple glob pattern matcher — supports `*`, `?`, and `[abc]`/`[a-z]`
/// character classes.  Does **not** match `/` with `*` (single-segment only).
///
/// This is intentionally self-contained to avoid pulling in an external
/// crate for such a small feature.
fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_inner(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_inner(pat: &[u8], txt: &[u8]) -> bool {
    let (mut pi, mut ti) = (0usize, 0usize);
    // "star" bookmarks for backtracking.
    let (mut star_pi, mut star_ti) = (usize::MAX, 0usize);

    while ti < txt.len() {
        if pi < pat.len() && pat[pi] == b'[' {
            // Character class: [abc] or [a-z] or [!abc].
            if let Some((class_end, matched)) = match_char_class(&pat[pi..], txt[ti]) {
                if matched {
                    pi += class_end;
                    ti += 1;
                    continue;
                }
            }
            // Class didn't match — try backtracking.
            if star_pi != usize::MAX {
                pi = star_pi + 1;
                star_ti += 1;
                ti = star_ti;
                continue;
            }
            return false;
        }

        if pi < pat.len() && pat[pi] == b'?' {
            // '?' matches any single character (except '/' for safety).
            if txt[ti] != b'/' {
                pi += 1;
                ti += 1;
                continue;
            }
        } else if pi < pat.len() && pat[pi] == b'*' {
            // '*' matches zero or more non-'/' characters.
            star_pi = pi;
            star_ti = ti;
            pi += 1;
            continue;
        } else if pi < pat.len() && pat[pi] == txt[ti] {
            pi += 1;
            ti += 1;
            continue;
        }

        // Mismatch — try to backtrack to last '*'.
        if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
            continue;
        }

        return false;
    }

    // Consume trailing '*'s in pattern.
    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }

    pi == pat.len()
}

/// Parse a `[...]` character class at the start of `pat` and check whether
/// `ch` is a member.  Returns `Some((bytes_consumed, matched))` or `None`
/// if the bracket sequence is malformed.
fn match_char_class(pat: &[u8], ch: u8) -> Option<(usize, bool)> {
    if pat.is_empty() || pat[0] != b'[' {
        return None;
    }
    let mut i = 1;
    let negate = if i < pat.len() && pat[i] == b'!' {
        i += 1;
        true
    } else {
        false
    };
    let mut matched = false;
    while i < pat.len() && pat[i] != b']' {
        if i + 2 < pat.len() && pat[i + 1] == b'-' && pat[i + 2] != b']' {
            // Range: [a-z]
            if ch >= pat[i] && ch <= pat[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if ch == pat[i] {
                matched = true;
            }
            i += 1;
        }
    }
    if i >= pat.len() {
        return None; // No closing ']'
    }
    // i is now at ']'.
    Some((i + 1, matched ^ negate))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_local_roundtrip() {
        let dir = std::env::temp_dir().join("potato_etl_test_transport");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.txt");
        let path_str = path.to_str().unwrap();

        let transport = LocalTransport;

        transport.write_bytes(path_str, b"hello world").await.unwrap();
        assert!(transport.exists(path_str).await.unwrap());

        let data = transport.read_bytes(path_str).await.unwrap();
        assert_eq!(data, b"hello world");

        transport.delete(path_str).await.unwrap();
        assert!(!transport.exists(path_str).await.unwrap());
    }

    #[tokio::test]
    async fn test_local_creates_dirs() {
        let dir = std::env::temp_dir().join("potato_etl_test_transport/nested/deep");
        let path = dir.join("file.txt");
        let path_str = path.to_str().unwrap();

        let transport = LocalTransport;
        transport.write_bytes(path_str, b"data").await.unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_create_transport_local() {
        let conn = ConnParams::Local { base_path: "/tmp".into() };
        let transport = create_transport(&conn).unwrap();
        assert!(transport.describe().contains("local"));
    }

    #[test]
    fn test_create_transport_rejects_database() {
        use crate::config::DbAuth;
        let conn = ConnParams::Postgres {
            host: "localhost".into(),
            port: None,
            database: "test".into(),
            auth: DbAuth::None,
            options: Default::default(),
        };
        assert!(create_transport(&conn).is_err());
    }

    #[test]
    fn test_unregistered_driver_gives_clear_error() {
        let conn = ConnParams::Sftp {
            host: "localhost".into(),
            port: 22,
            auth: crate::config::FileAuth::None,
            host_key: None,
            base_path: String::new(),
        };
        let err = create_transport(&conn).err().expect("expected error for unregistered driver");
        let msg = err.to_string();
        assert!(msg.contains("No transport registered"), "got: {msg}");
    }

    // ── Glob matcher tests ────────────────────────────────────────────────

    #[test]
    fn test_glob_match_star() {
        assert!(glob_match("xyz_*.json", "xyz_123.json"));
        assert!(glob_match("xyz_*.json", "xyz_.json"));
        assert!(glob_match("xyz_*.json", "xyz_abc_def.json"));
        assert!(!glob_match("xyz_*.json", "abc_123.json"));
        assert!(!glob_match("xyz_*.json", "xyz_123.csv"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(glob_match("data_?.csv", "data_1.csv"));
        assert!(glob_match("data_?.csv", "data_a.csv"));
        assert!(!glob_match("data_?.csv", "data_12.csv"));
        assert!(!glob_match("data_?.csv", "data_.csv"));
    }

    #[test]
    fn test_glob_match_char_class() {
        assert!(glob_match("[abc].txt", "a.txt"));
        assert!(glob_match("[abc].txt", "b.txt"));
        assert!(!glob_match("[abc].txt", "d.txt"));
    }

    #[test]
    fn test_glob_match_char_range() {
        assert!(glob_match("[0-9]_file.csv", "3_file.csv"));
        assert!(!glob_match("[0-9]_file.csv", "a_file.csv"));
    }

    #[test]
    fn test_glob_match_negated_class() {
        assert!(glob_match("[!0-9].txt", "a.txt"));
        assert!(!glob_match("[!0-9].txt", "5.txt"));
    }

    #[test]
    fn test_glob_match_star_at_edges() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*.json", "file.json"));
        assert!(glob_match("prefix*", "prefix_and_more"));
        assert!(glob_match("*suffix", "my_suffix"));
    }

    #[test]
    fn test_glob_match_multiple_stars() {
        assert!(glob_match("*_*_*.csv", "a_b_c.csv"));
        assert!(glob_match("*_*_*.csv", "abc_def_ghi.csv"));
        assert!(!glob_match("*_*_*.csv", "a_b.csv"));
    }

    #[test]
    fn test_glob_match_exact() {
        assert!(glob_match("exact.txt", "exact.txt"));
        assert!(!glob_match("exact.txt", "other.txt"));
    }

    #[test]
    fn test_is_glob_path() {
        assert!(is_glob_path("data/xyz_*.json"));
        assert!(is_glob_path("file?.csv"));
        assert!(is_glob_path("[abc].txt"));
        assert!(!is_glob_path("data/file.json"));
        assert!(!is_glob_path("/absolute/path.csv"));
    }

    #[test]
    fn test_split_glob_path() {
        assert_eq!(split_glob_path("data/xyz_*.json"), ("data", "xyz_*.json"));
        assert_eq!(split_glob_path("/opt/files/2024-??-01.csv"), ("/opt/files", "2024-??-01.csv"));
        assert_eq!(split_glob_path("*.csv"), (".", "*.csv"));
        assert_eq!(split_glob_path("a/b/c/*.parquet"), ("a/b/c", "*.parquet"));
    }

    #[tokio::test]
    async fn test_resolve_glob_literal() {
        let transport = LocalTransport;
        let paths = resolve_glob(&transport, "data/file.json", GlobSortOrder::Name).await.unwrap();
        assert_eq!(paths, vec!["data/file.json"]);
    }

    #[tokio::test]
    async fn test_resolve_glob_with_pattern() {
        // Create temp files to glob over.
        let dir = std::env::temp_dir().join("potato_etl_test_glob");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("xyz_001.json"), b"[]").unwrap();
        std::fs::write(dir.join("xyz_002.json"), b"[]").unwrap();
        std::fs::write(dir.join("xyz_003.csv"), b"a").unwrap();
        std::fs::write(dir.join("abc_001.json"), b"[]").unwrap();

        let pattern = format!("{}/xyz_*.json", dir.to_str().unwrap());
        let transport = LocalTransport;
        let paths = resolve_glob(&transport, &pattern, GlobSortOrder::Name).await.unwrap();
        assert_eq!(paths.len(), 2);
        assert!(paths[0].ends_with("xyz_001.json"));
        assert!(paths[1].ends_with("xyz_002.json"));

        // Cleanup.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_resolve_glob_no_match() {
        let dir = std::env::temp_dir().join("potato_etl_test_glob_empty");
        std::fs::create_dir_all(&dir).unwrap();

        let pattern = format!("{}/nonexistent_*.json", dir.to_str().unwrap());
        let transport = LocalTransport;
        let result = resolve_glob(&transport, &pattern, GlobSortOrder::Name).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no files matching"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_resolve_glob_name_desc() {
        let dir = std::env::temp_dir().join("potato_etl_test_glob_desc");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.json"), b"[]").unwrap();
        std::fs::write(dir.join("b.json"), b"[]").unwrap();
        std::fs::write(dir.join("c.json"), b"[]").unwrap();

        let pattern = format!("{}/*.json", dir.to_str().unwrap());
        let transport = LocalTransport;
        let paths = resolve_glob(&transport, &pattern, GlobSortOrder::NameDesc).await.unwrap();
        assert_eq!(paths.len(), 3);
        assert!(paths[0].ends_with("c.json"));
        assert!(paths[1].ends_with("b.json"));
        assert!(paths[2].ends_with("a.json"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_resolve_glob_recursive() {
        let dir = std::env::temp_dir().join("potato_etl_test_glob_recursive");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("sub1")).unwrap();
        std::fs::create_dir_all(dir.join("sub2/deep")).unwrap();
        std::fs::write(dir.join("report_top.csv"), b"a").unwrap();
        std::fs::write(dir.join("sub1/report_s1.csv"), b"b").unwrap();
        std::fs::write(dir.join("sub2/report_s2.csv"), b"c").unwrap();
        std::fs::write(dir.join("sub2/deep/report_deep.csv"), b"d").unwrap();
        std::fs::write(dir.join("sub2/other.txt"), b"e").unwrap();

        let pattern = format!("{}/**/report_*.csv", dir.to_str().unwrap());
        let transport = LocalTransport;
        let paths = resolve_glob(&transport, &pattern, GlobSortOrder::Name).await.unwrap();
        // Should match all report_*.csv files at any depth.
        assert!(paths.len() >= 3, "expected >=3 matches, got {}: {:?}", paths.len(), paths);
        assert!(paths.iter().all(|p| p.ends_with(".csv") && p.contains("report_")));
        // other.txt should NOT be in the results.
        assert!(!paths.iter().any(|p| p.contains("other.txt")));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_list_dirs_local() {
        let dir = std::env::temp_dir().join("potato_etl_test_list_dirs");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("subA")).unwrap();
        std::fs::create_dir_all(dir.join("subB")).unwrap();
        std::fs::write(dir.join("file.txt"), b"x").unwrap();

        let transport = LocalTransport;
        let mut dirs = transport.list_dirs(dir.to_str().unwrap()).await.unwrap();
        dirs.sort();
        assert_eq!(dirs, vec!["subA", "subB"]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_list_recursive_local() {
        let dir = std::env::temp_dir().join("potato_etl_test_list_recursive");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("a/b")).unwrap();
        std::fs::write(dir.join("top.txt"), b"1").unwrap();
        std::fs::write(dir.join("a/mid.txt"), b"2").unwrap();
        std::fs::write(dir.join("a/b/deep.txt"), b"3").unwrap();

        let transport = LocalTransport;
        let mut files = transport.list_recursive(dir.to_str().unwrap()).await.unwrap();
        files.sort();
        assert_eq!(files, vec!["a/b/deep.txt", "a/mid.txt", "top.txt"]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_glob_sort_order_from_str() {
        assert_eq!(GlobSortOrder::from_str_opt("name"), Some(GlobSortOrder::Name));
        assert_eq!(GlobSortOrder::from_str_opt("name_asc"), Some(GlobSortOrder::Name));
        assert_eq!(GlobSortOrder::from_str_opt("NAME_DESC"), Some(GlobSortOrder::NameDesc));
        assert_eq!(GlobSortOrder::from_str_opt("unknown"), None);
    }
}