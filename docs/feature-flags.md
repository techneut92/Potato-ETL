# Transport Crates & Feature Flags

## Architecture

File transports follow the same pattern as database drivers: each lives in its own crate with its own dependencies. The `common` crate provides only the `FileTransport` trait and `LocalTransport`. Remote transports are separate workspace members under `transports/`.

```
potato_etl/
├── common/                  ← FileTransport trait + LocalTransport + Parquet I/O
├── transports/
│   ├── cloud/               ← S3, Azure Blob, GCS (object_store 0.13)
│   ├── sftp/                ← SFTP (russh 0.57)
│   ├── ftp/                 ← FTP/FTPS (suppaftp 8)
│   ├── sharepoint/          ← SharePoint Online (reqwest + MS Graph)
│   └── smb/                 ← SMB/CIFS (pavao 0.2)
├── drivers/                 ← database drivers (same pattern)
│   ├── postgres/
│   ├── mssql/
│   ├── mysql/
│   ├── oracle/
│   └── databricks/
├── runtime/                 ← DAG executor + transport/driver registration
└── cli/                     ← binary, passes features through to runtime
```

## Transport crates

| Crate | Drivers | Key dependency | Version |
|---|---|---|---|
| `potato-etl-transport-cloud` | S3, Azure Blob, GCS | `object_store` | 0.13 |
| `potato-etl-transport-sftp` | SFTP | `russh` + `russh-sftp` | 0.57 / 2 |
| `potato-etl-transport-ftp` | FTP, FTPS | `suppaftp` | 8 |
| `potato-etl-transport-sharepoint` | SharePoint Online | `reqwest` + `url` | (workspace) |
| `potato-etl-transport-smb` | SMB / CIFS | `pavao` | 0.2 |

## How it works

1. Each transport crate implements the `FileTransport` trait from `common`
2. The `runtime` crate includes transport crates as optional dependencies, gated by feature flags
3. At startup, `potato_etl_runtime::init()` registers transport factories for all enabled features
4. `create_transport()` dispatches `ConnParams` to the registered factories
5. If no factory handles a driver, a clear error says which feature/crate to include

The registration happens automatically — you just enable the feature flag and call `init()`.

## CLI feature flags

Enable features when building the CLI binary:

```sh
# Everything: all database drivers + all file transports
cargo build -p potato-etl-cli --features all --release

# Specific database drivers + specific transports
cargo build -p potato-etl-cli --features "postgres,mssql,transport-cloud,transport-sftp" --release

# All transports, no database drivers (file-only pipelines)
cargo build -p potato-etl-cli --features transport-all --release

# Minimal: only local filesystem, no remote transports, no database drivers
cargo build -p potato-etl-cli --release
```

### Database driver features

| Feature | Driver | Notes |
|---|---|---|
| `postgres` | PostgreSQL | Pure Rust (sqlx) |
| `mssql` | SQL Server | Pure Rust (tiberius) |
| `mssql-bcp` | SQL Server + bcp | Requires mssql-tools18 |
| `mssql-odbc` | SQL Server + ODBC | Requires unixODBC + MS ODBC Driver 18 |
| `mysql` | MySQL / MariaDB / Aurora | Pure Rust (sqlx) |
| `oracle` | Oracle | Requires Oracle Instant Client |
| `databricks` | Databricks SQL | REST SQL Statement API |
| `databricks-odbc` | Databricks + ODBC | Requires Simba driver |
| `databricks-thrift` | Databricks + Thrift | High-perf large datasets |

### File transport features

| Feature | Transport | Connection drivers | System deps |
|---|---|---|---|
| `transport-cloud` | S3, Azure Blob, GCS | `s3`, `azure_blob`, `gcs` | None |
| `transport-sftp` | SFTP | `sftp` | None |
| `transport-ftp` | FTP / FTPS | `ftp` | None |
| `transport-sharepoint` | SharePoint Online | `sharepoint` | None |
| `transport-smb` | SMB / CIFS | `smb` | `libsmbclient-dev` (see below) |
| `transport-all` | All of the above | All remote file drivers | All of the above |

### Aggregate features

| Feature | Includes |
|---|---|
| `all` | Every database driver + every file transport |
| `transport-all` | All file transports (no database drivers) |

## System dependencies

Most features are pure Rust and need no system libraries. The exceptions are listed below — install the required packages **before** building.

### SMB (`transport-smb`)

The `pavao` crate links against `libsmbclient` via pkg-config. You need the development headers and pkg-config installed.

| Distro | Install command |
|---|---|
| **Fedora / RHEL / CentOS** | `sudo dnf install samba-devel pkgconf-pkg-config` |
| **Debian / Ubuntu** | `sudo apt install libsmbclient-dev pkg-config` |
| **Arch Linux** | `sudo pacman -S smbclient pkgconf` |
| **macOS (Homebrew)** | `brew install samba` |

If `pkg-config` cannot find `smbclient.pc`, set `PKG_CONFIG_PATH` to the directory containing it:

```sh
export PKG_CONFIG_PATH="/usr/lib64/pkgconfig:$PKG_CONFIG_PATH"
```

### Oracle (`oracle`)

Requires Oracle Instant Client (Basic + SDK) in `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH`. See [Oracle connection docs](./connections/oracle.md).

### MSSQL BCP (`mssql-bcp`)

Requires `mssql-tools18` (provides the `bcp` binary). See [MSSQL connection docs](./connections/mssql.md).

### MSSQL ODBC (`mssql-odbc`) / Databricks ODBC (`databricks-odbc`)

Requires `unixODBC` development headers and the relevant ODBC driver:

| Distro | Install command |
|---|---|
| **Fedora / RHEL** | `sudo dnf install unixODBC-devel` |
| **Debian / Ubuntu** | `sudo apt install unixodbc-dev` |
| **macOS** | `brew install unixodbc` |

Then install the [Microsoft ODBC Driver 18](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server) (for MSSQL) or the Simba Spark ODBC driver (for Databricks).

> **Tip:** If you don't need SMB, ODBC, BCP, or Oracle, you can skip all system dependencies entirely. Features like `postgres`, `mssql`, `mysql`, `databricks`, `transport-cloud`, `transport-sftp`, `transport-ftp`, and `transport-sharepoint` are pure Rust and build anywhere without extra packages.

## Feature chain

Features pass through three levels:

```
CLI (potato-etl-cli)
  │  --features transport-cloud
  │  maps to: potato-etl-runtime/transport-cloud
  ▼
Runtime (potato-etl-runtime)
  │  transport-cloud = ["dep:potato-etl-transport-cloud"]
  │  activates optional dependency
  ▼
Transport crate (potato-etl-transport-cloud)
  │  compiled and linked
  ▼
Runtime init()
  │  #[cfg(feature = "transport-cloud")]
  │  register_transport(cloud_factory);
  ▼
create_transport(ConnParams::S3 { .. }) → ObjectStoreTransport
```

## Always available (no feature flag needed)

| Capability | Notes |
|---|---|
| Local filesystem I/O | `read_csv`, `read_json`, `read_parquet`, `write_csv`, `write_json`, `write_parquet` with local paths |
| Parquet support | Enabled by default via `parquet` feature in `common` |
| REST API source/sink | Uses `reqwest` which is always in `common` |

## Common crate feature flags

The `common` crate itself has minimal feature flags:

| Feature | Dependencies | Description |
|---|---|---|
| `parquet` (default) | `parquet`, `bytes` | Parquet file I/O (`read_parquet` / `write_parquet`) |

All transport dependencies are isolated in their own crates — `common` has zero transport deps.

## Runtime behavior

If a pipeline references a remote file driver whose transport feature isn't enabled:

```
Error: No transport registered for driver 'sftp'.
       Make sure the corresponding transport crate is included as a
       dependency (e.g., potato-etl-transport-sftp for SFTP).
```

**Fix:** Rebuild with the required feature: `cargo build -p potato-etl-cli --features transport-sftp`

## Using as a library

If you're using `potato-etl-runtime` as a library (not via the CLI), enable transport features in your `Cargo.toml`:

```toml
[dependencies]
potato-etl-runtime = { path = "../runtime", features = ["postgres", "transport-cloud"] }
```

Then call `init()` once at startup:

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Register all transport factories for enabled features.
    potato_etl_runtime::init();

    let dag = potato_etl_runtime::Dag::from_yaml(&config)?;
    dag.run().await?;
    Ok(())
}
```

## Benefits over feature flags in common

| Before (feature flags in common) | After (separate crates) |
|---|---|
| All transport deps in common's `Cargo.toml` | Each transport crate owns its deps |
| Feature flag combinatorics (7 transports × `cfg` permutations) | Clean crate boundaries |
| `common` compile time grows with each transport | `common` stays lean |
| Downstream crates inherit optional deps | Only linked crates are compiled |

## Compile time impact

Only crates you include are compiled:

| Crate | Extra deps | Approximate compile delta |
|---|---|---|
| `transport-cloud` | `object_store` + cloud SDKs | +15-20s |
| `transport-sftp` | `russh` + crypto | +10-15s |
| `transport-ftp` | `suppaftp` + TLS | +3-5s |
| `transport-sharepoint` | `url` only (reqwest already in common) | +1s |
| `transport-smb` | `pavao` (libsmbclient bindings) | +5-8s |

## See also

- [Getting Started](./getting-started.md) — Quick start guide with build examples
- [File connections](./connections/file-connections.md) — Connection configuration per driver
- [read_parquet](./steps/read-parquet.md) / [write_parquet](./steps/write-parquet.md) — Parquet steps