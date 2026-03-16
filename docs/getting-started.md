# Getting Started

potato_etl is a high-performance ETL (Extract, Transform, Load) tool that moves data between databases and APIs. You define your pipeline in a single YAML file — connections, transforms, and destinations — then run it with one command.

## Quick example

```yaml
# pipeline.yaml
config:
  batch_size: 1000

connections:
  source:
    driver: postgres
    host: localhost
    database: app_db
    auth:
      type: user_pass
      username: etl_user
      password: "s3cr3t"

  target:
    driver: mssql
    host: sql-server.local
    port: 1433
    database: warehouse
    auth:
      type: user_pass
      username: sa
      password: "YourPassword1!"

steps:
  - id: orders
    type: read_db
    from:
      connection: source
      table: orders
      cursor: id

  - id: output
    type: write_db
    input: orders
    target:
      connection: target
      table: orders_copy
```

Run it:

```sh
potato_etl run --config pipeline.yaml
```

That's it. The tool reads from Postgres, streams in batches of 1,000 rows, and writes to SQL Server. No code required.

## How a pipeline file is structured

Every pipeline YAML has these top-level sections:

1. **`config`** — Global settings (batch size, logging)
2. **`connections`** — Named database and API connections
3. **`steps`** — The actual pipeline: sources, transforms, and sinks
4. **`environment`** *(optional)* — Variables evaluated once at pipeline start, referenced in expressions with `$name`
5. **`secrets`** *(optional)* — Secret manager provider configuration for resolving `secret::` references. See [Secret Manager Integration](./secrets.md).

Steps reference connections by name via the `from.connection` (database sources) or `target.connection` (database sinks) field, or `conn` (REST API steps). Steps reference each other via the `input` field — this is how you chain them together.

## Installing

potato_etl is distributed as a Rust binary. Database drivers and file transports are selected at compile time via feature flags — include only what you need.

```sh
# Everything: all database drivers + all file transports
cargo build -p potato-etl-cli --features all --release

# Just Postgres + MSSQL (no remote file transports)
cargo build -p potato-etl-cli --features "postgres,mssql" --release

# Postgres + S3/Azure/GCS cloud storage
cargo build -p potato-etl-cli --features "postgres,transport-cloud" --release

# MySQL + SFTP + FTP
cargo build -p potato-etl-cli --features "mysql,transport-sftp,transport-ftp" --release

# All transports, no database drivers (file-only pipelines)
cargo build -p potato-etl-cli --features transport-all --release
```

The binary is at `target/release/potato_etl`.

### Available features

**Database drivers:**

| Feature | Driver |
|---|---|
| `postgres` | PostgreSQL (pure Rust, sqlx) |
| `mssql` | SQL Server (pure Rust, tiberius) |
| `mssql-bcp` | SQL Server + bcp bulk loader |
| `mssql-odbc` | SQL Server + ODBC write path |
| `mysql` | MySQL / MariaDB / Aurora (pure Rust, sqlx) |
| `oracle` | Oracle (requires Instant Client) |
| `databricks` | Databricks SQL (REST API) |
| `databricks-odbc` | Databricks + ODBC read path |
| `databricks-thrift` | Databricks + Thrift protocol |

**File transports:**

| Feature | Transport | Crate |
|---|---|---|
| `transport-cloud` | S3, Azure Blob, GCS | `potato-etl-transport-cloud` |
| `transport-sftp` | SFTP | `potato-etl-transport-sftp` |
| `transport-ftp` | FTP / FTPS | `potato-etl-transport-ftp` |
| `transport-sharepoint` | SharePoint Online | `potato-etl-transport-sharepoint` |
| `transport-smb` | SMB / CIFS | `potato-etl-transport-smb` |
| `transport-all` | All of the above | — |

**Aggregate features:**

| Feature | Includes |
|---|---|
| `all` | Every database driver + every file transport |
| `transport-all` | All file transports (no database drivers) |

> **Note:** Local filesystem file I/O (`read_csv`, `read_json`, `read_parquet`, `write_csv`, `write_json`, `write_parquet` with local paths) is always available — no feature flag needed. Transport features are only required for *remote* storage (SFTP, S3, Azure, etc.). Parquet support (`parquet` feature) is enabled by default.

> **System dependencies:** Most features are pure Rust and need no system libraries. The exceptions are `transport-smb` (requires `libsmbclient-dev`), `oracle` (requires Instant Client), and the ODBC features (require `unixODBC-dev`). See [System dependencies](./feature-flags.md#system-dependencies) for per-distro install commands.

## CLI commands

| Command | What it does |
|---|---|
| `run` | Execute the full pipeline |
| `dry-run` | Run 1 batch per source, **no writes** — safe for testing |
| `schema` | Show the Arrow schema of any step's output |
| `validate` | Check config syntax and wiring without connecting to anything |
| `list-steps` | List all step IDs in execution order |
| `step-info` | Show details for a single step |
| `explain` | Describe what the pipeline would do (no connection needed) |
| `to-json` | Convert a YAML pipeline config to JSON |

### Common flags

```sh
# Override batch size
potato_etl run --config pipeline.yaml --batch-size 5000

# Verbose logging (shows schemas + data previews)
potato_etl run --config pipeline.yaml --verbose

# Dry-run with 3 batches, pretty output
potato_etl dry-run --config pipeline.yaml --batches 3 --format pretty
```

## Next steps

- [Connections](./connections/index.md) — How to configure every supported database and API
- [Pipeline Steps](./steps/index.md) — All available step types (sources, transforms, sinks)
- [Configuration Reference](./configuration-reference.md) — Every config option in detail
- [Examples](./examples.md) — Complete pipeline examples for common scenarios
- [Transport Crates & Feature Flags](./feature-flags.md) — Compile-time transport configuration and architecture

> **Tip:** You don't always need a database connection. `read_csv`, `read_json`, `read_parquet`, `write_csv`, `write_json`, and `write_parquet` steps let you build file-only pipelines — perfect for prototyping, testing, or format conversion. File steps also support remote storage (SFTP, S3, Azure Blob, GCS, SharePoint, FTP, SMB) via [file connections](./connections/file-connections.md) — just add the corresponding `transport-*` feature flag at build time.