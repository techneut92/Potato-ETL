# Configuration Reference

Complete reference for every configuration option in a potato_etl pipeline file.

## File format

Pipeline files can be written in **YAML** or **JSON**. Both formats support exactly the same options.

```sh
potato_etl run --config pipeline.yaml
potato_etl run --config pipeline.json
```

## Top-level structure

```yaml
config:           # Global pipeline settings
  ...
connections:      # Named database and API connections
  ...
secrets:          # Secret manager provider configuration (optional)
  ...
environment:      # Pipeline-level variables (optional)
  ...
steps:            # The pipeline: sources → transforms → sinks
  ...
```

The `steps` key can also be written as `pipeline` (legacy alias).

> **Async loading required for secrets:** When using `secret::` references in your
> config, you must load the pipeline with `Dag::from_yaml_async()` instead of
> `Dag::from_yaml()`. The sync loader will error if it detects `secret::`
> references, because secret resolution requires async I/O.

---

## config

Global pipeline behavior.

```yaml
config:
  batch_size: 1000
  channel_capacity: 4
  log_level: info
```

| Field | Default | Description |
|---|---|---|
| `batch_size` | `1000` | Rows per batch. Controls how many rows are fetched and processed at a time. |
| `channel_capacity` | `4` | Maximum RecordBatches buffered in each inter-component channel. Higher values hide latency spikes (e.g. a slow sink doesn't stall the source) at the cost of more memory. Peak memory per edge ≈ `channel_capacity × batch_size × avg_row_bytes`. |
| `log_level` | `info` | Logging verbosity: `error`, `warn`, `info`, `debug`, `trace`. At `debug`, every step logs its schema and a 10-row data preview. |

---

## connections

Named connections referenced by `conn` fields in steps. For detailed docs per driver, see [Connections](./connections/index.md).

### Database connections

```yaml
connections:
  name:
    driver: <driver>
    host: <hostname>
    port: <number>            # optional — driver default used
    database: <name>
    auth:
      type: <auth_type>
      # ... auth fields
    options:                  # optional — driver-specific tuning
      ...
```

**Supported drivers:**

| Driver | Aliases | Default port |
|---|---|---|
| `postgres` | `postgresql` | 5432 |
| `mssql` | — | 1433 |
| `oracle` | — | 1521 |
| `mysql` | `aurora`, `mariadb` | 3306 |
| `databricks` | — | (HTTP) |
| `rest_api` | — | — |

### File transport connections

```yaml
connections:
  name:
    driver: <file_driver>        # local | sftp | s3 | azure_blob | gcs | sharepoint | ftp | smb
    base_path: <path>            # base path for relative file references
    auth:                        # driver-specific authentication
      type: <file_auth_type>
      # ... auth fields
    # ... driver-specific fields
```

**Supported file drivers:**

| Driver | Aliases | Auth types | Transport crate | Notes |
|---|---|---|---|---|
| `local` | -- | `none` | *(built into common)* | Local filesystem |
| `sftp` | -- | `user_pass`, `key` | `potato-etl-transport-sftp` | SSH private key or password |
| `s3` | -- | `access_key`, `role_arn`, `default_credentials` | `potato-etl-transport-cloud` | AWS S3 or S3-compatible |
| `azure_blob` | -- | `client_credentials`, `connection_string`, `sas_token`, `default_credentials` | `potato-etl-transport-cloud` | Azure Blob Storage |
| `gcs` | -- | `service_account`, `default_credentials` | `potato-etl-transport-cloud` | Google Cloud Storage |
| `sharepoint` | `sharepoint_online` | `client_credentials` | `potato-etl-transport-sharepoint` | MS Graph API |
| `ftp` | `ftps` | `user_pass`, `none` | `potato-etl-transport-ftp` | FTP with optional TLS |
| `smb` | `cifs` | `user_pass` | `potato-etl-transport-smb` | Windows file shares |

Each transport lives in its own crate — include only what you need. See [Transport Crates](./feature-flags.md).

File steps reference connections using `from.connection` (sources) or `target.connection` (sinks):

```yaml
steps:
  - id: data
    type: read_csv
    from:
      connection: my_sftp
      path: incoming/data.csv      # relative to base_path

  - id: output
    type: write_json
    input: data
    target:
      connection: my_s3
      path: processed/result.json  # relative to base_path
    pretty: true
    wrap_key: employees
```

For full details, see [File Connections](./connections/file-connections.md).

### Auth types

| Type | Fields | Drivers |
|---|---|---|
| `user_pass` | `username`, `password` | All SQL |
| `pat` | `token` | Databricks |
| `oauth2_client_credentials` | `client_id`, `client_secret` | Databricks |
| `kerberos` | `principal`, `keytab` (opt) | MSSQL, Postgres, Oracle |
| `windows_integrated` | *(none)* | MSSQL |
| `certificate` | `cert_path`, `key_path`, `ca_path` (opt) | Postgres, MySQL, Oracle |
| `aws_iam` | `username`, `region` | Aurora/RDS |
| `none` | *(none)* | Any (trusted local) |

### PostgreSQL options

```yaml
options:
  ssl: require                    # disable|allow|prefer|require|verify-ca|verify-full
  connect_timeout: 30             # seconds
  application_name: my_etl        # visible in pg_stat_activity
  max_connections: 10             # connection pool size (default: 5)
  staging_table: true             # COPY BINARY via temp staging table for upsert (5-10x faster)
  init_sql:                       # SQL executed on each new pool connection
    - "SET work_mem = '256MB'"
    - "SET statement_timeout = 60000"
```

### SQL Server (MSSQL) options

```yaml
options:
  mode: tiberius               # tiberius (default) | bcp | odbc
  trust_cert: true                # ONLY for self-signed dev certs
  application_name: my_etl        # visible in sys.dm_exec_sessions
  login_timeout: 30               # seconds
  batch_size: 50000               # rows per bulk write operation (default: 10,000)
  staging_table: true             # force staging table for all write modes
  bcp_path: /opt/mssql-tools18/bin/bcp  # auto-detected if absent; implies mode: bcp
  init_sql:                       # SQL executed on each new connection
    - "SET LOCK_TIMEOUT 5000"
```

### MySQL options

```yaml
options:
  ssl_mode: required              # disabled|preferred|required
  connect_timeout: 30             # seconds
  charset: utf8mb4                # character set (default: utf8mb4)
  init_sql:                       # SQL executed on each new pool connection
    - "SET SESSION group_concat_max_len = 1048576"
```

### Oracle connection modes

```yaml
# Host + service name (preferred for Oracle 12c+)
driver: oracle
host: oracle.example.com
port: 1521
service: XEPDB1

# Host + SID (legacy)
driver: oracle
host: oracle.example.com
sid: XE

# TNS alias
driver: oracle
tns: XEPDB1_PROD
```

### Databricks options

```yaml
connections:
  dbx:
    driver: databricks
    host: adb-123.4.azuredatabricks.net
    http_path: /sql/1.0/warehouses/abc123
    auth:
      type: pat
      token: "${DATABRICKS_TOKEN}"
    options:
      catalog: main               # Unity Catalog (workspace default if absent)
      schema: analytics            # default schema
      connect_timeout: 120         # HTTP timeout in seconds
```

### REST API connections

```yaml
connections:
  name:
    driver: rest_api
    base_url: "https://..."
    auth:                         # optional
      type: bearer                # bearer | basic | api_key
      token: "${TOKEN}"
    headers:                      # optional — default headers for all requests
      Accept: "application/json"
    timeout_secs: 30              # default: 30
    rate_limit_rps: 10.0          # optional — max requests per second
```

**REST auth types:**

| Type | Fields |
|---|---|
| `bearer` | `token` |
| `basic` | `username`, `password` |
| `api_key` | `header`, `key` |

---

## secrets

Secret manager provider configuration (optional). Configures authentication and
endpoints for external secret managers. Connection values can then reference
secrets using the `secret::<provider>/<path>[#field]` syntax.

See [Secret Manager Integration](./secrets.md) for full documentation.

```yaml
secrets:
  vault:
    address: https://vault.internal:8200
    mount: secret
    auth:
      method: token

  azure:
    vault_url: https://my-vault.vault.azure.net

  gcp:
    project: my-gcp-project
```

| Provider | Prefix | Description |
|---|---|---|
| `vault` | `secret::vault/...` | HashiCorp Vault KV v2 |
| `azure` | `secret::azure/...` | Azure Key Vault |
| `gcp` | `secret::gcp/...` | Google Cloud Secret Manager |

---

## environment

Pipeline-level variables evaluated once at run start. Reference them in expressions with `$name` or `env("name")`.

```yaml
environment:
  load_ts: now()
  label: '"nightly_sync"'

steps:
  - id: enrich
    type: map
    input: source
    columns:
      inserted_at: $load_ts
      pipeline: env("label")
```

---

## steps

For detailed docs per step type, see [Pipeline Steps](./steps/index.md).

### read_db

```yaml
- id: <id>
  type: read_db
  from:
    connection: <connection>         # named connection (required)
    table: <table>                   # or query:
    query: "SELECT ..."              # alternative to table
    cursor: <column>                 # cursor pagination column
    schema: <namespace>              # SQL schema (e.g. hr, dbo)
  batch_size: 5000                 # override global batch_size
  normalize_columns: true          # lowercase column names (auto for Oracle)
  exclude:                         # drop columns immediately
    - col_to_skip
  schema:                          # unified schema block (preferred)
    arrow:
      columns:
        col_name:
          type: arrow_type
  options:                         # per-step driver options
    prefetch_rows: 500             # Oracle: rows per OCI round-trip
    fetch_array_size: 1000         # Oracle: internal array size
    identifier_case: upper         # as_is | upper | lower
```

### rest_api (source)

```yaml
- id: <id>
  type: rest_api
  conn: <connection>               # optional
  url: /path                       # appended to base_url, or full URL
  method: GET                      # GET | POST | PUT | PATCH | DELETE
  data_path: rows                  # dot path to data array (null = top-level)
  params:                          # extra query parameters
    state: open
  body: '{"query": "..."}'         # request body (POST/PUT)
  auth:                            # override connection auth
    type: bearer
    token: "..."
  headers:                         # extra headers (merged with connection)
    X-Custom: value
  rate_limit_rps: 10.0
  timeout_secs: 30
  allow_non_2xx: false             # true = non-2xx → empty, not error
  dedup_key: id                    # cross-page dedup field
  schema:                          # unified schema block (preferred)
    arrow:
      columns:
        col_name:
          type: arrow_type
  pagination:
    strategy: cursor               # cursor | offset | page | link_header | none
    # cursor strategy:
    cursor_path: meta.next_cursor
    cursor_param: cursor
    page_size: 100
    size_param: limit              # optional
    # offset strategy:
    offset_param: skip
    limit_param: take
    page_size: 100
    total_count_path: meta.total   # optional — stop condition
    has_more_path: has_more        # optional — stop condition
    # page strategy:
    page_param: page
    size_param: per_page
    page_size: 50
    first_page: 1
    total_count_path: count        # optional
    has_more_path: has_more        # optional
    # link_header strategy: (no extra fields)
```

### read_json (source)

```yaml
# Direct path (local filesystem):
- id: <id>
  type: read_json
  path: data/candidates.json     # filesystem path to JSON file (supports globs)
  data_path: candidates          # optional: dot-notation path to array
  batch_size: 500                # optional: override global batch_size
  sort_glob: name                # optional: name (asc, default) or name_desc (desc)

# Connection-based (local, SFTP, S3, etc.):
- id: <id>
  type: read_json
  from:
    connection: data_sftp        # named file connection
    path: incoming/candidates.json  # relative to connection's base_path
  data_path: candidates
  normalize_columns: true        # optional
  exclude:                       # optional
    - internal_field
  schema:                        # optional: unified schema block
    arrow:
      columns:
        col_name:
          type: arrow_type
```

### read_csv (source)

```yaml
# Direct path (local filesystem):
- id: <id>
  type: read_csv
  path: data/employees.csv       # filesystem path to CSV file (supports globs)
  delimiter: ","                 # optional (default: ",")
  has_header: true               # optional (default: true)
  batch_size: 5000               # optional: override global batch_size
  sort_glob: name                # optional: name (asc, default) or name_desc (desc)

# Connection-based (local, SFTP, S3, etc.):
- id: <id>
  type: read_csv
  from:
    connection: incoming_sftp    # named file connection
    path: reports/employees.csv  # relative to connection's base_path
  delimiter: ","
  normalize_columns: true        # optional
  exclude:                       # optional
    - internal_field
  schema:                        # optional: unified schema block
    arrow:
      columns:
        col_name:
          type: arrow_type
```

### read_parquet (source)

```yaml
# Direct path (local filesystem):
- id: <id>
  type: read_parquet
  path: data/events.parquet        # filesystem path (supports globs)
  columns: [event_id, user_id]     # optional: column projection
  batch_size: 8192                 # optional (default: config.batch_size)
  sort_glob: name                  # optional: name (asc, default) or name_desc (desc)

# Connection-based (local, SFTP, S3, etc.):
- id: <id>
  type: read_parquet
  from:
    connection: data_lake_s3       # named file connection
    path: events/latest.parquet    # relative to connection's base_path
  columns: [event_id, user_id]    # optional
```

### filter

```yaml
# Simple form:
- id: <id>
  type: filter
  input: <step>
  column: status
  value: active

# Expression form:
- id: <id>
  type: filter
  input: <step>
  condition: "status == \"active\" and amount >= 100"
```

### map

```yaml
- id: <id>
  type: map
  input: <step>
  select_only: false               # true = drop all unlisted columns
  columns:
    new_col: expression
    revenue: price * quantity
    load_ts: now()
    uid: json_get(payload, "user.id")
```

### rename

```yaml
- id: <id>
  type: rename
  input: <step>
  columns:
    old_name: new_name
```

### flatten

```yaml
- id: <id>
  type: flatten
  input: <step>
  select:
    target_col: source.nested.path
    item: source.array[0]
```

### unnest

```yaml
- id: <id>
  type: unnest
  input: <step>
  column: array_column           # array column to explode into rows
  parent_fields:                 # optional: carry parent columns
    output_name: parent_column
  fields:                        # optional: extract sub-fields from each element
    output_name: element_key
```

### join

```yaml
- id: <id>
  type: join
  left: <step>
  right: <step>
  on: <key_column>
  how: inner                       # inner | left | full
```

### aggregate

```yaml
- id: <id>
  type: aggregate
  input: <step>
  group_by: [col1, col2]
  metrics:
    metric_name: sum(column)       # sum | count | avg | mean | min | max | first
```

### python_transform

```yaml
# Inline code:
- id: <id>
  type: python_transform
  input: <step>
  code: |
    result = table.filter(table.column('status') == 'active')

# Named function:
- id: <id>
  type: python_transform
  input: <step>
  function: my_function_name
```

### write_db

```yaml
- id: <id>
  type: write_db
  input: <step>
  target:
    connection: <connection>         # named connection (required)
    table: <table>                   # target table name
    schema: <namespace>              # SQL schema (e.g. hr, dbo)
  mode: append                     # append | insert_ignore | upsert | merge_delete | truncate
  create_table: never              # never | if_not_exists | replace
  batch_size: 20000                # sub-chunk incoming batches

  # -- Unified schema block (preferred) ----------------------------------------
  schema:
    arrow:
      columns:
        col_timestamp:
          type: "timestamp[us, UTC]"
    database:
      columns:
        col:
          type: "SQL_TYPE"         # replaces old type_override
          primary_key: true
          unique: false
          nullable: true
          generated: false         # IDENTITY / SERIAL / AUTO_INCREMENT
          check_expr: "col >= 0"
          default_expr: "0"
          on_update_expr: "now()"  # MySQL: ON UPDATE; others: trigger
          description: "Human note"
          foreign_key:
            table: other_table
            column: id
            schema: other_schema   # optional — schema of the referenced table
      indexes:
        idx_name:
          columns: [col1, col2]
          unique: false
      constraints:
        chk_name:
          check: "col >= 0"

  values:                          # column mapping (batch col or env var / null)
    TARGET_COL: $source_col        # batch col → renamed, or env var → new column
    INJECTED_COL: $env_var_name    # env var → broadcast value as new column
    NULL_COL: null                 # explicit NULL (not DEFAULT — see write_db docs)

  # -- Legacy flat fields (still supported, schema: block takes precedence) ----
  # arrow_overrides:               # prefer schema.arrow.columns
  #   col: arrow_type
  # column_options:                # prefer schema.database.columns
  #   col:
  #     db_type: "SQL_TYPE"        # replaces removed type_override
  #     primary_key: true

  options:                         # per-step driver options
    # MSSQL:
    mode: bcp                      # tiberius (default) | bcp | odbc
    bcp_path: /opt/mssql-tools18/bin/bcp
    bcp_staging: true              # force staging even for append/truncate
    mssql:
      batch_size: 50000            # rows per bulk write operation
      staging_table: true          # force staging table for all write modes
    # Postgres:
    postgres:
      staging_table: true          # COPY BINARY via temp staging table (5-10x faster)
      max_connections: 10          # override connection pool size
    # Oracle:
    direct_path: true              # APPEND_VALUES fast bulk INSERT
    parallel: 4                    # parallel DML degree
    oci_batch_size: 50000          # rows per OCI execute call
    prefetch_rows: 500             # read-side Oracle tuning
    fetch_array_size: 1000
    # Databricks:
    databricks:
      chunk_prefetch: 4            # parallel Arrow IPC chunk downloads
      thrift_fetch_size: 100000    # rows per Thrift RPC
      warehouse_timeout: 600       # max seconds for warehouse cold-start
    # Cross-driver:
    identifier_case: upper         # as_is | upper | lower
```

### scd2_sink

```yaml
- id: <id>
  type: scd2_sink
  input: <step>
  target:
    connection: <connection>         # named connection (required)
    table: <history_table>           # target table name
    schema: <namespace>              # SQL schema (e.g. hr, dbo)
  key: <business_key_column>
  track:                           # columns to monitor (all non-key if omitted)
    - salary
    - department
  close_missing: false             # true = expire keys not seen in incoming data
  create_table: if_not_exists
  batch_size: 10000                # sub-chunk incoming batches

  # Unified schema block (same structure as write_db)
  schema:
    database:
      columns:
        col:
          type: "SQL_TYPE"
          primary_key: true
          nullable: false

  # Legacy flat fields (still supported)
  # column_options:
  #   col:
  #     db_type: "SQL_TYPE"
  #     primary_key: true

  scd2_columns:                    # custom system column names
    valid_from: effective_from     # default: valid_from
    valid_to: effective_to         # default: valid_to
    is_current: is_latest          # default: is_current
    scd_id: surrogate_key          # default: scd_id
  options:                         # per-step driver options (same as write_db)
    mode: bcp                      # tiberius (default) | bcp | odbc
    identifier_case: upper
```

### rest_api_sink

```yaml
- id: <id>
  type: rest_api_sink
  input: <step>
  conn: <connection>               # optional
  url: /api/endpoint
  method: POST                     # POST | PUT | PATCH | DELETE | GET
  mode: per_row                    # per_row | batch
  auth:                            # override connection auth
    type: bearer
    token: "..."
  headers:                         # extra headers (merged with connection)
    X-Custom: value
  field_map:                       # nested JSON construction
    user.name: full_name
    address.city: city
  json_column: payload_json        # use a pre-built JSON column as body
  wrap_key: records                # batch mode: wrap array in {"records": [...]}
  url_template: /api/users/{id}    # per_row: URL with {column} placeholders
  rate_limit_rps: 10.0
  timeout_secs: 30
  allow_non_2xx: false
```

### write_csv (sink)

```yaml
# Direct path (local filesystem):
- id: <id>
  type: write_csv
  input: <step>
  path: output/result.csv        # filesystem path for output
  delimiter: ","                 # optional (default: ",")
  has_header: true               # optional (default: true)

# Connection-based (local, SFTP, S3, etc.):
- id: <id>
  type: write_csv
  input: <step>
  target:
    connection: reports_sftp     # named file connection
    path: monthly/result.csv     # relative to connection's base_path
  delimiter: ","
```

### write_json (sink)

```yaml
# Direct path (local filesystem):
- id: <id>
  type: write_json
  input: <step>
  path: output/result.json       # filesystem path for output
  pretty: true                   # optional (default: false)
  wrap_key: employees            # optional: wrap array in {"employees": [...]}

# Connection-based (local, SFTP, S3, etc.):
- id: <id>
  type: write_json
  input: <step>
  target:
    connection: data_lake_s3     # named file connection
    path: processed/result.json  # relative to connection's base_path
  pretty: true
  wrap_key: employees
```

### write_parquet (sink)

```yaml
# Direct path (local filesystem):
- id: <id>
  type: write_parquet
  input: <step>
  path: output/result.parquet      # filesystem path for output
  compression: zstd                # optional: none, snappy, gzip, lz4, zstd (default: snappy)

# Connection-based (local, SFTP, S3, etc.):
- id: <id>
  type: write_parquet
  input: <step>
  target:
    connection: data_lake_s3       # named file connection
    path: processed/result.parquet # relative to connection's base_path
  compression: zstd
```

---

## Step driver options reference

These options can be set at the connection level (in `options:`) or at the step level (in the step's `options:`). Step-level values override connection-level values.

For detailed documentation, see [Step Driver Options](./steps/driver-options.md).

### MSSQL

| Option | Default | Scope | Description |
|---|---|---|---|
| `mode` | `tiberius` | write | Write-path mode: `tiberius` (pure Rust), `bcp` (CLI bulk-loader, ~500k-2M rows/sec), or `odbc` (ODBC Driver 18, no KEEPNULLS). |
| `bcp_path` | auto | write | Path to bcp binary. Auto-detected from well-known paths. Implicitly sets `mode: bcp`. |
| `bcp_staging` | auto | write | Force/skip staging table. Auto = staging for MERGE modes only. |
| `batch_size` | `10000` | write | Rows per bulk commit batch. |
| `staging_table` | auto | write | `true` = force staging for all modes; `false` = disable; auto = MERGE modes only. |

### Oracle

| Option | Default | Scope | Description |
|---|---|---|---|
| `prefetch_rows` | driver (2) | read | Rows fetched per OCI round-trip. More = fewer round-trips. |
| `fetch_array_size` | driver | read | Internal OCI array size. |
| `direct_path` | `false` | write | APPEND_VALUES hint. Fast bulk INSERT for heap tables without triggers/FK. |
| `parallel` | none | write | Parallel DML degree. Best combined with `direct_path`. |
| `oci_batch_size` | from batch_size | write | Max rows per OCI execute call. |

### Postgres

| Option | Default | Scope | Description |
|---|---|---|---|
| `staging_table` | `false` | write | COPY BINARY via temp staging table for upsert. ~5-10x faster. |
| `max_connections` | `5` | read/write | Connection pool size. |

### Databricks

| Option | Default | Scope | Description |
|---|---|---|---|
| `mode` | `api` | read | Read-path mode: `api` (REST), `odbc` (Simba), or `thrift` (RPC). |
| `chunk_prefetch` | `4` | read | Arrow IPC chunks to download in parallel (API mode). |
| `thrift_fetch_size` | `100000` | read | Rows per Thrift FetchResults RPC. |
| `warehouse_timeout` | `0` | read | Max seconds to wait for warehouse cold-start (0 = disabled). |

### Cross-driver

| Option | Default | Scope | Description |
|---|---|---|---|
| `identifier_case` | `as_is` | read/write | Case transform for SQL identifiers: `as_is`, `upper`, `lower`. |