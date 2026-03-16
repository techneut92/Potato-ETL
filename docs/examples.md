# Examples

Practical pipeline examples for common ETL scenarios.

## Copy a table between databases

The simplest pipeline: read from one database, write to another.

```yaml
config:
  batch_size: 5000

connections:
  source:
    driver: postgres
    host: localhost
    database: app_db
    auth:
      type: user_pass
      username: etl_user
      password: "etl_pass"

  target:
    driver: mssql
    host: sql-server.local
    database: warehouse
    auth:
      type: user_pass
      username: sa
      password: "YourPassword1!"
    options:
      trust_cert: true

steps:
  - id: src
    type: read_db
    from:
      connection: source
      table: orders
      cursor: id

  - id: out
    type: write_db
    input: src
    target:
      connection: target
      table: orders
    mode: truncate
```

## Multi-source join

Read from two different databases, join the results, compute a derived column, and upsert into a target.

```yaml
config:
  batch_size: 500

connections:
  pg:
    driver: postgres
    host: localhost
    database: etl_demo
    auth:
      type: user_pass
      username: etl_user
      password: "etl_pass"

  mssql:
    driver: mssql
    host: localhost
    database: etl_demo
    auth:
      type: user_pass
      username: sa
      password: "YourPassword1!"

steps:
  - id: employees
    type: read_db
    from:
      connection: pg
      table: employees
      cursor: id

  - id: departments
    type: read_db
    from:
      connection: mssql
      table: departments
      cursor: id

  - id: joined
    type: join
    left: employees
    right: departments
    on: department_id
    how: inner

  - id: with_bonus
    type: map
    input: joined
    columns:
      bonus: salary * 0.10

  - id: output
    type: write_db
    input: with_bonus
    target:
      connection: pg
      table: employee_details
    mode: upsert
    schema:
      database:
        columns:
          id:
            primary_key: true
```

## REST API to database with SCD2

Fetch data from a REST API, rename fields, and maintain a change-history table.
When the API returns all employees (full snapshot), `close_missing: true` ensures
that employees no longer returned by the API are expired in the history table.

```yaml
config:
  batch_size: 100

connections:
  afas:
    driver: rest_api
    base_url: "https://12345.afas.online/profitrestservices"
    auth:
      type: api_key
      header: Authorization
      key: "${AFAS_TOKEN}"
    headers:
      Accept: "application/json"
    timeout_secs: 30
    rate_limit_rps: 5.0

  pg:
    driver: postgres
    host: localhost
    database: etl_demo
    auth:
      type: user_pass
      username: etl_user
      password: "etl_pass"

steps:
  - id: employees_raw
    type: rest_api
    conn: afas
    url: /connectors/HrEmployees
    data_path: rows
    pagination:
      strategy: offset
      offset_param: skip
      limit_param: take
      page_size: 100
    schema:
      arrow:
        columns:
          EmId:
            type: utf8
          SaSa:
            type: float64
          HiDa:
            type: date32

  - id: employees
    type: rename
    input: employees_raw
    columns:
      EmId: employee_id
      FiNm: first_name
      LaNm: last_name
      SaSa: salary
      DpNm: department_name
      FtId: employment_type
      HiDa: hire_date

  - id: employees_history
    type: scd2_sink
    input: employees
    target:
      connection: pg
      schema: hr
      table: employees_history
    key: employee_id
    track:
      - salary
      - department_name
      - employment_type
    close_missing: true          # full-snapshot API — expire absent employees
    create_table: if_not_exists
    schema:
      database:
        columns:
          employee_id:
            type: "VARCHAR(50)"
            primary_key: true
            nullable: false
          salary:
            type: "NUMERIC(12,2)"
```

## Filter + aggregate

Read orders, filter to high-value active orders, then aggregate by customer.

```yaml
config:
  batch_size: 2000

connections:
  db:
    driver: postgres
    host: localhost
    database: shop
    auth:
      type: user_pass
      username: etl_user
      password: "etl_pass"

steps:
  - id: orders
    type: read_db
    from:
      connection: db
      table: orders
      cursor: id

  - id: enriched
    type: map
    input: orders
    columns:
      revenue: price * quantity

  - id: active_high_value
    type: filter
    input: enriched
    condition: "status == \"completed\" and revenue >= 100"

  - id: by_customer
    type: aggregate
    input: active_high_value
    group_by: [customer_id]
    metrics:
      total_revenue: sum(revenue)
      order_count: count()
      avg_order: avg(revenue)

  - id: output
    type: write_db
    input: by_customer
    target:
      connection: db
      table: customer_stats
    mode: truncate
    create_table: if_not_exists
```

## GitHub issues to Postgres

Fetch open issues from a GitHub repository and store them.

```yaml
config:
  batch_size: 100

connections:
  github:
    driver: rest_api
    base_url: "https://api.github.com"
    auth:
      type: bearer
      token: "${GITHUB_TOKEN}"
    headers:
      Accept: "application/vnd.github.v3+json"
      X-GitHub-Api-Version: "2022-11-28"
    timeout_secs: 30
    rate_limit_rps: 10.0

  pg:
    driver: postgres
    host: localhost
    database: etl_demo
    auth:
      type: user_pass
      username: etl_user
      password: "etl_pass"

steps:
  - id: issues
    type: rest_api
    conn: github
    url: /repos/owner/repo/issues
    data_path: null
    pagination:
      strategy: link_header

  - id: flat
    type: flatten
    input: issues
    select:
      issue_id: id
      title: title
      state: state
      author: user.login
      created: created_at

  - id: output
    type: write_db
    input: flat
    target:
      connection: pg
      table: github_issues
    mode: upsert
    create_table: if_not_exists
    schema:
      database:
        columns:
          issue_id:
            primary_key: true
```

## Python transform

Add a computed column using inline Python.

```yaml
config:
  batch_size: 500

connections:
  pg:
    driver: postgres
    host: localhost
    database: etl_demo
    auth:
      type: user_pass
      username: etl_user
      password: "etl_pass"

steps:
  - id: src
    type: read_db
    from:
      connection: pg
      table: employees
      cursor: id

  - id: with_bonus
    type: python_transform
    input: src
    code: |
      import pyarrow.compute as pc
      bonus = pc.multiply(table.column("salary"), 0.10)
      result = table.append_column("bonus", bonus.cast(pa.float64()))

  - id: output
    type: write_db
    input: with_bonus
    target:
      connection: pg
      table: employees_enriched
    mode: truncate
```

## Databricks read + write

Read from one Databricks catalog/schema and write to another.

```yaml
config:
  batch_size: 10000

connections:
  dbx:
    driver: databricks
    host: adb-1234567890123456.4.azuredatabricks.net
    http_path: /sql/1.0/warehouses/abc123def456
    auth:
      type: pat
      token: "${DATABRICKS_TOKEN}"
    options:
      catalog: main
      schema: raw

steps:
  - id: events
    type: read_db
    from:
      connection: dbx
      table: click_events

  - id: active
    type: filter
    input: events
    condition: "event_type == \"click\""

  - id: output
    type: write_db
    input: active
    target:
      connection: dbx
      table: main.analytics.filtered_clicks
    mode: append
```

## Unix epoch conversion with audit columns

Read from a source where timestamps are stored as unix epoch integers, convert them to proper timestamps, add pipeline audit columns, and let the database manage `updated_at` via a trigger.

```yaml
config:
  batch_size: 5000

connections:
  source_db:
    driver: postgres
    host: source.example.com
    database: raw_data
    auth:
      type: user_pass
      username: etl_user
      password: "${SOURCE_DB_PASSWORD}"

  target_db:
    driver: postgres
    host: target.example.com
    database: warehouse
    auth:
      type: user_pass
      username: etl_user
      password: "${TARGET_DB_PASSWORD}"

environment:
  insert_at_val: now()            # evaluated once at pipeline start

steps:
  - id: source_orders
    type: read_db
    from:
      connection: source_db
      table: raw_orders
      cursor: id

  # Convert unix epoch integers → proper timestamps, add audit columns
  - id: transform
    type: map
    input: source_orders
    columns:
      created: cast(created, "timestamp[s, UTC]")
      updated: cast(updated, "timestamp[s, UTC]")
      inserted_at: $insert_at_val
      updated_at: now()           # pipeline stamps it; server refreshes via trigger

  - id: sink_orders
    type: write_db
    input: transform
    target:
      connection: target_db
      table: orders
    mode: upsert
    create_table: if_not_exists
    schema:
      database:
        columns:
          order_id:
            primary_key: true
          inserted_at:
            type: TIMESTAMPTZ
            nullable: false
          updated_at:
            type: TIMESTAMPTZ
            nullable: false
            on_update_expr: "now()"   # Postgres: BEFORE UPDATE trigger
```

**Key points:**
- `cast(created, "timestamp[s, UTC]")` interprets the integer as seconds since epoch and produces `Timestamp[us, UTC]`. Use `"timestamp[ms, UTC]"` for millisecond epochs.
- `$insert_at_val` references the `environment:` variable — evaluated once at pipeline start, same value for every row across all batches.
- `now()` in the map step gives the current time per batch (slightly different per batch — use `$insert_at_val` if you need one consistent timestamp).
- `on_update_expr: "now()"` creates a `BEFORE UPDATE` trigger on Postgres (or inline `ON UPDATE CURRENT_TIMESTAMP` on MySQL) so the database refreshes `updated_at` on any future row modification, even outside this pipeline.
- `schema.database.columns.<col>.type` ensures the DDL uses `TIMESTAMPTZ` instead of the default `TIMESTAMP` inferred from Arrow.

## CSV to JSON conversion

Read a CSV file, filter and aggregate, then write results as both CSV and JSON. No database connection required.

```yaml
config:
  batch_size: 1000

steps:
  - id: employees
    type: read_csv
    path: data/employees.csv

  - id: active_only
    type: filter
    input: employees
    column: active
    value: "true"

  - id: write_active_json
    type: write_json
    input: active_only
    path: output/active_employees.json
    pretty: true
    wrap_key: employees

  - id: dept_stats
    type: aggregate
    input: active_only
    group_by: [department]
    metrics:
      headcount: count()
      total_salary: sum(salary)
      avg_salary: avg(salary)

  - id: write_stats_csv
    type: write_csv
    input: dept_stats
    path: output/department_stats.csv

  - id: write_stats_json
    type: write_json
    input: dept_stats
    path: output/department_stats.json
    pretty: true
```

**Key points:**
- No `connections:` block needed -- file I/O pipelines work standalone.
- `wrap_key: employees` wraps the JSON output in `{"employees": [...]}`.
- A single source can fan out to multiple sinks (both CSV and JSON).

## JSON to CSV with delimiter

Read a JSON API response dump and flatten it to a semicolon-delimited CSV.

```yaml
steps:
  - id: raw
    type: read_json
    path: data/candidates.json
    data_path: candidates

  - id: candidates
    type: flatten
    input: raw
    select:
      id: id
      name: first_name
      email: email
      status: status

  - id: output
    type: write_csv
    input: candidates
    path: output/candidates_flat.csv
    delimiter: ";"
    has_header: true
```

## Nested JSON normalization to files

Normalize a deeply nested JSON structure into separate relational tables -- all as local files, no database required. This is the file-based equivalent of the database normalization example.

```yaml
config:
  batch_size: 1000

steps:
  - id: raw
    type: read_json
    path: data/candidates.json
    data_path: candidates

  # Flat candidate records → CSV
  - id: candidates_flat
    type: flatten
    input: raw
    select:
      candidate_id: id
      first_name: first_name
      last_name: last_name
      email: email

  - id: write_candidates
    type: write_csv
    input: candidates_flat
    path: output/candidates.csv

  # Level 1 unnest: talent pools
  - id: pools_exploded
    type: unnest
    input: raw
    column: talent_pools
    parent_fields:
      candidate_id: id
    fields:
      pool_id: id
      pool_name: name
      pool_addresses: addresses

  # Junction: candidate ↔ talent pool
  - id: junction_cp
    type: flatten
    input: pools_exploded
    select:
      candidate_id: candidate_id
      pool_id: pool_id

  - id: write_junction_cp
    type: write_csv
    input: junction_cp
    path: output/candidate_talent_pools.csv

  # Deduplicated talent pools → JSON
  - id: pools_dedup
    type: aggregate
    input: pools_exploded
    group_by: [pool_id]
    metrics:
      pool_name: first(pool_name)

  - id: write_pools
    type: write_json
    input: pools_dedup
    path: output/talent_pools.json
    pretty: true

  # Level 2 unnest: addresses
  - id: addresses_exploded
    type: unnest
    input: pools_exploded
    column: pool_addresses
    parent_fields:
      pool_id: pool_id
    fields:
      address_id: id
      city: city
      country: country

  - id: write_addresses
    type: write_csv
    input: addresses_exploded
    path: output/addresses.csv
```

**Key points:**
- `unnest` explodes JSON arrays into rows, carrying `parent_fields` for junction tables.
- Chain multiple `unnest` steps for multi-level normalization.
- Combine `aggregate` with `first()` to deduplicate exploded rows into dimension tables.
- See `examples/cli/unnest/` for the complete runnable versions.

## SFTP to S3 file transfer with transform

Read CSV from an SFTP server, filter and enrich, write JSON to S3.

```yaml
config:
  batch_size: 5000

connections:
  partner_sftp:
    driver: sftp
    host: sftp.partner.com
    auth:
      type: key
      username: etl
      private_key: "secret::vault/sftp/partner#private_key"
    base_path: /outbox

  data_lake:
    driver: s3
    bucket: company-data-lake
    region: eu-west-1
    auth:
      type: default_credentials
    base_path: raw/partner

environment:
  load_ts: now()

steps:
  - id: orders
    type: read_csv
    from:
      connection: partner_sftp
      path: daily_orders.csv
    delimiter: ";"

  - id: valid_orders
    type: filter
    input: orders
    condition: "status != \"cancelled\" and total > 0"

  - id: enriched
    type: map
    input: valid_orders
    columns:
      ingested_at: $load_ts
      source: '"partner_sftp"'

  - id: output
    type: write_json
    input: enriched
    target:
      connection: data_lake
      path: orders/latest.json
    pretty: false
```

**Key points:**
- `from.connection` / `target.connection` reference named file connections
- `path` in `from`/`target` is relative to the connection's `base_path`
- Secrets use `secret::vault/...` syntax — resolved at load time
- Same transform steps (filter, map) work regardless of source/sink transport

## CSV → transform → Parquet (data lake ingestion)

Convert raw CSV files to compressed Parquet for analytics:

```yaml
config:
  batch_size: 10000

connections:
  data_lake:
    driver: s3
    bucket: company-data-lake
    region: eu-west-1
    auth:
      type: default_credentials
    base_path: raw

steps:
  - id: events
    type: read_csv
    path: data/raw_events.csv

  - id: cleaned
    type: filter
    input: events
    condition: "status != \"invalid\""

  - id: enriched
    type: map
    input: cleaned
    columns:
      ingested_at: now()
      event_date: cast(created_at, "date32")

  - id: output
    type: write_parquet
    input: enriched
    target:
      connection: data_lake
      path: events/latest.parquet
    compression: zstd
```

**Key points:**
- Parquet preserves the Arrow schema exactly, including types and metadata
- `zstd` compression gives ~4x compression with fast decompression
- Column projection on `read_parquet` avoids reading unnecessary data

## SharePoint → Parquet → SMB (enterprise data pipeline)

Read Excel exports from SharePoint, transform, and write Parquet to a Windows file share:

```yaml
config:
  batch_size: 5000

connections:
  sharepoint:
    driver: sharepoint
    site_url: "https://company.sharepoint.com/sites/Finance"
    auth:
      type: client_credentials
      tenant_id: "${AZURE_TENANT_ID}"
      client_id: "${AZURE_CLIENT_ID}"
      client_secret: "${AZURE_CLIENT_SECRET}"
    base_path: /Shared Documents/Reports

  file_share:
    driver: smb
    host: fileserver.corp.local
    share: DataWarehouse
    auth:
      type: user_pass
      username: "CORP\\etl_svc"
      password: "${SMB_PASSWORD}"
    base_path: /staging/finance

steps:
  - id: reports
    type: read_csv
    from:
      connection: sharepoint
      path: monthly/revenue_report.csv

  - id: cleaned
    type: filter
    input: reports
    condition: "revenue > 0"

  - id: output
    type: write_parquet
    input: cleaned
    target:
      connection: file_share
      path: revenue/latest.parquet
    compression: zstd
```

## FTP → transform → GCS (legacy system ingestion)

Pull data from a legacy FTP server, transform, and store in Google Cloud Storage:

```yaml
connections:
  legacy_ftp:
    driver: ftp
    host: ftp.legacy-partner.com
    tls: true
    auth:
      type: user_pass
      username: etl_pull
      password: "secret::vault/ftp/legacy-partner#password"
    base_path: /outbox

  gcs_archive:
    driver: gcs
    bucket: company-archive
    auth:
      type: service_account
      credentials_file: "${GCP_SA_KEY_FILE}"
    base_path: legacy/partner

steps:
  - id: orders
    type: read_csv
    from:
      connection: legacy_ftp
      path: daily_export.csv
    delimiter: "|"

  - id: enriched
    type: map
    input: orders
    columns:
      ingested_at: now()
      source: '"legacy_ftp"'

  - id: archive
    type: write_parquet
    input: enriched
    target:
      connection: gcs_archive
      path: orders/daily.parquet
    compression: zstd
```

## Running any example

```sh
# Full run
potato_etl run --config pipeline.yaml

# Safe preview (no writes)
potato_etl dry-run --config pipeline.yaml --format pretty

# Check schema of a step
potato_etl schema --config pipeline.yaml --step enriched

# Validate without connecting
potato_etl validate --config pipeline.yaml
```