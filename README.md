# potato_etl

> WARNING: This project is in active development.
> Consider this version -v0.0.0.0.0.1-alpha^99
> Honestly. it's pretty darn awesome already.

## Quick Disclaimer

This project came from the idea of building ETLs through yaml/json files. 
Although it can do more then that, it's already doing a great job at this part.
Why yaml files? Because they're human-friendly, and they're easy to read and write. But not only that,
it allows the way open to making front ends. 100% open source.

**Arrow-native Rust ETL library — and Python wheel — powering [PotatoFlow](../potatoflow/).**

`potato_etl` is a DAG-based extract-transform-load library written in Rust.
Pipelines are defined in YAML or JSON, executed as a directed acyclic graph of
typed steps, and process data as [Apache Arrow](https://arrow.apache.org/)
`RecordBatch`es end-to-end.  A Python wheel (built with
[maturin](https://maturin.rs/) / [PyO3 0.28](https://pyo3.rs/)) exposes the full
API to Python 3.9+ with zero-copy `pyarrow` interop.

> **Dependency note:** The Python wheel requires **PyO3 0.28** and **Arrow 58**
> (with the `pyarrow` feature).  Arrow 58's pyarrow integration mandates
> PyO3 ≥ 0.28, which introduced several breaking API changes — see the
> [PyO3 0.28 migration notes](#pyo3-028-migration) below.

---

## Table of contents

- [Workspace layout](#workspace-layout)
- [Quick start](#quick-start)
  - [Rust](#rust) (completely untested, but it compiles)
  - [Python](#python) (completely untested, but it compiles)
  - [CLI](#cli)
- [Pipeline format](#pipeline-format)
  - [Connections](#connections)
  - [Steps reference](#steps-reference)
- [Expression DSL](#expression-dsl)
- [LogicalType middleware](#logicaltype-middleware)
- [Write modes & DDL](#write-modes--ddl)
- [Secret manager integration](#secret-manager-integration)
- [Enum values](#enum-values)
- [Unified DDL generation](#unified-ddl-generation)
- [SCD Type 2 sink](#scd-type-2-sink)
- [REST API source & sink](#rest-api-source--sink)
- [Python API](#python-api)
- [CLI reference](#cli-reference)
- [Feature flags](#feature-flags)
- [PyO3 0.28 migration](#pyo3-028-migration)
- [License](#license)

---

## Workspace layout

```
potato_etl/
├── common/        # potato-etl-common  — shared types, schema, config, DDL generation, transforms, utilities
├── drivers/       # potato-etl-driver-* — database driver crates (postgres, mssql, mysql, oracle, databricks)
├── runtime/       # potato-etl-runtime — DAG executor + unified ReadDB/WriteDB/Scd2Sink
├── cli/           # potato-etl-cli  — binary crate  (potato_etl command)
├── python/        # potato_etl      — Python wheel   (maturin / PyO3)
├── docs/          # Documentation (connections, steps, schema reference)
├── scripts/       # Utility scripts
└── examples/
    └── cli/       # Ready-to-run pipelines, one folder per source
        ├── postgres/      seed.sql + 5 pipelines (copy, upsert, SCD2, types, aggregate)
        ├── mssql/         seed.sql + copy + bcp bulk-load
        ├── oracle/        seed.sql + copy with prefetch tuning
        ├── mysql/         seed.sql + copy / upsert
        ├── rest_api/      GitHub issues, cursor pagination, AFAS → SCD2
        ├── databricks/    read Delta table → aggregate → write
        ├── multi_source/  Postgres employees ⟕ MSSQL departments → Postgres
        ├── unnest/        nested JSON → normalized tables (DB + file output)
        └── csv_json/      CSV ↔ JSON conversions, file-only pipelines
```

### Modular architecture

The workspace was refactored from a monolithic `core` crate into a layered
structure:

- **`common`** contains all shared types (`PipelineDoc`, `ConnParams`, `ColumnOption`),
  the unified DDL generator (`generate_ddl` / `generate_ddl_with_schema`), the
  4-tier type resolver, schema application, expression evaluator, secret manager
  integration, and all Arrow metadata constants.
- **`drivers/`** contains one crate per database backend. Each driver implements
  the `DbSource` and `DbSink` traits and is gated behind a Cargo feature flag.
  Drivers delegate DDL generation to `common`'s `generate_ddl_with_schema()`,
  ensuring consistent constraint support (PRIMARY KEY, UNIQUE, FOREIGN KEY,
  CHECK, DEFAULT, indexes, `enum_values`) across all five targets.
- **`runtime`** wires sources, transforms, and sinks into a DAG executor. It
  depends on `common` and conditionally on each driver crate.

---

## Quick start

### Rust

Add `potato-etl-runtime` to your `Cargo.toml` with the database features you need:

```toml
[dependencies]
potato-etl-runtime = { path = "potato_etl/runtime", features = ["postgres"] }
```

Run a pipeline from a YAML file:

```rust
use potato_etl_runtime::Dag;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let yaml = std::fs::read_to_string("pipeline.yaml")?;
    let report = Dag::from_yaml(&yaml)?.run().await?;
    println!("{} rows written in {:?}", report.rows_written, report.duration);
    Ok(())
}
```

Build the pipeline programmatically:

```rust
use potato_etl_runtime::{Dag, ETLConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut dag = Dag::new(ETLConfig { batch_size: 1000 });

    let src  = dag.add_source("src",  "postgresql://etl:pass@localhost/mydb", "orders", "id");
    let flt  = dag.add_filter("flt",  &src,  "status", Some("active"), None);
    let _out = dag.add_sink("out",    &flt,  "postgresql://etl:pass@localhost/mydb",
                            "orders_active", Default::default());

    let report = dag.run().await?;
    println!("{} rows written", report.rows_written);
    Ok(())
}
```

### Python

Install the wheel (requires Rust 1.85+, maturin, and PyO3 0.28-compatible Python ≥ 3.9):

```bash
pip install maturin pyarrow
cd potato_etl/python
maturin develop --features postgres    # or: maturin build --release
```

Load a YAML pipeline:

```python
import potato_etl as etl

pipeline = etl.ETL.from_yaml(open("pipeline.yaml").read())
pipeline.register_transform("my_func", my_python_fn)   # if the YAML uses `function:`
stats = pipeline.run()
print(stats)
```

Build a pipeline in Python:

```python
import potato_etl as etl

p = etl.ETL(batch_size=500)

src      = p.read_db("postgresql://etl:pass@localhost/mydb", table="employees", cursor="id")
active   = p.filter(src, "status", "active")
enriched = p.map(active, {"load_ts": "now()", "revenue": "price * quantity"})
p.write_db(enriched, "postgresql://etl:pass@localhost/analytics", table="employees_enriched",
           mode="truncate")

stats = p.run()
```

### CLI

No installation needed. Run the CLI directly from the workspace root with
`cargo run`.  Pass your feature flags before `--`, and the CLI subcommand +
arguments after it:

```bash
# From the potato_etl/ workspace root
cargo run -p potato-etl-cli --features postgres -- run        --config pipeline.yaml
cargo run -p potato-etl-cli --features postgres -- dry-run    --config pipeline.yaml --format pretty
cargo run -p potato-etl-cli --features postgres -- schema     --config pipeline.yaml --step add_bonus
cargo run -p potato-etl-cli --features postgres -- validate   --config pipeline.yaml
cargo run -p potato-etl-cli --features postgres -- list-steps --config pipeline.yaml
cargo run -p potato-etl-cli --features postgres -- explain    --config pipeline.yaml
```

`validate` and `explain` do not connect to any database, so they work without
any feature flag too:

```bash
cargo run -p potato-etl-cli -- validate --config pipeline.yaml
cargo run -p potato-etl-cli -- explain  --config pipeline.yaml
```

Combine feature flags as needed:

```bash
cargo run -p potato-etl-cli --features postgres,mssql -- run --config pipeline.yaml
```

> **Tip:** Cargo caches the build, so the second invocation is as fast as a
> normal binary.  Use `--release` if you're running large pipelines and want the
> optimised build:
> ```bash
> cargo run -p potato-etl-cli --release --features postgres -- run --config pipeline.yaml
> ```

---

## Pipeline format

Pipelines are defined in **YAML** (human-friendly, supports multi-line Python
code blocks) or **JSON** (canonical storage format).  Both formats parse to the
same internal model; `Dag::yaml_to_json` converts losslessly.

```yaml
config:
  batch_size: 500          # rows per Arrow RecordBatch
  log_level: info          # error | warn | info | debug | trace

connections:
  pg:
    driver: postgres
    host: localhost
    database: mydb
    auth:
      type: user_pass
      username: etl_user
      password: "s3cr3t"

steps:
  - id: src
    type: read_db
    from:
      connection: pg
      table: employees
      cursor: id

  - id: active
    type: filter
    input: src
    column: status
    value: active

  - id: out
    type: write_db
    input: active
    target:
      connection: pg
      table: employees_active
    mode: upsert
    schema:
      database:
        columns:
          id:
            primary_key: true
```

### Connections

Define named connections in the top-level `connections:` map; reference them by
name in `from.connection` (database sources), `target.connection` (database sinks),
or `conn:` (REST API steps).  Direct connection URLs are also accepted.

| Driver key | Aliases | Feature flag |
|---|---|---|
| `postgres` | `postgresql` | `postgres` |
| `mssql` | — | `mssql` |
| `oracle` | — | `oracle` |
| `mysql` | `aurora`, `mariadb` | `mysql` |
| `databricks` | — | `databricks` |
| `rest_api` | — | _(always available)_ |

**File transport connections** — used by `read_csv`, `read_json`, `read_parquet`, `write_csv`, `write_json`, `write_parquet`:

| Driver key | Aliases | Auth types |
|---|---|---|
| `local` | — | `none` |
| `sftp` | — | `user_pass`, `key` |
| `s3` | — | `access_key`, `role_arn`, `default_credentials` |
| `azure_blob` | — | `client_credentials`, `connection_string`, `sas_token`, `default_credentials` |
| `gcs` | — | `service_account`, `default_credentials` |
| `sharepoint` | `sharepoint_online` | `client_credentials` |
| `ftp` | `ftps` | `user_pass`, `none` |
| `smb` | `cifs` | `user_pass` |

File steps reference connections via `from.connection` (sources) or `target.connection` (sinks):

```yaml
connections:
  reports_sftp:
    driver: sftp
    host: sftp.example.com
    auth:
      type: key
      username: deploy
      private_key: "${SFTP_KEY}"
    base_path: /uploads/reports

steps:
  - id: data
    type: read_csv
    from:
      connection: reports_sftp
      path: incoming/latest.csv       # relative to base_path

  - id: output
    type: write_json
    input: data
    target:
      connection: reports_sftp
      path: processed/result.json
    pretty: true
```

**Named connection fields (database drivers)**

| Field | Required | Notes |
|---|---|---|
| `driver` | yes | See table above |
| `host` | yes | Hostname or IP |
| `port` | no | Driver default if absent |
| `database` | yes | Database / catalog name |
| `username` | yes | — |
| `password` | yes | Any characters — percent-encoded automatically |
| `schema` | no | Default schema (MSSQL, Oracle) |
| `service` | Oracle | Service name (preferred over `sid` for 12c+) |
| `sid` | Oracle | Legacy SID |
| `tns` | Oracle | TNS alias or full descriptor |
| `http_path` | Databricks | SQL warehouse HTTP path |
| `token` | Databricks | Personal access token |
| `options` | no | Driver-specific key/value pairs (see examples) |

**Named connection fields (REST API driver)**

| Field | Notes |
|---|---|
| `base_url` | Prepended to step `url:` values that start with `/` |
| `auth` | `type: bearer \| api_key \| basic` — see examples |
| `headers` | Default request headers (step headers merged on top) |
| `timeout_secs` | Per-request timeout |
| `rate_limit_rps` | Max requests per second |

---

### Steps reference

#### `read_db` — database source

```yaml
- id: src
  type: read_db
  from:
    connection: pg              # named connection or full URL
    table: employees            # mutually exclusive with query:
    query: "SELECT ..."         # raw SQL — use for joins, filters, custom ordering
    cursor: id                  # cursor column for batched pagination
    schema: dbo                 # SQL schema / namespace (MSSQL / Oracle)
  normalize_columns: true     # lowercase column names (Oracle UPPERCASE)
  schema:                     # unified schema block
    arrow:
      columns:
        salary:
          type: float64
        is_active:
          type: boolean
  options: {}                 # driver-specific overrides (see pipeline_example.yaml)
```

#### `rest_api` — REST API source

```yaml
- id: issues
  type: rest_api
  conn: github_api            # optional — provides base_url + auth
  url: /repos/acme/app/issues # appended to base_url, or full URL
  data_path: null             # JSON path to the records array (null = top-level)
  dedup_key: id               # drop duplicate records (offset pagination drift)
  pagination:
    strategy: link_header     # link_header | cursor | offset | page | none
    cursor_path: "meta.next_cursor"
    cursor_param: cursor
    page_size: 100
```

Pagination strategies:

| Strategy | Description |
|---|---|
| `link_header` | RFC 5988 `Link: <url>; rel="next"` (GitHub, GitLab) |
| `cursor` | Opaque cursor token in response body |
| `offset` | Numeric `offset` + `limit` parameters |
| `page` | Page number + page size parameters |
| `none` | Single request, no pagination |

#### `read_json` — JSON file source

```yaml
- id: candidates
  type: read_json
  path: data/candidates.json       # supports glob patterns (e.g. data/*.json)
  data_path: candidates            # optional: dot-notation path to the array
  sort_glob: name                  # optional: name (default) or name_desc
```

#### `read_parquet` — Parquet file source

```yaml
- id: events
  type: read_parquet
  path: data/events.parquet        # supports glob patterns (e.g. lake/*.parquet)
  columns: [event_id, user_id]     # optional: column projection
  batch_size: 8192                 # optional (default: config.batch_size)
  sort_glob: name                  # optional: name (default) or name_desc
```

#### `read_csv` — CSV file source

```yaml
- id: employees
  type: read_csv
  path: data/employees.csv         # supports glob patterns (e.g. data/*.csv)
  delimiter: ","                   # optional (default: ",")
  has_header: true                 # optional (default: true)
  sort_glob: name                  # optional: name (default) or name_desc
```

#### `filter` — row filter

```yaml
- id: active
  type: filter
  input: src
  column: status              # simple equality: column == value
  value: active
  # — or —
  condition: "status == \"active\" and salary >= 3000"   # expression DSL
```

#### `map` — add / compute / rename columns

```yaml
- id: enrich
  type: map
  input: src
  columns:
    load_ts:    now()
    revenue:    price * quantity
    order_year: year(order_date)
    uid:        json_get(payload, "user.id")
  select_only: false          # true = emit ONLY the listed columns
```

#### `aggregate` — group-by + metrics

```yaml
- id: summary
  type: aggregate
  input: enrich
  group_by: [customer_id, region]
  metrics:
    total_sales:  sum(revenue)
    order_count:  count()
    avg_revenue:  avg(revenue)
    first_seen:   min(order_date)
```

Supported aggregate functions: `sum`, `count`, `min`, `max`, `avg` / `mean`, `first`.

#### `rename` — rename columns

```yaml
- id: canonical
  type: rename
  input: src
  columns:
    emp_id:   id
    emp_name: full_name
```

DDL hints (`column_options`, `schema.database.columns`) live on the sink step, not the rename step.

#### `flatten` — extract struct / JSON sub-fields

```yaml
- id: flat
  type: flatten
  input: api_src
  select:                     # omit to auto-expand all struct columns one level
    contractor_id: id
    city:          address.city
    tag_0:         tags[0]
```

#### `join` — hash join

```yaml
- id: joined
  type: join
  left: employees
  right: departments          # right side fully buffered — keep it smaller
  on: department_id
  how: inner                  # inner | left | full
```

#### `unnest` — explode array columns

```yaml
- id: exploded
  type: unnest
  input: raw
  column: talent_pools           # JSON array column to explode
  parent_fields:                 # carry parent columns into each row
    candidate_id: id
  fields:                        # extract sub-fields from each element
    pool_id: id
    pool_name: name
```

#### `python_transform` — Python code per batch

```yaml
- id: add_bonus
  type: python_transform
  input: src
  # Inline code — 'table' (pyarrow.Table) and 'pa' (pyarrow) are in scope.
  # Assign the output to 'result'.
  code: |
    import pyarrow.compute as pc
    bonus = pc.multiply(table.column("salary"), 0.10)
    result = table.append_column(pa.field("bonus", pa.float64()),
                                 bonus.cast(pa.float64()))
  # — or named function (must call register_transform() before run()) —
  function: validate_data
```

#### `write_db` — database sink

```yaml
- id: out
  type: write_db
  input: enrich
  target:
    connection: pg
    table: employees_enriched
    schema: etl               # target schema (MSSQL / Oracle)
  mode: upsert                # append | insert_ignore | upsert | merge_delete | truncate
  create_table: if_not_exists # never | if_not_exists | replace
  batch_size: 10000           # per-sink sub-chunk size (independent of global batch_size)
  schema:
    database:
      columns:
        id:
          primary_key: true     # MERGE key for upsert / merge_delete
  options:
    mode: bcp                 # MSSQL: use bcp CLI for high-throughput bulk loads
```

#### `scd2_sink` — slowly-changing dimension Type 2

```yaml
- id: history
  type: scd2_sink
  input: active
  target:
    connection: pg
    table: employees_history
  key: id                     # natural key
  track: [salary, department] # omit to track ALL non-key columns
  close_missing: false        # true = expire keys absent from incoming data
  scd2_columns:               # override system column names
    valid_from: eff_from
    valid_to:   eff_to
    is_current: is_latest
  create_table: if_not_exists
```

#### `rest_api_sink` — write to REST API

```yaml
- id: push
  type: rest_api_sink
  input: enrich
  conn: downstream_api
  url: /employees/{id}        # {column_name} placeholders per row
  method: PUT                 # GET | POST | PUT | PATCH | DELETE
  mode: per_row               # per_row | batch
  wrap_key: employees         # batch mode: wrap array under this key
  rate_limit_rps: 5.0
  field_map:                  # build nested JSON via dot-notation paths
    id:        "KnEmployee.Element.Fields.EmId"
    full_name: "KnEmployee.Element.Fields.FiNm"
```

#### `write_csv` — CSV file sink

```yaml
- id: output
  type: write_csv
  input: processed
  path: output/result.csv
  delimiter: ","                 # optional (default: ",")
  has_header: true               # optional (default: true)
```

#### `write_json` — JSON file sink

```yaml
- id: output
  type: write_json
  input: processed
  path: output/result.json
  pretty: true                   # optional (default: false)
  wrap_key: employees            # optional: wrap in {"employees": [...]}
```

#### `write_parquet` — Parquet file sink

```yaml
- id: output
  type: write_parquet
  input: processed
  path: output/result.parquet
  compression: zstd              # optional: none, snappy, gzip, lz4, zstd (default: snappy)
```

---

## Expression DSL

The `filter` (`condition:`), `map` (`columns:` values), and `aggregate`
(`metrics:` values) steps share a lightweight expression language.

### Operators

| Category | Operators |
|---|---|
| Arithmetic | `+` `-` `*` `/` |
| Comparison | `==` `!=` `<` `<=` `>` `>=` |
| Logical | `and` `or` `not` |
| Grouping | `(` `)` |

Null propagation follows **SQL three-valued logic** for `and` / `or`:
`true AND null → null`, `false AND null → false`,
`true OR null → true`, `false OR null → null`.

### Built-in functions

**Temporal**

| Function | Returns | Description |
|---|---|---|
| `now()` | `TimestampMicrosecond[UTC]` | Current UTC timestamp |
| `now_naive()` | `TimestampMicrosecond` | Current UTC timestamp without timezone info |
| `run_ts()` | `TimestampMicrosecond[UTC]` | Pipeline start time -- same across all batches in a run |
| `run_ts_naive()` | `TimestampMicrosecond` | Pipeline start time without timezone info |
| `year(col)` | `Int32` | Calendar year |
| `month(col)` | `Int32` | Month (1–12) |
| `day(col)` | `Int32` | Day of month (1–31) |
| `hour(col)` | `Int32` | Hour (0–23) |
| `minute(col)` | `Int32` | Minute (0–59) |
| `second(col)` | `Int32` | Second (0–59) |
| `epoch_to_timestamp(col)` | `TimestampMicrosecond[UTC]` | Convert unix epoch integer (seconds) to UTC timestamp |
| `epoch_to_timestamp(col, "ms")` | `TimestampMicrosecond[UTC]` | Convert unix epoch integer with explicit unit: `"s"`, `"ms"`, `"us"`, `"ns"` |

**String**

| Function | Aliases | Description |
|---|---|---|
| `upper(col)` | `ucase` | Uppercase |
| `lower(col)` | `lcase` | Lowercase |
| `trim(col)` | — | Strip leading/trailing whitespace |
| `length(col)` | `len`, `char_length` | Unicode character count → `Int32` |
| `concat(a, b, …)` | — | Concatenate strings |
| `coalesce(a, b, …)` | — | First non-null value |

**JSON**

| Function | Description |
|---|---|
| `json_get(col, "key.path[0]")` | Extract a scalar from a JSON string column via dot/bracket path |
| `json_length(col)` | Length of a JSON array column → `Int32` |

**Type casting**

| Function | Description |
|---|---|
| `cast(col, "float64")` | Cast column to any Arrow primitive type |

**Null helpers**

| Function | Aliases | Description |
|---|---|---|
| `is_null(col)` | `isnull` | True if value is null |
| `is_not_null(col)` | `isnotnull`, `not_null` | True if value is not null |
| `if_null(col, default)` | `ifnull`, `nvl` | Replace null with default |

**Aggregate (only in `aggregate` step metrics)**

`sum` · `count` · `min` · `max` · `avg` / `mean` · `first`

---

## LogicalType middleware

`LogicalType` is a cross-dialect semantic layer that maps source-specific column
types to a portable intent, then generates the correct DDL for the target
database.

| Variant | Arrow storage | Example source types | Postgres DDL | MSSQL DDL |
|---|---|---|---|---|
| `json` | `Utf8` | `jsonb`, `JSON` | `JSONB` | `NVARCHAR(MAX)` |
| `currency` | `Float64` | `money`, `smallmoney` | `NUMERIC(19,4)` | `MONEY` |
| `uuid` | `Utf8` | `uuid`, `uniqueidentifier` | `UUID` | `UNIQUEIDENTIFIER` |
| `xml` | `Utf8` | `xml`, `XMLTYPE` | `XML` | `XML` |
| `ip` | `Utf8` | `inet`, `cidr` | `INET` | `NVARCHAR(45)` |
| `macaddr` | `Utf8` | `macaddr`, `macaddr8` | `MACADDR` | `NVARCHAR(23)` |

`LogicalType` is set automatically by source drivers (e.g. Postgres sets `json`,
`uuid`, `ip`, etc.).  You can also set it explicitly via `schema.arrow.columns`:

```yaml
schema:
  arrow:
    columns:
      payload:
        logical_type: json       # semantic hint for DDL resolver
      employee_id:
        logical_type: uuid
  database:
    columns:
      employee_id:
        nullable: false
        primary_key: true
      payload:
        type: JSONB              # explicit SQL type override (highest priority)
```

Resolver priority (highest → lowest):

1. `schema.database.columns.<col>.type` / `column_options.<col>.db_type` — explicit SQL type override
2. `logical_type` (`etl.logical_type` Arrow metadata) — semantic enum
3. `source_db_type` — raw cross-dialect mapping
4. Arrow `DataType` fallback

> **Note:** The standalone `type_override` field has been **removed**. Use
> `schema.database.columns.<col>.type` (or legacy `column_options.<col>.db_type`)
> instead.

---

## Write modes & DDL

| Mode | Behaviour |
|---|---|
| `append` | `INSERT` rows; fails on PK conflict |
| `insert_ignore` | `INSERT … ON CONFLICT DO NOTHING` / `INSERT IGNORE` |
| `upsert` | `INSERT … ON CONFLICT DO UPDATE` / `MERGE`; key derived from `primary_key: true` columns |
| `merge_delete` | Upsert + delete rows in target that are absent from source |
| `truncate` | `TRUNCATE` then bulk-load |

`create_table`:

| Value | Behaviour |
|---|---|
| `never` _(default)_ | Error if table does not exist |
| `if_not_exists` | `CREATE TABLE IF NOT EXISTS` from the first batch's Arrow schema |
| `replace` | `DROP TABLE` then recreate |

---

## Secret manager integration

Secrets can be fetched from external secret managers at pipeline load time.
Use the `secret::` prefix in any string value in your YAML/JSON config — the
resolver walks the raw `Value` tree *before* deserialization, so existing
types (`ConnParams`, `DbAuth`, etc.) require no changes.

| Provider              | Prefix          | Aliases                       | Auth chain                                  |
|-----------------------|-----------------|-------------------------------|---------------------------------------------|
| HashiCorp Vault KV v2 | `secret::vault` | `secret::hashicorp`           | Token -> AppRole -> Kubernetes SA           |
| Azure Key Vault       | `secret::azure` | `secret::akv`                 | SPN -> Managed Identity -> Azure CLI        |
| Google Secret Manager | `secret::gcp`   | `secret::gsm`, `secret::google` | Service Account key -> GCE metadata server |

**Reference format:** `secret::<provider>/<path>[#field]`

```yaml
connections:
  pg_prod:
    driver: postgres
    host: prod-db.internal
    database: analytics
    auth:
      type: user_pass
      username: "secret::vault/prod/pg#username"
      password: "secret::vault/prod/pg#password"
```

Secret values are auto-detected as JSON, YAML, or plain string (tried in that
order).  Structured secrets support `#field` extraction:

```yaml
# Secret stored as JSON in Azure Key Vault:
#   {"host": "db.internal", "port": 5432, "password": "hunter2"}

connections:
  pg:
    host: "secret::azure/my-vault/db-config#host"
    port: "secret::azure/my-vault/db-config#port"
    auth:
      type: user_pass
      username: etl_user
      password: "secret::azure/my-vault/db-config#password"
```

Whole-connection resolution is also supported — store the full connection
object as a JSON secret:

```yaml
connections:
  pg_staging: "secret::vault/staging/pg_connection"
```

See [docs/secrets.md](docs/secrets.md) for provider-specific auth configuration
and environment variables.

---

## Enum values

Columns can specify a list of allowed values via `enum_values`. The DDL
generator creates the dialect-appropriate constraint automatically:

- **Postgres**: `CREATE TYPE "<col>_enum" AS ENUM (...)` (idempotent with `DO $$ ... EXCEPTION $$`)
- **MySQL**: inline `ENUM('draft', 'active', ...)` column type
- **MSSQL / Oracle / Databricks**: `ALTER TABLE ... ADD CHECK (<col> IN (...))` constraint

```yaml
schema:
  database:
    columns:
      status:
        enum_values: [draft, active, archived, deleted]
      priority:
        enum_values: [low, medium, high, critical]
```

The legacy `column_options` flat form also supports `enum_values`:

```yaml
column_options:
  status:
    enum_values: [draft, active, archived, deleted]
```

---

## Unified DDL generation

All five database drivers (Postgres, MSSQL, Oracle, MySQL, Databricks) now
delegate DDL generation to the shared `generate_ddl_with_schema()` function
in `potato-etl-common`.  This means **every** DDL feature works consistently
across all targets:

| Feature | Postgres | MSSQL | Oracle | MySQL | Databricks |
|---------|----------|-------|--------|-------|------------|
| PRIMARY KEY | inline | inline | inline | inline | inline |
| UNIQUE | inline | inline | inline | inline | inline |
| FOREIGN KEY | inline | inline | inline | inline | inline |
| CHECK constraints | inline | inline | inline | inline | inline |
| DEFAULT expressions | inline (auto-normalised) | inline (auto-normalised) | inline (auto-normalised) | inline (auto-normalised) | inline |
| ON UPDATE triggers | function + trigger | trigger | trigger | inline `ON UPDATE` | -- |
| Named indexes | `CREATE INDEX IF NOT EXISTS` | `CREATE INDEX` | `CREATE INDEX` | `CREATE INDEX` | `CREATE INDEX IF NOT EXISTS` |
| Named constraints | `ALTER TABLE ... ADD CONSTRAINT` | same | same | same | same |
| `enum_values` | `CREATE TYPE ... AS ENUM` | `CHECK` constraint | `CHECK` constraint | inline `ENUM(...)` | `CHECK` constraint |
| `CREATE TABLE IF NOT EXISTS` | native | `IF NOT EXISTS` wrapper | PL/SQL `EXECUTE IMMEDIATE` wrapper | native | native |
| Column comments | `COMMENT ON COLUMN` | -- | `COMMENT ON COLUMN` | -- | -- |

### `DdlOptions`

Drivers pass a `DdlOptions` struct to influence version-aware DDL generation:

```rust
DdlOptions {
    pg_major_version: Some(16),   // Postgres major version
    ora_major_version: Some(21),  // Oracle major version
}
```

| Option | Effect |
|--------|--------|
| `pg_major_version` | Reserved for future Postgres version-specific DDL |
| `ora_major_version >= 21` | Uses native `JSON` column type instead of `CLOB` |
| `ora_major_version >= 23` | Uses native `BOOLEAN` type instead of `NUMBER(1)` |

### Oracle smart identifier quoting

Oracle identifiers use "smart quoting" — simple identifiers (letters, digits,
`_`, `#`, `$`) that are **not** reserved words are emitted **unquoted** so
Oracle automatically uppercases them (standard convention).  Complex or reserved
identifiers are double-quoted to preserve exact spelling.  All other dialects
always quote identifiers.

---

## SCD Type 2 sink

The `scd2_sink` maintains a full version history of each entity:

- A new row is inserted whenever a **tracked column** changes.
- The previous row is expired: `valid_to = now()`, `is_current = false`.
- The `key` column identifies the same entity across versions.
- When `close_missing: true`, keys present in the database but absent from **all**
  incoming batches are expired at flush time. Use this for full-snapshot ingestion
  (e.g. an API that returns all records). Leave `false` (the default) for
  delta / cherry-pick ingestion where absence means "not fetched", not "deleted".

System columns (`valid_from`, `valid_to`, `is_current`, `scd_id`) can be renamed
via `scd2_columns:` to match an existing schema.

---

## REST API source & sink

### Source pagination strategies

| Strategy | Trigger to advance | Best for |
|---|---|---|
| `link_header` | RFC 5988 `Link: <url>; rel="next"` header | GitHub, GitLab, Jira |
| `cursor` | Opaque token in response body | Stripe, Salesforce |
| `offset` | Numeric offset + limit | Legacy APIs (drift-prone — use `dedup_key`) |
| `page` | Page number + page size | Simple paginated APIs |
| `none` | Single request | Small datasets |

### Sink modes

| Mode | Behaviour |
|---|---|
| `per_row` | One request per row; `{column}` placeholders in `url:` |
| `batch` | One request per Arrow batch; body is a JSON array (optionally wrapped under `wrap_key`) |

Auth types for both source and sink: `bearer`, `api_key` (custom header),
`basic` (username + password).

---

## Python API

```python
import potato_etl as etl

# ── Constructors ──────────────────────────────────────────────────────────────
p = etl.ETL(batch_size=1000)
p = etl.ETL.from_json(json_string)
p = etl.ETL.from_yaml(yaml_string)
p = etl.ETL.from_yaml_with_secrets(yaml_string)  # resolves secret:: refs
json_str  = etl.ETL.yaml_to_json(yaml_string)   # lossless YAML → JSON
codes     = p.inline_python_codes()              # dict of inline code blocks

# ── Sources ───────────────────────────────────────────────────────────────────
src  = p.read_db(conn_str, table="orders", cursor="id",
                 arrow_overrides={"amount": "float64"})
api  = p.read_api(url, data_path="data",
                  pagination="cursor", cursor_path="meta.next")

# ── Transforms ────────────────────────────────────────────────────────────────
flt  = p.filter(src, "status", "active")                    # simple equality
flt2 = p.filter(src, condition="amount > 100 and status == \"active\"")
mp   = p.map(src, {"ts": "now()", "rev": "price * qty"}, select_only=False)
agg  = p.aggregate(src, group_by=["region"], metrics={"n": "count()"})
ren  = p.rename(src, {"emp_id": "id", "emp_name": "full_name"})
low  = p.lowercase_columns(src)                              # lowercase all column names
flat = p.flatten(src, select={"city": "address.city"})
jn   = p.join(left, right, on="customer_id", how="inner")
py   = p.python_transform(src, my_fn)
blk  = p.build_objects(src, field_map={"id": "root.id", "name": "root.name"})

# ── Sinks ────────────────────────────────────────────────────────────────────
p.write_db(src, conn_str, table="out", mode="upsert",
           column_options={"id": {"primary_key": True}})
p.scd2_sink(src, conn_str, table="history", key="id", track=["salary"],
            close_missing=True)
p.write_api(src, url="/employees/{id}", method="PUT", mode="per_row")

# ── Named Python transforms ───────────────────────────────────────────────────
p.register_transform("validate", my_validate_fn)

# ── Environment variables ────────────────────────────────────────────────────
p.set_env("load_ts", "now()")                    # evaluated once at run start
p.set_env("label", '"nightly"')                  # reference in map: "$load_ts"

# ── Run ───────────────────────────────────────────────────────────────────────
stats = p.run()
# stats is a dict: {"rows_written": int, "duration_ms": int,
#                   "components": {"step_id": {"rows_in": int, "rows_out": int, ...}}}

# ── Fluent chaining (>> operator) ────────────────────────────────────────────
result = src >> flt >> mp      # equivalent to connecting steps in sequence
```

Inline Python code in `python_transform` receives two globals:

- `table` — `pyarrow.Table` containing the current batch
- `pa` — the `pyarrow` module

Assign the transformed result to `result` (a `pyarrow.Table` or `RecordBatch`).

---

## CLI reference

The CLI is run via `cargo run` from the workspace root (see [Quick start →
CLI](#cli) above).  The `--` separator divides Cargo flags from CLI arguments:

```
cargo run -p potato-etl-cli --features <FLAGS> -- <COMMAND> [OPTIONS]
```

| Command | Description |
|---|---|
| `run` | Execute the full pipeline — all sources, transforms, and sinks |
| `dry-run` | Read N batches per source; all sinks are no-ops (safe preview) |
| `schema` | Print the Arrow schema for a named step (requires a live connection) |
| `validate` | Check YAML/JSON structure and DAG wiring; no DB connection needed |
| `list-steps` | Print all step IDs in topological execution order |
| `step-info` | Show full details for a single step |
| `explain` | Describe every step statically; no DB connection needed |

**Common flags**

```
-c, --config <FILE>      Pipeline definition (.yaml or .json)
    --batch-size <N>     Override global batch_size from config
    --step <STEP_ID>     Step ID (schema, step-info)
    --batches <N>        Number of batches to read in dry-run (default: 1)
    --format <FORMAT>    pretty | table | json (command-specific default)
-v, --verbose            Enable debug logging
    --trace              Enable trace logging (very noisy)
```

**Output formats**

| Format | Best for |
|---|---|
| `pretty` | Human reading — aligned text with colours |
| `table` | Compact ASCII grid |
| `json` | Machine consumption / CI pipelines |

**Exit codes**: `0` success · `1` error (message printed to stderr).

---

## Feature flags

Enable database backends at compile time by passing `--features` to `cargo run`
(or `cargo build`):

```bash
# Run with one backend
cargo run -p potato-etl-cli --features postgres -- run --config pipeline.yaml

# Run with multiple backends
cargo run -p potato-etl-cli --features postgres,mssql,mysql -- run --config pipeline.yaml

# Python wheel (still requires maturin)
maturin build --release --features postgres,mssql,mysql
```

| Flag | Backend | System deps |
|---|---|---|
| `postgres` | PostgreSQL (sqlx) | None — pure Rust |
| `mysql` | MySQL / Aurora / MariaDB (sqlx) | None — pure Rust |
| `mssql` | SQL Server (tiberius, TDS protocol) | None — pure Rust |
| `oracle` | Oracle (rust-oracle, OCI) | Oracle Instant Client |
| `databricks` | Databricks SQL (HTTP API) | None — uses reqwest |

**File transport features:**

| Flag | Transport | System deps |
|---|---|---|
| `transport-cloud` | S3, Azure Blob, GCS | None |
| `transport-sftp` | SFTP | None |
| `transport-ftp` | FTP / FTPS | None |
| `transport-sharepoint` | SharePoint Online | None |
| `transport-smb` | SMB / CIFS | `libsmbclient-dev` |
| `transport-all` | All of the above | All of the above |
| `all` | Every database driver + every transport | All of the above |

No flag is enabled by default; add only what you need.
Local filesystem file I/O is always available without any feature flag.

See [docs/feature-flags.md](docs/feature-flags.md) for detailed architecture,
system dependency install commands, and compile-time impact.

---

## PyO3 0.28 migration

Arrow 58's `pyarrow` feature requires PyO3 ≥ 0.28.  PyO3 0.28 contains several
breaking changes relative to 0.22–0.23.  This section documents the changes
applied to the `python/` crate and serves as a reference for future
contributors.

### Summary of changes

| Area | Before (PyO3 ≤ 0.23) | After (PyO3 0.28) |
|---|---|---|
| `PyObject` type alias | Provided by PyO3 | Define locally: `type PyObject = Py<PyAny>;` |
| `py.allow_threads(f)` | Built-in method | Removed — use custom `allow_threads()` helper via `PyEval_SaveThread` / `PyEval_RestoreThread` FFI. The closure no longer requires `Send` (it runs on the same thread; the GIL is simply released). |
| `Python::with_gil(f)` | Built-in function | Removed for extension modules — use `Python::try_attach(f)` via custom `with_gil()` helper |
| `to_pyarrow(py)` return type | `PyResult<PyObject>` (`Py<PyAny>`) | `PyResult<Bound<'py, PyAny>>` — append `.into()` to convert back to `Py<PyAny>` where needed |
| `Py::bind(py)` | Converts `Py<T>` → `&Bound<'py, T>` | Removed when the value is already `Bound` — use the `Bound` value directly |
| `Bound::downcast::<T>()` | Cast to a concrete Python type | Deprecated — use `Bound::cast::<T>()` |
| `Py::extract::<T>()` | No `py` argument | Now requires `py`: `.extract::<T>(py)` |
| `Py::call1(py, args)` return type | `PyResult<PyObject>` | `PyResult<Bound<'py, PyAny>>` |
| `py.run(code, ...)` | Accepts `&str` | Accepts `&CStr` — convert with `CString::new(code)` first |
| `PyList::new(py, items)` | Infallible | Fallible — returns `PyResult`, use `?` |
| `#[pyclass]` thread safety | Implicit | Requires `#[pyclass(unsendable)]` for types containing `!Send` fields (e.g. `std::sync::mpsc::Receiver`) |
| `__next__` return type | `Option<PyObject>` | `PyResult<Py<PyAny>>` — raise `PyStopIteration` instead of returning `None` |

### Helper functions

Two small FFI helpers replace removed PyO3 APIs.  They live at the top of
`python/src/lib.rs`:

```rust
/// Release the GIL, run `f`, re-acquire the GIL.
/// The closure must NOT touch Python objects.
fn allow_threads<F, R>(_py: Python<'_>, f: F) -> R
where
    F: FnOnce() -> R,
{
    unsafe {
        let save = pyo3::ffi::PyEval_SaveThread();
        let result = f();
        pyo3::ffi::PyEval_RestoreThread(save);
        result
    }
}

/// Acquire the GIL (extension-module variant).
fn with_gil<F, R>(f: F) -> R
where
    F: for<'py> FnOnce(Python<'py>) -> R,
{
    Python::try_attach(f).expect("Python runtime must be active")
}
```

### `allow_threads` — relaxed bounds

The original `allow_threads` helper had `F: Send + FnOnce() -> R` bounds.  This
is too strict: `std::sync::mpsc::Receiver` is `!Sync`, which means `&Receiver`
is `!Send`, so a closure capturing `&self` (where `self` contains a `Receiver`)
would fail to compile.  Since the closure runs **on the same OS thread** (we
only release the GIL, we don't move the closure), `Send` is unnecessary.  The
bound was relaxed to `F: FnOnce() -> R`.

---

## License

Licensed under either of

- **Apache License, Version 2.0** ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- **MIT license** ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project by you, as defined in the Apache-2.0 license,
shall be dual-licensed as above, without any additional terms or conditions.