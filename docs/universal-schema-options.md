# Universal Schema Options Reference

This document describes **all universal options** available across potato_etl components, with a focus on schema-related configuration.

---

## Table of Contents

1. [Global Configuration](#global-configuration)
2. [Source-Side Schema Options (Universal for all Read Steps)](#source-side-schema-options-universal-for-all-read-steps)
3. [Sink-Side Schema Options (Universal for all Write Steps)](#sink-side-schema-options-universal-for-all-write-steps)
4. [Per-Step Driver Options](#per-step-driver-options)
5. [Schema Block (Unified Schema Configuration)](#schema-block-unified-schema-configuration)
6. [Write Modes](#write-modes)
7. [Create Table Modes](#create-table-modes)
8. [Logical Types](#logical-types)
9. [Complete Example](#complete-example)

---

## Global Configuration

Applied at the **top level** of your pipeline YAML. Affects all steps unless overridden per-step.

```yaml
config:
  batch_size: 1000              # Default number of rows per RecordBatch
  channel_capacity: 4           # Max RecordBatches buffered between components
  log_level: debug              # error | warn | info | debug | trace
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batch_size` | `usize` | `1000` | Number of rows per batch during pagination. Controls read throughput and memory usage. |
| `channel_capacity` | `usize` | `4` | Maximum `RecordBatch`es buffered in each inter-component channel. Higher values hide latency spikes between stages; memory per edge ≈ `channel_capacity × batch_size × avg_row_bytes`. |
| `log_level` | `enum` | `info` | Logging verbosity. At `debug`, every step logs input/output schema and first 10 rows. |

---

## Source-Side Schema Options (Universal for all Read Steps)

These options are available on **all source steps** (`read_db`, `rest_api`, `read_json`, `read_csv`, `read_parquet`). They are applied **immediately after reading**, before any transforms.

### Available on: `read_db`, `rest_api`, `read_json`, `read_csv`, `read_parquet`

```yaml
- id: source
  type: read_db
  from:
    connection: pg
    table: employees
  
  # -- Source Schema Options (universal) --------------------------------------
  
  arrow_overrides:
    employee_id: utf8            # Arrow type cast: INT64 -> Utf8
    salary: float64              # Arrow type cast: DECIMAL -> Float64
  
  normalize_columns: true      # Lowercase all column names (auto-detects for Oracle)
  
  exclude:                     # Drop columns immediately after reading
    - large_blob_data
    - internal_field
  
  batch_size: 5000             # Per-step override of global batch_size (reads only)
```

### `arrow_overrides`

**Type:** `HashMap<String, String>`  
**Default:** `{}`

Per-column Arrow type overrides. Maps column names to Arrow type strings. Uses `arrow::compute::cast()` internally.

On **sources**, applied immediately after reading, before any transforms.  
On **sinks**, applied at the sink boundary before value mapping and DDL generation.

```yaml
arrow_overrides:
  employee_id: utf8           # INT64 -> Utf8
  salary: float64             # DECIMAL -> Float64
  col_date: date32            # force Date32 (days since epoch)
  col_time: time64[us]        # force Time64 microseconds
  col_timestamp: timestamp[us]      # force Timestamp microseconds, no timezone
  col_timestamptz: timestamp[us, UTC]  # force Timestamp microseconds, UTC
```

**Supported Arrow type strings:**
- Integers: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
- Floats: `float16`, `float32`, `float64`
- Strings: `utf8`, `large_utf8`
- Binary: `binary`, `large_binary`
- Temporal: `date32`, `date64`, `time32[s]`, `time32[ms]`, `time64[us]`, `time64[ns]`
- Timestamps: `timestamp[s]`, `timestamp[ms]`, `timestamp[us]`, `timestamp[ns]`
- With timezone: `timestamp[us, UTC]`, `timestamp[ms, America/New_York]`
- Boolean: `boolean`
- Decimal: `decimal128(precision, scale)` -- e.g. `decimal128(19, 4)`

### `normalize_columns`

**Type:** `Option<bool>`  
**Default:** Auto-detect (`true` for Oracle, `false` otherwise)

Whether to lowercase all column names after reading.

```yaml
normalize_columns: true    # Always lowercase (useful for Oracle: EMPLOYEE_ID -> employee_id)
normalize_columns: false   # Always preserve original case
# normalize_columns: null  # Auto-detect: true for oracle://, false otherwise
```

**Use cases:**
- **Oracle**: Oracle uppercases unquoted identifiers. Set to `true` to normalize to lowercase for downstream compatibility.
- **Postgres/MySQL**: Usually `false` or omit (case-insensitive anyway).
- **MSSQL**: Depends on collation -- usually `false`.

### `exclude`

**Type:** `Vec<String>`  
**Default:** `[]`

List of column names to **drop immediately after reading**, before any transforms or schema overrides.

```yaml
exclude:
  - large_blob_data     # Drop this column from the stream
  - internal_metadata   # Drop this column
```

### `batch_size` (Read Override)

**Type:** `Option<usize>`  
**Default:** Inherits from global `config.batch_size`

Per-step read batch size override. Controls how many rows are fetched per query round-trip.

```yaml
batch_size: 5000   # Fetch 5000 rows per query (overrides global batch_size)
```

**When to use:**
- **Large tables**: Increase to 10,000-50,000 for higher throughput.
- **Wide rows**: Decrease to 500-1000 to avoid memory spikes.
- **Network latency**: Higher batch size amortizes round-trip cost.

---

## Sink-Side Schema Options (Universal for all Write Steps)

These options are available on **all sink steps** (`write_db`, `scd2_sink`). They are applied **at the sink boundary**, right before writing.

The preferred way to configure these is via the unified `schema:` block (see [Schema Block](#schema-block-unified-schema-configuration)). The legacy flat fields (`arrow_overrides`, `column_options`) are still accepted for backward compatibility.

### Available on: `write_db`, `scd2_sink`

```yaml
- id: sink
  type: write_db
  target:
    connection: mssql
    schema: dbo
    table: STG_ORDERS
  mode: upsert
  create_table: if_not_exists
  
  # -- Unified schema block (preferred) ----------------------------------------
  
  schema:
    arrow:
      columns:
        col_timestamp:
          type: "timestamp[us, UTC]"  # Arrow type cast at sink boundary
    database:
      columns:
        employee_id:
          primary_key: true
          nullable: false
        email:
          unique: true
          nullable: false
        col_money:
          type: DECIMAL(19,4)        # Force specific SQL type in DDL
        col_timestamp:
          type: DATETIME2(6)         # MSSQL: microsecond precision
        salary:
          check_expr: "salary >= 0"
          default_expr: "0.00"
        department_id:
          foreign_key:
            table: hr.departments
            column: id
        updated_at:
          default_expr: "now()"
          on_update_expr: "now()"    # Auto-update on every UPDATE
        status:
          enum_values: [draft, active, completed, cancelled]  # dialect-appropriate enum
      indexes:
        idx_orders_dept:
          columns: [department_id]
        idx_orders_salary:
          columns: [salary]
  
  # -- Value mapping (sibling of schema:) --------------------------------------
  
  values:
    LOAD_DATETIME: $inserted_at    # batch col or env var → new column LOAD_DATETIME
    SYSTEM_PRESENCE: null          # explicit NULL (database receives NULL, not DEFAULT)
    ORDER_ID: $order_id            # batch col 'order_id' → renamed to ORDER_ID
  
  batch_size: 20000                # Per-step write batch size (sink only)
```

### `arrow_overrides` (Sink — legacy flat form)

> **Prefer:** `schema.arrow.columns` in the unified schema block.

**Type:** `HashMap<String, String>`  
**Default:** `{}`

Same as source-side `arrow_overrides`, but applied **at the sink boundary** before value mapping and metadata stamping. Useful for casting Arrow types right before writing (e.g., ensuring a timestamp has the correct precision/timezone).

```yaml
# Legacy flat form:
arrow_overrides:
  created_at: timestamp[us, UTC]
  amount: decimal128(19, 4)

# Preferred unified form:
schema:
  arrow:
    columns:
      created_at:
        type: "timestamp[us, UTC]"
      amount:
        type: "decimal128(19, 4)"
```

### `values` (Value Mapping)

**Type:** `HashMap<String, ColumnMapping>`  
**Default:** `{}`

Value mapping applied at the sink boundary. Allows renaming columns and injecting NULL values before writing. (Previously named `columns` -- renamed to `values` to support future expression syntax.)

**Keys:** Target column names (as they will appear in the target table).  
**Values:** Either:
- `$name` -- Resolved in order: (1) batch column → rename to target; (2) pipeline `environment:` variable → broadcast value as new column.
- `null` -- Inject an explicit SQL NULL column.

```yaml
values:
  # Batch column or env var → target column
  LOAD_DATETIME: $inserted_at      # batch col 'inserted_at' → LOAD_DATETIME, or env var
  ORDER_ID: $order_id              # batch col 'order_id' → ORDER_ID
  CUSTOMER_ID: $customer_id        # batch col 'customer_id' → CUSTOMER_ID
  
  # Explicit NULL injection
  SYSTEM_PRESENCE: null            # database receives NULL (not DEFAULT)
  UPDATED_AT: null                 # database receives NULL (not DEFAULT)
```

**NULL injection semantics:**
- Columns mapped to `null` are **present in the DDL** (created by `create_table: if_not_exists`) and are **included in the INSERT** with explicit NULL values.
- Because the column IS sent, the database stores NULL — it does **not** trigger DEFAULT expressions or IDENTITY generation.
- **Exception:** MSSQL with `mode: odbc` (no KEEPNULLS) — SQL Server replaces NULL with the column's DEFAULT value. This is the one case where `null` mapping does trigger defaults.
- **To let the database fill a DEFAULT:** omit the column from `values:` entirely and don't include it in the batch. The alignment layer will skip it, and the database fills the DEFAULT.
- **Use case for `null`:** Columns where you explicitly want NULL (e.g., optional fields not yet populated, columns that should be NULL until a later pipeline fills them).

**Important:** Columns not listed in `values:` are passed through unchanged.

### `column_options` (legacy flat form)

> **Prefer:** `schema.database.columns` in the unified schema block.

**Type:** `HashMap<String, ColumnOption>`  
**Default:** `{}`

Per-column DDL hints applied at the sink boundary. Controls primary keys, unique constraints, indexes, foreign keys, CHECK constraints, DEFAULT expressions, SQL type overrides (`db_type`), and more.

When using the legacy flat form, use `db_type` for SQL type overrides (replaces the removed standalone `type_override` map). In the unified `schema.database.columns` block, use `type` instead of `db_type`.

See [Column Options (DDL Hints for Sinks)](#column-options-ddl-hints-for-sinks) for full details.

### `batch_size` (Write Override)

**Type:** `Option<usize>`  
**Default:** Inherits from global `config.batch_size`

Per-step write batch size override. Incoming `RecordBatch`es larger than this value are **automatically sub-chunked** before being passed to `write()`.

```yaml
batch_size: 20000   # Sub-chunk any incoming batch to 20,000 rows before writing
```

**Use case:**
- Use a **large source `batch_size`** (e.g., 50,000) for efficient reads.
- Use a **smaller sink `batch_size`** (e.g., 20,000) to reduce TDS packet size (MSSQL) or transaction size.

**Example:**
```yaml
config:
  batch_size: 50000    # Global: read 50k rows per query

pipeline:
  - id: source
    type: read_db
    from:
      connection: pg
      table: large_table
    # Reads 50,000 rows per query
  
  - id: sink
    type: write_db
    target:
      connection: mssql
      table: target
    batch_size: 20000   # Sub-chunks 50k -> 20k + 20k + 10k before writing
```

---

## Per-Step Driver Options

Driver-specific options for a pipeline step (`read_db`, `write_db`, `scd2_sink`). These **override** the connection-level `options:` block **for this step only**.

### Available on: `read_db`, `write_db`, `scd2_sink`

```yaml
- id: mssql_sink
  type: write_db
  target:
    connection: mssql_shared
    table: orders
  mode: upsert
  options:
    # -- MSSQL write options ---------------------------------------------------
    mode: bcp                   # tiberius (default) | bcp | odbc
    bcp_path: /opt/mssql-tools18/bin/bcp  # Explicit bcp path; implies mode: bcp
    bcp_staging: true           # Force staging even for Append mode
    mssql:
      batch_size: 50000         # Rows per bulk write operation
      staging_table: true       # Force staging table for all write modes
    # Postgres write options
    postgres:
      staging_table: true       # COPY BINARY via temp staging table (5-10x faster)
      max_connections: 10       # Override connection-level pool size
    
    # -- Oracle write options --------------------------------------------------
    direct_path: true           # APPEND_VALUES hint (fast bulk load)
    parallel: 4                 # Parallel DML degree (4 execution servers)
    oci_batch_size: 50000       # Rows per OCI round-trip
    
    # -- Oracle read options ---------------------------------------------------
    prefetch_rows: 500          # OCI prefetch (reduces round-trips)
    fetch_array_size: 1000      # OCI array fetch size

    # -- Databricks read options -----------------------------------------------
    databricks:
      chunk_prefetch: 4         # Parallel Arrow IPC chunk downloads (API mode)
      thrift_fetch_size: 100000 # Rows per Thrift RPC call
      warehouse_timeout: 600    # Max seconds for warehouse cold-start
    
    # -- Identifier case transformation ----------------------------------------
    identifier_case: upper      # as_is | upper | lower
```

### `options` (StepDriverOptions)

Keys that are **not supported** by the target driver are **silently ignored** -- it's safe to specify both MSSQL and Oracle options in the same block.

| Option | Driver | Type | Default | Description |
|--------|--------|------|---------|-------------|
| **MSSQL Write Options** |
| `mode` | MSSQL | `string` | `tiberius` | Write-path mode: `tiberius` (pure Rust, default), `bcp` (CLI bulk-loader, ~500k-2M rows/sec), or `odbc` (ODBC Driver 18, no KEEPNULLS). |
| `bcp_path` | MSSQL | `string` | Auto-discover | Explicit path to `bcp` binary. Setting this implicitly sets `mode: bcp`. |
| `bcp_staging` | MSSQL | `bool` | Auto | Force staging (`true`) even for Append/Truncate, or skip staging (`false`) even for MERGE modes. |
| `mssql.batch_size` | MSSQL | `usize` | `10000` | Rows per bulk write operation. |
| `mssql.staging_table` | MSSQL | `bool` | Auto | `true` = force staging for all modes; `false` = disable; auto = MERGE modes only. |
| **Postgres Write Options** |
| `postgres.staging_table` | Postgres | `bool` | `false` | Use COPY BINARY via temp staging table for upsert/insert_ignore/merge_delete. ~5-10x faster. |
| `postgres.max_connections` | Postgres | `u32` | `5` | Connection pool size for this step. |
| **Oracle Write Options** |
| `direct_path` | Oracle | `bool` | `false` | Use direct-path INSERT via `APPEND_VALUES` hint. Bypasses buffer cache, minimal redo. Incompatible with triggers/IOT/FK. **Caveat:** Auto-commits between batches -- not atomic. |
| `parallel` | Oracle | `u32` | `None` | Parallel DML degree. Issues `ALTER SESSION ENABLE PARALLEL DML` and adds `PARALLEL(table, N)` to INSERT hint. |
| `oci_batch_size` | Oracle | `usize` | Inherits `batch_size` | Max rows per OCI `batch.execute()` call. Set equal to pipeline `batch_size` to avoid sub-splitting. |
| **Oracle Read Options** |
| `prefetch_rows` | Oracle | `u32` | Driver default (2) | Rows fetched per OCI round-trip (`OCI_ATTR_PREFETCH_ROWS`). Increase to reduce network round-trips. |
| `fetch_array_size` | Oracle | `u32` | Driver default | Internal OCI array fetch size (`OCI_ATTR_FETCH_ARRAY_SIZE`). Works with `prefetch_rows`. |
| **Databricks Read Options** |
| `databricks.chunk_prefetch` | Databricks | `usize` | `4` | Number of Arrow IPC chunks to prefetch in parallel (API mode). Higher values improve throughput. |
| `databricks.thrift_fetch_size` | Databricks | `usize` | `100000` | Rows per Thrift `FetchResults` RPC call. |
| `databricks.warehouse_timeout` | Databricks | `u64` | `0` (disabled) | Maximum seconds to wait for warehouse cold-start / auto-resume. `0` = no timeout (watchdog logs only). Set to e.g. `600` for warehouses that may need to auto-resume. |
| **Identifier Case Transformation** |
| `identifier_case` | All | `enum` | `as_is` | How to transform SQL identifiers (table/column names) in DDL/DML. `as_is` \| `upper` \| `lower`. Oracle best practice: `upper` (matches internal storage). |

#### `identifier_case` (IdentifierCase)

Controls how identifiers are emitted in DDL (`CREATE TABLE`) and DML (`INSERT INTO`) statements.

| Value | Behavior | Use Case |
|-------|----------|----------|
| `as_is` | Keep identifiers exactly as they appear in the Arrow schema | Default. Postgres, MySQL. |
| `upper` | Transform all identifiers to UPPERCASE | **Oracle** -- matches internal storage, avoids quoted identifiers. |
| `lower` | Transform all identifiers to lowercase | Postgres convention. |

**Oracle case sensitivity:**
- Unquoted identifiers are **implicitly uppercased** by Oracle: `CREATE TABLE employees (...)` -> stored as `EMPLOYEES` in `ALL_TABLES`.
- Quoted identifiers preserve case but require quotes everywhere: `CREATE TABLE "Employees" (...)` -> must use `"Employees"` in all queries.
- **Best practice for Oracle:** use `identifier_case: upper` to match Oracle's internal storage without requiring quoted identifiers.

**Example:**
```yaml
- id: oracle_sink
  type: write_db
  target:
    connection: oracle_dw
    table: fact_orders
  options:
    identifier_case: upper  # ORDER_DATE, CUSTOMER_ID, TOTAL_PRICE, ...
```

---

## Schema Block (Unified Schema Configuration)

The `schema:` block is the preferred way to configure all schema-related options in a single structured block. It has two sub-blocks: `schema.arrow` for Arrow type casting and `schema.database` for DDL generation (column types, indexes, constraints).

> **Important — `from.schema` / `target.schema` vs `schema:`:** The `schema:` key at the step level is the unified schema configuration block (described here). The SQL namespace (e.g. `hr`, `dbo`, `public`) is configured via `from.schema` (on `read_db`) or `target.schema` (on `write_db` / `scd2_sink`).

### Available on: `read_db`, `rest_api`, `write_db`, `scd2_sink`

> On **sources** (`read_db`, `rest_api`), only `schema.arrow` is typically used — it overrides Arrow types after reading (same effect as flat `arrow_overrides`). `schema.database` is informational on sources and auto-filled from the DB catalog.
>
> On **sinks** (`write_db`, `scd2_sink`), both `schema.arrow` and `schema.database` are used — `schema.arrow` casts data before writing, and `schema.database` drives DDL generation (column types, indexes, constraints).

```yaml
- id: sink
  type: write_db
  target:
    connection: mssql
    schema: dbo
    table: STG_ORDERS
  mode: upsert
  create_table: if_not_exists
  
  # -- Unified Schema Block ---------------------------------------------------
  
  schema:
    arrow:
      columns:
        col_timestamp:
          type: "timestamp[us, UTC]"  # Arrow type cast at sink boundary

    database:
      columns:
        employee_id:
          primary_key: true
          nullable: false
        email:
          unique: true
          nullable: false
        col_money:
          type: DECIMAL(19,4)        # Force specific SQL type in DDL
        col_timestamp:
          type: DATETIME2(6)         # MSSQL: microsecond precision
        salary:
          check_expr: "salary >= 0"
          default_expr: "0.00"
        department_id:
          foreign_key:
            table: hr.departments
            column: id
        updated_at:
          default_expr: "now()"
          on_update_expr: "now()"    # Auto-update on every UPDATE
        status:
          enum_values: [draft, active, completed, cancelled]  # dialect-appropriate enum
      indexes:
        idx_orders_dept:
          columns: [department_id]
        idx_orders_salary:
          columns: [salary]
        idx_orders_email:
          columns: [email]
          unique: true
      constraints:
        chk_salary:
          check: "salary >= 0"
  
  # Value mapping is a sibling of schema:, not nested inside it
  values:
    LOAD_DATETIME: $inserted_at    # batch col or env var → new column LOAD_DATETIME
    SYSTEM_PRESENCE: null          # explicit NULL (database receives NULL, not DEFAULT)
    ORDER_ID: $order_id            # batch col 'order_id' → renamed to ORDER_ID
  
  batch_size: 20000                # Per-step write batch size (sink only)
```

### `schema` (ComponentSchema)

The `schema` block has two sub-blocks:

- **`schema.arrow`** — Arrow-side configuration:
  - `columns`: Per-column Arrow type overrides (`type`, `nullable`, `logical_type`).
- **`schema.database`** — Database-side configuration for DDL generation:
  - `columns`: Per-column DDL hints (`type`, `primary_key`, `nullable`, `unique`, `generated`, `check_expr`, `default_expr`, `on_update_expr`, `foreign_key`, `description`, `enum_values`). Note: `index` is **not** available in the unified block — use `schema.database.indexes` for named indexes.
  - `indexes`: Named multi-column indexes.
  - `constraints`: Named table-level constraints.

#### `schema.arrow.columns` fields

| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` | Arrow type string for casting (e.g. `"timestamp[us, UTC]"`, `"float64"`, `"utf8"`). Same syntax as flat `arrow_overrides`. |
| `nullable` | `bool` | Arrow-level nullable override for this column. |
| `logical_type` | `string` | Semantic / logical type annotation (e.g. `"json"`, `"uuid"`, `"currency"`). Stored as `etl.logical_type` in Arrow metadata and used by the 4-tier DDL type resolver to generate correct target types cross-database. |
| `value` | `string` | Value injection: injects a column with data from an environment variable (`$var_name`) or copies from another batch column (`col_name`). If the column already exists, its data is replaced. Can be combined with `type` to cast after injection and `logical_type` for DDL hints. Works on both sources and sinks. |

```yaml
schema:
  arrow:
    columns:
      payload:
        logical_type: json            # DDL resolver will emit JSONB (Postgres), NVARCHAR(MAX) (MSSQL), etc.
      user_id:
        logical_type: uuid            # DDL resolver will emit UUID (Postgres), UNIQUEIDENTIFIER (MSSQL), etc.
      created_at:
        type: "timestamp[us, UTC]"    # Cast to microsecond UTC timestamp
        nullable: false
      inserted_at:                    # Value injection: add column from env var
        value: $load_ts               # references pipeline environment variable
        type: "timestamp[us, UTC]"
```

#### `schema.database.columns` fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | `string` | -- | Explicit SQL type for DDL generation (e.g. `VARCHAR(50)`, `DECIMAL(19,4)`, `TIMESTAMPTZ`). Highest priority in the type resolver. |
| `primary_key` | `bool` | `false` | Include in PRIMARY KEY constraint. Also used as the MERGE key for `upsert`/`insert_ignore`/`merge_delete`. |
| `nullable` | `bool` | `true` | Whether the column allows NULL. Overrides Arrow `Field::is_nullable()`. |
| `unique` | `bool` | `false` | UNIQUE constraint on this column. |
| `generated` | `bool` | `false` | Column is auto-generated (IDENTITY / SERIAL / AUTO_INCREMENT). |
| `default_expr` | `string` | -- | SQL DEFAULT expression (e.g. `"now()"` — auto-normalised per dialect to `GETDATE()`, `SYSDATE`, etc.). |
| `on_update_expr` | `string` | -- | Auto-update on row modification. MySQL: inline `ON UPDATE`; Postgres/MSSQL/Oracle: creates a trigger. Databricks: not supported. |
| `check_expr` | `string` | -- | SQL CHECK constraint expression. |
| `foreign_key` | `object` | -- | Foreign key reference: `table`, `column`, optional `schema`. |
| `description` | `string` | -- | Human-readable column comment (`COMMENT ON COLUMN` for Postgres/Oracle). |
| `enum_values` | `list` | -- | Allowed values for this column. Postgres: `CREATE TYPE ... AS ENUM`; MySQL: inline `ENUM(...)`; MSSQL/Oracle/Databricks: `CHECK` constraint. |

> **`values` is a sibling, not nested.** The `values:` field (column mapping / NULL injection) is defined at the same level as `schema:`, not inside it.

> **Legacy fields still work.** The flat `arrow_overrides`, `column_options`, and `values` fields are still accepted alongside the `schema:` block. When both are present, `schema.arrow.columns` takes precedence over flat `arrow_overrides`, and `schema.database.columns` takes precedence over flat `column_options`, for columns specified in both.

> **Removed: `type_override`.** The standalone `type_override` map has been removed. Use `schema.database.columns.<col>.type` (or the legacy `column_options.<col>.db_type`) instead.

---

## Write Modes

Controls **how the sink writes data** to the target table. Available on `write_db` and `scd2_sink`.

### Available on: `write_db`

```yaml
mode: append          # Default: INSERT rows, error on duplicate PK
mode: insert_ignore   # INSERT and skip rows that already exist (no conflict error)
mode: upsert          # MERGE: update existing rows + insert new ones
mode: merge_delete    # MERGE + DELETE rows in target that are absent from source batch
mode: truncate        # TRUNCATE TABLE then INSERT (all batches in one transaction)
```

| Mode | SQL Behavior | When to Use |
|------|--------------|-------------|
| `append` | `INSERT INTO` -- errors on duplicate PK/unique constraint. | Default. Incremental loads where rows are guaranteed unique. |
| `insert_ignore` | `INSERT ... ON CONFLICT DO NOTHING` (Postgres), `MERGE ... WHEN NOT MATCHED` (MSSQL/Oracle). Silently skips rows that already exist. | Idempotent loads -- re-running the pipeline is safe. |
| `upsert` | `MERGE ... WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`. Updates existing rows + inserts new ones. Key columns derived from `column_options.primary_key: true`. | SCD Type 1 (overwrite). Incremental loads where rows may change. |
| `merge_delete` | `MERGE` + `DELETE FROM target WHERE key NOT IN (source)`. Turns the target into a **perfect mirror** of the incoming batch. | Full refresh. Mirror a source table. |
| `truncate` | `TRUNCATE TABLE` then `INSERT`. All batches in one transaction. | Full reload every run. Fastest for replace-all scenarios. |

**Key Resolution:**
- `upsert`, `insert_ignore`, `merge_delete`: The MERGE key is **always** derived from the Arrow schema -- mark columns with `column_options.primary_key: true`.
- There is **no separate `upsert_key` field**.

**Example:**
```yaml
- id: sink
  type: write_db
  target:
    connection: mssql
    table: customers
  mode: upsert              # MERGE mode
  column_options:
    customer_id:            # This is the MERGE key
      primary_key: true
      nullable: false
```

---

## Create Table Modes

Controls whether the sink automatically creates its target table. DDL is generated from the Arrow schema using the 4-tier type resolver.

### Available on: `write_db`, `scd2_sink`

```yaml
create_table: never          # Default: never touch the DDL
create_table: if_not_exists  # CREATE TABLE IF NOT EXISTS on first write (safe for production)
create_table: replace        # DROP TABLE IF EXISTS + CREATE TABLE on first write (destroys data!)
```

| Mode | SQL Behavior | When to Use |
|------|--------------|-------------|
| `never` | Never touch the DDL. Table must already exist. | Default. Production -- you control the DDL manually. |
| `if_not_exists` | `CREATE TABLE IF NOT EXISTS` on the first write. No-op if table exists. Safe to leave enabled in production. | Dev/test. First-time setup. Safe for production -- no data loss. |
| `replace` | `DROP TABLE IF EXISTS` + `CREATE TABLE` on the first write. **Destroys all existing data.** | Dev/test only. Full schema reset on every run. **Never use in production.** |

**DDL Generation:**
- Column types are resolved via the **4-tier resolver** (see [Type Resolution Priority](#type-resolution-priority)).
- Primary keys, unique constraints, indexes, foreign keys, CHECK constraints, DEFAULT expressions are all derived from `column_options`.
- Nullability is derived from `column_options.nullable` (overrides Arrow `Field::is_nullable()`).
- Triggers for `on_update_expr` are created in the `post_create` phase (Postgres/MSSQL/Oracle).

**Example:**
```yaml
- id: sink
  type: write_db
  target:
    connection: pg
    table: employees
  create_table: if_not_exists   # Safe for production
  mode: upsert
  column_options:
    employee_id:
      primary_key: true
      nullable: false
    email:
      unique: true
      nullable: false
```

**Generated DDL (Postgres):**
```sql
CREATE TABLE IF NOT EXISTS employees (
  employee_id BIGINT PRIMARY KEY NOT NULL,
  name TEXT,
  email TEXT UNIQUE NOT NULL,
  salary NUMERIC(19,4),
  inserted_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_employees_email ON employees (email);
```

---

## Logical Types

**Logical types** are the **middle layer** between the source DB type and the Arrow physical type. They carry **semantic meaning** through the pipeline via Arrow metadata (`etl.logical_type`).

Source connectors assign a `LogicalType` when they can determine the semantic meaning from the originating database. The sink's DDL resolver reads this to make better type decisions than the Arrow `DataType` alone allows.

### Why Logical Types?

An Arrow `utf8` column could be:
- Plain text
- JSON
- XML
- An IP address
- A UUID

All are **unambiguous once a `LogicalType` is attached**.

### Type Resolution Priority

1. **`column_options.db_type`** (highest -- explicit per-column SQL type override).
2. **`etl.logical_type`** Arrow metadata (semantic type like `json`, `uuid`, `currency`).
3. **Source DB type** cross-dialect mapping.
4. **Arrow `DataType`** fallback (lowest).

### Supported Logical Types

| LogicalType | Source Types | Target DDL Types |
|-------------|-------------|-----------------|
| **`json`** | `jsonb`, `json` (Postgres), `JSON` (MySQL 5.7+, Oracle 21c+) | **Postgres:** `JSONB`<br>**MSSQL:** `NVARCHAR(MAX)`<br>**Oracle:** `CLOB`<br>**MySQL:** `JSON`<br>**Databricks:** `STRING` |
| **`currency`** | `money`, `smallmoney` (MSSQL), `money` (Postgres) | **Postgres:** `NUMERIC(19,4)`<br>**MSSQL:** `MONEY`<br>**Oracle:** `NUMBER(19,4)`<br>**MySQL:** `DECIMAL(19,4)`<br>**Databricks:** `DECIMAL(19,4)` |
| **`uuid`** | `uuid` (Postgres), `uniqueidentifier` (MSSQL), `VARCHAR2(36)` (Oracle convention) | **Postgres:** `UUID`<br>**MSSQL:** `UNIQUEIDENTIFIER`<br>**Oracle:** `VARCHAR2(36)`<br>**MySQL:** `VARCHAR(36)`<br>**Databricks:** `STRING` |
| **`xml`** | `xml` (Postgres, MSSQL), `XMLTYPE` (Oracle) | **Postgres:** `XML`<br>**MSSQL:** `XML`<br>**Oracle:** `CLOB`<br>**MySQL:** `LONGTEXT`<br>**Databricks:** `STRING` |
| **`ip`** | `inet`, `cidr` (Postgres) | **Postgres:** `INET`<br>**MSSQL:** `NVARCHAR(45)`<br>**Oracle:** `VARCHAR2(45)`<br>**MySQL:** `VARCHAR(45)`<br>**Databricks:** `STRING` |
| **`mac_addr`** | `macaddr`, `macaddr8` (Postgres) | **Postgres:** `MACADDR`<br>**MSSQL:** `NVARCHAR(23)`<br>**Oracle:** `VARCHAR2(23)`<br>**MySQL:** `VARCHAR(23)`<br>**Databricks:** `STRING` |
| **`geometry`** | `point`, `line`, `lseg`, `box`, `circle`, `path`, `polygon`, `geometry`, `geography` (PostGIS / MSSQL Spatial) | Stored as WKT text at non-Postgres targets. |
| **`bit_string`** | `bit varying`, `varbit` (Postgres) | Stored as text at all targets except Postgres. |
| **`range`** | `int4range`, `int8range`, `numrange`, `daterange`, `tsrange`, `tstzrange` (Postgres) | Stored as text at all targets except Postgres. |
| **`array`** | `integer[]`, `text[]`, `uuid[]`, ... (Postgres) | **Postgres:** `TEXT` (serialised)<br>**MSSQL:** `NVARCHAR(MAX)`<br>**Oracle:** `CLOB`<br>**MySQL:** `JSON`<br>**Databricks:** `STRING` |
| **`full_text`** | `tsvector`, `tsquery` (Postgres) | Stored as text at all non-Postgres targets. |
| **`interval`** | `interval` (Postgres) | **Postgres:** `INTERVAL`<br>**MSSQL:** `NVARCHAR(50)`<br>**Oracle:** `INTERVAL DAY TO SECOND`<br>**MySQL:** `VARCHAR(50)`<br>**Databricks:** `STRING` |
| **`hstore`** | `hstore` (Postgres extension) | Serialised as JSON string `{"k":"v"}` at non-Postgres targets. |
| **`enum`** | Postgres `CREATE TYPE mood AS ENUM(...)`, MSSQL user-defined types | Stored as text everywhere. |

### Example: Cross-Database UUID Handling

**Source (Postgres):**
```sql
CREATE TABLE users (
  id UUID PRIMARY KEY,
  email TEXT
);
```

**Arrow schema metadata (automatic):**
```
Field { name: "id", data_type: Utf8, nullable: false, metadata: {
  "etl.logical_type": "uuid",
  "etl.source_db": "postgres",
  "etl.source_db_type": "uuid"
}}
```

**Target DDL (auto-resolved by logical type):**

- **Postgres:** `id UUID PRIMARY KEY`
- **MSSQL:** `id UNIQUEIDENTIFIER PRIMARY KEY`
- **Oracle:** `id VARCHAR2(36) PRIMARY KEY`
- **MySQL:** `id VARCHAR(36) PRIMARY KEY`
- **Databricks:** `id STRING PRIMARY KEY`

**No manual `db_type` needed** -- the logical type resolver handles it automatically.

---

## Complete Example

Demonstrates **all universal schema options** in a single pipeline: Databricks -> Postgres with Arrow type casts, audit columns via environment variables, `db_type` overrides, column options, and driver options.

```yaml
config:
  batch_size: 1000
  channel_capacity: 4
  log_level: debug

connections:
  
  databricks_src:
    driver: databricks
    host: adb-1234567890123456.4.azuredatabricks.net
    http_path: /sql/1.0/warehouses/abc123def456
    auth:
      type: oauth2_client_credentials
      client_id: "${DATABRICKS_CLIENT_ID}"
      client_secret: "${DATABRICKS_CLIENT_SECRET}"
    options:
      catalog: main
      schema: analytics
  
  postgres_dest:
    driver: postgres
    host: localhost
    port: 5432
    database: warehouse
    auth:
      type: user_pass
      username: etl_user
      password: secret
    options:
      ssl: require
      application_name: potato_etl

environment:
  load_ts: now()                     # evaluated once at pipeline start

pipeline:
  
  # -- Read from Databricks ----------------------------------------------------
  
  - id: source
    type: read_db
    from:
      connection: databricks_src
      table: raw_events
    batch_size: 5000                 # Per-step read batch size override
    
    # -- Source Schema Options -------------------------------------------------
    
    arrow_overrides:
      employee_id: utf8              # INT64 -> Utf8
      created: timestamp[us, UTC]    # Cast to Timestamp microseconds UTC
      updated: timestamp[us, UTC]    # Cast to Timestamp microseconds UTC
    
    normalize_columns: true          # Lowercase all column names
    
    exclude:                         # Drop these columns from the stream
      - internal_blob_data
      - temp_scratch_field
  
  # -- Write to Postgres -------------------------------------------------------
  
  - id: sink
    type: write_db
    input: source
    target:
      connection: postgres_dest
      schema: analytics
      table: events
    mode: upsert                     # MERGE mode
    create_table: if_not_exists      # Auto-create DDL on first run
    batch_size: 20000                # Per-step write batch size override
    
    # -- Value mapping (sibling of schema:) ------------------------------------
    
    values:
      # Batch column renames
      EVENT_ID: $event_id            # batch col 'event_id' → renamed to EVENT_ID
      USER_ID: $user_id              # batch col 'user_id' → renamed to USER_ID
      
      # Audit columns from environment variables
      inserted_at: $load_ts          # env var load_ts (now()) → new column inserted_at
      updated_at: $load_ts           # initial value; server refreshes via trigger on UPDATE
    
    # -- Unified Schema Block --------------------------------------------------
    
    schema:
      database:
        columns:
          event_id:
            type: VARCHAR(50)        # Force VARCHAR(50) in DDL
            primary_key: true        # MERGE key
            nullable: false
          
          user_id:
            nullable: false
          
          created:
            type: TIMESTAMPTZ        # Force TIMESTAMPTZ in DDL
          
          updated:
            type: TIMESTAMPTZ
          
          status:
            check_expr: "status IN ('pending', 'active', 'completed', 'failed')"
            default_expr: "'pending'"
          
          inserted_at:
            type: TIMESTAMPTZ
            nullable: false
          
          updated_at:
            type: TIMESTAMPTZ
            default_expr: "now()"    # fallback DEFAULT for INSERTs outside this pipeline
            on_update_expr: "now()"  # Postgres trigger refreshes on any UPDATE
            nullable: false
        
        indexes:
          idx_events_user_id:
            columns: [user_id]
    
    # -- Per-Step Driver Options ------------------------------------------------
    
    options:
      identifier_case: lower         # Postgres convention: lowercase identifiers
```

**Generated DDL (Postgres):**
```sql
CREATE TABLE IF NOT EXISTS analytics.events (
  event_id VARCHAR(50) PRIMARY KEY NOT NULL,
  user_id TEXT NOT NULL,
  created TIMESTAMPTZ,
  updated TIMESTAMPTZ,
  status TEXT CHECK (status IN ('pending', 'active', 'completed', 'failed')) DEFAULT 'pending',
  inserted_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_user_id ON analytics.events (user_id);

-- Postgres trigger for on_update_expr
CREATE OR REPLACE FUNCTION update_updated_at_events()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_updated_at_events
BEFORE UPDATE ON analytics.events
FOR EACH ROW EXECUTE FUNCTION update_updated_at_events();
```

**Data flow:**
1. Read from Databricks `raw_events` (5000 rows/batch).
2. Apply `arrow_overrides`: `employee_id` INT64 -> Utf8, `created`/`updated` cast to Timestamp(Microsecond, UTC).
3. Lowercase all column names (`normalize_columns: true`).
4. Drop `internal_blob_data`, `temp_scratch_field` (`exclude`).
5. Sub-chunk to 20,000 rows/batch (`batch_size: 20000`).
6. Apply `values` mapping: rename `event_id` → `EVENT_ID`, `user_id` → `USER_ID`. Inject `inserted_at` and `updated_at` from environment variable `load_ts` (pipeline start timestamp).
7. Stamp `schema.database.columns` as Arrow metadata (including `type` → `etl.db_type`).
8. Generate DDL on first run (`create_table: if_not_exists`), including named indexes from `schema.database.indexes`.
9. MERGE into Postgres (`mode: upsert`, key = `event_id`).
10. On future UPDATEs (inside or outside this pipeline), the Postgres trigger refreshes `updated_at` to `now()`.

---

## Summary

| Option Category | Where | Applied When | Configures |
|----------------|-------|--------------|-----------| 
| **Global Config** | `config:` | Pipeline start | `batch_size`, `channel_capacity`, `log_level` |
| **Source Schema** | `read_db`, `rest_api` | After reading | `arrow_overrides`, `normalize_columns`, `exclude`, `batch_size` (read) |
| **Unified Schema** | `schema:` on sink | Before writing / DDL | `schema.arrow.columns` (Arrow casts), `schema.database.columns` (DDL), `schema.database.indexes`, `schema.database.constraints` |
| **Value Mapping** | `values:` on sink | Before writing | Column renaming and NULL injection at the sink boundary |
| **Driver Options** | `options:` on step | Per-step override | MSSQL bcp/ODBC, Oracle direct-path, identifier case, prefetch, etc. |
| **Write Mode** | `mode:` on sink | Before writing | How data is written: append, upsert, merge_delete, truncate, insert_ignore |
| **Create Table** | `create_table:` on sink | First write | Whether to auto-generate DDL: never, if_not_exists, replace |
| **DB Namespace** | `from.schema` / `target.schema` | SQL generation | SQL schema / namespace (e.g. `hr`, `dbo`, `public`). |
| **Logical Types** | Arrow metadata | DDL resolution | Semantic type (json, uuid, currency, ...) -- cross-database type mapping |

**Key principles:**
- **`schema:` block** is the preferred way to configure Arrow casts (`schema.arrow.columns`) and DDL hints (`schema.database.columns`, `indexes`, `constraints`).
- **`from.schema` / `target.schema`** is the SQL namespace (e.g. `hr`, `dbo`), configured inside the `from:` or `target:` block.
- **`values`** (formerly `columns`) handles column renaming and NULL injection at the sink boundary.
- **`schema.database.columns.<col>.type`** replaces the removed standalone `type_override` map. In the legacy flat `column_options`, use `db_type` instead.
- **Legacy flat fields** (`arrow_overrides`, `column_options`) are still accepted; `schema:` block takes precedence when both are set.
- **Source-side options** (`arrow_overrides`, `normalize_columns`, `exclude`) are applied **immediately after reading**, before any transforms.
- **Sink-side options** (`schema`, `values`, legacy `arrow_overrides`/`column_options`) are applied **at the sink boundary**, right before writing.
- **Driver options** (`options:`) are **per-step overrides** of connection-level settings.
- **No global schema state** -- all schema configuration is **per-step** and **explicit**.
- **DDL is generated from Arrow schema** using the **4-tier type resolver** (`schema.database.columns.type` / `column_options.db_type` -> `etl.logical_type` -> source DB type -> Arrow type).