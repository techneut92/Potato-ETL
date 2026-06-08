# Step Driver Options

Both `read_db` and `write_db` (and `scd2_sink`) accept an `options:` block that overrides connection-level driver settings for that specific step. This is the same set of options regardless of whether it's a source or sink -- unsupported keys for the current driver are silently ignored.

## MSSQL options

```yaml
options:
  mode: bcp                  # tiberius (default) | bcp | odbc
  bcp_path: /opt/mssql-tools18/bin/bcp  # explicit path (auto-detected if absent)
  bcp_staging: true          # force staging table even for append/truncate modes
```

| Option | Default | Description |
|---|---|---|
| `mode` | from conn | Write-path mode: `tiberius` (default, pure Rust), `bcp` (CLI bulk-loader, ~500k-2M rows/sec), or `odbc` (ODBC Driver 18, lets SQL Server apply DEFAULTs). Overrides the connection-level `mode`. |
| `bcp_path` | auto | Explicit path to the bcp binary. Setting this implicitly sets `mode: bcp`. |
| `bcp_staging` | auto | `true` = force staging table even for append/truncate; `false` = skip staging. Auto = staging for MERGE modes only (upsert/insert_ignore/merge_delete). |

> **Note:** The legacy `bcp: true` / `odbc: true` shorthand fields have been removed.
> Use `mode: bcp` or `mode: odbc` instead.

> **Column names must not be bracketed.** Never wrap an MSSQL column name in
> T-SQL brackets in the pipeline — e.g. `rename_to: "[KEY]"`. Brackets are SQL
> *quoting syntax*, not part of the name; the driver quotes identifiers itself
> and handles reserved words like `KEY`, `USER`, `ORDER` automatically. A
> bracketed name is rejected at write time (it would become `[[KEY]]` and would
> never match the real column). Use the bare name: `rename_to: KEY`.
>
> Reserved-word columns work natively in `odbc` and `bcp` modes. The `tiberius`
> mode currently **cannot** bulk-load a column whose name is a T-SQL reserved
> word (tiberius emits the `INSERT BULK` column list unquoted); use `mode: odbc`
> or `mode: bcp` for such tables.

## Oracle options

```yaml
options:
  prefetch_rows: 500       # rows per OCI round-trip (read only)
  fetch_array_size: 1000   # internal OCI array size (read only)
  direct_path: true        # APPEND_VALUES hint for fast bulk INSERT (write only)
  parallel: 4              # parallel DML degree (write only)
  oci_batch_size: 50000    # rows per OCI execute call (write only)
```

| Option | Default | Description |
|---|---|---|
| `prefetch_rows` | driver default (2) | Rows fetched per OCI round-trip. Higher = fewer round-trips, more memory. |
| `fetch_array_size` | driver default | Internal OCI array size. Works in tandem with `prefetch_rows`. |
| `direct_path` | `false` | Use `APPEND_VALUES` hint for direct-path INSERT. Bypasses buffer cache, minimal redo. Only for append/truncate on plain heap tables (no triggers, no IOT, no FK). |
| `parallel` | none (serial) | Parallel DML degree. Combined with `direct_path`, enables multi-process direct-path loads. |
| `oci_batch_size` | from batch_size | Max rows per OCI `execute()` call. Also sets the SCD2 SQL chunk size for `WHERE key IN (...)` queries. Match to your pipeline batch_size for best results. |

## Cross-driver options

```yaml
options:
  identifier_case: upper       # transform SQL identifiers: as_is | upper | lower
  on_missing_column: skip      # skip (default) | error
```

| Option | Default | Description |
|---|---|---|
| `identifier_case` | `as_is` | Case transformation for table/column names in DDL and DML. `upper` is recommended for Oracle (matches its internal storage). `lower` is idiomatic for Postgres. |
| `on_missing_column` | `skip` | What to do when the **target table** has a column that the incoming batch does **not** provide (matched by name, after `identifier_case` and any `rename_to`). `skip` (default) omits the column so the database supplies `NULL` / its `DEFAULT`. `error` is an opt-in strict mode that fails **only** when the missing column is `NOT NULL` with no default — i.e. the DB genuinely can't fill it. Applies to every database sink (Postgres, MSSQL, MySQL, Oracle, Databricks). |

> **Why `skip` is the default:** the database is the authority on `NOT NULL`. A
> `NOT NULL`, no-default column you omit is rejected by the DB anyway; a nullable
> or defaulted column (e.g. an `inserted_at DEFAULT now()` audit column) is meant
> to be filled by the server. Writing a subset of a table's columns is a normal,
> supported pattern, so the engine doesn't second-guess it. Set
> `on_missing_column: error` if you'd rather fail early with a clear message
> than let the DB reject a required column mid-load.
>
> Extra batch columns **not** in the table are dropped (with a debug log) — with
> one exception, below.

### Discarded `rename_to` (always enforced)

Independent of `on_missing_column`, a `rename_to` whose target matches **no**
column in the table is **always an error** — that rename silently sent its data
nowhere. This catches the common mistake of renaming to a name the table doesn't
actually have:

```yaml
# table column is really `KEY`, so this discards the data → hard error
key:
  rename_to: ATTRIBUTE_KEY
```
```
rename_to target 'ATTRIBUTE_KEY' matches no column in target table [stg.FOO] —
the rename produced a column the table doesn't have, so its data would be
silently discarded. Fix the target name to match an existing column, or remove
the rename.
```

A `rename_to` for a source column that isn't selected is harmless dead config —
it never reaches the batch, so it doesn't trigger this check.

## Databricks options

```yaml
options:
  mode: api                  # api (default) | odbc | thrift
  databricks:
    chunk_prefetch: 4        # parallel Arrow IPC chunk downloads (default: 4)
    thrift_fetch_size: 100000  # rows per Thrift RPC call (default: 100,000)
    warehouse_timeout: 600   # max seconds to wait for warehouse cold-start (default: 0 = disabled)
```

| Option | Default | Description |
|---|---|---|
| `mode` | `api` | Read-path mode: `api` (REST SQL Statement API), `odbc` (Simba ODBC driver), or `thrift` (Thrift RPC). |
| `databricks.chunk_prefetch` | `4` | Number of Arrow IPC chunks to download in parallel (API mode only). Higher values improve throughput at the cost of memory. |
| `databricks.thrift_fetch_size` | `100000` | Rows per `FetchResults` RPC call (Thrift mode only). |
| `databricks.warehouse_timeout` | `0` (disabled) | Maximum seconds to wait for the first batch from a Databricks SQL warehouse. Covers cold-start / auto-resume latency. Set to `600` (10 minutes) for warehouses that may need to auto-resume. `0` = no timeout (watchdog logs only). |

## Postgres options (step-level)

```yaml
options:
  postgres:
    staging_table: true      # COPY BINARY via temp staging table for upsert (5-10x faster)
    max_connections: 10      # override connection-level pool size
```

| Option | Default | Description |
|---|---|---|
| `postgres.staging_table` | `false` (inherits connection) | Use COPY BINARY via temporary staging table for `insert_ignore`, `upsert`, and `merge_delete`. ~5-10x faster than parameterized INSERTs. Requires temp table permissions. |
| `postgres.max_connections` | `5` (inherits connection) | Connection pool size for this step. |

## MSSQL options (step-level)

```yaml
options:
  mode: bcp                  # override connection-level mode for this step
  bcp_staging: true          # force staging even for append mode
  mssql:
    staging_table: true      # force staging table for all write modes
    batch_size: 50000        # rows per bulk operation
```

| Option | Default | Description |
|---|---|---|
| `bcp_staging` | auto | Override bcp staging behaviour: `true` = force staging even for append/truncate; `false` = skip staging (dangerous for MERGE modes). |
| `mssql.staging_table` | auto (inherits connection) | Same as connection-level `staging_table` — `true` forces staging for all modes. |
| `mssql.batch_size` | `10000` (inherits connection) | Rows per bulk write operation for this step. Also sets the SCD2 SQL chunk size for `WHERE key IN (...)` queries. |

## MySQL options (step-level)

MySQL step-level options are currently limited to `init_sql` (see [init_sql](#init_sql-connection-level) below) and `identifier_case` (cross-driver). MySQL connections inherit `init_sql` from the connection level when not overridden per-step.

## init_sql (connection-level)

All database drivers (except Oracle) support an `init_sql` option at the connection level. These SQL statements are executed once per connection (or on each new pool connection) before any pipeline work begins. Use them for session-level settings.

At the **connection level**, `init_sql` is a flat field inside `options:`:

```yaml
# PostgreSQL — connection level
connections:
  pg:
    driver: postgres
    # ...
    options:
      init_sql:
        - "SET work_mem = '256MB'"
        - "SET statement_timeout = 60000"

# MSSQL — connection level
connections:
  mssql:
    driver: mssql
    # ...
    options:
      init_sql:
        - "SET LOCK_TIMEOUT 5000"
        - "SET DEADLOCK_PRIORITY LOW"

# MySQL — connection level
connections:
  mysql:
    driver: mysql
    # ...
    options:
      init_sql:
        - "SET SESSION group_concat_max_len = 1048576"

# Databricks — connection level
connections:
  dbx:
    driver: databricks
    # ...
    options:
      init_sql:
        - "SET spark.sql.shuffle.partitions = 200"
```

At the **step level**, `init_sql` is nested under the driver sub-key inside `StepDriverOptions`, or as a top-level driver-agnostic field:

```yaml
# Step-level: nested under driver sub-key
steps:
  - id: sink
    type: write_db
    options:
      postgres:
        init_sql:
          - "SET work_mem = '512MB'"       # override connection-level for this step
      # -- or driver-agnostic --
      init_sql:
        - "SET work_mem = '512MB'"         # used by Databricks and as fallback
```

| Driver | Connection-level | Step-level | Inherits to step? |
|---|---|---|---|
| Postgres | `options.init_sql` | `options.postgres.init_sql` | Yes — step inherits when step-level is empty |
| MSSQL | `options.init_sql` | `options.mssql.init_sql` | Yes |
| MySQL | `options.init_sql` | `options.mysql.init_sql` | Yes |
| Databricks | `options.init_sql` | `options.init_sql` (top-level) | Yes |
| Oracle | Not supported | Not supported | — |