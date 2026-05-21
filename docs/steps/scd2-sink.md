# scd2_sink

Category: **Sink**

Slowly-Changing Dimension Type 2 sink. Automatically maintains a change-history table:

- When a tracked column changes, the old row is expired (`valid_to = now()`, `is_current = false`) and a new version is inserted
- Unchanged rows are left untouched
- New entities are inserted with `is_current = true`
- When `close_missing: true`, entities that exist in the database but are absent from the incoming data are expired

## Basic usage

```yaml
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
    - department_code
    - employment_type
  create_table: if_not_exists
```

## All fields

| Field | Required | Default | Description |
|---|---|---|---|
| `target.connection` | yes | -- | Named connection reference |
| `target.table` | yes | -- | Target history table |
| `target.schema` | no | -- | Database schema / namespace |
| `input` | yes | -- | Step to read from |
| `key` | yes | -- | Business key column (identifies the entity) |
| `track` | no | all | Columns to monitor for changes. If omitted, all non-key columns are tracked. |
| `close_missing` | no | `false` | When `true`, expire current rows whose key was **not** seen in the incoming data. See [Close-missing mode](#close-missing-mode). |
| `create_table` | no | `never` | Auto-create the history table |
| `batch_size` | no | global | Override the pipeline-level batch size for this step. Controls both the incoming batch sub-chunking *and* the SQL chunk size for `WHERE key IN (...)` queries in write and flush paths. |
| `schema` | no | -- | Unified schema block with `schema.arrow.columns` and `schema.database.columns` / `indexes` / `constraints`. Same structure as `write_db`. |
| `scd2_columns` | no | defaults | Custom names for the system columns (see below) |
| `options` | no | -- | Per-step driver options (see [Step driver options](./driver-options.md)) |

## Close-missing mode

The `close_missing` option controls how the SCD2 sink handles keys that are **present in the database** (`is_current = true`) but **absent from the incoming data**.

### The problem

There are three common ingestion patterns, and each one requires different handling of absent keys:

| # | Pattern | What you receive | What "absent" means |
|---|---------|------------------|---------------------|
| 1 | **Full snapshot** (single batch) | All records at once | "Record no longer exists" -- expire it |
| 2 | **Full snapshot** (paginated) | All records over N pages/batches | Same as #1, but you only know after the last page |
| 3 | **Delta / cherry-pick** | Only changed or specifically requested records | "Not fetched" -- don't touch it |

### Configuration

```yaml
# Situation 1 & 2: Full snapshot
# All candidates come from the API. If a candidate is not in the result,
# they should be marked as no longer current.
- id: scd2_candidates
  type: scd2_sink
  input: all_candidates
  target:
    connection: dwh
    table: dim_candidates
  key: candidate_id
  track:
    - first_name
    - last_name
    - email
    - status
  close_missing: true    # <-- expire keys not seen in incoming data

# Situation 3: Delta
# Only one specific candidate was fetched. Do not touch other records.
- id: scd2_single
  type: scd2_sink
  input: single_candidate
  target:
    connection: dwh
    table: dim_candidates
  key: candidate_id
  track:
    - first_name
    - last_name
    - email
    - status
  close_missing: false   # <-- default: only process keys in the batch
```

### How it works internally

| Mode | `close_missing: false` (default) | `close_missing: true` |
|------|------|------|
| **Per batch** | Fetch current rows for keys IN this batch. Compare, insert/update as needed. | Same, plus collect all seen keys in a set. |
| **At flush** | Close the connection pool. Done. | 1. Query ALL current keys from the database. 2. Compute `missing = current_keys - seen_keys`. 3. Close missing keys (`is_current = false`, `valid_to = NOW()`). 4. Close the connection pool. |

This means **paginated snapshots work automatically**. Whether you send 1 batch or 50 pages, the sink collects all seen keys across all batches, and only at `flush()` (after the last batch) executes the close-missing query.

### Supported databases

`close_missing` is supported in all five database drivers:

| Driver | Close-missing query |
|---|---|
| **PostgreSQL** | `UPDATE schema.table SET valid_to = now(), is_current = false WHERE is_current = true AND key IN (...)` |
| **MySQL** | `UPDATE table SET valid_to = NOW(6), is_current = 0 WHERE is_current = 1 AND key IN (...)` |
| **MSSQL** | `UPDATE [schema].[table] SET [valid_to] = SYSDATETIMEOFFSET(), [is_current] = 0 WHERE [is_current] = 1 AND [key] IN (...)` |
| **Oracle** | `UPDATE schema.table SET valid_to = SYSTIMESTAMP, is_current = 0 WHERE is_current = 1 AND key = :1` (batched) |
| **Databricks** | `UPDATE table SET valid_to = current_timestamp(), is_current = false WHERE is_current = true AND key IN (...)` |

The close-missing query is chunked to avoid query-size limits. The chunk size defaults to 1000 (or the per-step `batch_size` if set). Oracle defaults to 20,000 (matching its OCI batch size) unless overridden via `batch_size` or driver options.

### Python API

```python
pipeline.scd2_sink(
    source,
    conn="postgresql://...",
    table="dim_candidates",
    key="candidate_id",
    track=["first_name", "last_name", "email", "status"],
    close_missing=True,    # <-- full-snapshot mode
    create_table="if_not_exists",
)
```

## Custom SCD2 column names

By default the sink manages four system columns: `valid_from`, `valid_to`, `is_current`, and `scd_id`. You can rename them:

```yaml
  scd2_columns:
    valid_from: effective_from
    valid_to: effective_to
    is_current: is_latest
    scd_id: surrogate_key
```