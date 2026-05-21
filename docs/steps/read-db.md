# read_db

Category: **Source**

Reads rows from a database table in batches. The `cursor` column controls pagination -- the source keeps fetching batches until no more rows are returned.

## Basic usage

```yaml
- id: orders
  type: read_db
  from:
    connection: source_db
    table: orders
    cursor: id
```

## All fields

```yaml
- id: <id>
  type: read_db
  from:
    connection: <connection>       # named connection (required)
    table: <table>                 # table name (use table or query, not both)
    query: "SELECT ..."            # custom SQL query (alternative to table)
    cursor: <column>               # cursor column for batched pagination
    schema: <namespace>            # database schema / namespace (e.g. hr, dbo)
  batch_size: <n>                  # override pipeline-level batch_size
  normalize_columns: true          # lowercase all column names (auto for Oracle)
  exclude: [col_a, col_b]         # drop columns immediately after reading
  schema:                          # unified schema block
    arrow:
      columns:
        col_name:
          type: utf8
  options: {}                      # per-step driver options
```

| Field | Required | Default | Description |
|---|---|---|---|
| `from.connection` | yes | -- | Named connection reference |
| `from.table` | yes* | -- | Table name to read. *Use `table` or `query`, not both. |
| `from.query` | no | -- | Custom SQL query instead of reading the full table |
| `from.cursor` | no | -- | Column for cursor-based pagination (recommended for large tables) |
| `from.schema` | no | -- | Database schema / namespace (e.g. `hr`, `dbo`) |
| `batch_size` | no | global | Override the pipeline-level `batch_size` for this source only |
| `normalize_columns` | no | auto | Lowercase all column names. Auto-enabled for Oracle, off for others. |
| `exclude` | no | `[]` | List of column names to drop immediately after reading |
| `schema` | no | -- | Unified schema block. `schema.arrow.columns` overrides Arrow types after reading; `schema.database.columns` stamps metadata (PK, etc.) that propagates downstream. |
| `options` | no | -- | Per-step driver options (see [Step driver options](./driver-options.md)) |

## Custom query

```yaml
- id: recent_orders
  type: read_db
  from:
    connection: source_db
    query: "SELECT * FROM orders WHERE created_at > '2025-01-01'"
    cursor: id
```

## Excluding columns

Drop columns you don't need (saves memory and avoids type incompatibilities):

```yaml
- id: users
  type: read_db
  from:
    connection: source_db
    table: user_details
    cursor: id
  exclude:
    - large_blob_data
    - internal_notes
```

## Arrow overrides

Cast columns to specific Arrow types after reading. Use the unified `schema:` block:

```yaml
- id: source
  type: read_db
  from:
    connection: pg
    table: type_showcase
  schema:
    arrow:
      columns:
        employee_id:
          type: utf8
        hire_date:
          type: timestamp
        col_date:
          type: date32
        col_time:
          type: "time64[us]"
        col_timestamptz:
          type: "timestamp[us, UTC]"
```

See [Expression Reference > Type casting](./expression-reference.md#type-casting) for the full list of supported type strings.
