# read_csv

Read a local CSV file as a pipeline source.

## When to use

- Loading CSV exports, reports, or seed data from disk
- Converting CSV files to other formats (JSON, database tables)
- Prototyping pipelines without a database connection
- Ingesting data from systems that export CSV

## Configuration

### Direct path (local filesystem)

```yaml
- id: employees
  type: read_csv
  path: data/employees.csv
  delimiter: ","                 # optional (default: ",")
  has_header: true               # optional (default: true)
  batch_size: 5000               # optional: override global batch_size
  sort_glob: name                # optional: sort order for glob matches
  normalize_columns: true        # optional: lowercase all column names
  exclude:
    - internal_id               # optional: list of column names to drop
  schema:
    arrow:
      columns:
        salary:
          type: float64
        start_date:
          type: date32
        active:
          type: boolean
```

### Connection-based (local, SFTP, S3, etc.)

```yaml
- id: employees
  type: read_csv
  from:
    connection: incoming_sftp    # named file connection
    path: reports/employees.csv  # relative to connection's base_path
  delimiter: ","
```

| Field | Required | Default | Description |
|---|---|---|---|
| `path` | * | -- | Filesystem path to the CSV file (relative to CWD). Mutually exclusive with `from`. |
| `from` | * | -- | Connection-based file location (`connection` + `path`). Mutually exclusive with `path`. |
| `delimiter` | no | `","` | Single-character column delimiter. Common values: `","`, `";"`, `"\t"`, `"|"`. |
| `has_header` | no | `true` | Whether the first row contains column names. When `false`, columns are named `column_0`, `column_1`, etc. |
| `batch_size` | no | global | Override the global `config.batch_size` for this source. |
| `sort_glob` | no | `name` | Sort order for glob matches: `name` (ascending) or `name_desc` (descending). Only used when `path` contains glob characters. |
| `normalize_columns` | no | `false` | Lowercase all column names after reading. |
| `exclude` | no | `[]` | List of column names to drop immediately after reading. |
| `schema` | no | -- | Unified schema block. `schema.arrow.columns` overrides inferred Arrow types after reading. See [Universal Schema Options](../universal-schema-options.md). |

\* Exactly one of `path` or `from` must be provided.

## Glob patterns

The `path` field supports glob patterns to read multiple files at once. All matched files are processed sequentially as a single logical source.

| Pattern | Meaning |
|---------|---------|
| `*` | Matches any sequence of non-`/` characters |
| `?` | Matches any single non-`/` character |
| `[abc]` | Character class |
| `[a-z]` | Character range |
| `[!0-9]` | Negated class |
| `**` | Recursive: matches zero or more directory levels |

```yaml
# All CSV files in a directory
- id: all_reports
  type: read_csv
  path: data/report_*.csv

# Recursive: search subdirectories
- id: deep_search
  type: read_csv
  path: incoming/**/export_*.csv

# Descending alphabetical order
- id: newest_first
  type: read_csv
  path: data/daily_*.csv
  sort_glob: name_desc
```

When `path` contains no glob characters, the step reads a single file (backwards compatible).

## Schema inference

Arrow's CSV reader infers the schema from the first 100 rows of the file. All columns are read as their best-fit Arrow type:

| CSV content | Inferred Arrow type |
|---|---|
| `123`, `-45` | `Int64` |
| `3.14`, `1e10` | `Float64` |
| `true`, `false` | `Boolean` |
| Everything else | `Utf8` |

Use `schema.arrow.columns` to override inferred types when the auto-detection is wrong (e.g., ZIP codes like `01234` that should stay as `Utf8`, not be parsed as integers).

## Examples

### Basic CSV read

```yaml
- id: orders
  type: read_csv
  path: data/orders.csv
```

### Semicolon-delimited file

```yaml
- id: european_data
  type: read_csv
  path: data/export.csv
  delimiter: ";"
```

### Tab-separated values

```yaml
- id: tsv_data
  type: read_csv
  path: data/export.tsv
  delimiter: "\t"
```

### CSV to database

```yaml
connections:
  pg:
    driver: postgres
    host: localhost
    database: analytics
    auth:
      type: user_pass
      username: etl_user
      password: "etl_pass"

steps:
  - id: employees
    type: read_csv
    path: data/employees.csv

  - id: active_only
    type: filter
    input: employees
    column: active
    value: "true"

  - id: output
    type: write_db
    input: active_only
    target:
      connection: pg
      table: employees
    mode: truncate
    create_table: if_not_exists
```

### CSV to JSON conversion

```yaml
steps:
  - id: data
    type: read_csv
    path: data/report.csv

  - id: output
    type: write_json
    input: data
    path: output/report.json
    pretty: true
```

## Error handling

| Condition | Behavior |
|---|---|
| File not found | Error: `read_csv: cannot open '<path>'` |
| Malformed CSV row | Error: `read_csv: error reading batch` |
| Empty file (header only) | Returns an empty `RecordBatch` with inferred column names |

## See also

- [read_json](./read-json.md) — JSON file source
- [write_csv](./write-csv.md) — CSV file sink
- [read_db](./read-db.md) — Database source
- [File connections](../connections/file-connections.md) — Remote storage setup
- [Transport Crates & Feature Flags](../feature-flags.md) — Remote transports require `transport-*` features at build time