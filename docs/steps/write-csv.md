# write_csv

Write Arrow data to a local CSV file.

## When to use

- Exporting pipeline results for downstream tools that expect CSV
- Generating reports for spreadsheet consumption
- Prototyping pipelines without a database connection
- Creating denormalized flat files from normalized data

## Configuration

### Direct path (local filesystem)

```yaml
- id: output
  type: write_csv
  input: transformed
  path: output/result.csv
  delimiter: ","                 # optional (default: ",")
  has_header: true               # optional (default: true)
```

### Connection-based (local, SFTP, S3, etc.)

```yaml
- id: output
  type: write_csv
  input: transformed
  target:
    connection: reports_sftp     # named file connection
    path: monthly/result.csv     # relative to connection's base_path
  delimiter: ","
```

| Field | Required | Default | Description |
|---|---|---|---|
| `input` | yes | -- | Step ID to read data from. |
| `path` | * | -- | Filesystem path for the output CSV file. Parent directories are created automatically. Mutually exclusive with `target`. |
| `target` | * | -- | Connection-based file target (`connection` + `path`). Mutually exclusive with `path`. |
| `delimiter` | no | `","` | Single-character column delimiter. |
| `has_header` | no | `true` | Whether to write a header row with column names. |

\* Exactly one of `path` or `target` must be provided.

## Behavior

- All incoming batches are **collected in memory** and written as a single CSV file.
- If the output file already exists, it is **overwritten**.
- Parent directories are created automatically (e.g., `output/reports/data.csv` creates `output/reports/` if missing).
- Column values are formatted using Arrow's default display format.

## Examples

### Basic CSV output

```yaml
- id: output
  type: write_csv
  input: processed
  path: output/results.csv
```

### Semicolon-delimited output

```yaml
- id: output
  type: write_csv
  input: candidates
  path: output/candidates.csv
  delimiter: ";"
  has_header: true
```

### Multi-output pipeline (CSV + JSON)

```yaml
steps:
  - id: employees
    type: read_csv
    path: data/employees.csv

  - id: active
    type: filter
    input: employees
    column: active
    value: "true"

  - id: stats
    type: aggregate
    input: active
    group_by: [department]
    metrics:
      headcount: count()
      avg_salary: avg(salary)

  - id: write_stats_csv
    type: write_csv
    input: stats
    path: output/department_stats.csv

  - id: write_stats_json
    type: write_json
    input: stats
    path: output/department_stats.json
    pretty: true
```

## Dry-run behavior

In `dry-run` mode, `write_csv` is replaced with a no-op pass-through transform. No file is created or modified.

## See also

- [read_csv](./read-csv.md) — CSV file source
- [write_json](./write-json.md) — JSON file sink
- [write_db](./write-db.md) — Database sink
- [File connections](../connections/file-connections.md) — Remote storage setup
- [Transport Crates & Feature Flags](../feature-flags.md) — Remote transports require `transport-*` features at build time