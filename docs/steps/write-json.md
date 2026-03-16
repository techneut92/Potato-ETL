# write_json

Write Arrow data to a local JSON file.

## When to use

- Exporting pipeline results as JSON for APIs, frontends, or downstream services
- Creating human-readable data dumps with pretty-printing
- Wrapping output in a named key for API-compatible JSON structures
- Prototyping pipelines without a database connection

## Configuration

### Direct path (local filesystem)

```yaml
- id: output
  type: write_json
  input: transformed
  path: output/result.json
  pretty: true                   # optional (default: false)
  wrap_key: employees            # optional: wrap array in {"employees": [...]}
```

### Connection-based (local, SFTP, S3, etc.)

```yaml
- id: output
  type: write_json
  input: transformed
  target:
    connection: data_lake_s3     # named file connection
    path: processed/result.json  # relative to connection's base_path
  pretty: true
  wrap_key: employees
```

| Field | Required | Default | Description |
|---|---|---|---|
| `input` | yes | -- | Step ID to read data from. |
| `path` | * | -- | Filesystem path for the output JSON file. Parent directories are created automatically. Mutually exclusive with `target`. |
| `target` | * | -- | Connection-based file target (`connection` + `path`). Mutually exclusive with `path`. |
| `pretty` | no | `false` | Pretty-print the JSON output with indentation. |
| `wrap_key` | no | `null` | Wrap the output array in an object with this key. When `null`, the output is a bare JSON array. |

\* Exactly one of `path` or `target` must be provided.

## Output format

Each row becomes a JSON object with column names as keys. Values are automatically typed:

| Arrow type | JSON output |
|---|---|
| `Int64` | `123` (number) |
| `Float64` | `3.14` (number) |
| `Boolean` | `true` / `false` |
| `Utf8` | `"string"` |
| `Null` | `null` |

### Bare array (default)

```json
[
  {"id": 1, "name": "Alice", "active": true},
  {"id": 2, "name": "Bob", "active": false}
]
```

### Wrapped with `wrap_key: employees`

```json
{
  "employees": [
    {"id": 1, "name": "Alice", "active": true},
    {"id": 2, "name": "Bob", "active": false}
  ]
}
```

## Behavior

- All incoming batches are **collected in memory** and written as a single JSON file.
- If the output file already exists, it is **overwritten**.
- Parent directories are created automatically.
- When `pretty: true`, the output uses 2-space indentation.

## Examples

### Basic JSON output

```yaml
- id: output
  type: write_json
  input: processed
  path: output/results.json
```

### Pretty-printed with wrapper

```yaml
- id: output
  type: write_json
  input: active_employees
  path: output/active_employees.json
  pretty: true
  wrap_key: employees
```

### Nested JSON normalization to files

A complete pipeline that reads nested JSON, normalizes it into separate tables, and writes each as a different file format:

```yaml
steps:
  - id: raw
    type: read_json
    path: data/candidates.json
    data_path: candidates

  - id: candidates_flat
    type: flatten
    input: raw
    select:
      id: id
      name: first_name
      email: email

  - id: write_candidates
    type: write_csv
    input: candidates_flat
    path: output/candidates.csv

  - id: pools_exploded
    type: unnest
    input: raw
    column: talent_pools
    parent_fields:
      candidate_id: id
    fields:
      pool_id: id
      pool_name: name

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
```

## Dry-run behavior

In `dry-run` mode, `write_json` is replaced with a no-op pass-through transform. No file is created or modified.

## See also

- [read_json](./read-json.md) — JSON file source
- [write_csv](./write-csv.md) — CSV file sink
- [write_db](./write-db.md) — Database sink
- [rest_api_sink](./rest-api-sink.md) — REST API sink
- [File connections](../connections/file-connections.md) — Remote storage setup
- [Transport Crates & Feature Flags](../feature-flags.md) — Remote transports require `transport-*` features at build time