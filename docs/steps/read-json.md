# read_json

Read a local JSON file as a pipeline source.

## When to use

- Loading seed data, fixtures, or configuration from JSON files
- Ingesting API response dumps saved to disk
- Prototyping pipelines without a database connection
- Normalizing nested JSON structures (combine with `unnest` and `flatten`)

## Configuration

### Direct path (local filesystem)

```yaml
- id: candidates
  type: read_json
  path: data/candidates.json
  data_path: candidates          # optional: dot-notation path to the array
  batch_size: 500                # optional: override global batch_size
  sort_glob: name_desc           # optional: sort order for glob matches
```

### Connection-based (local, SFTP, S3, etc.)

```yaml
- id: candidates
  type: read_json
  from:
    connection: data_sftp        # named file connection
    path: incoming/candidates.json  # relative to connection's base_path
  data_path: candidates
```

| Field | Required | Default | Description |
|---|---|---|---|
| `path` | * | -- | Filesystem path to the JSON file (relative to CWD). Mutually exclusive with `from`. |
| `from` | * | -- | Connection-based file location (`connection` + `path`). Mutually exclusive with `path`. |
| `data_path` | no | `null` | Dot-notation path to the array of records within the JSON document. When `null` or empty, the top-level value must be a JSON array. |
| `batch_size` | no | global | Override the global `config.batch_size` for this source. |
| `sort_glob` | no | `name` | Sort order for glob matches: `name` (ascending, default) or `name_desc` (descending). Only used when `path` contains glob characters. |
| `normalize_columns` | no | `false` | Lowercase all column names after reading. |
| `exclude` | no | `[]` | List of column names to drop immediately after reading. |
| `schema` | no | -- | Unified schema block. `schema.arrow.columns` overrides inferred Arrow types after reading. See [Universal Schema Options](../universal-schema-options.md). |

\* Exactly one of `path` or `from` must be provided.

## Glob patterns

The `path` field supports glob patterns to read multiple files at once. All matched files are processed sequentially as a single logical source, producing a unified stream of `RecordBatch`es.

| Pattern | Meaning |
|---------|---------|
| `*` | Matches any sequence of non-`/` characters |
| `?` | Matches any single non-`/` character |
| `[abc]` | Character class |
| `[a-z]` | Character range |
| `[!0-9]` | Negated class |
| `**` | Recursive: matches zero or more directory levels |

```yaml
# All JSON files in a directory
- id: all_data
  type: read_json
  path: data/export_*.json
  data_path: records

# Recursive: search subdirectories
- id: deep_search
  type: read_json
  path: incoming/**/data_*.json

# Descending alphabetical order (newest-named files first)
- id: newest_first
  type: read_json
  path: data/snapshot_*.json
  sort_glob: name_desc
```

When `path` contains no glob characters, the step reads a single file (backwards compatible).

## How `data_path` works

The file is loaded into memory as a single `serde_json::Value`. If `data_path` is set, the reader navigates into the JSON structure using dot-separated keys.

```json
{
  "meta": { "count": 3 },
  "data": {
    "candidates": [
      { "id": 1, "name": "Alice" },
      { "id": 2, "name": "Bob" }
    ]
  }
}
```

| `data_path` value | What is read |
|---|---|
| `null` / omitted | Top-level value (must be an array) |
| `data.candidates` | The array at `data.candidates` |
| `meta` | Error -- `meta` is an object, not an array |

## Schema inference

All columns are initially inferred as their JSON type:

| JSON type | Arrow type |
|---|---|
| integer | `Int64` |
| float | `Float64` |
| string | `Utf8` |
| boolean | `Boolean` |
| null | `Null` |
| object | `Utf8` (JSON-serialized) |
| array | `Utf8` (JSON-serialized) |

Use `schema.arrow.columns` to override inferred types when needed.

## Examples

### Top-level array

```yaml
# data/users.json: [{"id": 1, "name": "Alice"}, ...]
- id: users
  type: read_json
  path: data/users.json
```

### Nested data path

```yaml
# data/response.json: {"results": [{"id": 1}, ...]}
- id: records
  type: read_json
  path: data/response.json
  data_path: results
```

### Combined with unnest for nested JSON normalization

```yaml
steps:
  - id: raw
    type: read_json
    path: data/candidates.json
    data_path: candidates

  - id: pools_exploded
    type: unnest
    input: raw
    column: talent_pools
    parent_fields:
      candidate_id: id
    fields:
      pool_id: id
      pool_name: name
```

## Error handling

| Condition | Behavior |
|---|---|
| File not found | Error: `read_json: cannot read file '<path>'` |
| Invalid JSON | Error: `read_json: JSON parse error in '<path>'` |
| `data_path` points to non-array | Error: `read_json: expected JSON array at data_path '<path>', got <type>` |
| Empty array | Returns an empty `RecordBatch` with an empty schema |

## See also

- [read_csv](./read-csv.md) — CSV file source
- [unnest](./unnest.md) — Explode array columns into rows
- [flatten](./flatten.md) — Extract nested fields into columns
- [write_json](./write-json.md) — JSON file sink
- [File connections](../connections/file-connections.md) — Remote storage setup
- [Transport Crates & Feature Flags](../feature-flags.md) — Remote transports require `transport-*` features at build time