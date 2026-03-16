# read_parquet (source)

Reads a Parquet file and converts it to Arrow `RecordBatch`es.

## Parameters

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `path` | `string` | yes* | -- | Filesystem path to the Parquet file |
| `from.connection` | `string` | yes* | -- | Named file connection |
| `from.path` | `string` | yes* | -- | Path relative to connection's `base_path` |
| `columns` | `string[]` | no | all | Column projection -- only read these columns |
| `batch_size` | `integer` | no | `config.batch_size` | Override global batch size for this source |
| `schema` | `object` | no | -- | Unified schema block (arrow overrides, etc.) |
| `normalize_columns` | `boolean` | no | `false` | Lowercase all column names |
| `exclude` | `string[]` | no | -- | Columns to exclude from output |
| `sort_glob` | `string` | no | `name` | Sort order for glob matches: `name` (ascending) or `name_desc` (descending) |

\* Supply either `path` (direct) or `from` (connection-based), not both.

## Glob patterns

The `path` field supports glob patterns to read multiple Parquet files at once. All matched files are processed sequentially as a single logical source.

| Pattern | Meaning |
|---------|---------|
| `*` | Matches any sequence of non-`/` characters |
| `?` | Matches any single non-`/` character |
| `[abc]` | Character class |
| `[a-z]` | Character range |
| `[!0-9]` | Negated class |
| `**` | Recursive: matches zero or more directory levels |

```yaml
# All Parquet files in a directory
- id: all_events
  type: read_parquet
  path: lake/events_*.parquet

# Recursive: search subdirectories
- id: deep_search
  type: read_parquet
  path: data/**/part_*.parquet

# Descending alphabetical order (newest-named files first)
- id: newest_first
  type: read_parquet
  path: lake/events_*.parquet
  sort_glob: name_desc
```

When `path` contains no glob characters, the step reads a single file (backwards compatible).

## Examples

### Direct path (local filesystem)

```yaml
steps:
  - id: events
    type: read_parquet
    path: data/events.parquet
    batch_size: 8192
```

### Connection-based (S3)

```yaml
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
    type: read_parquet
    from:
      connection: data_lake
      path: events/2024/events.parquet
    columns: [event_id, user_id, event_type, created_at]
```

### With schema overrides

```yaml
steps:
  - id: events
    type: read_parquet
    path: data/events.parquet
    normalize_columns: true
    exclude:
      - internal_metadata
    schema:
      arrow:
        columns:
          event_id:
            type: Utf8
```

## Notes

- **Streaming reader**: Internally, the Parquet reader uses `ParquetStreamReader` which reads row groups lazily. This keeps peak memory proportional to `batch_size` rather than total file size — even multi-GB files are processed efficiently.
- **Column projection** (`columns`) is applied at the Parquet reader level, avoiding I/O for unused columns. This can dramatically reduce read time for wide tables.
- Schema inference is automatic from the Parquet file's embedded schema — no manual schema configuration needed for most use cases.
- All 8 transport drivers are supported: local, SFTP, S3, Azure Blob, GCS, SharePoint, FTP, SMB. Remote transports require the corresponding `transport-*` feature flag at build time.
- Requires the `parquet` feature flag (enabled by default). See [Transport Crates & Feature Flags](../feature-flags.md).

## See also

- [write_parquet](./write-parquet.md) — Parquet file sink
- [read_csv](./read-csv.md) / [read_json](./read-json.md) — Other file sources
- [File connections](../connections/file-connections.md) — Remote storage setup
- [Transport Crates & Feature Flags](../feature-flags.md) — Compile-time feature configuration