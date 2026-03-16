# write_parquet (sink)

Writes Arrow `RecordBatch`es to a Parquet file.

## Parameters

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `input` | `string` | yes | -- | Step ID to consume data from |
| `path` | `string` | yes* | -- | Filesystem path for the output file |
| `target.connection` | `string` | yes* | -- | Named file connection |
| `target.path` | `string` | yes* | -- | Path relative to connection's `base_path` |
| `compression` | `string` | no | `snappy` | Compression codec: `none`, `snappy`, `gzip`, `lz4`, `zstd` |

\* Supply either `path` (direct) or `target` (connection-based), not both.

## Compression codecs

| Codec | Ratio | Speed | Best for |
|---|---|---|---|
| `none` | 1x | fastest | Already-compressed data, debugging |
| `snappy` | ~2x | very fast | General purpose (default) |
| `gzip` | ~4x | moderate | Long-term archival, bandwidth-sensitive |
| `lz4` | ~2.5x | fast | Performance-sensitive workloads |
| `zstd` | ~4x | fast | Best balance of compression and speed |

## Examples

### Direct path (local filesystem)

```yaml
steps:
  - id: output
    type: write_parquet
    input: transformed
    path: output/result.parquet
    compression: zstd
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
    base_path: processed

steps:
  - id: output
    type: write_parquet
    input: enriched
    target:
      connection: data_lake
      path: events/2024/events.parquet
    compression: zstd
```

### Full pipeline: CSV -> transform -> Parquet

```yaml
config:
  batch_size: 10000

steps:
  - id: raw
    type: read_csv
    path: data/raw_events.csv

  - id: cleaned
    type: filter
    input: raw
    condition: "status != \"invalid\""

  - id: enriched
    type: map
    input: cleaned
    columns:
      ingested_at: now()
      event_date: date(created_at)

  - id: output
    type: write_parquet
    input: enriched
    path: output/events.parquet
    compression: zstd
```

## Notes

- All batches are collected in memory before writing to a single Parquet file.
- Parent directories are created automatically for local filesystem paths.
- Arrow schema is preserved exactly as-is in the Parquet file metadata.
- All 8 transport drivers are supported: local, SFTP, S3, Azure Blob, GCS, SharePoint, FTP, SMB. Remote transports require the corresponding `transport-*` feature flag at build time.
- Requires the `parquet` feature flag (enabled by default). See [Transport Crates & Feature Flags](../feature-flags.md).

## See also

- [read_parquet](./read-parquet.md) — Parquet file source
- [write_csv](./write-csv.md) / [write_json](./write-json.md) — Other file sinks
- [File connections](../connections/file-connections.md) — Remote storage setup
- [Transport Crates & Feature Flags](../feature-flags.md) — Compile-time feature configuration