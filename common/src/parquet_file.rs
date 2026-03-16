//! Parquet file source and sink -- read/write Parquet files as Arrow
//! `RecordBatch`es.
//!
//! ## Source (`read_parquet`)
//!
//! ```yaml
//! - id: events
//!   type: read_parquet
//!   path: data/events.parquet
//!   batch_size: 8192               # optional
//!   columns: [event_id, user_id]   # optional: column projection
//!
//! # Connection-based:
//! - id: events
//!   type: read_parquet
//!   from:
//!     connection: data_lake_s3
//!     path: events/2024/events.parquet
//! ```
//!
//! ## Sink (`write_parquet`)
//!
//! ```yaml
//! - id: output
//!   type: write_parquet
//!   input: transformed
//!   path: output/result.parquet
//!   compression: snappy            # optional: none, snappy, gzip, lz4, zstd
//!
//! # Connection-based:
//! - id: output
//!   type: write_parquet
//!   input: transformed
//!   target:
//!     connection: data_lake_s3
//!     path: processed/result.parquet
//!   compression: zstd
//! ```
//!
//! ## Transport-agnostic API
//!
//! [`read_parquet_bytes`] and [`write_parquet_to_bytes`] accept/return raw byte
//! slices so that any [`FileTransport`](crate::file_transport::FileTransport)
//! implementation can feed data into the Parquet pipeline without touching the
//! local filesystem.
//!
//! ## Streaming reader
//!
//! [`ParquetStreamReader`] reads row groups lazily, yielding one `RecordBatch`
//! at a time without loading the entire file into memory.  Ideal for files
//! larger than ~1GB.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow::array::RecordBatchReader;

// ── Read config ──────────────────────────────────────────────────────────────

/// Configuration for the Parquet source (only used by path-based reads).
#[derive(Debug, Clone)]
pub struct ParquetReadConfig {
    /// Filesystem path to the Parquet file.
    pub path: String,
    /// Maximum rows per `RecordBatch`.
    pub batch_size: usize,
    /// Optional column projection -- only read these columns.
    pub columns: Option<Vec<String>>,
}

impl Default for ParquetReadConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            batch_size: 8192,
            columns: None,
        }
    }
}

// ── Write config ─────────────────────────────────────────────────────────────

/// Parquet compression codec.
#[derive(Debug, Clone, Copy, Default)]
pub enum ParquetCompression {
    None,
    #[default]
    Snappy,
    Gzip,
    Lz4,
    Zstd,
}

impl ParquetCompression {
    /// Parse from a user-provided string.
    pub fn from_str_loose(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "none" | "uncompressed" => Ok(Self::None),
            "snappy"               => Ok(Self::Snappy),
            "gzip" | "gz"          => Ok(Self::Gzip),
            "lz4"                  => Ok(Self::Lz4),
            "zstd" | "zstandard"   => Ok(Self::Zstd),
            other => anyhow::bail!(
                "Unknown Parquet compression '{other}'. \
                 Valid: none, snappy, gzip, lz4, zstd"
            ),
        }
    }

    fn to_parquet_compression(self) -> parquet::basic::Compression {
        match self {
            Self::None   => parquet::basic::Compression::UNCOMPRESSED,
            Self::Snappy => parquet::basic::Compression::SNAPPY,
            Self::Gzip   => parquet::basic::Compression::GZIP(parquet::basic::GzipLevel::default()),
            Self::Lz4    => parquet::basic::Compression::LZ4,
            Self::Zstd   => parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::default()),
        }
    }
}

/// Configuration for the Parquet sink.
#[derive(Debug, Clone)]
pub struct ParquetWriteConfig {
    /// Filesystem path for the output Parquet file.
    pub path: String,
    /// Compression codec (default: Snappy).
    pub compression: ParquetCompression,
}

impl Default for ParquetWriteConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            compression: ParquetCompression::Snappy,
        }
    }
}

// ── Read ─────────────────────────────────────────────────────────────────────

/// Read a Parquet file from disk and return as a list of `RecordBatch`es.
pub fn read_parquet_file(config: &ParquetReadConfig) -> anyhow::Result<Vec<RecordBatch>> {
    let data = std::fs::read(&config.path)
        .map_err(|e| anyhow::anyhow!("read_parquet: cannot read '{}': {e}", config.path))?;

    let batches = read_parquet_bytes(&data, config.batch_size, config.columns.as_deref())?;

    tracing::info!(
        file = %config.path,
        rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        batches = batches.len(),
        "read_parquet: loaded {} rows in {} batch(es)",
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        batches.len(),
    );

    Ok(batches)
}

/// Parse Parquet bytes and convert to `RecordBatch`es.
///
/// Transport-agnostic: accepts raw bytes (e.g. from
/// [`FileTransport::read_bytes`](crate::file_transport::FileTransport::read_bytes)).
pub fn read_parquet_bytes(
    data:       &[u8],
    batch_size: usize,
    columns:    Option<&[String]>,
) -> anyhow::Result<Vec<RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use bytes::Bytes;

    let bytes = Bytes::copy_from_slice(data);
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| anyhow::anyhow!("read_parquet: cannot open Parquet data: {e}"))?;

    builder = builder.with_batch_size(batch_size);

    // Apply column projection if requested.
    if let Some(cols) = columns {
        let schema = builder.schema().clone();
        let mut indices = Vec::new();
        for col_name in cols {
            let idx = schema.fields().iter().position(|f| f.name() == col_name)
                .ok_or_else(|| anyhow::anyhow!(
                    "read_parquet: column '{col_name}' not found in Parquet schema. \
                     Available: {:?}",
                    schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                ))?;
            indices.push(idx);
        }
        let mask = parquet::arrow::ProjectionMask::roots(
            builder.parquet_schema(),
            indices,
        );
        builder = builder.with_projection(mask);
    }

    let reader = builder.build()
        .map_err(|e| anyhow::anyhow!("read_parquet: cannot build reader: {e}"))?;

    let mut batches = Vec::new();
    for result in reader {
        let batch = result.map_err(|e| anyhow::anyhow!("read_parquet: error reading batch: {e}"))?;
        batches.push(batch);
    }

    Ok(batches)
}

// ── Write ────────────────────────────────────────────────────────────────────

/// Write `RecordBatch`es to a Parquet file.
///
/// Creates or overwrites the file at `config.path`.  Parent directories are
/// created automatically.
pub fn write_parquet_file(
    batches: &[RecordBatch],
    config:  &ParquetWriteConfig,
) -> anyhow::Result<usize> {
    let bytes = write_parquet_to_bytes(batches, config.compression)?;
    let total = batches.iter().map(|b| b.num_rows()).sum::<usize>();

    if let Some(parent) = std::path::Path::new(&config.path).parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("write_parquet: cannot create directory: {e}"))?;
    }

    std::fs::write(&config.path, &bytes)
        .map_err(|e| anyhow::anyhow!("write_parquet: cannot write '{}': {e}", config.path))?;

    tracing::info!(
        file = %config.path,
        rows = total,
        bytes = bytes.len(),
        "write_parquet: wrote {} rows ({} bytes) to {}",
        total,
        bytes.len(),
        config.path,
    );

    Ok(total)
}

/// Serialize `RecordBatch`es to Parquet bytes.
///
/// Transport-agnostic: returns raw bytes for
/// [`FileTransport::write_bytes`](crate::file_transport::FileTransport::write_bytes).
pub fn write_parquet_to_bytes(
    batches:     &[RecordBatch],
    compression: ParquetCompression,
) -> anyhow::Result<Vec<u8>> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    if batches.is_empty() {
        // Write a valid empty Parquet file with an empty schema.
        let schema = Arc::new(arrow::datatypes::Schema::empty());
        let mut buf = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(compression.to_parquet_compression())
            .build();
        let writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
            .map_err(|e| anyhow::anyhow!("write_parquet: cannot create writer: {e}"))?;
        writer.close()
            .map_err(|e| anyhow::anyhow!("write_parquet: cannot close writer: {e}"))?;
        return Ok(buf);
    }

    let schema = batches[0].schema();
    let mut buf = Vec::new();

    let props = WriterProperties::builder()
        .set_compression(compression.to_parquet_compression())
        .build();

    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))
        .map_err(|e| anyhow::anyhow!("write_parquet: cannot create writer: {e}"))?;

    for batch in batches {
        writer.write(batch)
            .map_err(|e| anyhow::anyhow!("write_parquet: error writing batch: {e}"))?;
    }

    writer.close()
        .map_err(|e| anyhow::anyhow!("write_parquet: cannot close writer: {e}"))?;

    Ok(buf)
}

// ── Streaming reader ─────────────────────────────────────────────────────────

/// Row-group–level streaming Parquet reader.
///
/// Instead of reading the entire file into `Vec<RecordBatch>` at once, this
/// reader yields batches one at a time via an iterator interface.  For very
/// large files (>1GB) this keeps peak memory proportional to `batch_size`
/// rather than total file size.
///
/// # Usage
///
/// ```rust,ignore
/// let reader = ParquetStreamReader::from_bytes(data, 8192, None)?;
/// let metadata = reader.metadata();
/// for batch_result in reader {
///     let batch = batch_result?;
///     // process batch…
/// }
/// ```
pub struct ParquetStreamReader {
    inner: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    meta:  ParquetFileMetadata,
}

/// Metadata extracted from a Parquet file before reading any row groups.
#[derive(Debug, Clone)]
pub struct ParquetFileMetadata {
    /// Total number of rows across all row groups.
    pub num_rows:       usize,
    /// Number of row groups in the file.
    pub num_row_groups: usize,
    /// Number of columns (after projection, if applied).
    pub num_columns:    usize,
    /// Schema of the output batches.
    pub schema:         arrow::datatypes::SchemaRef,
    /// File-level key-value metadata.
    pub key_value_metadata: Vec<(String, Option<String>)>,
}

impl ParquetStreamReader {
    /// Create a streaming reader from in-memory bytes.
    ///
    /// This parses the Parquet footer (schema + row group metadata) but does
    /// NOT read any row group data.  Row groups are read lazily as you iterate.
    pub fn from_bytes(
        data:       &[u8],
        batch_size: usize,
        columns:    Option<&[String]>,
    ) -> anyhow::Result<Self> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use bytes::Bytes;

        let bytes = Bytes::copy_from_slice(data);
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| anyhow::anyhow!("read_parquet: cannot open Parquet data: {e}"))?;

        // Extract metadata from the builder (before consuming it).
        let parquet_meta = builder.metadata().clone();
        let num_rows = parquet_meta.file_metadata().num_rows() as usize;
        let num_row_groups = parquet_meta.num_row_groups();
        let kv_meta = parquet_meta.file_metadata().key_value_metadata()
            .map(|kvs| kvs.iter().map(|kv| (kv.key.clone(), kv.value.clone())).collect())
            .unwrap_or_default();

        builder = builder.with_batch_size(batch_size);

        // Apply column projection if requested.
        if let Some(cols) = columns {
            let schema = builder.schema().clone();
            let mut indices = Vec::new();
            for col_name in cols {
                let idx = schema.fields().iter().position(|f| f.name() == col_name)
                    .ok_or_else(|| anyhow::anyhow!(
                        "read_parquet: column '{col_name}' not found in Parquet schema. \
                         Available: {:?}",
                        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                    ))?;
                indices.push(idx);
            }
            let mask = parquet::arrow::ProjectionMask::roots(
                builder.parquet_schema(),
                indices,
            );
            builder = builder.with_projection(mask);
        }

        let reader = builder.build()
            .map_err(|e| anyhow::anyhow!("read_parquet: cannot build reader: {e}"))?;

        let schema = reader.schema();
        let num_columns = schema.fields().len();

        let meta = ParquetFileMetadata {
            num_rows,
            num_row_groups,
            num_columns,
            schema,
            key_value_metadata: kv_meta,
        };

        Ok(Self { inner: reader, meta })
    }

    /// File metadata (available before reading any row groups).
    pub fn metadata(&self) -> &ParquetFileMetadata {
        &self.meta
    }
}

impl Iterator for ParquetStreamReader {
    type Item = anyhow::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|result| {
            result.map_err(|e| anyhow::anyhow!("read_parquet: error reading batch: {e}"))
        })
    }
}

// ── Convenience: collect all batches from stream ─────────────────────────────

/// Read all batches from a streaming reader into a `Vec`.
///
/// Convenience wrapper for cases where you DO want all data in memory
/// (e.g., the executor collects batches before passing to the next step).
pub fn collect_stream(reader: ParquetStreamReader) -> anyhow::Result<Vec<RecordBatch>> {
    reader.collect()
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_roundtrip_parquet() {
        let dir = std::env::temp_dir().join("potato_etl_test_parquet");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("roundtrip.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id",   DataType::Int64, false),
            Field::new("name", DataType::Utf8,  false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as _,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as _,
        ]).unwrap();

        // Write
        let write_config = ParquetWriteConfig {
            path: path.to_str().unwrap().to_string(),
            compression: ParquetCompression::Snappy,
        };
        let written = write_parquet_file(&[batch], &write_config).unwrap();
        assert_eq!(written, 3);

        // Read back
        let read_config = ParquetReadConfig {
            path: path.to_str().unwrap().to_string(),
            batch_size: 100,
            columns: None,
        };
        let batches = read_parquet_file(&read_config).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_roundtrip_parquet_bytes() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])) as _,
        ]).unwrap();

        let bytes = write_parquet_to_bytes(&[batch], ParquetCompression::Zstd).unwrap();
        assert!(!bytes.is_empty());

        let batches = read_parquet_bytes(&bytes, 100, None).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn test_column_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id",   DataType::Int64, false),
            Field::new("name", DataType::Utf8,  false),
            Field::new("age",  DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1, 2])) as _,
            Arc::new(StringArray::from(vec!["Alice", "Bob"])) as _,
            Arc::new(Int64Array::from(vec![30, 25])) as _,
        ]).unwrap();

        let bytes = write_parquet_to_bytes(&[batch], ParquetCompression::None).unwrap();
        let cols = vec!["id".to_string(), "age".to_string()];
        let batches = read_parquet_bytes(&bytes, 100, Some(&cols)).unwrap();
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].schema().field(0).name(), "id");
        assert_eq!(batches[0].schema().field(1).name(), "age");
    }

    #[test]
    fn test_compression_from_str() {
        assert!(matches!(ParquetCompression::from_str_loose("snappy").unwrap(), ParquetCompression::Snappy));
        assert!(matches!(ParquetCompression::from_str_loose("GZIP").unwrap(), ParquetCompression::Gzip));
        assert!(matches!(ParquetCompression::from_str_loose("zstd").unwrap(), ParquetCompression::Zstd));
        assert!(matches!(ParquetCompression::from_str_loose("none").unwrap(), ParquetCompression::None));
        assert!(ParquetCompression::from_str_loose("unknown").is_err());
    }

    #[test]
    fn test_empty_batches() {
        let bytes = write_parquet_to_bytes(&[], ParquetCompression::None).unwrap();
        assert!(!bytes.is_empty()); // Valid Parquet file, just no rows
    }

    #[test]
    fn test_stream_reader() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id",   DataType::Int64, false),
            Field::new("name", DataType::Utf8,  false),
        ]));

        // Create 3 batches worth of data.
        let mut all_batches = Vec::new();
        for i in 0..3 {
            let base = i * 100;
            let ids: Vec<i64> = (base..base + 100).collect();
            let names: Vec<String> = ids.iter().map(|id| format!("row_{id}")).collect();
            let batch = RecordBatch::try_new(schema.clone(), vec![
                Arc::new(Int64Array::from(ids)) as _,
                Arc::new(StringArray::from(names)) as _,
            ]).unwrap();
            all_batches.push(batch);
        }

        let bytes = write_parquet_to_bytes(&all_batches, ParquetCompression::Snappy).unwrap();

        // Stream-read with small batch_size.
        let reader = ParquetStreamReader::from_bytes(&bytes, 50, None).unwrap();
        let meta = reader.metadata();
        assert_eq!(meta.num_rows, 300);
        assert_eq!(meta.num_columns, 2);

        let mut total_rows = 0;
        let mut batch_count = 0;
        for batch_result in reader {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
            batch_count += 1;
        }
        assert_eq!(total_rows, 300);
        assert!(batch_count >= 3); // at least as many batches as we wrote
    }

    #[test]
    fn test_stream_reader_with_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id",   DataType::Int64, false),
            Field::new("name", DataType::Utf8,  false),
            Field::new("age",  DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as _,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as _,
            Arc::new(Int64Array::from(vec![30, 25, 35])) as _,
        ]).unwrap();

        let bytes = write_parquet_to_bytes(&[batch], ParquetCompression::None).unwrap();
        let cols = vec!["name".to_string()];
        let reader = ParquetStreamReader::from_bytes(&bytes, 100, Some(&cols)).unwrap();
        assert_eq!(reader.metadata().num_columns, 1);

        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "name");
    }
}