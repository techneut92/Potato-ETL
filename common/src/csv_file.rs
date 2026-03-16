//! CSV file source and sink — read/write local CSV files as Arrow `RecordBatch`es.
//!
//! ## Source (`read_csv`)
//!
//! ```yaml
//! - id: employees
//!   type: read_csv
//!   path: data/employees.csv
//!   delimiter: ","        # optional (default: ",")
//!   has_header: true      # optional (default: true)
//! ```
//!
//! ## Sink (`write_csv`)
//!
//! ```yaml
//! - id: output
//!   type: write_csv
//!   input: transformed
//!   path: output/result.csv
//!   delimiter: ","        # optional (default: ",")
//!   has_header: true      # optional (default: true)
//! ```
//!
//! ## Transport-agnostic API
//!
//! [`read_csv_bytes`] and [`write_csv_to_bytes`] accept/return raw byte
//! slices so that any [`FileTransport`](crate::file_transport::FileTransport)
//! implementation can feed data into the CSV pipeline without touching the
//! local filesystem.

use std::sync::Arc;

use arrow::csv as arrow_csv;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

// ── Read ─────────────────────────────────────────────────────────────────────

/// Configuration for the CSV source.
#[derive(Debug, Clone)]
pub struct CsvReadConfig {
    /// Filesystem path to the CSV file.
    pub path: String,
    /// Column delimiter (default: `,`).
    pub delimiter: u8,
    /// Whether the first row is a header row (default: `true`).
    pub has_header: bool,
    /// Maximum rows per `RecordBatch`.
    pub batch_size: usize,
}

impl Default for CsvReadConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            delimiter: b',',
            has_header: true,
            batch_size: 1000,
        }
    }
}

/// Read a CSV file from disk and return as a list of `RecordBatch`es.
///
/// Schema is inferred from the file.  All columns are initially read as their
/// inferred Arrow type (Int64, Float64, Utf8, Boolean).
pub fn read_csv_file(config: &CsvReadConfig) -> anyhow::Result<Vec<RecordBatch>> {
    let data = std::fs::read(&config.path)
        .map_err(|e| anyhow::anyhow!("read_csv: cannot open '{}': {e}", config.path))?;

    let batches = read_csv_bytes(&data, config.delimiter, config.has_header, config.batch_size)?;

    tracing::info!(
        file = %config.path,
        rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        batches = batches.len(),
        "read_csv: loaded {} rows in {} batch(es)",
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        batches.len(),
    );

    Ok(batches)
}

/// Parse CSV bytes and convert to `RecordBatch`es.
///
/// Transport-agnostic: accepts raw bytes (e.g. from
/// [`FileTransport::read_bytes`](crate::file_transport::FileTransport::read_bytes)).
pub fn read_csv_bytes(
    data:       &[u8],
    delimiter:  u8,
    has_header: bool,
    batch_size: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    use std::io::Cursor;

    // Infer schema from the data (reads up to 100 rows for inference).
    let (schema, _) = arrow_csv::reader::Format::default()
        .with_delimiter(delimiter)
        .with_header(has_header)
        .infer_schema(Cursor::new(data), Some(100))?;

    let schema_ref: SchemaRef = Arc::new(schema);

    let csv_reader = arrow_csv::ReaderBuilder::new(Arc::clone(&schema_ref))
        .with_delimiter(delimiter)
        .with_header(has_header)
        .with_batch_size(batch_size)
        .build(Cursor::new(data))?;

    let mut batches = Vec::new();
    for result in csv_reader {
        let batch = result.map_err(|e| anyhow::anyhow!("read_csv: error reading batch: {e}"))?;
        batches.push(batch);
    }

    Ok(batches)
}

// ── Write ────────────────────────────────────────────────────────────────────

/// Configuration for the CSV sink.
#[derive(Debug, Clone)]
pub struct CsvWriteConfig {
    /// Filesystem path for the output CSV file.
    pub path: String,
    /// Column delimiter (default: `,`).
    pub delimiter: u8,
    /// Whether to write a header row (default: `true`).
    pub has_header: bool,
}

impl Default for CsvWriteConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            delimiter: b',',
            has_header: true,
        }
    }
}

/// Write `RecordBatch`es to a CSV file.
///
/// Creates or overwrites the file at `config.path`.  Parent directories are
/// created automatically.
pub fn write_csv_file(
    batches: &[RecordBatch],
    config:  &CsvWriteConfig,
) -> anyhow::Result<usize> {
    let bytes = write_csv_to_bytes(batches, config.delimiter, config.has_header)?;
    let total = batches.iter().map(|b| b.num_rows()).sum::<usize>();

    if let Some(parent) = std::path::Path::new(&config.path).parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("write_csv: cannot create directory: {e}"))?;
    }

    std::fs::write(&config.path, &bytes)
        .map_err(|e| anyhow::anyhow!("write_csv: cannot write '{}': {e}", config.path))?;

    tracing::info!(
        file = %config.path,
        rows = total,
        "write_csv: wrote {} rows to {}",
        total,
        config.path,
    );

    Ok(total)
}

/// Serialize `RecordBatch`es to CSV bytes.
///
/// Transport-agnostic: returns raw bytes for
/// [`FileTransport::write_bytes`](crate::file_transport::FileTransport::write_bytes).
pub fn write_csv_to_bytes(
    batches:    &[RecordBatch],
    delimiter:  u8,
    has_header: bool,
) -> anyhow::Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let mut buf = Vec::new();
    {
        let mut csv_writer = arrow_csv::WriterBuilder::new()
            .with_delimiter(delimiter)
            .with_header(has_header)
            .build(&mut buf);

        for batch in batches {
            csv_writer.write(batch)
                .map_err(|e| anyhow::anyhow!("write_csv: error writing batch: {e}"))?;
        }
    }

    Ok(buf)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_roundtrip_csv() {
        let dir = std::env::temp_dir().join("potato_etl_test_csv");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("roundtrip.csv");

        // Write
        let schema = Arc::new(Schema::new(vec![
            Field::new("id",   DataType::Int64, false),
            Field::new("name", DataType::Utf8,  false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as _,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as _,
        ]).unwrap();

        let write_config = CsvWriteConfig {
            path: path.to_str().unwrap().to_string(),
            delimiter: b',',
            has_header: true,
        };
        let rows_written = write_csv_file(&[batch], &write_config).unwrap();
        assert_eq!(rows_written, 3);

        // Read back
        let read_config = CsvReadConfig {
            path: path.to_str().unwrap().to_string(),
            delimiter: b',',
            has_header: true,
            batch_size: 100,
        };
        let batches = read_csv_file(&read_config).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_read_csv_not_found() {
        let config = CsvReadConfig {
            path: "/nonexistent/file.csv".into(),
            ..Default::default()
        };
        assert!(read_csv_file(&config).is_err());
    }

    #[test]
    fn test_write_csv_creates_dirs() {
        let dir = std::env::temp_dir().join("potato_etl_test_csv/nested/deep");
        let path = dir.join("output.csv");

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![42])) as _,
        ]).unwrap();

        let config = CsvWriteConfig {
            path: path.to_str().unwrap().to_string(),
            ..Default::default()
        };
        let rows = write_csv_file(&[batch], &config).unwrap();
        assert_eq!(rows, 1);
        assert!(path.exists());
    }
}