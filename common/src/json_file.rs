//! JSON file source — reads a local JSON file and converts rows to Arrow
//! `RecordBatch`es.
//!
//! ## Usage
//!
//! ```yaml
//! - id: candidates
//!   type: read_json
//!   path: data/candidates.json
//!   data_path: candidates        # optional: dot-notation path to the array
//! ```
//!
//! The file is read into memory and parsed as a single `serde_json::Value`.
//! If `data_path` is set, the value at that path is expected to be an array
//! of objects.  If `data_path` is absent or empty, the top-level value must
//! be an array.
//!
//! ## Transport-agnostic API
//!
//! [`read_json_bytes`] and [`write_json_to_bytes`] accept/return raw byte
//! slices so that any [`FileTransport`](crate::file_transport::FileTransport)
//! implementation can feed data into the JSON pipeline without touching the
//! local filesystem.  The path-based functions ([`read_json_file`] /
//! [`write_json_file`]) are thin wrappers that handle local file I/O.

use arrow::record_batch::RecordBatch;

use crate::util::arrow::json_rows_to_record_batch;

// ── Write config ─────────────────────────────────────────────────────────────

/// Configuration for the JSON file sink.
#[derive(Debug, Clone)]
pub struct JsonWriteConfig {
    /// Filesystem path for the output JSON file.
    pub path: String,
    /// Pretty-print the JSON output (default: `false`).
    pub pretty: bool,
    /// Wrap the array in `{"<key>": [...]}`.  `None` = bare array.
    pub wrap_key: Option<String>,
}

impl Default for JsonWriteConfig {
    fn default() -> Self {
        Self { path: String::new(), pretty: false, wrap_key: None }
    }
}

// ── Read ─────────────────────────────────────────────────────────────────────

/// Read a JSON file from disk and convert it to `RecordBatch`es.
///
/// - `path`:       Filesystem path to the JSON file.
/// - `data_path`:  Optional dot-notation path to the array of rows within the
///                 JSON document.  `None` or `""` means the top-level value is
///                 the array.
/// - `batch_size`: Maximum rows per `RecordBatch`.
pub fn read_json_file(
    path:       &str,
    data_path:  Option<&str>,
    batch_size: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    let contents = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("read_json: cannot read file '{path}': {e}"))?;

    let batches = read_json_bytes(&contents, data_path, batch_size)?;

    tracing::info!(
        file = %path,
        rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        batches = batches.len(),
        "read_json: loaded {} rows in {} batch(es)",
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        batches.len(),
    );

    Ok(batches)
}

/// Parse JSON bytes and convert to `RecordBatch`es.
///
/// Transport-agnostic: accepts raw bytes (e.g. from
/// [`FileTransport::read_bytes`](crate::file_transport::FileTransport::read_bytes)).
///
/// See [`read_json_file`] for parameter documentation.
pub fn read_json_bytes(
    data:       &[u8],
    data_path:  Option<&str>,
    batch_size: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    let contents = std::str::from_utf8(data)
        .map_err(|e| anyhow::anyhow!("read_json: invalid UTF-8: {e}"))?;

    let root: serde_json::Value = serde_json::from_str(contents)
        .map_err(|e| anyhow::anyhow!("read_json: JSON parse error: {e}"))?;

    let rows_val = extract_data_path(&root, data_path);

    let rows = match rows_val {
        serde_json::Value::Array(arr) => arr,
        serde_json::Value::Null => {
            return Ok(vec![RecordBatch::new_empty(
                std::sync::Arc::new(arrow::datatypes::Schema::empty()),
            )]);
        }
        other => {
            let dp_msg = data_path
                .filter(|s| !s.is_empty())
                .map(|s| format!(" at data_path '{s}'"))
                .unwrap_or_default();
            anyhow::bail!(
                "read_json: expected JSON array{dp_msg}, got {}",
                json_type_name(&other)
            );
        }
    };

    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(
            std::sync::Arc::new(arrow::datatypes::Schema::empty()),
        )]);
    }

    let effective = batch_size.max(1);
    let mut batches = Vec::new();
    for chunk in rows.chunks(effective) {
        batches.push(json_rows_to_record_batch(chunk));
    }

    Ok(batches)
}

// ── Write ────────────────────────────────────────────────────────────────────

/// Write `RecordBatch`es to a JSON file.
///
/// Each row becomes a JSON object with column names as keys.  All batches
/// are collected into a single JSON array.  Parent directories are created
/// automatically.
pub fn write_json_file(
    batches: &[RecordBatch],
    config:  &JsonWriteConfig,
) -> anyhow::Result<usize> {
    let bytes = write_json_to_bytes(batches, config.pretty, config.wrap_key.as_deref())?;
    let total = batches.iter().map(|b| b.num_rows()).sum::<usize>();

    if let Some(parent) = std::path::Path::new(&config.path).parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("write_json: cannot create directory: {e}"))?;
    }

    std::fs::write(&config.path, &bytes)
        .map_err(|e| anyhow::anyhow!("write_json: cannot write '{}': {e}", config.path))?;

    tracing::info!(
        file = %config.path,
        rows = total,
        "write_json: wrote {} rows to {}",
        total,
        config.path,
    );

    Ok(total)
}

/// Serialize `RecordBatch`es to JSON bytes.
///
/// Transport-agnostic: returns raw bytes for
/// [`FileTransport::write_bytes`](crate::file_transport::FileTransport::write_bytes).
pub fn write_json_to_bytes(
    batches:  &[RecordBatch],
    pretty:   bool,
    wrap_key: Option<&str>,
) -> anyhow::Result<Vec<u8>> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let mut rows: Vec<serde_json::Value> = Vec::new();

    for batch in batches {
        let schema = batch.schema();
        let opts = FormatOptions::default();
        let formatters: Vec<Option<ArrayFormatter>> = (0..batch.num_columns())
            .map(|ci| ArrayFormatter::try_new(batch.column(ci).as_ref(), &opts).ok())
            .collect();

        for row_idx in 0..batch.num_rows() {
            let mut obj = serde_json::Map::new();
            for (ci, field) in schema.fields().iter().enumerate() {
                let val = if batch.column(ci).is_null(row_idx) {
                    serde_json::Value::Null
                } else if let Some(ref fmt) = formatters[ci] {
                    let s = fmt.value(row_idx).to_string();
                    // Try to parse as number/bool for cleaner JSON output.
                    if let Ok(n) = s.parse::<i64>() {
                        serde_json::Value::Number(n.into())
                    } else if let Ok(n) = s.parse::<f64>() {
                        serde_json::json!(n)
                    } else if s == "true" {
                        serde_json::Value::Bool(true)
                    } else if s == "false" {
                        serde_json::Value::Bool(false)
                    } else {
                        serde_json::Value::String(s)
                    }
                } else {
                    serde_json::Value::Null
                };
                obj.insert(field.name().clone(), val);
            }
            rows.push(serde_json::Value::Object(obj));
        }
    }

    let output: serde_json::Value = match wrap_key {
        Some(key) => serde_json::json!({ key: rows }),
        None      => serde_json::Value::Array(rows),
    };

    let json_bytes = if pretty {
        serde_json::to_vec_pretty(&output)?
    } else {
        serde_json::to_vec(&output)?
    };

    Ok(json_bytes)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn extract_data_path<'a>(
    value:     &'a serde_json::Value,
    data_path: Option<&str>,
) -> serde_json::Value {
    let path = match data_path {
        None | Some("") => return value.clone(),
        Some(p)         => p,
    };
    let mut current = value;
    for key in path.split('.') {
        match current.get(key) {
            Some(v) => current = v,
            None    => return serde_json::Value::Null,
        }
    }
    current.clone()
}

fn json_type_name(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::Null     => "null",
        serde_json::Value::Bool(_)  => "bool",
        serde_json::Value::Number(_)=> "number",
        serde_json::Value::String(_)=> "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_)=> "object",
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn write_temp(name: &str, content: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("potato_etl_test_json");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn test_read_json_file_with_data_path() {
        let file = write_temp("dp.json", r#"{"results": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}"#);
        let batches = read_json_file(file.to_str().unwrap(), Some("results"), 100).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
    }

    #[test]
    fn test_read_json_file_top_level_array() {
        let file = write_temp("top.json", r#"[{"id": 1}, {"id": 2}, {"id": 3}]"#);
        let batches = read_json_file(file.to_str().unwrap(), None, 2).unwrap();
        assert_eq!(batches.len(), 2); // 2 + 1
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
    }

    #[test]
    fn test_read_json_file_not_found() {
        let result = read_json_file("/nonexistent/file.json", None, 100);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot read file"));
    }

    #[test]
    fn test_read_json_file_not_array() {
        let file = write_temp("obj.json", r#"{"key": "value"}"#);
        let result = read_json_file(file.to_str().unwrap(), None, 100);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expected JSON array"));
    }

    #[test]
    fn test_write_json_file_bare_array() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};

        let dir = std::env::temp_dir().join("potato_etl_test_json");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("write_test.json");

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("id",   DataType::Int64, false),
            Field::new("name", DataType::Utf8,  false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            std::sync::Arc::new(Int64Array::from(vec![1, 2])) as _,
            std::sync::Arc::new(StringArray::from(vec!["Alice", "Bob"])) as _,
        ]).unwrap();

        let config = JsonWriteConfig {
            path: path.to_str().unwrap().to_string(),
            pretty: false,
            wrap_key: None,
        };
        let total = write_json_file(&[batch], &config).unwrap();
        assert_eq!(total, 2);

        // Read back and verify
        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_write_json_file_wrapped() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let dir = std::env::temp_dir().join("potato_etl_test_json");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("write_wrapped.json");

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            std::sync::Arc::new(Int64Array::from(vec![42])) as _,
        ]).unwrap();

        let config = JsonWriteConfig {
            path: path.to_str().unwrap().to_string(),
            pretty: true,
            wrap_key: Some("data".into()),
        };
        let total = write_json_file(&[batch], &config).unwrap();
        assert_eq!(total, 1);

        let contents = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
        assert!(parsed.get("data").unwrap().is_array());
    }
}