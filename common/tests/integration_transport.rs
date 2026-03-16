//! Integration tests for the file transport abstraction.
//!
//! Tests the `FileTransport` trait, `LocalTransport`, the `TransportRegistry`,
//! and Parquet streaming reader.  Remote transport tests live in each transport
//! crate's own test suite.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use potato_etl_common::config::ConnParams;
use potato_etl_common::file_transport::{FileTransport, LocalTransport, create_transport};

// ── Helper ───────────────────────────────────────────────────────────────────

fn make_test_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id",   DataType::Int64, false),
        Field::new("name", DataType::Utf8,  false),
    ]));
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("row_{i}")).collect();
    RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)) as _,
        Arc::new(StringArray::from(names)) as _,
    ]).unwrap()
}

// ── Local transport ──────────────────────────────────────────────────────────

mod local {
    use super::*;

    #[tokio::test]
    async fn test_write_read_delete_cycle() {
        let dir = std::env::temp_dir()
            .join("potato_etl_integration")
            .join(format!("local_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test_cycle.bin");
        let path_str = path.to_str().unwrap();

        let transport = LocalTransport;

        assert!(!transport.exists(path_str).await.unwrap());

        let data = b"integration test data";
        transport.write_bytes(path_str, data).await.unwrap();
        assert!(transport.exists(path_str).await.unwrap());

        let read = transport.read_bytes(path_str).await.unwrap();
        assert_eq!(read, data);

        let entries = transport.list(dir.to_str().unwrap()).await.unwrap();
        assert!(entries.contains(&"test_cycle.bin".to_string()));

        transport.delete(path_str).await.unwrap();
        assert!(!transport.exists(path_str).await.unwrap());
    }

    #[tokio::test]
    async fn test_nested_directory_creation() {
        let dir = std::env::temp_dir()
            .join("potato_etl_integration")
            .join(format!("local_nested_{}", std::process::id()))
            .join("a").join("b").join("c");
        let path = dir.join("deep.txt");
        let path_str = path.to_str().unwrap();

        let transport = LocalTransport;
        transport.write_bytes(path_str, b"deep data").await.unwrap();
        assert!(path.exists());
    }

    #[tokio::test]
    async fn test_overwrite_existing_file() {
        let dir = std::env::temp_dir()
            .join("potato_etl_integration")
            .join(format!("local_overwrite_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("overwrite.txt");
        let path_str = path.to_str().unwrap();

        let transport = LocalTransport;
        transport.write_bytes(path_str, b"version 1").await.unwrap();
        transport.write_bytes(path_str, b"version 2").await.unwrap();

        let read = transport.read_bytes(path_str).await.unwrap();
        assert_eq!(read, b"version 2");
    }

    #[tokio::test]
    async fn test_read_nonexistent_file_errors() {
        let transport = LocalTransport;
        let result = transport.read_bytes("/nonexistent/path/file.txt").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_transport_from_conn_params() {
        let conn = ConnParams::Local { base_path: "/tmp".into() };
        let transport = create_transport(&conn).unwrap();
        assert!(transport.describe().contains("local"));
    }
}

// ── Transport registry ───────────────────────────────────────────────────────

mod registry {
    use super::*;
    use potato_etl_common::config::{DbAuth, FileAuth};

    #[test]
    fn test_database_conn_rejected() {
        let conn = ConnParams::Postgres {
            host: "localhost".into(),
            port: None,
            database: "test".into(),
            auth: DbAuth::None,
            options: Default::default(),
        };
        let err = create_transport(&conn).err().expect("expected error for database conn");
        assert!(err.to_string().contains("not a file-transport driver"));
    }

    #[test]
    fn test_unregistered_remote_driver() {
        // Without registering any transport factories, remote drivers should
        // give a clear error about missing transport crate.
        let conn = ConnParams::Sftp {
            host: "localhost".into(),
            port: 22,
            auth: FileAuth::None,
            host_key: None,
            base_path: String::new(),
        };
        let err = create_transport(&conn).err().expect("expected error for unregistered driver");
        let msg = err.to_string();
        assert!(
            msg.contains("No transport registered") || msg.contains("transport crate"),
            "error should mention missing transport: {msg}"
        );
    }
}

// ── Parquet streaming reader ─────────────────────────────────────────────────

#[cfg(feature = "parquet")]
mod parquet_stream {
    use super::*;
    use potato_etl_common::parquet_file::*;

    #[tokio::test]
    async fn test_parquet_roundtrip_via_transport() {
        let dir = std::env::temp_dir()
            .join("potato_etl_integration")
            .join(format!("local_parquet_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("transport_roundtrip.parquet");
        let path_str = path.to_str().unwrap();

        let transport = LocalTransport;
        let batch = make_test_batch(1000);

        let bytes = write_parquet_to_bytes(&[batch], ParquetCompression::Zstd).unwrap();
        transport.write_bytes(path_str, &bytes).await.unwrap();

        let read_bytes = transport.read_bytes(path_str).await.unwrap();
        let batches = read_parquet_bytes(&read_bytes, 500, None).unwrap();

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn test_stream_reader_basic() {
        let batch = make_test_batch(5000);
        let bytes = write_parquet_to_bytes(&[batch], ParquetCompression::Snappy).unwrap();

        let reader = ParquetStreamReader::from_bytes(&bytes, 512, None).unwrap();
        let meta = reader.metadata();
        assert_eq!(meta.num_rows, 5000);
        assert_eq!(meta.num_columns, 2);

        let mut total_rows = 0;
        let mut batch_count = 0;
        for batch_result in reader {
            let batch = batch_result.unwrap();
            assert!(batch.num_rows() <= 512);
            total_rows += batch.num_rows();
            batch_count += 1;
        }
        assert_eq!(total_rows, 5000);
        assert!(batch_count >= 10);
    }

    #[test]
    fn test_stream_reader_with_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id",    DataType::Int64,   false),
            Field::new("name",  DataType::Utf8,    false),
            Field::new("score", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as _,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as _,
            Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0])) as _,
        ]).unwrap();

        let bytes = write_parquet_to_bytes(&[batch], ParquetCompression::None).unwrap();
        let cols = vec!["id".to_string(), "score".to_string()];
        let reader = ParquetStreamReader::from_bytes(&bytes, 100, Some(&cols)).unwrap();
        assert_eq!(reader.metadata().num_columns, 2);

        let batches = collect_stream(reader).unwrap();
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].schema().field(0).name(), "id");
        assert_eq!(batches[0].schema().field(1).name(), "score");
    }
}