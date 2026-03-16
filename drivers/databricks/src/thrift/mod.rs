//! Databricks Thrift transport — TCLIService binary protocol over HTTPS.
//!
//! This is the same wire protocol used by the Python `databricks-sql-connector`,
//! Apache Hive JDBC, and Spark Thrift Server.  It provides column-oriented
//! result sets which map directly to Arrow arrays.
//!
//! ## Module layout
//!
//! ```text
//! protocol.rs     — Thrift Binary encoding reader / writer
//! rpc.rs          — Databricks-specific RPC builders + parsers
//! arrow_convert.rs — Thrift column data → Arrow RecordBatch conversion
//! client.rs       — Session-managed ThriftClient for DML + queries
//! source.rs       — DatabricksThriftSource (SourceBuilder)
//! sink.rs         — DatabricksThriftSink   (SinkBuilder)
//! ```

pub mod protocol;
pub mod rpc;
pub mod arrow_convert;
pub mod client;
pub mod source;
pub mod sink;
