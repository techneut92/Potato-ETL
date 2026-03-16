//! Shared configuration and options types.
//!
//! This module was previously a single 2800-line file.  It is now split into:
//!
//! | Submodule         | Contents                                              |
//! |-------------------|-------------------------------------------------------|
//! | `auth`            | `AuthConfig`, `DbAuth`                                |
//! | `connections`     | `ConnParams` / `ConnectionDef`, `to_url()`            |
//! | `driver_options`  | Per-driver option structs, `StepDriverOptions`        |
//! | `schema_config`   | `ComponentSchema`, `DatabaseSchemaConfig`, etc.       |
//! | `steps`           | `ReadOptions`, `WriteOptions`, `SinkMode`, etc.       |
//! | `encoding`        | `pct_encode`, `pct_decode`                            |
//! | `secrets`         | `SecretsConfig`                                       |

use serde::{Deserialize, Serialize};

pub mod auth;
pub mod connections;
pub mod driver_options;
pub mod schema_config;
pub mod steps;
pub mod encoding;
pub mod secrets;

// ── Serde helpers (crate-internal) ────────────────────────────────────────────

pub(crate) mod serde_helpers {
    use serde::Deserialize;

    /// Deserializes `T` from any YAML/JSON value, treating an explicit `null`
    /// the same as a missing key — both produce `T::default()`.
    pub fn deserialize_null_as_default<'de, D, T>(d: D) -> Result<T, D::Error>
    where
        D: serde::Deserializer<'de>,
        T: Default + Deserialize<'de>,
    {
        Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
    }
}

// Re-export for use by downstream crates (e.g. runtime's serde `deserialize_with` paths).
pub use serde_helpers::deserialize_null_as_default;

// ── LogLevel ──────────────────────────────────────────────────────────────────

/// Pipeline-level logging verbosity, set in `ETLConfig`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    /// Returns the level as a `tracing`-compatible filter string.
    pub fn as_filter_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn  => "warn",
            Self::Info  => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

impl Default for LogLevel {
    fn default() -> Self { Self::Info }
}

// ── ETLConfig ─────────────────────────────────────────────────────────────────

/// Global pipeline configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ETLConfig {
    /// Number of rows per batch during pagination.  Default: 1000.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum `RecordBatch`es buffered in each inter-component channel.
    /// Default: **4** (3 rounds ahead).
    ///
    /// Higher values hide latency spikes between stages (e.g. a slow COPY
    /// doesn't stall the source because 3 batches can queue up), at the cost
    /// of proportionally more memory per pipeline edge.
    /// Peak memory per edge ≈ `channel_capacity × batch_size × avg_row_bytes`.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,

    /// Logging verbosity for pipeline execution.  Default: `info`.
    #[serde(default)]
    pub log_level: LogLevel,
}

fn default_batch_size() -> usize { 1_000 }
fn default_channel_capacity() -> usize { 4 }

impl Default for ETLConfig {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            channel_capacity: 4,
            log_level: LogLevel::default(),
        }
    }
}

// ── Flat re-exports ───────────────────────────────────────────────────────────
//
// Everything that was previously accessed as `config::Foo` continues to work.

pub use auth::{AuthConfig, DbAuth};
pub use auth::FileAuth;
pub use connections::{ConnParams, ConnectionDef};
pub use driver_options::{
    PostgresOptions, MssqlOptions, OracleOptions, MySqlOptions,
    DatabricksOptions, DatabricksOdbcOptions, DatabricksSourceOptions,
    IdentifierCase, StepDriverOptions,
};
pub use schema_config::{
    SourceSchemaConfig, SinkSchemaConfig,
    ComponentSchema, DatabaseSchemaConfig, DatabaseColumnDef,
    ArrowSchemaConfig, ArrowColumnDef, IndexDef, ConstraintDef,
};
pub use steps::{
    ReadOptions, WriteOptions, SinkMode, CreateTableMode,
    SourceLocation, SinkTarget, FileSourceLocation, FileSinkTarget, JoinHow,
};
pub use encoding::{pct_encode, pct_decode};
pub use secrets::SecretsConfig;