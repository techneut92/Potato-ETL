//! Universal type coercion layer for database sinks.
//!
//! ## Purpose
//!
//! When writing Arrow batches to a target database, the Arrow types must match
//! what the database expects for each SQL type. This varies by dialect:
//!
//! - **MSSQL `DATETIME2`** requires timezone-naive timestamps
//! - **Postgres `TIMESTAMPTZ`** requires timezone-aware timestamps
//! - **Oracle `NUMBER(10,0)`** may need Int64 -> Decimal128 conversion
//!
//! This module provides a **dialect-agnostic framework** where each database
//! sink registers its type conversion rules and the framework applies them
//! automatically before writing batches.
//!
//! ## Two Scenarios
//!
//! | Scenario | Detection Method | Coercion Source |
//! |----------|------------------|-----------------|
//! | **Table exists** | Introspected `target_columns` -> SQL type | Registry: `sql_to_arrow_type()` |
//! | **Table doesn't exist** | `source_db_type` metadata -> DDL will create this | DDL resolver's mapping |
//!
//! ## Architecture
//!
//! ```text
//! +-------------------------------------------------------------+
//! | Arrow Batch (from source, with source_db_type metadata)     |
//! +-------------------------------------------------------------+
//!                            |
//!          +-----------------+------------------+
//!          |                                    |
//!     Table Exists?                      Table Doesn't Exist?
//!          |                                    |
//!          v                                    v
//! +----------------------+          +---------------------------+
//! | Introspect SQL types |          | Use source_db_type meta   |
//! | (e.g., DATETIME2)    |          | (DDL will create this)    |
//! +----------------------+          +---------------------------+
//!          |                                    |
//!          +------------------+-----------------+
//!                             |
//!          +---------------------------------------------+
//!          | TypeCoercionRegistry (dialect-specific)     |
//!          |   - sql_to_arrow_type()                     |
//!          |   - needs_coercion()                        |
//!          +---------------------------------------------+
//!                             |
//!          +---------------------------------------------+
//!          | coerce_batch_for_target()                   |
//!          |   - Applies conversions                     |
//!          |   - Returns coerced RecordBatch             |
//!          +---------------------------------------------+
//!                             |
//!                      Write to Database
//! ```

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, FixedSizeBinaryBuilder, StringArray, LargeStringArray, StringBuilder};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::schema::constants::META_DB_TYPE;

// -- Type Coercion Registry Trait ---------------------------------------------

/// Dialect-specific type coercion rules.
///
/// Each database sink (MSSQL, Postgres, Oracle, MySQL, Databricks) implements
/// this trait to declare what Arrow types it expects for each SQL type.
pub trait TypeCoercionRegistry {
    /// Returns the expected Arrow type for a given SQL type in this dialect.
    ///
    /// ## Example (MSSQL)
    ///
    /// ```ignore
    /// sql_to_arrow_type("DATETIME2") -> Timestamp[us, None]  (no timezone)
    /// sql_to_arrow_type("DATETIMEOFFSET") -> Timestamp[us, UTC]  (with timezone)
    /// sql_to_arrow_type("INT") -> Int32
    /// ```
    ///
    /// Returns `None` if the SQL type is not recognized or does not require
    /// special coercion (the sink's native writer will handle it).
    fn sql_to_arrow_type(&self, sql_type: &str) -> Option<DataType>;

    /// Returns the coercion operation needed (if any) when writing an Arrow
    /// type to a target SQL type.
    ///
    /// This is used when the table already exists and we've introspected the
    /// SQL schema. The registry checks if the current Arrow type matches what
    /// the target SQL type expects.
    ///
    /// Returns `None` if no coercion is needed (types already compatible).
    fn needs_coercion(
        &self,
        arrow_type: &DataType,
        sql_type: &str,
    ) -> Option<CoercionOperation>;
}

// -- Coercion Operations ------------------------------------------------------

/// A type conversion operation that must be applied to an Arrow array before
/// writing to the target database.
#[derive(Debug, Clone, PartialEq)]
pub enum CoercionOperation {
    /// Strip timezone metadata: `Timestamp[us, UTC]` -> `Timestamp[us, None]`
    ///
    /// Used by: MSSQL `DATETIME2`, MySQL `DATETIME`
    StripTimezone,

    /// Add timezone metadata: `Timestamp[us, None]` -> `Timestamp[us, UTC]`
    ///
    /// Used by: Postgres `TIMESTAMPTZ` (if source is naive)
    AddTimezone(Arc<str>),

    /// Cast to a different Arrow type using `arrow::compute::cast`.
    ///
    /// Examples:
    /// - `Int64` -> `Int32` (overflow check)
    /// - `LargeUtf8` -> `Utf8`
    /// - `Date32` -> `Date64`
    Cast(DataType),

    /// Adjust decimal precision/scale.
    ///
    /// Example: `Decimal128(38,10)` -> `Decimal128(18,2)` for target `DECIMAL(18,2)`
    AdjustDecimal {
        target_precision: u8,
        target_scale: i8,
    },

    /// Parse UUID text strings to 16-byte binary.
    ///
    /// Converts `Utf8` / `LargeUtf8` arrays containing UUID strings
    /// (e.g., `"550e8400-e29b-41d4-a716-446655440000"`) to
    /// `FixedSizeBinary(16)` arrays.
    ///
    /// Used by: Postgres `UUID` columns in COPY BINARY mode.
    ParseUuid,

    /// Format 16-byte binary UUIDs back to canonical `Utf8` strings.
    ///
    /// Converts `FixedSizeBinary(16)` arrays to `Utf8` arrays with canonical
    /// UUID format (`"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`).
    ///
    /// Used by: MSSQL `UNIQUEIDENTIFIER` when source is FixedSizeBinary(16)
    /// (e.g., from Postgres UUID).
    FormatUuid,
}

// -- Compiled Coercion Plan ---------------------------------------------------

/// A pre-compiled coercion plan that avoids per-batch O(fields^2) detection.
///
/// Compile once with [`compile_coercion_plan`], then apply cheaply to every
/// subsequent batch with [`apply_coercion_plan`].  The plan stores:
///
/// - Per-column operations (index -> coercion op) -- no name lookups at apply time
/// - Pre-built target schema -- just swap in, no field reconstruction per batch
///
/// This eliminates the repeated `to_lowercase()` + linear `.find()` scan that
/// `coerce_batch_for_target()` does on every batch.
pub struct CoercionPlan {
    /// `operations[i]` = coercion to apply to column `i`.  `None` = pass through.
    operations: Vec<Option<CoercionOperation>>,
    /// Pre-built target field definitions (with coerced types).
    /// Stored so we can build the output schema without re-running detection.
    target_fields: Vec<Field>,
    /// True if at least one column requires coercion.
    has_coercions: bool,
}

/// Compile a coercion plan from the current batch schema + target columns.
///
/// Call this once (on the first batch), then use [`apply_coercion_plan`] for
/// all subsequent batches with the same schema.
pub fn compile_coercion_plan(
    batch_schema: &SchemaRef,
    target_columns: Option<&[TargetColumn]>,
    registry: &dyn TypeCoercionRegistry,
) -> anyhow::Result<Option<CoercionPlan>> {
    let mut has_coercions = false;
    let mut operations: Vec<Option<CoercionOperation>> = Vec::with_capacity(batch_schema.fields().len());
    let mut target_fields: Vec<Field> = Vec::with_capacity(batch_schema.fields().len());

    if let Some(target_cols) = target_columns {
        for field in batch_schema.fields().iter() {
            let field_name_lower = field.name().to_lowercase();
            let target_col = target_cols
                .iter()
                .find(|tc| tc.name.to_lowercase() == field_name_lower);

            if let Some(tc) = target_col {
                if let Some(op) = registry.needs_coercion(field.data_type(), &tc.data_type) {
                    let target_type = coercion_target_type(field.data_type(), &op);
                    let new_field = Field::new(field.name(), target_type, field.is_nullable())
                        .with_metadata(field.metadata().clone());
                    target_fields.push(new_field);
                    has_coercions = true;
                    operations.push(Some(op));
                } else {
                    target_fields.push((**field).clone());
                    operations.push(None);
                }
            } else {
                target_fields.push((**field).clone());
                operations.push(None);
            }
        }
    } else {
        for field in batch_schema.fields().iter() {
            if let Some(source_db_type) = field.metadata().get(META_DB_TYPE) {
                if let Some(expected_arrow) = registry.sql_to_arrow_type(source_db_type) {
                    if field.data_type() != &expected_arrow {
                        let op = CoercionOperation::Cast(expected_arrow.clone());
                        let new_field = Field::new(field.name(), expected_arrow, field.is_nullable())
                            .with_metadata(field.metadata().clone());
                        target_fields.push(new_field);
                        has_coercions = true;
                        operations.push(Some(op));
                    } else {
                        target_fields.push((**field).clone());
                        operations.push(None);
                    }
                } else {
                    target_fields.push((**field).clone());
                    operations.push(None);
                }
            } else {
                target_fields.push((**field).clone());
                operations.push(None);
            }
        }
    }

    if !has_coercions {
        return Ok(None);
    }

    // Log what was compiled (once, not per batch)
    for (idx, op) in operations.iter().enumerate() {
        if let Some(op) = op {
            tracing::debug!(
                column = batch_schema.field(idx).name(),
                operation = ?op,
                "type_coercion: compiled coercion (once)"
            );
        }
    }

    Ok(Some(CoercionPlan { operations, target_fields, has_coercions }))
}

/// Apply a pre-compiled coercion plan to a batch.
///
/// This is the hot-path function -- no name lookups, no detection logic.
/// Just applies the stored coercion operations by column index.
pub fn apply_coercion_plan(
    batch: RecordBatch,
    plan: &CoercionPlan,
) -> anyhow::Result<RecordBatch> {
    if !plan.has_coercions {
        return Ok(batch);
    }

    let schema = batch.schema();
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (idx, op) in plan.operations.iter().enumerate() {
        match op {
            Some(op) => {
                let (_, new_array) = apply_coercion_operation(
                    schema.field(idx),
                    batch.column(idx),
                    op,
                )?;
                new_columns.push(new_array);
            }
            None => {
                new_columns.push(Arc::clone(batch.column(idx)));
            }
        }
    }

    let new_schema = Arc::new(Schema::new_with_metadata(
        plan.target_fields.clone(),
        schema.metadata().clone(),
    ));
    Ok(RecordBatch::try_new(new_schema, new_columns)?)
}

/// Determine the output DataType for a coercion operation.
fn coercion_target_type(_source: &DataType, op: &CoercionOperation) -> DataType {
    match op {
        CoercionOperation::StripTimezone => DataType::Timestamp(TimeUnit::Microsecond, None),
        CoercionOperation::AddTimezone(tz) => DataType::Timestamp(TimeUnit::Microsecond, Some(tz.clone())),
        CoercionOperation::Cast(target) => target.clone(),
        CoercionOperation::AdjustDecimal { target_precision, target_scale } => {
            DataType::Decimal128(*target_precision, *target_scale)
        }
        CoercionOperation::ParseUuid => DataType::FixedSizeBinary(16),
        CoercionOperation::FormatUuid => DataType::Utf8,
    }
}

// -- Generic Batch Coercion ---------------------------------------------------

/// Column metadata from table introspection (used when table exists).
///
/// This is a simplified version that all sinks can use. Each sink already has
/// its own `ColumnMetadata` struct; this is the common interface.
pub struct TargetColumn {
    pub name: String,
    pub data_type: String,  // SQL type (e.g., "DATETIME2", "VARCHAR", "INT")
}

/// Coerces an Arrow batch to match the target database schema.
///
/// Applies dialect-specific type conversions to ensure the Arrow types are
/// compatible with what the target SQL types expect.
///
/// ## Scenarios
///
/// 1. **Table exists** (`target_columns` is `Some`)
///    - Uses introspected SQL types to determine needed coercions
///    - Calls `registry.needs_coercion(arrow_type, sql_type)`
///
/// 2. **Table doesn't exist** (`target_columns` is `None`)
///    - Uses `source_db_type` metadata from Arrow fields
///    - Maps `source_db_type` -> expected Arrow type via `registry.sql_to_arrow_type()`
///    - The DDL generator will use the same mapping, so types will match
///
/// ## Example (MSSQL)
///
/// ```ignore
/// // Scenario 1: Table exists
/// target_columns = [TargetColumn { name: "created_at", data_type: "datetime2" }]
/// batch field = Timestamp[us, UTC]
/// -> registry.needs_coercion() returns StripTimezone
/// -> batch is coerced to Timestamp[us, None]
///
/// // Scenario 2: Table doesn't exist
/// batch field metadata: source_db_type = "timestamptz"
/// -> registry.sql_to_arrow_type("timestamptz") returns Timestamp[us, None] (for MSSQL)
/// -> batch field is Timestamp[us, UTC]
/// -> coerce to Timestamp[us, None]
/// ```
pub fn coerce_batch_for_target(
    batch: RecordBatch,
    target_columns: Option<&[TargetColumn]>,
    registry: &dyn TypeCoercionRegistry,
) -> anyhow::Result<RecordBatch> {
    let schema = batch.schema();
    let mut needs_coercion = false;

    // -- Phase 1: Detect what needs coercion ----------------------------------

    let mut operations: Vec<Option<CoercionOperation>> = Vec::with_capacity(schema.fields().len());

    if let Some(target_cols) = target_columns {
        // Scenario 1: Table exists -- use introspected SQL types
        for field in schema.fields().iter() {
            let field_name_lower = field.name().to_lowercase();
            let target_col = target_cols
                .iter()
                .find(|tc| tc.name.to_lowercase() == field_name_lower);

            if let Some(tc) = target_col {
                if let Some(op) = registry.needs_coercion(field.data_type(), &tc.data_type) {
                    needs_coercion = true;
                    operations.push(Some(op));
                } else {
                    operations.push(None);
                }
            } else {
                operations.push(None);
            }
        }
    } else {
        // Scenario 2: Table doesn't exist -- use source_db_type metadata
        for field in schema.fields().iter() {
            if let Some(source_db_type) = field.metadata().get(META_DB_TYPE) {
                if let Some(expected_arrow) = registry.sql_to_arrow_type(source_db_type) {
                    if field.data_type() != &expected_arrow {
                        needs_coercion = true;
                        operations.push(Some(CoercionOperation::Cast(expected_arrow)));
                    } else {
                        operations.push(None);
                    }
                } else {
                    operations.push(None);
                }
            } else {
                operations.push(None);
            }
        }
    }

    if !needs_coercion {
        return Ok(batch);
    }

    // -- Phase 2: Apply coercions ---------------------------------------------

    let mut new_fields: Vec<Field> = Vec::with_capacity(schema.fields().len());
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for (idx, field) in schema.fields().iter().enumerate() {
        match &operations[idx] {
            Some(op) => {
                let (new_field, new_array) = apply_coercion_operation(
                    field,
                    batch.column(idx),
                    op,
                )?;
                new_fields.push(new_field);
                new_columns.push(new_array);

                tracing::debug!(
                    column = field.name(),
                    operation = ?op,
                    "type_coercion: applied coercion"
                );
            }
            None => {
                new_fields.push((**field).clone());
                new_columns.push(Arc::clone(batch.column(idx)));
            }
        }
    }

    let new_schema = Arc::new(Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    Ok(RecordBatch::try_new(new_schema, new_columns)?)
}

/// Applies a single coercion operation to a field and its array.
fn apply_coercion_operation(
    field: &Field,
    array: &ArrayRef,
    operation: &CoercionOperation,
) -> anyhow::Result<(Field, ArrayRef)> {
    match operation {
        CoercionOperation::StripTimezone => {
            // Timestamp[us, UTC] -> Timestamp[us, None]
            let new_type = DataType::Timestamp(TimeUnit::Microsecond, None);
            let new_array = cast(array, &new_type)
                .map_err(|e| anyhow::anyhow!("StripTimezone cast failed: {e}"))?;
            let new_field = Field::new(field.name(), new_type, field.is_nullable())
                .with_metadata(field.metadata().clone());
            Ok((new_field, new_array))
        }

        CoercionOperation::AddTimezone(tz) => {
            // Timestamp[us, None] -> Timestamp[us, UTC]
            let new_type = DataType::Timestamp(TimeUnit::Microsecond, Some(tz.clone()));
            let new_array = cast(array, &new_type)
                .map_err(|e| anyhow::anyhow!("AddTimezone cast failed: {e}"))?;
            let new_field = Field::new(field.name(), new_type, field.is_nullable())
                .with_metadata(field.metadata().clone());
            Ok((new_field, new_array))
        }

        CoercionOperation::Cast(target_type) => {
            let new_array = cast(array, target_type)
                .map_err(|e| anyhow::anyhow!("Cast to {:?} failed: {e}", target_type))?;
            let new_field = Field::new(field.name(), target_type.clone(), field.is_nullable())
                .with_metadata(field.metadata().clone());
            Ok((new_field, new_array))
        }

        CoercionOperation::AdjustDecimal {
            target_precision,
            target_scale,
        } => {
            let target_type = DataType::Decimal128(*target_precision, *target_scale);
            let new_array = cast(array, &target_type)
                .map_err(|e| {
                    let source_desc = match array.data_type() {
                        DataType::Decimal128(p, s) => format!("Decimal128({},{})", p, s),
                        _ => "unknown".to_string(),
                    };
                    anyhow::anyhow!(
                        "AdjustDecimal cast failed (precision/scale overflow): \
                         {} -> Decimal128({},{}): {e}",
                        source_desc, target_precision, target_scale
                    )
                })?;
            let new_field = Field::new(field.name(), target_type, field.is_nullable())
                .with_metadata(field.metadata().clone());
            Ok((new_field, new_array))
        }

        CoercionOperation::ParseUuid => {
            let target_type = DataType::FixedSizeBinary(16);
            let new_array = parse_uuid_array(array)?;
            let new_field = Field::new(field.name(), target_type, field.is_nullable())
                .with_metadata(field.metadata().clone());
            Ok((new_field, new_array))
        }

        CoercionOperation::FormatUuid => {
            let target_type = DataType::Utf8;
            let new_array = format_uuid_array(array)?;
            let new_field = Field::new(field.name(), target_type, field.is_nullable())
                .with_metadata(field.metadata().clone());
            Ok((new_field, new_array))
        }
    }
}

/// Parses a Utf8 or LargeUtf8 array of UUID strings into FixedSizeBinary(16).
///
/// Accepted formats:
/// - `"550e8400-e29b-41d4-a716-446655440000"` (canonical 36 chars)
/// - `"550e8400e29b41d4a716446655440000"` (32 hex chars, no hyphens)
fn parse_uuid_array(array: &ArrayRef) -> anyhow::Result<ArrayRef> {
    let len = array.len();

    let mut builder = FixedSizeBinaryBuilder::with_capacity(len, 16);

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("ParseUuid: expected StringArray"))?;
            for i in 0..len {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    let bytes = parse_uuid_str(arr.value(i))
                        .ok_or_else(|| anyhow::anyhow!(
                            "ParseUuid: invalid UUID string at row {}: {:?}", i, arr.value(i)
                        ))?;
                    builder.append_value(&bytes)
                        .map_err(|e| anyhow::anyhow!("ParseUuid: append failed at row {}: {e}", i))?;
                }
            }
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>()
                .ok_or_else(|| anyhow::anyhow!("ParseUuid: expected LargeStringArray"))?;
            for i in 0..len {
                if arr.is_null(i) {
                    builder.append_null();
                } else {
                    let bytes = parse_uuid_str(arr.value(i))
                        .ok_or_else(|| anyhow::anyhow!(
                            "ParseUuid: invalid UUID string at row {}: {:?}", i, arr.value(i)
                        ))?;
                    builder.append_value(&bytes)
                        .map_err(|e| anyhow::anyhow!("ParseUuid: append failed at row {}: {e}", i))?;
                }
            }
        }
        other => anyhow::bail!("ParseUuid: unsupported source type {:?}", other),
    }

    Ok(Arc::new(builder.finish()))
}

/// Formats a FixedSizeBinary(16) array of UUIDs into canonical Utf8 strings.
///
/// Output format: `"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"`
fn format_uuid_array(array: &ArrayRef) -> anyhow::Result<ArrayRef> {
    let len = array.len();

    // Ensure the array is FixedSizeBinary(16)
    let arr = array.as_any().downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .ok_or_else(|| anyhow::anyhow!("FormatUuid: expected FixedSizeBinary(16)"))?;

    let mut builder = StringBuilder::with_capacity(len, len * 36);
    for i in 0..len {
        if arr.is_null(i) {
            builder.append_null();
        } else {
            let bytes = arr.value(i);
            if bytes.len() != 16 {
                return Err(anyhow::anyhow!("FormatUuid: invalid UUID length at row {}: {}", i, bytes.len()));
            }
            let uuid_str = format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5],
                bytes[6], bytes[7],
                bytes[8], bytes[9],
                bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
            );
            builder.append_value(&uuid_str);
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parses a UUID string (with or without hyphens) into 16 bytes.
fn parse_uuid_str(s: &str) -> Option<[u8; 16]> {
    let hex: Vec<u8> = s.bytes().filter(|b| *b != b'-').collect();
    if hex.len() != 32 { return None; }

    let mut out = [0u8; 16];
    for i in 0..16 {
        let hi = hex_nibble(hex[i * 2])?;
        let lo = hex_nibble(hex[i * 2 + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

#[inline]
fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::TimestampMicrosecondArray;
    use std::collections::HashMap;

    struct MockRegistry;

    impl TypeCoercionRegistry for MockRegistry {
        fn sql_to_arrow_type(&self, sql_type: &str) -> Option<DataType> {
            match sql_type.to_lowercase().as_str() {
                "datetime2" => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
                "timestamptz" => Some(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
                _ => None,
            }
        }

        fn needs_coercion(&self, arrow_type: &DataType, sql_type: &str) -> Option<CoercionOperation> {
            match (arrow_type, sql_type.to_lowercase().as_str()) {
                (DataType::Timestamp(_, Some(_)), "datetime2") => Some(CoercionOperation::StripTimezone),
                (DataType::Timestamp(_, None), "timestamptz") => Some(CoercionOperation::AddTimezone("UTC".into())),
                _ => None,
            }
        }
    }

    fn make_timestamp_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        ]));

        let ts_array = Arc::new(TimestampMicrosecondArray::from(vec![
            1646092800000000, // 2022-03-01 00:00:00 UTC
        ]));

        RecordBatch::try_new(schema, vec![ts_array]).unwrap()
    }

    #[test]
    fn test_coerce_batch_strip_timezone() {
        let batch = make_timestamp_batch();
        let target_cols = vec![TargetColumn {
            name: "created_at".to_string(),
            data_type: "datetime2".to_string(),
        }];

        let registry = MockRegistry;
        let result = coerce_batch_for_target(batch, Some(&target_cols), &registry).unwrap();

        let schema = result.schema();
        let field = schema.field(0);
        assert_eq!(field.data_type(), &DataType::Timestamp(TimeUnit::Microsecond, None));
    }

    #[test]
    fn test_coerce_batch_no_target_columns() {
        // Scenario: Table doesn't exist, use source_db_type metadata
        let mut metadata = HashMap::new();
        metadata.insert(META_DB_TYPE.to_string(), "datetime2".to_string());

        let schema = Arc::new(Schema::new(vec![
            Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)
                .with_metadata(metadata),
        ]));

        let ts_array = Arc::new(TimestampMicrosecondArray::from(vec![
            1646092800000000,
        ]));

        let batch = RecordBatch::try_new(schema, vec![ts_array]).unwrap();

        let registry = MockRegistry;
        let result = coerce_batch_for_target(batch, None, &registry).unwrap();

        let schema = result.schema();
        let field = schema.field(0);
        // Should be coerced to timezone-naive because source_db_type="datetime2"
        // maps to Timestamp[us, None] in MockRegistry
        assert_eq!(field.data_type(), &DataType::Timestamp(TimeUnit::Microsecond, None));
    }

    // ── UUID parsing ─────────────────────────────────────────────────────

    #[test]
    fn test_parse_uuid_str_canonical() {
        let bytes = parse_uuid_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(
            bytes,
            [0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
             0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00]
        );
    }

    #[test]
    fn test_parse_uuid_str_no_hyphens() {
        let bytes = parse_uuid_str("550e8400e29b41d4a716446655440000").unwrap();
        assert_eq!(
            bytes,
            [0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
             0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00]
        );
    }

    #[test]
    fn test_parse_uuid_str_uppercase() {
        let bytes = parse_uuid_str("550E8400-E29B-41D4-A716-446655440000").unwrap();
        assert_eq!(bytes[0], 0x55);
        assert_eq!(bytes[4], 0xe2);
    }

    #[test]
    fn test_parse_uuid_str_invalid() {
        assert!(parse_uuid_str("not-a-uuid").is_none());
        assert!(parse_uuid_str("").is_none());
        assert!(parse_uuid_str("550e8400-e29b-41d4-a716-44665544000g").is_none()); // 'g' invalid
    }

    #[test]
    fn test_parse_uuid_array_with_nulls() {
        let arr = Arc::new(StringArray::from(vec![
            Some("550e8400-e29b-41d4-a716-446655440000"),
            None,
            Some("00000000-0000-0000-0000-000000000000"),
        ])) as ArrayRef;

        let result = parse_uuid_array(&arr).unwrap();
        assert_eq!(result.len(), 3);
        assert!(!result.is_null(0));
        assert!(result.is_null(1));
        assert!(!result.is_null(2));

        let fsb = result.as_any().downcast_ref::<arrow::array::FixedSizeBinaryArray>().unwrap();
        assert_eq!(fsb.value(0)[0], 0x55);
        assert_eq!(fsb.value(2), &[0u8; 16]);
    }

    // ── UUID formatting ────────────────────────────────────────────────

    #[test]
    fn test_format_uuid_array() {
        let bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
            0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
        ];

        let mut builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(3, 16);
        builder.append_value(&bytes).unwrap();
        builder.append_value(&bytes).unwrap();
        builder.append_value(&bytes).unwrap();
        let arr = Arc::new(builder.finish()) as ArrayRef;

        let result = format_uuid_array(&arr).unwrap();
        assert_eq!(result.len(), 3);
        assert!(!result.is_null(0));
        assert!(!result.is_null(1));
        assert!(!result.is_null(2));

        let str_arr = result.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(str_arr.value(1), "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(str_arr.value(2), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_format_uuid_array_with_nulls() {
        let bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
            0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
        ];

        let mut builder = arrow::array::FixedSizeBinaryBuilder::with_capacity(3, 16);
        builder.append_value(&bytes).unwrap();
        builder.append_null();
        builder.append_value(&bytes).unwrap();
        let arr = Arc::new(builder.finish()) as ArrayRef;

        let result = format_uuid_array(&arr).unwrap();
        assert_eq!(result.len(), 3);
        assert!(!result.is_null(0));
        assert!(result.is_null(1));
        assert!(!result.is_null(2));

        let str_arr = result.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(str_arr.value(2), "550e8400-e29b-41d4-a716-446655440000");
    }

    // ── UUID roundtrip: ParseUuid → FormatUuid ──────────────────────────

    #[test]
    fn test_uuid_roundtrip_parse_then_format() {
        // Start with UUID strings → parse to binary → format back to strings.
        let uuids = vec![
            "550e8400-e29b-41d4-a716-446655440000",
            "00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ];
        let str_arr = Arc::new(StringArray::from(uuids.clone())) as ArrayRef;

        // ParseUuid: Utf8 → FixedSizeBinary(16)
        let binary = parse_uuid_array(&str_arr).unwrap();
        assert_eq!(binary.data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(binary.len(), 3);

        // FormatUuid: FixedSizeBinary(16) → Utf8
        let back = format_uuid_array(&binary).unwrap();
        let result = back.as_any().downcast_ref::<StringArray>().unwrap();

        for (i, expected) in uuids.iter().enumerate() {
            assert_eq!(result.value(i), *expected, "roundtrip mismatch at row {i}");
        }
    }

    #[test]
    fn test_uuid_roundtrip_with_nulls() {
        let str_arr = Arc::new(StringArray::from(vec![
            Some("a1b2c3d4-e5f6-7890-abcd-ef1234567890"),
            None,
            Some("12345678-1234-1234-1234-123456789abc"),
            None,
        ])) as ArrayRef;

        let binary = parse_uuid_array(&str_arr).unwrap();
        let back = format_uuid_array(&binary).unwrap();
        let result = back.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(result.value(0), "a1b2c3d4-e5f6-7890-abcd-ef1234567890");
        assert!(result.is_null(1));
        assert_eq!(result.value(2), "12345678-1234-1234-1234-123456789abc");
        assert!(result.is_null(3));
    }
}