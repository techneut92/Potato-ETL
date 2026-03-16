//! Databricks TCLIService Thrift RPC — session, execute, fetch, close.
//!
//! All RPCs use the Thrift Binary encoding over HTTPS, the same wire format
//! as the Python `databricks-sql-connector`.

use anyhow::{anyhow, bail, Result};
use bytes::{Buf, Bytes};
use reqwest::Client;

use super::protocol::*;

// ── TCLIService method names ─────────────────────────────────────────────────

const OPEN_SESSION: &str = "OpenSession";
const EXECUTE_STATEMENT: &str = "ExecuteStatement";
const GET_OPERATION_STATUS: &str = "GetOperationStatus";
const FETCH_RESULTS: &str = "FetchResults";
const CLOSE_OPERATION: &str = "CloseOperation";
const CLOSE_SESSION: &str = "CloseSession";
const GET_RESULT_SET_METADATA: &str = "GetResultSetMetadata";

// TOperationState values
pub const OP_STATE_FINISHED: i32 = 2;
pub const OP_STATE_CANCELED: i32 = 3;
pub const OP_STATE_CLOSED: i32 = 4;
pub const OP_STATE_ERROR: i32 = 5;

/// Default fetch orientation: FETCH_NEXT.
const FETCH_ORIENTATION_NEXT: i32 = 0;

// ── Databricks / Hive TTypeId values (from TCLIService.thrift) ───────────────

/// Databricks SQL type IDs — used to interpret Thrift column data correctly.
#[allow(dead_code)]
pub mod dbx_type {
    pub const BOOLEAN:   i32 = 0;
    pub const TINYINT:   i32 = 1;
    pub const SMALLINT:  i32 = 2;
    pub const INT:       i32 = 3;
    pub const BIGINT:    i32 = 4;
    pub const FLOAT:     i32 = 5;
    pub const DOUBLE:    i32 = 6;
    pub const STRING:    i32 = 7;
    pub const TIMESTAMP: i32 = 8;
    pub const BINARY:    i32 = 9;
    pub const DECIMAL:   i32 = 15;
    pub const DATE:      i32 = 17;
    pub const VARCHAR:   i32 = 18;
    pub const CHAR:      i32 = 19;
    pub const TIMESTAMP_NTZ: i32 = 22;
}

// ── Column metadata ──────────────────────────────────────────────────────────

/// Column metadata from `GetResultSetMetadata` — name + Databricks SQL type.
#[derive(Clone, Debug)]
pub struct ColumnMeta {
    pub name: String,
    /// Databricks TTypeId (see `dbx_type` constants).
    pub type_id: i32,
}

// ── Handle types ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct SessionHandle {
    pub(crate) guid: Vec<u8>,
    pub(crate) secret: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct OperationHandle {
    pub(crate) guid: Vec<u8>,
    pub(crate) secret: Vec<u8>,
    pub(crate) op_type: i32,
    pub(crate) has_result_set: bool,
}

// ── Thrift HTTP transport ────────────────────────────────────────────────────

/// POST a Thrift binary payload to the Databricks SQL endpoint.
pub async fn thrift_post(
    client: &Client,
    url: &str,
    auth_header: &str,
    payload: Bytes,
) -> Result<Bytes> {
    let resp = client
        .post(url)
        .header("Content-Type", "application/x-thrift")
        .header("Authorization", auth_header)
        .body(reqwest::Body::from(payload))
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("Thrift HTTP {status}: {body}");
    }

    Ok(resp.bytes().await?)
}

// ── OpenSession ──────────────────────────────────────────────────────────────

pub fn build_open_session(catalog: Option<&str>, schema: Option<&str>) -> Bytes {
    let mut w = ThriftWriter::with_capacity(128);
    w.write_message_begin(OPEN_SESSION, 1, 1);

    // OpenSession_args { 1: TOpenSessionReq req }
    w.write_field_begin(T_STRUCT, 1);

    // field 2: configuration map<string,string>
    let config = [
        ("spark.thriftserver.arrowBasedRowSet.timestampAsString", "false"),
    ];
    w.write_field_begin(T_MAP, 2);
    w.write_map_begin(T_STRING, T_STRING, config.len() as i32);
    for (k, v) in &config { w.write_string(k); w.write_string(v); }

    // field 8: initialNamespace (TNamespace)
    if catalog.is_some() || schema.is_some() {
        w.write_field_begin(T_STRUCT, 8);
        if let Some(c) = catalog {
            w.write_field_begin(T_STRING, 1); w.write_string(c);
        }
        if let Some(s) = schema {
            w.write_field_begin(T_STRING, 2); w.write_string(s);
        }
        w.write_field_stop(); // end TNamespace
    }

    // field 10: clientProtocolI64 (Spark CLI V7)
    w.write_field_begin(T_I64, 10);
    w.write_i64(42247);

    // field 11: canUseMultipleCatalogs
    w.write_field_begin(T_BOOL, 11);
    w.write_bool(true);

    w.write_field_stop(); // end TOpenSessionReq
    w.write_field_stop(); // end OpenSession_args
    w.finish()
}

pub fn parse_open_session(buf: Bytes) -> Result<SessionHandle> {
    let mut r = ThriftReader::new(buf);
    r.read_message_begin()?;

    let (ft, _fid) = r.read_field_begin()?;
    if ft != T_STRUCT { bail!("Expected OpenSession_result wrapper struct, got type {ft}"); }

    let mut handle: Option<SessionHandle> = None;
    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_STRUCT, 1) => check_status(&mut r, OPEN_SESSION)?,
            (T_I32, 2) => { r.read_i32(); }
            (T_STRUCT, 3) => {
                let (guid, secret) = read_handle_identifier(&mut r)?;
                handle = Some(SessionHandle { guid, secret });
            }
            _ => r.skip(ft)?,
        }
    }
    handle.ok_or_else(|| anyhow!("No session handle in OpenSession response"))
}

// ── ExecuteStatement ─────────────────────────────────────────────────────────

pub fn build_execute_statement(session: &SessionHandle, sql: &str) -> Bytes {
    let mut w = ThriftWriter::with_capacity(64 + sql.len());
    w.write_message_begin(EXECUTE_STATEMENT, 1, 2);

    // ExecuteStatement_args { 1: TExecuteStatementReq req }
    w.write_field_begin(T_STRUCT, 1);

    // field 1: sessionHandle
    w.write_field_begin(T_STRUCT, 1);
    write_session_handle(&mut w, session);

    // field 2: statement
    w.write_field_begin(T_STRING, 2);
    w.write_string(sql);

    // field 4: runAsync = true
    w.write_field_begin(T_BOOL, 4);
    w.write_bool(true);

    // field 7: queryTimeout = 0 (no timeout)
    w.write_field_begin(T_I64, 7);
    w.write_i64(0);

    w.write_field_stop(); // end TExecuteStatementReq
    w.write_field_stop(); // end ExecuteStatement_args
    w.finish()
}

pub fn parse_execute_statement(buf: Bytes) -> Result<OperationHandle> {
    let mut r = ThriftReader::new(buf);
    r.read_message_begin()?;
    let (ft, _) = r.read_field_begin()?;
    if ft != T_STRUCT { bail!("Expected ExecuteStatement_result wrapper, got {ft}"); }
    parse_operation_handle_from(&mut r, EXECUTE_STATEMENT)
}

// ── GetOperationStatus ───────────────────────────────────────────────────────

pub fn build_get_operation_status(op: &OperationHandle) -> Bytes {
    let mut w = ThriftWriter::with_capacity(64);
    w.write_message_begin(GET_OPERATION_STATUS, 1, 3);

    w.write_field_begin(T_STRUCT, 1);
    w.write_field_begin(T_STRUCT, 1);
    write_operation_handle(&mut w, op);
    w.write_field_stop();
    w.write_field_stop();
    w.finish()
}

/// Returns (TOperationState, optional error message).
pub fn parse_get_operation_status(buf: Bytes) -> Result<(i32, Option<String>)> {
    let mut r = ThriftReader::new(buf);
    r.read_message_begin()?;
    let (ft, _) = r.read_field_begin()?;
    if ft != T_STRUCT { bail!("Expected GetOperationStatus_result wrapper, got {ft}"); }

    let mut state = -1i32;
    let mut error_msg: Option<String> = None;
    let mut display_msg: Option<String> = None;
    let mut diagnostic_info: Option<String> = None;
    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_STRUCT, 1) => skip_struct(&mut r)?,
            (T_I32, 2) => state = r.read_i32(),
            (T_STRING, 4) => error_msg = Some(r.read_string()?),      // errorMessage
            (T_STRING, 5) => display_msg = Some(r.read_string()?),     // displayMessage
            (T_STRING, 7) => diagnostic_info = Some(r.read_string()?), // diagnosticInfo
            _ => r.skip(ft)?,
        }
    }
    // Pick the most informative error string available
    let best_msg = display_msg.or(error_msg).or(diagnostic_info);
    Ok((state, best_msg))
}

// ── GetResultSetMetadata ─────────────────────────────────────────────────────

pub fn build_get_result_set_metadata(op: &OperationHandle) -> Bytes {
    let mut w = ThriftWriter::with_capacity(64);
    w.write_message_begin(GET_RESULT_SET_METADATA, 1, 7);

    // GetResultSetMetadata_args { 1: TGetResultSetMetadataReq req }
    w.write_field_begin(T_STRUCT, 1);

    // field 1: operationHandle
    w.write_field_begin(T_STRUCT, 1);
    write_operation_handle(&mut w, op);

    w.write_field_stop(); // end TGetResultSetMetadataReq
    w.write_field_stop(); // end GetResultSetMetadata_args
    w.finish()
}

/// Parse GetResultSetMetadata response → list of (column_name, type_name).
///
/// TGetResultSetMetadataResp {
///   1: TStatus status
///   2: TTableSchema schema {
///     1: list<TColumnDesc> columns {
///       1: string columnName
///       2: TTypeDesc typeDesc
///       3: i32 position
///     }
///   }
/// }
pub fn parse_get_result_set_metadata(buf: Bytes) -> Result<Vec<ColumnMeta>> {
    let mut r = ThriftReader::new(buf);
    r.read_message_begin()?;

    let (ft, _) = r.read_field_begin()?;
    if ft != T_STRUCT { bail!("Expected GetResultSetMetadata_result wrapper, got {ft}"); }

    let mut col_meta: Vec<ColumnMeta> = Vec::new();
    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_STRUCT, 1) => check_status(&mut r, GET_RESULT_SET_METADATA)?,
            (T_STRUCT, 2) => {
                // TTableSchema
                loop {
                    let (ft2, fid2) = r.read_field_begin()?;
                    if ft2 == T_STOP { break; }
                    match (ft2, fid2) {
                        (T_LIST, 1) => {
                            // list<TColumnDesc>
                            let _elem_type = r.buf.get_u8();
                            let num_cols = r.buf.get_i32();
                            col_meta.reserve(num_cols as usize);
                            for _ in 0..num_cols {
                                let meta = parse_column_desc(&mut r)?;
                                col_meta.push(meta);
                            }
                        }
                        _ => r.skip(ft2)?,
                    }
                }
            }
            _ => r.skip(ft)?,
        }
    }
    Ok(col_meta)
}

/// Parse a single TColumnDesc struct → ColumnMeta.
///
/// ```text
/// TColumnDesc {
///   1: string columnName
///   2: TTypeDesc typeDesc {
///     1: list<TTypeEntry> types [
///       TTypeEntry (union/struct) {
///         1: TPrimitiveTypeEntry { 1: TTypeId type (i32) }
///       }
///     ]
///   }
///   3: i32 position
/// }
/// ```
fn parse_column_desc(r: &mut ThriftReader) -> Result<ColumnMeta> {
    let mut name = String::new();
    let mut type_id: i32 = dbx_type::STRING; // default to STRING if we can't parse

    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_STRING, 1) => name = r.read_string()?,
            (T_STRUCT, 2) => {
                // TTypeDesc
                type_id = parse_type_desc(r)?;
            }
            _ => r.skip(ft)?,
        }
    }

    Ok(ColumnMeta { name, type_id })
}

/// Parse TTypeDesc → extract the TTypeId from the first TPrimitiveTypeEntry.
fn parse_type_desc(r: &mut ThriftReader) -> Result<i32> {
    let mut type_id = dbx_type::STRING;

    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_LIST, 1) => {
                // list<TTypeEntry>
                let _elem_type = r.buf.get_u8();
                let count = r.buf.get_i32();
                for i in 0..count {
                    // TTypeEntry is a union (encoded as a struct with one field)
                    loop {
                        let (ft2, fid2) = r.read_field_begin()?;
                        if ft2 == T_STOP { break; }
                        match (ft2, fid2) {
                            (T_STRUCT, 1) if i == 0 => {
                                // TPrimitiveTypeEntry — only parse from first entry
                                loop {
                                    let (ft3, fid3) = r.read_field_begin()?;
                                    if ft3 == T_STOP { break; }
                                    match (ft3, fid3) {
                                        (T_I32, 1) => type_id = r.read_i32(),
                                        _ => r.skip(ft3)?,
                                    }
                                }
                            }
                            _ => r.skip(ft2)?,
                        }
                    }
                }
            }
            _ => r.skip(ft)?,
        }
    }

    Ok(type_id)
}

// ── FetchResults ─────────────────────────────────────────────────────────────

pub fn build_fetch_results(op: &OperationHandle, max_rows: i64) -> Bytes {
    let mut w = ThriftWriter::with_capacity(64);
    w.write_message_begin(FETCH_RESULTS, 1, 4);

    w.write_field_begin(T_STRUCT, 1);
    w.write_field_begin(T_STRUCT, 1);
    write_operation_handle(&mut w, op);

    w.write_field_begin(T_I32, 2);
    w.write_i32(FETCH_ORIENTATION_NEXT);

    w.write_field_begin(T_I64, 3);
    w.write_i64(max_rows);

    w.write_field_stop();
    w.write_field_stop();
    w.finish()
}

/// Parsed column data from a TColumn union — column-oriented.
pub enum ThriftColData {
    Bool   { values: Vec<bool>,   nulls: Vec<u8> },
    Byte   { values: Vec<i8>,     nulls: Vec<u8> },
    I16    { values: Vec<i16>,    nulls: Vec<u8> },
    I32    { values: Vec<i32>,    nulls: Vec<u8> },
    I64    { values: Vec<i64>,    nulls: Vec<u8> },
    Double { values: Vec<f64>,    nulls: Vec<u8> },
    Str    { values: Vec<Bytes>,  nulls: Vec<u8> },
}

impl ThriftColData {
    pub fn len(&self) -> usize {
        match self {
            Self::Bool   { values, .. } => values.len(),
            Self::Byte   { values, .. } => values.len(),
            Self::I16    { values, .. } => values.len(),
            Self::I32    { values, .. } => values.len(),
            Self::I64    { values, .. } => values.len(),
            Self::Double { values, .. } => values.len(),
            Self::Str    { values, .. } => values.len(),
        }
    }
}

/// Returns `true` if bit `i` is set in the null bitmap (1 = null).
#[inline]
pub fn is_null(nulls: &[u8], i: usize) -> bool {
    let byte = i / 8;
    let bit = i % 8;
    byte < nulls.len() && (nulls[byte] >> bit) & 1 == 1
}

/// Parse a TRowSet → (has_more_rows, columns).
pub fn parse_fetch_results(buf: Bytes) -> Result<(bool, Vec<ThriftColData>)> {
    let mut r = ThriftReader::new(buf);
    r.read_message_begin()?;
    let (ft, _) = r.read_field_begin()?;
    if ft != T_STRUCT { bail!("Expected FetchResults_result wrapper, got {ft}"); }

    let mut has_more = false;
    let mut columns: Vec<ThriftColData> = Vec::new();

    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_STRUCT, 1) => skip_struct(&mut r)?,
            (T_BOOL, 2)   => has_more = r.read_bool(),
            (T_STRUCT, 3) => {
                // TRowSet
                parse_row_set(&mut r, &mut columns)?;
            }
            _ => r.skip(ft)?,
        }
    }
    Ok((has_more, columns))
}

fn parse_row_set(r: &mut ThriftReader, columns: &mut Vec<ThriftColData>) -> Result<()> {
    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_LIST, 3) => {
                // list<TColumn>
                let _elem_type = r.buf.get_u8();
                let num_cols = r.buf.get_i32();
                columns.reserve(num_cols as usize);
                for _ in 0..num_cols {
                    // TColumn is a union: exactly one typed sub-struct field
                    let mut col: Option<ThriftColData> = None;
                    loop {
                        let (ft3, fid3) = r.read_field_begin()?;
                        if ft3 == T_STOP { break; }
                        if ft3 == T_STRUCT {
                            col = Some(read_typed_column(r, fid3)?);
                        } else {
                            r.skip(ft3)?;
                        }
                    }
                    if let Some(c) = col { columns.push(c); }
                }
            }
            _ => r.skip(ft)?,
        }
    }
    Ok(())
}

/// Read a TXxxColumn struct: field 1 = list<T> values, field 2 = binary nulls.
fn read_typed_column(r: &mut ThriftReader, col_field_id: i16) -> Result<ThriftColData> {
    // Preallocate based on column type — we'll know the count from the list header.
    let mut nulls: Vec<u8> = Vec::new();

    enum RawCol {
        Bool(Vec<bool>),
        Byte(Vec<i8>),
        I16(Vec<i16>),
        I32(Vec<i32>),
        I64(Vec<i64>),
        Double(Vec<f64>),
        Str(Vec<Bytes>),
        Empty,
    }
    let mut raw = RawCol::Empty;

    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_LIST, 1) => {
                let elem_type = r.buf.get_u8();
                let count = r.buf.get_i32() as usize;
                match elem_type {
                    T_BOOL => {
                        let mut v = Vec::with_capacity(count);
                        for _ in 0..count { v.push(r.read_bool()); }
                        raw = RawCol::Bool(v);
                    }
                    T_BYTE => {
                        let mut v = Vec::with_capacity(count);
                        for _ in 0..count { v.push(r.buf.get_i8()); }
                        raw = RawCol::Byte(v);
                    }
                    T_I16 => {
                        let mut v = Vec::with_capacity(count);
                        for _ in 0..count { v.push(r.buf.get_i16()); }
                        raw = RawCol::I16(v);
                    }
                    T_I32 => {
                        let mut v = Vec::with_capacity(count);
                        for _ in 0..count { v.push(r.buf.get_i32()); }
                        raw = RawCol::I32(v);
                    }
                    T_I64 => {
                        let mut v = Vec::with_capacity(count);
                        for _ in 0..count { v.push(r.buf.get_i64()); }
                        raw = RawCol::I64(v);
                    }
                    T_DOUBLE => {
                        let mut v = Vec::with_capacity(count);
                        for _ in 0..count { v.push(f64::from_bits(r.buf.get_u64())); }
                        raw = RawCol::Double(v);
                    }
                    T_STRING => {
                        // Zero-copy: keep as Bytes slices
                        let mut v = Vec::with_capacity(count);
                        for _ in 0..count { v.push(r.read_string_bytes()); }
                        raw = RawCol::Str(v);
                    }
                    _ => bail!("Unknown list elem type {elem_type} in TColumn"),
                }
            }
            (T_STRING, 2) => { nulls = r.read_bytes()?; }
            _ => r.skip(ft)?,
        }
    }

    // Map raw + nulls into ThriftColData based on the TColumn field id
    // (determines the semantic type regardless of wire type).
    Ok(match (col_field_id, raw) {
        (1, RawCol::Bool(v))   => ThriftColData::Bool   { values: v, nulls },
        (2, RawCol::Byte(v))   => ThriftColData::Byte   { values: v, nulls },
        (3, RawCol::I16(v))    => ThriftColData::I16    { values: v, nulls },
        (4, RawCol::I32(v))    => ThriftColData::I32    { values: v, nulls },
        (5, RawCol::I64(v))    => ThriftColData::I64    { values: v, nulls },
        (6, RawCol::Double(v)) => ThriftColData::Double { values: v, nulls },
        (7, RawCol::Str(v)) | (8, RawCol::Str(v)) => ThriftColData::Str { values: v, nulls },
        _ => bail!("Unexpected TColumn sub-field id {col_field_id} or type mismatch"),
    })
}

// ── CloseOperation / CloseSession ────────────────────────────────────────────

pub fn build_close_operation(op: &OperationHandle) -> Bytes {
    let mut w = ThriftWriter::with_capacity(64);
    w.write_message_begin(CLOSE_OPERATION, 1, 5);
    w.write_field_begin(T_STRUCT, 1);
    w.write_field_begin(T_STRUCT, 1);
    write_operation_handle(&mut w, op);
    w.write_field_stop();
    w.write_field_stop();
    w.finish()
}

pub fn build_close_session(session: &SessionHandle) -> Bytes {
    let mut w = ThriftWriter::with_capacity(64);
    w.write_message_begin(CLOSE_SESSION, 1, 6);
    w.write_field_begin(T_STRUCT, 1);
    w.write_field_begin(T_STRUCT, 1);
    write_session_handle(&mut w, session);
    w.write_field_stop();
    w.write_field_stop();
    w.finish()
}

// ── Shared serialisation helpers ─────────────────────────────────────────────

fn write_handle_identifier(w: &mut ThriftWriter, guid: &[u8], secret: &[u8]) {
    w.write_field_begin(T_STRING, 1); w.write_bytes(guid);
    w.write_field_begin(T_STRING, 2); w.write_bytes(secret);
    w.write_field_stop();
}

fn write_session_handle(w: &mut ThriftWriter, s: &SessionHandle) {
    w.write_field_begin(T_STRUCT, 1);
    write_handle_identifier(w, &s.guid, &s.secret);
    w.write_field_stop();
}

fn write_operation_handle(w: &mut ThriftWriter, op: &OperationHandle) {
    w.write_field_begin(T_STRUCT, 1);
    write_handle_identifier(w, &op.guid, &op.secret);
    w.write_field_begin(T_I32, 2); w.write_i32(op.op_type);
    w.write_field_begin(T_BOOL, 3); w.write_bool(op.has_result_set);
    w.write_field_stop();
}

/// Read a TSessionHandle / TOperationHandle's inner THandleIdentifier.
fn read_handle_identifier(r: &mut ThriftReader) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut guid = Vec::new();
    let mut secret = Vec::new();
    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_STRUCT, 1) => {
                loop {
                    let (ft2, fid2) = r.read_field_begin()?;
                    if ft2 == T_STOP { break; }
                    match (ft2, fid2) {
                        (T_STRING, 1) => guid = r.read_bytes()?,
                        (T_STRING, 2) => secret = r.read_bytes()?,
                        _ => r.skip(ft2)?,
                    }
                }
            }
            _ => r.skip(ft)?,
        }
    }
    Ok((guid, secret))
}

fn parse_operation_handle_from(r: &mut ThriftReader, call: &str) -> Result<OperationHandle> {
    let mut op: Option<OperationHandle> = None;
    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_STRUCT, 1) => check_status(r, call)?,
            (T_STRUCT, 2) => {
                let mut guid = Vec::new();
                let mut secret = Vec::new();
                let mut op_type = 0i32;
                let mut has_result = false;
                loop {
                    let (ft2, fid2) = r.read_field_begin()?;
                    if ft2 == T_STOP { break; }
                    match (ft2, fid2) {
                        (T_STRUCT, 1) => {
                            loop {
                                let (ft3, fid3) = r.read_field_begin()?;
                                if ft3 == T_STOP { break; }
                                match (ft3, fid3) {
                                    (T_STRING, 1) => guid = r.read_bytes()?,
                                    (T_STRING, 2) => secret = r.read_bytes()?,
                                    _ => r.skip(ft3)?,
                                }
                            }
                        }
                        (T_I32, 2) => op_type = r.read_i32(),
                        (T_BOOL, 3) => has_result = r.read_bool(),
                        _ => r.skip(ft2)?,
                    }
                }
                op = Some(OperationHandle { guid, secret, op_type, has_result_set: has_result });
            }
            _ => r.skip(ft)?,
        }
    }
    op.ok_or_else(|| anyhow!("No operation handle in {call} response"))
}

/// Read a TStatus struct and bail if statusCode > 1 (0=SUCCESS, 1=SUCCESS_WITH_INFO).
fn check_status(r: &mut ThriftReader, call: &str) -> Result<()> {
    let mut code = -1i32;
    let mut msg = String::new();
    loop {
        let (ft, fid) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        match (ft, fid) {
            (T_I32, 1) => code = r.read_i32(),
            (T_STRING, 3) => msg = r.read_string()?,
            _ => r.skip(ft)?,
        }
    }
    if code > 1 {
        bail!("{call} failed (statusCode={code}): {msg}");
    }
    Ok(())
}

fn skip_struct(r: &mut ThriftReader) -> Result<()> {
    loop {
        let (ft, _) = r.read_field_begin()?;
        if ft == T_STOP { break; }
        r.skip(ft)?;
    }
    Ok(())
}