//! Thrift Binary Protocol primitives — writer and reader.
//!
//! Implements the strict binary framing used by Databricks SQL warehouses
//! (same wire format as the Python `databricks-sql-connector`).

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

// ── Thrift binary protocol type tags ─────────────────────────────────────────

pub const T_STOP: u8 = 0;
pub const T_VOID: u8 = 1;
pub const T_BOOL: u8 = 2;
pub const T_BYTE: u8 = 3;
pub const T_DOUBLE: u8 = 4;
pub const T_I16: u8 = 6;
pub const T_I32: u8 = 8;
pub const T_I64: u8 = 10;
pub const T_STRING: u8 = 11; // also covers BINARY
pub const T_STRUCT: u8 = 12;
pub const T_MAP: u8 = 13;
pub const T_SET: u8 = 14;
pub const T_LIST: u8 = 15;

// ── ThriftWriter ─────────────────────────────────────────────────────────────

pub struct ThriftWriter {
    buf: BytesMut,
}

impl ThriftWriter {
    /// Create with pre-allocated capacity (avoids early reallocations).
    pub fn with_capacity(cap: usize) -> Self {
        Self { buf: BytesMut::with_capacity(cap) }
    }

    pub fn new() -> Self {
        Self::with_capacity(256)
    }

    /// Strict binary framing: 0x80010000 | msg_type, then name, then seq_id.
    pub fn write_message_begin(&mut self, name: &str, msg_type: u8, seq_id: i32) {
        let header: u32 = 0x8001_0000 | (msg_type as u32);
        self.buf.put_u32(header);
        self.write_string(name);
        self.buf.put_i32(seq_id);
    }

    #[inline]
    pub fn write_field_begin(&mut self, field_type: u8, field_id: i16) {
        self.buf.put_u8(field_type);
        self.buf.put_i16(field_id);
    }

    #[inline]
    pub fn write_field_stop(&mut self) {
        self.buf.put_u8(T_STOP);
    }

    #[inline]
    pub fn write_string(&mut self, s: &str) {
        self.buf.put_i32(s.len() as i32);
        self.buf.put(s.as_bytes());
    }

    #[inline]
    pub fn write_bytes(&mut self, b: &[u8]) {
        self.buf.put_i32(b.len() as i32);
        self.buf.put(b);
    }

    #[inline]
    pub fn write_i32(&mut self, v: i32) { self.buf.put_i32(v); }
    #[inline]
    pub fn write_i64(&mut self, v: i64) { self.buf.put_i64(v); }
    #[inline]
    pub fn write_bool(&mut self, v: bool) { self.buf.put_u8(u8::from(v)); }

    pub fn write_map_begin(&mut self, key_type: u8, val_type: u8, size: i32) {
        self.buf.put_u8(key_type);
        self.buf.put_u8(val_type);
        self.buf.put_i32(size);
    }

    pub fn finish(self) -> Bytes { self.buf.freeze() }
}

// ── ThriftReader ─────────────────────────────────────────────────────────────

pub struct ThriftReader {
    pub buf: Bytes,
}

impl ThriftReader {
    pub fn new(buf: Bytes) -> Self { Self { buf } }

    #[inline]
    pub fn remaining(&self) -> usize { self.buf.remaining() }

    pub fn read_message_begin(&mut self) -> Result<(String, u8, i32)> {
        let first = self.buf.get_u32();
        if first & 0x8000_0000 == 0 {
            bail!("Old-style Thrift framing is not supported");
        }
        let msg_type = (first & 0xFF) as u8;
        let name = self.read_string()?;
        let seq_id = self.buf.get_i32();
        Ok((name, msg_type, seq_id))
    }

    #[inline]
    pub fn read_field_begin(&mut self) -> Result<(u8, i16)> {
        let ft = self.buf.get_u8();
        if ft == T_STOP { return Ok((T_STOP, 0)); }
        let fid = self.buf.get_i16();
        Ok((ft, fid))
    }

    pub fn read_string(&mut self) -> Result<String> {
        let len = self.buf.get_i32() as usize;
        let bytes = self.buf.copy_to_bytes(len);
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    /// Read a string as zero-copy Bytes (avoids UTF-8 validation + allocation
    /// when the caller only needs raw bytes, e.g. for Arrow StringArray).
    #[inline]
    pub fn read_string_bytes(&mut self) -> Bytes {
        let len = self.buf.get_i32() as usize;
        self.buf.copy_to_bytes(len)
    }

    pub fn read_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.buf.get_i32() as usize;
        Ok(self.buf.copy_to_bytes(len).to_vec())
    }

    /// Read a binary field as zero-copy `Bytes` — avoids the `.to_vec()`
    /// allocation compared to `read_bytes`.
    #[inline]
    pub fn read_bytes_zero_copy(&mut self) -> Bytes {
        let len = self.buf.get_i32() as usize;
        self.buf.copy_to_bytes(len)
    }

    #[inline]
    pub fn read_i32(&mut self) -> i32 { self.buf.get_i32() }
    #[inline]
    pub fn read_i64(&mut self) -> i64 { self.buf.get_i64() }
    #[inline]
    pub fn read_bool(&mut self) -> bool { self.buf.get_u8() != 0 }

    /// Skip a value of the given type.
    pub fn skip(&mut self, type_tag: u8) -> Result<()> {
        match type_tag {
            T_BOOL | T_BYTE => { self.buf.advance(1); }
            T_I16  => { self.buf.advance(2); }
            T_I32  => { self.buf.advance(4); }
            T_I64 | T_DOUBLE => { self.buf.advance(8); }
            T_STRING => {
                let len = self.buf.get_i32() as usize;
                self.buf.advance(len);
            }
            T_STRUCT => {
                loop {
                    let (ft, _) = self.read_field_begin()?;
                    if ft == T_STOP { break; }
                    self.skip(ft)?;
                }
            }
            T_MAP => {
                let kt = self.buf.get_u8();
                let vt = self.buf.get_u8();
                let size = self.buf.get_i32();
                for _ in 0..size { self.skip(kt)?; self.skip(vt)?; }
            }
            T_LIST | T_SET => {
                let et = self.buf.get_u8();
                let size = self.buf.get_i32();
                for _ in 0..size { self.skip(et)?; }
            }
            _ => bail!("Unknown Thrift type tag: {}", type_tag),
        }
        Ok(())
    }
}