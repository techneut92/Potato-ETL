//! Percent-encoding helpers for connection URLs (RFC 3986 §3.2.1).

/// Percent-encodes a username or password for use in the `user:pass@` part of
/// a connection URL (RFC 3986 §3.2.1 userinfo component).
///
/// Characters that are **not** encoded (unreserved + safe sub-delimiters):
/// `A-Z a-z 0-9 - . _ ~ ! $ & ' ( ) * + , ; =`
///
/// Characters that **are** encoded (among others): `@ : / # ? [ ] % space`
///
/// This means a password of `"p@ss:w/rd#1%2"` becomes
/// `"p%40ss%3Aw%2Frd%231%252"` and round-trips correctly through the
/// driver-side URL parsers.
pub fn pct_encode(s: &str) -> String {
    // Safe set: unreserved chars + sub-delims that are unambiguous in userinfo.
    // Notably absent: `:` (user/pass split), `@` (userinfo/host split),
    // `/` (path split), `#` (fragment split), `?` (query split),
    // `[` `]` (IP-literal brackets), `%` (would double-encode).
    const SAFE: &[u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$&'()*+,;=";

    let mut out = String::with_capacity(s.len() + 8);
    for byte in s.bytes() {
        if SAFE.contains(&byte) {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            write!(out, "%{byte:02X}").unwrap();
        }
    }
    out
}

/// Decodes a percent-encoded string (e.g. from the userinfo part of a URL).
///
/// `%XX` sequences are decoded to their byte values; the resulting byte
/// sequence is interpreted as UTF-8.  Invalid percent sequences are left
/// as-is (best-effort).
pub fn pct_decode(s: &str) -> anyhow::Result<String> {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            // Safety: next two bytes are ASCII hex digits (or we fall through).
            if let Ok(hex_str) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(byte) = u8::from_str_radix(hex_str, 16) {
                    out.push(byte);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out)
        .map_err(|e| anyhow::anyhow!("Invalid UTF-8 after percent-decoding '{s}': {e}"))
}
