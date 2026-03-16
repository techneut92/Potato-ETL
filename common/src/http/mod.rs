//! REST API source and sink component.
//!
//! # Source (`fetch_rest_api`)
//! Reads data from an HTTP endpoint and converts JSON rows to Arrow `RecordBatch`es.
//!
//! # Sink (`send_to_rest_api`)
//! Sends Arrow `RecordBatch`es as JSON to an HTTP endpoint.
//! Each row (per-row mode) or batch (batch mode) is sent as a separate request.
//!
//! # Pagination (source)
//!
//! | Strategy     | Description                                               |
//! |--------------|-----------------------------------------------------------|
//! | `None`       | Single request                                            |
//! | `Cursor`     | Cursor in response body → query parameter on next request |
//! | `Offset`     | Increment `offset`/`limit` until empty response           |
//! | `Page`       | Increment page number until empty response                |
//! | `LinkHeader` | Follow RFC 5988 `Link: <url>; rel="next"` header          |
//!
//! ## Offset / Page pagination and insertion drift
//!
//! **Problem:** If new records are inserted at the source while the ETL is
//! paginating, the offset shifts.  Records already fetched may re-appear on
//! the next page, and records that shift into a past page are silently skipped.
//!
//! **Recommendations (strongest first):**
//!
//! 1. **Prefer `Cursor` or `LinkHeader` pagination** — the server controls
//!    the position so insertions at the source do not affect your cursor.
//! 2. **Set `dedup_key`** — when `Offset` or `Page` pagination is the only
//!    option, provide a unique field name (e.g. `"id"`) so that duplicate
//!    records fetched across page boundaries are dropped before the data
//!    reaches downstream transforms and the sink.
//! 3. **Use `total_count_path`** — if the API includes a total-record-count
//!    field in its response (e.g. `"meta.total"` or `"count"`), set this path
//!    so that pagination stops as soon as the expected number of distinct rows
//!    have been collected, instead of relying on an empty final page.
//! 4. **Use `has_more_path`** — set the path to a boolean `"has_more"` field
//!    (e.g. `"pagination.has_more"`) for a more reliable stop condition than
//!    an empty page.
//!
//! # Authentication
//!
//! | Type       | Header                                                      |
//! |------------|-------------------------------------------------------------|
//! | `Bearer`   | `Authorization: Bearer <token>`                             |
//! | `Basic`    | `Authorization: Basic <base64(user:pass)>`                  |
//! | `ApiKey`   | Arbitrary header (e.g. `X-API-Key: <key>`)                  |
//! |            | Use `ApiKey` for custom schemes such as AFAS:               |
//! |            | `header="Authorization"`, `key="AfasToken <base64(xml)>"`   |

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use base64::Engine as _;
use reqwest::header::{HeaderMap, HeaderName, AUTHORIZATION};
use serde::{Deserialize, Serialize};

// AuthConfig is defined in config (shared with connection params); re-export
// here so existing imports of `potato_etl_runtime::http::AuthConfig` continue to work.
pub use crate::config::AuthConfig;

// ── HTTP method ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod { #[default] Get, Post, Put, Patch, Delete }

impl HttpMethod {
    fn to_reqwest(&self) -> reqwest::Method {
        match self {
            Self::Get    => reqwest::Method::GET,
            Self::Post   => reqwest::Method::POST,
            Self::Put    => reqwest::Method::PUT,
            Self::Patch  => reqwest::Method::PATCH,
            Self::Delete => reqwest::Method::DELETE,
        }
    }
}

// ── Pagination ────────────────────────────────────────────────────────────────

/// Pagination strategy for the REST API source.
///
/// # Choosing the right strategy
///
/// - **`Cursor`** — safest for live APIs; server-side cursor is unaffected by
///   concurrent inserts.
/// - **`LinkHeader`** — follow RFC 5988 `Link: <url>; rel="next"` headers;
///   also immune to insertion drift.
/// - **`Offset` / `Page`** — use only when no cursor/link-header option exists.
///   Always set [`RestApiOptions::dedup_key`] when using these strategies on a
///   source that receives concurrent writes.
///
/// See the module-level documentation for a full discussion of insertion drift
/// and mitigation options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum PaginationConfig {
    #[default]
    None,
    Cursor {
        /// JSONPath-like dot-notation path to the cursor value in the response.
        /// Example: `"meta.next_cursor"` or `"pagination.cursor"`.
        cursor_path:  String,
        /// Query parameter name to pass the cursor on the next request.
        cursor_param: String,
        page_size:    usize,
        #[serde(default)]
        size_param:   Option<String>,
    },
    Offset {
        offset_param: String,
        limit_param:  String,
        page_size:    usize,
        /// Dot-notation path to a total-record-count field in the response body.
        #[serde(default)]
        total_count_path: Option<String>,
        /// Dot-notation path to a boolean `"has_more"` field in the response.
        #[serde(default)]
        has_more_path: Option<String>,
    },
    Page {
        page_param: String,
        size_param: String,
        page_size:  usize,
        first_page: usize,
        /// See `Offset::total_count_path`.
        #[serde(default)]
        total_count_path: Option<String>,
        /// See `Offset::has_more_path`.
        #[serde(default)]
        has_more_path: Option<String>,
    },
    /// Follow RFC 5988 `Link: <url>; rel="next"` headers.
    /// Immune to insertion drift because the server controls the next URL.
    LinkHeader,
}

impl PaginationConfig {
    /// Returns the number of rows to request per HTTP call, or `None` for
    /// strategies where the page size is server-controlled (`LinkHeader`) or
    /// pagination is disabled (`None`).
    pub fn page_size(&self) -> Option<usize> {
        match self {
            Self::Cursor { page_size, .. } => Some(*page_size),
            Self::Offset { page_size, .. } => Some(*page_size),
            Self::Page   { page_size, .. } => Some(*page_size),
            Self::None | Self::LinkHeader  => Option::None,
        }
    }
}

// ── Shared HTTP connection fields ────────────────────────────────────────────

/// Fields shared between REST API source and sink options.
///
/// Implement this trait to enable `merge_rest_conn_defaults()` on any
/// REST options struct, eliminating duplicate merge logic.
pub trait RestHttpCommon {
    fn auth_mut(&mut self)           -> &mut Option<AuthConfig>;
    fn headers_mut(&mut self)        -> &mut std::collections::HashMap<String, String>;
    fn timeout_secs_mut(&mut self)   -> &mut Option<u64>;
    fn rate_limit_rps_mut(&mut self) -> &mut Option<f64>;

    /// Merges connection-level defaults into step-level options.
    ///
    /// - `auth`:           step wins if set; otherwise connection default.
    /// - `headers`:        connection defaults first, step headers override on conflict.
    /// - `timeout_secs`:   connection value used when step has no explicit timeout.
    /// - `rate_limit_rps`: connection value used when step has no explicit limit.
    fn merge_conn_defaults(
        &mut self,
        conn_auth:     &Option<AuthConfig>,
        conn_headers:  &std::collections::HashMap<String, String>,
        conn_timeout:  u64,
        conn_rate_rps: &Option<f64>,
    ) {
        if self.auth_mut().is_none() {
            *self.auth_mut() = conn_auth.clone();
        }
        let mut merged = conn_headers.clone();
        merged.extend(self.headers_mut().drain());
        *self.headers_mut() = merged;
        if self.timeout_secs_mut().is_none() {
            *self.timeout_secs_mut() = Some(conn_timeout);
        }
        if self.rate_limit_rps_mut().is_none() {
            *self.rate_limit_rps_mut() = conn_rate_rps.clone();
        }
    }
}

// ── Source options ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RestApiOptions {
    #[serde(default)]
    pub method:         HttpMethod,
    #[serde(default)]
    pub auth:           Option<AuthConfig>,
    #[serde(default)]
    pub headers:        std::collections::HashMap<String, String>,
    #[serde(default)]
    pub params:         std::collections::HashMap<String, String>,
    #[serde(default)]
    pub body:           Option<String>,
    /// JSONPath-like pointer to the array of rows in the response.
    /// Empty string / None = top-level array.
    #[serde(default)]
    pub data_path:      Option<String>,
    #[serde(default)]
    pub pagination:     PaginationConfig,
    /// Maximum requests per second (rate limiting). None = unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_rps: Option<f64>,
    /// Per-request timeout in seconds. Default: 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs:   Option<u64>,
    /// If true, non-2xx responses are treated as empty (not as errors).
    #[serde(default)]
    pub allow_non_2xx:  bool,
    /// Field name to use for cross-page deduplication.
    ///
    /// **Required when using `Offset` or `Page` pagination on a live source.**
    #[serde(default)]
    pub dedup_key:      Option<String>,
}

impl RestHttpCommon for RestApiOptions {
    fn auth_mut(&mut self)           -> &mut Option<AuthConfig> { &mut self.auth }
    fn headers_mut(&mut self)        -> &mut std::collections::HashMap<String, String> { &mut self.headers }
    fn timeout_secs_mut(&mut self)   -> &mut Option<u64> { &mut self.timeout_secs }
    fn rate_limit_rps_mut(&mut self) -> &mut Option<f64> { &mut self.rate_limit_rps }
}

// ── Sink write mode
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkWriteMode {
    /// Send each row as a separate request.
    #[default]
    PerRow,
    /// Send each batch as one request (array body).
    Batch,
}

// ── Sink options ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RestApiSinkOptions {
    #[serde(default)]
    pub method:         HttpMethod,
    #[serde(default)]
    pub auth:           Option<AuthConfig>,
    #[serde(default)]
    pub headers:        std::collections::HashMap<String, String>,
    #[serde(default)]
    pub mode:           SinkWriteMode,
    /// Field mapping for nested JSON construction (dot-notation paths).
    /// Empty = use all columns as flat JSON keys.
    #[serde(default)]
    pub field_map:      std::collections::HashMap<String, String>,
    /// Name of the pre-built JSON column (from `build_objects`).
    #[serde(default)]
    pub json_column:    Option<String>,
    /// Wrap the array in this key when mode=Batch. None = bare array.
    #[serde(default)]
    pub wrap_key:       Option<String>,
    /// URL template with `{column_name}` placeholders (used in PerRow mode).
    #[serde(default)]
    pub url_template:   Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_rps: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs:   Option<u64>,
    #[serde(default)]
    pub allow_non_2xx:  bool,
}

impl RestHttpCommon for RestApiSinkOptions {
    fn auth_mut(&mut self)           -> &mut Option<AuthConfig> { &mut self.auth }
    fn headers_mut(&mut self)        -> &mut std::collections::HashMap<String, String> { &mut self.headers }
    fn timeout_secs_mut(&mut self)   -> &mut Option<u64> { &mut self.timeout_secs }
    fn rate_limit_rps_mut(&mut self) -> &mut Option<f64> { &mut self.rate_limit_rps }
}

// ── Source implementation ─────────────────────────────────────────────────────

/// Fetch data from a REST API and return as a list of `RecordBatch`es.
pub async fn fetch_rest_api(
    url:        &str,
    opts:       &RestApiOptions,
    batch_size: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    let effective_batch_size = batch_size.max(1);

    // Build client with auth + custom headers.
    let default_headers = build_auth_headers(opts.auth.as_ref())?;
    let mut header_builder = default_headers;
    for (k, v) in &opts.headers {
        if let (Ok(name), Ok(val)) = (k.parse::<HeaderName>(), v.parse()) {
            header_builder.insert(name, val);
        }
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(opts.timeout_secs.unwrap_or(30)))
        .default_headers(header_builder)
        .build()?;

    let rate_interval = opts.rate_limit_rps
        .map(|rps| Duration::from_secs_f64(1.0 / rps.max(f64::EPSILON)));

    let mut row_buffer: Vec<serde_json::Value> = Vec::with_capacity(effective_batch_size);
    let mut output:     Vec<RecordBatch>       = Vec::new();
    let mut seen_keys:  HashSet<String>        = HashSet::new();

    // Pagination state.
    let mut offset:   usize          = 0;
    let mut page_num: usize          = match &opts.pagination {
        PaginationConfig::Page { first_page, .. } => *first_page,
        _ => 1,
    };
    let mut cursor:   Option<String> = None;
    let mut next_url: Option<String> = None;
    let mut done = false;
    // used to skip the inter-request sleep before the very first call
    let mut first_request = true;

    while !done {
        if !first_request {
            if let Some(interval) = rate_interval {
                tokio::time::sleep(interval).await;
            }
        }
        first_request = false;

        let request_url = next_url.take().unwrap_or_else(|| url.to_string());
        let mut req = client.request(opts.method.to_reqwest(), &request_url);

        // Static query params.
        if !opts.params.is_empty() {
            req = req.query(&opts.params.iter().collect::<Vec<_>>());
        }

        // Pagination query params.
        match &opts.pagination {
            PaginationConfig::Offset { offset_param, limit_param, page_size, .. } => {
                req = req.query(&[
                    (offset_param.as_str(), offset.to_string()),
                    (limit_param.as_str(),  page_size.to_string()),
                ]);
            }
            PaginationConfig::Page { page_param, size_param, page_size, .. } => {
                req = req.query(&[
                    (page_param.as_str(), page_num.to_string()),
                    (size_param.as_str(), page_size.to_string()),
                ]);
            }
            PaginationConfig::Cursor { cursor_param, size_param, page_size, .. } => {
                if let Some(ref c) = cursor {
                    req = req.query(&[(cursor_param.as_str(), c.as_str())]);
                }
                if let Some(sp) = size_param {
                    req = req.query(&[(sp.as_str(), page_size.to_string())]);
                }
            }
            _ => {}
        }

        if let Some(ref body_str) = opts.body {
            req = req.body(body_str.clone());
        }

        let response = req.send().await
            .map_err(|e| anyhow::anyhow!("HTTP request failed ({}): {e}", request_url))?;

        if !response.status().is_success() {
            if opts.allow_non_2xx { break; }
            return Err(anyhow::anyhow!(
                "HTTP {} for {}", response.status(), request_url
            ));
        }

        // Capture Link header BEFORE consuming body.
        let link_next: Option<String> = match &opts.pagination {
            PaginationConfig::LinkHeader => extract_link_next(response.headers()),
            _ => None,
        };

        let body: serde_json::Value = response.json().await
            .map_err(|e| anyhow::anyhow!("JSON parse error from {request_url}: {e}"))?;

        let rows_arr: Vec<serde_json::Value> = {
            let val = extract_json_path(&body, opts.data_path.as_deref());
            match val {
                serde_json::Value::Array(arr) => arr,
                _ => vec![],
            }
        };
        let page_row_count = rows_arr.len();

        // Dedup and push to row_buffer.
        for row in rows_arr {
            if let Some(ref key_field) = opts.dedup_key {
                let key_str = row.get(key_field)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_default();
                if !seen_keys.insert(key_str) {
                    continue; // duplicate — skip
                }
            }
            row_buffer.push(row);

            // Flush a RecordBatch whenever the buffer is full.
            if row_buffer.len() >= effective_batch_size {
                let batch_rows: Vec<_> = row_buffer.drain(..effective_batch_size).collect();
                output.push(crate::util::arrow::json_rows_to_record_batch(&batch_rows));
            }
        }

        // Advance pagination / determine termination.
        match &opts.pagination {
            PaginationConfig::None => {
                done = true;
            }
            PaginationConfig::LinkHeader => {
                match link_next {
                    Some(next) => next_url = Some(next),
                    None       => done = true,
                }
            }
            PaginationConfig::Cursor { cursor_path, page_size, .. } => {
                let new_cursor_val = extract_json_path(&body, Some(cursor_path));
                let new_cursor = new_cursor_val.as_str().map(|s| s.to_string());
                if page_row_count == 0 || page_row_count < *page_size || new_cursor.is_none() {
                    done = true;
                } else {
                    cursor = new_cursor;
                }
            }
            PaginationConfig::Offset { page_size, total_count_path, has_more_path, .. } => {
                offset += page_row_count;
                done = page_row_count == 0
                    || page_row_count < *page_size
                    || has_more_is_false(&body, has_more_path.as_deref())
                    || total_count_reached(&body, total_count_path.as_deref(), seen_keys.len(), opts.dedup_key.is_some(), offset);
            }
            PaginationConfig::Page { page_size, total_count_path, has_more_path, .. } => {
                offset += page_row_count;
                page_num += 1;
                done = page_row_count == 0
                    || page_row_count < *page_size
                    || has_more_is_false(&body, has_more_path.as_deref())
                    || total_count_reached(&body, total_count_path.as_deref(), seen_keys.len(), opts.dedup_key.is_some(), offset);
            }
        }
    }

    // Final flush of any remaining rows.
    if !row_buffer.is_empty() {
        output.push(crate::util::arrow::json_rows_to_record_batch(&row_buffer));
    }

    Ok(output)
}

// ── Source helpers ────────────────────────────────────────────────────────────

/// Build a `HeaderMap` from an `AuthConfig`.
fn build_auth_headers(auth: Option<&AuthConfig>) -> anyhow::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    if let Some(auth) = auth {
        match auth {
            AuthConfig::Bearer { token } => {
                headers.insert(AUTHORIZATION, format!("Bearer {token}").parse()?);
            }
            AuthConfig::Basic { username, password } => {
                let encoded = base64::engine::general_purpose::STANDARD
                    .encode(format!("{username}:{password}"));
                headers.insert(AUTHORIZATION, format!("Basic {encoded}").parse()?);
            }
            AuthConfig::ApiKey { header, key } => {
                headers.insert(header.parse::<HeaderName>()?, key.parse()?);
            }
        }
    }
    Ok(headers)
}

/// Dot-notation path descent: `"meta.next_cursor"` → `body["meta"]["next_cursor"]`.
///
/// Returns a clone of the matched value, or `Value::Null` if the path is absent.
fn extract_json_path(value: &serde_json::Value, path: Option<&str>) -> serde_json::Value {
    let path = match path {
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

/// Parse `rel="next"` from a `Link` response header (RFC 5988).
fn extract_link_next(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get(reqwest::header::LINK)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            s.split(',')
                .find(|part| part.contains("rel=\"next\""))
                .and_then(|part| {
                    let start = part.find('<')? + 1;
                    let end   = part.find('>')?;
                    Some(part[start..end].trim().to_string())
                })
        })
}

/// Returns `true` when the `has_more` field at `path` is `false` (or `0`).
fn has_more_is_false(body: &serde_json::Value, path: Option<&str>) -> bool {
    match path {
        Some(p) => {
            let val = extract_json_path(body, Some(p));
            val == serde_json::Value::Bool(false)
                || val.as_u64() == Some(0)
        }
        None => false,
    }
}

/// Returns `true` when the seen/offset count has reached the API's total.
fn total_count_reached(
    body:      &serde_json::Value,
    path:      Option<&str>,
    seen:      usize,
    use_dedup: bool,
    offset:    usize,
) -> bool {
    match path {
        Some(p) => {
            let val = extract_json_path(body, Some(p));
            if let Some(total) = val.as_u64() {
                let current = if use_dedup { seen } else { offset };
                current >= total as usize
            } else {
                false
            }
        }
        None => false,
    }
}

// ── Sink implementation ───────────────────────────────────────────────────────

/// Send `RecordBatch`es to a REST API.
///
/// ## Write modes
///
/// | `mode`     | Behaviour                                                  |
/// |------------|----------------------------------------------------------  |
/// | `PerRow`   | One HTTP request per row; URL template placeholders filled |
/// | `Batch`    | One HTTP request per `RecordBatch`; body = JSON array      |
///
/// Returns the total number of rows sent.
pub async fn send_to_rest_api(
    batches: &[RecordBatch],
    url:     &str,
    opts:    &RestApiSinkOptions,
) -> anyhow::Result<usize> {
    let mut default_headers = build_auth_headers(opts.auth.as_ref())?;
    for (k, v) in &opts.headers {
        if let (Ok(name), Ok(val)) = (k.parse::<HeaderName>(), v.parse()) {
            default_headers.insert(name, val);
        }
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(opts.timeout_secs.unwrap_or(30)))
        .default_headers(default_headers)
        .build()?;

    let rate_interval = opts.rate_limit_rps
        .map(|rps| Duration::from_secs_f64(1.0 / rps.max(f64::EPSILON)));

    let mut total_sent = 0usize;

    match opts.mode {
        SinkWriteMode::Batch => {
            for (i, batch) in batches.iter().enumerate() {
                if i > 0 {
                    if let Some(iv) = rate_interval { tokio::time::sleep(iv).await; }
                }
                let rows = record_batch_to_sink_rows(batch, &opts.field_map, opts.json_column.as_deref());
                let body: serde_json::Value = match &opts.wrap_key {
                    Some(key) => serde_json::json!({ key: rows }),
                    None      => serde_json::Value::Array(rows),
                };

                let resp = client.request(opts.method.to_reqwest(), url)
                    .json(&body)
                    .send().await
                    .map_err(|e| anyhow::anyhow!("Batch sink request failed: {e}"))?;

                if !resp.status().is_success() && !opts.allow_non_2xx {
                    return Err(anyhow::anyhow!("Batch sink HTTP {}: {url}", resp.status()));
                }
                total_sent += batch.num_rows();
            }
        }

        SinkWriteMode::PerRow => {
            let mut first_row = true;
            for batch in batches {
                let rows = record_batch_to_sink_rows(batch, &opts.field_map, opts.json_column.as_deref());
                for row in rows {
                    if !first_row {
                        if let Some(iv) = rate_interval { tokio::time::sleep(iv).await; }
                    }
                    first_row = false;

                    let row_url = match &opts.url_template {
                        Some(template) => interpolate_url(template, &row),
                        None           => url.to_string(),
                    };

                    let resp = client.request(opts.method.to_reqwest(), &row_url)
                        .json(&row)
                        .send().await
                        .map_err(|e| anyhow::anyhow!("PerRow sink request failed: {e}"))?;

                    if !resp.status().is_success() && !opts.allow_non_2xx {
                        return Err(anyhow::anyhow!("PerRow sink HTTP {}: {row_url}", resp.status()));
                    }
                    total_sent += 1;
                }
            }
        }
    }

    Ok(total_sent)
}

// ── Sink helpers ──────────────────────────────────────────────────────────────

/// Convert a `RecordBatch` to a `Vec<serde_json::Value>` for HTTP sink output.
///
/// Respects `field_map` (rename columns for the outgoing JSON) and
/// `json_column` (use a pre-built JSON column directly, bypassing conversion).
fn record_batch_to_sink_rows(
    batch:       &RecordBatch,
    field_map:   &HashMap<String, String>,
    json_column: Option<&str>,
) -> Vec<serde_json::Value> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    // If a pre-built JSON column is specified, use it directly.
    if let Some(col_name) = json_column {
        if let Ok(idx) = batch.schema().index_of(col_name) {
            let col  = batch.column(idx);
            let opts = FormatOptions::default();
            if let Ok(fmt) = ArrayFormatter::try_new(col.as_ref(), &opts) {
                return (0..batch.num_rows())
                    .filter_map(|row| {
                        if col.is_null(row) { return None; }
                        serde_json::from_str(&fmt.value(row).to_string()).ok()
                    })
                    .collect();
            }
        }
    }

    let schema = batch.schema();
    let opts   = FormatOptions::default();
    let formatters: Vec<Option<ArrayFormatter>> = (0..batch.num_columns())
        .map(|ci| ArrayFormatter::try_new(batch.column(ci).as_ref(), &opts).ok())
        .collect();

    (0..batch.num_rows()).map(|row| {
        let mut obj = serde_json::Map::new();
        for (ci, field) in schema.fields().iter().enumerate() {
            let col = batch.column(ci);
            let val = if col.is_null(row) {
                serde_json::Value::Null
            } else if let Some(fmt) = &formatters[ci] {
                let s = fmt.value(row).to_string();
                // Attempt numeric / boolean parse to preserve native JSON types.
                serde_json::from_str::<serde_json::Value>(&s)
                    .unwrap_or(serde_json::Value::String(s))
            } else {
                serde_json::Value::Null
            };
            // Apply field_map rename (output key may differ from Arrow field name).
            let out_key = field_map.get(field.name())
                .cloned()
                .unwrap_or_else(|| field.name().clone());
            obj.insert(out_key, val);
        }
        serde_json::Value::Object(obj)
    }).collect()
}

/// Substitute `{column_name}` placeholders in a URL template with row values.
fn interpolate_url(template: &str, row: &serde_json::Value) -> String {
    if let Some(obj) = row.as_object() {
        let mut result = template.to_string();
        for (key, val) in obj {
            let placeholder  = format!("{{{key}}}");
            let replacement  = val.as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| val.to_string());
            result = result.replace(&placeholder, &replacement);
        }
        result
    } else {
        template.to_string()
    }
}