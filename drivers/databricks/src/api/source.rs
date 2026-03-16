//! Databricks REST API read source — prefetched Arrow IPC chunks via the
//! SQL Statement Execution API.

use std::io::Cursor;
use std::collections::VecDeque;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::stream::BoxStream;
use reqwest::Client;

use potato_etl_common::config::StepDriverOptions;
use potato_etl_common::db::traits::SourceBuilder;
use crate::conn::{DatabricksConnParams, dbx_full_table};
use super::{ExternalLink, resolve_next_link};

const MAX_RETRIES: u32 = 5;
const INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2);
const DEFAULT_CHUNK_PREFETCH: usize = 4;

pub struct DatabricksApiSource {
    params: DatabricksConnParams,
    table: String,
    schema_name: String,
    custom_query: Option<String>,
    batch_size: Option<usize>,
    chunk_prefetch: usize,
}

impl DatabricksApiSource {
    pub fn new(params: DatabricksConnParams) -> Self {
        Self {
            params,
            table: String::new(),
            schema_name: String::new(),
            custom_query: None,
            batch_size: None,
            chunk_prefetch: DEFAULT_CHUNK_PREFETCH,
        }
    }
}

impl SourceBuilder for DatabricksApiSource {
    fn table(mut self: Box<Self>, t: String) -> Box<dyn SourceBuilder> { self.table = t; self }
    fn schema(mut self: Box<Self>, s: String) -> Box<dyn SourceBuilder> { self.schema_name = s; self }
    fn query(mut self: Box<Self>, q: String) -> Box<dyn SourceBuilder> { self.custom_query = Some(q); self }
    fn cursor(self: Box<Self>, _c: String) -> Box<dyn SourceBuilder> { self }
    fn batch_size(mut self: Box<Self>, n: usize) -> Box<dyn SourceBuilder> { self.batch_size = Some(n); self }

    fn with_driver_options(mut self: Box<Self>, opts: &StepDriverOptions) -> Box<dyn SourceBuilder> {
        if let Some(ref dbx) = opts.databricks {
            if let Some(cp) = dbx.chunk_prefetch { self.chunk_prefetch = cp; }
        }
        if !opts.init_sql.is_empty() {
            self.params.init_sql = opts.init_sql.clone();
        }
        self
    }

    fn read_schema<'a>(&'a self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<SchemaRef>> + Send + 'a>> {
        Box::pin(async { anyhow::bail!("Databricks read_schema: schema is derived from first batch") })
    }

    fn exec(self: Box<Self>) -> BoxStream<'static, anyhow::Result<RecordBatch>> {
        let Self { params, table, schema_name, custom_query, chunk_prefetch, .. } = *self;
        let chunk_prefetch = chunk_prefetch.max(1);

        Box::pin(async_stream::try_stream! {
            let sql = build_query(
                &table, &schema_name,
                params.catalog.as_deref(), params.schema.as_deref(),
                custom_query.as_deref(),
            );

            // Build a tuned client for chunk downloads.
            let client = Client::builder()
                .https_only(true)
                .pool_max_idle_per_host(chunk_prefetch + 2)
                .tcp_nodelay(true)
                .connect_timeout(std::time::Duration::from_secs(30))
                .timeout(std::time::Duration::from_secs(120))
                .build()?;

            let api = super::StatementClient::with_client(params, client.clone());

            // Execute init_sql before the main query.
            api.execute_init_sql().await?;

            let resp = api.submit_statement(&sql).await?;
            let mut all_links: Vec<ExternalLink> = resp
                .result.as_ref()
                .map(|r| r.external_links.clone())
                .unwrap_or_default();
            let mut next_page = resolve_next_link(
                resp.result.as_ref().and_then(|r| r.next_chunk_internal_link.as_deref()),
                &all_links,
            );
            while let Some(il) = next_page {
                let page = api.fetch_chunk_links_page(&il).await?;
                next_page = resolve_next_link(
                    page.next_chunk_internal_link.as_deref(),
                    &page.external_links,
                );
                all_links.extend(page.external_links);
            }

            type PH = tokio::task::JoinHandle<anyhow::Result<Vec<RecordBatch>>>;
            let mut prefetch: VecDeque<PH> = all_links.iter().take(chunk_prefetch).map(|l| {
                let c = client.clone();
                let u = l.external_link.clone();
                let idx = l.chunk_index;
                tokio::spawn(async move { download_and_parse_chunk(&c, &u, idx).await })
            }).collect();

            for i in 0..all_links.len() {
                let batches = match prefetch.pop_front() {
                    Some(h) => h.await??,
                    None => download_and_parse_chunk(
                        &client,
                        &all_links[i].external_link,
                        all_links[i].chunk_index,
                    ).await?,
                };
                let next_idx = i + chunk_prefetch;
                if let Some(next) = all_links.get(next_idx) {
                    let c = client.clone();
                    let u = next.external_link.clone();
                    let idx = next.chunk_index;
                    prefetch.push_back(tokio::spawn(async move {
                        download_and_parse_chunk(&c, &u, idx).await
                    }));
                }
                for batch in batches {
                    if batch.num_rows() > 0 { yield batch; }
                }
            }
        })
    }

    fn try_clone(&self) -> Option<Box<dyn SourceBuilder>> { None }
}

// ── Chunk download helpers ───────────────────────────────────────────────────

async fn download_and_parse_chunk(
    client: &Client,
    url: &str,
    chunk_index: usize,
) -> anyhow::Result<Vec<RecordBatch>> {
    let bytes = retry_transient(
        || async {
            client
                .get(url)
                .send()
                .await
                .map_err(|e| RetryableError::classify(e, chunk_index))?
                .error_for_status()
                .map_err(|e| RetryableError::classify(e, chunk_index))?
                .bytes()
                .await
                .map_err(|e| RetryableError::classify(e, chunk_index))
        },
        chunk_index,
    )
    .await?;

    let mut reader = StreamReader::try_new(Cursor::new(&bytes[..]), None)?;
    let mut batches = Vec::new();
    for result in &mut reader {
        batches.push(result?);
    }
    Ok(batches)
}

enum RetryableError {
    Transient(anyhow::Error),
    Permanent(anyhow::Error),
}

impl RetryableError {
    fn classify(err: reqwest::Error, ci: usize) -> Self {
        if let Some(s) = err.status() {
            if matches!(s.as_u16(), 429 | 500 | 502 | 503 | 504) {
                return Self::Transient(anyhow::anyhow!("chunk {ci}: HTTP {s}: {err}"));
            }
        }
        if err.is_timeout() || err.is_connect() {
            Self::Transient(anyhow::anyhow!("chunk {ci}: {err}"))
        } else {
            Self::Permanent(anyhow::anyhow!("chunk {ci}: {err}"))
        }
    }
}

async fn retry_transient<F, Fut, T>(mut op: F, _ci: usize) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, RetryableError>>,
{
    let mut backoff = INITIAL_BACKOFF;
    let mut attempt = 0u32;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(RetryableError::Permanent(e)) => return Err(e),
            Err(RetryableError::Transient(e)) => {
                attempt += 1;
                if attempt > MAX_RETRIES {
                    return Err(e);
                }
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
        }
    }
}

// ── Query builder ────────────────────────────────────────────────────────────

fn build_query(
    table: &str,
    schema: &str,
    catalog: Option<&str>,
    conn_schema: Option<&str>,
    custom_q: Option<&str>,
) -> String {
    let eff = if !schema.is_empty() {
        schema
    } else {
        conn_schema.unwrap_or("")
    };
    match custom_q {
        Some(q) => q.to_string(),
        None => format!("SELECT * FROM {}", dbx_full_table(catalog, eff, table)),
    }
}