//! MSSQL connection params, client type alias, and shared SQL helpers.

use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tiberius::{Client, Config, AuthMethod};

use potato_etl_common::config::pct_decode;

// ── Type alias ────────────────────────────────────────────────────────────────

pub type MssqlClient = Client<Compat<TcpStream>>;

// ── Write-path mode ───────────────────────────────────────────────────────────

/// Which write path the MSSQL sink uses.
///
/// Configured via `mode=` in the connection string or `mode:` in YAML options.
/// Legacy `bcp=true` / `odbc=true` params have been removed — use `mode=bcp`
/// or `mode=odbc` instead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MssqlWriteMode {
    /// Pure-Rust TDS via tiberius.  Default.
    #[default]
    Tiberius,
    /// BCP CLI subprocess (`mssql-tools18`).
    Bcp,
    /// ODBC columnar binding (no KEEPNULLS).
    Odbc,
}

// ── Connection params ─────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct MssqlConnParams {
    pub host:             String,
    pub port:             u16,
    pub user:             String,
    pub pass:             String,
    pub database:         String,
    pub application_name: Option<String>,
    pub login_timeout:    Option<u32>,
    pub trust_cert:       bool,
    pub mode:             MssqlWriteMode,
    pub bcp_path:         Option<String>,
    pub batch_size:       Option<usize>,
    pub bcp_staging:      Option<bool>,
    pub bcp_packet_size:  Option<usize>,
    pub bcp_max_errors:   Option<usize>,
    /// SQL statements executed on every new connection, after built-in SET options.
    pub init_sql:         Vec<String>,
}

impl MssqlConnParams {
    pub fn parse(conn_str: &str) -> anyhow::Result<Self> {
        let s = conn_str.strip_prefix("mssql://")
            .ok_or_else(|| anyhow::anyhow!(
                "MSSQL connection string must start with 'mssql://'. Got: {conn_str}"
            ))?;

        let (creds, rest) = s.split_once('@')
            .ok_or_else(|| anyhow::anyhow!("Missing '@' in MSSQL connection string"))?;
        let (raw_user, raw_pass) = creds.split_once(':')
            .ok_or_else(|| anyhow::anyhow!("Missing ':' between user and password in MSSQL connection string"))?;

        let user = pct_decode(raw_user)?;
        let pass = pct_decode(raw_pass)?;

        let (host_port, db_and_query) = rest.split_once('/')
            .ok_or_else(|| anyhow::anyhow!("Missing '/' before database name in MSSQL connection string"))?;

        let (host, port) = if let Some((h, p)) = host_port.split_once(':') {
            (h.to_string(), p.parse::<u16>().unwrap_or(1433))
        } else {
            (host_port.to_string(), 1433)
        };

        let (database, query) = match db_and_query.split_once('?') {
            Some((db, qs)) => (db.to_string(), qs.to_string()),
            None           => (db_and_query.to_string(), String::new()),
        };

        let mut application_name: Option<String> = None;
        let mut login_timeout:    Option<u32>    = None;
        let mut trust_cert                        = false;
        let mut mode: Option<String>              = None;
        let mut bcp_path                          = None;
        let mut batch_size                        = None;
        let mut bcp_staging: Option<bool>         = None;
        let mut bcp_packet_size                   = None;
        let mut bcp_max_errors                    = None;
        let mut init_sql                          = Vec::new();

        for param in query.split('&').filter(|s| !s.is_empty()) {
            if let Some((k, v)) = param.split_once('=') {
                match k {
                    "application_name" => application_name = Some(pct_decode(v)?),
                    "login_timeout"    => login_timeout    = v.parse::<u32>().ok(),
                    "trust_cert"       => trust_cert       = v.eq_ignore_ascii_case("true"),
                    "mode"                => mode            = Some(v.to_string()),
                    "bcp_path"            => bcp_path       = Some(pct_decode(v)?),
                    "batch_size"          => batch_size     = v.parse::<usize>().ok(),
                    "bcp_staging"         => bcp_staging     = Some(v.eq_ignore_ascii_case("true")),
                    "bcp_packet_size"     => bcp_packet_size = v.parse::<usize>().ok(),
                    "bcp_max_errors"      => bcp_max_errors  = v.parse::<usize>().ok(),
                    "init_sql"            => init_sql.push(pct_decode(v)?),
                    _                     => {}
                }
            }
        }

        // Resolve mode= into MssqlWriteMode.
        let mut mode = match mode {
            Some(ref m) => match m.as_str() {
                "bcp"      => MssqlWriteMode::Bcp,
                "odbc"     => MssqlWriteMode::Odbc,
                "tiberius" => MssqlWriteMode::Tiberius,
                other => anyhow::bail!(
                    "Unknown MSSQL mode '{other}'. Valid modes: tiberius, bcp, odbc"
                ),
            },
            None => MssqlWriteMode::Tiberius,
        };

        // bcp_path implies mode=bcp when no explicit mode is set.
        if bcp_path.is_some() && mode == MssqlWriteMode::Tiberius {
            mode = MssqlWriteMode::Bcp;
        }

        Ok(Self {
            host, port, user, pass, database,
            application_name, login_timeout, trust_cert,
            mode, bcp_path, batch_size, bcp_staging,
            bcp_packet_size, bcp_max_errors,
            init_sql,
        })
    }

    pub async fn connect(&self) -> anyhow::Result<MssqlClient> {
        let mut config = Config::new();
        config.host(&self.host);
        config.port(self.port);
        config.authentication(AuthMethod::sql_server(&self.user, &self.pass));
        config.database(&self.database);

        if let Some(name) = &self.application_name {
            config.application_name(name);
        }
        if self.trust_cert {
            tracing::warn!(
                host = %self.host,
                "MSSQL trust_cert=true: TLS certificate validation is disabled. \
                 This must not be used in production environments."
            );
            config.trust_cert();
        }

        let mut client = if let Some(secs) = self.login_timeout {
            let timeout = std::time::Duration::from_secs(secs as u64);
            let tcp = tokio::time::timeout(timeout, TcpStream::connect(config.get_addr()))
                .await
                .map_err(|_| anyhow::anyhow!("MSSQL TCP connect timed out after {secs}s"))?
                .map_err(|e| anyhow::anyhow!("TCP connection to MSSQL failed: {e}"))?;
            tcp.set_nodelay(true)?;
            tokio::time::timeout(timeout, Client::connect(config, tcp.compat_write()))
                .await
                .map_err(|_| anyhow::anyhow!("MSSQL TDS handshake timed out after {secs}s"))?
                .map_err(|e| anyhow::anyhow!("TDS connection to MSSQL failed: {e}"))?
        } else {
            let tcp = TcpStream::connect(config.get_addr()).await
                .map_err(|e| anyhow::anyhow!("TCP connection to MSSQL failed: {e}"))?;
            tcp.set_nodelay(true)?;
            Client::connect(config, tcp.compat_write()).await
                .map_err(|e| anyhow::anyhow!("TDS connection to MSSQL failed: {e}"))?
        };

        client.simple_query(
            "SET NOCOUNT ON; SET XACT_ABORT ON; SET ARITHABORT ON;"
        ).await
            .map_err(|e| anyhow::anyhow!("MSSQL session SET options failed: {e}"))?
            .into_results().await
            .map_err(|e| anyhow::anyhow!("MSSQL session SET options result drain failed: {e}"))?;
        tracing::debug!(
            host = %self.host,
            "MSSQL: session options applied (NOCOUNT, XACT_ABORT, ARITHABORT)"
        );

        // ── User-defined init_sql ────────────────────────────────────────────
        if !self.init_sql.is_empty() {
            for stmt in &self.init_sql {
                tracing::debug!(sql = %stmt, "MSSQL init_sql: executing");
                client.simple_query(stmt).await
                    .map_err(|e| anyhow::anyhow!("MSSQL init_sql failed: {stmt}: {e}"))?
                    .into_results().await
                    .map_err(|e| anyhow::anyhow!("MSSQL init_sql result drain failed: {stmt}: {e}"))?;
            }
            tracing::info!(
                host = %self.host,
                count = self.init_sql.len(),
                "MSSQL: {} user init_sql statement(s) applied",
                self.init_sql.len(),
            );
        }

        Ok(client)
    }
}

// ── Shared SQL helpers ───────────────────────────────────────────────────────

pub fn mssql_build_query(
    table:       &str,
    schema:      &str,
    custom_q:    Option<&str>,
    cursor_col:  Option<&str>,
    last_cursor: Option<&str>,
    batch_size:  usize,
) -> String {
    let base = match custom_q {
        Some(q) => format!("SELECT * FROM ({q}) AS etl_q"),
        None    => format!("SELECT * FROM [{schema}].[{table}]"),
    };

    match (cursor_col, last_cursor) {
        (Some(col), Some(val)) => {
            let safe = val.replace('\'', "''");
            format!("SELECT TOP {batch_size} * FROM ({base}) AS _q WHERE [{col}] > '{safe}' ORDER BY [{col}] ASC")
        }
        (Some(col), None) =>
            format!("SELECT TOP {batch_size} * FROM ({base}) AS _q ORDER BY [{col}] ASC"),
        _ =>
            format!("SELECT TOP {batch_size} * FROM ({base}) AS _q"),
    }
}

pub fn mssql_build_offset_query(
    table:      &str,
    schema:     &str,
    custom_q:   Option<&str>,
    offset:     usize,
    batch_size: usize,
) -> String {
    let base = match custom_q {
        Some(q) => format!("SELECT * FROM ({q}) AS etl_q"),
        None    => format!("SELECT * FROM [{schema}].[{table}]"),
    };
    format!(
        "SELECT * FROM ({base}) AS _q \
         ORDER BY (SELECT NULL) \
         OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
    )
}

// ── Table introspection ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TableColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub has_default: bool,
}

pub async fn introspect_table_columns(
    client: &mut MssqlClient,
    schema_name: &str,
    table_name: &str,
) -> anyhow::Result<Option<Vec<TableColumnInfo>>> {
    let sql =
        "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.COLUMN_DEFAULT,
                COLUMNPROPERTY(
                    OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME),
                    c.COLUMN_NAME,
                    'IsIdentity'
                ) AS IS_IDENTITY,
                COLUMNPROPERTY(
                    OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME),
                    c.COLUMN_NAME,
                    'IsComputed'
                ) AS IS_COMPUTED
           FROM INFORMATION_SCHEMA.COLUMNS c
          WHERE c.TABLE_SCHEMA = @P1 AND c.TABLE_NAME = @P2
          ORDER BY c.ORDINAL_POSITION";

    let rows = client
        .query(sql, &[&schema_name, &table_name])
        .await?
        .into_first_result()
        .await?;

    if rows.is_empty() {
        return Ok(None);
    }

    let cols: Vec<TableColumnInfo> = rows
        .iter()
        .map(|row| {
            let name: &str       = row.get("COLUMN_NAME").unwrap_or("");
            let data_type: &str  = row.get("DATA_TYPE").unwrap_or("");
            let nullable: &str   = row.get("IS_NULLABLE").unwrap_or("YES");
            let default: Option<&str> = row.get("COLUMN_DEFAULT");
            let is_identity: i32 = row.get("IS_IDENTITY").unwrap_or(0);
            let is_computed: i32 = row.get("IS_COMPUTED").unwrap_or(0);
            TableColumnInfo {
                name: name.to_string(),
                data_type: data_type.to_ascii_lowercase(),
                nullable: nullable.eq_ignore_ascii_case("YES"),
                has_default: default.is_some() || is_identity == 1 || is_computed == 1,
            }
        })
        .collect();

    Ok(Some(cols))
}