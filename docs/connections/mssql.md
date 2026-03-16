# SQL Server (MSSQL)

Driver: `mssql`
Default port: `1433`
Feature flag: `mssql`

## Connection

```yaml
connections:
  my_mssql:
    driver: mssql
    host: sql-server.local
    port: 1433                    # optional -- defaults to 1433
    database: warehouse
    auth:
      type: user_pass
      username: sa
      password: "YourPassword1!"
    options:                      # optional
      trust_cert: true
```

## Supported auth types

- `user_pass` -- username + password
- `kerberos` -- Kerberos with principal + optional keytab
- `windows_integrated` -- SSPI / NTLM (no credentials needed)

## Options

All fields are optional. Omitting `options:` entirely is valid.

```yaml
options:
  mode: tiberius            # tiberius (default) | bcp | odbc
  trust_cert: true          # ONLY for self-signed dev certs -- never in production
  application_name: my_etl  # visible in sys.dm_exec_sessions.program_name
  login_timeout: 30         # seconds
  batch_size: 50000         # rows per bulk write operation (default: 10,000)
  staging_table: true       # force staging table for all write modes
  bcp_path: /opt/mssql-tools18/bin/bcp   # explicit bcp path (auto-detected if absent)
  init_sql:                 # SQL executed on each new connection
    - "SET LOCK_TIMEOUT 5000"
```

| Option | Default | Description |
|---|---|---|
| `mode` | `tiberius` | Write-path mode: `tiberius` (pure Rust, default), `bcp` (CLI bulk-loader), or `odbc` (ODBC Driver 18). See [Write path comparison](#write-path-comparison). |
| `trust_cert` | `false` | Skip TLS certificate validation. **Never enable in production.** |
| `application_name` | -- | Application name visible in `sys.dm_exec_sessions.program_name` |
| `login_timeout` | driver default | Seconds before the login handshake times out |
| `batch_size` | `10000` | Rows per bulk write operation (bcp `-b` flag, tiberius buffer size, or ODBC chunk size) |
| `staging_table` | auto | `true` = force staging table for ALL write modes; `false` = disable staging; `null/absent` = auto (staging for MERGE modes only). **Warning:** disabling staging for upsert/insert_ignore/merge_delete causes data integrity errors. |
| `bcp_path` | auto-detected | Explicit path to the bcp binary. Setting this implicitly sets `mode: bcp`. |
| `init_sql` | `[]` | List of SQL statements executed on each new connection. Use for session-level settings like `SET LOCK_TIMEOUT`, `SET DEADLOCK_PRIORITY`, etc. |

## Write path comparison

| | tiberius `bulk_insert` | `bcp` CLI | ODBC |
|---|---|---|---|
| External dependency | None (pure Rust) | `mssql-tools18` | unixODBC + ODBC Driver 18 |
| Known bugs | DATE->TIME COLMETADATA bug | None | None |
| Throughput | ~50k-150k rows/sec | ~500k-2M+ rows/sec | ~8,500 rows/sec |
| DEFAULTs / IDENTITY | Suppressed (KEEPNULLS) | Suppressed (KEEPNULLS) | Applied (no KEEPNULLS) |

**When to use bcp:** Loading large volumes into SQL Server. Requires `mssql-tools18` installed:

```sh
sudo apt install mssql-tools18   # Debian/Ubuntu
sudo dnf install mssql-tools18   # RHEL/Fedora
```

**When to use ODBC:** Target table has `DEFAULT` constraints or `IDENTITY` columns and you want SQL Server to fill them automatically for NULL values.

## Examples

**Basic connection:**

```yaml
connections:
  mssql:
    driver: mssql
    host: sql-server.local
    database: DW
    auth:
      type: user_pass
      username: sa
      password: "YourPassword1!"
    options:
      trust_cert: true
```

**Kerberos:**

```yaml
connections:
  mssql_corp:
    driver: mssql
    host: sql-server.corp.local
    database: DW
    auth:
      type: kerberos
      principal: "etl_svc@CORP.LOCAL"
      keytab: "/etc/krb5.keytab"
```

**Windows Integrated:**

```yaml
connections:
  mssql_local:
    driver: mssql
    host: sql-server.local
    database: DW
    auth:
      type: windows_integrated
```

**High-throughput bcp:**

```yaml
connections:
  mssql_fast:
    driver: mssql
    host: sql-server.prod
    database: warehouse
    auth:
      type: user_pass
      username: etl_svc
      password: "${MSSQL_PASSWORD}"
    options:
      mode: bcp
      batch_size: 50000
```