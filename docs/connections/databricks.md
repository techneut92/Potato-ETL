# Databricks

Driver: `databricks`
Protocol: HTTP (SQL Statement API)
Feature flag: `databricks`

No extra system libraries required -- uses `reqwest` which is already a dependency.

## Connection

```yaml
connections:
  dbx:
    driver: databricks
    host: adb-1234567890123456.4.azuredatabricks.net
    http_path: /sql/1.0/warehouses/abc123def456
    auth:
      type: pat
      token: "${DATABRICKS_TOKEN}"
    options:                      # optional
      catalog: main
      schema: analytics
```

## Supported auth types

### Personal Access Token (PAT)

```yaml
auth:
  type: pat
  token: "${DATABRICKS_TOKEN}"
```

### OAuth2 client credentials (M2M / service principal)

The token is exchanged via `POST /oidc/v1/token` and cached automatically.

```yaml
auth:
  type: oauth2_client_credentials
  client_id: "${DATABRICKS_CLIENT_ID}"
  client_secret: "${DATABRICKS_CLIENT_SECRET}"
```

## Options

All fields are optional. Omitting `options:` entirely is valid.

```yaml
options:
  catalog: main             # Unity Catalog catalog (workspace default if absent)
  schema: analytics         # default database / schema
  connect_timeout: 120      # HTTP timeout in seconds for long-running queries
  init_sql:                 # List of SQL statements executed at the start of each source/sink operation
    - "SET spark.sql.shuffle.partitions=200"

  # ODBC transport (fallback for environments where REST API is blocked)
  odbc:
    enable: true
    driver_path: /opt/databricks/databricksodbc/lib/64/libdatabricksodbc64.so
    port: 443                              # TCP port (default: 443)
    ssl: true                              # SSL/TLS (default: true)
    thrift_transport: 2                    # 2 = HTTP (default), 0 = binary
    use_native_query: true                 # pass SQL verbatim (default: true)
    string_column_length: 65535            # force Simba STRING DisplaySize
    use_unicode_sql_character_types: true   # SQL_WVARCHAR instead of SQL_VARCHAR
    use_long_varchar: true                 # force SQL_WLONGVARCHAR for STRING cols

  # Extra driver key=value params (appended verbatim to ODBC conn string)
  url_params:
    - "RowsFetchedPerBlock=50000"
    - "Timeout=300"

  # Transport mode: api (default) or odbc
  mode: api
```

| Option | Default | Description |
|---|---|---|
| `catalog` | workspace default | Unity Catalog catalog |
| `schema` | -- | Default database / schema for unqualified table references |
| `connect_timeout` | `120` | HTTP client timeout in seconds for the SQL Statement API |
| `init_sql` | `[]` | List of SQL statements executed at the start of each source/sink operation. Use for Spark config like `SET spark.sql.shuffle.partitions`. |

### ODBC options (`options.odbc`)

| Option | Default | Description |
|---|---|---|
| `enable` | `false` | Use ODBC instead of REST API for reads |
| `driver_path` | (registered name) | Explicit path to Simba ODBC `.so` / `.dylib` / `.dll` |
| `port` | `443` | TCP port for the ODBC connection |
| `ssl` | `true` | Enable SSL/TLS (SSL=1 in conn string) |
| `thrift_transport` | `2` | Thrift transport mode (2=HTTP, 0=binary) |
| `use_native_query` | `true` | Pass SQL verbatim without Simba rewriting |
| `string_column_length` | -- | Override STRING column DisplaySize (e.g. 65535) |
| `use_unicode_sql_character_types` | -- | Force SQL_WVARCHAR for string columns |
| `use_long_varchar` | -- | Force SQL_WLONGVARCHAR for STRING columns |

> **Note:** The ODBC source automatically runs `DESCRIBE TABLE` and wraps
> STRING/BINARY columns in `CAST(col AS STRING)` / `CAST(col AS BINARY)` to
> force the Simba driver to report unbounded types. The `string_column_length`,
> `use_unicode_sql_character_types`, and `use_long_varchar` options provide
> additional defense-in-depth but are not required when the CAST introspection
> is active.

## Examples

**PAT auth:**

```yaml
connections:
  dbx:
    driver: databricks
    host: adb-1234567890123456.4.azuredatabricks.net
    http_path: /sql/1.0/warehouses/abc123def456
    auth:
      type: pat
      token: "${DATABRICKS_TOKEN}"
    options:
      catalog: main
      schema: raw
```

**OAuth2 M2M:**

```yaml
connections:
  dbx_oauth:
    driver: databricks
    host: adb-1234567890123456.7.azuredatabricks.net
    http_path: /sql/1.0/warehouses/ecd78902dfed40f2
    auth:
      type: oauth2_client_credentials
      client_id: "${DATABRICKS_CLIENT_ID}"
      client_secret: "${DATABRICKS_CLIENT_SECRET}"
    options:
      catalog: main
      schema: analytics
      connect_timeout: 300
```