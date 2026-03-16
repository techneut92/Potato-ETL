# PostgreSQL

Driver: `postgres` (alias: `postgresql`)
Default port: `5432`
Feature flag: `postgres`

## Connection

```yaml
connections:
  my_pg:
    driver: postgres
    host: db.example.com
    port: 5432                    # optional -- defaults to 5432
    database: mydb
    auth:
      type: user_pass
      username: etl_user
      password: "p@ss:w0rd"
    options:                      # optional
      ssl: require
```

## Supported auth types

- `user_pass` -- username + password
- `kerberos` -- Kerberos with principal + optional keytab
- `certificate` -- mTLS with client cert/key and optional CA
- `aws_iam` -- AWS IAM token auth (Aurora/RDS)
- `none` -- trusted local (e.g. `trust` auth, Unix socket)

## Options

All fields are optional. Omitting `options:` entirely is valid.

```yaml
options:
  ssl: require              # disable | allow | prefer | require | verify-ca | verify-full
  connect_timeout: 30       # seconds before connection attempt times out
  application_name: my_etl  # visible in pg_stat_activity and slow-query logs
  max_connections: 5        # connection pool size (default: 5)
  staging_table: true       # use COPY BINARY via temp staging table for upsert/insert_ignore
  init_sql:                 # SQL executed on each new pool connection
    - "SET work_mem = '256MB'"
```

| Option | Default | Description |
|---|---|---|
| `ssl` | server default (usually `prefer`) | SSL mode passed as the `sslmode` connection parameter |
| `connect_timeout` | driver default | Seconds before a connection attempt is abandoned |
| `application_name` | -- | Application name shown in `pg_stat_activity` |
| `max_connections` | `5` | Connection pool size. Increase when multiple concurrent sinks write to the same instance. |
| `staging_table` | `false` | Use a temporary staging table for `insert_ignore`, `upsert`, and `merge_delete` write modes. COPY-BINARY-loads data into a temp table, then performs a single `INSERT … ON CONFLICT` from it. ~5-10x faster than chunked parameterized INSERTs for large batches. Requires permissions to create temp tables. |
| `init_sql` | `[]` | List of SQL statements executed on each new connection in the pool. Use for session-level variables like `work_mem`, `statement_timeout`, `search_path`. |

## Examples

**Basic connection:**

```yaml
connections:
  pg:
    driver: postgres
    host: localhost
    database: app_db
    auth:
      type: user_pass
      username: etl_user
      password: "etl_pass"
```

**SSL + timeout:**

```yaml
connections:
  pg_secure:
    driver: postgres
    host: prod-db.example.com
    database: warehouse
    auth:
      type: user_pass
      username: etl_svc
      password: "${PG_PASSWORD}"
    options:
      ssl: verify-full
      connect_timeout: 10
      application_name: potato_etl_prod
```

**mTLS certificate auth:**

```yaml
connections:
  pg_mtls:
    driver: postgres
    host: secure-db.example.com
    database: mydb
    auth:
      type: certificate
      cert_path: "/etc/certs/client.crt"
      key_path: "/etc/certs/client.key"
      ca_path: "/etc/certs/ca.crt"
```

**AWS IAM (Aurora):**

```yaml
connections:
  aurora_pg:
    driver: postgres
    host: cluster.eu-west-1.rds.amazonaws.com
    database: mydb
    auth:
      type: aws_iam
      username: "etl_user"
      region: "eu-west-1"
```