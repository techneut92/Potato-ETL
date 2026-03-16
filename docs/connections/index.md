# Connections

Connections are defined once in the `connections:` section and referenced by name throughout the pipeline. Each connection is identified by the `driver` field.

## Supported drivers

| Driver | Aliases | Default port | Feature flag | Docs |
|---|---|---|---|---|
| `postgres` | `postgresql` | 5432 | `postgres` | [PostgreSQL](./postgres.md) |
| `mssql` | -- | 1433 | `mssql` | [SQL Server](./mssql.md) |
| `oracle` | -- | 1521 | `oracle` | [Oracle](./oracle.md) |
| `mysql` | `aurora`, `mariadb` | 3306 | `mysql` | [MySQL](./mysql.md) |
| `databricks` | -- | -- (HTTP) | `databricks` | [Databricks](./databricks.md) |
| `rest_api` | -- | -- | *(always available)* | [REST API](./rest-api.md) |

### File transport drivers

| Driver | Aliases | Auth types | Feature | Docs |
|---|---|---|---|---|
| `local` | -- | `none` | *(always available)* | [File Connections](./file-connections.md) |
| `sftp` | -- | `user_pass`, `key` | `transport-sftp` | [File Connections](./file-connections.md) |
| `s3` | -- | `access_key`, `role_arn`, `default_credentials` | `transport-cloud` | [File Connections](./file-connections.md) |
| `azure_blob` | -- | `client_credentials`, `connection_string`, `sas_token`, `default_credentials` | `transport-cloud` | [File Connections](./file-connections.md) |
| `gcs` | -- | `service_account`, `default_credentials` | `transport-cloud` | [File Connections](./file-connections.md) |
| `sharepoint` | `sharepoint_online` | `client_credentials` | `transport-sharepoint` | [File Connections](./file-connections.md) |
| `ftp` | `ftps` | `user_pass`, `none` | `transport-ftp` | [File Connections](./file-connections.md) |
| `smb` | `cifs` | `user_pass` | `transport-smb` | [File Connections](./file-connections.md) |

> Remote file transports require the corresponding feature flag at build time. See [Transport Crates & Feature Flags](../feature-flags.md).

## Authentication

Every database connection requires an `auth:` block. The `type` field selects the authentication method.

| Type | Fields | Drivers |
|---|---|---|
| `user_pass` | `username`, `password` | All SQL databases |
| `pat` | `token` | Databricks |
| `oauth2_client_credentials` | `client_id`, `client_secret` | Databricks |
| `kerberos` | `principal`, `keytab` (optional) | MSSQL, Postgres, Oracle |
| `windows_integrated` | *(none)* | MSSQL |
| `certificate` | `cert_path`, `key_path`, `ca_path` (optional) | Postgres, MySQL, Oracle |
| `aws_iam` | `username`, `region` | Aurora/RDS (Postgres, MySQL) |
| `none` | *(none)* | Any (trusted local) |

### Username + password (most common)

Works with all SQL databases.

```yaml
connections:
  my_db:
    driver: postgres
    host: db.example.com
    database: mydb
    auth:
      type: user_pass
      username: etl_user
      password: "p@ss:w0rd!#special"
```

Passwords can contain any characters -- special characters are handled automatically.

### Kerberos

```yaml
auth:
  type: kerberos
  principal: "etl_svc@CORP.LOCAL"
  keytab: "/etc/krb5.keytab"    # optional -- uses default credential cache if absent
```

### Windows Integrated (SSPI)

No credentials needed -- uses the current OS session.

```yaml
auth:
  type: windows_integrated
```

### Client certificate (mTLS)

```yaml
auth:
  type: certificate
  cert_path: "/etc/certs/client.crt"
  key_path: "/etc/certs/client.key"
  ca_path: "/etc/certs/ca.crt"     # optional
```

### AWS IAM (Aurora / RDS)

Generates a short-lived auth token using your AWS credentials.

```yaml
auth:
  type: aws_iam
  username: "etl_user"
  region: "eu-west-1"
```

### No authentication

For trusted local connections (e.g. Postgres `trust` auth, Unix socket).

```yaml
auth:
  type: none
```

## Environment variables in connection values

Use `${VAR_NAME}` syntax to reference environment variables. This keeps secrets out of your config files.

```yaml
auth:
  type: pat
  token: "${DATABRICKS_TOKEN}"
```

Then run with:

```sh
DATABRICKS_TOKEN="dapi..." potato_etl run --config pipeline.yaml
```

## Secret manager integration

For production deployments, use the `secret::` prefix to fetch credentials from
an external secret manager at pipeline load time. Secrets are resolved on the
raw YAML/JSON `Value` tree **before** deserialization, so no code changes are
needed.

| Provider              | Prefix          | Aliases                          |
|-----------------------|-----------------|----------------------------------|
| HashiCorp Vault KV v2 | `secret::vault` | `secret::hashicorp`              |
| Azure Key Vault       | `secret::azure` | `secret::akv`                    |
| Google Secret Manager | `secret::gcp`   | `secret::gsm`, `secret::google`  |

**Reference format:** `secret::<provider>/<path>[#field]`

### Individual field references (recommended)

```yaml
connections:
  pg_prod:
    driver: postgres
    host: prod-db.internal
    database: analytics
    auth:
      type: user_pass
      username: "secret::vault/prod/pg#username"
      password: "secret::vault/prod/pg#password"
```

### Structured secret with field extraction

Secret values are auto-detected as JSON, YAML, or plain string. Use `#field`
to extract a specific key from a structured secret:

```yaml
connections:
  pg:
    host: "secret::azure/my-vault/db-config#host"
    port: "secret::azure/my-vault/db-config#port"
    auth:
      type: user_pass
      username: etl_user
      password: "secret::azure/my-vault/db-config#password"
```

### Whole-connection resolution

Store the entire connection object as a JSON secret and reference it directly:

```yaml
connections:
  pg_staging: "secret::vault/staging/pg_connection"
```

See [secrets.md](../secrets.md) for provider-specific auth configuration and
environment variables.