# File Connections

File connections let file steps (`read_csv`, `read_json`, `read_parquet`, `write_csv`, `write_json`, `write_parquet`) read from and write to remote storage backends -- SFTP servers, S3 buckets, Azure Blob, Google Cloud Storage, SharePoint, FTP, and SMB shares -- using the same connection model as database steps.

## Supported drivers

| Driver | Aliases | Auth types | Transport crate | Status |
|---|---|---|---|---|
| `local` | -- | `none` | *(built into common)* | Available |
| `sftp` | -- | `user_pass`, `key` | `potato-etl-transport-sftp` | Available |
| `s3` | -- | `access_key`, `role_arn`, `default_credentials` | `potato-etl-transport-cloud` | Available |
| `azure_blob` | -- | `client_credentials`, `connection_string`, `sas_token`, `default_credentials` | `potato-etl-transport-cloud` | Available |
| `gcs` | -- | `service_account`, `default_credentials` | `potato-etl-transport-cloud` | Available |
| `sharepoint` | `sharepoint_online` | `client_credentials` | `potato-etl-transport-sharepoint` | Available |
| `ftp` | `ftps` | `user_pass`, `none` | `potato-etl-transport-ftp` | Available |
| `smb` | `cifs` | `user_pass` | `potato-etl-transport-smb` | Available |

> **Note:** All 8 transport drivers are fully implemented. Each lives in its own crate following the same pattern as database drivers. Each uses lazy connection — the network handshake happens on the first I/O operation, not at pipeline load time. See [Transport Crates](../feature-flags.md) for details on including only the transports you need.

## How file connections work

1. Define a named connection with a `driver` and `base_path`
2. Reference it from file steps using `from.connection` (sources) or `target.connection` (sinks)
3. The step's `path` is resolved relative to the connection's `base_path`

```yaml
connections:
  reports:
    driver: sftp
    host: sftp.example.com
    auth:
      type: key
      username: deploy
      private_key: "${SFTP_KEY}"
    base_path: /uploads/reports

steps:
  - id: data
    type: read_csv
    from:
      connection: reports
      path: incoming/latest.csv       # → /uploads/reports/incoming/latest.csv

  - id: output
    type: write_json
    input: data
    target:
      connection: reports
      path: processed/result.json     # → /uploads/reports/processed/result.json
    pretty: true
```

### Backwards compatibility

File steps without a connection reference still work -- they default to local filesystem access:

```yaml
# These two are equivalent:
- id: data
  type: read_csv
  path: data/employees.csv

- id: data
  type: read_csv
  from:
    connection: my_local     # connection with driver: local, base_path: ""
    path: data/employees.csv
```

## File auth types

| Type | Fields | Drivers |
|---|---|---|
| `user_pass` | `username`, `password` | SFTP, FTP, SMB |
| `key` | `username`, `private_key`, `passphrase` (optional) | SFTP |
| `access_key` | `access_key_id`, `secret_access_key`, `session_token` (optional) | S3 |
| `role_arn` | `role_arn`, `external_id` (optional) | S3 (STS AssumeRole) |
| `client_credentials` | `tenant_id`, `client_id`, `client_secret` | SharePoint, Azure Blob |
| `service_account` | `credentials_file` | GCS |
| `connection_string` | `connection_string` | Azure Blob |
| `sas_token` | `token` | Azure Blob |
| `default_credentials` | *(none)* | S3, GCS, Azure Blob |
| `none` | *(none)* | Local, anonymous FTP |

---

## Driver reference

### local

Local filesystem. Primarily useful for defining a `base_path` to keep step paths short.

```yaml
connections:
  local_data:
    driver: local
    base_path: /data/warehouse
```

| Field | Required | Default | Description |
|---|---|---|---|
| `base_path` | no | `""` (CWD) | Base directory for resolving step paths |

### sftp

SSH File Transfer Protocol. Supports password and private key authentication.

```yaml
connections:
  reports_sftp:
    driver: sftp
    host: sftp.example.com
    port: 22
    auth:
      type: key
      username: deploy
      private_key: "${SFTP_PRIVATE_KEY}"
      passphrase: "${KEY_PASSPHRASE}"    # optional
    base_path: /uploads/reports
    host_key: "ssh-ed25519 AAAA..."      # optional: strict host key checking
```

| Field | Required | Default | Description |
|---|---|---|---|
| `host` | yes | -- | SFTP server hostname |
| `port` | no | `22` | SSH port |
| `auth` | yes | -- | `user_pass` or `key` |
| `base_path` | no | `""` | Remote base directory |
| `host_key` | no | -- | Known host key for verification |

### s3

Amazon S3 or S3-compatible object storage (MinIO, Cloudflare R2, DigitalOcean Spaces).

```yaml
connections:
  data_lake:
    driver: s3
    bucket: my-data-lake
    region: eu-west-1
    auth:
      type: access_key
      access_key_id: "${AWS_ACCESS_KEY_ID}"
      secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
    base_path: etl/output

  # S3-compatible (MinIO)
  minio:
    driver: s3
    bucket: etl-data
    region: us-east-1
    endpoint: "http://minio.internal:9000"
    force_path_style: true
    auth:
      type: access_key
      access_key_id: minioadmin
      secret_access_key: minioadmin

  # AWS default credential chain (env vars, instance profile, SSO)
  s3_default:
    driver: s3
    bucket: my-bucket
    region: eu-west-1
    auth:
      type: default_credentials
```

| Field | Required | Default | Description |
|---|---|---|---|
| `bucket` | yes | -- | S3 bucket name |
| `region` | no | `us-east-1` | AWS region |
| `auth` | no | `none` | `access_key`, `role_arn`, or `default_credentials` |
| `base_path` | no | `""` | Key prefix for all operations |
| `endpoint` | no | -- | Custom endpoint for S3-compatible services |
| `force_path_style` | no | `false` | Use path-style addressing (required for MinIO, R2) |

### azure_blob

Azure Blob Storage.

```yaml
connections:
  azure_storage:
    driver: azure_blob
    account: mystorageaccount
    container: etl-data
    auth:
      type: connection_string
      connection_string: "${AZURE_STORAGE_CONN}"
    base_path: raw/incoming

  # Service principal auth
  azure_sp:
    driver: azure_blob
    account: mystorageaccount
    container: etl-data
    auth:
      type: client_credentials
      tenant_id: "${AZURE_TENANT_ID}"
      client_id: "${AZURE_CLIENT_ID}"
      client_secret: "${AZURE_CLIENT_SECRET}"
```

| Field | Required | Default | Description |
|---|---|---|---|
| `account` | yes | -- | Azure storage account name |
| `container` | yes | -- | Blob container name |
| `auth` | yes | -- | `connection_string`, `sas_token`, `client_credentials`, or `default_credentials` |
| `base_path` | no | `""` | Blob prefix for all operations |

### gcs

Google Cloud Storage.

```yaml
connections:
  gcs_bucket:
    driver: gcs
    bucket: my-etl-bucket
    auth:
      type: service_account
      credentials_file: "${GCP_SA_KEY_FILE}"
    base_path: output
```

| Field | Required | Default | Description |
|---|---|---|---|
| `bucket` | yes | -- | GCS bucket name |
| `auth` | yes | -- | `service_account` or `default_credentials` |
| `base_path` | no | `""` | Object prefix for all operations |

### sharepoint

Microsoft SharePoint Online via the MS Graph API.

```yaml
connections:
  company_sp:
    driver: sharepoint
    site_url: "https://company.sharepoint.com/sites/DataTeam"
    auth:
      type: client_credentials
      tenant_id: "${AZURE_TENANT_ID}"
      client_id: "${AZURE_CLIENT_ID}"
      client_secret: "${AZURE_CLIENT_SECRET}"
    base_path: /Shared Documents/ETL
    drive_id: "b!abc123..."              # optional: specific drive ID
```

| Field | Required | Default | Description |
|---|---|---|---|
| `site_url` | yes | -- | Full SharePoint site URL |
| `auth` | yes | -- | `client_credentials` (Azure AD app registration) |
| `base_path` | no | `Shared Documents` | Document library path |
| `drive_id` | no | -- | Override drive ID (uses default document library if absent) |

**Azure AD setup required:**
1. Register an app in Azure AD
2. Grant `Sites.ReadWrite.All` (application) permission
3. Admin consent the permission
4. Use the app's `client_id` and `client_secret`

### ftp

FTP with optional explicit TLS (FTPS).

```yaml
connections:
  legacy_ftp:
    driver: ftp
    host: ftp.example.com
    port: 21
    auth:
      type: user_pass
      username: etl_user
      password: "${FTP_PASSWORD}"
    tls: true
    base_path: /incoming
```

| Field | Required | Default | Description |
|---|---|---|---|
| `host` | yes | -- | FTP server hostname |
| `port` | no | `21` | FTP port |
| `auth` | no | `none` | `user_pass` or `none` (anonymous) |
| `base_path` | no | `""` | Remote base directory |
| `tls` | no | `false` | Enable explicit FTPS |
| `passive` | no | `true` | Use passive mode |

### smb

SMB/CIFS Windows file shares.

```yaml
connections:
  file_share:
    driver: smb
    host: fileserver.corp.local
    share: DataDrop
    port: 445
    auth:
      type: user_pass
      username: "DOMAIN\\etl_user"
      password: "${SMB_PASSWORD}"
    base_path: /etl/incoming
```

| Field | Required | Default | Description |
|---|---|---|---|
| `host` | yes | -- | SMB server hostname |
| `share` | yes | -- | Share name |
| `port` | no | `445` | SMB port |
| `auth` | no | `none` | `user_pass` with optional domain prefix |
| `base_path` | no | `""` | Path within the share |

---

## Complete example: SFTP → transform → S3

```yaml
config:
  batch_size: 5000

connections:
  incoming:
    driver: sftp
    host: partner.example.com
    auth:
      type: key
      username: etl
      private_key: "secret::vault/sftp/partner#private_key"
    base_path: /outbox

  data_lake:
    driver: s3
    bucket: company-data-lake
    region: eu-west-1
    auth:
      type: default_credentials
    base_path: raw/partner

steps:
  - id: orders
    type: read_csv
    from:
      connection: incoming
      path: daily_orders.csv
    delimiter: ";"

  - id: cleaned
    type: filter
    input: orders
    condition: "status != \"cancelled\""

  - id: output
    type: write_json
    input: cleaned
    target:
      connection: data_lake
      path: orders/latest.json
    pretty: false
```

## See also

- [Connections index](./index.md) -- All connection types (database + file)
- [read_csv](../steps/read-csv.md) / [read_json](../steps/read-json.md) / [read_parquet](../steps/read-parquet.md) -- File source steps
- [write_csv](../steps/write-csv.md) / [write_json](../steps/write-json.md) / [write_parquet](../steps/write-parquet.md) -- File sink steps
- [Secrets](../secrets.md) -- Secret manager integration for credentials
- [Feature Flags](../feature-flags.md) -- Enable/disable individual transports at compile time