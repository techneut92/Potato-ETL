# Secret Manager Integration

`potato_etl` resolves `secret::` references in your pipeline YAML/JSON **before** the config is deserialized into typed structs. This means any string value in your config can be a secret reference -- connection credentials, API keys, tokens, or even entire connection objects.

## Reference format

```
secret::<provider>/<path>[#field]
```

| Component | Description |
|-----------|-------------|
| `provider` | Secret manager to query (see table below) |
| `path` | Provider-specific path to the secret |
| `#field` | Optional -- extract a single field from a structured (JSON/YAML) secret value |

## Supported providers

| Provider | Prefix | Aliases | Auth chain |
|----------|--------|---------|------------|
| **HashiCorp Vault** (KV v2) | `secret::vault` | `secret::hashicorp` | `VAULT_TOKEN` env var -> AppRole (`VAULT_ROLE_ID` + `VAULT_SECRET_ID`) -> Kubernetes Service Account |
| **Azure Key Vault** | `secret::azure` | `secret::akv` | Service Principal (`AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET` + `AZURE_TENANT_ID`) -> Managed Identity -> Azure CLI |
| **Google Secret Manager** | `secret::gcp` | `secret::gsm`, `secret::google` | `GOOGLE_APPLICATION_CREDENTIALS` (service account key file) -> GCE metadata server |

## Usage patterns

### Individual field references (recommended)

Reference specific fields within a structured secret:

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

### Plain string secrets

When the secret is a single value (not structured), omit the `#field`:

```yaml
connections:
  databricks:
    driver: databricks
    host: adb-123.azuredatabricks.net
    http_path: /sql/1.0/warehouses/abc123
    auth:
      type: pat
      token: "secret::azure/my-vault/databricks-token"
```

### Structured secrets with auto-detection

Secret values are tried as **JSON**, then **YAML**, then **plain string** (in that order). This means you can store secrets in any format:

**JSON secret in Vault (`prod/pg`):**
```json
{
  "username": "etl_user",
  "password": "s3cr3t!",
  "host": "db.internal",
  "port": 5432
}
```

**YAML secret in Azure Key Vault (`db-config`):**
```yaml
host: db.internal
port: 5432
password: hunter2
```

Both work with `#field` extraction:
```yaml
host: "secret::azure/my-vault/db-config#host"
password: "secret::azure/my-vault/db-config#password"
```

### Whole-connection resolution

Store the entire connection definition as a JSON secret and reference it at the top level:

```yaml
connections:
  pg_staging: "secret::vault/staging/pg_connection"
```

The secret value must be a valid connection object:
```json
{
  "driver": "postgres",
  "host": "staging-db.internal",
  "database": "analytics",
  "auth": {
    "type": "user_pass",
    "username": "etl_user",
    "password": "staging_pass"
  }
}
```

## Provider configuration

### HashiCorp Vault

**Pipeline-level configuration** (optional — most settings fall back to environment variables):

```yaml
secrets:
  vault:
    address: https://vault.internal:8200   # or $VAULT_ADDR
    mount: secret                          # KV v2 mount point (default: "secret")
    namespace: admin                       # Vault Enterprise namespace (optional)
    auth:
      method: token
      token: "${VAULT_TOKEN}"              # or omit — falls back to $VAULT_TOKEN

      # -- Alternative: AppRole --
      # method: approle
      # role_id: "${VAULT_ROLE_ID}"
      # secret_id: "${VAULT_SECRET_ID}"

      # -- Alternative: Kubernetes SA --
      # method: kubernetes
      # role: my-app-role
      # token_path: /var/run/secrets/kubernetes.io/serviceaccount/token  # default
      # mount_path: kubernetes  # Vault auth mount path (default: "kubernetes")
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `address` | no | `$VAULT_ADDR` | Vault server URL (e.g. `https://vault.internal:8200`) |
| `mount` | no | `secret` | KV v2 secrets engine mount point |
| `namespace` | no | -- | Vault Enterprise namespace |
| `auth.method` | no | `token` | Auth method: `token`, `approle`, or `kubernetes` |

| Environment variable | Description |
|---------------------|-------------|
| `VAULT_ADDR` | Vault server URL (fallback for `address`) |
| `VAULT_TOKEN` | Static token (fallback for `auth.token`) |
| `VAULT_ROLE_ID` | AppRole role ID |
| `VAULT_SECRET_ID` | AppRole secret ID |

**Path mapping:** `secret::vault/prod/pg` -> `GET /v1/<mount>/data/prod/pg`

### Azure Key Vault

**Pipeline-level configuration** (optional — auth is auto-detected):

```yaml
secrets:
  azure:
    vault_url: https://my-vault.vault.azure.net   # optional — auto-constructed from path

    # Explicit service principal (optional — skips auto-detection):
    # tenant_id: "${AZURE_TENANT_ID}"
    # client_id: "${AZURE_CLIENT_ID}"
    # client_secret: "${AZURE_CLIENT_SECRET}"

    # User-assigned managed identity (optional — default is system-assigned):
    # managed_identity_client_id: 12345678-1234-...
```

Authentication follows a **credential chain** (similar to Python's `DefaultAzureCredential`), tried in order:

1. **Service Principal** — if `tenant_id` + `client_id` + `client_secret` are available
2. **Managed Identity** — Azure IMDS endpoint (for AKS, App Service, VMs). Set `managed_identity_client_id` for user-assigned identities.
3. **Azure CLI** — shells out to `az account get-access-token`

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `vault_url` | no | auto-constructed | Override the vault URL (normally built from path) |
| `tenant_id` | no | `$AZURE_TENANT_ID` | Azure AD tenant ID |
| `client_id` | no | `$AZURE_CLIENT_ID` | Azure AD client (application) ID |
| `client_secret` | no | `$AZURE_CLIENT_SECRET` | Azure AD client secret |
| `managed_identity_client_id` | no | -- | Client ID for user-assigned managed identity |

**Path mapping:** `secret::azure/my-vault/secret-name` -> vault name `my-vault`, secret name `secret-name`

### Google Secret Manager

**Pipeline-level configuration** (optional — uses Application Default Credentials):

```yaml
secrets:
  gcp:
    project: my-gcp-project                           # optional — taken from path
    credentials_file: /path/to/service-account.json    # optional — falls back to $GOOGLE_APPLICATION_CREDENTIALS
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `project` | no | from path | Default GCP project ID (used when path contains only a secret name) |
| `credentials_file` | no | `$GOOGLE_APPLICATION_CREDENTIALS` | Path to service account key JSON file |

**Path mapping:** `secret::gcp/my-project/secret-name` -> project `my-project`, secret `secret-name` (latest version)

## Mixing secret references with environment variables

Secret references (`secret::...`) and environment variable references (`${...}`) can coexist in the same config. Environment variables are expanded first, then secret references are resolved:

```yaml
connections:
  pg:
    driver: postgres
    host: "${DB_HOST}"                              # env var
    database: analytics
    auth:
      type: user_pass
      username: "secret::vault/prod/pg#username"    # secret manager
      password: "secret::vault/prod/pg#password"    # secret manager
```