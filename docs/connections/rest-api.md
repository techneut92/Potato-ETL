# REST API

Driver: `rest_api`
Always available (no feature flag required).

REST API connections define a base URL, default authentication, and shared headers. Steps then append their specific path.

## Connection

```yaml
connections:
  my_api:
    driver: rest_api
    base_url: "https://api.example.com"
    auth:                         # optional
      type: bearer
      token: "${API_TOKEN}"
    headers:                      # optional -- default headers for all requests
      Accept: "application/json"
    timeout_secs: 30              # default: 30
    rate_limit_rps: 10.0          # optional -- max requests per second
```

| Field | Required | Default | Description |
|---|---|---|---|
| `base_url` | yes | -- | Base URL for all requests. Step `url` paths are appended to this. |
| `auth` | no | -- | Default authentication applied to every request. Overridden per-step. |
| `headers` | no | `{}` | Default headers sent with every request. Step-level headers merge on top (step wins on conflict). |
| `timeout_secs` | no | `30` | Per-request timeout in seconds |
| `rate_limit_rps` | no | unlimited | Maximum requests per second |

## URL resolution

The step `url` field is resolved against `base_url`:

- If `url` starts with `http://` or `https://` -- used as-is
- If `url` starts with `/` -- appended to `base_url`
- If `url` is empty -- `base_url` is used directly

## Auth types

| Type | Fields | Use case |
|---|---|---|
| `bearer` | `token` | OAuth / JWT bearer tokens |
| `basic` | `username`, `password` | HTTP Basic authentication |
| `api_key` | `header`, `key` | Any custom header (X-API-Key, AfasToken, etc.) |

## Examples

**GitHub API:**

```yaml
connections:
  github:
    driver: rest_api
    base_url: "https://api.github.com"
    auth:
      type: bearer
      token: "${GITHUB_TOKEN}"
    headers:
      Accept: "application/vnd.github.v3+json"
      X-GitHub-Api-Version: "2022-11-28"
    timeout_secs: 30
    rate_limit_rps: 10.0
```

**AFAS Profit (AfasToken scheme):**

```yaml
connections:
  afas:
    driver: rest_api
    base_url: "https://12345.afas.online/profitrestservices"
    auth:
      type: api_key
      header: Authorization
      key: "AfasToken <base64_encoded_token>"
    headers:
      Accept: "application/json"
    timeout_secs: 30
    rate_limit_rps: 5.0
```

**Basic auth:**

```yaml
connections:
  internal_api:
    driver: rest_api
    base_url: "https://internal.example.com/api/v1"
    auth:
      type: basic
      username: "service_account"
      password: "${API_PASSWORD}"
```
