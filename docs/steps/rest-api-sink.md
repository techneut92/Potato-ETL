# rest_api_sink

Category: **Sink**

POST or PUT batches to a REST API endpoint. Supports per-row and batch modes, URL templates, and nested JSON construction.

## Basic usage

```yaml
- id: push_to_api
  type: rest_api_sink
  input: enriched
  conn: target_api
  url: /api/records
  method: POST
  mode: per_row
```

## All fields

| Field | Required | Default | Description |
|---|---|---|---|
| `conn` | no | -- | REST API connection (provides base_url, auth, headers) |
| `url` | yes | -- | URL path (appended to base_url) or full URL |
| `input` | yes | -- | Step to read from |
| `method` | no | `GET` | HTTP method: `POST`, `PUT`, `PATCH`, `DELETE`, `GET` |
| `mode` | no | `per_row` | Send mode: `per_row` (one request per row) or `batch` (one request per batch as array) |
| `auth` | no | from conn | Override the connection's authentication |
| `headers` | no | from conn | Extra headers (merged on top of connection headers) |
| `field_map` | no | `{}` | Nested JSON construction via dot-notation paths (see below) |
| `json_column` | no | -- | Name of a pre-built JSON column to use as the request body |
| `wrap_key` | no | -- | In `batch` mode, wrap the array in this key. `null` = bare array. |
| `url_template` | no | -- | URL template with `{column}` placeholders (per_row mode) |
| `rate_limit_rps` | no | from conn | Max requests per second |
| `timeout_secs` | no | `30` | Per-request timeout in seconds |
| `allow_non_2xx` | no | `false` | If `true`, non-2xx responses are ignored instead of causing an error |

## Per-row with URL templates

Use `{column_name}` placeholders in the URL -- they're replaced with the row's value:

```yaml
- id: update_users
  type: rest_api_sink
  input: users
  conn: api
  url_template: /api/users/{user_id}
  method: PUT
```

## Batch mode with wrapping

Send the entire batch as one JSON array, optionally wrapped in a key:

```yaml
- id: bulk_insert
  type: rest_api_sink
  input: enriched
  conn: api
  url: /api/records/bulk
  method: POST
  mode: batch
  wrap_key: records     # sends {"records": [{...}, {...}, ...]}
```

## Nested JSON via field_map

Construct nested JSON from flat columns:

```yaml
  field_map:
    user.name: full_name
    user.email: email
    address.city: city
    address.zip: postal_code
```

This produces JSON like `{"user": {"name": "...", "email": "..."}, "address": {"city": "...", "zip": "..."}}`.
