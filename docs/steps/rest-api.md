# rest_api (source)

Category: **Source**

Reads data from a REST API endpoint with automatic pagination.

## Basic usage

```yaml
- id: issues
  type: rest_api
  conn: github
  url: /repos/owner/repo/issues
  data_path: null
  pagination:
    strategy: offset
    offset_param: skip
    limit_param: take
    page_size: 100
```

## All fields

| Field | Required | Default | Description |
|---|---|---|---|
| `conn` | no | -- | REST API connection name (provides base_url, auth, headers) |
| `url` | yes | -- | URL path (appended to connection's base_url) or full URL |
| `method` | no | `GET` | HTTP method: `GET`, `POST`, `PUT`, `PATCH`, `DELETE` |
| `data_path` | no | `null` | Dot-notation path to the data array in the response. `null` = top-level array. |
| `params` | no | `{}` | Extra query parameters appended to the URL |
| `body` | no | -- | Request body (for POST/PUT) |
| `auth` | no | from conn | Override the connection's authentication for this step |
| `headers` | no | from conn | Extra headers (merged on top of connection headers; step wins on conflict) |
| `pagination` | no | none | Pagination configuration (see below) |
| `rate_limit_rps` | no | from conn | Max requests per second |
| `timeout_secs` | no | `30` | Per-request timeout in seconds |
| `allow_non_2xx` | no | `false` | If `true`, non-2xx responses return empty data instead of erroring |
| `dedup_key` | no | -- | Field name for cross-page deduplication (see below) |
| `schema` | no | -- | Unified schema block. `schema.arrow.columns` overrides inferred Arrow types after reading (preferred over flat `arrow_overrides`). |
| `arrow_overrides` | no | `{}` | Legacy flat Arrow type overrides per column. Prefer `schema.arrow.columns`. |
| `normalize_columns` | no | `false` | Lowercase all column names after reading |
| `exclude` | no | `[]` | List of column names to drop immediately after reading |

## Pagination strategies

### Cursor

Safest for live APIs. The server provides a cursor token for the next page:

```yaml
pagination:
  strategy: cursor
  cursor_path: meta.next_cursor     # where to find the cursor in the response
  cursor_param: cursor              # query parameter name for the next request
  page_size: 100
  size_param: limit                 # optional -- query param for page size
```

### Offset

Skip/limit pagination:

```yaml
pagination:
  strategy: offset
  offset_param: skip
  limit_param: take
  page_size: 100
  total_count_path: meta.total      # optional -- stop when total reached
  has_more_path: pagination.has_more  # optional -- stop when false
```

### Page

Page-number pagination:

```yaml
pagination:
  strategy: page
  page_param: page
  size_param: per_page
  page_size: 50
  first_page: 1                     # first page number (usually 0 or 1)
  total_count_path: count           # optional
  has_more_path: has_more           # optional
```

### LinkHeader

Follows RFC 5988 `Link: <url>; rel="next"` headers. No configuration needed:

```yaml
pagination:
  strategy: link_header
```

## Deduplication

When using `offset` or `page` pagination on a live data source, new records can be inserted between page fetches, causing duplicates. Set `dedup_key` to a unique field (like `id`) to automatically drop records you've already seen:

```yaml
- id: employees
  type: rest_api
  conn: api
  url: /employees
  dedup_key: employee_id
  pagination:
    strategy: offset
    offset_param: skip
    limit_param: take
    page_size: 100
```

`cursor` and `link_header` pagination are immune to this problem by design.