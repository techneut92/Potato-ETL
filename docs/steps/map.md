# map

Category: **Transform**

Add computed columns or derive new values using an expression language. Columns are evaluated in the order they appear -- later expressions can reference columns produced by earlier ones.

## Basic usage

```yaml
- id: enrich
  type: map
  input: source
  columns:
    load_ts: now()
    revenue: price * quantity
    order_year: year(order_date)
    uid: json_get(payload, "user.id")
    tag_0: json_get(payload, "tags[0]")
```

## All fields

| Field | Required | Default | Description |
|---|---|---|---|
| `input` | yes | -- | Step to read from |
| `columns` | yes | -- | Map of `output_name: expression` |
| `select_only` | no | `false` | When `true`, **only** columns listed in `columns` appear in the output. All unlisted columns are dropped. |

## select_only example

Project down to just three columns:

```yaml
- id: select
  type: map
  input: source
  select_only: true
  columns:
    id: id
    name: first_name
    total: price * quantity
```

## Available functions and operators

See the [Expression Reference](./expression-reference.md) for the complete list of functions, operators, and literals.

## Common patterns

### Unix epoch to timestamp

```yaml
- id: fix_dates
  type: map
  input: source
  columns:
    created_at: cast(created, "timestamp[s, UTC]")     # seconds
    updated_at: cast(updated, "timestamp[ms, UTC]")    # milliseconds
```

### Add audit columns from environment variables

```yaml
environment:
  load_ts: now()

steps:
  - id: enrich
    type: map
    input: source
    columns:
      inserted_at: $load_ts       # pipeline start time (consistent across batches)
      batch_ts: now()             # current batch time (varies per batch)
```