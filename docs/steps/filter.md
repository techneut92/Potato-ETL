# filter

Category: **Transform**

Keep only rows that match a condition. Use either the simple `column`/`value` form or the more powerful `condition` expression -- not both.

## Simple column/value filter

Keeps rows where `column` equals `value`:

```yaml
- id: active_only
  type: filter
  input: source
  column: status
  value: active
```

## Expression filter

```yaml
- id: high_value
  type: filter
  input: source
  condition: "status == \"active\" and revenue >= 500"
```

## All fields

| Field | Required | Description |
|---|---|---|
| `input` | yes | Step to read from |
| `column` | no | Column name (simple form) |
| `value` | no | Value to match (simple form) |
| `condition` | no | Expression string (expression form) |

See the [Expression Reference](./expression-reference.md) for the full list of operators and functions available in `condition`.
