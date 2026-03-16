# join

Category: **Transform**

Hash-join two branches on a shared key column.

## Basic usage

```yaml
- id: enriched
  type: join
  left: orders
  right: customers
  on: customer_id
  how: inner
```

## All fields

| Field | Required | Default | Description |
|---|---|---|---|
| `left` | yes | -- | Step ID for the left side |
| `right` | yes | -- | Step ID for the right side |
| `on` | yes | -- | Key column name (must exist in both sides) |
| `how` | no | `inner` | Join type: `inner`, `left`, `full` |

## Join types

| Type | Behavior |
|---|---|
| `inner` | Only rows where the key exists in both sides |
| `left` | All rows from the left side; nulls where the right side has no match |
| `full` | All rows from both sides; nulls where either side has no match |

Note: The right side is fully materialised in memory before joining. For large right-side datasets, ensure sufficient memory.
