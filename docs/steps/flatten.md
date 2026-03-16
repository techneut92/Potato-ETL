# flatten

Category: **Transform**

Extract fields from nested structs or JSON string columns into top-level columns. Supports dot notation for nested objects and bracket notation for array indices.

## Basic usage

```yaml
- id: flat
  type: flatten
  input: source
  select:
    user_id: payload.user.id
    tag_0: payload.tags[0]
    city: address.city
```

## All fields

| Field | Required | Description |
|---|---|---|
| `input` | yes | Step to read from |
| `select` | yes | Map of `target_column: source.path` |
