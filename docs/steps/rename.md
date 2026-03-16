# rename

Category: **Transform**

Rename columns. Arrow metadata (types, constraints) is preserved through the rename.

## Basic usage

```yaml
- id: clean_names
  type: rename
  input: source
  columns:
    EmId: employee_id
    FiNm: first_name
    LaNm: last_name
    SaSa: salary
```

## All fields

| Field | Required | Description |
|---|---|---|
| `input` | yes | Step to read from |
| `columns` | yes | Map of `old_name: new_name` |
