# unnest

Explode a JSON array column into individual rows, optionally extracting sub-fields and carrying parent columns for junction table construction.

## When to use

- Normalizing nested JSON arrays into flat relational rows
- Building junction tables from one-to-many relationships embedded in JSON
- Multi-level denormalization (chain multiple `unnest` steps)
- Preparing nested API responses for database ingestion

## Configuration

```yaml
- id: pools_exploded
  type: unnest
  input: raw
  column: talent_pools           # array column to explode
  parent_fields:                 # optional: carry parent columns into output
    candidate_id: id
  fields:                        # optional: extract sub-fields from each array element
    pool_id: id
    pool_name: name
    pool_addresses: addresses
```

| Field | Required | Default | Description |
|---|---|---|---|
| `input` | yes | -- | Step ID to read data from. |
| `column` | yes | -- | Name of the array column to explode. Each element becomes a separate row. |
| `fields` | no | `{}` | Map of `output_name: source_key` to extract from each array element. When empty, the entire element is kept as a JSON string column. |
| `parent_fields` | no | `{}` | Map of `output_name: parent_column` to carry from the parent row into each exploded row. Essential for building junction tables. |

## How it works

Given input data like:

```json
[
  {
    "id": 1,
    "name": "Alice",
    "tags": [
      {"label": "rust", "level": "expert"},
      {"label": "python", "level": "intermediate"}
    ]
  }
]
```

With this configuration:

```yaml
- id: exploded
  type: unnest
  input: raw
  column: tags
  parent_fields:
    person_id: id
  fields:
    tag_label: label
    tag_level: level
```

The output is:

| person_id | tag_label | tag_level |
|---|---|---|
| 1 | rust | expert |
| 1 | python | intermediate |

## Multi-level unnesting

You can chain `unnest` steps to normalize deeply nested structures. Each level references the output of the previous `unnest`:

```yaml
steps:
  - id: raw
    type: read_json
    path: data/candidates.json
    data_path: candidates

  # Level 1: explode talent_pools array
  - id: pools_exploded
    type: unnest
    input: raw
    column: talent_pools
    parent_fields:
      candidate_id: id
    fields:
      pool_id: id
      pool_name: name
      pool_addresses: addresses    # keep nested array for next level

  # Level 2: explode addresses array from within talent_pools
  - id: addresses_exploded
    type: unnest
    input: pools_exploded
    column: pool_addresses
    parent_fields:
      pool_id: pool_id
    fields:
      address_id: id
      city: city
      country: country
```

## Junction table pattern

Use `parent_fields` + `flatten` (or `aggregate`) to build proper junction tables:

```yaml
steps:
  - id: pools_exploded
    type: unnest
    input: raw
    column: talent_pools
    parent_fields:
      candidate_id: id
    fields:
      pool_id: id

  # Junction table: candidate <-> talent pool
  - id: junction
    type: flatten
    input: pools_exploded
    select:
      candidate_id: candidate_id
      pool_id: pool_id

  - id: write_junction
    type: write_csv
    input: junction
    path: output/candidate_pools.csv
```

## Dimension table pattern

Combine `unnest` with `aggregate` to deduplicate and create dimension tables:

```yaml
steps:
  - id: pools_exploded
    type: unnest
    input: raw
    column: talent_pools
    parent_fields:
      candidate_id: id
    fields:
      pool_id: id
      pool_name: name
      pool_priority: priority

  # Deduplicate: one row per pool
  - id: pools_dim
    type: aggregate
    input: pools_exploded
    group_by: [pool_id]
    metrics:
      pool_name: first(pool_name)
      pool_priority: first(pool_priority)

  - id: write_pools
    type: write_json
    input: pools_dim
    path: output/talent_pools.json
    pretty: true
```

## Complete example

See `examples/cli/unnest/02_nested_json_to_files.yaml` for a full 3-level normalization pipeline that reads nested JSON and produces 5 output files (candidates, talent pools, addresses, and two junction tables).

## Error handling

| Condition | Behavior |
|---|---|
| `column` does not exist | Error during execution |
| `column` value is not a JSON array | Row is skipped (no output for that row) |
| `column` value is `null` | Row is skipped |
| `fields` key not found in array element | Output column is `null` for that row |

## See also

- [read_json](./read-json.md) -- JSON file source (common input for unnest)
- [flatten](./flatten.md) -- Extract struct/JSON sub-fields (non-array)
- [aggregate](./aggregate.md) -- Deduplicate exploded rows for dimension tables
