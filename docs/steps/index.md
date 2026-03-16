# Pipeline Steps

Steps are the building blocks of a pipeline. They run in dependency order -- each step declares its `input` to create the data flow. Sources have no input; transforms and sinks reference the step they read from.

## Step types at a glance

| Type | Category | Description | Docs |
|---|---|---|---|
| `read_db` | Source | Read from a database table or custom query | [read_db](./read-db.md) |
| `rest_api` | Source | Read from a REST API with automatic pagination | [rest_api](./rest-api.md) |
| `read_json` | Source | Read a JSON file as a source | [read_json](./read-json.md) |
| `read_csv` | Source | Read a CSV file as a source | [read_csv](./read-csv.md) |
| `read_parquet` | Source | Read a Parquet file as a source | [read_parquet](./read-parquet.md) |
| `filter` | Transform | Keep rows matching a condition | [filter](./filter.md) |
| `map` | Transform | Add or compute columns using expressions | [map](./map.md) |
| `rename` | Transform | Rename columns | [rename](./rename.md) |
| `flatten` | Transform | Extract nested fields (struct/JSON) into top-level columns | [flatten](./flatten.md) |
| `unnest` | Transform | Explode array columns into individual rows | [unnest](./unnest.md) |
| `join` | Transform | Hash-join two branches on a key column | [join](./join.md) |
| `aggregate` | Transform | Group-by with metric aggregations | [aggregate](./aggregate.md) |
| `python_transform` | Transform | Run Python code per batch | [python_transform](./python-transform.md) |
| `write_db` | Sink | Write to a database table | [write_db](./write-db.md) |
| `scd2_sink` | Sink | Slowly-Changing Dimension Type 2 history table | [scd2_sink](./scd2-sink.md) |
| `rest_api_sink` | Sink | POST/PUT batches to a REST API | [rest_api_sink](./rest-api-sink.md) |
| `write_csv` | Sink | Write data to a CSV file | [write_csv](./write-csv.md) |
| `write_json` | Sink | Write data to a JSON file | [write_json](./write-json.md) |
| `write_parquet` | Sink | Write data to a Parquet file | [write_parquet](./write-parquet.md) |

## Related references

- [Expression Reference](./expression-reference.md) -- all functions, operators, and literals for `map`, `filter`, `aggregate`, and `environment` expressions
- [Step Driver Options](./driver-options.md) -- per-step MSSQL, Oracle, Postgres, Databricks, MySQL, and cross-driver tuning options

---

## Environment variables

Define pipeline-level variables that are evaluated once at the start of the run. Reference them in `map`, `filter`, and other expression contexts with `$name` or `env("name")`.

```yaml
environment:
  load_ts: now()
  label: '"nightly_sync"'
  pipeline_name: '"pg_to_mssql_etl"'

steps:
  - id: enrich
    type: map
    input: source
    columns:
      inserted_at: $load_ts          # $name shorthand
      pipeline: env("label")         # env() function form (equivalent)
```

This is useful for stamping every row with the same run timestamp or tagging rows with a pipeline identifier.

---

## Step execution order

Steps run in dependency order -- the tool resolves the DAG automatically. You don't need to worry about ordering in the YAML, but it reads more naturally when you list sources first, transforms second, and sinks last.

A pipeline can have multiple sources, multiple transforms branching and merging, and multiple sinks. The only rules:

- Every `input` must reference an existing step ID
- Every `conn` must reference an existing connection name
- The graph must be acyclic (no circular dependencies)

Use `potato_etl validate --config pipeline.yaml` to check all of this without connecting to any database.