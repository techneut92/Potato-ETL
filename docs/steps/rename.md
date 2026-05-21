# rename

Category: **Transform**

Rename columns. Arrow metadata is preserved through the rename, so anything stamped on the source — `primary_key: true`, type overrides, `description`, etc. — follows the column to its new name.

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

## When to use `rename` vs the `schema:` block

| Want to… | Use |
|---|---|
| Rename one column the same way for many downstream sinks | A single `rename` step, then point every sink at it. Renames live in one place and metadata is preserved. |
| Rename a column only for one specific sink | `schema.database.columns.<src>.rename_to: <target>` on that sink (key = source name, case-insensitive). |
| Drop a column inline with its DDL hints | `schema.database.columns.<src>.drop: true`. |
| Inject an environment variable as a new column | `schema.arrow.columns.<col>.value: $env_var` on the sink. |
| Inject an explicit `NULL` column | `schema.arrow.columns.<col>.value: null`. |
| Inject a literal scalar | `schema.arrow.columns.<col>.value: 'genesys'` (or `42`, `true`). |
| Inject the result of an expression | `schema.arrow.columns.<col>.value: truncate($source.body, 4000)`. |

A separate `rename` step is preferred for *broad* renames because the intent is explicit and the new names are stated once. The `schema:` block forms above are right when the transformation is sink-specific.

> The legacy top-level `values:` field has been removed; the loader rejects pipelines that still use it.

## Metadata preservation

`apply_rename` rebuilds the schema with the new field names but clones `f.metadata()` onto each new `Field`. That means:

- `primary_key: true` stamped on the source carries through the rename, and downstream sinks pick it up during `create_table` and as the upsert merge key.
- Arrow type overrides set on the source survive too.
- The data arrays themselves are never cloned — only the schema is rebuilt.

Matching is case-insensitive: `EVENTTIME → event_time` works whether the upstream column arrived as `EventTime`, `eventtime`, or `EVENTTIME` (useful for Oracle sources, which uppercase unquoted identifiers).

## Multi-target fan-out

When one source feeds several differently-named targets, do the rename **once** and let each sink apply its own case transform via `identifier_case`. The PKs you stamp on the source propagate everywhere.

```yaml
steps:
  - id: source
    type: read_db
    from: { connection: mysql_src, query: "select * from audits" }
    schema:
      database:
        columns:
          value_hash: { primary_key: true }   # stamped once

  - id: renamed
    type: rename
    input: source
    columns:
      divisionId:  division_id
      auditId:     audit_id
      eventTime:   event_time
      # …

  - id: write_pg
    type: write_db
    input: renamed
    target: { connection: pg, schema: public, table: audits }
    mode: upsert
    # PK + upsert key inherited from source; no DDL block needed here.

  - id: write_ods
    type: write_db
    input: renamed
    target: { connection: ods, table: EXT_GENESYS_AUDITS }
    mode: upsert
    options: { identifier_case: upper }    # division_id → DIVISION_ID

  - id: write_dwh
    type: write_db
    input: renamed
    target: { connection: dwh, schema: stg, table: STG_GEN_AUDITS }
    mode: truncate
    options: { odbc: true, identifier_case: upper }
```

`identifier_case` is a pure case transform (see [universal schema options](../universal-schema-options.md#identifier_case-identifiercase)) — it cannot turn `divisionId` into `DIVISION_ID`. Do the word-shape change (camelCase → snake_case) in `rename`, then let `identifier_case` handle the casing per target.
