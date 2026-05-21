# write_db

Category: **Sink**

Write rows to a database table. Supports multiple write modes, automatic table creation, column mapping, and per-column DDL hints.

## Basic usage

```yaml
- id: output
  type: write_db
  input: enriched
  target:
    connection: target_db
    table: orders_enriched
  mode: upsert
  create_table: if_not_exists
  schema:
    database:
      columns:
        order_id:
          primary_key: true
```

## All fields

| Field | Required | Default | Description |
|---|---|---|---|
| `target.connection` | yes | -- | Named connection reference |
| `target.table` | yes | -- | Target table name |
| `target.schema` | no | -- | Database schema / namespace (e.g. `hr`, `dbo`) |
| `input` | yes | -- | Step to read from |
| `mode` | no | `append` | Write mode (see below) |
| `create_table` | no | `never` | Auto-create the target table (see below) |
| `batch_size` | no | global | Sub-chunk incoming batches to this size before writing |
| `schema` | no | -- | Unified schema block with `schema.arrow.columns` and `schema.database.columns` / `indexes` / `constraints` (see [Unified schema block](#unified-schema-block)) |
| `options` | no | -- | Per-step driver options (see [Step driver options](./driver-options.md)) |

## Write modes

| Mode | Behavior |
|---|---|
| `append` | Insert rows. Errors on duplicate PK. *(default)* |
| `insert_ignore` | Insert and silently skip duplicates |
| `upsert` | Update existing rows + insert new ones (MERGE) |
| `merge_delete` | Like upsert, but also deletes target rows absent from the source |
| `truncate` | TRUNCATE the table first, then insert everything |

For `upsert`, `insert_ignore`, and `merge_delete`, the merge key is derived from columns marked with `primary_key: true` in `schema.database.columns`.

## Auto table creation

| `create_table` | Behavior |
|---|---|
| `never` | Don't touch DDL *(default)* |
| `if_not_exists` | Create the table if it doesn't exist; no-op if it does. Safe for production. |
| `replace` | Drop and recreate the table every run. **Development only -- destroys data.** |

DDL is generated from the Arrow schema of the first batch. Column types, nullability, primary keys, foreign keys, defaults, unique constraints, check constraints, and indexes are all derived from column options and metadata.

## Unified schema block

The `schema:` block is the canonical way to configure Arrow type casts, DDL column types, indexes, and constraints in a single structured block.

```yaml
- id: sink
  type: write_db
  input: source
  target:
    connection: pg
    schema: hr
    table: employees
  mode: upsert
  create_table: if_not_exists

  schema:
    arrow:
      columns:
        created_at:
          type: "timestamp[us, UTC]"
        salary:
          type: float64
    database:
      columns:
        employee_id:
          type: VARCHAR(50)       # SQL type override for DDL
          primary_key: true
          nullable: false
          generated: true         # IDENTITY / SERIAL / AUTO_INCREMENT
        email:
          unique: true
          nullable: false
        salary:
          type: DECIMAL(19,4)
          check_expr: "salary >= 0"
          default_expr: "0.00"
        inserted_at:
          type: TIMESTAMPTZ
          default_expr: "now()"
          nullable: false
        updated_at:
          type: TIMESTAMPTZ
          default_expr: "now()"
          on_update_expr: "now()"
          nullable: false
      indexes:
        idx_employees_dept:
          columns: [department_id, hire_date]
        idx_employees_email:
          columns: [email]
          unique: true
      constraints:
        chk_status:
          check: "status IN ('active','inactive','terminated')"
```

**`schema.arrow.columns`** — per-column Arrow type overrides applied at the sink boundary before writing.

**`schema.database.columns`** — per-column DDL hints: `type`, `primary_key`, `nullable`, `unique`, `generated`, `check_expr`, `default_expr`, `on_update_expr`, `foreign_key`, `description`, `enum_values`. Note: `index` is **not** available here — use `schema.database.indexes` for named indexes instead.

**`schema.database.indexes`** — named, multi-column indexes emitted as `CREATE INDEX` after table creation.

**`schema.database.constraints`** — named table-level constraints emitted as `ALTER TABLE ... ADD CONSTRAINT` after table creation.

## Per-column DDL hints (`schema.database.columns`)

Drives the `CREATE TABLE` statement (when `create_table: if_not_exists` / `replace`) and the merge-key selection for upsert modes.

```yaml
  schema:
    database:
      columns:
        employee_id:
          primary_key: true           # part of the primary key + merge key for upsert
          nullable: false             # NOT NULL
          generated: true             # IDENTITY / SERIAL / AUTO_INCREMENT
        email:
          unique: true                # UNIQUE constraint
          nullable: false
        salary:
          type: DECIMAL(19,4)         # explicit SQL type for DDL
          check_expr: "salary >= 0"   # CHECK constraint
          default_expr: "0.00"        # DEFAULT value
          description: "Monthly gross salary"  # COMMENT ON COLUMN (Postgres/Oracle)
        status:
          enum_values: [draft, active, archived, deleted]  # dialect-appropriate enum
        updated_at:
          default_expr: "now()"       # DEFAULT now() -- auto-normalised per dialect
          on_update_expr: "now()"     # MySQL: ON UPDATE; Postgres/MSSQL/Oracle: trigger
        department_id:
          foreign_key:                # FOREIGN KEY constraint
            table: hr.departments
            column: id
            schema: hr                # optional -- schema of the referenced table
      indexes:
        idx_employees_dept:
          columns: [department_id]
```

| Option | Type | Default | Description |
|---|---|---|---|
| `type` | string | -- | Explicit SQL type for DDL generation (e.g. `DECIMAL(19,4)`, `VARCHAR(50)`). Highest priority in the type resolver. |
| `primary_key` | bool | `false` | Include in PRIMARY KEY constraint (and merge key for `upsert` / `insert_ignore` / `merge_delete`) |
| `unique` | bool | `false` | UNIQUE constraint |
| `nullable` | bool | `true` | Whether the column allows NULL |
| `generated` | bool | `false` | Column is auto-generated (IDENTITY / SERIAL / AUTO_INCREMENT) |
| `check_expr` | string | -- | SQL CHECK expression |
| `default_expr` | string | -- | SQL DEFAULT expression (e.g. `"now()"` -- auto-normalised per dialect) |
| `on_update_expr` | string | -- | Auto-update on row modification. MySQL: inline `ON UPDATE`; Postgres/MSSQL/Oracle: trigger. Databricks: not supported. |
| `foreign_key` | object | -- | Foreign key reference (`table`, `column`, optional `schema`) |
| `description` | string | -- | Human-readable comment |
| `enum_values` | list | -- | Allowed values. Postgres: `CREATE TYPE ... AS ENUM`; MySQL: inline `ENUM(...)`; MSSQL/Oracle/Databricks: `CHECK` constraint. |

For standalone indexes, use `schema.database.indexes` (named, multi-column-capable). Per-column `index: true` shorthand is not supported in the unified block.

> **Note:** All five drivers (Postgres, MSSQL, Oracle, MySQL, Databricks) now use
> the shared `generate_ddl_with_schema()` function from `potato-etl-common` for
> DDL generation. This means PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK, DEFAULT,
> indexes, named constraints, and `enum_values` work consistently across all
> targets. Databricks is intentionally excluded from some constraint features
> because Delta Lake uses `USING DELTA` syntax and has different constraint
> semantics.

## Renaming and value injection

The legacy top-level `values:` field has been removed. Three primitives now cover what it did:

| Goal | Where to put it | Example |
|---|---|---|
| Rename a column | `schema.database.columns.<src>.rename_to: <target>` (key = source name, case-insensitive) | `divisionId: { rename_to: DIVISION_ID }` |
| Drop a column | `schema.database.columns.<src>.drop: true` | `legacy_col: { drop: true }` |
| Inject a column from an env var | `schema.arrow.columns.<col>.value: $name` | `inserted_at: { value: $load_ts, type: "timestamp[us, UTC]" }` |
| Inject a column from another batch column | `schema.arrow.columns.<col>.value: $source.col` | `event_ts: { value: $source.created_at }` |
| Inject a literal | `schema.arrow.columns.<col>.value: <scalar>` | `source_system: { value: 'genesys' }` |
| Inject a NULL column | `schema.arrow.columns.<col>.value: null` | `archived_at: { value: null, type: "timestamp[us]" }` |
| Inject the result of an expression | `schema.arrow.columns.<col>.value: <expr>` | `body: { value: truncate($source.raw_body, 4000) }` |

> **Top-level `values:` is rejected at load.** Pipelines that still carry it get a parse-time error with a migration hint pointing here. There is no migration shim.

```yaml
environment:
  load_ts: now()                       # evaluated once at pipeline start

steps:
  - id: source
    type: read_db
    from:
      connection: pg
      table: orders

  - id: sink
    type: write_db
    input: source
    target:
      connection: target_db
      table: orders_archive
    create_table: if_not_exists
    schema:
      arrow:
        columns:
          inserted_at:
            value: $load_ts            # inject env var (broadcast to every row)
            type: "timestamp[us, UTC]"
          updated_at:
            value: $load_ts            # initial value; server refreshes via trigger
            type: "timestamp[us, UTC]"
          source_system:
            value: 'orders'            # literal string broadcast
      database:
        columns:
          inserted_at:
            type: TIMESTAMPTZ
          updated_at:
            type: TIMESTAMPTZ
            default_expr: "now()"      # server fills on INSERT (when column omitted)
            on_update_expr: "now()"    # server updates on UPDATE
          legacy_temp:
            drop: true                 # drop the column entirely
```

The value string is parsed via the expression DSL ([`expression-reference.md`](./expression-reference.md)). Top-level dispatch:

- `null` → NULL fill
- `$name` (no dot) → env-var broadcast
- `$step.col` or `$source.col` → batch column copy, case-insensitive
- bare identifier / quoted string / number / bool → broadcast literal
- anything with `(` → expression evaluation (function calls, arithmetic, casts)

> **Note:** `schema.database.columns` only affects columns that are **present in the batch**. If a column is listed but doesn't exist in the data stream (and isn't created via `schema.arrow.columns.<col>.value:`), it is silently ignored and won't appear in the generated DDL.

## Patterns

### Adding a server-stamped metadata column (e.g. `LOAD_DATETIME`)

The cleanest place to inject a column from an environment variable is `schema.arrow.columns.<col>.value:` — it puts the column in the batch *and* keeps it co-located with any Arrow type override. The DDL hint then takes effect because the column is now in the batch.

```yaml
environment:
  load_ts: now()                          # evaluated once at pipeline start

steps:
  - id: sink
    type: write_db
    input: source
    target: { connection: dwh, schema: stg, table: STG_ORDERS }
    mode: truncate
    schema:
      arrow:
        columns:
          LOAD_DATETIME:
            value: $load_ts               # → puts the col in the batch
            type: "timestamp[us, UTC]"    # optional cast
      database:
        columns:
          LOAD_DATETIME:
            default_expr: "getdate()"     # server-side default for direct inserts
```

`schema.arrow.columns.X.value:` injects the column into the batch, and `schema.database.columns.X.default_expr:` keeps a server-side default for direct inserts that bypass the pipeline. Both lines refer to the same target column, which is fine; the runtime stamps the DDL hint and injects the value in one pass.

### Multi-target fan-out (one source → many sinks)

Source-stamped metadata flows through `rename` and `identifier_case` (see [rename](./rename.md) and [identifier_case](../universal-schema-options.md#identifier_case-identifiercase)), so you don't need to repeat it. The recipe:

1. Stamp primary keys, type overrides, and constraints **once** on the source step.
2. If a target needs a different word shape than the source (e.g. camelCase source → snake_case PG), do that in a single `rename` step. The targets that don't need it can consume `source` directly.
3. Per-sink: `identifier_case` for casing only; no per-sink rename map; no per-sink PK declaration.

```yaml
steps:
  - id: source
    type: read_db
    from: { connection: mysql_src, query: "select * from audits" }
    schema:
      arrow:
        columns:
          eventTime: { type: "timestamp[us]" }
      database:
        columns:
          value_hash: { primary_key: true }    # stamped once

  - id: renamed                                # only the targets that need snake_case use this
    type: rename
    input: source
    columns: { divisionId: division_id, auditId: audit_id, eventTime: event_time }

  - id: write_pg
    type: write_db
    input: renamed                             # snake_case naming for PG
    target: { connection: pg, schema: public, table: audits }
    mode: upsert
    keys: [value_hash]                         # post-rename name

  - id: write_ods
    type: write_db
    input: source                              # ODS reads source directly
    target: { connection: ods, table: EXT_GENESYS_AUDITS }
    mode: upsert
    keys: [value_hash]                         # source col name
    options: { identifier_case: upper }        # divisionId → DIVISIONID

  - id: write_dwh
    type: write_db
    input: source                              # DWH reads source directly
    target: { connection: dwh, schema: stg, table: STG_GEN_AUDITS }
    mode: truncate
    options: { odbc: true, identifier_case: upper }
```

- `identifier_case` is *case only*. If a target needs `SNAKE_CASE_UPPER` from a camelCase source, route it through `rename` *and* `identifier_case: upper`.
- `keys:` references columns as they appear in the batch the sink consumes. PG (post-rename) uses `value_hash`; ODS/DWH (pre-rename source) use the source name verbatim.
- Each sink still needs its own `keys:` for upsert mode; the PK flag on source covers DDL, but the merge predicate is declared per-sink.
- For sinks that don't need DDL (existing tables, `create_table: never`) the `schema.database` block can be omitted entirely on that sink — the source-stamped metadata is irrelevant once the table already exists.