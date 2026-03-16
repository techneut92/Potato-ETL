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
| `values` | no | `{}` | Column mapping -- batch col rename, env var injection, or explicit NULL (see below). Formerly named `columns` (renamed in code; the old name is not accepted). |
| `arrow_overrides` | no | `{}` | Legacy flat Arrow type overrides. Prefer `schema.arrow.columns`. |
| `column_options` | no | `{}` | Legacy flat DDL hints (see below). Prefer `schema.database.columns`. |
| `options` | no | -- | Per-step driver options (see [Step driver options](./driver-options.md)) |

## Write modes

| Mode | Behavior |
|---|---|
| `append` | Insert rows. Errors on duplicate PK. *(default)* |
| `insert_ignore` | Insert and silently skip duplicates |
| `upsert` | Update existing rows + insert new ones (MERGE) |
| `merge_delete` | Like upsert, but also deletes target rows absent from the source |
| `truncate` | TRUNCATE the table first, then insert everything |

For `upsert`, `insert_ignore`, and `merge_delete`, the merge key is derived from columns marked with `primary_key: true` in `column_options`.

## Auto table creation

| `create_table` | Behavior |
|---|---|
| `never` | Don't touch DDL *(default)* |
| `if_not_exists` | Create the table if it doesn't exist; no-op if it does. Safe for production. |
| `replace` | Drop and recreate the table every run. **Development only -- destroys data.** |

DDL is generated from the Arrow schema of the first batch. Column types, nullability, primary keys, foreign keys, defaults, unique constraints, check constraints, and indexes are all derived from column options and metadata.

## Unified schema block

The `schema:` block is the preferred way to configure Arrow type casts, DDL column types, indexes, and constraints in a single structured block. It replaces the legacy flat `arrow_overrides`, `column_options`, and the removed `type_override` fields.

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
          type: VARCHAR(50)       # SQL type for DDL (replaces old type_override)
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

**`schema.arrow.columns`** — per-column Arrow type overrides applied at the sink boundary before writing. Same semantics as the legacy flat `arrow_overrides`.

**`schema.database.columns`** — per-column DDL hints. The `type` field replaces the old standalone `type_override` map; other fields (`primary_key`, `nullable`, `unique`, `generated`, `check_expr`, `default_expr`, `on_update_expr`, `foreign_key`, `description`) are the same. Note: `index` is **not** available here — use `schema.database.indexes` for named indexes instead.

**`schema.database.indexes`** — named, multi-column indexes emitted as `CREATE INDEX` after table creation.

**`schema.database.constraints`** — named table-level constraints emitted as `ALTER TABLE ... ADD CONSTRAINT` after table creation.

> **Legacy fields:** The flat `arrow_overrides`, `column_options`, and `values` fields still work alongside the `schema:` block. When both are present, `schema.arrow.columns` wins over flat `arrow_overrides`, and `schema.database.columns` wins over flat `column_options`, for columns specified in both.

> **Removed:** The standalone `type_override` field has been removed. Use `schema.database.columns.<col>.type` (or the legacy `column_options.<col>.db_type`) instead.

## Column options (legacy flat form)

Per-column DDL hints. Prefer `schema.database.columns` for new pipelines.

## Column options

Per-column DDL hints for auto-created tables:

```yaml
  column_options:
    employee_id:
      primary_key: true           # part of the primary key
      nullable: false             # NOT NULL
      generated: true             # IDENTITY / SERIAL / AUTO_INCREMENT
    email:
      unique: true                # UNIQUE constraint
      nullable: false
    salary:
      db_type: DECIMAL(19,4)      # explicit SQL type for DDL
      check_expr: "salary >= 0"   # CHECK constraint
      default_expr: "0.00"        # DEFAULT value
      index: true                 # create a standalone index
      description: "Monthly gross salary"  # COMMENT ON COLUMN (Postgres/Oracle)
    status:
      enum_values: [draft, active, archived, deleted]  # dialect-appropriate enum constraint
    updated_at:
      default_expr: "now()"       # DEFAULT now() -- auto-normalised per dialect
      on_update_expr: "now()"     # MySQL: ON UPDATE; Postgres/MSSQL/Oracle: trigger
    department_id:
      foreign_key:                # FOREIGN KEY constraint
        table: hr.departments
        column: id
        schema: hr                # optional -- schema of the referenced table
      index: true
```

| Option | Type | Default | Description |
|---|---|---|---|
| `db_type` | string | -- | Explicit SQL type for DDL generation (e.g. `DECIMAL(19,4)`, `VARCHAR(50)`). In the unified `schema.database.columns` block, use `type` instead. |
| `primary_key` | bool | `false` | Include in PRIMARY KEY constraint |
| `unique` | bool | `false` | UNIQUE constraint |
| `index` | bool | `false` | Create a standalone index |
| `nullable` | bool | `true` | Whether the column allows NULL |
| `generated` | bool | `false` | Column is auto-generated (IDENTITY / SERIAL / AUTO_INCREMENT) |
| `check_expr` | string | -- | SQL CHECK expression |
| `default_expr` | string | -- | SQL DEFAULT expression (e.g. `"now()"` -- auto-normalised per dialect) |
| `on_update_expr` | string | -- | Auto-update on row modification. MySQL: inline `ON UPDATE`; Postgres/MSSQL/Oracle: trigger. Databricks: not supported. |
| `foreign_key` | object | -- | Foreign key reference (`table`, `column`, optional `schema`) |
| `description` | string | -- | Human-readable comment |
| `enum_values` | list | -- | Allowed values. Postgres: `CREATE TYPE ... AS ENUM`; MySQL: inline `ENUM(...)`; MSSQL/Oracle/Databricks: `CHECK` constraint. |

> **Note:** All five drivers (Postgres, MSSQL, Oracle, MySQL, Databricks) now use
> the shared `generate_ddl_with_schema()` function from `potato-etl-common` for
> DDL generation. This means PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK, DEFAULT,
> indexes, named constraints, and `enum_values` work consistently across all
> targets. Databricks is intentionally excluded from some constraint features
> because Delta Lake uses `USING DELTA` syntax and has different constraint
> semantics.

## Value mapping

Map target column names to value sources at the sink boundary -- a lightweight alternative to adding a separate `rename` or `map` step. The field is called `values` (previously `columns`; the old name is **not** accepted as an alias — you must update your config).

```yaml
  values:
    LOAD_DATETIME: $inserted_at     # batch col or env var → new column LOAD_DATETIME
    CREATED_AT: $load_ts            # env var load_ts (e.g. now()) → new column CREATED_AT
    SYSTEM_PRESENCE: null            # explicit NULL (database receives NULL, not DEFAULT)
    ORDER_ID: $id                    # batch col 'id' → renamed to ORDER_ID
```

- `$name` is resolved in order: (1) batch column named `name` → data used, field renamed to target; (2) pipeline `environment:` variable named `name` → value broadcast to all rows as new column; (3) neither → error.
- `null` injects an explicit SQL NULL into every row. Because the column IS included in the INSERT, the database stores NULL — it does **not** trigger DEFAULT expressions. Exception: MSSQL with `mode: odbc` (no KEEPNULLS) replaces NULL with the column's DEFAULT.
- To let the database fill a DEFAULT, **omit** the column from `values:` entirely and don't include it in the batch — the alignment layer will skip it.
- Columns not listed are passed through unchanged

### Injecting columns from environment variables

The `values:` mapping can reference pipeline-level variables from the `environment:` block. This is useful for injecting audit columns like `inserted_at` without a separate `map` step:

```yaml
environment:
  load_ts: now()                    # evaluated once at pipeline start

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
    values:
      inserted_at: $load_ts         # inject pipeline timestamp
      updated_at: $load_ts          # initial value; server refreshes via trigger
    schema:
      database:
        columns:
          inserted_at:
            type: TIMESTAMPTZ
          updated_at:
            type: TIMESTAMPTZ
            default_expr: "now()"     # server fills on INSERT (when column omitted)
            on_update_expr: "now()"   # server updates on UPDATE
```

> **Note:** `column_options` / `schema.database.columns` only affect columns that are **present in the batch**. If a column is listed but doesn't exist in the data stream, it is silently ignored and won't appear in the generated DDL. Always ensure the column is in the batch — either from the source, a `map` step, or the sink's `values:` mapping.