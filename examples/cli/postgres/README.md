# examples/cli/postgres

Self-contained Postgres example project for `potato_etl`.

## Setup

```bash
# Create the database and user (run once)
psql -U postgres -c "CREATE USER etl_user WITH PASSWORD 'etl_pass';"
psql -U postgres -c "CREATE DATABASE etl_demo OWNER etl_user;"
psql -U postgres -c "GRANT ALL ON DATABASE etl_demo TO etl_user;"

# Seed the schema and data
psql -U etl_user -d etl_demo -f examples/cli/postgres/seed.sql
```

## Running the examples

All commands are run from the **workspace root** (`potato_etl/`).

```bash
# 01 — plain copy: employees → employees_active (truncate)
cargo run -p potato-etl-cli --features postgres -- run \
  --config examples/cli/postgres/01_copy.yaml

# 02 — filter active rows, compute bonus, upsert
cargo run -p potato-etl-cli --features postgres -- run \
  --config examples/cli/postgres/02_filter_upsert.yaml

# 03 — SCD Type 2 history: track salary/status changes over time
#       Tip: UPDATE a salary, then re-run to see the new version appear.
cargo run -p potato-etl-cli --features postgres -- run \
  --config examples/cli/postgres/03_scd2.yaml

# 04 — type showcase: every Postgres type through the Arrow pipeline
cargo run -p potato-etl-cli --features postgres -- run \
  --config examples/cli/postgres/04_types_showcase.yaml

# 05 — aggregate: payroll statistics grouped by department
cargo run -p potato-etl-cli --features postgres -- run \
  --config examples/cli/postgres/05_aggregate.yaml
```

## Useful CLI commands

```bash
# Validate pipeline YAML without a DB connection
cargo run -p potato-etl-cli -- validate \
  --config examples/cli/postgres/04_types_showcase.yaml

# Static step-by-step walkthrough (no connection)
cargo run -p potato-etl-cli -- explain \
  --config examples/cli/postgres/03_scd2.yaml

# Print the Arrow schema of a named step (requires a live connection)
cargo run -p potato-etl-cli --features postgres -- schema \
  --config examples/cli/postgres/02_filter_upsert.yaml --step enriched

# Dry-run: read 1 batch per source, all sinks are no-ops
cargo run -p potato-etl-cli --features postgres -- dry-run \
  --config examples/cli/postgres/05_aggregate.yaml --format pretty
```

## What seed.sql creates

| Table | Rows | Purpose |
|---|---|---|
| `departments` | 5 | Lookup table — referenced by employees |
| `employees` | 12 | Source — mix of active / inactive / on_leave |
| `orders` | 7 | Secondary source — referenced in multi-source example |
| `type_showcase` | 3 | One row per interesting Postgres type |
| `employees_active` | — | Target for 01 / 02 |
| `employees_history` | — | SCD2 target for 03 |
| `dept_salary_summary` | — | Aggregate target for 05 |

## Postgres → Arrow type mapping

| Postgres type | Arrow `DataType` | `LogicalType` |
|---|---|---|
| `SERIAL` / `INTEGER` | `Int32` | — |
| `BIGINT` | `Int64` | — |
| `NUMERIC(p,s)` | `Decimal128(p,s)` | — |
| `REAL` | `Float32` | — |
| `DOUBLE PRECISION` | `Float64` | — |
| `BOOLEAN` | `Boolean` | — |
| `CHAR(n)` / `VARCHAR(n)` / `TEXT` | `Utf8` | — |
| `DATE` | `Date32` | — |
| `TIME` | `Time64(Microsecond)` | — |
| `TIMESTAMP` | `Timestamp(µs, None)` | — |
| `TIMESTAMPTZ` | `Timestamp(µs, UTC)` | — |
| `UUID` | `Utf8` | `uuid` |
| `JSONB` / `JSON` | `Utf8` | `json` |
| `INET` / `CIDR` | `Utf8` | `ip` |
| `MACADDR` | `Utf8` | `macaddr` |
| `BYTEA` | `LargeBinary` | — |
| `TEXT[]` | `Utf8` (JSON-serialised) | — |
