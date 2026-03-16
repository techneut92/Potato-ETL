# aggregate

Category: **Transform**

Group rows by one or more columns and compute aggregate metrics. All incoming batches are materialised in memory before computing.

## Basic usage

```yaml
- id: by_customer
  type: aggregate
  input: source
  group_by: [customer_id]
  metrics:
    total_sales: sum(revenue)
    order_count: count()
    avg_revenue: avg(revenue)
```

## All fields

| Field | Required | Description |
|---|---|---|
| `input` | yes | Step to read from |
| `group_by` | yes | List of column names to group by. If empty, all rows collapse to a single result row (global aggregation). |
| `metrics` | yes | Map of `output_name: aggregate_expr` |

## Available aggregation functions

| Expression | Return type | Description |
|---|---|---|
| `sum(col)` | Float64 | Sum of non-null values |
| `count()` | Int64 | Total row count in group |
| `count(col)` | Int64 | Non-null row count in group |
| `avg(col)` | Float64 | Average of non-null values |
| `mean(col)` | Float64 | Alias for `avg` |
| `min(col)` | same as col | Minimum value (string comparison for non-numeric) |
| `max(col)` | same as col | Maximum value |
| `first(col)` | same as col | First (lowest-index) value in group |

## Global aggregation

If `group_by` is empty, all rows collapse to a single result row:

```yaml
- id: totals
  type: aggregate
  input: source
  group_by: []
  metrics:
    total_revenue: sum(revenue)
    row_count: count()
```
