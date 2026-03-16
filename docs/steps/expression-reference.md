# Expression Reference

Expressions are used in `map` columns, `filter` conditions, `aggregate` metrics, and `environment` variables. They support column references, literals, operators, and function calls.

## Literals

| Syntax | Type | Example |
|---|---|---|
| Integer | Int64 | `42`, `-7` |
| Float | Float64 | `3.14`, `-0.5` |
| String | Utf8 | `"active"`, `'hello'` |
| Boolean | Boolean | `true`, `false` |
| Null | Null | `null` |
| Column ref | (column type) | `price`, `order_date` |
| Env variable | (variable type) | `$load_ts`, `$label` |

## Arithmetic operators

| Operator | Description |
|---|---|
| `+` | Addition |
| `-` | Subtraction |
| `*` | Multiplication |
| `/` | Division (returns null on divide by zero) |

## Comparison operators

| Operator | Description |
|---|---|
| `==` | Equal |
| `!=` | Not equal |
| `<` | Less than |
| `<=` | Less than or equal |
| `>` | Greater than |
| `>=` | Greater than or equal |

## Logical operators

| Operator | Description |
|---|---|
| `and` | Logical AND (three-valued: `false AND null = false`) |
| `or` | Logical OR |
| `not` | Logical NOT |

## Timestamp functions

| Function | Return type | Description |
|---|---|---|
| `now()` | Timestamp[us, UTC] | Current UTC time, same value for all rows in a batch |
| `now_naive()` | Timestamp[us] | Current UTC time without timezone info |
| `run_ts()` | Timestamp[us, UTC] | Pipeline start time -- same across all batches in a run |
| `run_ts_naive()` | Timestamp[us] | Pipeline start time without timezone info |
| `epoch_to_timestamp(col)` | Timestamp[us, UTC] | Convert unix epoch integer (seconds) to UTC timestamp. Aliases: `from_epoch`, `from_unix`. |
| `epoch_to_timestamp(col, "ms")` | Timestamp[us, UTC] | Convert unix epoch integer with explicit unit: `"s"`, `"ms"`, `"us"`, `"ns"` |

### Converting unix epoch timestamps

Columns containing unix epoch integers (seconds, milliseconds, or microseconds since 1970-01-01) can be converted to proper Arrow timestamps using `cast()` with bracket notation. The time unit in the bracket must match the source data's unit:

| Source unit | Expression | Result type |
|---|---|---|
| Seconds (e.g. `1700000000`) | `cast(col, "timestamp[s, UTC]")` | Timestamp[s, UTC] |
| Milliseconds (e.g. `1700000000000`) | `cast(col, "timestamp[ms, UTC]")` | Timestamp[ms, UTC] |
| Microseconds (e.g. `1700000000000000`) | `cast(col, "timestamp[us, UTC]")` | Timestamp[us, UTC] |

Drop the `, UTC` part for a naive (timezone-unaware) timestamp: `cast(col, "timestamp[s]")`.

**Example — source columns are unix seconds:**

```yaml
- id: fix_dates
  type: map
  input: source
  columns:
    created_at: cast(created, "timestamp[s, UTC]")
    updated_at: cast(updated, "timestamp[s, UTC]")
```

## Date/time extraction

| Function | Return type | Description |
|---|---|---|
| `year(col)` | Int32 | Works on Date32, Timestamp, or `"YYYY-MM-DD"` strings |
| `month(col)` | Int32 | Month (1-12) |
| `day(col)` | Int32 | Day of month |
| `hour(col)` | Int32 | Hour (0-23) |
| `minute(col)` | Int32 | Minute (0-59) |
| `second(col)` | Int32 | Second (0-59) |

## String functions

| Function | Aliases | Return type | Description |
|---|---|---|---|
| `upper(col)` | `ucase` | Utf8 | Uppercase |
| `lower(col)` | `lcase` | Utf8 | Lowercase |
| `trim(col)` | -- | Utf8 | Strip leading/trailing whitespace |
| `length(col)` | `len`, `char_length` | Int32 | UTF-8 character count |
| `concat(a, b, ...)` | -- | Utf8 | Concatenate values (any number of arguments) |

## JSON functions

| Function | Return type | Description |
|---|---|---|
| `json_get(col, "key")` | Utf8 | Extract field from JSON string; supports dot paths (`"a.b.c"`) and array indices (`"arr[0]"`) |
| `json_length(col)` | Int32 | Length of a JSON array. Alias: `json_array_length` |

## Null-handling functions

| Function | Aliases | Return type | Description |
|---|---|---|---|
| `coalesce(a, b, ...)` | -- | same as args | First non-null value. Preserves the original Arrow type when all arguments share the same type; falls back to `Utf8` when types are mixed. |
| `is_null(col)` | `isnull` | Boolean | True if the value is null |
| `is_not_null(col)` | `isnotnull`, `not_null` | Boolean | True if the value is not null |
| `if_null(col, default)` | `ifnull`, `nvl` | same as args | Returns `col` if non-null, otherwise `default`. Preserves the original Arrow type when both arguments share the same type; falls back to `Utf8` when types differ. |

## Type casting

| Function | Return type | Description |
|---|---|---|
| `cast(col, "type")` | varies | Cast to an Arrow type string, e.g. `"int32"`, `"float64"`, `"utf8"`, `"timestamp"` |

**Supported type strings** (used in `cast()`, `arrow_overrides` on sources, and `arrow_overrides` on sinks):

| Plain name | Aliases | Arrow type |
|---|---|---|
| `boolean` | `bool` | Boolean |
| `int8` | -- | Int8 |
| `int16` | `smallint` | Int16 |
| `int32` | `int`, `integer` | Int32 |
| `int64` | `bigint` | Int64 |
| `float32` | `real` | Float32 |
| `float64` | `double`, `double_precision` | Float64 |
| `utf8` | `string`, `text`, `varchar` | Utf8 |
| `large_utf8` | `longtext` | LargeUtf8 |
| `date32` | `date` | Date32 |
| `timestamp` | `ts`, `datetime` | Timestamp[us] (no timezone) |
| `binary` | -- | Binary |
| `large_binary` | -- | LargeBinary |

**Bracket notation** for precise control over time units and timezones:

| Syntax | Arrow type |
|---|---|
| `timestamp[s]`, `timestamp[ms]`, `timestamp[us]`, `timestamp[ns]` | Timestamp with time unit |
| `timestamp[us, UTC]` | Timestamp with timezone |
| `time32[s]`, `time32[ms]` | Time32 |
| `time64[us]`, `time64[ns]` | Time64 |
| `duration[s]`, `duration[ms]`, `duration[us]`, `duration[ns]` | Duration |

## Environment variable access

| Function | Description |
|---|---|
| `$name` | Shorthand -- reference a pipeline environment variable |
| `env("name")` | Function form -- equivalent to `$name` |

## Aggregate-only functions

These can only be used inside `aggregate` step `metrics:` blocks, not in `map` or `filter` expressions:

| Function | Return type | Description |
|---|---|---|
| `sum(col)` | Float64 | Sum of non-null values |
| `count()` | Int64 | Total row count in group |
| `count(col)` | Int64 | Non-null row count in group |
| `avg(col)` | Float64 | Average of non-null values |
| `mean(col)` | Float64 | Alias for `avg` |
| `min(col)` | same as col | Minimum value |
| `max(col)` | same as col | Maximum value |
| `first(col)` | same as col | First value in group |