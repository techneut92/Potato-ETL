# python_transform

Category: **Transform**

Run Python code per batch. The code receives a PyArrow `Table` as `table` and the `pa` (pyarrow) module. You must assign your result to the `result` variable.

> **Requires:** PyO3 0.28+, Arrow 58 with `pyarrow` feature, `pyarrow` Python
> package installed in the runtime environment.

## Inline code

```yaml
- id: add_bonus
  type: python_transform
  input: source
  code: |
    import pyarrow.compute as pc
    bonus = pc.multiply(table.column("salary"), 0.10)
    result = table.append_column("bonus", bonus.cast(pa.float64()))
```

## Named function

Register a function via the Rust or Python API before running the pipeline:

```yaml
- id: enrich
  type: python_transform
  input: source
  function: my_registered_function
```

## All fields

| Field | Required | Description |
|---|---|---|
| `input` | yes | Step to read from |
| `code` | no* | Inline Python code. *Use `code` or `function`, not both. |
| `function` | no* | Name of a pre-registered transform function |

## In-scope variables

| Variable | Type | Description |
|---|---|---|
| `table` | `pyarrow.Table` | The current batch of data |
| `pa` | module | The `pyarrow` module |
| `result` | (you assign) | Set this to your output `Table` or `RecordBatch` |

## Examples

**Filter rows:**

```python
result = table.filter(table.column('status') == 'active')
```

**Add a computed column:**

```python
import pyarrow.compute as pc
bonus = pc.multiply(table.column("salary"), 0.10)
result = table.append_column("bonus", bonus.cast(pa.float64()))
```

**Select specific columns:**

```python
result = table.select(["id", "name", "email"])
```
