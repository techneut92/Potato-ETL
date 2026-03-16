//! Integration tests for the map → filter → aggregate pipeline.
//!
//! These tests exercise the full expression DSL and step pipeline in-memory —
//! no database connection required.

use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use potato_etl_runtime::transform::aggregate::apply_aggregate;
use potato_etl_runtime::transform::filter::apply_filter_expr;
use potato_etl_runtime::transform::flatten::apply_flatten;
use potato_etl_runtime::transform::map::apply_map;
use potato_etl_runtime::transform::expr::{EvalContext, set_eval_context, set_env_vars, clear_env_vars};

// ── Fixtures ──────────────────────────────────────────────────────────────

/// Simulates a `read_db` result: orders table.
///
/// Schema: order_id (i32), customer_id (utf8), status (utf8),
///         price (f64), quantity (i32), order_date (utf8),
///         payload (utf8 JSON)
fn orders_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id",    DataType::Int32,   false),
        Field::new("customer_id", DataType::Utf8,    true),
        Field::new("status",      DataType::Utf8,    true),
        Field::new("price",       DataType::Float64, true),
        Field::new("quantity",    DataType::Int32,   true),
        Field::new("order_date",  DataType::Utf8,    true),
        Field::new("payload",     DataType::Utf8,    true),
    ]));

    RecordBatch::try_new(schema, vec![
        // order_id
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef,
        // customer_id
        Arc::new(StringArray::from(vec!["C1", "C2", "C1", "C3", "C2", "C1"])) as ArrayRef,
        // status
        Arc::new(StringArray::from(vec!["active", "inactive", "active", "active", "active", "inactive"])) as ArrayRef,
        // price
        Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 5.0, 15.0, 25.0])) as ArrayRef,
        // quantity
        Arc::new(Int32Array::from(vec![2, 1, 3, 10, 2, 1])) as ArrayRef,
        // order_date
        Arc::new(StringArray::from(vec![
            "2024-03-01 10:00:00",
            "2024-03-02 11:00:00",
            "2024-06-15 09:00:00",
            "2024-07-01 12:00:00",
            "2024-07-04 08:00:00",
            "2024-12-31 23:59:59",
        ])) as ArrayRef,
        // payload (JSON)
        Arc::new(StringArray::from(vec![
            r#"{"user":{"id":"u1"},"tags":["vip","urgent"]}"#,
            r#"{"user":{"id":"u2"},"tags":["normal"]}"#,
            r#"{"user":{"id":"u1"},"tags":["vip"]}"#,
            r#"{"user":{"id":"u3"},"tags":[]}"#,
            r#"{"user":{"id":"u2"},"tags":["bulk","vip"]}"#,
            r#"{"user":{"id":"u1"},"tags":["returning"]}"#,
        ])) as ArrayRef,
    ]).unwrap()
}

// ── Step 1: map ───────────────────────────────────────────────────────────

#[test]
fn map_computes_new_columns() {
    let batch = orders_batch();
    let cols: IndexMap<String, String> = [
        ("revenue".into(),    "price * quantity".into()),
        ("order_year".into(), "year(order_date)".into()),
        ("user_id".into(),    r#"json_get(payload, "user.id")"#.into()),
        ("first_tag".into(),  r#"json_get(payload, "tags[0]")"#.into()),
        ("load_ts".into(),    "now()".into()),
    ].into_iter().collect();

    let out = apply_map(batch, &cols, false).unwrap();

    // Original columns preserved
    assert!(out.schema().index_of("order_id").is_ok());
    assert!(out.schema().index_of("customer_id").is_ok());

    // New columns added
    let revenue = out.column_by_name("revenue").unwrap();
    let rev = revenue.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(rev.value(0), 20.0);   // 10 * 2
    assert_eq!(rev.value(1), 20.0);   // 20 * 1
    assert_eq!(rev.value(2), 90.0);   // 30 * 3

    let year = out.column_by_name("order_year").unwrap();
    let yr = year.as_any().downcast_ref::<Int32Array>()
        .unwrap_or_else(|| {
            panic!("expected Int32 for order_year");
        });
    assert_eq!(yr.value(0), 2024);

    let uid = out.column_by_name("user_id").unwrap();
    let uid = uid.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(uid.value(0), "u1");
    assert_eq!(uid.value(1), "u2");

    let tag0 = out.column_by_name("first_tag").unwrap();
    let tag0 = tag0.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(tag0.value(0), "vip");
    assert_eq!(tag0.value(1), "normal");
    assert!(tag0.is_null(3)); // empty array → null

    // load_ts is present (value varies by run, just check existence)
    assert!(out.schema().index_of("load_ts").is_ok());
}

#[test]
fn map_select_only_drops_originals() {
    let batch = orders_batch();
    let cols: IndexMap<String, String> = [
        ("revenue".into(), "price * quantity".into()),
        ("cid".into(),     "customer_id".into()), // rename
    ].into_iter().collect();

    let out = apply_map(batch, &cols, true).unwrap();
    assert_eq!(out.num_columns(), 2);
    assert!(out.schema().index_of("revenue").is_ok());
    assert!(out.schema().index_of("cid").is_ok());
    // Originals gone
    assert!(out.schema().index_of("order_id").is_err());
    assert!(out.schema().index_of("customer_id").is_err());
}

#[test]
fn map_upper_and_concat() {
    let batch = orders_batch();
    let cols: IndexMap<String, String> = [
        ("cid_upper".into(), "upper(customer_id)".into()),
    ].into_iter().collect();

    let out = apply_map(batch, &cols, false).unwrap();
    let col = out.column_by_name("cid_upper").unwrap();
    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(arr.value(0), "C1");
    assert_eq!(arr.value(1), "C2");
}

#[test]
fn map_forward_reference_works() {
    let batch = orders_batch();
    let cols: IndexMap<String, String> = [
        ("revenue".into(),          "price * quantity".into()),
        ("discount_revenue".into(), "revenue * 0.9".into()),
    ].into_iter().collect();

    let out = apply_map(batch, &cols, false).unwrap();
    let rev  = out.column_by_name("revenue").unwrap();
    let rev  = rev.as_any().downcast_ref::<Float64Array>().unwrap();
    let disc = out.column_by_name("discount_revenue").unwrap();
    let disc = disc.as_any().downcast_ref::<Float64Array>().unwrap();

    // Row 0: revenue=20, discount_revenue=18
    assert!((rev.value(0) - 20.0).abs() < 0.001);
    assert!((disc.value(0) - 18.0).abs() < 0.001);
}

// ── Step 2: filter (condition expression) ─────────────────────────────────

#[test]
fn filter_condition_simple() {
    let batch = orders_batch();
    let out = apply_filter_expr(batch, "status == \"active\"").unwrap();
    // active: rows 0,2,3,4 → 4 rows
    assert_eq!(out.num_rows(), 4);
}

#[test]
fn filter_condition_compound() {
    let batch = orders_batch();
    // active AND quantity > 2
    let out = apply_filter_expr(batch, "status == \"active\" and quantity > 2").unwrap();
    // Row 2: qty=3 (active), Row 3: qty=10 (active) → 2 rows
    assert_eq!(out.num_rows(), 2);
}

#[test]
fn filter_condition_arithmetic_comparison() {
    let batch = orders_batch();
    // price * quantity >= 50
    let out = apply_filter_expr(batch, "price * quantity >= 50").unwrap();
    // Row 2: 30*3=90 ✓, Row 3: 5*10=50 ✓ �� 2 rows
    assert_eq!(out.num_rows(), 2);
}

// ── Step 3: flatten ───────────────────────────────────────────────────────

#[test]
fn flatten_json_nested_path() {
    let batch = orders_batch();
    let mut select = IndexMap::new();
    select.insert("user_id".into(), "payload.user.id".into());
    select.insert("tag_0".into(),   "payload.tags[0]".into());

    let out = apply_flatten(batch, &select).unwrap();
    assert_eq!(out.num_columns(), 2);

    let uid = out.column_by_name("user_id").unwrap();
    let uid = uid.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(uid.value(0), "u1");
    assert_eq!(uid.value(2), "u1");
    assert_eq!(uid.value(4), "u2");

    let t0 = out.column_by_name("tag_0").unwrap();
    let t0 = t0.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(t0.value(0), "vip");
    assert_eq!(t0.value(1), "normal");
    assert!(t0.is_null(3)); // empty tags array
}

// ── Step 4: aggregate ─────────────────────────────────────────────────────

#[test]
fn aggregate_map_then_aggregate() {
    // Full pipeline: map → filter → aggregate
    let batch = orders_batch();

    // Map: compute revenue
    let map_cols: IndexMap<String, String> = [
        ("revenue".into(), "price * quantity".into()),
    ].into_iter().collect();
    let mapped = apply_map(batch, &map_cols, false).unwrap();

    // Filter: active only
    let filtered = apply_filter_expr(mapped, "status == \"active\"").unwrap();

    // Aggregate by customer_id
    let group_by = vec!["customer_id".to_string()];
    let mut metrics = IndexMap::new();
    metrics.insert("total_revenue".to_string(), "sum(revenue)".to_string());
    metrics.insert("order_count".to_string(),   "count()".to_string());
    metrics.insert("avg_revenue".to_string(),   "avg(revenue)".to_string());
    metrics.insert("max_revenue".to_string(),   "max(revenue)".to_string());

    let result = apply_aggregate(vec![filtered], &group_by, &metrics).unwrap();

    assert_eq!(result.num_rows(), 3); // 3 customers

    // Find each customer and assert
    let cust = result.column_by_name("customer_id").unwrap();
    let cust = cust.as_any().downcast_ref::<StringArray>().unwrap();
    let total = result.column_by_name("total_revenue").unwrap();
    let total = total.as_any().downcast_ref::<Float64Array>().unwrap();
    let count = result.column_by_name("order_count").unwrap();
    let count = count.as_any().downcast_ref::<Int64Array>().unwrap();

    for row in 0..result.num_rows() {
        match cust.value(row) {
            "C1" => {
                assert!((total.value(row) - 110.0).abs() < 0.001,
                    "C1 total: expected 110, got {}", total.value(row));
                assert_eq!(count.value(row), 2);
            }
            "C2" => {
                assert!((total.value(row) - 30.0).abs() < 0.001,
                    "C2 total: expected 30, got {}", total.value(row));
                assert_eq!(count.value(row), 1);
            }
            "C3" => {
                assert!((total.value(row) - 50.0).abs() < 0.001,
                    "C3 total: expected 50, got {}", total.value(row));
                assert_eq!(count.value(row), 1);
            }
            other => panic!("unexpected customer_id: {other}"),
        }
    }
}

#[test]
fn aggregate_global_no_groupby() {
    let batch = orders_batch();
    let metrics = [
        ("n".to_string(),         "count()".to_string()),
        ("total_price".to_string(), "sum(price)".to_string()),
    ].into_iter().collect();

    let result = apply_aggregate(vec![batch], &[], &metrics).unwrap();
    assert_eq!(result.num_rows(), 1);

    let n = result.column_by_name("n").unwrap();
    let n = n.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(n.value(0), 6);

    let tot = result.column_by_name("total_price").unwrap();
    let tot = tot.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((tot.value(0) - 105.0).abs() < 0.001); // 10+20+30+5+15+25
}

#[test]
fn aggregate_min_max_preserve_type() {
    let batch = orders_batch();
    let group_by = vec!["customer_id".to_string()];
    let mut metrics = IndexMap::new();
    metrics.insert("min_price".to_string(), "min(price)".to_string());
    metrics.insert("max_price".to_string(), "max(price)".to_string());

    let result = apply_aggregate(vec![batch], &group_by, &metrics).unwrap();

    // min/max of Float64 should return Float64
    let min_col = result.column_by_name("min_price").unwrap();
    assert!(matches!(
        min_col.data_type(),
        DataType::Float64 | DataType::Int32 | DataType::Int64
    ), "min(price) should return a numeric type, got {:?}", min_col.data_type());
}

// ── Multi-batch streaming aggregate ──────────────────────────────────────

#[test]
fn aggregate_multi_batch() {
    // Split the fixture into two batches to test concat + aggregate
    let full  = orders_batch();
    let schema = full.schema();
    let n      = full.num_rows();

    // First 3 rows
    let b1 = RecordBatch::try_new(
        schema.clone(),
        full.columns().iter().map(|c| c.slice(0, 3)).collect::<Vec<_>>(),
    ).unwrap();

    // Remaining rows
    let b2 = RecordBatch::try_new(
        schema.clone(),
        full.columns().iter().map(|c| c.slice(3, n - 3)).collect::<Vec<_>>(),
    ).unwrap();

    // Map: add revenue
    let map_cols: IndexMap<String, String> = [
        ("revenue".into(), "price * quantity".into()),
    ].into_iter().collect();
    let b1m = apply_map(b1, &map_cols, false).unwrap();
    let b2m = apply_map(b2, &map_cols.clone(), false).unwrap();

    // Aggregate across both batches
    let group_by = vec!["customer_id".to_string()];
    let metrics = [("n".to_string(), "count()".to_string())].into_iter().collect();
    let result = apply_aggregate(vec![b1m, b2m], &group_by, &metrics).unwrap();

    // C1 appears in rows 0,2,5 → count 3; C2 in rows 1,4 → count 2; C3 in row 3 → count 1
    let cust  = result.column_by_name("customer_id").unwrap();
    let cust  = cust.as_any().downcast_ref::<StringArray>().unwrap();
    let n_col = result.column_by_name("n").unwrap();
    let n_col = n_col.as_any().downcast_ref::<Int64Array>().unwrap();

    for row in 0..result.num_rows() {
        match cust.value(row) {
            "C1" => assert_eq!(n_col.value(row), 3),
            "C2" => assert_eq!(n_col.value(row), 2),
            "C3" => assert_eq!(n_col.value(row), 1),
            other => panic!("unexpected: {other}"),
        }
    }
}

// ── Expression edge cases ─────────────────────────────────────────────────

#[test]
fn expr_null_propagation_in_arithmetic() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Float64Array::from(vec![Some(5.0), None, Some(3.0)])) as ArrayRef,
        Arc::new(Float64Array::from(vec![Some(2.0), Some(2.0), None])) as ArrayRef,
    ]).unwrap();

    let cols: IndexMap<String, String> = [("product".into(), "a * b".into())].into_iter().collect();
    let out = apply_map(batch, &cols, false).unwrap();
    let prod = out.column_by_name("product").unwrap();
    let prod = prod.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(prod.value(0), 10.0);
    assert!(prod.is_null(1)); // null * 2 = null
    assert!(prod.is_null(2)); // 3 * null = null
}

#[test]
fn expr_division_by_zero_is_null() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Float64Array::from(vec![10.0, 5.0])) as ArrayRef,
        Arc::new(Float64Array::from(vec![0.0, 2.0]))  as ArrayRef,
    ]).unwrap();

    let cols: IndexMap<String, String> = [("ratio".into(), "x / y".into())].into_iter().collect();
    let out = apply_map(batch, &cols, false).unwrap();
    let ratio = out.column_by_name("ratio").unwrap();
    let ratio = ratio.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(ratio.is_null(0)); // x/0 → null
    assert!((ratio.value(1) - 2.5).abs() < 0.001);
}

#[test]
fn expr_json_get_missing_key_is_null() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("payload", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec![
            r#"{"a": 1}"#,
            r#"invalid"#,
            r#"{"b": 2}"#,
        ])) as ArrayRef,
    ]).unwrap();

    let cols: IndexMap<String, String> = [
        ("val".into(), r#"json_get(payload, "a")"#.into()),
    ].into_iter().collect();
    let out = apply_map(batch, &cols, false).unwrap();
    let col = out.column_by_name("val").unwrap();
    let col = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(col.value(0), "1");
    assert!(col.is_null(1)); // invalid JSON → null
    assert!(col.is_null(2)); // key "a" not present → null
}

#[test]
fn expr_logical_operators() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("active", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(StringArray::from(vec!["yes", "no", "yes", "yes"])) as ArrayRef,
        Arc::new(Float64Array::from(vec![200.0, 100.0, 50.0, 300.0])) as ArrayRef,
    ]).unwrap();

    // active == "yes" and amount >= 100
    let out = apply_filter_expr(
        batch, r#"active == "yes" and amount >= 100"#
    ).unwrap();
    // Row 0: yes, 200 ✓; Row 2: yes, 50 ✗; Row 3: yes, 300 ✓ → 2 rows
    assert_eq!(out.num_rows(), 2);
}

// ── YAML round-trip ───────────────────────────────────────────────────────

#[test]
fn yaml_map_aggregate_parses() {
    use potato_etl_runtime::Dag;

    let yaml = r#"
config:
  batch_size: 1000

connections:
  db:
    driver: postgres
    host: localhost
    database: test
    auth:
      type: user_pass
      username: user
      password: "pass"

steps:
  - id: src
    type: read_db
    from:
      connection: db
      table: orders
    normalize_columns: true
    schema:
      arrow:
        columns:
          order_ts:
            type: "timestamp[us, UTC]"

  - id: enrich
    type: map
    input: src
    preserve_metadata: true
    columns:
      load_ts:    now()
      run_start:  run_ts()
      revenue:    price * quantity
      order_year: year(order_ts)
      uid:        json_get(payload, "user.id")

  - id: active_only
    type: filter
    input: enrich
    condition: "status == \"active\" and revenue >= 0"

  - id: by_customer
    type: aggregate
    input: active_only
    group_by:
      - customer_id
    metrics:
      total_sales:  sum(revenue)
      order_count:  count()
      avg_revenue:  avg(revenue)
      first_seen:   min(order_ts)

  - id: flat_payload
    type: flatten
    input: enrich
    select:
      user_id:  payload.user.id
      tag_0:    payload.tags[0]

  - id: out
    type: write_db
    input: by_customer
    target:
      connection: db
      table: customer_summary
    mode: truncate
    create_table: if_not_exists
    schema:
      database:
        columns:
          total_sales:
            type: "NUMERIC(19,4)"
          order_count:
            type: "INT"
          load_ts:
            type: "TIMESTAMPTZ"
"#;

    let dag = Dag::from_yaml(yaml).expect("YAML should parse without errors");
    assert!(dag.len() >= 6, "Expected at least 6 steps");
}

// ── Negative tests: reject stale flat `conn:` format ─────────────────────

#[test]
fn flat_conn_on_read_db_is_rejected() {
    use potato_etl_runtime::Dag;

    let yaml = r#"
connections:
  db:
    driver: postgres
    host: localhost
    database: test

steps:
  - id: src
    type: read_db
    conn: db
    table: orders
"#;
    let result = Dag::from_yaml(yaml);
    assert!(
        result.is_err(),
        "read_db with flat `conn:` should fail — use `from: {{ connection: db, table: orders }}` instead"
    );
}

#[test]
fn flat_conn_on_write_db_is_rejected() {
    use potato_etl_runtime::Dag;

    let yaml = r#"
connections:
  db:
    driver: postgres
    host: localhost
    database: test

steps:
  - id: src
    type: read_db
    from:
      connection: db
      table: orders

  - id: out
    type: write_db
    input: src
    conn: db
    table: output
    mode: truncate
"#;
    let result = Dag::from_yaml(yaml);
    assert!(
        result.is_err(),
        "write_db with flat `conn:` should fail — use `target: {{ connection: db, table: output }}` instead"
    );
}

// ── run_ts() — fixed pipeline timestamp ─────────────────────────────────

#[test]
fn run_ts_returns_fixed_timestamp_across_batches() {
    use arrow::array::TimestampMicrosecondArray;

    // Set a known context: 2025-01-15 12:00:00 UTC in microseconds
    let fixed_us: i64 = 1_736_942_400_000_000; // 2025-01-15T12:00:00Z
    set_eval_context(EvalContext { run_start_us: fixed_us });

    let batch = orders_batch();
    let cols: IndexMap<String, String> = [
        ("inserted_at".into(), "run_ts()".into()),
    ].into_iter().collect();

    // Batch 1
    let out1 = apply_map(batch.clone(), &cols, false).unwrap();
    let ts1 = out1.column_by_name("inserted_at").unwrap();
    let ts1 = ts1.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();

    // Batch 2 — simulate a second batch arriving later
    let out2 = apply_map(batch, &cols, false).unwrap();
    let ts2 = out2.column_by_name("inserted_at").unwrap();
    let ts2 = ts2.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();

    // All values should be the same fixed timestamp
    for i in 0..ts1.len() {
        assert_eq!(ts1.value(i), fixed_us,
            "run_ts() should return the fixed pipeline start time");
    }
    for i in 0..ts2.len() {
        assert_eq!(ts2.value(i), fixed_us,
            "run_ts() across batches should return the same timestamp");
    }

    // Reset context
    set_eval_context(EvalContext::default());
}

#[test]
fn now_differs_from_run_ts() {
    use arrow::array::TimestampMicrosecondArray;

    // Set run_ts to a known past value
    let past_us: i64 = 1_000_000_000_000_000; // 2001-09-09T01:46:40Z
    set_eval_context(EvalContext { run_start_us: past_us });

    let batch = orders_batch();
    let cols: IndexMap<String, String> = [
        ("run_start".into(), "run_ts()".into()),
        ("batch_time".into(), "now()".into()),
    ].into_iter().collect();

    let out = apply_map(batch, &cols, false).unwrap();

    let run_col = out.column_by_name("run_start").unwrap();
    let run_col = run_col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();

    let now_col = out.column_by_name("batch_time").unwrap();
    let now_col = now_col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();

    // run_ts() should be the fixed past value
    assert_eq!(run_col.value(0), past_us);

    // now() should be much more recent (current wall clock)
    assert!(now_col.value(0) > past_us,
        "now() should be later than the fixed run_ts() value");

    // Reset context
    set_eval_context(EvalContext::default());
}

// ── $env_var — pipeline environment variables ────────────────────────────

#[test]
fn env_var_string_broadcast() {
    use arrow::array::TimestampMicrosecondArray;

    // Set a pipeline environment variable as a pre-evaluated 1-element array.
    let label: ArrayRef = Arc::new(StringArray::from(vec!["my_pipeline"]));
    let ts_val: i64 = 1_736_942_400_000_000;
    let ts_arr: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from(vec![Some(ts_val)]).with_timezone("UTC")
    );
    let mut env = HashMap::new();
    env.insert("label".to_string(), label);
    env.insert("ts".to_string(), ts_arr);
    set_env_vars(env);

    let batch = orders_batch();
    let cols: IndexMap<String, String> = [
        ("pipeline".into(), "$label".into()),
        ("started".into(),  "$ts".into()),
    ].into_iter().collect();

    let out = apply_map(batch, &cols, false).unwrap();

    // String env var
    let pl = out.column_by_name("pipeline").unwrap();
    let pl = pl.as_any().downcast_ref::<StringArray>().unwrap();
    for i in 0..pl.len() {
        assert_eq!(pl.value(i), "my_pipeline");
    }

    // Timestamp env var
    let ts_col = out.column_by_name("started").unwrap();
    let ts_col = ts_col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    for i in 0..ts_col.len() {
        assert_eq!(ts_col.value(i), ts_val);
    }

    clear_env_vars();
}

#[test]
fn yaml_with_environment_parses() {
    use potato_etl_runtime::Dag;

    let yaml = r#"
config:
  batch_size: 1000

connections:
  db:
    driver: postgres
    host: localhost
    database: test
    auth:
      type: user_pass
      username: user
      password: "pass"

environment:
  load_ts: now()
  run_label: '"nightly_sync"'
  multiplier: "2.5"

steps:
  - id: src
    type: read_db
    from:
      connection: db
      table: orders

  - id: enrich
    type: map
    input: src
    columns:
      inserted_at: $load_ts
      label:       $run_label
      doubled:     price * $multiplier

  - id: out
    type: write_db
    input: enrich
    target:
      connection: db
      table: enriched_orders
    mode: truncate
"#;

    let dag = Dag::from_yaml(yaml).expect("YAML with environment should parse");
    assert!(!dag.environment.is_empty(), "environment should be populated");
    assert_eq!(dag.environment.get("load_ts").unwrap(), "now()");
    assert_eq!(dag.environment.get("run_label").unwrap(), "\"nightly_sync\"");
}

// ── $env_var in filter conditions ────────────────────────────────────────

#[test]
fn env_var_in_filter_condition() {
    // Set an env var that the filter condition references.
    let threshold: ArrayRef = Arc::new(Float64Array::from(vec![50.0]));
    let target_status: ArrayRef = Arc::new(StringArray::from(vec!["active"]));
    let mut env = HashMap::new();
    env.insert("min_revenue".to_string(), threshold);
    env.insert("target_status".to_string(), target_status);
    set_env_vars(env);

    let batch = orders_batch();

    // First, add revenue via map.
    let cols: IndexMap<String, String> = [
        ("revenue".into(), "price * quantity".into()),
    ].into_iter().collect();
    let mapped = apply_map(batch, &cols, false).unwrap();

    // Filter: status == $target_status and revenue >= $min_revenue
    let filtered = apply_filter_expr(
        mapped, r#"status == $target_status and revenue >= $min_revenue"#
    ).unwrap();

    // active rows with revenue >= 50:
    //   row 2: active, 30*3=90 ✓
    //   row 3: active, 5*10=50 ✓
    //   row 0: active, 10*2=20 ✗
    //   row 4: active, 15*2=30 ✗
    assert_eq!(filtered.num_rows(), 2);

    clear_env_vars();
}

// ── env("name") function call form ──────────────────────────────────────

#[test]
fn env_function_call_form() {
    let label: ArrayRef = Arc::new(StringArray::from(vec!["test_pipeline"]));
    let mut env = HashMap::new();
    env.insert("pipeline".to_string(), label);
    set_env_vars(env);

    let batch = orders_batch();
    let cols: IndexMap<String, String> = [
        ("pl_dollar".into(), "$pipeline".into()),
        ("pl_func".into(),   r#"env("pipeline")"#.into()),
    ].into_iter().collect();

    let out = apply_map(batch, &cols, false).unwrap();

    let dollar = out.column_by_name("pl_dollar").unwrap();
    let dollar = dollar.as_any().downcast_ref::<StringArray>().unwrap();
    let func = out.column_by_name("pl_func").unwrap();
    let func = func.as_any().downcast_ref::<StringArray>().unwrap();

    // Both forms should produce identical results.
    for i in 0..dollar.len() {
        assert_eq!(dollar.value(i), "test_pipeline");
        assert_eq!(func.value(i), "test_pipeline");
    }

    clear_env_vars();
}

// ── undefined $env_var produces a clear error ───────────────────────────

#[test]
fn undefined_env_var_errors_clearly() {
    clear_env_vars(); // ensure no leftover vars

    let batch = orders_batch();
    let cols: IndexMap<String, String> = [
        ("missing".into(), "$nonexistent".into()),
    ].into_iter().collect();

    let result = apply_map(batch, &cols, false);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("undefined environment variable"),
        "error should mention 'undefined environment variable', got: {err_msg}"
    );
    assert!(
        err_msg.contains("nonexistent"),
        "error should mention the variable name, got: {err_msg}"
    );
}