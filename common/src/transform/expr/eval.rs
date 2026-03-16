//! Arrow evaluator — turns an `Expr` AST into Arrow arrays.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array,
    StringArray, TimestampMicrosecondArray,
};
use arrow::compute::{cast, filter_record_batch};
use arrow::compute::kernels::zip::zip as arrow_zip;
use arrow::record_batch::RecordBatch;
use chrono::Utc;

use crate::util::schema::parse_arrow_type;
use super::ast::{Expr, BinOp, UnaryOp, parse};
use super::{current_eval_context, lookup_env_var};
use super::helpers::*;

// ── Public evaluator API ──────────────────────────────────────────────────────

/// Evaluate `expr` against `batch`, returning an `ArrayRef` of `batch.num_rows()` elements.
///
/// The returned array's `DataType` depends on the expression:
/// - Arithmetic → `Float64`
/// - Comparison / logical → `Boolean`
/// - String functions → `Utf8`
/// - Temporal functions → `Int32`
/// - `now()` → `Timestamp(Microsecond, Some("UTC"))`
/// - Column references → same type as the column in the batch
pub fn eval(expr: &Expr, batch: &RecordBatch) -> anyhow::Result<ArrayRef> {
    let n = batch.num_rows();
    match expr {
        // ── Literals ─────────────────────────────────────────────────────────
        Expr::Int(v) => Ok(Arc::new(broadcast_i64(*v, n))),
        Expr::Float(v) => Ok(Arc::new(broadcast_f64(*v, n))),
        Expr::Str(s) => Ok(Arc::new(broadcast_str(s, n))),
        Expr::Bool(b) => Ok(Arc::new(broadcast_bool(*b, n))),
        Expr::Null => {
            let nulls: StringArray = (0..n).map(|_| Option::<&str>::None).collect();
            Ok(Arc::new(nulls))
        }

        // ── Column reference ────────────────────────────────────────────────
        Expr::Column(col) => {
            batch.column_by_name(col)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("column '{col}' not found in batch (available: {})",
                    batch.schema().fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>().join(", ")
                ))
        }

        // ── Binary operators ──────────────────────────────────────────────────
        Expr::BinOp { op, left, right } => {
            let l = eval(left, batch)?;
            let r = eval(right, batch)?;
            eval_binop(*op, l, r, n)
        }

        // ── Unary operators ───────────────────────────────────────────────────
        Expr::Unary { op, operand } => {
            let a = eval(operand, batch)?;
            match op {
                UnaryOp::Neg => {
                    let f = to_float64(&a)?;
                    let neg: Float64Array = f.iter()
                        .map(|v| v.map(|x| -x))
                        .collect();
                    Ok(Arc::new(neg))
                }
                UnaryOp::Not => {
                    let b = to_boolean(&a)?;
                    let result: BooleanArray = b.iter()
                        .map(|v| v.map(|x| !x))
                        .collect();
                    Ok(Arc::new(result))
                }
            }
        }

        // ── Array index: expr[i] ──────────────────────────────────────────────
        Expr::Index { expr: base_expr, index } => {
            let base = eval(base_expr, batch)?;
            let idx  = eval(index, batch)?;
            let str_arr = to_string_array(&base)?;
            let idx_arr = to_int64(&idx)?;
            let result: StringArray = str_arr.iter().zip(idx_arr.iter())
                .map(|(v, i)| {
                    let s = v?;
                    let idx = i? as usize;
                    let val: serde_json::Value = serde_json::from_str(s).ok()?;
                    let elem = val.get(idx)?;
                    Some(elem.as_str().map(|s| s.to_string())
                        .unwrap_or_else(|| elem.to_string()))
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ── Function calls ────────────────────────────────────────────────────
        Expr::Call { func, args } => eval_call(func, args, batch, n),

        // ── Environment variable reference ────────────────────────────────────
        Expr::EnvVar(name) => {
            if let Some(arr) = lookup_env_var(name) {
                return broadcast_array(&arr, n);
            }
            anyhow::bail!(
                "undefined environment variable '${name}'. \
                 Define it in the top-level `environment:` block of your pipeline config."
            )
        }
    }
}

/// Evaluate `expr` as a boolean mask for row filtering.
pub fn eval_bool_mask(expr: &Expr, batch: &RecordBatch) -> anyhow::Result<BooleanArray> {
    let arr = eval(expr, batch)?;
    to_boolean(&arr)
}

/// Apply a condition expression string to filter a batch.
pub fn apply_condition(batch: RecordBatch, condition: &str) -> anyhow::Result<RecordBatch> {
    let expr = parse(condition)?;
    let mask = eval_bool_mask(&expr, &batch)?;
    Ok(filter_record_batch(&batch, &mask)?)
}

// ── Function call evaluator ───────────────────────────────────────────────────

fn eval_call(func: &str, args: &[Expr], batch: &RecordBatch, n: usize) -> anyhow::Result<ArrayRef> {
    match func {
        // ── Temporal ─────────────────────────────────────────────────────────
        "now" => {
            anyhow::ensure!(args.is_empty(), "now() takes no arguments");
            let ts = Utc::now().timestamp_micros();
            let arr: TimestampMicrosecondArray = (0..n).map(|_| Some(ts)).collect();
            Ok(Arc::new(arr.with_timezone("UTC")) as ArrayRef)
        }
        "now_naive" => {
            anyhow::ensure!(args.is_empty(), "now_naive() takes no arguments");
            let ts = Utc::now().timestamp_micros();
            let arr: TimestampMicrosecondArray = (0..n).map(|_| Some(ts)).collect();
            Ok(Arc::new(arr) as ArrayRef)
        }
        "run_ts" => {
            anyhow::ensure!(args.is_empty(), "run_ts() takes no arguments");
            let ts = current_eval_context().run_start_us;
            let arr: TimestampMicrosecondArray = (0..n).map(|_| Some(ts)).collect();
            Ok(Arc::new(arr.with_timezone("UTC")) as ArrayRef)
        }
        "run_ts_naive" => {
            anyhow::ensure!(args.is_empty(), "run_ts_naive() takes no arguments");
            let ts = current_eval_context().run_start_us;
            let arr: TimestampMicrosecondArray = (0..n).map(|_| Some(ts)).collect();
            Ok(Arc::new(arr) as ArrayRef)
        }
        // ── Environment variable (function form) ─────────────────────────────
        "env" => {
            anyhow::ensure!(args.len() == 1, "env() takes exactly 1 argument (variable name)");
            let name = match &args[0] {
                Expr::Str(s) => s.clone(),
                other => anyhow::bail!("env() argument must be a string literal, got {other:?}"),
            };
            if let Some(arr) = lookup_env_var(&name) {
                return broadcast_array(&arr, n);
            }
            anyhow::bail!(
                "undefined environment variable '{name}'. \
                 Define it in the top-level `environment:` block of your pipeline config."
            )
        }
        "year"   => temporal_part(args, batch, TemporalPart::Year),
        "month"  => temporal_part(args, batch, TemporalPart::Month),
        "day"    => temporal_part(args, batch, TemporalPart::Day),
        "hour"   => temporal_part(args, batch, TemporalPart::Hour),
        "minute" => temporal_part(args, batch, TemporalPart::Minute),
        "second" => temporal_part(args, batch, TemporalPart::Second),

        // ── String ────────────────────────────────────────────────────────────
        "upper" | "ucase" => {
            anyhow::ensure!(args.len() == 1, "upper() takes 1 argument");
            let a = eval(&args[0], batch)?;
            let s = to_string_array(&a)?;
            let result: StringArray = s.iter()
                .map(|v| v.map(|x| x.to_uppercase()))
                .collect();
            Ok(Arc::new(result))
        }
        "lower" | "lcase" => {
            anyhow::ensure!(args.len() == 1, "lower() takes 1 argument");
            let a = eval(&args[0], batch)?;
            let s = to_string_array(&a)?;
            let result: StringArray = s.iter()
                .map(|v| v.map(|x| x.to_lowercase()))
                .collect();
            Ok(Arc::new(result))
        }
        "trim" => {
            anyhow::ensure!(args.len() == 1, "trim() takes 1 argument");
            let a = eval(&args[0], batch)?;
            let s = to_string_array(&a)?;
            let result: StringArray = s.iter()
                .map(|v| v.map(|x| x.trim().to_string()))
                .collect();
            Ok(Arc::new(result))
        }
        "length" | "len" | "char_length" => {
            anyhow::ensure!(args.len() == 1, "{func}() takes 1 argument");
            let a = eval(&args[0], batch)?;
            let s = to_string_array(&a)?;
            let result: Int32Array = s.iter()
                .map(|v| v.map(|x| x.chars().count() as i32))
                .collect();
            Ok(Arc::new(result))
        }
        "concat" => {
            anyhow::ensure!(!args.is_empty(), "concat() requires at least 1 argument");
            let parts: Vec<ArrayRef> = args.iter()
                .map(|a| eval(a, batch))
                .collect::<anyhow::Result<_>>()?;
            let strs: Vec<Vec<Option<String>>> = parts.iter()
                .map(|a| {
                    let s = to_string_array(a)?;
                    Ok(s.iter().map(|v| v.map(|x| x.to_string())).collect::<Vec<_>>())
                })
                .collect::<anyhow::Result<_>>()?;
            let result: StringArray = (0..n).map(|i| {
                let mut out = String::new();
                for part in &strs {
                    if let Some(s) = &part[i] { out.push_str(s); }
                }
                Some(out)
            }).collect();
            Ok(Arc::new(result))
        }
        "coalesce" => {
            anyhow::ensure!(!args.is_empty(), "coalesce() requires at least 1 argument");
            let arrs: Vec<ArrayRef> = args.iter()
                .map(|a| eval(a, batch))
                .collect::<anyhow::Result<_>>()?;

            let first_type = arrs[0].data_type().clone();
            let all_same = arrs.iter().all(|a| a.data_type() == &first_type);

            if all_same {
                let mut result = arrs.last().unwrap().clone();
                for arr in arrs[..arrs.len() - 1].iter().rev() {
                    let mask: BooleanArray = (0..n)
                        .map(|i| Some(arr.is_valid(i)))
                        .collect();
                    result = arrow_zip(&mask, arr, &result)?;
                }
                Ok(result)
            } else {
                let strs: Vec<Vec<Option<String>>> = arrs.iter()
                    .map(|a| {
                        let s = to_string_array(a)?;
                        Ok(s.iter().map(|v| v.map(|x| x.to_string())).collect::<Vec<_>>())
                    })
                    .collect::<anyhow::Result<_>>()?;
                let result: StringArray = (0..n).map(|i| {
                    strs.iter().find_map(|part| part[i].clone())
                }).collect();
                Ok(Arc::new(result))
            }
        }

        // ── JSON ──────────────────────────────────────────────────────────────
        "json_get" => {
            anyhow::ensure!(args.len() == 2, "json_get(col, \"key\") takes 2 arguments");
            let arr = eval(&args[0], batch)?;
            let key_arr = eval(&args[1], batch)?;
            let col_strs  = to_string_array(&arr)?;
            let key_strs  = to_string_array(&key_arr)?;
            let result: StringArray = col_strs.iter().zip(key_strs.iter())
                .map(|(json_val, key_val)| {
                    let json_str = json_val?;
                    let key = key_val?;
                    let val: serde_json::Value = serde_json::from_str(json_str).ok()?;
                    json_path_get(&val, key)
                })
                .collect();
            Ok(Arc::new(result))
        }
        "json_length" | "json_array_length" => {
            anyhow::ensure!(args.len() == 1, "{func}(col) takes 1 argument");
            let arr = eval(&args[0], batch)?;
            let strs = to_string_array(&arr)?;
            let result: Int32Array = strs.iter()
                .map(|v| {
                    v.and_then(|s| {
                        let val: serde_json::Value = serde_json::from_str(s).ok()?;
                        Some(val.as_array()?.len() as i32)
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ── Type casting ──────────────────────────────────────────────────────
        "cast" => {
            anyhow::ensure!(args.len() == 2, "cast(col, \"type_str\") takes 2 arguments");
            let arr = eval(&args[0], batch)?;
            let type_str = match &args[1] {
                Expr::Str(s) => s.clone(),
                other => {
                    let a = eval(other, batch)?;
                    let s = to_string_array(&a)?;
                    s.value(0).to_string()
                }
            };
            let target = parse_arrow_type(&type_str)?;
            Ok(cast(&arr, &target)?)
        }

        // ── Null helpers ──────────────────────────────────────────────────────
        "is_null" | "isnull" => {
            anyhow::ensure!(args.len() == 1, "{func}() takes 1 argument");
            let a = eval(&args[0], batch)?;
            let result: BooleanArray = (0..n).map(|i| Some(a.is_null(i))).collect();
            Ok(Arc::new(result))
        }
        "is_not_null" | "isnotnull" | "not_null" => {
            anyhow::ensure!(args.len() == 1, "{func}() takes 1 argument");
            let a = eval(&args[0], batch)?;
            let result: BooleanArray = (0..n).map(|i| Some(a.is_valid(i))).collect();
            Ok(Arc::new(result))
        }
        "if_null" | "ifnull" | "nvl" => {
            anyhow::ensure!(args.len() == 2, "{func}(col, default) takes 2 arguments");
            let a   = eval(&args[0], batch)?;
            let def = eval(&args[1], batch)?;

            if a.data_type() == def.data_type() {
                let mask: BooleanArray = (0..n)
                    .map(|i| Some(a.is_valid(i)))
                    .collect();
                Ok(arrow_zip(&mask, &a, &def)?)
            } else {
                let a_strs   = to_string_array(&a)?;
                let def_strs = to_string_array(&def)?;
                let result: StringArray = a_strs.iter().zip(def_strs.iter())
                    .map(|(v, d)| if v.is_some() { v.map(|s| s.to_string()) } else { d.map(|s| s.to_string()) })
                    .collect();
                Ok(Arc::new(result))
            }
        }

        // ── Aggregate stubs ───────────────────────────────────────────────────
        "sum" | "count" | "min" | "max" | "avg" | "mean" | "first" => {
            anyhow::bail!(
                "'{func}()' is an aggregate function and can only be used in an `aggregate` step's \
                 `metrics:` block, not in `map` expressions"
            )
        }

        // ── Epoch → Timestamp conversion ──────────────────────────────────────
        "epoch_to_timestamp" | "from_epoch" | "from_unix" => {
            anyhow::ensure!(
                args.len() == 1 || args.len() == 2,
                "{func}() takes 1 or 2 arguments: {func}(col) or {func}(col, \"s\"|\"ms\"|\"us\"|\"ns\")"
            );
            let arr = eval(&args[0], batch)?;
            let unit = if args.len() == 2 {
                match &args[1] {
                    Expr::Str(s) => s.clone(),
                    other => anyhow::bail!(
                        "{func}() second argument must be a string literal (\"s\", \"ms\", \"us\", \"ns\"), got {other:?}"
                    ),
                }
            } else {
                "s".to_string()
            };
            let i64_arr = to_int64(&arr)?;
            let us_arr: TimestampMicrosecondArray = match unit.as_str() {
                "s"  => i64_arr.iter().map(|v| v.map(|x| x * 1_000_000)).collect(),
                "ms" => i64_arr.iter().map(|v| v.map(|x| x * 1_000)).collect(),
                "us" => i64_arr.iter().map(|v| v.map(|x| x)).collect(),
                "ns" => i64_arr.iter().map(|v| v.map(|x| x / 1_000)).collect(),
                other => anyhow::bail!(
                    "{func}() unit must be \"s\", \"ms\", \"us\", or \"ns\", got \"{other}\""
                ),
            };
            Ok(Arc::new(us_arr.with_timezone("UTC")) as ArrayRef)
        }

        other => anyhow::bail!("unknown function '{other}()'"),
    }
}

// ── Binary operator evaluator ─────────────────────────────────────────────────

fn eval_binop(op: BinOp, l: ArrayRef, r: ArrayRef, n: usize) -> anyhow::Result<ArrayRef> {
    match op {
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div => {
            let lf = to_float64(&l)?;
            let rf = to_float64(&r)?;
            let result: Float64Array = match op {
                BinOp::Add => lf.iter().zip(rf.iter())
                    .map(|(a, b)| match (a, b) { (Some(x), Some(y)) => Some(x + y), _ => None })
                    .collect(),
                BinOp::Sub => lf.iter().zip(rf.iter())
                    .map(|(a, b)| match (a, b) { (Some(x), Some(y)) => Some(x - y), _ => None })
                    .collect(),
                BinOp::Mul => lf.iter().zip(rf.iter())
                    .map(|(a, b)| match (a, b) { (Some(x), Some(y)) => Some(x * y), _ => None })
                    .collect(),
                BinOp::Div => lf.iter().zip(rf.iter())
                    .map(|(a, b)| match (a, b) {
                        (Some(x), Some(y)) => if y == 0.0 { None } else { Some(x / y) },
                        _ => None,
                    })
                    .collect(),
                _ => unreachable!(),
            };
            Ok(Arc::new(result))
        }

        BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
            eval_comparison(op, &l, &r, n)
        }

        BinOp::And => {
            let lb = to_boolean(&l)?;
            let rb = to_boolean(&r)?;
            let result: BooleanArray = lb.iter().zip(rb.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(x), Some(y))                  => Some(x && y),
                    _                                   => None,
                })
                .collect();
            Ok(Arc::new(result))
        }
        BinOp::Or => {
            let lb = to_boolean(&l)?;
            let rb = to_boolean(&r)?;
            let result: BooleanArray = lb.iter().zip(rb.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(x), Some(y))                => Some(x || y),
                    _                                 => None,
                })
                .collect();
            Ok(Arc::new(result))
        }
    }
}

fn eval_comparison(op: BinOp, l: &ArrayRef, r: &ArrayRef, _n: usize) -> anyhow::Result<ArrayRef> {
    let numeric = (|| -> anyhow::Result<ArrayRef> {
        let lf = to_float64(l)?;
        let rf = to_float64(r)?;
        let result: BooleanArray = lf.iter().zip(rf.iter())
            .map(|(a, b)| match (a, b) {
                (Some(x), Some(y)) => Some(match op {
                    BinOp::Eq => x == y, BinOp::Ne => x != y,
                    BinOp::Lt => x < y,  BinOp::Le => x <= y,
                    BinOp::Gt => x > y,  BinOp::Ge => x >= y,
                    _ => unreachable!(),
                }),
                _ => Some(false),
            })
            .collect();
        Ok(Arc::new(result))
    })();

    if let Ok(arr) = numeric {
        return Ok(arr);
    }

    let ls = to_string_array(l)?;
    let rs = to_string_array(r)?;
    let result: BooleanArray = ls.iter().zip(rs.iter())
        .map(|(a, b)| match (a, b) {
            (Some(x), Some(y)) => Some(match op {
                BinOp::Eq => x == y, BinOp::Ne => x != y,
                BinOp::Lt => x < y,  BinOp::Le => x <= y,
                BinOp::Gt => x > y,  BinOp::Ge => x >= y,
                _ => unreachable!(),
            }),
            _ => Some(false),
        })
        .collect();
    Ok(Arc::new(result))
}