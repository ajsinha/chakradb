//! Scalar expression evaluation with SQL three-valued logic.
//!
//! The whole point of this module is that `NULL` propagates correctly.
//! `1 = NULL` is not `false`, it is `NULL`; `NULL AND false` is `false` but
//! `NULL AND true` is `NULL`. Getting this wrong is the single most common way a
//! query engine returns subtly incorrect answers, so it is tested exhaustively.

use super::value::{batch_value, column_index, row_value, Value};
use crate::batch::Batch;
use crate::schema::Row;
use std::cmp::Ordering;

/// A scalar expression over one row.
#[derive(Debug, Clone)]
pub enum Expr {
    /// A column reference, pre-resolved to its index.
    Column(usize),
    Literal(Value),
    /// Unary `NOT`, `-`.
    Unary(UnaryOp, Box<Expr>),
    Binary(BinaryOp, Box<Expr>, Box<Expr>),
    /// `expr IS NULL` / `IS NOT NULL`. These never return NULL themselves —
    /// that is the whole reason they exist.
    IsNull(Box<Expr>, bool),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

impl Expr {
    /// Resolve a column by name at plan time, so evaluation is index-based.
    pub fn column(name: &str) -> Result<Expr, String> {
        column_index(name)
            .map(Expr::Column)
            .ok_or_else(|| format!("no such column: {name}"))
    }

    /// Record which of the four columns this expression reads.
    pub fn columns_used(&self, used: &mut [bool; 4]) {
        match self {
            Expr::Column(i) => {
                if *i < 4 {
                    used[*i] = true;
                }
            }
            Expr::Literal(_) => {}
            Expr::Unary(_, e) | Expr::IsNull(e, _) => e.columns_used(used),
            Expr::Binary(_, l, r) => {
                l.columns_used(used);
                r.columns_used(used);
            }
        }
    }

    /// True if a part with these per-column `(min, max)` zonemap bounds **cannot**
    /// contain any row matching this predicate — so the part can be skipped
    /// (DuckDB-style zonemap pruning). Conservative: returns `false` whenever it
    /// can't prove exclusion.
    pub fn excludes(&self, bounds: &[Option<(Value, Value)>]) -> bool {
        match self {
            Expr::Binary(BinaryOp::And, l, r) => l.excludes(bounds) || r.excludes(bounds),
            Expr::Binary(BinaryOp::Or, l, r) => l.excludes(bounds) && r.excludes(bounds),
            Expr::Binary(op, l, r)
                if matches!(
                    op,
                    BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
                ) =>
            {
                // Normalise to `Column op Literal`.
                let (col, cmp, lit) = match (l.as_ref(), r.as_ref()) {
                    (Expr::Column(c), Expr::Literal(v)) => (*c, *op, v),
                    (Expr::Literal(v), Expr::Column(c)) => (*c, flip(*op), v),
                    _ => return false,
                };
                match bounds.get(col).and_then(|b| b.as_ref()) {
                    Some((mn, mx)) => range_excludes(cmp, mn, mx, lit),
                    None => false,
                }
            }
            _ => false,
        }
    }

    /// Evaluate against batch row `i`, columnar — no `Row` materialised.
    pub fn eval_at(&self, batch: &Batch, i: usize) -> Value {
        match self {
            Expr::Column(c) => batch_value(batch, *c, i),
            Expr::Literal(v) => v.clone(),
            Expr::IsNull(e, negated) => Value::Bool(e.eval_at(batch, i).is_null() != *negated),
            Expr::Unary(op, e) => eval_unary(*op, e.eval_at(batch, i)),
            Expr::Binary(op, l, r) => match op {
                BinaryOp::And => eval_and(l.eval_at(batch, i), r.eval_at(batch, i)),
                BinaryOp::Or => eval_or(l.eval_at(batch, i), r.eval_at(batch, i)),
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                    eval_arith(*op, l.eval_at(batch, i), r.eval_at(batch, i))
                }
                _ => eval_compare(*op, l.eval_at(batch, i), r.eval_at(batch, i)),
            },
        }
    }

    /// Evaluate against a row.
    pub fn eval(&self, row: &Row) -> Value {
        match self {
            Expr::Column(i) => row_value(row, *i),
            Expr::Literal(v) => v.clone(),
            Expr::IsNull(e, negated) => {
                let is_null = e.eval(row).is_null();
                Value::Bool(is_null != *negated)
            }
            Expr::Unary(op, e) => eval_unary(*op, e.eval(row)),
            Expr::Binary(op, l, r) => self.eval_binary(*op, l, r, row),
        }
    }

    fn eval_binary(&self, op: BinaryOp, l: &Expr, r: &Expr, row: &Row) -> Value {
        // AND/OR have to short-circuit *and* honour three-valued logic, so they
        // cannot go through the generic path below.
        match op {
            BinaryOp::And => return eval_and(l.eval(row), r.eval(row)),
            BinaryOp::Or => return eval_or(l.eval(row), r.eval(row)),
            _ => {}
        }
        let (lv, rv) = (l.eval(row), r.eval(row));
        match op {
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                eval_arith(op, lv, rv)
            }
            _ => eval_compare(op, lv, rv),
        }
    }
}

/// Flip a comparison when its operands are swapped (`lit op col` → `col op' lit`).
fn flip(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other, // Eq is symmetric
    }
}

/// Whether the value range `[mn, mx]` provably contains no value satisfying
/// `col <cmp> lit`.
fn range_excludes(cmp: BinaryOp, mn: &Value, mx: &Value, lit: &Value) -> bool {
    use std::cmp::Ordering::*;
    match cmp {
        BinaryOp::Eq => lit.total_cmp(mn) == Less || lit.total_cmp(mx) == Greater,
        BinaryOp::Lt => mn.total_cmp(lit) != Less, // min >= lit
        BinaryOp::LtEq => mn.total_cmp(lit) == Greater, // min > lit
        BinaryOp::Gt => mx.total_cmp(lit) != Greater, // max <= lit
        BinaryOp::GtEq => mx.total_cmp(lit) == Less, // max < lit
        _ => false,
    }
}

fn eval_unary(op: UnaryOp, v: Value) -> Value {
    match op {
        UnaryOp::Neg => match v {
            // wrapping_neg matches the wrapping integer arithmetic elsewhere and
            // avoids a debug-build panic on i64::MIN / i128::MIN.
            Value::Int(i) => Value::Int(i.wrapping_neg()),
            Value::Float(f) => Value::Float(-f),
            Value::Decimal(m, s) => Value::Decimal(m.wrapping_neg(), s),
            _ => Value::Null,
        },
        UnaryOp::Not => match v {
            Value::Bool(b) => Value::Bool(!b),
            _ => Value::Null, // NOT NULL (the value) is NULL
        },
    }
}

fn eval_arith(op: BinaryOp, l: Value, r: Value) -> Value {
    // Integer arithmetic stays integer; any float makes it float. NULL poisons.
    if l.is_null() || r.is_null() {
        return Value::Null;
    }
    if let (Value::Int(a), Value::Int(b)) = (&l, &r) {
        let (a, b) = (*a, *b);
        return match op {
            BinaryOp::Add => Value::Int(a.wrapping_add(b)),
            BinaryOp::Sub => Value::Int(a.wrapping_sub(b)),
            BinaryOp::Mul => Value::Int(a.wrapping_mul(b)),
            BinaryOp::Div => {
                if b == 0 {
                    Value::Null // division by zero → NULL, as SQLite does
                } else {
                    Value::Int(a / b)
                }
            }
            BinaryOp::Mod => {
                if b == 0 {
                    Value::Null
                } else {
                    Value::Int(a.wrapping_rem(b))
                }
            }
            _ => unreachable!(),
        };
    }
    // Exact decimal arithmetic: when a `Decimal` meets a `Decimal` or an `Int`,
    // add/sub align scales and mul adds them, all in i128 — never through f64.
    // Division and any i128 overflow fall through to the float path below.
    if matches!(l, Value::Decimal(..)) || matches!(r, Value::Decimal(..)) {
        let dec = |v: &Value| match v {
            Value::Int(i) => Some((*i as i128, 0u32)),
            Value::Decimal(m, s) => Some((*m, *s)),
            _ => None,
        };
        if let (Some((am, asc)), Some((bm, bsc))) = (dec(&l), dec(&r)) {
            let exact = match op {
                BinaryOp::Add | BinaryOp::Sub => {
                    let scale = asc.max(bsc);
                    match (
                        crate::value::rescale(am, asc, scale),
                        crate::value::rescale(bm, bsc, scale),
                    ) {
                        (Some(x), Some(y)) => {
                            let m = if op == BinaryOp::Add {
                                x.checked_add(y)
                            } else {
                                x.checked_sub(y)
                            };
                            m.map(|m| Value::Decimal(m, scale))
                        }
                        _ => None,
                    }
                }
                BinaryOp::Mul => am
                    .checked_mul(bm)
                    .map(|m| Value::Decimal(m, asc + bsc)),
                _ => None, // Div / Mod → float path
            };
            if let Some(v) = exact {
                return v;
            }
        }
    }

    let (a, b) = match (l.as_f64(), r.as_f64()) {
        (Some(a), Some(b)) => (a, b),
        _ => return Value::Null,
    };
    match op {
        BinaryOp::Add => Value::Float(a + b),
        BinaryOp::Sub => Value::Float(a - b),
        BinaryOp::Mul => Value::Float(a * b),
        BinaryOp::Div => {
            if b == 0.0 {
                Value::Null
            } else {
                Value::Float(a / b)
            }
        }
        BinaryOp::Mod => {
            if b == 0.0 {
                Value::Null
            } else {
                Value::Float(a % b)
            }
        }
        _ => unreachable!(),
    }
}

fn eval_compare(op: BinaryOp, l: Value, r: Value) -> Value {
    match l.sql_cmp(&r) {
        None => Value::Null, // NULL on either side → unknown
        Some(ord) => Value::Bool(match op {
            BinaryOp::Eq => ord == Ordering::Equal,
            BinaryOp::NotEq => ord != Ordering::Equal,
            BinaryOp::Lt => ord == Ordering::Less,
            BinaryOp::LtEq => ord != Ordering::Greater,
            BinaryOp::Gt => ord == Ordering::Greater,
            BinaryOp::GtEq => ord != Ordering::Less,
            _ => unreachable!(),
        }),
    }
}

/// `AND` truth table including UNKNOWN: `false AND anything = false`, even if
/// the other side is NULL. That is what makes it not a plain boolean AND.
fn eval_and(l: Value, r: Value) -> Value {
    match (&l, &r) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Value::Bool(false),
        (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
        _ => Value::Null,
    }
}

/// `OR`: `true OR anything = true`, even against NULL.
fn eval_or(l: Value, r: Value) -> Value {
    match (&l, &r) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Value::Bool(true),
        (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> Row {
        Row::new(10, 20, 2.5, "hello")
    }

    fn lit_i(i: i64) -> Box<Expr> {
        Box::new(Expr::Literal(Value::Int(i)))
    }
    fn null() -> Box<Expr> {
        Box::new(Expr::Literal(Value::Null))
    }

    #[test]
    fn column_and_literal() {
        assert_eq!(Expr::column("pk").unwrap().eval(&row()).render(), "10");
        assert_eq!(Expr::column("c").unwrap().eval(&row()).render(), "hello");
        assert_eq!(Expr::Literal(Value::Int(7)).eval(&row()).render(), "7");
    }

    #[test]
    fn unknown_column_is_an_error() {
        assert!(Expr::column("nope").is_err());
    }

    #[test]
    fn integer_arithmetic_stays_integer() {
        let e = Expr::Binary(BinaryOp::Add, lit_i(2), lit_i(3));
        assert_eq!(e.eval(&row()).render(), "5");
        assert_eq!(e.eval(&row()).type_char(), 'I');
    }

    #[test]
    fn division_by_zero_is_null() {
        let e = Expr::Binary(BinaryOp::Div, lit_i(1), lit_i(0));
        assert!(e.eval(&row()).is_null());
    }

    // --- Zonemap part pruning (`excludes`) -------------------------------

    /// A part holding column 0 in `[10, 20]` and column 1 in `[100, 200]`.
    fn bounds() -> Vec<Option<(Value, Value)>> {
        vec![
            Some((Value::Int(10), Value::Int(20))),
            Some((Value::Int(100), Value::Int(200))),
        ]
    }
    fn col(i: usize) -> Box<Expr> {
        Box::new(Expr::Column(i))
    }
    fn cmp(op: BinaryOp, c: usize, v: i64) -> Expr {
        Expr::Binary(op, col(c), lit_i(v))
    }

    #[test]
    fn excludes_prunes_when_range_cannot_match() {
        // col0 = 5 → 5 < min(10): prunable.
        assert!(cmp(BinaryOp::Eq, 0, 5).excludes(&bounds()));
        // col0 = 25 → 25 > max(20): prunable.
        assert!(cmp(BinaryOp::Eq, 0, 25).excludes(&bounds()));
        // col0 < 10 → all values >= 10: prunable.
        assert!(cmp(BinaryOp::Lt, 0, 10).excludes(&bounds()));
        // col0 > 20 → all values <= 20: prunable.
        assert!(cmp(BinaryOp::Gt, 0, 20).excludes(&bounds()));
        // col0 >= 21 / <= 9: prunable.
        assert!(cmp(BinaryOp::GtEq, 0, 21).excludes(&bounds()));
        assert!(cmp(BinaryOp::LtEq, 0, 9).excludes(&bounds()));
    }

    #[test]
    fn excludes_keeps_part_that_might_match() {
        // Any of these overlaps [10, 20], so the part must be scanned.
        assert!(!cmp(BinaryOp::Eq, 0, 15).excludes(&bounds()));
        assert!(!cmp(BinaryOp::Lt, 0, 15).excludes(&bounds()));
        assert!(!cmp(BinaryOp::Gt, 0, 15).excludes(&bounds()));
        assert!(!cmp(BinaryOp::Eq, 0, 10).excludes(&bounds())); // boundary
        assert!(!cmp(BinaryOp::Eq, 0, 20).excludes(&bounds())); // boundary
    }

    #[test]
    fn excludes_handles_literal_on_the_left() {
        // `5 = col0` normalises to `col0 = 5` → prunable; `15 = col0` is not.
        let swapped = Expr::Binary(BinaryOp::Eq, lit_i(5), col(0));
        assert!(swapped.excludes(&bounds()));
        let keep = Expr::Binary(BinaryOp::Gt, lit_i(30), col(0)); // 30 > col0 == col0 < 30
        assert!(!keep.excludes(&bounds()));
        let prune = Expr::Binary(BinaryOp::Lt, lit_i(30), col(0)); // 30 < col0 == col0 > 30
        assert!(prune.excludes(&bounds()));
    }

    #[test]
    fn excludes_combines_and_or() {
        // AND excludes if *either* side excludes.
        let and = Expr::Binary(
            BinaryOp::And,
            Box::new(cmp(BinaryOp::Eq, 0, 15)), // keep
            Box::new(cmp(BinaryOp::Eq, 1, 999)), // prune (999 > 200)
        );
        assert!(and.excludes(&bounds()));
        // OR excludes only if *both* sides exclude.
        let or = Expr::Binary(
            BinaryOp::Or,
            Box::new(cmp(BinaryOp::Eq, 0, 5)), // prune
            Box::new(cmp(BinaryOp::Eq, 1, 150)), // keep
        );
        assert!(!or.excludes(&bounds()));
        let or_both = Expr::Binary(
            BinaryOp::Or,
            Box::new(cmp(BinaryOp::Eq, 0, 5)), // prune
            Box::new(cmp(BinaryOp::Eq, 1, 999)), // prune
        );
        assert!(or_both.excludes(&bounds()));
    }

    #[test]
    fn excludes_is_conservative_without_bounds() {
        // No bounds recorded for a column, or a non-comparison predicate → keep.
        let no_bounds = vec![None, None];
        assert!(!cmp(BinaryOp::Eq, 0, 5).excludes(&no_bounds));
        assert!(!cmp(BinaryOp::Eq, 9, 5).excludes(&bounds())); // out-of-range column
    }

    #[test]
    fn arithmetic_with_null_is_null() {
        let e = Expr::Binary(BinaryOp::Add, lit_i(1), null());
        assert!(e.eval(&row()).is_null());
    }

    #[test]
    fn comparison_with_null_is_null_not_false() {
        // The crux of three-valued logic.
        let e = Expr::Binary(BinaryOp::Eq, lit_i(1), null());
        assert!(e.eval(&row()).is_null(), "1 = NULL must be NULL");
        let ne = Expr::Binary(BinaryOp::NotEq, null(), lit_i(1));
        assert!(ne.eval(&row()).is_null(), "NULL <> 1 must be NULL");
    }

    #[test]
    fn comparison_operators() {
        assert!(Expr::Binary(BinaryOp::Lt, lit_i(1), lit_i(2))
            .eval(&row())
            .is_true());
        assert!(Expr::Binary(BinaryOp::GtEq, lit_i(2), lit_i(2))
            .eval(&row())
            .is_true());
        assert!(!Expr::Binary(BinaryOp::Gt, lit_i(1), lit_i(2))
            .eval(&row())
            .is_true());
    }

    #[test]
    fn is_null_never_returns_null() {
        assert!(Expr::IsNull(null(), false).eval(&row()).is_true());
        assert!(!Expr::IsNull(lit_i(1), false).eval(&row()).is_true());
        assert!(
            Expr::IsNull(lit_i(1), true).eval(&row()).is_true(),
            "IS NOT NULL"
        );
    }

    #[test]
    fn and_truth_table() {
        let t = || Box::new(Expr::Literal(Value::Bool(true)));
        let f = || Box::new(Expr::Literal(Value::Bool(false)));
        assert!(Expr::Binary(BinaryOp::And, t(), t()).eval(&row()).is_true());
        assert!(!Expr::Binary(BinaryOp::And, t(), f()).eval(&row()).is_true());
        // false AND NULL = false (short-circuit through UNKNOWN)
        assert!(!Expr::Binary(BinaryOp::And, f(), null())
            .eval(&row())
            .is_true());
        assert_eq!(
            Expr::Binary(BinaryOp::And, f(), null())
                .eval(&row())
                .render(),
            "0"
        );
        // true AND NULL = NULL
        assert!(Expr::Binary(BinaryOp::And, t(), null())
            .eval(&row())
            .is_null());
    }

    #[test]
    fn or_truth_table() {
        let t = || Box::new(Expr::Literal(Value::Bool(true)));
        let f = || Box::new(Expr::Literal(Value::Bool(false)));
        assert!(Expr::Binary(BinaryOp::Or, f(), t()).eval(&row()).is_true());
        assert!(!Expr::Binary(BinaryOp::Or, f(), f()).eval(&row()).is_true());
        // true OR NULL = true
        assert!(Expr::Binary(BinaryOp::Or, t(), null())
            .eval(&row())
            .is_true());
        // false OR NULL = NULL
        assert!(Expr::Binary(BinaryOp::Or, f(), null())
            .eval(&row())
            .is_null());
    }

    #[test]
    fn negation() {
        assert_eq!(
            Expr::Unary(UnaryOp::Neg, lit_i(5)).eval(&row()).render(),
            "-5"
        );
        let not_t = Expr::Unary(UnaryOp::Not, Box::new(Expr::Literal(Value::Bool(true))));
        assert!(!not_t.eval(&row()).is_true());
        // NOT NULL (value) is NULL
        assert!(Expr::Unary(UnaryOp::Not, null()).eval(&row()).is_null());
    }

    #[test]
    fn nested_expression() {
        // (pk + a) > 25  →  (10 + 20) > 25  →  true
        let sum = Expr::Binary(
            BinaryOp::Add,
            Box::new(Expr::Column(0)),
            Box::new(Expr::Column(1)),
        );
        let e = Expr::Binary(BinaryOp::Gt, Box::new(sum), lit_i(25));
        assert!(e.eval(&row()).is_true());
    }

    #[test]
    fn text_comparison() {
        let e = Expr::Binary(
            BinaryOp::Eq,
            Box::new(Expr::Column(3)),
            Box::new(Expr::Literal(Value::Text("hello".into()))),
        );
        assert!(e.eval(&row()).is_true());
    }
}
