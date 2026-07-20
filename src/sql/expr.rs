//! Scalar expression evaluation with SQL three-valued logic.
//!
//! The whole point of this module is that `NULL` propagates correctly.
//! `1 = NULL` is not `false`, it is `NULL`; `NULL AND false` is `false` but
//! `NULL AND true` is `NULL`. Getting this wrong is the single most common way a
//! query engine returns subtly incorrect answers, so it is tested exhaustively.

use super::value::{column_index, row_value, Value};
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

fn eval_unary(op: UnaryOp, v: Value) -> Value {
    match op {
        UnaryOp::Neg => match v {
            Value::Int(i) => Value::Int(-i),
            Value::Float(f) => Value::Float(-f),
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
        assert!(Expr::Binary(BinaryOp::Lt, lit_i(1), lit_i(2)).eval(&row()).is_true());
        assert!(Expr::Binary(BinaryOp::GtEq, lit_i(2), lit_i(2)).eval(&row()).is_true());
        assert!(!Expr::Binary(BinaryOp::Gt, lit_i(1), lit_i(2)).eval(&row()).is_true());
    }

    #[test]
    fn is_null_never_returns_null() {
        assert!(Expr::IsNull(null(), false).eval(&row()).is_true());
        assert!(!Expr::IsNull(lit_i(1), false).eval(&row()).is_true());
        assert!(Expr::IsNull(lit_i(1), true).eval(&row()).is_true(), "IS NOT NULL");
    }

    #[test]
    fn and_truth_table() {
        let t = || Box::new(Expr::Literal(Value::Bool(true)));
        let f = || Box::new(Expr::Literal(Value::Bool(false)));
        assert!(Expr::Binary(BinaryOp::And, t(), t()).eval(&row()).is_true());
        assert!(!Expr::Binary(BinaryOp::And, t(), f()).eval(&row()).is_true());
        // false AND NULL = false (short-circuit through UNKNOWN)
        assert!(!Expr::Binary(BinaryOp::And, f(), null()).eval(&row()).is_true());
        assert_eq!(
            Expr::Binary(BinaryOp::And, f(), null()).eval(&row()).render(),
            "0"
        );
        // true AND NULL = NULL
        assert!(Expr::Binary(BinaryOp::And, t(), null()).eval(&row()).is_null());
    }

    #[test]
    fn or_truth_table() {
        let t = || Box::new(Expr::Literal(Value::Bool(true)));
        let f = || Box::new(Expr::Literal(Value::Bool(false)));
        assert!(Expr::Binary(BinaryOp::Or, f(), t()).eval(&row()).is_true());
        assert!(!Expr::Binary(BinaryOp::Or, f(), f()).eval(&row()).is_true());
        // true OR NULL = true
        assert!(Expr::Binary(BinaryOp::Or, t(), null()).eval(&row()).is_true());
        // false OR NULL = NULL
        assert!(Expr::Binary(BinaryOp::Or, f(), null()).eval(&row()).is_null());
    }

    #[test]
    fn negation() {
        assert_eq!(Expr::Unary(UnaryOp::Neg, lit_i(5)).eval(&row()).render(), "-5");
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
