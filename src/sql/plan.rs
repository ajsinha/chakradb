//! Logical plans and the translation from `sqlparser`'s AST.
//!
//! Per `requirements.md` §9, ChakraDB parses with `sqlparser` under
//! **`PostgreSqlDialect`** — never `GenericDialect`, which is a permissive union
//! that accepts SQL no real database accepts and can parse valid SQL into a
//! *different* tree. Compatibility is defined as a documented subset with a
//! conformance suite, not as a claim.
//!
//! This module walks the AST into a small plan the executor understands. Where
//! it meets a construct outside the subset it returns a clear error rather than
//! guessing — an honest "unsupported" beats a wrong answer.

use super::expr::{BinaryOp, Expr, UnaryOp};
use super::value::{column_index, Value, COLUMNS};
use crate::schema::Row;
use sqlparser::ast as sa;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// An aggregate function over a column (or `*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// A projected output column.
#[derive(Debug, Clone)]
pub enum Projection {
    /// A scalar expression, with its output label.
    Expr(Expr, String),
    /// An aggregate over a column index (`None` = `COUNT(*)`).
    Agg(AggFn, Option<usize>, String),
}

/// A sort key.
#[derive(Debug, Clone)]
pub struct OrderKey {
    pub expr: Expr,
    pub ascending: bool,
}

/// The plans the executor can run. Deliberately small.
#[derive(Debug, Clone)]
pub enum Plan {
    CreateTable {
        name: String,
    },
    Insert {
        table: String,
        rows: Vec<Row>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    Update {
        table: String,
        /// `(column index, expression)` assignments.
        sets: Vec<(usize, Expr)>,
        filter: Option<Expr>,
    },
    Select {
        table: String,
        projections: Vec<Projection>,
        filter: Option<Expr>,
        /// Present iff the query aggregates. Empty vec means a single group.
        group_by: Vec<usize>,
        order_by: Vec<OrderKey>,
        limit: Option<usize>,
        distinct: bool,
    },
}

/// Parse and plan a single SQL statement.
pub fn plan(sql: &str) -> Result<Plan, String> {
    let dialect = PostgreSqlDialect {};
    let mut stmts =
        Parser::parse_sql(&dialect, sql).map_err(|e| format!("parse error: {e}"))?;
    if stmts.len() != 1 {
        return Err(format!("expected one statement, got {}", stmts.len()));
    }
    plan_statement(stmts.pop().unwrap())
}

fn plan_statement(stmt: sa::Statement) -> Result<Plan, String> {
    match stmt {
        sa::Statement::CreateTable(ct) => Ok(Plan::CreateTable {
            name: object_name(&ct.name),
        }),
        sa::Statement::Insert(ins) => plan_insert(ins),
        sa::Statement::Delete(del) => plan_delete(del),
        sa::Statement::Update(u) => plan_update(u.table, u.assignments, u.selection),
        sa::Statement::Query(q) => plan_query(*q),
        other => Err(format!("unsupported statement: {other:?}")),
    }
}

fn object_name(n: &sa::ObjectName) -> String {
    n.0.iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(".")
        .trim_matches('"')
        .to_string()
}

fn table_from_factor(tf: &sa::TableFactor) -> Result<String, String> {
    match tf {
        sa::TableFactor::Table { name, .. } => Ok(object_name(name)),
        other => Err(format!("unsupported table expression: {other:?}")),
    }
}

fn plan_insert(ins: sa::Insert) -> Result<Plan, String> {
    let table = table_name_of(&ins.table)
        .ok_or("INSERT target is not a plain table name")?;
    let source = ins.source.ok_or("INSERT without VALUES")?;
    let values = match *source.body {
        sa::SetExpr::Values(v) => v.rows,
        other => return Err(format!("unsupported INSERT source: {other:?}")),
    };
    // Map the statement's column list onto our fixed schema.
    let col_order: Vec<usize> = if ins.columns.is_empty() {
        (0..COLUMNS.len()).collect()
    } else {
        ins.columns
            .iter()
            .map(|c| { let n = object_name(c); column_index(&n).ok_or_else(|| format!("no such column: {n}")) })
            .collect::<Result<_, _>>()?
    };
    let mut rows = Vec::with_capacity(values.len());
    for tuple in values {
        if tuple.content.len() != col_order.len() {
            return Err("INSERT column/value count mismatch".into());
        }
        let mut fields = [Value::Null, Value::Null, Value::Null, Value::Null];
        for (slot, e) in col_order.iter().zip(tuple.content) {
            fields[*slot] = literal_value(&e)?;
        }
        rows.push(row_from_fields(fields)?);
    }
    Ok(Plan::Insert { table, rows })
}

fn table_name_of(t: &sa::TableObject) -> Option<String> {
    match t {
        sa::TableObject::TableName(n) => Some(object_name(n)),
        _ => None,
    }
}

fn plan_delete(del: sa::Delete) -> Result<Plan, String> {
    let table = match &del.from {
        sa::FromTable::WithFromKeyword(t) | sa::FromTable::WithoutKeyword(t) => {
            let first = t.first().ok_or("DELETE without a table")?;
            table_from_factor(&first.relation)?
        }
    };
    let filter = del.selection.map(|e| plan_expr(&e)).transpose()?;
    Ok(Plan::Delete { table, filter })
}

fn plan_update(
    table: sa::TableWithJoins,
    assignments: Vec<sa::Assignment>,
    selection: Option<sa::Expr>,
) -> Result<Plan, String> {
    let name = table_from_factor(&table.relation)?;
    let mut sets = Vec::new();
    for a in assignments {
        let col = match &a.target {
            sa::AssignmentTarget::ColumnName(n) => object_name(n),
            other => return Err(format!("unsupported assignment target: {other:?}")),
        };
        let idx = column_index(&col).ok_or_else(|| format!("no such column: {col}"))?;
        sets.push((idx, plan_expr(&a.value)?));
    }
    let filter = selection.map(|e| plan_expr(&e)).transpose()?;
    Ok(Plan::Update {
        table: name,
        sets,
        filter,
    })
}

fn plan_query(q: sa::Query) -> Result<Plan, String> {
    let select = match *q.body {
        sa::SetExpr::Select(s) => s,
        other => return Err(format!("unsupported query body: {other:?}")),
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err("queries must read exactly one table (no joins in M2)".into());
    }
    let table = table_from_factor(&select.from[0].relation)?;
    let filter = select.selection.as_ref().map(plan_expr).transpose()?;

    let mut projections = Vec::new();
    let mut has_agg = false;
    for item in &select.projection {
        match plan_projection(item) {
            Ok(p) => {
                if matches!(p, Projection::Agg(..)) {
                    has_agg = true;
                }
                projections.push(p);
            }
            Err(w) if w == "__wildcard__" => {
                // Expand `*` to every column of the fixed schema.
                for (i, name) in COLUMNS.iter().enumerate() {
                    projections.push(Projection::Expr(Expr::Column(i), name.to_string()));
                }
            }
            Err(e) => return Err(e),
        }
    }

    let group_by: Vec<usize> = match &select.group_by {
        sa::GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| match plan_expr(e)? {
                Expr::Column(i) => Ok(i),
                _ => Err("GROUP BY must be a column".to_string()),
            })
            .collect::<Result<_, _>>()?,
        sa::GroupByExpr::All(_) => return Err("GROUP BY ALL is unsupported".into()),
    };
    if !group_by.is_empty() {
        has_agg = true;
    }

    let order_by = plan_order_by(&q.order_by)?;
    let limit = plan_limit(&q.limit_clause)?;

    Ok(Plan::Select {
        table,
        projections,
        filter,
        group_by: if has_agg { group_by } else { Vec::new() },
        order_by,
        limit,
        distinct: select.distinct.is_some(),
    })
}

fn plan_projection(item: &sa::SelectItem) -> Result<Projection, String> {
    match item {
        sa::SelectItem::Wildcard(_) => {
            // Expanded by the caller; represent as the pk column here is wrong,
            // so signal it specially.
            Err("__wildcard__".into())
        }
        sa::SelectItem::UnnamedExpr(e) => project_expr(e, expr_label(e)),
        sa::SelectItem::ExprWithAlias { expr, alias } => project_expr(expr, alias.value.clone()),
        other => Err(format!("unsupported projection: {other:?}")),
    }
}

fn project_expr(e: &sa::Expr, label: String) -> Result<Projection, String> {
    if let Some((f, arg)) = try_aggregate(e)? {
        return Ok(Projection::Agg(f, arg, label));
    }
    Ok(Projection::Expr(plan_expr(e)?, label))
}

fn try_aggregate(e: &sa::Expr) -> Result<Option<(AggFn, Option<usize>)>, String> {
    let sa::Expr::Function(f) = e else {
        return Ok(None);
    };
    let name = f.name.to_string().to_ascii_lowercase();
    let agg = match name.as_str() {
        "count" => AggFn::Count,
        "sum" => AggFn::Sum,
        "min" => AggFn::Min,
        "max" => AggFn::Max,
        "avg" => AggFn::Avg,
        _ => return Ok(None),
    };
    let args = match &f.args {
        sa::FunctionArguments::List(l) => &l.args,
        _ => return Err(format!("unsupported call to {name}")),
    };
    if args.len() != 1 {
        return Err(format!("{name} takes one argument"));
    }
    let arg = match &args[0] {
        sa::FunctionArg::Unnamed(sa::FunctionArgExpr::Wildcard) => None,
        sa::FunctionArg::Unnamed(sa::FunctionArgExpr::Expr(sa::Expr::Identifier(id))) => {
            Some(column_index(&id.value).ok_or_else(|| format!("no such column: {id}"))?)
        }
        other => return Err(format!("unsupported aggregate argument: {other:?}")),
    };
    Ok(Some((agg, arg)))
}

fn plan_order_by(ob: &Option<sa::OrderBy>) -> Result<Vec<OrderKey>, String> {
    let Some(ob) = ob else { return Ok(Vec::new()) };
    let exprs = match &ob.kind {
        sa::OrderByKind::Expressions(e) => e,
        sa::OrderByKind::All(_) => return Err("ORDER BY ALL is unsupported".into()),
    };
    exprs
        .iter()
        .map(|o| {
            Ok(OrderKey {
                expr: plan_expr(&o.expr)?,
                ascending: o.options.asc.unwrap_or(true),
            })
        })
        .collect()
}

fn plan_limit(limit: &Option<sa::LimitClause>) -> Result<Option<usize>, String> {
    match limit {
        None => Ok(None),
        Some(sa::LimitClause::LimitOffset { limit: Some(e), .. }) => {
            Ok(Some(as_usize(e)?))
        }
        Some(sa::LimitClause::LimitOffset { limit: None, .. }) => Ok(None),
        Some(other) => Err(format!("unsupported LIMIT: {other:?}")),
    }
}

fn expr_label(e: &sa::Expr) -> String {
    match e {
        sa::Expr::Identifier(id) => id.value.clone(),
        other => other.to_string(),
    }
}

/// Translate a scalar AST expression into our [`Expr`].
pub fn plan_expr(e: &sa::Expr) -> Result<Expr, String> {
    match e {
        sa::Expr::Identifier(id) => Expr::column(&id.value),
        sa::Expr::CompoundIdentifier(parts) => {
            Expr::column(&parts.last().map(|p| p.value.clone()).unwrap_or_default())
        }
        sa::Expr::Value(v) => Ok(Expr::Literal(literal_value_inner(&v.value)?)),
        sa::Expr::Nested(inner) => plan_expr(inner),
        sa::Expr::IsNull(inner) => Ok(Expr::IsNull(Box::new(plan_expr(inner)?), false)),
        sa::Expr::IsNotNull(inner) => Ok(Expr::IsNull(Box::new(plan_expr(inner)?), true)),
        sa::Expr::UnaryOp { op, expr } => {
            let u = match op {
                sa::UnaryOperator::Not => UnaryOp::Not,
                sa::UnaryOperator::Minus => UnaryOp::Neg,
                other => return Err(format!("unsupported unary op: {other:?}")),
            };
            Ok(Expr::Unary(u, Box::new(plan_expr(expr)?)))
        }
        sa::Expr::BinaryOp { left, op, right } => {
            let b = binary_op(op)?;
            Ok(Expr::Binary(
                b,
                Box::new(plan_expr(left)?),
                Box::new(plan_expr(right)?),
            ))
        }
        other => Err(format!("unsupported expression: {other:?}")),
    }
}

fn binary_op(op: &sa::BinaryOperator) -> Result<BinaryOp, String> {
    use sa::BinaryOperator as B;
    Ok(match op {
        B::Plus => BinaryOp::Add,
        B::Minus => BinaryOp::Sub,
        B::Multiply => BinaryOp::Mul,
        B::Divide => BinaryOp::Div,
        B::Modulo => BinaryOp::Mod,
        B::Eq => BinaryOp::Eq,
        B::NotEq => BinaryOp::NotEq,
        B::Lt => BinaryOp::Lt,
        B::LtEq => BinaryOp::LtEq,
        B::Gt => BinaryOp::Gt,
        B::GtEq => BinaryOp::GtEq,
        B::And => BinaryOp::And,
        B::Or => BinaryOp::Or,
        other => return Err(format!("unsupported binary op: {other:?}")),
    })
}

fn literal_value(e: &sa::Expr) -> Result<Value, String> {
    match e {
        sa::Expr::Value(v) => literal_value_inner(&v.value),
        sa::Expr::UnaryOp {
            op: sa::UnaryOperator::Minus,
            expr,
        } => match literal_value(expr)? {
            Value::Int(i) => Ok(Value::Int(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            _ => Err("cannot negate non-numeric literal".into()),
        },
        other => Err(format!("expected a literal, got {other:?}")),
    }
}

fn literal_value_inner(v: &sa::Value) -> Result<Value, String> {
    match v {
        sa::Value::Null => Ok(Value::Null),
        sa::Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Value::Int(i))
            } else {
                n.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| format!("bad number literal: {n}"))
            }
        }
        sa::Value::SingleQuotedString(s) | sa::Value::DoubleQuotedString(s) => {
            Ok(Value::Text(s.clone()))
        }
        sa::Value::Boolean(b) => Ok(Value::Bool(*b)),
        other => Err(format!("unsupported literal: {other:?}")),
    }
}

fn as_usize(e: &sa::Expr) -> Result<usize, String> {
    match literal_value(e)? {
        Value::Int(i) if i >= 0 => Ok(i as usize),
        _ => Err("expected a non-negative integer".into()),
    }
}

fn row_from_fields(f: [Value; 4]) -> Result<Row, String> {
    let pk = match &f[0] {
        Value::Int(i) => *i,
        v => return Err(format!("pk must be an integer, got {}", v.render())),
    };
    let a = match &f[1] {
        Value::Int(i) => *i,
        Value::Null => 0,
        v => return Err(format!("a must be an integer, got {}", v.render())),
    };
    let b = match &f[2] {
        Value::Float(x) => *x,
        Value::Int(i) => *i as f64,
        Value::Null => 0.0,
        v => return Err(format!("b must be a number, got {}", v.render())),
    };
    let c = match &f[3] {
        Value::Text(s) => s.clone(),
        Value::Null => String::new(),
        v => v.render(),
    };
    Ok(Row::new(pk, a, b, c))
}
