//! Planner coverage: parsing the documented SQL subset into logical plans,
//! and rejecting what lies outside it with a clear error.

use chakradb::sql::plan::{plan, AggFn, Plan, Projection};

#[test]
fn create_table() {
    assert!(matches!(
        plan("CREATE TABLE t (pk INT)").unwrap(),
        Plan::CreateTable { name, .. } if name == "t"
    ));
}

#[test]
fn insert_with_explicit_columns() {
    let p = plan("INSERT INTO t (pk, a, b, c) VALUES (1, 2, 3.5, 'x')").unwrap();
    match p {
        Plan::Insert { table, rows } => {
            assert_eq!(table, "t");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].pk(), 1);
            assert_eq!(rows[0].c(), "x");
        }
        _ => panic!(),
    }
}

#[test]
fn insert_multiple_rows() {
    let p = plan("INSERT INTO t VALUES (1,2,3,'a'), (4,5,6,'b')").unwrap();
    match p {
        Plan::Insert { rows, .. } => assert_eq!(rows.len(), 2),
        _ => panic!(),
    }
}

#[test]
fn insert_negative_number() {
    let p = plan("INSERT INTO t VALUES (-5, -10, -2.5, 'n')").unwrap();
    match p {
        Plan::Insert { rows, .. } => {
            assert_eq!(rows[0].pk(), -5);
            assert_eq!(rows[0].b(), -2.5);
        }
        _ => panic!(),
    }
}

#[test]
fn select_star() {
    // Wildcard is handled by the executor; the planner flags it.
    let p = plan("SELECT * FROM t");
    assert!(p.is_ok(), "select * should plan");
}

#[test]
fn select_with_filter_and_limit() {
    let p = plan("SELECT pk FROM t WHERE a > 5 LIMIT 10").unwrap();
    match p {
        Plan::Select {
            filter, limit, ..
        } => {
            assert!(filter.is_some());
            assert_eq!(limit, Some(10));
        }
        _ => panic!(),
    }
}

#[test]
fn select_order_by_desc() {
    let p = plan("SELECT pk FROM t ORDER BY pk DESC").unwrap();
    match p {
        Plan::Select { order_by, .. } => {
            assert_eq!(order_by.len(), 1);
            assert!(!order_by[0].ascending);
        }
        _ => panic!(),
    }
}

#[test]
fn select_aggregate() {
    let p = plan("SELECT COUNT(*), SUM(a) FROM t").unwrap();
    match p {
        Plan::Select { projections, .. } => {
            assert!(matches!(projections[0], Projection::Agg(AggFn::Count, None, _)));
            assert!(matches!(projections[1], Projection::Agg(AggFn::Sum, Some(1), _)));
        }
        _ => panic!(),
    }
}

#[test]
fn select_group_by() {
    let p = plan("SELECT a, COUNT(*) FROM t GROUP BY a").unwrap();
    match p {
        Plan::Select { group_by, .. } => assert_eq!(group_by, vec![1]),
        _ => panic!(),
    }
}

#[test]
fn delete_with_filter() {
    let p = plan("DELETE FROM t WHERE pk = 5").unwrap();
    assert!(matches!(p, Plan::Delete { filter: Some(_), .. }));
}

#[test]
fn update_with_sets() {
    let p = plan("UPDATE t SET a = 99, c = 'new' WHERE pk = 1").unwrap();
    match p {
        Plan::Update { sets, filter, .. } => {
            assert_eq!(sets.len(), 2);
            assert!(filter.is_some());
        }
        _ => panic!(),
    }
}

#[test]
fn unsupported_join_is_rejected() {
    let e = plan("SELECT * FROM t JOIN u ON t.pk = u.pk");
    assert!(e.is_err(), "joins are out of the M2 subset");
}

#[test]
fn syntax_error_is_reported() {
    assert!(plan("SELCT * FRM").is_err());
}

#[test]
fn distinct_is_planned() {
    let p = plan("SELECT DISTINCT a FROM t").unwrap();
    assert!(matches!(p, Plan::Select { distinct: true, .. }));
}
