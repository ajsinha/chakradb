//! Built-in graph capabilities: adjacency, snapshot views, and core algorithms.

use chakradb::{Database, Graph};
use std::sync::Arc;

fn graph() -> Graph {
    let be: Arc<dyn chakradb::sql::SqlBackend> = Arc::new(Database::new());
    Graph::open(be, "g").unwrap()
}

/// A small directed graph:
///   1 -> 2 -> 3 -> 4
///   1 -> 3
///   4 -> 1        (a cycle back)
///   5 (isolated component: 5 -> 6)
fn sample() -> Graph {
    let g = graph();
    g.add_edges([
        (1, 2, 1.0),
        (2, 3, 1.0),
        (3, 4, 1.0),
        (1, 3, 1.0),
        (4, 1, 1.0),
        (5, 6, 1.0),
    ])
    .unwrap();
    g
}

#[test]
fn out_neighbors_via_pruned_scan() {
    let g = sample();
    let mut n = g.out_neighbors(1).unwrap();
    n.sort_unstable();
    assert_eq!(n, vec![2, 3]);
    assert_eq!(g.out_neighbors(3).unwrap(), vec![4]);
    assert!(g.out_neighbors(99).unwrap().is_empty());
}

#[test]
fn view_counts() {
    let v = sample().view().unwrap();
    assert_eq!(v.node_count(), 6); // 1..6
    assert_eq!(v.edge_count(), 6);
    assert_eq!(v.out_degree(1), 2);
    assert_eq!(v.out_degree(6), 0);
}

#[test]
fn bfs_and_shortest_path() {
    let v = sample().view().unwrap();
    let dist = v.bfs(1);
    assert_eq!(dist[&1], 0);
    assert_eq!(dist[&2], 1);
    assert_eq!(dist[&3], 1); // via the 1->3 shortcut, not 1->2->3
    assert_eq!(dist[&4], 2);
    assert!(!dist.contains_key(&5), "different component unreachable");

    assert_eq!(v.shortest_path(1, 4), Some(vec![1, 3, 4]));
    assert_eq!(v.shortest_path(1, 1), Some(vec![1]));
    assert_eq!(v.shortest_path(1, 5), None);
}

#[test]
fn connected_components_are_weak() {
    let v = sample().view().unwrap();
    let c = v.connected_components();
    // {1,2,3,4} form one component; {5,6} another.
    assert_eq!(c[&1], c[&4]);
    assert_eq!(c[&5], c[&6]);
    assert_ne!(c[&1], c[&5]);
}

#[test]
fn pagerank_ranks_a_hub_highest() {
    // A star: 2,3,4,5 all point at 1 -> node 1 should rank highest.
    let g = graph();
    g.add_edges([(2, 1, 1.0), (3, 1, 1.0), (4, 1, 1.0), (5, 1, 1.0)])
        .unwrap();
    let pr = g.view().unwrap().pagerank(30, 0.85);
    let top = pr.iter().max_by(|a, b| a.1.partial_cmp(b.1).unwrap()).unwrap();
    assert_eq!(*top.0, 1, "the hub everyone points to ranks first");
    let sum: f64 = pr.values().sum();
    assert!((sum - 1.0).abs() < 1e-6, "scores form a distribution");
}

#[test]
fn triangle_count() {
    let g = graph();
    // One triangle 1-2-3, plus a dangling edge 3-4.
    g.add_edges([(1, 2, 1.0), (2, 3, 1.0), (3, 1, 1.0), (3, 4, 1.0)])
        .unwrap();
    assert_eq!(g.view().unwrap().triangle_count(), 1);
}

#[test]
fn in_degree_and_in_neighbors() {
    // A fan-in hub: 2,3,4 all send to 1.
    let g = graph();
    g.add_edges([(2, 1, 1.0), (3, 1, 1.0), (4, 1, 1.0), (1, 5, 1.0)])
        .unwrap();
    let v = g.view().unwrap();
    assert_eq!(v.in_degree(1), 3); // fan-in of 3
    assert_eq!(v.out_degree(1), 1);
    let mut ins = v.in_neighbors(1);
    ins.sort_unstable();
    assert_eq!(ins, vec![2, 3, 4]);
    assert_eq!(v.in_degree(2), 0);
}

#[test]
fn strongly_connected_components_find_a_laundering_cycle() {
    // A round-trip 1 -> 2 -> 3 -> 1 (a cycle), plus a dangling sink 3 -> 4.
    let g = graph();
    g.add_edges([(1, 2, 1.0), (2, 3, 1.0), (3, 1, 1.0), (3, 4, 1.0)])
        .unwrap();
    let cycles = g.view().unwrap().laundering_cycles();
    assert_eq!(cycles.len(), 1, "one non-trivial SCC");
    let mut ring = cycles[0].clone();
    ring.sort_unstable();
    assert_eq!(ring, vec![1, 2, 3], "the cycle members");
}

#[test]
fn no_cycle_in_a_dag() {
    let g = graph();
    g.add_edges([(1, 2, 1.0), (2, 3, 1.0), (1, 3, 1.0)]).unwrap();
    assert!(g.view().unwrap().laundering_cycles().is_empty());
}

#[test]
fn personalized_pagerank_scores_by_proximity_to_seeds() {
    // A chain 1 -> 2 -> 3 -> 4; seed the risk at node 1.
    let g = graph();
    g.add_edges([(1, 2, 1.0), (2, 3, 1.0), (3, 4, 1.0)]).unwrap();
    let ppr = g.view().unwrap().personalized_pagerank(&[1], 40, 0.85);
    // Risk decays with distance from the seed: 1 > 2 > 3 > 4.
    assert!(ppr[&1] > ppr[&2]);
    assert!(ppr[&2] > ppr[&3]);
    assert!(ppr[&3] > ppr[&4]);
    // An unrelated seed set that isn't present yields nothing.
    assert!(g.view().unwrap().personalized_pagerank(&[999], 10, 0.85).is_empty());
}

#[test]
fn view_is_a_consistent_snapshot_under_writes() {
    // The wedge, for graphs: an algorithm's view is stable while the graph grows.
    let g = sample();
    let v = g.view().unwrap();
    let before = v.edge_count();
    // Keep writing edges after the view was taken.
    g.add_edges([(6, 7, 1.0), (7, 8, 1.0)]).unwrap();
    assert_eq!(v.edge_count(), before, "the snapshot view did not change");
    // A fresh view sees the new edges.
    assert_eq!(g.view().unwrap().edge_count(), before + 2);
}
