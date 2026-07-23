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
fn dijkstra_weighted_paths() {
    // 1 -(1)-> 2 -(1)-> 4 ;  1 -(5)-> 4  ; the two-hop route is cheaper.
    let g = graph();
    g.add_edges([(1, 2, 1.0), (2, 4, 1.0), (1, 4, 5.0)]).unwrap();
    let v = g.view().unwrap();
    let d = v.dijkstra(1);
    assert_eq!(d[&2], 1.0);
    assert_eq!(d[&4], 2.0, "prefers the cheaper two-hop route");
    let (path, cost) = v.weighted_shortest_path(1, 4).unwrap();
    assert_eq!(path, vec![1, 2, 4]);
    assert_eq!(cost, 2.0);
    assert!(v.weighted_shortest_path(4, 1).is_none());
}

#[test]
fn topological_order_and_cycle() {
    let g = graph();
    g.add_edges([(1, 2, 1.0), (1, 3, 1.0), (3, 4, 1.0), (2, 4, 1.0)])
        .unwrap();
    let order = g.view().unwrap().topological_order().unwrap();
    let pos = |x: chakradb::NodeId| order.iter().position(|&y| y == x).unwrap();
    assert!(pos(1) < pos(2) && pos(2) < pos(4) && pos(3) < pos(4));
    // Add a back-edge to make a cycle → no topological order.
    g.add_edge(4, 1, 1.0).unwrap();
    assert!(g.view().unwrap().topological_order().is_none());
}

#[test]
fn centrality_and_community() {
    // A star with center 1 (1<->2..5) → center has highest betweenness/closeness.
    let g = graph();
    for x in 2..=5 {
        g.add_edge(1, x, 1.0).unwrap();
        g.add_edge(x, 1, 1.0).unwrap();
    }
    let v = g.view().unwrap();
    let bc = v.betweenness_centrality();
    let cc = v.closeness_centrality();
    let top_bc = bc.iter().max_by(|a, b| a.1.partial_cmp(b.1).unwrap()).unwrap();
    assert_eq!(*top_bc.0, 1, "the hub has the highest betweenness");
    assert!(cc[&1] >= cc[&2]);
    // One connected star → label propagation puts everyone in one community.
    let comm = v.label_propagation(10);
    assert!(comm.values().collect::<std::collections::HashSet<_>>().len() <= 2);
}

#[test]
fn k_core_and_similarity() {
    // A triangle 1-2-3 (each undirected degree 2) plus a pendant 3->4.
    let g = graph();
    g.add_edges([(1, 2, 1.0), (2, 3, 1.0), (3, 1, 1.0), (3, 4, 1.0)])
        .unwrap();
    let v = g.view().unwrap();
    let core = v.k_core();
    assert_eq!(core[&1], 2); // in the 2-core (triangle)
    assert_eq!(core[&4], 1); // pendant: only the 1-core
    // Common neighbours: both 1 and 2 have an out-edge to 3.
    let g2 = graph();
    g2.add_edges([(1, 3, 1.0), (2, 3, 1.0), (1, 9, 1.0)]).unwrap();
    let v2 = g2.view().unwrap();
    assert_eq!(v2.common_neighbors(1, 2), vec![3]);
    assert_eq!(v2.jaccard_similarity(1, 1), 1.0, "a node is identical to itself");
    // out(1)={3,9}, out(2)={3} → |∩|=1, |∪|=2 → 0.5.
    assert!((v2.jaccard_similarity(1, 2) - 0.5).abs() < 1e-9);
}

#[test]
fn eisenberg_noe_default_cascade() {
    use std::collections::HashMap;
    // A liability chain: 1 owes 2 owes 3 (each 100). Node 1 has no external cash.
    //   external assets: only node 1 has 60; 2 and 3 have nothing of their own.
    let g = graph();
    g.add_edges([(1, 2, 100.0), (2, 3, 100.0)]).unwrap();
    let v = g.view().unwrap();

    let mut ext = HashMap::new();
    ext.insert(1u32, 60.0); // node 1 can only source 60 against a 100 liability

    let r = v.eisenberg_noe(&ext);
    // Node 1 pays only 60 of the 100 it owes → it defaults.
    assert!((r.payments[&1] - 60.0).abs() < 1e-6);
    assert!(r.defaulted.contains(&1));
    // Node 2 receives 60, owes 100 → pays 60, also defaults (contagion).
    assert!((r.payments[&2] - 60.0).abs() < 1e-6);
    assert!(r.defaulted.contains(&2));
    // Node 3 owes nothing → never defaults; ends with the 60 it received.
    assert!(!r.defaulted.contains(&3));
    assert!((r.equity[&3] - 60.0).abs() < 1e-6);
}

#[test]
fn eisenberg_noe_all_solvent_when_funded() {
    use std::collections::HashMap;
    let g = graph();
    g.add_edges([(1, 2, 100.0), (2, 3, 100.0)]).unwrap();
    let v = g.view().unwrap();
    // Node 1 fully funded → everyone pays in full, no defaults.
    let ext = HashMap::from([(1u32, 100.0)]);
    let r = v.eisenberg_noe(&ext);
    assert!(r.defaulted.is_empty(), "fully-funded network clears");
    assert!((r.payments[&1] - 100.0).abs() < 1e-6);
    assert!((r.payments[&2] - 100.0).abs() < 1e-6);
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
