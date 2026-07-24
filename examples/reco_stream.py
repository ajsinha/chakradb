#!/usr/bin/env python3
"""
Real-time Recommendations in Python — event-driven.
===================================================

The Python mirror of ``examples/reco_pipeline.rs``. A stream of user interactions
drives a live recommender: as engagement arrives (via ``conn.on_change``), a
worker maintains the user↔item graph and answers "what next?" with the built-in
link-prediction algorithms:

  * ``view.recommend(user, k)`` — random-walk-with-restart collaborative filtering.
  * ``view.adamic_adar(a, b)``  — "you might also like" item similarity.
  * degree / ``view.pagerank`` — what's trending.

Run:  python examples/reco_stream.py
"""

from __future__ import annotations

import os
import sys
import threading

try:
    import chakradb
except ModuleNotFoundError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", "bindings", "python", "python"))
    import chakradb

ITEM_BASE = 1_000_000
N_USERS, N_ITEMS = 5_000, 400
N_INTERACTIONS = 20_000
T2_INTERVAL = 5_000
COHORT = range(1, 13)          # power users
TARGET = 13                    # recommend for this user
BUNDLE = [100, 101, 102]       # items the cohort co-engages
TRENDING = 1                   # item nearly everyone touches


def item_node(raw): return ITEM_BASE + raw


class Rng:
    def __init__(self, seed): self.s = seed & 0xFFFFFFFFFFFFFFFF
    def nxt(self):
        self.s = (self.s + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
        z = self.s
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
        return z ^ (z >> 31)
    def rng(self, lo, hi): return lo + self.nxt() % (hi - lo)
    def chance(self, p): return (self.nxt() >> 11) / (1 << 53) < p


class Recommender:
    def __init__(self, conn, graph):
        self.conn = conn
        self.graph = graph
        self.seen = 0
        self.recommendations = []
        self.trending = []
        self.lock = threading.Lock()

    def on_change(self, old, new):
        if new is None:
            return
        item = item_node(int(new["item_id"]))
        user = int(new["user_id"])
        with self.lock:
            self.graph.add_edge(user, item, 1.0)   # bipartite, both directions
            self.graph.add_edge(item, user, 1.0)
        self.seen += 1
        if self.seen % T2_INTERVAL == 0:
            self.refresh()

    def refresh(self):
        with self.lock:
            view = self.graph.view()
        recs = [(n - ITEM_BASE, s) for n, s in view.recommend(TARGET, 40) if n >= ITEM_BASE]
        self.recommendations = recs[:10]
        items = [n for n in view.connected_components() if n >= ITEM_BASE]
        self.trending = sorted(((n - ITEM_BASE, view.out_degree(n)) for n in items),
                               key=lambda t: t[1], reverse=True)[:10]


def main() -> None:
    rule = "=" * 64
    print(f"\n{rule}\n  ChakraDB — Real-Time Recommendations (Python)\n{rule}\n")
    conn = chakradb.connect(":memory:")
    conn.execute("""CREATE TABLE interactions (id INTEGER PRIMARY KEY, user_id INTEGER,
        item_id INTEGER, kind VARCHAR(8), ts TIMESTAMP)""")
    graph = conn.graph("engagement")
    worker = Recommender(conn, graph)

    done = threading.Event()

    def react(old, new):
        worker.on_change(old, new)
        if worker.seen >= N_INTERACTIONS:
            done.set()

    conn.on_change("interactions", react)

    rng = Rng(0x00A471CE2026)
    idc = [1]

    def emit(user, item_raw):
        conn.execute(f"INSERT INTO interactions VALUES ({idc[0]}, {user}, {item_raw}, 'view', "
                     f"'2026-03-01 12:00:00')")
        idc[0] += 1

    # Injected co-engagement + trending, then random background.
    for u in COHORT:
        for b in BUNDLE:
            emit(u, b)
    emit(TARGET, BUNDLE[0]); emit(TARGET, BUNDLE[1])
    for u in range(1, N_USERS + 1):
        if rng.chance(0.4):
            emit(u, TRENDING)
    while idc[0] <= N_INTERACTIONS:
        emit(rng.rng(1, N_USERS + 1), rng.rng(2, N_ITEMS))

    done.wait(timeout=60)
    worker.refresh()

    print(f"Reacted to {worker.seen} interactions.\n")
    print(f"── Recommendations for user {TARGET} (items not yet engaged) ──")
    for item, score in worker.recommendations[:5]:
        tag = "  ← the co-engaged item the cohort completes" if item == BUNDLE[2] else ""
        print(f"  item {item:>4}  score {score:.4f}{tag}")
    print("\n── Trending items ──")
    for item, deg in worker.trending[:5]:
        print(f"  item {item:>4}  {deg} distinct users")

    rec_ids = [i for i, _ in worker.recommendations]
    assert BUNDLE[2] in rec_ids, f"item {BUNDLE[2]} should be recommended, got {rec_ids}"
    assert BUNDLE[0] not in rec_ids and BUNDLE[1] not in rec_ids, "engaged items not recommended"
    assert worker.trending[0][0] == TRENDING, "trending item ranks first"

    print(f"\n{rule}\n  Recommendations verified — the cohort's item surfaced live.\n{rule}\n")
    conn.close()


if __name__ == "__main__":
    main()
