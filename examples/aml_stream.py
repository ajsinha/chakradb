#!/usr/bin/env python3
"""
Real-time streaming AML in Python — the event-driven design.
============================================================

The Python mirror of ``examples/aml_pipeline.rs``. Where ``aml_app.py`` scores a
finished dataset, this is the *live* system: a generator streams transactions
into ChakraDB, and a perpetually-running worker reacts to each committed
transaction the instant it lands — via ``conn.on_change`` — never blocking the
writer.

It shows the tiered detection model:

  * **T0 (per-event, O(1)):** degree counters + known-bad lookups fired on every
    committed transaction, at ingest speed.
  * **T2 (periodic, snapshot-isolated):** the heavy global algorithms —
    ``laundering_cycles`` + ``personalized_pagerank`` — run every few thousand
    events over a consistent ``graph.view()``.

The callback runs on a background thread; while a T2 pass holds the GIL, ingest
pauses briefly — which is exactly why the *scalable* worker is the Rust one
(``aml_pipeline.rs``). This Python version is the readable, ergonomic mirror.

Run:  python examples/aml_stream.py
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

# --- Account-id layout (distinct ranges → assertable typologies) ------------
LEGIT_LO, LEGIT_HI = 1, 400_000            # a large, sparse account space
COLLECTOR = 900_000
MULES = range(900_001, 900_013)            # 12 smurfs
RING = [910_000, 910_001, 910_002, 910_003]
DISTRIBUTOR = 920_000
FANOUT = range(920_001, 920_021)           # 20 downstream mules

N_LEGIT = 20_000                           # background traffic (Python: modest)
T2_INTERVAL = 5_000
CTR_THRESHOLD = 10_000.0
FAN_IN_THRESHOLD, MIN_STRUCTURED, FAN_OUT_THRESHOLD = 8, 5, 15


class Rng:
    """Deterministic SplitMix64 — same dataset on every run."""
    def __init__(self, seed: int):
        self.s = seed & 0xFFFFFFFFFFFFFFFF

    def next(self) -> int:
        self.s = (self.s + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
        z = self.s
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
        return z ^ (z >> 31)

    def range(self, lo: int, hi: int) -> int:
        return lo + self.next() % (hi - lo)


class Worker:
    """The perpetual AML worker: T0 counters + periodic T2 global passes."""

    def __init__(self, conn):
        self.conn = conn
        self.graph = conn.graph("transfers")
        self.known_bad = {DISTRIBUTOR}
        self.in_sources: dict[int, set[int]] = {}
        self.out_targets: dict[int, set[int]] = {}
        self.near_threshold: dict[int, int] = {}
        self.alerts: dict[int, set[str]] = {}
        self.seen = 0
        self.alert_id = 0
        self.lock = threading.Lock()

    # T0 — runs on every committed transaction.
    def on_change(self, old, new):
        if new is None:
            return
        src, dst, amt = new["src"], new["dst"], float(new["amount"])
        self.graph.add_edge(src, dst, amt)      # mirror into the payment graph

        senders = self.in_sources.setdefault(dst, set()); senders.add(src)
        if 9_000.0 <= amt < CTR_THRESHOLD:
            self.near_threshold[dst] = self.near_threshold.get(dst, 0) + 1
        receivers = self.out_targets.setdefault(src, set()); receivers.add(dst)

        if len(senders) >= FAN_IN_THRESHOLD and self.near_threshold.get(dst, 0) >= MIN_STRUCTURED:
            self.alert(dst, "structuring-collector")
        if len(receivers) >= FAN_OUT_THRESHOLD:
            self.alert(src, "mule-fan-out")
        if src in self.known_bad and dst != src:
            self.alert(dst, "pays-from-known-bad")

        self.seen += 1
        if self.seen % T2_INTERVAL == 0:
            self.global_pass()

    # T2 — periodic global pass over a consistent snapshot.
    def global_pass(self):
        view = self.graph.view()
        for ring in view.laundering_cycles():
            for member in ring:
                self.alert(member, "laundering-cycle")
        risk = view.personalized_pagerank(list(self.known_bad), iterations=30)
        ranked = sorted(((n, r) for n, r in risk.items() if n not in self.known_bad),
                        key=lambda t: t[1], reverse=True)
        for acct, _ in ranked[:5]:
            self.alert(acct, "high-risk-exposure")

    def alert(self, account: int, typology: str):
        reasons = self.alerts.setdefault(account, set())
        if typology not in reasons:
            reasons.add(typology)
            with self.lock:
                self.alert_id += 1
                self.conn.execute(
                    f"INSERT INTO alerts VALUES ({self.alert_id}, {account}, '{typology}')")


def main() -> None:
    rule = "=" * 64
    print(f"\n{rule}\n  ChakraDB — Real-Time AML Stream (Python, event-driven)\n{rule}\n")

    conn = chakradb.connect(":memory:")
    conn.execute("""CREATE TABLE transactions (
        txn_id INTEGER PRIMARY KEY, src INTEGER, dst INTEGER,
        amount DECIMAL(14,2), ts TIMESTAMP)""")
    conn.execute("CREATE TABLE alerts (id INTEGER PRIMARY KEY, account INTEGER, typology VARCHAR(32))")

    worker = Worker(conn)
    total = 12 + 1 + 4 + 20 + N_LEGIT      # typology injections + legit stream
    done = threading.Event()

    def react(old, new):
        worker.on_change(old, new)
        if worker.seen >= total:
            done.set()

    conn.on_change("transactions", react)

    # --- Generator: stream synthetic transactions -------------------------
    rng = Rng(0x00A471CE2026)
    txn_id = [1]

    def emit(src, dst, amount):
        ts = f"2026-03-{rng.range(1,29):02d} {rng.range(0,24):02d}:{rng.range(0,60):02d}:00"
        conn.execute(
            f"INSERT INTO transactions VALUES ({txn_id[0]}, {src}, {dst}, {amount:.2f}, '{ts}')")
        txn_id[0] += 1

    # Inject the laundering typologies, then bury them in legitimate traffic.
    for mule in MULES:
        emit(mule, COLLECTOR, CTR_THRESHOLD - rng.range(50, 900))
    emit(COLLECTOR, RING[0], 95_000.0)
    for i, s in enumerate(RING):
        emit(s, RING[(i + 1) % len(RING)], 90_000.0)
    for mule in FANOUT:
        emit(DISTRIBUTOR, mule, rng.range(2_000, 8_000))
    for _ in range(N_LEGIT):
        a, b = rng.range(LEGIT_LO, LEGIT_HI), rng.range(LEGIT_LO, LEGIT_HI + 1)
        if a == b:
            continue
        src, dst = (a, b) if a < b else (b, a)
        emit(src, dst, rng.range(20, 4_000) + 0.99)

    done.wait(timeout=60)
    worker.global_pass()                    # final sweep

    print(f"Worker reacted to {worker.seen} committed transactions via on_change.")
    print(f"Persisted {worker.alert_id} alerts.\n")
    print("── Accounts flagged (typology ensemble over the live stream) ──")
    for acct in sorted(worker.alerts):
        print(f"  {acct:>7} [{label(acct):<12}]: {', '.join(sorted(worker.alerts[acct]))}")

    # Self-check: every planted typology caught live.
    def has(a, kind):
        return any(kind in r for r in worker.alerts.get(a, set()))
    assert has(COLLECTOR, "structuring"), "collector must be flagged"
    assert all(has(r, "cycle") for r in RING), "ring must be flagged"
    assert has(DISTRIBUTOR, "fan-out"), "distributor must be flagged"

    print(f"\n{rule}\n  All typologies detected live — Python pipeline verified.\n{rule}\n")
    conn.close()


def label(a: int) -> str:
    if a == COLLECTOR:
        return "collector"
    if a in MULES:
        return "smurf"
    if a in RING:
        return "ring"
    if a == DISTRIBUTOR:
        return "distributor"
    if a in FANOUT:
        return "mule"
    return "other"


if __name__ == "__main__":
    main()
