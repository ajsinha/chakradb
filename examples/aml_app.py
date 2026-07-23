#!/usr/bin/env python3
"""
Real-Time Anti-Money-Laundering (AML) on ChakraDB — Python edition
==================================================================

A complete, self-contained AML application built on a *single* embedded ChakraDB
database. Transactions live in SQL tables (exact ``DECIMAL`` money, ``TIMESTAMP``
time) accessed through the PEP-249 driver; the **payment graph** is the very same
data seen through ChakraDB's built-in graph engine via ``conn.graph(name)``.

One consistent snapshot (``graph.view()``) powers an ensemble of detectors, each
mapped to a real laundering typology:

  Typology                         Primitive (built into ChakraDB core)
  -------------------------------  ------------------------------------------
  Structuring / smurfing (fan-in)  view.in_degree           + near-threshold SQL
  Layering / round-tripping        view.laundering_cycles   (strongly-conn. comps)
  Mule fan-out (distribution)      view.out_degree
  Risk propagation from known-bad  view.personalized_pagerank(seeds)
  Velocity / rapid movement        temporal SQL over ``transactions``

It generates its own **synthetic** dataset — a sea of legitimate traffic with a
handful of laundering rings deliberately injected — then scores every account,
ranks Suspicious-Activity-Report (SAR) candidates, and asserts that each planted
ring is caught. This is the Python mirror of ``examples/aml_realtime.rs``.

Run it
------
Build the extension once, then run the app::

    cd bindings/python && maturin develop        # or: pip install -e .
    python examples/aml_app.py

If the extension is already built into the source tree (``bindings/python/python/
chakradb/_core.abi3.so``), this script adds it to ``sys.path`` automatically.
"""

from __future__ import annotations

import os
import random
import sys

# --- Locate the chakradb package (built extension) --------------------------
try:
    import chakradb
except ModuleNotFoundError:
    _here = os.path.dirname(os.path.abspath(__file__))
    _pkg = os.path.join(_here, "..", "bindings", "python", "python")
    sys.path.insert(0, os.path.abspath(_pkg))
    import chakradb


# ---------------------------------------------------------------------------
# Account-id layout. Distinct ranges make the planted rings easy to assert on.
# ---------------------------------------------------------------------------
LEGIT = range(1, 201)          # 200 ordinary retail accounts

COLLECTOR = 500                # structuring: gathers the smurfed cash
MULES = range(501, 513)        # 12 smurfs, each depositing just under threshold

RING = [700, 701, 702, 703]    # layering: a round-trip cycle

DISTRIBUTOR = 800              # known-bad source of illicit funds (a PPR seed)
FANOUT = range(801, 821)       # 20 downstream mules receiving the spray

CTR_THRESHOLD = 10_000.0       # the reporting line structuring tries to dodge

# Detector thresholds
FAN_IN_THRESHOLD = 8
MIN_STRUCTURED_DEPOSITS = 5
FAN_OUT_THRESHOLD = 15


def main() -> None:
    conn = chakradb.connect(":memory:")
    banner("ChakraDB — Real-Time AML (Python)")
    schema(conn)

    # 1. Generate a synthetic world: legit traffic + injected laundering rings.
    rng = random.Random(0x00A471CE2026)
    edges = generate(conn, rng)
    txns = scalar(conn, "SELECT COUNT(*) FROM transactions")
    accounts = len({a for a, _, _ in edges} | {b for _, b, _ in edges})
    print(f"Ingested {txns} transactions across {accounts} accounts.\n")

    # 2. Project the payment graph and freeze one consistent snapshot. Every
    #    detector reads THIS graph — coherent even as new payments keep landing.
    g = conn.graph("transfers")
    g.add_edges(edges)
    view = g.view()
    print(f"Payment graph: {view.node_count()} accounts (nodes), "
          f"{view.edge_count()} counterparty edges.\n")

    # 3. Run the detector ensemble over the single snapshot.
    known_bad = [DISTRIBUTOR]
    fan_in = detect_structuring(conn, view)
    fan_out = detect_fan_out(view)
    rings = view.laundering_cycles()
    ring_members = {n for ring in rings for n in ring}
    risk = view.personalized_pagerank(known_bad, iterations=50, damping=0.85)

    report_structuring(conn, fan_in)
    report_layering(rings)
    report_fan_out(fan_out)
    report_risk_propagation(risk, known_bad)
    report_velocity(conn)

    # 4. Fuse the signals into one score per account and rank SAR candidates.
    candidates = score(view, fan_in, fan_out, ring_members, risk, set(known_bad))
    report_sar(candidates, fan_in, fan_out, ring_members, set(known_bad))

    # 5. Self-check: every planted typology must surface.
    verify(fan_in, fan_out, ring_members, risk, candidates)

    banner("All typologies detected — AML pipeline verified.")
    conn.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def schema(conn) -> None:
    conn.execute("""
        CREATE TABLE accounts (
            id     INTEGER PRIMARY KEY,
            owner  VARCHAR(64) NOT NULL,
            kind   VARCHAR(16) NOT NULL DEFAULT 'retail',
            opened DATE
        )""")
    conn.execute("""
        CREATE TABLE transactions (
            txn_id INTEGER PRIMARY KEY,
            src    INTEGER NOT NULL,
            dst    INTEGER NOT NULL,
            amount DECIMAL(14,2) NOT NULL CHECK (amount > 0),
            ts     TIMESTAMP NOT NULL
        )""")
    conn.execute("""
        CREATE TABLE known_bad (
            id     INTEGER PRIMARY KEY,
            reason VARCHAR(64) NOT NULL
        )""")
    conn.commit()


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------
def generate(conn, rng: random.Random) -> list[tuple[int, int, float]]:
    """Build the world; return payment edges and write rows to ``transactions``."""
    edges: list[tuple[int, int, float]] = []
    txn_id = [1]  # boxed so the nested helper can mutate it

    def account(acc_id: int, kind: str) -> None:
        conn.execute(
            f"INSERT INTO accounts VALUES ({acc_id}, 'owner_{acc_id}', '{kind}', '2026-01-01')"
        )

    def emit(src: int, dst: int, amount: float) -> None:
        ts = synth_timestamp(rng)
        conn.execute(
            f"INSERT INTO transactions VALUES "
            f"({txn_id[0]}, {src}, {dst}, {amount:.2f}, '{ts}')"
        )
        edges.append((src, dst, amount))
        txn_id[0] += 1

    # -- Accounts -----------------------------------------------------------
    for i in LEGIT:
        account(i, "retail")
    account(COLLECTOR, "personal")
    for i in MULES:
        account(i, "personal")
    for i in RING:
        account(i, "shell")
    account(DISTRIBUTOR, "shell")
    for i in FANOUT:
        account(i, "personal")
    conn.execute(
        f"INSERT INTO known_bad VALUES ({DISTRIBUTOR}, 'prior SAR: illicit funds source')"
    )

    # -- (a) Legitimate background traffic ----------------------------------
    # Sparse and acyclic: money flows "downhill" in id order (src < dst), so the
    # legit core is a DAG and the only cycle in the whole graph is a planted ring.
    for _ in range(350):
        a, b = rng.randrange(1, 200), rng.randrange(1, 201)
        if a == b:
            continue
        src, dst = (a, b) if a < b else (b, a)
        emit(src, dst, rng.randrange(20, 4000) + 0.99)

    # -- (b) Structuring / smurfing (fan-in) --------------------------------
    for mule in MULES:
        emit(mule, COLLECTOR, CTR_THRESHOLD - rng.randrange(50, 900))  # 9,100–9,950
    emit(COLLECTOR, RING[0], 95_000.0)  # collector forwards the aggregate onward

    # -- (c) Layering / round-tripping (a cycle) ----------------------------
    for i, src in enumerate(RING):
        dst = RING[(i + 1) % len(RING)]
        emit(src, dst, 90_000.0 - i * 1_500.0)  # small "fees" shaved off each hop

    # -- (d) Mule fan-out (distribution / integration) ----------------------
    for mule in FANOUT:
        amount = rng.randrange(2000, 8000) + 0.50
        emit(DISTRIBUTOR, mule, amount)
        if rng.random() < 0.5:                      # some mules push into retail
            emit(mule, rng.randrange(1, 201), amount * 0.6)

    conn.commit()
    return edges


def synth_timestamp(rng: random.Random) -> str:
    """A valid 'YYYY-MM-DD HH:MM:SS' timestamp in March 2026."""
    return (f"2026-03-{rng.randrange(1, 29):02d} "
            f"{rng.randrange(0, 24):02d}:{rng.randrange(0, 60):02d}:{rng.randrange(0, 60):02d}")


# ---------------------------------------------------------------------------
# Detectors — each a thin wrapper over one built-in graph/SQL primitive.
# ---------------------------------------------------------------------------
def detect_structuring(conn, view) -> list[tuple[int, int]]:
    """HTAP detector: graph fan-in (distinct sources) AND SQL near-threshold
    evidence. Requiring both separates a smurf collector from a popular account."""
    hits = []
    for node in view.connected_components():          # keys = every node
        deg = view.in_degree(node)
        if deg >= FAN_IN_THRESHOLD and near_threshold_deposits(conn, node) >= MIN_STRUCTURED_DEPOSITS:
            hits.append((node, deg))
    hits.sort(key=lambda t: t[1], reverse=True)
    return hits


def near_threshold_deposits(conn, acct: int) -> int:
    return scalar(
        conn,
        f"SELECT COUNT(*) FROM transactions "
        f"WHERE dst = {acct} AND amount >= 9000 AND amount < {CTR_THRESHOLD:.0f}",
    )


def detect_fan_out(view) -> list[tuple[int, int]]:
    hits = [(n, view.out_degree(n)) for n in view.connected_components()]
    hits = [(n, d) for n, d in hits if d >= FAN_OUT_THRESHOLD]
    hits.sort(key=lambda t: t[1], reverse=True)
    return hits


# ---------------------------------------------------------------------------
# Scoring — fuse the signals into one interpretable risk score per account.
# ---------------------------------------------------------------------------
def score(view, fan_in, fan_out, ring_members, risk, known_bad) -> list[tuple[int, float]]:
    fan_in_set = {n for n, _ in fan_in}
    fan_out_set = {n for n, _ in fan_out}
    max_risk = max(risk.values(), default=0.0) or 1e-12

    out = []
    for node in view.connected_components():
        s = 3.0 * risk.get(node, 0.0) / max_risk
        if node in ring_members:
            s += 3.0
        if node in fan_in_set:
            s += 2.5
        if node in fan_out_set:
            s += 2.0
        if node in known_bad:
            s += 1.0
        if s > 0.5:
            out.append((node, s))
    out.sort(key=lambda t: t[1], reverse=True)
    return out


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def report_structuring(conn, fan_in) -> None:
    section("Detector 1 — Structuring (fan-in of near-threshold deposits)")
    for acct, deg in fan_in:
        sub = near_threshold_deposits(conn, acct)
        total = scalar_str(conn, f"SELECT SUM(amount) FROM transactions WHERE dst = {acct}")
        print(f"  account {acct:>4}: {deg} distinct sources, "
              f"{sub} just-under-threshold deposits, ${total} received")
    print()


def report_layering(rings) -> None:
    section("Detector 2 — Layering (round-trip cycles / SCCs)")
    if not rings:
        print("  (none)")
    for i, ring in enumerate(rings, 1):
        print(f"  ring #{i}: {sorted(ring)}  — funds return to origin")
    print()


def report_fan_out(fan_out) -> None:
    section("Detector 3 — Mule fan-out (distribution hubs)")
    for acct, deg in fan_out:
        print(f"  account {acct:>4}: pays out to {deg} distinct counterparties")
    print()


def report_risk_propagation(risk, known_bad) -> None:
    section("Detector 4 — Risk propagation (personalized PageRank from known-bad)")
    ranked = sorted(((n, r) for n, r in risk.items() if n not in known_bad),
                    key=lambda t: t[1], reverse=True)
    downstream = sum(1 for _, r in ranked if r > 0.0)
    print(f"  seed(s) {known_bad} → risk reached {downstream} downstream accounts. Top exposures:")
    for acct, r in ranked[:6]:
        print(f"  account {acct:>4}: risk {r:.4f}")
    print()


def report_velocity(conn) -> None:
    section("Detector 5 — Velocity (temporal SQL on the same live rows)")
    n = scalar(conn, f"SELECT COUNT(*) FROM transactions "
                     f"WHERE amount >= 9000 AND amount < {CTR_THRESHOLD:.0f}")
    big = scalar(conn, "SELECT COUNT(*) FROM transactions WHERE amount >= 50000")
    print(f"  {n} near-threshold (9,000–10,000) deposits system-wide")
    print(f"  {big} large movements (>= 50,000) consistent with layering\n")


def report_sar(candidates, fan_in, fan_out, ring_members, known_bad) -> None:
    fan_in_set = {n for n, _ in fan_in}
    fan_out_set = {n for n, _ in fan_out}
    section("SAR candidates — fused risk ranking (top 15)")
    print(f"  {'acct':>5}  {'score':>6}  reasons")
    print(f"  {'-' * 5}  {'-' * 6}  {'-' * 40}")
    for acct, sc in candidates[:15]:
        why = []
        if acct in known_bad:
            why.append("known-bad")
        if acct in ring_members:
            why.append("laundering-cycle")
        if acct in fan_in_set:
            why.append("structuring-collector")
        if acct in fan_out_set:
            why.append("mule-fan-out")
        if not why:
            why.append("risk-propagation")
        print(f"  {acct:>5}  {sc:>6.2f}  {', '.join(why)}")
    print()


# ---------------------------------------------------------------------------
# Verification — the planted crimes must all be caught.
# ---------------------------------------------------------------------------
def verify(fan_in, fan_out, ring_members, risk, candidates) -> None:
    fan_in_set = {n for n, _ in fan_in}
    fan_out_set = {n for n, _ in fan_out}
    assert COLLECTOR in fan_in_set, f"structuring collector {COLLECTOR} must be flagged"
    for r in RING:
        assert r in ring_members, f"ring member {r} must be in a laundering cycle"
    assert DISTRIBUTOR in fan_out_set, f"distributor {DISTRIBUTOR} must be flagged"
    seeded = sum(1 for m in FANOUT if risk.get(m, 0.0) > 0.0)
    assert seeded >= 10, f"risk should reach the distributor's mules (got {seeded})"
    top = {acct for acct, _ in candidates[:15]}
    assert COLLECTOR in top and DISTRIBUTOR in top
    assert all(r in top for r in RING)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def scalar(conn, q: str) -> int:
    try:
        return int(scalar_str(conn, q))
    except ValueError:
        return 0


def scalar_str(conn, q: str) -> str:
    row = conn.execute(q).fetchone()
    return str(row[0]) if row and row[0] is not None else ""


def banner(title: str) -> None:
    rule = "=" * 64
    print(f"\n{rule}\n  {title}\n{rule}\n")


def section(title: str) -> None:
    print(f"── {title} ──")


if __name__ == "__main__":
    main()
