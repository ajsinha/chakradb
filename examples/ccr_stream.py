#!/usr/bin/env python3
"""
Real-time Counterparty Credit & Market Risk in Python — event-driven.
=====================================================================

The Python mirror of ``examples/ccr_pipeline.rs``. A streaming market-data feed
drives real-time **exposure, limit, VaR, and systemic-risk** calculations,
reacting to each price tick through ``conn.on_change`` and the built-in graph
engine — including ``view.eisenberg_noe`` for default-cascade contagion.

  * **T0 (per tick):** reprice the affected book, update each counterparty's
    netted current exposure, check single-name limits.
  * **T2 (periodic):** historical-simulation VaR, plus the counterparty exposure
    network — ``pagerank`` (systemic importance), ``eisenberg_noe`` (default
    cascade), ``laundering_cycles`` (circular exposures).

Run:  python examples/ccr_stream.py
"""

from __future__ import annotations

import os
import sys
import threading
from collections import deque

try:
    import chakradb
except ModuleNotFoundError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                    "..", "bindings", "python", "python"))
    import chakradb

# --- Scenario layout --------------------------------------------------------
HUB, WEAK = 1, 2                 # systemic hub; thinly-capitalised cascade origin
CHAIN = [3, 4]                   # dragged under by WEAK
RING = [6, 7, 8]                 # circular exposures
CONC = HUB                       # also holds a concentrated trade
PORTFOLIO = 0
INSTR_CONC, INSTR_VOL = 101, 105

N_TICKS = 15_000
T2_INTERVAL = 3_000
VAR_WINDOW = 512
VAR_LIMIT = 2_000_000.0
CONC_LIMIT = 3_000_000.0


class Rng:
    def __init__(self, seed): self.s = seed & 0xFFFFFFFFFFFFFFFF
    def nxt(self):
        self.s = (self.s + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
        z = self.s
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
        return z ^ (z >> 31)
    def rng(self, lo, hi): return lo + self.nxt() % (hi - lo)


class RiskWorker:
    """T0 per-tick exposure/limits/VaR-window; T2 systemic-risk pass."""

    def __init__(self, conn, exposures, trades, external_assets):
        self.conn = conn
        self.exposures = exposures            # the counterparty exposure graph
        self.external_assets = external_assets
        self.trades = trades                  # list of dicts
        self.by_instrument = {}
        for i, t in enumerate(trades):
            self.by_instrument.setdefault(t["instrument"], []).append(i)
        self.mtm = [0.0] * len(trades)
        self.cp_exposure = {}
        self.prices = {i: 100.0 for i in range(101, 106)}
        self.limits = {CONC: CONC_LIMIT}
        self.pnl_window = deque(maxlen=VAR_WINDOW)
        self.alerts: dict[int, set[str]] = {}
        self.seen = 0
        self.alert_id = 0
        self.lock = threading.Lock()

    def on_change(self, old, new):
        if new is None:
            return
        self.on_tick(int(new["instrument"]), float(new["price"]))
        self.seen += 1
        if self.seen % T2_INTERVAL == 0:
            self.global_pass()

    def on_tick(self, instrument, price):
        old = self.prices.get(instrument, 100.0)
        idxs = self.by_instrument.get(instrument)
        if not idxs:
            self.prices[instrument] = price
            return
        notional_sum = sum(self.trades[i]["notional"] for i in idxs)
        self.pnl_window.append((price - old) * notional_sum)
        for i in idxs:
            t = self.trades[i]
            new_mtm = t["notional"] * (price - t["trade_price"])
            self.cp_exposure[t["counterparty"]] = \
                self.cp_exposure.get(t["counterparty"], 0.0) + (new_mtm - self.mtm[i])
            self.mtm[i] = new_mtm
        self.prices[instrument] = price
        for i in idxs:
            cp = self.trades[i]["counterparty"]
            ce = max(self.cp_exposure.get(cp, 0.0), 0.0)
            if cp in self.limits and ce > self.limits[cp]:
                self.alert(cp, "single-name-limit-breach")

    def global_pass(self):
        if len(self.pnl_window) >= 32:
            losses = sorted((-p for p in self.pnl_window), reverse=True)
            var = max(losses[int(len(losses) * 0.01)], 0.0)
            if var > VAR_LIMIT:
                self.alert(PORTFOLIO, "portfolio-VaR-breach")
        view = self.exposures.view()
        pr = view.pagerank()
        if pr:
            self.alert(max(pr, key=pr.get), "systemically-important")
        clearing = view.eisenberg_noe(self.external_assets)
        for cp in clearing["defaulted"]:
            self.alert(cp, "default-cascade")
        for ring in view.laundering_cycles():
            for cp in ring:
                self.alert(cp, "circular-exposure")

    def alert(self, entity, kind):
        kinds = self.alerts.setdefault(entity, set())
        if kind not in kinds:
            kinds.add(kind)
            with self.lock:
                self.alert_id += 1
                self.conn.execute(
                    f"INSERT INTO risk_alerts VALUES ({self.alert_id}, {entity}, '{kind}')")


def main() -> None:
    rule = "=" * 64
    print(f"\n{rule}\n  ChakraDB — Real-Time CCR & Market Risk (Python)\n{rule}\n")
    conn = chakradb.connect(":memory:")
    conn.execute("""CREATE TABLE market_data (tick_id INTEGER PRIMARY KEY,
        instrument INTEGER, price DECIMAL(12,4), ts TIMESTAMP)""")
    conn.execute("CREATE TABLE risk_alerts (id INTEGER PRIMARY KEY, entity INTEGER, kind VARCHAR(40))")

    # Structural setup: interbank exposure network + external assets + trades.
    external_assets = {HUB: 5_000_000.0, WEAK: 10_000.0, CHAIN[0]: 20_000.0,
                       CHAIN[1]: 20_000.0, 5: 2_000_000.0,
                       RING[0]: 2_000_000.0, RING[1]: 2_000_000.0, RING[2]: 2_000_000.0}
    exposures = conn.graph("exposure_net")
    for cp in [WEAK, CHAIN[0], CHAIN[1], 5, *RING]:
        exposures.add_edge(cp, HUB, 200_000.0)           # everyone owes the hub
    exposures.add_edge(WEAK, CHAIN[0], 100_000.0)         # cascade chain
    exposures.add_edge(CHAIN[0], CHAIN[1], 100_000.0)
    exposures.add_edge(RING[0], RING[1], 50_000.0)        # circular exposure
    exposures.add_edge(RING[1], RING[2], 50_000.0)
    exposures.add_edge(RING[2], RING[0], 50_000.0)

    trades = [
        {"counterparty": CONC, "instrument": INSTR_CONC, "notional": 100_000.0, "trade_price": 100.0},
        {"counterparty": 5, "instrument": INSTR_VOL, "notional": 150_000.0, "trade_price": 100.0},
        {"counterparty": RING[0], "instrument": 102, "notional": 5_000.0, "trade_price": 100.0},
        {"counterparty": WEAK, "instrument": 104, "notional": 5_000.0, "trade_price": 100.0},
    ]

    worker = RiskWorker(conn, exposures, trades, external_assets)
    done = threading.Event()

    def react(old, new):
        worker.on_change(old, new)
        if worker.seen >= N_TICKS:
            done.set()

    conn.on_change("market_data", react)

    # Stream the market-data feed with risks injected.
    rng = Rng(0x00A471CE2026)
    prices = {i: 100.0 for i in range(101, 106)}
    tick_id = [1]

    def emit(instr, price):
        conn.execute(f"INSERT INTO market_data VALUES ({tick_id[0]}, {instr}, {price:.4f}, "
                     f"'2026-03-01 09:00:00')")
        tick_id[0] += 1

    for _ in range(N_TICKS):
        instr = 101 + rng.rng(0, 5)
        if instr == INSTR_CONC:
            prices[instr] += 0.05                          # ramp → limit breach
        elif instr == INSTR_VOL:
            prices[instr] = max(prices[instr] + rng.rng(-25, 26), 1.0)  # swings → VaR
        else:
            prices[instr] += (rng.rng(-1, 2)) * 0.1
        emit(instr, prices[instr])

    done.wait(timeout=60)
    worker.global_pass()

    print(f"Worker reacted to {worker.seen} ticks; persisted {worker.alert_id} alerts.\n")
    print("── Risk alerts (live over the tick stream) ──")
    for entity in sorted(worker.alerts):
        who = "portfolio" if entity == PORTFOLIO else f"cp {entity}"
        print(f"  {who:<14}: {', '.join(sorted(worker.alerts[entity]))}")

    def has(e, kind):
        return any(kind in k for k in worker.alerts.get(e, set()))
    assert has(CONC, "limit"), "limit breach must fire"
    assert has(PORTFOLIO, "VaR"), "VaR breach must fire"
    assert has(HUB, "systemic"), "hub must be systemic"
    assert has(WEAK, "cascade") and all(has(c, "cascade") for c in CHAIN), "cascade must propagate"
    assert all(has(r, "circular") for r in RING), "circular exposures must be flagged"

    print(f"\n{rule}\n  All injected risks detected live — Python CCR pipeline verified.\n{rule}\n")
    conn.close()


if __name__ == "__main__":
    main()
