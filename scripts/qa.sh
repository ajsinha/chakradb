#!/usr/bin/env bash
# ChakraDB QA runner.
#
#   scripts/qa.sh            # quick: fmt + clippy + build & test both profiles
#   scripts/qa.sh full       # quick + 10k crash trials + benchmarks (+ DuckDB if present)
#   scripts/qa.sh soak       # full + the 6-hour durability soak
#
# "Both profiles" = the default DataFusion build and the lean
# `--no-default-features` interpreter-only build. Everything must be green in both.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-quick}"
DUCKDB="${DUCKDB:-/home/ashutosh/duckdb/duckdb}"
HITS="${HITS:-/home/ashutosh/duckdb/hits.csv}"

step() { printf '\n\033[1;36m==== %s ====\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m✓ %s\033[0m\n' "$*"; }

# ---- static checks -------------------------------------------------------
step "rustfmt (check only)"
cargo fmt --check
ok "formatting clean"

step "clippy — lean profile (deny warnings)"
cargo clippy --no-default-features --all-targets -- -D warnings
ok "clippy lean clean"

step "clippy — default profile (deny warnings)"
cargo clippy --all-targets -- -D warnings
ok "clippy default clean"

# ---- correctness: both profiles -----------------------------------------
step "test — lean (interpreter-only, no heavy deps)"
cargo test --no-default-features
ok "lean suite green"

step "test — default (HTAP router: interpreter + DataFusion)"
cargo test
ok "default suite green"

if [ "$MODE" = "quick" ]; then
  printf '\n\033[1;32mQUICK QA PASSED\033[0m\n'
  exit 0
fi

# ---- durability: the M1-1 crash criterion --------------------------------
step "crash-consistency — 10,000 randomized crash trials (M1-1)"
CHAKRA_CRASH_TRIALS=10000 cargo test --release --test crash_consistency -- --nocapture
ok "10k crash trials verified"

# ---- benchmarks (informational; printed for inspection) ------------------
step "benchmarks — M0 / M1 / M2 acceptance"
cargo run --release --bin m0-bench | tail -20
cargo run --release --bin m1-bench | tail -20
cargo run --release --bin m2-bench | tail -20

# ---- head-to-head vs DuckDB, if available --------------------------------
if [ -x "$DUCKDB" ] && [ -f "$HITS" ]; then
  step "Gate 2 — ChakraDB interpreter vs DuckDB (500k rows)"
  cargo run --release --bin gate2-bench -- "$HITS" 20
  bash scripts/gate2_duckdb.sh "$HITS" 20
  step "Gate 2 — ChakraDB + DataFusion vs DuckDB"
  cargo run --release --features datafusion --bin df-bench -- "$HITS" 20
  ok "DuckDB comparison done"
else
  printf '\033[1;33m(skipping DuckDB comparison: set DUCKDB=... and HITS=... to enable)\033[0m\n'
fi

if [ "$MODE" != "soak" ]; then
  printf '\n\033[1;32mFULL QA PASSED\033[0m\n'
  exit 0
fi

# ---- the long durability soak (M1-3) -------------------------------------
step "soak — 6 hours of continuous write+compact+scan (M1-3)"
CHAKRA_SOAK_SECS=21600 cargo test --release --test soak -- --nocapture
printf '\n\033[1;32mSOAK QA PASSED\033[0m\n'
