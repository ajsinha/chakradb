#!/usr/bin/env bash
# DuckDB half of the Gate 2 comparison. Same 500k-row CSV, same queries.
# Loads once into a persistent DB, then times each query (median of RUNS).
# Note: .timer only takes effect when the script is fed on stdin, not via -c.
set -e
DUCKDB=${DUCKDB:-/home/ashutosh/duckdb/duckdb}
CSV=${1:-/home/ashutosh/duckdb/hits.csv}
RUNS=${2:-20}
DB=$(mktemp -u /tmp/gate2_XXXX.db)

"$DUCKDB" "$DB" -c "CREATE TABLE hits AS SELECT * FROM read_csv_auto('$CSV');" >/dev/null

run_query () {
  local label="$1"; local sql="$2"
  { printf '.timer on\n'; for i in $(seq 1 "$RUNS"); do printf '%s\n' "$sql"; done; } \
    | "$DUCKDB" "$DB" 2>&1 \
    | grep -oE 'real [0-9.]+' | awk '{print $2}' \
    | sort -n | awk -v l="$label" '{a[NR]=$1} END{printf "| %s | %.3f |\n", l, a[int((NR+1)/2)]*1000}'
}

echo "# DuckDB Gate 2 head-to-head ($($DUCKDB --version))"
echo "| query | DuckDB p50 (ms) |"
echo "|---|---|"
run_query "COUNT(*)"              "SELECT COUNT(*) FROM hits;"
run_query "SUM(a) WHERE a > 500"  "SELECT SUM(a) FROM hits WHERE a > 500;"
run_query "GROUP BY a"            "SELECT a, COUNT(*) FROM hits GROUP BY a;"
run_query "ORDER BY b LIMIT 100"  "SELECT pk FROM hits ORDER BY b DESC LIMIT 100;"
run_query "COUNT(DISTINCT a)"     "SELECT COUNT(DISTINCT a) FROM hits;"
rm -f "$DB"
