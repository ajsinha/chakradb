#!/usr/bin/env bash
# DuckDB half of the ClickBench-shaped comparison. Reads the SAME CSV the
# `clickbench` bin generated, runs the identical query subset, prints median ms.
#   scripts/clickbench_duckdb.sh <csv> [runs]
set -euo pipefail
DUCKDB=${DUCKDB:-/home/ashutosh/duckdb/duckdb}
CSV=${1:-/tmp/clickbench.csv}
RUNS=${2:-5}
DB=$(mktemp -u /tmp/clickbench_XXXX.db)

"$DUCKDB" "$DB" -c "CREATE TABLE hits AS SELECT * FROM read_csv_auto('$CSV', header=true);" >/dev/null

declare -a Q=(
  "SELECT COUNT(*) FROM hits;"
  "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;"
  "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;"
  "SELECT AVG(UserID) FROM hits;"
  "SELECT COUNT(DISTINCT UserID) FROM hits;"
  "SELECT COUNT(DISTINCT SearchPhrase) FROM hits;"
  "SELECT MIN(EventDate), MAX(EventDate) FROM hits;"
  "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;"
  "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;"
  "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;"
  "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;"
  "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;"
  "SELECT ResolutionWidth, COUNT(*) FROM hits GROUP BY ResolutionWidth ORDER BY COUNT(*) DESC LIMIT 10;"
)
declare -a L=(Q0 Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12)

echo "# DuckDB — ClickBench-shaped ($($DUCKDB --version))"
echo "| query | DuckDB p50 (ms) |"
echo "|---|---|"
for i in "${!Q[@]}"; do
  { printf '.timer on\n'; for _ in $(seq 1 "$RUNS"); do printf '%s\n' "${Q[$i]}"; done; } \
    | "$DUCKDB" "$DB" 2>&1 \
    | grep -oE 'real [0-9.]+' | awk '{print $2}' \
    | sort -n | awk -v l="${L[$i]}" '{a[NR]=$1} END{printf "| %s | %.1f |\n", l, a[int((NR+1)/2)]*1000}'
done
rm -f "$DB"
