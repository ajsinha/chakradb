# ClickBench-shaped Validation

**What:** the arbitrary-schema engine loads a **105-column** analytical table and
DataFusion runs the standard ClickBench query subset — validated for correctness
and performance against DuckDB on identical input.

**Honest scope:** **synthetic data** over the real ClickBench `hits` schema and
real query shapes, at a **scaled-down** row count (1M, not the official 100M).
This validates capability (a wide table loads and queries) and *relative*
performance; it is **not** an official ClickBench submission. Harness:
`src/bin/clickbench.rs` + `scripts/clickbench_duckdb.sh`, run on identical CSV.

---

## Correctness — identical to DuckDB

Every checked query returns the **same result** as DuckDB on the same CSV,
including exact floats:

| query | ChakraDB + DataFusion | DuckDB |
|---|---|---|
| `COUNT(*)` | 1,000,000 | 1,000,000 |
| `COUNT(*) WHERE AdvEngineID<>0` | 99,661 | 99,661 |
| `SUM/COUNT/AVG` | 299101 / 1000000 / 1279.702063 | identical |
| `AVG(UserID)` | 49996.788803 | identical |
| `COUNT(DISTINCT UserID)` | 99,993 | 99,993 |
| `MIN/MAX(EventDate)` | 19000 / 19364 | identical |

## Performance — 1M rows, 105 columns (median of 5)

| query | ChakraDB+DF | DuckDB |
|---|---|---|
| Q0 `COUNT(*)` | 0.0 ms | 0.0 ms |
| Q1 filtered count | 1.9 | 1.0 |
| Q2 sum/avg | 1.9 | 1.0 |
| Q3 avg | 1.6 | 1.0 |
| Q4 `COUNT(DISTINCT UserID)` | 10.2 | 10.0 |
| Q5 `COUNT(DISTINCT SearchPhrase)` | 5.0 | 8.0 |
| Q6 min/max date | 2.8 | 0.0 |
| Q7 group by adv engine | 6.6 | 1.0 |
| Q8 region × distinct users | 21.7 | 20.0 |
| Q9 top phrases | 11.3 | 7.0 |
| Q10 top users | 16.9 | 13.0 |
| Q11 phrase by time | 9.5 | 6.0 |
| Q12 top widths | 8.7 | 1.0 |

**Read:** mostly within ~1–2× of DuckDB, faster on Q5, tie on several; a couple of
simple `GROUP BY`s (Q7, Q12) are where DuckDB's maturity shows most. On the axis
this validates — *a wide analytical table, queried correctly and competitively* —
the arbitrary-schema + DataFusion stack holds up.

## Notes / residuals

- **Load was 18.9 s for 1M rows** (row-at-a-time `INSERT` building `Vec<Value>`
  then sealing to Arrow). Bulk ingest is the write-path item on the roadmap.
- A DataFusion gotcha surfaced and was fixed: it lowercases unquoted identifiers,
  so CamelCase columns (`AdvEngineID`) failed to resolve. The bridge now disables
  identifier normalization, matching DuckDB's case-insensitive behavior for
  exact-case queries.
- Empty CSV fields are loaded as `NULL` (matching DuckDB's `read_csv`), so the two
  engines agree on `<> ''` filters and `COUNT(DISTINCT)`.
