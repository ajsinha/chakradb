# Exact Decimal Arithmetic

```{=latex}
\epigraph{Round numbers are always false.}{--- Samuel Johnson}
```

Money must not round. ChakraDB's `DECIMAL(p, s)` is exact fixed-point — an `i128`
unscaled mantissa with a scale, backed by Arrow `Decimal128`, never `f64`. This
chapter is how values are parsed, compared, and summed without ever touching a
float.

## Representation

A decimal is `(mantissa, scale)`: the number is `mantissa / 10^scale`. So `12.34` is
`Decimal(1234, 2)`, and `-0.01` is `Decimal(-1, 2)`. `DECIMAL(p, s)` bounds the
**precision** `p` (total significant digits, `p ≤ 38` for `i128`) and the **scale**
`s`. Values are stored at the column's scale.

## Exact parsing from text

The exactness starts at ingestion: a decimal literal is parsed from its **source
text**, never via `f64`. `9.99` becomes `Decimal(999, 2)` directly — not the nearest
double `9.9900000000000002…` rounded back.

> **ALGORITHM 13 — Parse and fit a decimal literal**
> ```text
> Input:  the literal's text t; target type DECIMAL(p, s)
> Output: Decimal(m, s), or REJECT
> 1  (digits, point) ← split t on '.'                  ▷ integer part, fraction
> 2  from ← len(fraction);  raw ← integer ++ fraction as i128   ▷ exact, base-10
> 3  m ← Rescale(raw, from, s)                          ▷ align to the column scale
> 4  if |m| ≥ 10^p: REJECT                              ▷ exceeds declared precision
> 5  return Decimal(m, s)
> ```

Line 4 enforces precision: `1000` into `DECIMAL(3,0)` (max `999`) is rejected, not
silently stored — a real correctness guard for money.

Rescaling to a target scale is exact when growing, and rounds half-away-from-zero
when shrinking (SQL `CAST` rounding):

> ```text
> Rescale(m, from, to):
>   if to = from: return m
>   if to > from: return m · 10^(to−from)              ▷ exact
>   div ← 10^(from−to);  half ← div/2                   ▷ shrinking → round
>   return (m + sign(m)·half) / div
> ```

## Exact comparison

Two decimals of different scales are compared by aligning to the larger scale in
`i128` — never through `f64`, which would conflate values a distributed ledger must
distinguish:

> ```text
> cmp(Decimal(a, sa), Decimal(b, sb)):
>   s ← max(sa, sb)
>   return compare_i128( a·10^(s−sa),  b·10^(s−sb) )    ▷ exact; f64 only on overflow
> ```

So `10.00` and `10.0` and the integer `10` all compare equal, exactly. Decimals also
order correctly against integers (widened to scale 0).

## Exact aggregation and arithmetic

`SUM` of a decimal column accumulates the `i128` mantissa at the shared scale and
stays exact — the interpreter's accumulator keeps a running `i128` while every value
is a decimal of one scale (falling back to `f64` only on a genuine mix). `MIN`/`MAX`
are exact via the comparison above. Arithmetic:

- `+` / `−` — align scales, add/subtract mantissas (checked; `f64` only on
  `i128` overflow).
- `×` — multiply mantissas, add scales: `Decimal(a, sa) × Decimal(b, sb) =
  Decimal(a·b, sa+sb)`.
- `÷`, `%` — fall back to `f64` (exact decimal division has an
  implementation-defined scale; documented as the one approximate operator).
- `AVG` — returns a float (a mean is inherently fractional).

> **Proposition 9 (Exactness of storage, comparison, and sum).** For decimals that
> fit `DECIMAL(p, s)`, round-trip storage, comparison, and `SUM` introduce no error.
>
> *Proof sketch.* Storage keeps the exact base-10 mantissa (ALG 13, parsed from
> text, no `f64`). Comparison aligns scales by exact `i128` multiplication.
> Summation adds exact `i128` mantissas at a common scale. None of these operations
> uses floating point, so no rounding occurs. The famous witness — `0.1 + 0.2`,
> which is `0.30000000000000004` in `f64` — stores and sums to exactly `0.3`
> (`tests/decimal.rs`). ∎

## Where the float boundary is

The only places a decimal meets `f64` are division/modulo, `AVG`, and an explicit
cast to `FLOAT`. Everywhere a ledger cares — INSERT, comparison, `SUM`, `MIN`/`MAX`,
`+`/`−`/`×` — the arithmetic is exact `i128`. That boundary is stated plainly so no
one is surprised by a rounded cent.

## Temporal Encoding


`DATE` and `TIMESTAMP` are **logical types over integer storage**: a date is a count
of days since the Unix epoch, a timestamp a count of microseconds. That choice keeps
comparison, zonemaps, keys, and MVCC working unchanged on integers, while exposing
the columns to Arrow (and DataFusion) as their native temporal types.

## The representation

| Type | Stored as | Arrow type | Literal |
|---|---|---|---|
| `DATE` | `i64` days since 1970-01-01 | `Date32` | `'YYYY-MM-DD'` or `DATE '…'` |
| `TIMESTAMP` | `i64` µs since the epoch | `Timestamp(µs)` | `'YYYY-MM-DD[ T]HH:MM:SS[.ffffff]'` |

Because the physical value is an integer, a `DATE` column sorts, prunes, and
compares exactly like an integer column — a date range `WHERE d >= '2024-01-01'`
prunes via [zonemaps](storage.md), and a date can be a primary key.

## Civil ↔ days

The conversion between a calendar date and a day number uses Howard Hinnant's
proleptic-Gregorian algorithm — exact, branch-light, and dependency-free (the core
crate forbids `unsafe` and has no `chrono`).

> **ALGORITHM 14 — Days from a civil date**
> ```text
> Input:  year y, month m, day d
> Output: days since 1970-01-01
> 1  y ← y − (m ≤ 2 ? 1 : 0)                            ▷ March-based year
> 2  era ← (y ≥ 0 ? y : y−399) / 400
> 3  yoe ← y − era·400                                   ▷ year of era  [0,399]
> 4  doy ← (153·(m > 2 ? m−3 : m+9) + 2)/5 + d − 1       ▷ day of year  [0,365]
> 5  doe ← yoe·365 + yoe/4 − yoe/100 + doy               ▷ day of era   [0,146096]
> 6  return era·146097 + doe − 719468
> ```

The inverse (`days → (y, m, d)`) is the mirror image and is used for rendering. A
timestamp splits into `days · 86_400_000_000 + micros_of_day`; rendering uses
floored division so pre-epoch instants format correctly.

## Parsing is bounded

A hostile literal must not overflow the integer math. Parsing bounds the year and
uses checked arithmetic, so an out-of-range date is *rejected*, never wrapped:

> ```text
> parse_date(t):
>   (y, m, d) ← split t on '-'
>   if |y| > 262143 or m ∉ [1,12] or d ∉ [1,31]: REJECT   ▷ bounded → no overflow
>   return days_from_civil(y, m, d)
> parse_timestamp(t):
>   days ← parse_date(date_part);  µs ← parse_time(time_part)
>   return checked(days · 86_400_000_000 + µs)            ▷ None on overflow → REJECT
> ```

The bound (`|year| ≤ 262143`) comfortably covers the range Arrow `Timestamp` can
represent and keeps every downstream multiplication inside `i64`. Before this bound,
`DATE '300000-01-01'` overflowed — a debug panic, and worse, a silent wrap to a
garbage epoch in release. Now it errors cleanly.

## Rendering, consistently across engines

A `DATE`/`TIMESTAMP` column stores an integer, but must *display* as a date string.
Two paths render it, and they agree:

- **DataFusion** reads the native `Date32`/`Timestamp` Arrow array and renders it.
- **The interpreter** threads each projection's output type so a bare temporal
  column renders through the civil-from-days formatter, not as a raw integer.

So `SELECT hire_date FROM emp` shows `2024-01-15` whether the query ran on the
interpreter (a point lookup) or on DataFusion (a scan) — the tests assert both
paths, and a hostile literal errors instead of panicking.

## Why logical-over-integer

> **Proposition 10 (Temporal correctness is inherited).** Ordering, zonemap
> pruning, and MVCC visibility are correct for `DATE`/`TIMESTAMP` with no new code.
>
> *Proof sketch.* Each is defined on the physical value. Dates/timestamps are stored
> as monotonically-increasing integers (a later instant is a larger integer), so
> integer ordering *is* chronological ordering; the zonemap min/max are the
> earliest/latest instants; and visibility compares CSNs, untouched by the column
> type. The only temporal-specific code is text↔integer conversion at the parse and
> render boundaries (ALG 14). ∎
