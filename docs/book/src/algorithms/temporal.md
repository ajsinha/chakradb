# Temporal Encoding

```{=latex}
\epigraph{Time is the longest distance between two places.}{--- Tennessee Williams}
```

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
prunes via [zonemaps](pruning.md), and a date can be a primary key.

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
