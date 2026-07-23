# Exact Decimal Arithmetic

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
