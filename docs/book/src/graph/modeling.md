# Modeling a Property Graph

A property graph is **nodes and edges**, each with a label/type and properties. On
ChakraDB it maps to ordinary tables — with one twist that makes traversal fast.

## The tables

```sql
-- Nodes: keyed by node id → fast point lookup and id-range scan.
CREATE TABLE nodes (
  id     INT PRIMARY KEY,
  label  VARCHAR(64),
  props  TEXT               -- JSON, or promote hot properties to real columns
);

-- Edges: keyed (src, dst) src-major → clustered adjacency by source.
CREATE TABLE edges (
  key    INT PRIMARY KEY,   -- encode(src, dst); see Clustered Adjacency
  src    INT,
  dst    INT,
  type   VARCHAR(32),
  weight DOUBLE
);
```

The `Graph` handle manages the edge encoding for you; the schema above is what it
creates under the hood (`{name}_edges`). A companion `nodes` table for node
properties is optional in v1 — nodes are otherwise implied by the ids that appear
in edges.

## Hot vs. cold properties

Put **hot** edge properties — the ones you filter on — in real typed columns.
They get [zonemaps](../algorithms/pruning.md), so a query like "follow-edges created
after T" prunes:

```sql
SELECT dst FROM edges
WHERE key >= (X<<32) AND key < ((X+1)<<32)   -- neighbors of X (clustered)
  AND type = 'follows' AND weight > 0.5;      -- pruned by zonemaps
```

Put **cold / sparse** properties in a `props` blob. This is the classic
wide-vs-narrow trade: typed columns for what you query, a blob for the long tail.

## Directed vs. undirected

Edges are stored **directed**. Model an undirected edge by inserting both `(u,v)`
and `(v,u)`, or let the [CSR builder](csr.md) symmetrize for undirected algorithms
(connected components, triangles do this internally). For fast **backward**
traversal, keep a second table keyed `(dst, src)` — the reverse adjacency.

## Transactions keep it consistent

Adding an edge that touches multiple tables (an edge row, a reverse-edge row, a
degree counter) belongs in **one transaction**, so a reader never sees half an
edge:

```sql
BEGIN;
  INSERT INTO edges VALUES (encode(1,2), 1, 2, 'follows', 1.0);
  INSERT INTO edges_rev VALUES (encode(2,1), 1, 2);
COMMIT;
```

## What's *not* modeled

- **Composite / multi-column keys** — not yet native, hence the `(src,dst)`
  encoding (the roadmap's top item).
- **Foreign keys** — an explicit non-goal; referential integrity between nodes and
  edges is the application's responsibility.
- **Node ids beyond 2³¹** — the packed-key encoding's current ceiling.

The next chapters — [Clustered Adjacency](adjacency.md) and [The CSR
Snapshot](csr.md) — show why this simple mapping traverses fast.
