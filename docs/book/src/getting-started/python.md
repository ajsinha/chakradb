# Python in Five Minutes

```{=latex}
\epigraph{Simple things should be simple, complex things should be possible.}{--- Alan Kay}
```

The Python bindings wrap the exact same engine as the Rust core. The SQL surface
is exposed through a standard [PEP 249 (DB-API 2.0)](../guide/python-driver.md)
driver, so if you have used `sqlite3` you already know the shape of it.

## Connect, execute, fetch

```python
import chakradb

conn = chakradb.connect(":memory:")          # or a directory path for durability
cur = conn.execute("""
    CREATE TABLE accounts (
        id      INTEGER PRIMARY KEY,
        owner   VARCHAR(64) NOT NULL,
        balance DECIMAL(14,2) NOT NULL DEFAULT 0.00
    )""")

conn.execute("INSERT INTO accounts VALUES (1, 'ada', 500.00)")
conn.execute("INSERT INTO accounts VALUES (2, 'grace', 1250.50)")
conn.commit()

for row in conn.execute("SELECT id, owner, balance FROM accounts ORDER BY id"):
    print(row)          # (1, 'ada', '500.00')  ...
```

`connect`, `Connection.execute`, `Cursor.fetchone/fetchmany/fetchall`,
`commit`, `rollback`, and the full [exception hierarchy](../reference/errors.md)
(`IntegrityError`, `ProgrammingError`, `OperationalError`, …) all behave exactly
as PEP 249 specifies.

## Parameters and transactions

Parameters use the `qmark` style; transactions are explicit around a unit of
work:

```python
conn.execute("INSERT INTO accounts VALUES (?, ?, ?)", (3, 'lin', 42.00))

try:
    conn.execute("BEGIN")  # or rely on autocommit-off via .execute + .commit
    conn.execute("INSERT INTO accounts VALUES (10, 'noether', 300.00)")
    conn.execute("INSERT INTO accounts VALUES (11, 'hopper', 300.00)")
    conn.commit()
except chakradb.IntegrityError as e:
    conn.rollback()
    print("rejected:", e)
```

Type and constraint violations surface as `IntegrityError`; malformed SQL as
`ProgrammingError`; a write–write conflict as `OperationalError`.

## The graph engine, from Python

Any table can be viewed as a graph. `conn.graph(name)` returns a handle backed
by table `name` in the same database; edges are ordinary transactional rows:

```python
g = conn.graph("payments")
g.add_edges([(1, 2, 100.0), (2, 3, 250.0), (3, 1, 90.0)])   # (src, dst, weight)

view = g.view()                        # one consistent snapshot for analytics
print(view.laundering_cycles())        # [[1, 2, 3]] — a round-trip ring
print(view.pagerank())                 # {1: 0.33, 2: 0.33, 3: 0.33}
print(view.personalized_pagerank(seeds=[1]))
```

Every algorithm returns plain Python `dict`/`list` values keyed by node id. The
full catalogue is in [A Graph in Five Minutes](graph.md) and
[Graph Algorithms](../graph/algorithms.md).

## A complete application

`examples/aml_app.py` is a full real-time anti-money-laundering system in ~300
lines of Python: it builds a synthetic payment network, runs an ensemble of
graph detectors, and ranks suspicious accounts. Walk through it in the
[Real-Time AML case study](../case-studies/aml.md).
