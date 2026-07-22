# ChakraDB for Python

A [PEP 249](https://peps.python.org/pep-0249/) (DB-API 2.0) driver for
[ChakraDB](https://github.com/ajsinha/chakradb) — an embedded HTAP database. It
behaves like the standard library's `sqlite3`: `connect()` → `Connection` →
`Cursor`, with `execute` / `executemany` / `fetchone` / `fetchmany` / `fetchall`,
`description`, `rowcount`, and the standard exception hierarchy.

```python
import chakradb

con = chakradb.connect(":memory:")          # or a directory path for durability
cur = con.cursor()
cur.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT)")
cur.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "alice", 9.5))
cur.execute("SELECT name, score FROM t WHERE id = ?", (1,))
print(cur.fetchone())                        # ('alice', 9.5)  — typed
```

## Durability

A durable database is a **directory** (it holds a write-ahead log, a manifest,
and Arrow parts), not a single file. Pass a path and it is created/opened, and
recovered on reopen:

```python
con = chakradb.connect("./mydb")
con.execute("CREATE TABLE users (email TEXT PRIMARY KEY, age INT)")
con.execute("INSERT INTO users VALUES (?, ?)", ("alice@x.com", 30))
con.close()
# ... later / after a crash ...
con = chakradb.connect("./mydb")
print(con.execute("SELECT age FROM users WHERE email = ?", ("alice@x.com",)).fetchone())
```

Statements auto-commit and are durable when `execute` returns; `commit()` is a
no-op and there are no multi-statement transactions yet.

## Building

```bash
pip install maturin
cd bindings/python
maturin develop            # build + install into the current venv (interpreter SQL)
maturin develop --features datafusion   # + the vectorised analytical engine
pytest tests/
```

## Notes

- **Parameters** use `qmark` style (`?`), like `sqlite3`. Values are
  single-quote-escaped before substitution (the driver has no native bind
  protocol yet), so `?` placeholders are injection-safe.
- **Types** round-trip as Python `int` / `float` / `str` / `None`. The one known
  edge is that a text value equal to the string `"NULL"` is returned as `None`.
- **Concurrency:** the GIL is released during query execution, so multiple Python
  threads can run queries against one process while writes are in flight — the
  HTAP property ChakraDB exists for.
