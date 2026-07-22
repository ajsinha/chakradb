"""PEP 249 (DB-API 2.0) conformance for the ChakraDB Python driver."""

import os
import tempfile

import chakradb
import pytest


def test_module_globals():
    assert chakradb.apilevel == "2.0"
    assert chakradb.threadsafety in (0, 1, 2, 3)
    assert chakradb.paramstyle == "qmark"
    # Exception hierarchy is rooted correctly.
    assert issubclass(chakradb.OperationalError, chakradb.DatabaseError)
    assert issubclass(chakradb.DatabaseError, chakradb.Error)
    assert issubclass(chakradb.IntegrityError, chakradb.DatabaseError)


def test_connect_execute_fetch():
    con = chakradb.connect(":memory:")
    cur = con.cursor()
    cur.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT)")
    assert cur.rowcount == 0
    cur.execute("INSERT INTO t VALUES (?, ?, ?)", (1, "alice", 9.5))
    cur.execute("INSERT INTO t VALUES (?, ?, ?)", (2, "bob", 7.0))
    assert cur.rowcount == 1

    cur.execute("SELECT id, name, score FROM t WHERE id = ?", (1,))
    row = cur.fetchone()
    assert row == (1, "alice", 9.5)          # typed: int, str, float
    assert cur.fetchone() is None
    # description reports column names and DB-API type objects.
    names = [d[0] for d in cur.description]
    assert names == ["id", "name", "score"]
    assert cur.description[0][1] == chakradb.NUMBER
    assert cur.description[1][1] == chakradb.STRING
    con.close()


def test_fetchmany_fetchall_and_iteration():
    con = chakradb.connect()
    con.execute("CREATE TABLE n (k INT PRIMARY KEY)")
    con.executemany("INSERT INTO n VALUES (?)", [(i,) for i in range(10)])
    cur = con.execute("SELECT k FROM n ORDER BY k")
    assert cur.fetchmany(3) == [(0,), (1,), (2,)]
    assert cur.fetchone() == (3,)
    rest = cur.fetchall()
    assert rest[0] == (4,) and rest[-1] == (9,)
    # Iteration protocol.
    cur = con.execute("SELECT k FROM n ORDER BY k")
    assert [r[0] for r in cur] == list(range(10))


def test_aggregates_and_group_by():
    con = chakradb.connect()
    con.execute("CREATE TABLE s (id INT PRIMARY KEY, region TEXT, amt INT)")
    con.executemany(
        "INSERT INTO s VALUES (?, ?, ?)",
        [(1, "w", 10), (2, "e", 20), (3, "w", 5), (4, "e", 7)],
    )
    rows = con.execute(
        "SELECT region, SUM(amt) FROM s GROUP BY region ORDER BY region"
    ).fetchall()
    assert rows == [("e", 27), ("w", 15)]


def test_null_round_trips_as_none():
    con = chakradb.connect()
    con.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    con.execute("INSERT INTO t VALUES (?, ?)", (1, None))
    assert con.execute("SELECT v FROM t WHERE id = 1").fetchone() == (None,)


def test_parameter_binding_escapes_quotes():
    con = chakradb.connect()
    con.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
    tricky = "O'Brien'; DROP TABLE t; --"
    con.execute("INSERT INTO t VALUES (?, ?)", (1, tricky))
    # The value is stored verbatim; the injection attempt did not execute.
    assert con.execute("SELECT s FROM t WHERE id = 1").fetchone() == (tricky,)
    assert con.execute("SELECT COUNT(*) FROM t").fetchone() == (1,)


def test_errors_map_to_dbapi_exceptions():
    con = chakradb.connect()
    con.execute("CREATE TABLE t (id INT PRIMARY KEY)")
    con.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(chakradb.IntegrityError):
        con.execute("INSERT INTO t VALUES (1)")          # duplicate key
    with pytest.raises(chakradb.ProgrammingError):
        con.execute("SELECT nope FROM t")                # unknown column
    with pytest.raises(chakradb.ProgrammingError):
        con.execute("NOT SQL AT ALL")


def test_durability_across_reopen():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "mydb")
        con = chakradb.connect(path)
        con.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        con.execute("INSERT INTO t VALUES (?, ?)", (1, "alice"))
        con.close()
        # Reopen the same directory: data survives.
        con2 = chakradb.connect(path)
        assert con2.execute("SELECT name FROM t WHERE id = 1").fetchone() == ("alice",)
        con2.close()


def test_context_manager():
    with chakradb.connect() as con:
        con.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        con.execute("INSERT INTO t VALUES (1)")
        assert con.execute("SELECT COUNT(*) FROM t").fetchone() == (1,)
