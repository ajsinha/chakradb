"""ChakraDB — a Python DB-API 2.0 (PEP 249) driver.

Works like ``sqlite3``: ``connect()`` returns a :class:`Connection`, which yields
:class:`Cursor` objects you ``execute()`` on and ``fetch*`` from.

    import chakradb
    con = chakradb.connect(":memory:")          # or a directory path for durability
    cur = con.cursor()
    cur.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    cur.execute("INSERT INTO t VALUES (?, ?)", (1, "alice"))
    cur.execute("SELECT name FROM t WHERE id = ?", (1,))
    print(cur.fetchone())                        # ('alice',)

A durable database is a *directory* (it holds a WAL, manifest, and parts), not a
single file — pass a path and it is created/opened and recovered on reopen.
Statements auto-commit (each is durable on return); ``commit()`` is a no-op and
there are no multi-statement transactions yet.
"""

from __future__ import annotations

import datetime as _dt
import time as _time
from collections.abc import Sequence

from . import _core

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "Date",
    "Time",
    "Timestamp",
    "Binary",
    "STRING",
    "NUMBER",
    "DATETIME",
    "ROWID",
]

# ---- PEP 249 module globals ------------------------------------------------
apilevel = "2.0"
#: 1 = threads may share the module, but not connections.
threadsafety = 1
#: Parameters are marked with a question mark, like sqlite3's default.
paramstyle = "qmark"


# ---- Exception hierarchy (PEP 249 §Exceptions) -----------------------------
class Warning(Exception):
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# Native error categories → DB-API exception classes.
_CORE_MAP = {
    _core.IntegrityError: IntegrityError,
    _core.ProgrammingError: ProgrammingError,
    _core.OperationalError: OperationalError,
}


def _translate(exc: BaseException) -> BaseException:
    return _CORE_MAP.get(type(exc), DatabaseError)(str(exc))


# ---- DB-API type objects ---------------------------------------------------
class _TypeSet(frozenset):
    def __eq__(self, other):
        return other in self

    def __ne__(self, other):
        return other not in self

    __hash__ = frozenset.__hash__


STRING = _TypeSet(["T"])
NUMBER = _TypeSet(["I", "R"])
DATETIME = _TypeSet([])
ROWID = _TypeSet(["I"])



def Date(year, month, day):
    return _dt.date(year, month, day)


def Time(hour, minute, second):
    return _dt.time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    return _dt.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    return Date(*_time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(*_time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(*_time.localtime(ticks)[:6])


def Binary(x):
    return bytes(x)


# ---- qmark parameter binding ----------------------------------------------
def _render(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, bytes):
        return "'" + v.decode("latin-1").replace("'", "''") + "'"
    # Strings and everything else: single-quote and escape embedded quotes.
    return "'" + str(v).replace("'", "''") + "'"


def _bind(sql: str, params) -> str:
    """Substitute qmark (?) placeholders that lie outside string literals."""
    if not params:
        if "?" in _strip_strings(sql):
            raise ProgrammingError("statement expects parameters but none supplied")
        return sql
    if not isinstance(params, Sequence) or isinstance(params, (str, bytes)):
        raise ProgrammingError("parameters must be a sequence")

    out = []
    it = iter(params)
    in_str = False
    i = 0
    n = 0
    used = 0
    total = len(params)
    while i < len(sql):
        c = sql[i]
        if c == "'":
            # Toggle, honouring the '' escape for a literal quote.
            if in_str and i + 1 < len(sql) and sql[i + 1] == "'":
                out.append("''")
                i += 2
                continue
            in_str = not in_str
            out.append(c)
        elif c == "?" and not in_str:
            try:
                val = next(it)
            except StopIteration:
                raise ProgrammingError("not enough parameters for statement")
            out.append(_render(val))
            used += 1
        else:
            out.append(c)
        i += 1
        n += 1
    if used != total:
        raise ProgrammingError(
            f"statement used {used} of {total} supplied parameters"
        )
    return "".join(out)


def _strip_strings(sql: str) -> str:
    out = []
    in_str = False
    i = 0
    while i < len(sql):
        c = sql[i]
        if c == "'":
            if in_str and i + 1 < len(sql) and sql[i + 1] == "'":
                i += 2
                continue
            in_str = not in_str
        elif not in_str:
            out.append(c)
        i += 1
    return "".join(out)


# ---- Cursor ----------------------------------------------------------------
class Cursor:
    def __init__(self, connection: "Connection"):
        self.connection = connection
        self.arraysize = 1
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self._rows: list = []
        self._pos = 0
        self._closed = False

    def _check(self):
        if self._closed:
            raise ProgrammingError("cursor is closed")
        self.connection._check()

    def execute(self, operation: str, parameters=()):
        self._check()
        self.connection._maybe_begin(operation)
        bound = _bind(operation, parameters)
        try:
            cols, types, rows, rowcount, is_query = self.connection._conn.execute(bound)
        except Exception as exc:  # native errors → DB-API hierarchy
            raise _translate(exc) from None
        if is_query:
            # PEP 249: the type_code is a value that compares equal to a module
            # type object (STRING/NUMBER/...). We use the raw type char, and the
            # type objects' __eq__ matches it.
            self.description = [
                (name, ty, None, None, None, None, None)
                for name, ty in zip(cols, types)
            ]
            self._rows = rows
            self.rowcount = len(rows)
        else:
            self.description = None
            self._rows = []
            self.rowcount = rowcount
        self._pos = 0
        return self

    def executemany(self, operation: str, seq_of_parameters):
        self._check()
        total = 0
        for params in seq_of_parameters:
            self.execute(operation, params)
            if self.rowcount and self.rowcount > 0:
                total += self.rowcount
        self.rowcount = total
        self._rows = []
        self.description = None
        return self

    def fetchone(self):
        self._check()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size=None):
        self._check()
        if size is None:
            size = self.arraysize
        end = min(self._pos + size, len(self._rows))
        out = self._rows[self._pos : end]
        self._pos = end
        return out

    def fetchall(self):
        self._check()
        out = self._rows[self._pos :]
        self._pos = len(self._rows)
        return out

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def close(self):
        self._closed = True
        self._rows = []

    # No-ops required by the DB-API.
    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ---- Connection ------------------------------------------------------------
class Connection:
    # PEP 249 exposes the exception classes on the connection too.
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    def __init__(self, database: str = ":memory:", autocommit: bool = True):
        try:
            self._conn = _core.Connection(database)
        except Exception as exc:
            raise _translate(exc) from None
        self._closed = False
        #: When False, statements run inside a transaction that ``commit()`` /
        #: ``rollback()`` control (PEP 249 style). When True (the default,
        #: ChakraDB's HTAP fast path), statements auto-commit and analytical
        #: reads use the vectorised engine.
        self.autocommit = autocommit

    def _check(self):
        if self._closed:
            raise ProgrammingError("connection is closed")

    _TXN_KEYWORDS = ("begin", "start", "commit", "rollback", "end")

    def _maybe_begin(self, sql: str):
        """In transaction mode, open a transaction before the first statement."""
        if self.autocommit:
            return
        first = ""
        stripped = sql.lstrip()
        if stripped:
            first = stripped.split(None, 1)[0].lower()
        if first in self._TXN_KEYWORDS:
            return  # explicit transaction control; don't auto-begin
        if not self._conn.in_transaction():
            try:
                self._conn.begin()
            except Exception as exc:
                raise _translate(exc) from None

    def cursor(self) -> Cursor:
        self._check()
        return Cursor(self)

    def execute(self, operation: str, parameters=()) -> Cursor:
        """Convenience: create a cursor, execute, and return it (like sqlite3)."""
        return self.cursor().execute(operation, parameters)

    def executemany(self, operation: str, seq_of_parameters) -> Cursor:
        return self.cursor().executemany(operation, seq_of_parameters)

    def commit(self):
        self._check()
        if self._conn.in_transaction():
            try:
                self._conn.commit_txn()
            except Exception as exc:
                raise _translate(exc) from None
        # In autocommit mode, statements are already durable — nothing to do.

    def rollback(self):
        self._check()
        if self._conn.in_transaction():
            try:
                self._conn.rollback_txn()
            except Exception as exc:
                raise _translate(exc) from None

    def close(self):
        if not self._closed:
            # An open transaction is rolled back on close (PEP 249).
            if self._conn.in_transaction():
                try:
                    self._conn.rollback_txn()
                except Exception:
                    pass
            self._conn.close()
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # DB-API: the connection context manager commits/closes on exit.
        self.close()


def connect(database: str = ":memory:", autocommit: bool = True, **kwargs) -> Connection:
    """Open a ChakraDB connection.

    ``database`` is ``":memory:"`` for an ephemeral database, or a directory
    path for a durable, crash-safe one (created if absent, recovered on reopen).

    With ``autocommit=False`` the connection runs in transaction mode: statements
    execute inside a transaction that ``commit()`` / ``rollback()`` control.
    """
    return Connection(database, autocommit=autocommit)
