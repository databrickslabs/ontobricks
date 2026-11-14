"""psycopg-compatible connection over one SQLite registry file.

The Lakebase registry mixins use a small slice of psycopg:
``with store._connect() as conn, conn.cursor(row_factory=dict_row) as cur``,
``cur.execute(sql, params)``, ``fetchone`` / ``fetchall`` / ``rowcount``,
and ``conn.transaction()``. :class:`SQLiteConnection` offers exactly that
slice over SQLite, translating each statement with :class:`SQLiteDialect`
and converting values both ways so callers get the same Python types
psycopg returns (``dict`` / ``list`` for jsonb, aware ``datetime`` for
timestamptz, ``bool`` for boolean).

Concurrency model: one Databricks App process owns the file. All registry
work in the process is serialised by a per-file re-entrant lock, and nested
``_connect()`` calls on the same thread share the outer connection and its
transaction. The outermost ``with`` block is one transaction: it commits on
success and rolls back on error, as a pooled psycopg connection does.
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, FrozenSet, Iterator, List, Optional, Sequence

from back.core.logging import get_logger
from back.objects.registry.store.sqlite.SQLiteDialect import SQLiteDialect

logger = get_logger(__name__)

_SCHEMA_FILE = Path(__file__).with_name("schema.sql")
_COLUMN_RE = re.compile(r"^\s+(\w+)\s+(jsonb|timestamptz|boolean|date)\b", re.MULTILINE)


def _column_types() -> Dict[str, str]:
    """Map registry column names to the Postgres type the caller expects back."""
    types = {name: kind for name, kind in _COLUMN_RE.findall(_SCHEMA_FILE.read_text("utf-8"))}
    # Computed columns the mixins alias in SELECTs.
    types.update({"is_stale": "boolean", "last_run": "timestamptz"})
    return types


_COLUMN_TYPES = _column_types()


def _utc_iso(value: datetime) -> str:
    """Canonical timestamp text, identical to the schema's DEFAULT format."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def _sql_now() -> str:
    return _utc_iso(datetime.now(timezone.utc))


def _version_key(version: Any) -> str:
    """Sort key ordering dotted versions numerically ('2' < '10', '1.2' < '1.10')."""
    parts = str(version or "").split(".")
    return ".".join(p.zfill(12) if p.isdigit() else p for p in parts)


class _FirstBy:
    """SQLite aggregate: the value whose sort keys are the largest (Postgres array_agg(...)[1])."""

    def __init__(self) -> None:
        self._best_keys: Optional[tuple] = None
        self._value: Any = None

    def step(self, value: Any, *keys: Any) -> None:
        candidate = tuple("" if k is None else k for k in keys)
        if self._best_keys is None or candidate > self._best_keys:
            self._best_keys, self._value = candidate, value

    def finalize(self) -> Any:
        return self._value


class _Cursor:
    """psycopg-style cursor; rows are converted once, at execute time."""

    def __init__(self, raw: sqlite3.Connection, as_dict: bool) -> None:
        self._raw = raw
        self._as_dict = as_dict
        self._rows: List[Any] = []
        self.rowcount = -1
        self.description: Optional[Sequence[Any]] = None

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __iter__(self) -> Iterator[Any]:
        rows, self._rows = self._rows, []
        return iter(rows)

    def close(self) -> None:
        self._rows = []

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> "_Cursor":
        text, lists, stamps = SQLiteDialect.translate(sql)
        bound = SQLiteConnection.adapt_params(params or (), lists, stamps)
        cur = self._raw.execute(text, bound)
        self.description = cur.description
        if cur.description:
            names = [d[0] for d in cur.description]
            self._rows = [self._convert(names, r) for r in cur.fetchall()]
            self.rowcount = len(self._rows)
        else:
            self._rows = []
            self.rowcount = cur.rowcount
        return self

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any]]) -> "_Cursor":
        text, lists, stamps = SQLiteDialect.translate(sql)
        bound = [SQLiteConnection.adapt_params(p, lists, stamps) for p in seq_of_params]
        cur = self._raw.executemany(text, bound)
        self._rows, self.description, self.rowcount = [], None, cur.rowcount
        return self

    def fetchone(self) -> Any:
        return self._rows.pop(0) if self._rows else None

    def fetchall(self) -> List[Any]:
        rows, self._rows = self._rows, []
        return rows

    def fetchmany(self, size: int = 1) -> List[Any]:
        rows, self._rows = self._rows[:size], self._rows[size:]
        return rows

    def _convert(self, names: List[str], row: Sequence[Any]) -> Any:
        values = [SQLiteConnection.read_value(_COLUMN_TYPES.get(n), v) for n, v in zip(names, row)]
        return dict(zip(names, values)) if self._as_dict else tuple(values)


class SQLiteConnection:
    """One psycopg-shaped transaction over a SQLite registry file."""

    _locks: Dict[str, threading.RLock] = {}
    _locks_guard = threading.Lock()
    _local = threading.local()

    def __init__(
        self,
        path: str,
        schema: str,
        on_commit: Optional[Callable[[], None]] = None,
    ) -> None:
        self._path = path
        self._schema = schema
        self._on_commit = on_commit

    # -- context manager: the outermost block owns the transaction ---------

    def __enter__(self) -> "SQLiteConnection":
        self._lock().acquire()
        state = self._state()
        if state["depth"] == 0:
            state["raw"] = self._open()
            state["changes"] = state["raw"].total_changes
            state["raw"].execute("BEGIN IMMEDIATE")
        state["depth"] += 1
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        state = self._state()
        state["depth"] -= 1
        try:
            if state["depth"] == 0:
                self._finish(state, failed=exc_type is not None)
        finally:
            self._lock().release()

    def cursor(self, row_factory: Any = None) -> _Cursor:
        return _Cursor(self._state()["raw"], as_dict=row_factory is not None)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Savepoint, like psycopg's nested ``conn.transaction()``."""
        raw = self._state()["raw"]
        name = "sp_%s" % uuid.uuid4().hex
        raw.execute("SAVEPOINT " + name)
        try:
            yield
        except BaseException:
            raw.execute("ROLLBACK TO " + name)
            raw.execute("RELEASE " + name)
            raise
        raw.execute("RELEASE " + name)

    def run_script(self, script: str) -> None:
        """Run a multi-statement DDL script in its own connection, outside any transaction.

        ``sqlite3.executescript`` commits whatever transaction is open, so it
        must never run inside the ``with`` block.
        """
        with self._lock():
            raw = self._open()
            try:
                raw.executescript(script)
            finally:
                raw.close()
        if self._on_commit is not None:
            self._on_commit()

    # -- value conversion ---------------------------------------------------

    @staticmethod
    def adapt_params(
        params: Sequence[Any], lists: FrozenSet[int], stamps: FrozenSet[int]
    ) -> List[Any]:
        out: List[Any] = []
        for index, value in enumerate(params):
            if index in lists:
                out.append(json.dumps([str(v) if isinstance(v, uuid.UUID) else v for v in value or ()]))
            elif index in stamps and isinstance(value, str) and value:
                out.append(_utc_iso(datetime.fromisoformat(value.replace("Z", "+00:00"))))
            else:
                out.append(SQLiteConnection._write_value(value))
        return out

    @staticmethod
    def _write_value(value: Any) -> Any:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, datetime):
            return _utc_iso(value)
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, (uuid.UUID, Decimal)):
            return str(value) if isinstance(value, uuid.UUID) else float(value)
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value)
        return value

    @staticmethod
    def read_value(kind: Optional[str], value: Any) -> Any:
        if value is None or kind is None:
            return value
        if kind == "jsonb":
            return json.loads(value) if isinstance(value, (str, bytes)) else value
        if kind == "timestamptz" and isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        if kind == "boolean":
            return bool(value)
        if kind == "date" and isinstance(value, str):
            return date.fromisoformat(value[:10])
        return value

    # -- plumbing -------------------------------------------------------------

    def _lock(self) -> threading.RLock:
        with SQLiteConnection._locks_guard:
            return SQLiteConnection._locks.setdefault(self._path, threading.RLock())

    def _state(self) -> Dict[str, Any]:
        states = SQLiteConnection._local.__dict__.setdefault("states", {})
        return states.setdefault(self._path, {"raw": None, "depth": 0, "changes": 0})

    def _open(self) -> sqlite3.Connection:
        raw = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False, timeout=30)
        raw.execute("ATTACH DATABASE ? AS %s" % self._quoted_schema(), (self._path,))
        raw.execute("PRAGMA %s.journal_mode=WAL" % self._quoted_schema())
        raw.execute("PRAGMA foreign_keys=ON")
        raw.execute("PRAGMA busy_timeout=30000")
        raw.create_function("now", 0, _sql_now)
        raw.create_function("gen_random_uuid", 0, lambda: str(uuid.uuid4()))
        raw.create_function("version_key", 1, _version_key, deterministic=True)
        raw.create_aggregate("first_by", -1, _FirstBy)
        return raw

    def _finish(self, state: Dict[str, Any], *, failed: bool) -> None:
        raw: sqlite3.Connection = state["raw"]
        wrote = False
        try:
            if failed:
                raw.execute("ROLLBACK")
            else:
                raw.execute("COMMIT")
                wrote = raw.total_changes != state["changes"]
        finally:
            raw.close()
            state["raw"] = None
        if wrote and self._on_commit is not None:
            self._on_commit()

    def _quoted_schema(self) -> str:
        return '"%s"' % self._schema.replace('"', '""')
