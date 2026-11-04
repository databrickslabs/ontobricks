"""Scripted psycopg stand-in shared by the ``test_v090_lakebase_*`` suites.

The fake records every statement (normalised SQL, params, row factory) and
replays scripted results matched by SQL substring, so the real Lakebase
registry mixins run end-to-end without a Postgres server.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, List, Optional

import pytest

from back.objects.registry.store.lakebase.store import LakebaseRegistryStore

SCHEMA = "ontobricks_registry"
QSCHEMA = f'"{SCHEMA}"'
REGISTRY_ID = "reg-1"
DICT_ROW = object()

_READY_FLAGS = (
    "_build_runs_ready",
    "_graph_analytics_ready",
    "_graph_analytics_runs_ready",
    "_status_column_ready",
    "_quorum_column_ready",
    "_mcp_policy_column_ready",
    "_review_events_ready",
    "_change_events_ready",
    "_collab_tables_ready",
    "_edit_locks_ready",
    "_schedule_columns_ready",
    "_cohort_schedules_imported",
    "_domain_documents_ready",
    "_simulation_tables_ready",
)


def norm(sql: str) -> str:
    return re.sub(r"\s+", " ", str(sql)).strip()


@dataclass
class Call:
    sql: str
    params: Any
    row_factory: Any
    many: bool = False


@dataclass
class _Rule:
    needle: str
    rows: List[Any]
    rowcount: int
    raises: Optional[BaseException]
    once: bool
    used: bool = False


@dataclass
class FakeDB:
    calls: List[Call] = field(default_factory=list)
    rules: List[_Rule] = field(default_factory=list)
    connections: int = 0
    transactions: int = 0
    connect_error: Optional[BaseException] = None

    def on(self, needle, *, rows=None, rowcount=None, raises=None, once=True):
        """Script the result for the next statement whose SQL contains *needle*."""
        rows = list(rows or [])
        if rowcount is None:
            rowcount = len(rows)
        self.rules.append(_Rule(norm(needle), rows, rowcount, raises, once))
        return self

    def _match(self, sql: str) -> Optional[_Rule]:
        for rule in self.rules:
            if rule.needle in sql and not (rule.once and rule.used):
                rule.used = True
                return rule
        return None

    @contextmanager
    def connect(self):
        if self.connect_error is not None:
            raise self.connect_error
        self.connections += 1
        yield _FakeConn(self)

    def sql_with(self, needle: str) -> List[Call]:
        n = norm(needle)
        return [c for c in self.calls if n in c.sql]

    def one(self, needle: str) -> Call:
        found = self.sql_with(needle)
        assert len(found) == 1, f"expected one statement with {needle!r}, got {len(found)}"
        return found[0]

    def assert_not_interpolated(self, *values: str) -> None:
        """User-supplied values must travel as bind params, never in SQL text."""
        for call in self.calls:
            for value in values:
                assert value not in call.sql, f"{value!r} interpolated into SQL: {call.sql}"


class _FakeConn:
    def __init__(self, db: FakeDB):
        self._db = db

    def cursor(self, row_factory=None):
        return _FakeCursor(self._db, row_factory)

    @contextmanager
    def transaction(self):
        self._db.transactions += 1
        yield


class _FakeCursor:
    def __init__(self, db: FakeDB, row_factory):
        self._db = db
        self._row_factory = row_factory
        self._rows: List[Any] = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def _apply(self, sql):
        rule = self._db._match(sql)
        if rule is None:
            self._rows, self.rowcount = [], 0
            return
        if rule.raises is not None:
            raise rule.raises
        self._rows, self.rowcount = list(rule.rows), rule.rowcount

    def execute(self, sql, params=None):
        sql = norm(sql)
        self._db.calls.append(Call(sql, params, self._row_factory))
        self._apply(sql)

    def executemany(self, sql, seq):
        sql = norm(sql)
        self._db.calls.append(Call(sql, list(seq), self._row_factory, many=True))
        self._apply(sql)

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows


class _Cfg:
    catalog = "cat"
    schema = "sch"
    volume = "vol"


class _Auth:
    host = "lakebase.example"
    database = "databricks_postgres"
    user = "sp-app"
    instance_name = "proj-1"


def make_store(db: FakeDB, *, ready: bool = True, registry_id=REGISTRY_ID):
    """Real ``LakebaseRegistryStore`` wired to *db*; no auth / pool / network."""
    store = LakebaseRegistryStore.__new__(LakebaseRegistryStore)
    store._cfg = _Cfg()
    store._schema = SCHEMA
    store._database = ""
    store._auth = _Auth()
    store._registry_id = registry_id
    for flag in _READY_FLAGS:
        setattr(store, flag, ready)
    store._connect = db.connect
    return store


@pytest.fixture
def fake_psycopg(monkeypatch):
    """Make every mixin's ``_require_psycopg`` return a recognisable dict_row."""
    from back.objects.registry.store.lakebase import store as store_module

    monkeypatch.setattr(store_module, "_require_psycopg", lambda: (None, DICT_ROW))
    return DICT_ROW
