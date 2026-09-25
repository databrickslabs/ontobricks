"""Atomic lifecycle guard for destructive Lakebase version deletion."""

from contextlib import contextmanager

import pytest

from back.core.errors import ConflictError
from back.objects.registry.store.lakebase.store import LakebaseRegistryStore


class _Cursor:
    def __init__(self, rowcount):
        self.rowcount = rowcount
        self.sql = ""
        self.params = ()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _store(rowcount):
    cursor = _Cursor(rowcount)
    store = LakebaseRegistryStore.__new__(LakebaseRegistryStore)
    store._schema = "registry"
    store._registry_id = "registry-id"
    store._status_column_ready = True
    store._cfg = type("Cfg", (), {"catalog": "c", "schema": "s", "volume": "v"})()
    store._auth = type("Auth", (), {"host": "host", "database": "db"})()
    store._database = ""

    @contextmanager
    def connect():
        yield _Connection(cursor)

    store._connect = connect
    return store, cursor


def test_delete_version_sql_guards_draft_state():
    store, cursor = _store(rowcount=1)

    ok, message = store.delete_version("acme", "1")

    assert (ok, message) == (True, "")
    assert "AND v.status = %s" in cursor.sql
    assert cursor.params == ("registry-id", "acme", "1", "DRAFT")


def test_zero_row_guarded_delete_reports_conflict():
    store, _cursor = _store(rowcount=0)

    with pytest.raises(ConflictError, match="no longer Draft"):
        store.delete_version("acme", "1")
