"""Strict Lakebase document listing for destructive cleanup."""

import importlib
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from back.objects.registry.store.base import StoreError
from back.objects.registry.store.lakebase.store import LakebaseRegistryStore

store_module = importlib.import_module(
    "back.objects.registry.store.lakebase.store"
)


class _Cursor:
    def __init__(self, *, rows=None, error=None):
        self._rows = rows or []
        self._error = error

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _sql, _params):
        if self._error:
            raise self._error

    def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, **_kwargs):
        return self._cursor


def _store(*, rows=None, error=None, table_ready=True):
    cursor = _Cursor(rows=rows, error=error)
    store = LakebaseRegistryStore.__new__(LakebaseRegistryStore)
    store._schema = "registry"
    store._registry_id = "registry-id"
    store._ensure_domain_documents_table = MagicMock(return_value=table_ready)

    @contextmanager
    def connect():
        yield _Connection(cursor)

    store._connect = connect
    return store


def test_strict_listing_preserves_genuine_empty_result():
    store = _store(rows=[])

    with patch.object(store_module, "_require_psycopg", return_value=(None, object())):
        assert store.list_documents("acme", "1", strict=True) == []


def test_strict_listing_raises_query_failure():
    store = _store(error=RuntimeError("query failed"))

    with (
        patch.object(store_module, "_require_psycopg", return_value=(None, object())),
        pytest.raises(StoreError, match="query failed"),
    ):
        store.list_documents("acme", "1", strict=True)


def test_strict_listing_raises_when_document_table_is_unavailable():
    store = _store(table_ready=False)

    with pytest.raises(StoreError, match="table unavailable"):
        store.list_documents("acme", "1", strict=True)


def test_default_listing_remains_tolerant_for_read_only_callers():
    store = _store(error=RuntimeError("query failed"))

    with patch.object(store_module, "_require_psycopg", return_value=(None, object())):
        assert store.list_documents("acme", "1") == []
