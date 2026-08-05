"""Unit tests for the Lakebase change-audit store methods (mocked cursor).

Exercises the SQL-orchestration branches of
:meth:`LakebaseRegistryStore.record_change_events` and
:meth:`~LakebaseRegistryStore.list_change_events` without a real Postgres:
the connection / cursor are mocked, so these assert the call shape and the
row mapping, not the actual persistence semantics (those live in the gated
integration suite).
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("psycopg")

from back.objects.registry import RegistryCfg
from back.objects.registry.store.lakebase.store import LakebaseRegistryStore

CFG = RegistryCfg(catalog="cat", schema="sch", volume="vol")
_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


class _FakeCursor:
    def __init__(self, fetchone_results=None, fetchall_results=None):
        self._fetchone = list(fetchone_results or [])
        self._fetchall = list(fetchall_results or [])
        self.executed = []
        self.many = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((str(sql), params))

    def executemany(self, sql, seq):
        self.many.append((str(sql), list(seq)))

    def fetchone(self):
        return self._fetchone.pop(0) if self._fetchone else None

    def fetchall(self):
        return self._fetchall.pop(0) if self._fetchall else []


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self, *args, **kwargs):
        return self._cur


def _store():
    with patch(
        "back.objects.registry.store.lakebase.store.get_lakebase_auth",
        MagicMock(return_value=MagicMock()),
    ):
        st = LakebaseRegistryStore(registry_cfg=CFG, schema="ontobricks_registry")
    st._change_events_ready = True  # skip lazy-heal DDL round-trip
    st._registry_id = "reg-1"
    return st


def _bind(st, cur):
    @contextmanager
    def _cm():
        yield _FakeConn(cur)

    return patch.object(st, "_connect", _cm)


def _events():
    return [
        {
            "ts": _NOW.isoformat(),
            "action": "class_added",
            "entity_type": "class",
            "entity_ref": "http://x#Customer",
            "summary": "Customer",
            "source": "user",
            "meta": {},
        },
        {
            "ts": _NOW.isoformat(),
            "action": "mapping_entity_updated",
            "entity_type": "mapping_entity",
            "entity_ref": "http://x#Customer",
            "summary": "Customer",
            "source": "agent",
            "meta": {"k": "v"},
        },
    ]


# ----------------------------------------------------------------------
# record_change_events
# ----------------------------------------------------------------------


def test_record_noop_on_empty_events():
    st = _store()
    # No DB access at all when there is nothing to flush.
    ok, msg = st.record_change_events("acme", "1", "alice@acme.com", [])
    assert ok is True
    assert msg == ""


def test_record_inserts_one_row_per_event():
    st = _store()
    cur = _FakeCursor(fetchone_results=[("dom-1",)])
    with _bind(st, cur):
        ok, msg = st.record_change_events(
            "acme", "1", "alice@acme.com", _events()
        )
    assert ok is True
    assert msg == ""
    # domain-id lookup, then one executemany with 2 param tuples.
    assert cur.executed[0][0].strip().startswith("SELECT id FROM")
    assert len(cur.many) == 1
    _, params = cur.many[0]
    assert len(params) == 2
    # actor stamped on every row; source preserved per event.
    assert params[0][0] == "dom-1"
    assert params[0][2] == "alice@acme.com"
    assert params[0][3] == "user"
    assert params[1][3] == "agent"
    assert params[0][4] == "class_added"


def test_record_returns_false_when_domain_missing():
    st = _store()
    cur = _FakeCursor(fetchone_results=[None])
    with _bind(st, cur):
        ok, msg = st.record_change_events(
            "ghost", "1", "alice@acme.com", _events()
        )
    assert ok is False
    assert "not found" in msg
    assert cur.many == []


def test_record_swallows_db_error():
    st = _store()

    @contextmanager
    def _boom():
        raise RuntimeError("connection lost")
        yield  # pragma: no cover

    with patch.object(st, "_connect", _boom):
        ok, msg = st.record_change_events(
            "acme", "1", "alice@acme.com", _events()
        )
    assert ok is False
    assert "connection lost" in msg


# ----------------------------------------------------------------------
# list_change_events
# ----------------------------------------------------------------------


def _row(action="class_added", source="user"):
    return {
        "id": "row-1",
        "folder": "acme",
        "version": "1",
        "actor": "alice@acme.com",
        "source": source,
        "action": action,
        "entity_type": "class",
        "entity_ref": "http://x#Customer",
        "summary": "Customer",
        "meta": {"k": "v"},
        "occurred_at": _NOW,
        "created_at": _NOW,
    }


def test_list_maps_rows_to_events():
    st = _store()
    cur = _FakeCursor(fetchall_results=[[_row(), _row(action="class_removed")]])
    with _bind(st, cur):
        out = st.list_change_events("acme", limit=100)
    assert len(out) == 2
    assert out[0]["action"] == "class_added"
    assert out[0]["actor"] == "alice@acme.com"
    assert out[0]["entity_ref"] == "http://x#Customer"
    assert out[0]["occurred_at"] == _NOW.isoformat()
    assert out[1]["action"] == "class_removed"


def test_list_filters_by_version_and_limit():
    st = _store()
    cur = _FakeCursor(fetchall_results=[[]])
    with _bind(st, cur):
        st.list_change_events("acme", version="2", limit=42)
    sql, params = cur.executed[0]
    assert "e.version = %s" in sql
    assert params[-1] == 42  # LIMIT bound last
    assert "2" in params


def test_list_empty_on_error():
    st = _store()

    @contextmanager
    def _boom():
        raise RuntimeError("nope")
        yield  # pragma: no cover

    with patch.object(st, "_connect", _boom):
        assert st.list_change_events("acme") == []
