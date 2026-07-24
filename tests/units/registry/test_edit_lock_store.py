"""Unit tests for the Lakebase edit-lock store methods (mocked cursor).

Exercises the SQL-orchestration branches of
:class:`LakebaseRegistryStore` edit-lock methods without a real Postgres:
the connection / cursor are mocked, so these assert the call shape and the
result mapping (free / self / stale / force / heartbeat-lost), not the
actual ON CONFLICT semantics (those live in the gated integration suite).
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
    def __init__(self, fetchone_results=None, rowcount=0, fetchall_result=None):
        self._fetchone = list(fetchone_results or [])
        self._fetchall = list(fetchall_result or [])
        self.rowcount = rowcount
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((str(sql), params))

    def fetchone(self):
        return self._fetchone.pop(0) if self._fetchone else None

    def fetchall(self):
        return list(self._fetchall)


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
    # Skip the lazy-heal + registry-id DB round-trips.
    st._edit_locks_ready = True
    st._registry_id = "reg-1"
    return st


def _bind(st, cur):
    @contextmanager
    def _cm():
        yield _FakeConn(cur)

    return patch.object(st, "_connect", _cm)


def _live_row(email="alice@acme.com", name="Alice", is_stale=False):
    return {
        "holder_email": email,
        "holder_name": name,
        "holder_session": "sess",
        "acquired_at": _NOW,
        "heartbeat_at": _NOW,
        "is_stale": is_stale,
    }


# ----------------------------------------------------------------------
# acquire_edit_lock
# ----------------------------------------------------------------------


def test_acquire_when_free_grants_to_caller():
    st = _store()
    # upsert RETURNING wins, then the live row is the caller.
    cur = _FakeCursor(fetchone_results=[{"holder_email": "alice@acme.com"}, _live_row()])
    with _bind(st, cur):
        res = st.acquire_edit_lock(
            "acme", "1", holder_email="alice@acme.com", holder_name="Alice"
        )
    assert res["acquired"] is True
    assert res["is_self"] is True
    assert res["holder_email"] == "alice@acme.com"
    # The upsert is an ON CONFLICT that reclaims a stale lease (make_interval).
    # Params tail is (force, ttl, ttl); default call → ttl 0 (lease off).
    upsert_sql, upsert_params = cur.executed[0]
    assert "ON CONFLICT" in upsert_sql
    assert "make_interval" in upsert_sql
    assert upsert_params[-3] is False  # force
    assert upsert_params[-2:] == (0, 0)  # ttl disabled by default


def test_acquire_when_held_by_other_returns_not_acquired():
    st = _store()
    # upsert WHERE fails (no RETURNING row), live row is someone else.
    cur = _FakeCursor(
        fetchone_results=[None, _live_row(email="bob@acme.com", name="Bob")]
    )
    with _bind(st, cur):
        res = st.acquire_edit_lock(
            "acme", "1", holder_email="alice@acme.com", holder_name="Alice"
        )
    assert res["acquired"] is False
    assert res["is_self"] is False
    assert res["holder_email"] == "bob@acme.com"


def test_acquire_force_passes_true():
    st = _store()
    cur = _FakeCursor(fetchone_results=[{"holder_email": "alice@acme.com"}, _live_row()])
    with _bind(st, cur):
        st.acquire_edit_lock(
            "acme", "1", holder_email="alice@acme.com", force=True
        )
    _, upsert_params = cur.executed[0]
    assert upsert_params[-3] is True  # force (ttl pair follows)


def test_acquire_passes_ttl_for_stale_reclaim():
    st = _store()
    cur = _FakeCursor(fetchone_results=[{"holder_email": "alice@acme.com"}, _live_row()])
    with _bind(st, cur):
        st.acquire_edit_lock(
            "acme", "1", holder_email="alice@acme.com", ttl_seconds=600
        )
    upsert_sql, upsert_params = cur.executed[0]
    assert "make_interval" in upsert_sql
    # force False, then the TTL passed twice (guard + interval).
    assert upsert_params[-3:] == (False, 600, 600)
    # The live-row read after the upsert also carries the TTL (is_stale calc).
    _, live_params = cur.executed[1]
    assert live_params[:2] == (600, 600)


def test_acquire_no_domain_row_returns_empty():
    st = _store()
    # Domain folder not found → no upsert row and no live row.
    cur = _FakeCursor(fetchone_results=[None, None])
    with _bind(st, cur):
        res = st.acquire_edit_lock("ghost", "1", holder_email="alice@acme.com")
    assert res["acquired"] is False
    assert res["holder_email"] == ""


# ----------------------------------------------------------------------
# release / force_release
# ----------------------------------------------------------------------


def test_release_true_when_row_deleted():
    st = _store()
    cur = _FakeCursor(rowcount=1)
    with _bind(st, cur):
        assert st.release_edit_lock("acme", "1", holder_email="alice@acme.com") is True


def test_release_false_when_not_holder():
    st = _store()
    cur = _FakeCursor(rowcount=0)
    with _bind(st, cur):
        assert st.release_edit_lock("acme", "1", holder_email="alice@acme.com") is False


def test_force_release_true_when_row_deleted():
    st = _store()
    cur = _FakeCursor(rowcount=1)
    with _bind(st, cur):
        assert st.force_release_edit_lock("acme", "1") is True


# ----------------------------------------------------------------------
# get_edit_lock
# ----------------------------------------------------------------------


def test_get_edit_lock_returns_holder():
    st = _store()
    cur = _FakeCursor(fetchone_results=[_live_row(email="bob@acme.com", name="Bob")])
    with _bind(st, cur):
        lock = st.get_edit_lock("acme", "1", ttl_seconds=600)
    assert lock is not None
    assert lock["holder_email"] == "bob@acme.com"
    assert lock["holder_name"] == "Bob"
    assert lock["is_stale"] is False  # fresh lease
    # The staleness TTL is threaded into the SELECT (is_stale calc).
    _, params = cur.executed[0]
    assert params[:2] == (600, 600)


def test_get_edit_lock_flags_stale_lease():
    st = _store()
    cur = _FakeCursor(
        fetchone_results=[_live_row(email="bob@acme.com", is_stale=True)]
    )
    with _bind(st, cur):
        lock = st.get_edit_lock("acme", "1", ttl_seconds=600)
    assert lock["is_stale"] is True


def test_get_edit_lock_none_when_absent():
    st = _store()
    cur = _FakeCursor(fetchone_results=[None])
    with _bind(st, cur):
        assert st.get_edit_lock("acme", "1") is None


# ----------------------------------------------------------------------
# renew_edit_lock (lease keep-alive)
# ----------------------------------------------------------------------


def test_renew_true_when_holder_row_updated():
    st = _store()
    cur = _FakeCursor(rowcount=1)
    with _bind(st, cur):
        assert (
            st.renew_edit_lock("acme", "1", holder_email="alice@acme.com")
            is True
        )
    sql, _ = cur.executed[0]
    assert "heartbeat_at = now()" in sql


def test_renew_false_when_not_holder():
    st = _store()
    cur = _FakeCursor(rowcount=0)
    with _bind(st, cur):
        assert (
            st.renew_edit_lock("acme", "1", holder_email="alice@acme.com")
            is False
        )


# ----------------------------------------------------------------------
# list_all_edit_locks (admin overview)
# ----------------------------------------------------------------------


def test_list_all_edit_locks_maps_rows():
    st = _store()
    rows = [
        {**_live_row(email="bob@acme.com", name="Bob"), "folder": "acme",
         "version": "1", "status": "DRAFT"},
        {**_live_row(), "folder": "beta", "version": "2", "status": "IN-REVIEW"},
    ]
    cur = _FakeCursor(fetchall_result=rows)
    with _bind(st, cur):
        locks = st.list_all_edit_locks()
    assert len(locks) == 2
    assert locks[0]["folder"] == "acme"
    assert locks[0]["version"] == "1"
    assert locks[0]["status"] == "DRAFT"
    assert locks[0]["holder_email"] == "bob@acme.com"
    # JOINs domains + domain_versions for folder + status.
    sql, params = cur.executed[0]
    assert "domain_edit_locks" in sql
    assert "domain_versions" in sql
    # TTL pair (is_stale calc) precedes the registry id.
    assert params == (0, 0, "reg-1")


def test_list_all_edit_locks_empty():
    st = _store()
    cur = _FakeCursor(fetchall_result=[])
    with _bind(st, cur):
        assert st.list_all_edit_locks() == []
