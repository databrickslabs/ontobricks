"""Real SQL of :class:`Collab` and :class:`EditLocks`.

Comments/tasks CRUD plus the single-editor DRAFT lock: atomic acquire with
same-holder refresh, admin take-over, stale-lease reclaim, holder-scoped
release and the admin overview.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from ._v090_fake_pg import (
    DICT_ROW,
    QSCHEMA,
    REGISTRY_ID,
    SCHEMA,
    FakeDB,
    fake_psycopg,  # noqa: F401
    make_store,
)

pytestmark = pytest.mark.unit

_T0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
_T1 = datetime(2026, 9, 1, 8, 10, tzinfo=timezone.utc)


def _unavailable_store():
    return make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)


# ---------------------------------------------------------------------------
# Collab tables
# ---------------------------------------------------------------------------


def test_collab_tables_created_with_indexes_when_absent():
    db = FakeDB()
    store = make_store(db, ready=False)

    assert store._ensure_collab_tables() is True
    assert db.calls[0].params == (SCHEMA,)
    assert db.sql_with(f"CREATE TABLE IF NOT EXISTS {QSCHEMA}.domain_comments")
    assert db.sql_with(f"CREATE TABLE IF NOT EXISTS {QSCHEMA}.domain_tasks")
    for idx in ("idx_domain_comments_lookup", "idx_domain_tasks_assignee", "idx_domain_tasks_domain"):
        assert db.sql_with(idx)
    assert store._collab_tables_ready is True


def test_collab_tables_skip_when_present_and_fail_soft():
    db = FakeDB().on("information_schema", rows=[(1,)])
    assert make_store(db, ready=False)._ensure_collab_tables() is True
    assert not db.sql_with("CREATE")
    assert _unavailable_store()._ensure_collab_tables() is False


# ---------------------------------------------------------------------------
# Comments
# ---------------------------------------------------------------------------


def _comment_row(**over):
    row = {
        "id": "c1",
        "version": "2",
        "parent_id": None,
        "author": "alice",
        "body": "hi",
        "resolved": False,
        "created_at": _T0,
    }
    row.update(over)
    return row


def test_insert_comment_returns_mapped_row(fake_psycopg):
    db = FakeDB().on("INSERT INTO", rows=[_comment_row(parent_id="p0")])

    out = make_store(db).insert_comment("acme", "2", author="alice", body="hi", parent_id="p0")

    assert out == {
        "id": "c1",
        "folder": "acme",
        "version": "2",
        "parent_id": "p0",
        "author": "alice",
        "body": "hi",
        "resolved": False,
        "created_at": _T0.isoformat(),
    }
    call = db.one("INSERT INTO")
    assert f"INSERT INTO {QSCHEMA}.domain_comments" in call.sql
    assert "RETURNING id" in call.sql
    assert call.params == ("2", "p0", "alice", "hi", REGISTRY_ID, "acme")
    assert call.row_factory is DICT_ROW


def test_insert_comment_top_level_unknown_domain_errors_and_missing_table(fake_psycopg):
    db = FakeDB()
    assert make_store(db).insert_comment("ghost", "1", author="", body="") is None
    assert db.calls[0].params[1:4] == (None, "", "")
    db.on("INSERT", raises=RuntimeError())
    assert make_store(db).insert_comment("a", "1", author="u", body="b") is None
    assert _unavailable_store().insert_comment("a", "1", author="u", body="b") is None


def test_list_comments_filters_version_and_unresolved(fake_psycopg):
    db = FakeDB().on("domain_comments c", rows=[_comment_row(created_at=None, author=None)])

    out = make_store(db).list_comments("acme", "2", include_resolved=False)

    assert out[0]["folder"] == "" and out[0]["author"] == "" and out[0]["created_at"] == ""
    call = db.calls[0]
    assert "AND c.version = %s" in call.sql
    assert "AND c.resolved = false" in call.sql
    assert "ORDER BY c.created_at ASC, c.id ASC" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "2")


def test_list_comments_unfiltered_errors_and_missing_table(fake_psycopg):
    db = FakeDB()
    assert make_store(db).list_comments("acme") == []
    assert "c.resolved = false" not in db.calls[0].sql
    assert db.calls[0].params == (REGISTRY_ID, "acme")
    db.on("domain_comments c", raises=RuntimeError())
    assert make_store(db).list_comments("acme") == []
    assert _unavailable_store().list_comments("acme") == []


def test_resolve_comment_found_missing_error_and_missing_table():
    db = FakeDB().on("UPDATE", rowcount=1)
    store = make_store(db)
    assert store.resolve_comment("acme", "c1", resolved=False) == (True, "")
    assert db.one("UPDATE").params == (False, REGISTRY_ID, "acme", "c1")
    assert store.resolve_comment("acme", "c1") == (False, "Comment not found")
    db.on("UPDATE", raises=RuntimeError("e"))
    assert store.resolve_comment("acme", "c1") == (False, "e")
    assert _unavailable_store().resolve_comment("a", "c") == (False, "comments backend unavailable")


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


def _task_row(**over):
    row = {
        "id": "t1",
        "version": "2",
        "assignee": "bob",
        "created_by": "alice",
        "title": "Fix",
        "description": None,
        "status": None,
        "due_date": date(2026, 10, 1),
        "comment_id": "c1",
        "created_at": _T0,
        "updated_at": _T1,
    }
    row.update(over)
    return row


def test_insert_task_maps_row_and_binds_optional_fields(fake_psycopg):
    db = FakeDB().on("INSERT INTO", rows=[_task_row()])

    out = make_store(db).insert_task(
        "acme", "2", assignee="bob", created_by="alice", title="Fix", due_date="", comment_id=None
    )

    assert out == {
        "id": "t1",
        "folder": "acme",
        "version": "2",
        "assignee": "bob",
        "created_by": "alice",
        "title": "Fix",
        "description": "",
        "status": "open",
        "due_date": "2026-10-01",
        "comment_id": "c1",
        "created_at": _T0.isoformat(),
        "updated_at": _T1.isoformat(),
    }
    call = db.one("INSERT INTO")
    assert f"INSERT INTO {QSCHEMA}.domain_tasks" in call.sql
    assert call.params == ("2", "bob", "alice", "Fix", "", None, None, REGISTRY_ID, "acme")


def test_insert_task_unknown_domain_errors_and_missing_table(fake_psycopg):
    db = FakeDB()
    assert make_store(db).insert_task("ghost", "1", assignee="a", created_by="b", title="t") is None
    db.on("INSERT", raises=RuntimeError())
    assert make_store(db).insert_task("a", "1", assignee="a", created_by="b", title="t") is None
    assert _unavailable_store().insert_task("a", "1", assignee="a", created_by="b", title="t") is None


def test_list_tasks_newest_first_and_version_scoped(fake_psycopg):
    row = _task_row(folder="acme", due_date=None, comment_id=None, created_at=None, updated_at=None)
    db = FakeDB().on("domain_tasks t", rows=[row])

    out = make_store(db).list_tasks("acme", "2")

    assert out[0]["due_date"] == "" and out[0]["comment_id"] == ""
    assert out[0]["updated_at"] == ""
    call = db.calls[0]
    assert "AND t.version = %s" in call.sql
    assert "ORDER BY t.created_at DESC, t.id DESC" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "2")


def test_list_tasks_unscoped_errors_and_missing_table(fake_psycopg):
    db = FakeDB()
    assert make_store(db).list_tasks("acme") == []
    assert db.calls[0].params == (REGISTRY_ID, "acme")
    db.on("domain_tasks t", raises=RuntimeError())
    assert make_store(db).list_tasks("acme") == []
    assert _unavailable_store().list_tasks("acme") == []


def test_list_tasks_for_assignee_is_case_insensitive(fake_psycopg):
    db = FakeDB().on("domain_tasks t", rows=[_task_row(folder="acme")])

    out = make_store(db).list_tasks_for_assignee("Bob@X")

    assert out[0]["folder"] == "acme"
    call = db.calls[0]
    assert "lower(t.assignee) = lower(%s)" in call.sql
    assert call.params == (REGISTRY_ID, "Bob@X")
    db.on("domain_tasks t", raises=RuntimeError())
    assert make_store(db).list_tasks_for_assignee("x") == []
    assert _unavailable_store().list_tasks_for_assignee("x") == []


def test_update_task_status_found_missing_error_and_missing_table():
    db = FakeDB().on("UPDATE", rowcount=1)
    store = make_store(db)
    assert store.update_task_status("acme", "t1", "done") == (True, "")
    call = db.one("UPDATE")
    assert "SET status = %s, updated_at = now()" in call.sql
    assert call.params == ("done", REGISTRY_ID, "acme", "t1")
    assert store.update_task_status("acme", "t1", "done") == (False, "Task not found")
    db.on("UPDATE", raises=RuntimeError("e"))
    assert store.update_task_status("acme", "t1", "done") == (False, "e")
    assert _unavailable_store().update_task_status("a", "t", "s") == (False, "tasks backend unavailable")


# ---------------------------------------------------------------------------
# Edit locks
# ---------------------------------------------------------------------------


def _lock_row(**over):
    row = {
        "holder_email": "alice@x",
        "holder_name": "Alice",
        "holder_session": "s1",
        "acquired_at": _T0,
        "heartbeat_at": _T1,
        "is_stale": False,
    }
    row.update(over)
    return row


def test_edit_lock_table_created_when_absent_and_fail_soft():
    db = FakeDB()
    store = make_store(db, ready=False)
    assert store._ensure_domain_edit_locks_table() is True
    create = db.one("CREATE TABLE IF NOT EXISTS")
    assert f"{QSCHEMA}.domain_edit_locks" in create.sql
    assert "PRIMARY KEY (domain_id, version)" in create.sql

    present = FakeDB().on("information_schema", rows=[(1,)])
    assert make_store(present, ready=False)._ensure_domain_edit_locks_table() is True
    assert not present.sql_with("CREATE")
    assert _unavailable_store()._ensure_domain_edit_locks_table() is False


def test_acquire_edit_lock_upsert_guards_takeover_conditions(fake_psycopg):
    db = FakeDB()
    db.on("INSERT INTO", rows=[{"holder_email": "alice@x"}])
    db.on("AS is_stale", rows=[_lock_row()])

    out = make_store(db).acquire_edit_lock(
        "acme", "2", holder_email="ALICE@x", holder_name="Alice", ttl_seconds=-5
    )

    assert out == {
        "acquired": True,
        "is_self": True,
        "holder_email": "alice@x",
        "holder_name": "Alice",
        "acquired_at": _T0.isoformat(),
    }
    ins = db.one("INSERT INTO")
    assert "ON CONFLICT (domain_id, version) DO UPDATE" in ins.sql
    assert "heartbeat_at = now()" in ins.sql
    # Same-holder refresh keeps acquired_at; a new holder resets it.
    assert "THEN \"ontobricks_registry\".domain_edit_locks.acquired_at ELSE now()" in ins.sql
    # Conflict guard: same holder OR force OR stale lease.
    assert "make_interval(secs => %s)" in ins.sql
    assert ins.params == ("2", "ALICE@x", "Alice", "", REGISTRY_ID, "acme", False, 0, 0)
    read = db.one("AS is_stale")
    assert read.params == (0, 0, REGISTRY_ID, "acme", "2")


def test_acquire_edit_lock_reports_other_live_holder(fake_psycopg):
    db = FakeDB().on("AS is_stale", rows=[_lock_row(holder_email="bob@x", holder_name="Bob")])

    out = make_store(db).acquire_edit_lock("acme", "2", holder_email="alice@x", ttl_seconds=300)

    assert out["acquired"] is False and out["is_self"] is False
    assert out["holder_email"] == "bob@x"
    assert db.one("INSERT INTO").params[-3:] == (False, 300, 300)


def test_acquire_edit_lock_force_takeover_flag_is_bound(fake_psycopg):
    db = FakeDB().on("AS is_stale", rows=[_lock_row(holder_email="admin@x")])

    out = make_store(db).acquire_edit_lock("acme", "2", holder_email="admin@x", force=True)

    assert out["acquired"] is True
    assert db.one("INSERT INTO").params[6] is True


def test_acquire_edit_lock_no_domain_error_and_missing_table(fake_psycopg):
    free = {"acquired": False, "is_self": False, "holder_email": ""}
    assert make_store(FakeDB()).acquire_edit_lock("ghost", "1", holder_email="a") == free
    db = FakeDB().on("INSERT", raises=RuntimeError())
    assert make_store(db).acquire_edit_lock("a", "1", holder_email="a") == free
    assert _unavailable_store().acquire_edit_lock("a", "1", holder_email="a") == free


def test_release_edit_lock_is_holder_scoped_case_insensitive():
    db = FakeDB().on("DELETE FROM", rowcount=1)
    store = make_store(db)

    assert store.release_edit_lock("acme", "2", holder_email="Alice@X") is True
    call = db.one("DELETE FROM")
    assert "AND lower(l.holder_email) = lower(%s)" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "2", "Alice@X")
    assert store.release_edit_lock("acme", "2", holder_email="bob") is False
    db.on("DELETE", raises=RuntimeError())
    assert store.release_edit_lock("acme", "2", holder_email="a") is False
    assert _unavailable_store().release_edit_lock("a", "1", holder_email="a") is False


def test_force_release_edit_lock_is_unconditional():
    db = FakeDB().on("DELETE FROM", rowcount=1)
    store = make_store(db)

    assert store.force_release_edit_lock("acme", "2") is True
    call = db.one("DELETE FROM")
    assert "holder_email" not in call.sql
    assert call.params == (REGISTRY_ID, "acme", "2")
    assert store.force_release_edit_lock("acme", "2") is False
    db.on("DELETE", raises=RuntimeError())
    assert store.force_release_edit_lock("acme", "2") is False
    assert _unavailable_store().force_release_edit_lock("a", "1") is False


def test_renew_edit_lock_bumps_heartbeat_only_for_holder():
    db = FakeDB().on("UPDATE", rowcount=1)
    store = make_store(db)

    assert store.renew_edit_lock("acme", "2", holder_email="alice@x") is True
    call = db.one("UPDATE")
    assert "SET heartbeat_at = now()" in call.sql
    assert "lower(l.holder_email) = lower(%s)" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "2", "alice@x")
    assert store.renew_edit_lock("acme", "2", holder_email="alice@x") is False, "lease lost"
    db.on("UPDATE", raises=RuntimeError())
    assert store.renew_edit_lock("acme", "2", holder_email="a") is False
    assert _unavailable_store().renew_edit_lock("a", "1", holder_email="a") is False


def test_get_edit_lock_exposes_stale_lease(fake_psycopg):
    db = FakeDB().on("AS is_stale", rows=[_lock_row(is_stale=True, heartbeat_at=None)])
    store = make_store(db)

    lock = store.get_edit_lock("acme", "2", ttl_seconds=60)

    assert lock["is_stale"] is True and lock["heartbeat_at"] == ""
    assert db.calls[0].params == (60, 60, REGISTRY_ID, "acme", "2")
    assert store.get_edit_lock("acme", "2") is None
    db.on("AS is_stale", raises=RuntimeError())
    assert store.get_edit_lock("acme", "2") is None
    assert _unavailable_store().get_edit_lock("a", "1") is None


def test_list_all_edit_locks_admin_overview(fake_psycopg):
    rows = [
        {**_lock_row(), "folder": "acme", "version": "2", "status": None},
        {**_lock_row(is_stale=True), "folder": "beta", "version": "1", "status": "REVIEW"},
    ]
    db = FakeDB().on("LEFT JOIN", rows=rows)

    out = make_store(db).list_all_edit_locks(ttl_seconds=120)

    assert [(r["folder"], r["version"], r["status"], r["is_stale"]) for r in out] == [
        ("acme", "2", "DRAFT", False),
        ("beta", "1", "REVIEW", True),
    ]
    assert out[0]["holder_email"] == "alice@x"
    call = db.calls[0]
    assert "ORDER BY l.acquired_at DESC" in call.sql
    assert call.params == (120, 120, REGISTRY_ID)


def test_list_all_edit_locks_errors_and_missing_table(fake_psycopg):
    db = FakeDB().on("LEFT JOIN", raises=RuntimeError())
    assert make_store(db).list_all_edit_locks() == []
    assert _unavailable_store().list_all_edit_locks() == []
