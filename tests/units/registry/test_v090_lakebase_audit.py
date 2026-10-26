"""Real SQL of the review/validation and change-audit mixins.

:class:`ReviewEvents` records lifecycle transitions;
:class:`ChangeEvents` records ontology/mapping edits with
before→after payloads in ``meta``. Both are driven through a scripted cursor.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

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


@pytest.mark.parametrize(
    "method,flag,table,index",
    [
        ("_ensure_review_events_table", "_review_events_ready", "domain_review_events", "idx_review_events_domain_version"),
        ("_ensure_change_events_table", "_change_events_ready", "domain_change_events", "idx_change_events_domain_version"),
    ],
)
def test_audit_tables_created_lazily_and_fail_soft(method, flag, table, index):
    db = FakeDB()
    store = make_store(db, ready=False)
    assert getattr(store, method)() is True
    assert db.calls[0].params == (SCHEMA,)
    assert f"table_name = '{table}'" in db.calls[0].sql
    assert db.sql_with(f"CREATE TABLE IF NOT EXISTS {QSCHEMA}.{table}")
    assert db.sql_with(index)
    assert getattr(store, flag) is True

    present = FakeDB().on("information_schema", rows=[(1,)])
    assert getattr(make_store(present, ready=False), method)() is True
    assert not present.sql_with("CREATE")

    broken = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert getattr(broken, method)() is False


# ---------------------------------------------------------------------------
# Review events
# ---------------------------------------------------------------------------


def test_record_review_event_binds_transition_and_meta():
    db = FakeDB().on("INSERT INTO", rowcount=1)

    ok, msg = make_store(db).record_review_event(
        "acme",
        "2",
        "alice@x",
        "approve",
        from_status="REVIEW",
        to_status="PUBLISHED",
        comment="LGTM'); --",
        meta={"quorum": 2},
    )

    assert (ok, msg) == (True, "")
    call = db.one("INSERT INTO")
    assert f"INSERT INTO {QSCHEMA}.domain_review_events" in call.sql
    assert call.params == (
        "2",
        "alice@x",
        "approve",
        "REVIEW",
        "PUBLISHED",
        "LGTM'); --",
        json.dumps({"quorum": 2}),
        REGISTRY_ID,
        "acme",
    )
    db.assert_not_interpolated("LGTM", "alice@x")


def test_record_review_event_unknown_domain_error_and_missing_table():
    db = FakeDB()
    assert make_store(db).record_review_event("ghost", "1", "", "") == (
        False,
        "Domain 'ghost' not found",
    )
    assert db.calls[0].params[1:7] == ("", "", "", "", "", "{}")

    db = FakeDB().on("INSERT", raises=RuntimeError("e"))
    assert make_store(db).record_review_event("a", "1", "u", "x") == (False, "e")

    db = FakeDB().on("information_schema", raises=RuntimeError())
    assert make_store(db, ready=False).record_review_event("a", "1", "u", "x") == (
        False,
        "review audit log unavailable",
    )


def _review_row(**over):
    row = {
        "id": "e1",
        "folder": "acme",
        "version": "2",
        "actor": None,
        "action": "submit",
        "from_status": None,
        "to_status": "REVIEW",
        "comment": None,
        "meta": None,
        "created_at": _T0,
    }
    row.update(over)
    return row


def test_list_review_events_chronological_and_version_scoped(fake_psycopg):
    db = FakeDB().on("domain_review_events e", rows=[_review_row()])

    events = make_store(db).list_review_events("acme", "2")

    assert events == [
        {
            "id": "e1",
            "folder": "acme",
            "version": "2",
            "actor": "",
            "action": "submit",
            "from_status": "",
            "to_status": "REVIEW",
            "comment": "",
            "meta": {},
            "created_at": _T0.isoformat(),
        }
    ]
    call = db.calls[0]
    assert "AND e.version = %s" in call.sql
    assert "ORDER BY e.created_at ASC, e.id ASC" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "2")
    assert call.row_factory is DICT_ROW


def test_list_review_events_all_versions_errors_and_missing_table(fake_psycopg):
    db = FakeDB()
    assert make_store(db).list_review_events("acme") == []
    assert db.calls[0].params == (REGISTRY_ID, "acme")
    db.on("domain_review_events e", raises=RuntimeError())
    assert make_store(db).list_review_events("acme") == []
    bad = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert bad.list_review_events("acme") == []


def test_list_all_review_events_registry_wide(fake_psycopg):
    db = FakeDB().on("domain_review_events e", rows=[_review_row(id=None, created_at=None)])

    events = make_store(db).list_all_review_events()

    assert events[0]["id"] == "" and events[0]["created_at"] == ""
    assert db.calls[0].params == (REGISTRY_ID,)
    db.on("domain_review_events e", raises=RuntimeError())
    assert make_store(db).list_all_review_events() == []
    bad = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert bad.list_all_review_events() == []


# ---------------------------------------------------------------------------
# Change events (before -> after audit)
# ---------------------------------------------------------------------------


def test_record_change_events_batches_rows_with_before_after_meta():
    db = FakeDB().on("SELECT id FROM", rows=[("dom-1",)])
    events = [
        {
            "action": "update",
            "entity_type": "class",
            "entity_ref": "Person",
            "summary": "label changed",
            "meta": {"before": {"label": "Persn"}, "after": {"label": "Person"}},
            "ts": "2026-09-01T08:00:00Z",
        },
        {"action": "add", "source": "agent", "occurred_at": "2026-09-01T09:00:00Z"},
        {"action": "remove"},
    ]

    ok, msg = make_store(db).record_change_events("acme", "2", "bob@x", events)

    assert (ok, msg) == (True, "")
    assert db.one("SELECT id FROM").params == (REGISTRY_ID, "acme")
    batch = db.one("INSERT INTO")
    assert batch.many is True
    assert f"INSERT INTO {QSCHEMA}.domain_change_events" in batch.sql
    assert "COALESCE(%s::timestamptz, now())" in batch.sql
    first, second, third = batch.params
    assert first[:8] == ("dom-1", "2", "bob@x", "user", "update", "class", "Person", "label changed")
    assert json.loads(first[8]) == {"before": {"label": "Persn"}, "after": {"label": "Person"}}
    assert first[9] == "2026-09-01T08:00:00Z"
    assert second[3] == "agent" and second[9] == "2026-09-01T09:00:00Z"
    assert third[8] == "{}" and third[9] is None


def test_record_change_events_empty_batch_is_a_noop():
    db = FakeDB()

    assert make_store(db, ready=False).record_change_events("acme", "1", "u", []) == (True, "")
    assert db.calls == []


def test_record_change_events_unknown_domain_error_and_missing_table():
    db = FakeDB()
    assert make_store(db).record_change_events("ghost", "1", "u", [{"action": "a"}]) == (
        False,
        "Domain 'ghost' not found",
    )
    assert not db.sql_with("INSERT")

    db = FakeDB().on("SELECT id", raises=RuntimeError("e"))
    assert make_store(db).record_change_events("a", "1", "u", [{}]) == (False, "e")

    db = FakeDB().on("information_schema", raises=RuntimeError())
    assert make_store(db, ready=False).record_change_events("a", "1", "u", [{}]) == (
        False,
        "change audit log unavailable",
    )


def _change_row(**over):
    row = {
        "id": "c1",
        "folder": "acme",
        "version": "2",
        "actor": None,
        "source": None,
        "action": "update",
        "entity_type": None,
        "entity_ref": "Person",
        "summary": None,
        "meta": {"before": 1, "after": 2},
        "occurred_at": _T0,
        "created_at": None,
    }
    row.update(over)
    return row


def test_list_change_events_maps_rows_and_applies_limit(fake_psycopg):
    db = FakeDB().on("domain_change_events e", rows=[_change_row()])

    events = make_store(db).list_change_events("acme", "2", limit=10)

    assert events == [
        {
            "id": "c1",
            "folder": "acme",
            "version": "2",
            "actor": "",
            "source": "user",
            "action": "update",
            "entity_type": "",
            "entity_ref": "Person",
            "summary": "",
            "meta": {"before": 1, "after": 2},
            "occurred_at": _T0.isoformat(),
            "created_at": "",
        }
    ]
    call = db.calls[0]
    assert "AND e.version = %s" in call.sql and "LIMIT %s" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "2", 10)


def test_list_change_events_default_limit_errors_and_missing_table(fake_psycopg):
    db = FakeDB()
    assert make_store(db).list_change_events("acme", limit=0) == []
    assert db.calls[0].params == (REGISTRY_ID, "acme", 500)
    db.on("domain_change_events e", raises=RuntimeError())
    assert make_store(db).list_change_events("acme") == []
    bad = make_store(FakeDB().on("information_schema", raises=RuntimeError()), ready=False)
    assert bad.list_change_events("acme") == []
