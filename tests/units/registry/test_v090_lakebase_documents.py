"""Real SQL of the Lakebase Knowledge Store (:class:`Documents`).

Documents moved from UC Volumes to the ``domain_documents`` table in 0.9.0;
the parsed corpus is carried forward across versions (text only, never the
transient source bytes). Also pins the tolerant/strict defaults of the
abstract :class:`RegistryStore` document API.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from back.objects.registry.store.base import RegistryStore, StoreError

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

_PARSED_AT = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)


def _meta_row(**over):
    row = {
        "filename": "a.pdf",
        "source_hash": None,
        "parser": None,
        "status": None,
        "size_bytes": None,
        "output_schema": None,
        "error": None,
        "parsed_at": None,
    }
    row.update(over)
    return row


# ---------------------------------------------------------------------------
# Lazy table
# ---------------------------------------------------------------------------


def test_ensure_documents_table_creates_table_and_index_when_absent():
    db = FakeDB()
    store = make_store(db, ready=False)

    assert store._ensure_domain_documents_table() is True
    assert db.calls[0].params == (SCHEMA,)
    create = db.one("CREATE TABLE IF NOT EXISTS")
    assert f"{QSCHEMA}.domain_documents" in create.sql
    assert "UNIQUE (domain_id, version, filename)" in create.sql
    assert "CHECK (status IN ('pending', 'ready', 'failed'))" in create.sql
    assert db.sql_with("idx_domain_documents_lookup")
    assert store._domain_documents_ready is True


def test_ensure_documents_table_skips_ddl_when_present_and_fails_soft():
    db = FakeDB().on("information_schema.tables", rows=[(1,)])
    store = make_store(db, ready=False)
    assert store._ensure_domain_documents_table() is True
    assert not db.sql_with("CREATE TABLE")

    broken = make_store(
        FakeDB().on("information_schema", raises=RuntimeError("no usage")), ready=False
    )
    assert broken._ensure_domain_documents_table() is False
    assert broken._domain_documents_ready is False


@pytest.mark.parametrize(
    "call,expected",
    [
        (lambda s: s.upsert_document("f", "1", filename="a", source_hash="h", parser="p", status="pending", size_bytes=1), (False, "domain_documents table unavailable")),
        (lambda s: s.list_documents("f", "1"), []),
        (lambda s: s.get_document("f", "1", "a"), None),
        (lambda s: s.read_document_text("f", "1", "a"), None),
        (lambda s: s.read_document_bytes("f", "1", "a"), None),
        (lambda s: s.set_document_ready("f", "1", "a", parsed_text="t"), (False, "domain_documents table unavailable")),
        (lambda s: s.delete_documents("f", "1", ["a"]), ["domain_documents table unavailable"]),
        (lambda s: s.copy_documents_to_version("f", "1", "2"), (False, "domain_documents table unavailable")),
        (lambda s: s.count_documents("f", "1"), 0),
    ],
)
def test_every_operation_degrades_when_table_unavailable(call, expected):
    db = FakeDB().on("information_schema", raises=RuntimeError("down"), once=False)
    store = make_store(db, ready=False)

    assert call(store) == expected


# ---------------------------------------------------------------------------
# Upsert / read
# ---------------------------------------------------------------------------


def test_upsert_document_binds_every_value_and_upserts_on_natural_key():
    db = FakeDB().on("INSERT INTO", rowcount=1)
    store = make_store(db)

    ok, err = store.upsert_document(
        "acme",
        3,
        filename="evil'); DROP--.pdf",
        source_hash="sha",
        parser="pdf",
        status="ready",
        size_bytes=None,
        source_bytes=b"\x00\x01",
        parsed_text=None,
    )

    assert (ok, err) == (True, "")
    call = db.one("INSERT INTO")
    assert f"INSERT INTO {QSCHEMA}.domain_documents" in call.sql
    assert "ON CONFLICT (domain_id, version, filename) DO UPDATE" in call.sql
    assert "CASE WHEN %s = 'ready' THEN now() ELSE NULL END" in call.sql
    assert call.params == (
        "3",
        "evil'); DROP--.pdf",
        "sha",
        "pdf",
        "ready",
        "",
        b"\x00\x01",
        0,
        "",
        "",
        "ready",
        REGISTRY_ID,
        "acme",
    )
    db.assert_not_interpolated("DROP--")


def test_upsert_document_unknown_folder_and_error():
    store = make_store(FakeDB().on("INSERT INTO", rowcount=0))
    assert store.upsert_document(
        "ghost", "1", filename="a", source_hash="h", parser="p", status="pending", size_bytes=1
    ) == (False, "No domain row matched folder 'ghost'")

    store = make_store(FakeDB().on("INSERT INTO", raises=RuntimeError("e")))
    assert store.upsert_document(
        "f", "1", filename="a", source_hash="h", parser="p", status="pending", size_bytes=1
    ) == (False, "e")


def test_list_documents_projects_metadata_only_in_filename_order(fake_psycopg):
    rows = [
        _meta_row(),
        _meta_row(
            filename="b.md",
            source_hash="h",
            parser="md",
            status="ready",
            size_bytes=42,
            output_schema="s",
            error="",
            parsed_at=_PARSED_AT,
        ),
    ]
    db = FakeDB().on("FROM \"ontobricks_registry\".domain_documents dd", rows=rows)

    docs = make_store(db).list_documents("acme", 2)

    assert docs[0] == {
        "filename": "a.pdf",
        "source_hash": "",
        "parser": "",
        "status": "pending",
        "size_bytes": 0,
        "output_schema": "",
        "error": "",
        "parsed_at": "",
    }
    assert docs[1]["parsed_at"] == _PARSED_AT.isoformat()
    assert docs[1]["size_bytes"] == 42
    call = db.calls[0]
    assert "parsed_text" not in call.sql and "source_bytes" not in call.sql
    assert "ORDER BY dd.filename" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "2")
    assert call.row_factory is DICT_ROW


def test_strict_listing_propagates_query_and_table_failures(fake_psycopg):
    db = FakeDB().on("domain_documents dd", raises=RuntimeError("timeout"))
    with pytest.raises(StoreError, match="listing failed: timeout"):
        make_store(db).list_documents("acme", "1", strict=True)

    unavailable = make_store(
        FakeDB().on("information_schema", raises=RuntimeError()), ready=False
    )
    with pytest.raises(StoreError, match="table unavailable"):
        unavailable.list_documents("acme", "1", strict=True)


def test_get_document_hit_miss_error(fake_psycopg):
    db = FakeDB().on("dd.filename = %s", rows=[_meta_row(status="failed", error="bad")])
    store = make_store(db)

    doc = store.get_document("acme", "1", "a.pdf")
    assert doc["status"] == "failed" and doc["error"] == "bad"
    assert db.calls[0].params == (REGISTRY_ID, "acme", "1", "a.pdf")
    assert store.get_document("acme", "1", "a.pdf") is None
    db.on("dd.filename = %s", raises=RuntimeError("e"))
    assert store.get_document("acme", "1", "a.pdf") is None


def test_read_document_text_defaults_and_errors():
    db = FakeDB().on("SELECT dd.parsed_text", rows=[(None, None, None)])
    store = make_store(db)

    assert store.read_document_text("acme", "1", "a") == ("", "", "pending")
    assert store.read_document_text("acme", "1", "a") is None
    db.on("SELECT dd.parsed_text", raises=RuntimeError("e"))
    assert store.read_document_text("acme", "1", "a") is None


def test_read_document_bytes_returns_bytes_or_none_once_cleared():
    db = FakeDB()
    db.on("SELECT dd.source_bytes", rows=[(memoryview(b"abc"),)])
    db.on("SELECT dd.source_bytes", rows=[(None,)])
    store = make_store(db)

    assert store.read_document_bytes("acme", "1", "a") == b"abc"
    assert store.read_document_bytes("acme", "1", "a") is None
    db.on("SELECT dd.source_bytes", raises=RuntimeError("e"))
    assert store.read_document_bytes("acme", "1", "a") is None


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------


def test_set_document_ready_persists_text_and_nulls_bytes():
    db = FakeDB().on("UPDATE", rowcount=1)

    assert make_store(db).set_document_ready("acme", "1", "a.pdf", parsed_text="# md") == (True, "")
    call = db.one("UPDATE")
    assert "source_bytes = NULL" in call.sql
    assert "parsed_at = now()" in call.sql
    assert call.params == ("ready", "# md", "", REGISTRY_ID, "acme", "1", "a.pdf")


def test_set_document_failed_keeps_bytes_for_retry():
    db = FakeDB().on("UPDATE", rowcount=1)

    assert make_store(db).set_document_failed("acme", "1", "a.pdf", error="parse") == (True, "")
    call = db.one("UPDATE")
    assert "source_bytes = NULL" not in call.sql
    assert "parsed_text" not in call.sql
    assert call.params == ("failed", "parse", REGISTRY_ID, "acme", "1", "a.pdf")


def test_set_document_pending_preserves_parsed_at():
    db = FakeDB().on("UPDATE", rowcount=1)

    assert make_store(db).set_document_pending("acme", "1", "a.pdf") == (True, "")
    call = db.one("UPDATE")
    assert "parsed_at = parsed_at" in call.sql
    assert call.params[0] == "pending"


def test_status_update_reports_missing_row_and_errors():
    store = make_store(FakeDB().on("UPDATE", rowcount=0))
    assert store.set_document_pending("acme", "1", "x.pdf") == (False, "Document 'x.pdf' not found")
    store = make_store(FakeDB().on("UPDATE", raises=RuntimeError("e")))
    assert store.set_document_pending("acme", "1", "x.pdf") == (False, "e")


def test_status_update_returns_unavailable_when_table_missing():
    db = FakeDB().on("information_schema", raises=RuntimeError("x"))
    store = make_store(db, ready=False)

    assert store.set_document_failed("a", "1", "b", error="e") == (
        False,
        "domain_documents table unavailable",
    )


# ---------------------------------------------------------------------------
# Delete / copy / count
# ---------------------------------------------------------------------------


def test_delete_documents_noop_on_empty_list():
    db = FakeDB()

    assert make_store(db, ready=False).delete_documents("acme", "1", []) == []
    assert db.calls == []


def test_delete_documents_binds_filenames_as_array():
    db = FakeDB()

    assert make_store(db).delete_documents("acme", 1, ["a.pdf", "b.md"]) == []
    call = db.one("DELETE FROM")
    assert "dd.filename = ANY(%s)" in call.sql
    assert call.params == (REGISTRY_ID, "acme", "1", ["a.pdf", "b.md"])


def test_delete_documents_returns_error_messages():
    db = FakeDB().on("DELETE FROM", raises=RuntimeError("locked"))

    assert make_store(db).delete_documents("acme", "1", ["a"]) == ["locked"]


def test_copy_documents_to_version_carries_text_but_never_bytes():
    db = FakeDB()

    assert make_store(db).copy_documents_to_version("acme", 1, 2) == (True, "")
    inserts = db.sql_with("INSERT INTO")
    call = next(c for c in inserts if "domain_documents" in c.sql)
    assert "dd.parsed_text, NULL" in call.sql, "source_bytes must not be copied"
    assert "ON CONFLICT (domain_id, version, filename) DO NOTHING" in call.sql
    assert call.params == ("2", REGISTRY_ID, "acme", "1")
    assert any("simulations" in c.sql for c in inserts)


def test_copy_documents_to_version_reports_error():
    db = FakeDB().on("INSERT INTO", raises=RuntimeError("e"))

    assert make_store(db).copy_documents_to_version("acme", "1", "2") == (False, "e")


def test_count_documents_hit_and_error():
    db = FakeDB().on("SELECT count(*)", rows=[(7,)])
    store = make_store(db)

    assert store.count_documents("acme", 1) == 7
    assert db.calls[0].params == (REGISTRY_ID, "acme", "1")
    assert store.count_documents("acme", 1) == 0
    db.on("SELECT count(*)", raises=RuntimeError("e"))
    assert store.count_documents("acme", 1) == 0


# ---------------------------------------------------------------------------
# Base-class defaults (stores without a document table)
# ---------------------------------------------------------------------------


def test_base_document_api_defaults_are_tolerant():
    assert RegistryStore.upsert_document(
        None, "f", "1", filename="a", source_hash="h", parser="p", status="s", size_bytes=0
    ) == (False, "document store not supported")
    assert RegistryStore.list_documents(None, "f", "1") == []
    assert RegistryStore.get_document(None, "f", "1", "a") is None
    assert RegistryStore.read_document_text(None, "f", "1", "a") is None
    assert RegistryStore.read_document_bytes(None, "f", "1", "a") is None
    assert RegistryStore.set_document_ready(None, "f", "1", "a", parsed_text="t") == (
        False,
        "document store not supported",
    )
    assert RegistryStore.set_document_failed(None, "f", "1", "a", error="e")[0] is False
    assert RegistryStore.set_document_pending(None, "f", "1", "a")[0] is False
    assert RegistryStore.delete_documents(None, "f", "1", ["a"]) == []
    assert RegistryStore.copy_documents_to_version(None, "f", "1", "2") == (True, "")
    assert RegistryStore.count_documents(None, "f", "1") == 0


def test_base_strict_listing_refuses_to_report_empty_corpus():
    with pytest.raises(StoreError, match="not supported"):
        RegistryStore.list_documents(None, "f", "1", strict=True)
