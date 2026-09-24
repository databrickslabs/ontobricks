"""Contract tests for the RegistryStore document API reference implementation.

These lock the semantics that ``DocumentParseService`` and the document routes
depend on. The ``LakebaseRegistryStore`` mirrors this behaviour in SQL; the
in-memory ``FakeDocumentStore`` is the executable specification.
"""

from tests.fixtures.factories.registry import FakeDocumentStore

F, V = "acme", "1"


def _seed_binary_pending(store, filename="spec.pdf"):
    store.upsert_document(
        F,
        V,
        filename=filename,
        source_hash="abc123",
        parser="ai_parse_document",
        status="pending",
        size_bytes=1024,
        output_schema="2.0",
        source_bytes=b"%PDF-1.4 raw bytes",
    )


def test_list_projection_excludes_heavy_columns():
    store = FakeDocumentStore()
    _seed_binary_pending(store)
    rows = store.list_documents(F, V)
    assert len(rows) == 1
    row = rows[0]
    assert row["filename"] == "spec.pdf"
    assert row["status"] == "pending"
    assert "parsed_text" not in row
    assert "source_bytes" not in row


def test_set_ready_persists_text_and_nulls_bytes():
    store = FakeDocumentStore()
    _seed_binary_pending(store)
    assert store.read_document_bytes(F, V, "spec.pdf") == b"%PDF-1.4 raw bytes"

    ok, _ = store.set_document_ready(F, V, "spec.pdf", parsed_text="# Parsed")
    assert ok

    assert store.read_document_text(F, V, "spec.pdf") == (
        "# Parsed",
        "ai_parse_document",
        "ready",
    )
    assert store.read_document_bytes(F, V, "spec.pdf") is None


def test_set_failed_keeps_bytes_for_retry():
    store = FakeDocumentStore()
    _seed_binary_pending(store)
    store.set_document_failed(F, V, "spec.pdf", error="Document parsing failed")

    meta = store.get_document(F, V, "spec.pdf")
    assert meta["status"] == "failed"
    assert meta["error"] == "Document parsing failed"
    # Bytes retained so retry can re-run without a re-upload.
    assert store.read_document_bytes(F, V, "spec.pdf") == b"%PDF-1.4 raw bytes"


def test_delete_documents_removes_many():
    store = FakeDocumentStore()
    _seed_binary_pending(store, "a.pdf")
    _seed_binary_pending(store, "b.pdf")
    _seed_binary_pending(store, "c.pdf")

    assert store.count_documents(F, V) == 3
    errors = store.delete_documents(F, V, ["a.pdf", "c.pdf"])
    assert errors == []
    remaining = [r["filename"] for r in store.list_documents(F, V)]
    assert remaining == ["b.pdf"]


def test_copy_to_version_carries_text_not_bytes():
    store = FakeDocumentStore()
    _seed_binary_pending(store)
    store.set_document_ready(F, V, "spec.pdf", parsed_text="# Parsed")

    ok, _ = store.copy_documents_to_version(F, V, "2")
    assert ok

    assert store.read_document_text(F, "2", "spec.pdf") == (
        "# Parsed",
        "ai_parse_document",
        "ready",
    )
    assert store.read_document_bytes(F, "2", "spec.pdf") is None
    assert store.count_documents(F, "2") == 1
