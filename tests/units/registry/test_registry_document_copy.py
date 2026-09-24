"""Version-copy contracts for the Lakebase Knowledge Store corpus."""

from unittest.mock import MagicMock

from back.objects.registry.RegistryService import RegistryCfg, RegistryService
from tests.fixtures.factories.registry import FakeDocumentStore


def _service(store):
    service = RegistryService(RegistryCfg("cat", "sch", "vol"), MagicMock(), store=store)
    service._resolved_domains_folder = "domains"
    return service


def _seed_ready(store, folder, version, filename):
    store.upsert_document(
        folder,
        version,
        filename=filename,
        source_hash="h",
        parser="ai_parse_document",
        status="pending",
        size_bytes=10,
        source_bytes=b"%PDF",
    )
    store.set_document_ready(folder, version, filename, parsed_text="# Parsed")


def test_copy_version_documents_carries_parsed_rows_forward():
    store = FakeDocumentStore()
    _seed_ready(store, "sales", "1", "spec.pdf")
    _seed_ready(store, "sales", "1", "notes.md")
    service = _service(store)

    copied, errors = service.copy_version_documents("sales", "1", "2")

    assert copied == 2
    assert errors == []
    # Parsed text carried forward; transient bytes never copied.
    assert store.read_document_text("sales", "2", "spec.pdf") == (
        "# Parsed",
        "ai_parse_document",
        "ready",
    )
    assert store.read_document_bytes("sales", "2", "spec.pdf") is None


def test_copy_version_documents_empty_source_is_successful_noop():
    store = FakeDocumentStore()
    service = _service(store)

    copied, errors = service.copy_version_documents("sales", "1", "2")

    assert copied == 0
    assert errors == []
