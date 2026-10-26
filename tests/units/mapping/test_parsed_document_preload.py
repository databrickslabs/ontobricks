"""Mapping preloads ready documents from the Lakebase Knowledge Store."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

from back.objects.mapping import Mapping
from tests.fixtures.factories.registry import FakeDocumentStore


def _seed_store():
    store = FakeDocumentStore()
    store.upsert_document(
        "sales",
        "1",
        filename="spec.pdf",
        source_hash="h1",
        parser="ai_parse_document",
        status="pending",
        size_bytes=10,
        source_bytes=b"%PDF",
    )
    store.set_document_ready(
        "sales", "1", "spec.pdf", parsed_text="Customer maps to crm.customer."
    )
    store.upsert_document(
        "sales",
        "1",
        filename="manual.pdf",
        source_hash="h2",
        parser="ai_parse_document",
        status="pending",
        size_bytes=10,
        source_bytes=b"%PDF",
    )
    return store


def test_fetch_documents_for_agent_uses_ready_text_and_reports_pending(monkeypatch):
    module = importlib.import_module("back.objects.mapping.Mapping")
    registry_module = importlib.import_module("back.objects.registry")

    svc = SimpleNamespace(store=_seed_store())
    monkeypatch.setattr(
        registry_module.RegistryService,
        "from_context",
        classmethod(lambda cls, _domain, _settings: svc),
    )
    monkeypatch.setattr(module, "get_settings", lambda: SimpleNamespace())

    domain = SimpleNamespace(
        uc_domain_folder="sales", domain_folder="sales", current_version="1"
    )
    documents = Mapping.fetch_documents_for_agent(domain, "https://workspace", "token")

    assert documents == [
        {
            "name": "manual.pdf",
            "content": "",
            "parse_status": "pending",
            "error": "Document parsing is not ready",
        },
        {
            "name": "spec.pdf",
            "content": "Customer maps to crm.customer.",
            "parse_status": "ready",
        },
    ]
