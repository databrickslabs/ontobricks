"""Agent document tools read only the Lakebase Knowledge Store corpus."""

from __future__ import annotations

import json

import pytest

from agents.tools import documents as docs
from agents.tools.context import ToolContext
from back.core.databricks import DocumentParseService
from tests.fixtures.factories.registry import FakeDocumentStore

FOLDER, VERSION = "dom", "1"


def _ctx(documents=None) -> ToolContext:
    return ToolContext(
        host="https://test.databricks.com",
        token="test-token",
        registry={"catalog": "main", "schema": "ob", "volume": "docs"},
        domain_folder=FOLDER,
        domain_version=VERSION,
        documents=list(documents or []),
    )


def _service_with(*rows) -> DocumentParseService:
    store = FakeDocumentStore()
    for filename, status, parsed_text in rows:
        store.upsert_document(
            FOLDER,
            VERSION,
            filename=filename,
            source_hash="h",
            parser="ai_parse_document",
            status="pending",
            size_bytes=12,
            source_bytes=b"%PDF",
        )
        if status == "ready":
            store.set_document_ready(FOLDER, VERSION, filename, parsed_text=parsed_text)
        elif status == "failed":
            store.set_document_failed(
                FOLDER, VERSION, filename, error="Document parsing is not ready"
            )
    return DocumentParseService(store)


def _use(monkeypatch, service):
    monkeypatch.setattr(
        docs, "_document_scope", lambda _ctx: (service, FOLDER, VERSION)
    )


def test_read_ready_pdf_returns_parsed_text(monkeypatch):
    service = _service_with(("spec.pdf", "ready", "Page one\n\nPage two"))
    _use(monkeypatch, service)

    out = json.loads(docs.tool_read_document(_ctx(), filename="spec.pdf"))

    assert out["content"] == "Page one\n\nPage two"
    assert out["parse_status"] == "ready"


@pytest.mark.parametrize("status", ["pending", "failed"])
def test_unavailable_pdf_returns_structured_status(monkeypatch, status):
    service = _service_with(("spec.pdf", status, ""))
    _use(monkeypatch, service)

    out = json.loads(docs.tool_read_document(_ctx(), filename="spec.pdf"))

    assert out["parse_status"] == status
    assert "content" not in out or out.get("content") == ""


def test_list_documents_reports_status(monkeypatch):
    service = _service_with(("spec.pdf", "ready", "x"))
    _use(monkeypatch, service)

    out = json.loads(docs.tool_list_documents(_ctx()))

    assert out["files"] == [
        {
            "name": "spec.pdf",
            "size": 12,
            "parse_status": "ready",
            "parser": "ai_parse_document",
        }
    ]


def test_no_registry_returns_error(monkeypatch):
    monkeypatch.setattr(docs, "_document_scope", lambda _ctx: None)
    out = json.loads(docs.tool_list_documents(_ctx()))
    assert "error" in out


def test_documents_context_separates_unavailable_documents():
    ctx = _ctx(
        [
            {"name": "terms.md", "content": "Asset has Location", "parse_status": "ready"},
            {
                "name": "manual.pdf",
                "content": "",
                "parse_status": "pending",
                "error": "Document parsing is not ready",
            },
        ]
    )

    out = json.loads(docs.tool_get_documents_context(ctx))

    assert [item["name"] for item in out["documents"]] == ["terms.md"]
    assert out["unavailable_documents"] == [
        {
            "name": "manual.pdf",
            "parse_status": "pending",
            "error": "Document parsing is not ready",
        }
    ]
