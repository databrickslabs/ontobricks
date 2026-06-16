"""Unit tests for ``agents.tools.documents``.

Covers the document-reading tool's three paths:

* plain-text files are decoded as UTF-8 (unchanged behaviour),
* binary files with no SQL warehouse fall back to an informative error,
* binary files with a warehouse are parsed to markdown via
  ``ai_parse_document`` (warehouse query mocked), with truncation and an
  in-run cache.
"""

from __future__ import annotations

import json

import pytest

from agents.tools import documents as docs
from agents.tools.context import ToolContext


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _ctx(warehouse_id: str = "") -> ToolContext:
    return ToolContext(
        host="https://test.databricks.com",
        token="test-token",
        registry={"catalog": "main", "schema": "ob", "volume": "docs"},
        domain_folder="dom",
        domain_version="1",
        warehouse_id=warehouse_id,
    )


class _FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200) -> None:
        self.content = content
        self.status_code = status_code
        self.headers = {"content-type": "application/octet-stream"}

    def raise_for_status(self) -> None:  # pragma: no cover - trivial
        return None


class _FakeClient:
    """Stand-in for DatabricksClient that records query calls."""

    last_query = None
    call_count = 0

    def __init__(self, *_args, **kwargs) -> None:
        self.warehouse_id = kwargs.get("warehouse_id", "wh-123")

    def execute_query(self, query: str):
        _FakeClient.last_query = query
        _FakeClient.call_count += 1
        return [{"parsed": _FakeClient.payload}]


@pytest.fixture(autouse=True)
def _reset_fake_client():
    _FakeClient.last_query = None
    _FakeClient.call_count = 0
    # Mirrors the live ai_parse_document v2.0 shape: text in elements[].content,
    # pages carry only id/image_uri.
    _FakeClient.payload = json.dumps(
        {
            "document": {
                "elements": [
                    {"id": 0, "type": "text", "content": "Page one"},
                    {"id": 1, "type": "text", "content": "Page two"},
                ],
                "pages": [{"id": 0, "image_uri": None}],
            },
            "metadata": {"version": "2.0"},
        }
    )
    yield


def _patch_client(monkeypatch, client_cls=None):
    import importlib

    dbc_module = importlib.import_module("back.core.databricks.DatabricksClient")
    monkeypatch.setattr(dbc_module, "DatabricksClient", client_cls or _FakeClient)


# ---------------------------------------------------------------------------
# tool_read_document — text path (unchanged)
# ---------------------------------------------------------------------------


def test_read_text_file_decodes_utf8(monkeypatch):
    monkeypatch.setattr(
        docs.requests, "get", lambda *a, **k: _FakeResponse(b"hello world")
    )
    out = json.loads(docs.tool_read_document(_ctx(), filename="notes.txt"))
    assert out["content"] == "hello world"
    assert out["truncated"] is False
    assert "parsed_with" not in out


# ---------------------------------------------------------------------------
# tool_read_document — binary path
# ---------------------------------------------------------------------------


def test_read_pdf_without_warehouse_returns_error(monkeypatch):
    # Should never hit the warehouse; no client patched.
    out = json.loads(docs.tool_read_document(_ctx(), filename="spec.pdf"))
    assert "error" in out
    assert "ai_parse_document" in out["error"]


def test_read_pdf_with_warehouse_parses_markdown(monkeypatch):
    _patch_client(monkeypatch)
    out = json.loads(docs.tool_read_document(_ctx("wh-123"), filename="spec.pdf"))
    assert out["content"] == "Page one\n\nPage two"
    assert out["parsed_with"] == "ai_parse_document"
    assert "ai_parse_document" in _FakeClient.last_query
    assert "READ_FILES" in _FakeClient.last_query
    assert "spec.pdf" in _FakeClient.last_query
    assert "map('version', '2.0')" in _FakeClient.last_query


def test_read_pdf_caches_within_run(monkeypatch):
    _patch_client(monkeypatch)
    ctx = _ctx("wh-123")
    docs.tool_read_document(ctx, filename="spec.pdf")
    docs.tool_read_document(ctx, filename="spec.pdf")
    assert _FakeClient.call_count == 1


def test_read_pdf_truncates_long_output(monkeypatch):
    long_text = "x" * (docs._MAX_DOC_CHARS + 500)
    _FakeClient.payload = json.dumps(
        {"document": {"elements": [{"type": "text", "content": long_text}]}}
    )
    _patch_client(monkeypatch)
    out = json.loads(docs.tool_read_document(_ctx("wh-123"), filename="big.pdf"))
    assert out["truncated"] is True
    assert out["size"] == docs._MAX_DOC_CHARS + 500
    assert "truncated" in out["content"]


def test_read_pdf_warehouse_failure_falls_back(monkeypatch):
    class _Boom(_FakeClient):
        def execute_query(self, query: str):
            raise RuntimeError("warehouse down")

    _patch_client(monkeypatch, _Boom)
    out = json.loads(docs.tool_read_document(_ctx("wh-123"), filename="spec.pdf"))
    assert "error" in out
