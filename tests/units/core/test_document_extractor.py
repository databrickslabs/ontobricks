"""Unit tests for ``back.core.databricks.DocumentExtractor``.

Covers the generic, reusable extractor: the swap-contract surface
(``supports`` / ``file_extension`` / ``is_available`` / ``extract``), the
warehouse query (schema pinned to v2.0), caching, graceful fallbacks, and the
``extract_text_from_parsed`` schema logic (v2.0 elements-first, with page and
markdown-blob fallbacks).
"""

from __future__ import annotations

import json

import pytest

from back.core.databricks import DocumentExtractor


class _FakeClient:
    def __init__(self, payload=None, warehouse_id="wh-123", raises=False) -> None:
        self.warehouse_id = warehouse_id
        self.payload = payload
        self.raises = raises
        self.last_query = None
        self.call_count = 0

    def execute_query(self, query: str):
        self.last_query = query
        self.call_count += 1
        if self.raises:
            raise RuntimeError("warehouse down")
        return [{"parsed": self.payload}]


_V2 = json.dumps({"document": {"elements": [{"type": "text", "content": "Hello"}]}})


# ---------------------------------------------------------------------------
# supports() / file_extension()
# ---------------------------------------------------------------------------


def test_supports_and_extension():
    assert DocumentExtractor.supports("pdf")
    assert DocumentExtractor.supports("PDF")
    assert not DocumentExtractor.supports("txt")
    assert not DocumentExtractor.supports("")
    assert DocumentExtractor.file_extension("a/b/spec.PDF") == "pdf"
    assert DocumentExtractor.file_extension("noext") == ""


# ---------------------------------------------------------------------------
# extract()
# ---------------------------------------------------------------------------


def test_is_available_requires_warehouse():
    assert DocumentExtractor(client=_FakeClient(_V2)).is_available()
    assert not DocumentExtractor(client=_FakeClient(_V2, warehouse_id="")).is_available()


def test_extract_without_warehouse_returns_none():
    ex = DocumentExtractor(client=_FakeClient(_V2, warehouse_id=""))
    assert ex.extract("/Volumes/x/spec.pdf") is None


def test_extract_runs_pinned_v2_query():
    client = _FakeClient(_V2)
    ex = DocumentExtractor(client=client)
    assert ex.extract("/Volumes/x/spec.pdf") == "Hello"
    assert "ai_parse_document" in client.last_query
    assert "map('version', '2.0')" in client.last_query
    assert "READ_FILES" in client.last_query
    assert "spec.pdf" in client.last_query


def test_extract_uses_cache():
    client = _FakeClient(_V2)
    ex = DocumentExtractor(client=client)
    cache: dict = {}
    ex.extract("/Volumes/x/spec.pdf", cache=cache)
    ex.extract("/Volumes/x/spec.pdf", cache=cache)
    assert client.call_count == 1


def test_extract_returns_none_on_query_failure():
    ex = DocumentExtractor(client=_FakeClient(_V2, raises=True))
    assert ex.extract("/Volumes/x/spec.pdf") is None


def test_extract_returns_none_on_empty_text():
    ex = DocumentExtractor(client=_FakeClient(json.dumps({"document": {}})))
    assert ex.extract("/Volumes/x/spec.pdf") is None


def test_extract_handles_bad_json():
    ex = DocumentExtractor(client=_FakeClient("not-json"))
    assert ex.extract("/Volumes/x/spec.pdf") is None


# ---------------------------------------------------------------------------
# extract_text_from_parsed()
# ---------------------------------------------------------------------------


def test_text_prefers_elements():
    parsed = {
        "document": {
            "elements": [{"type": "text", "content": "A"}, {"content": "B"}],
            "pages": [{"id": 0, "image_uri": None}],
        }
    }
    assert DocumentExtractor.extract_text_from_parsed(parsed) == "A\n\nB"


def test_text_uses_figure_description():
    parsed = {
        "document": {
            "elements": [{"type": "figure", "content": None, "description": "a chart"}]
        }
    }
    assert DocumentExtractor.extract_text_from_parsed(parsed) == "a chart"


def test_text_falls_back_to_pages():
    parsed = {
        "document": {"pages": [{"content": "P1"}, {"content": "P2"}], "elements": []}
    }
    assert DocumentExtractor.extract_text_from_parsed(parsed) == "P1\n\nP2"


def test_text_falls_back_to_markdown_blob():
    parsed = {"document": {"markdown": "# Title"}}
    assert DocumentExtractor.extract_text_from_parsed(parsed) == "# Title"


def test_text_empty_when_nothing():
    assert DocumentExtractor.extract_text_from_parsed({"document": {}}) == ""
