"""Volume-free parsing: DocumentExtractor.extract_from_bytes (inline base64)."""

import base64
import json

from back.core.databricks.DocumentExtractor import DocumentExtractor


class _FakeClient:
    """Records the executed SQL and returns a canned ai_parse_document row."""

    warehouse_id = "wh-123"

    def __init__(self, parsed_payload):
        self.last_query = None
        self._payload = parsed_payload

    def execute_query(self, query):
        self.last_query = query
        return [{"parsed": json.dumps(self._payload)}]


def _payload(text):
    return {"document": {"elements": [{"content": text}]}}


def test_extract_from_bytes_returns_parsed_text():
    client = _FakeClient(_payload("Hello parsed world"))
    extractor = DocumentExtractor(client=client)

    text = extractor.extract_from_bytes(b"raw-pdf-bytes")

    assert text == "Hello parsed world"


def test_extract_from_bytes_inlines_base64_and_pins_schema():
    content = b"raw-pdf-bytes"
    client = _FakeClient(_payload("x"))
    extractor = DocumentExtractor(client=client)

    extractor.extract_from_bytes(content)

    q = client.last_query
    assert "ai_parse_document(" in q
    assert "unbase64(" in q
    assert "'2.0'" in q
    # The exact base64 of the input bytes must be present in the statement.
    assert base64.b64encode(content).decode("ascii") in q
    # No Volume path input.
    assert "READ_FILES(" not in q


def test_extract_from_bytes_without_warehouse_returns_none():
    class _NoWh:
        warehouse_id = ""

        def execute_query(self, query):  # pragma: no cover - not reached
            raise AssertionError("should not query without a warehouse")

    extractor = DocumentExtractor(client=_NoWh())
    assert extractor.extract_from_bytes(b"x") is None
