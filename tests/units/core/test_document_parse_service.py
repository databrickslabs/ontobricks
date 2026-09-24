"""DocumentParseService backed by the Lakebase document store (no UC Volume)."""

import pytest

from back.core.databricks.DocumentParseService import (
    DocumentParseService,
    ParseStatus,
)
from tests.fixtures.factories.registry import FakeDocumentStore

F, V = "acme", "1"


class _FakeExtractor:
    """Records calls and returns a canned parsed text (or None to fail)."""

    def __init__(self, result="# Parsed text"):
        self._result = result
        self.calls = []

    def extract_from_bytes(self, content):
        self.calls.append(content)
        return self._result


@pytest.fixture(autouse=True)
def _reset_active_state():
    DocumentParseService._active_parses.clear()
    yield
    DocumentParseService._active_parses.clear()


def _service(extractor=None):
    return DocumentParseService(FakeDocumentStore(), extractor)


# -- upload size cap -------------------------------------------------


def test_upload_over_10mb_is_rejected_without_parsing():
    extractor = _FakeExtractor()
    svc = _service(extractor)
    oversize = b"x" * (DocumentParseService.MAX_UPLOAD_BYTES + 1)

    submission = svc.prepare_upload(F, V, "big.pdf", oversize)

    assert submission.parse_status is ParseStatus.FAILED
    assert submission.should_parse is False
    assert extractor.calls == []
    row = svc._store.get_document(F, V, "big.pdf")
    assert row["status"] == "failed"


def test_max_upload_bytes_is_ten_megabytes():
    assert DocumentParseService.MAX_UPLOAD_BYTES == 10 * 1024 * 1024


# -- plaintext -------------------------------------------------------


def test_plaintext_upload_is_ready_without_extractor():
    svc = _service(extractor=None)
    submission = svc.prepare_upload(F, V, "notes.md", b"# Title\ntext")

    assert submission.parse_status is ParseStatus.READY
    assert submission.should_parse is False
    doc = svc.read_document(F, V, "notes.md")
    assert doc["parse_status"] == "ready"
    assert doc["content"] == "# Title\ntext"
    assert doc["parsed_with"] == "plaintext"


def test_plaintext_non_utf8_is_failed():
    svc = _service()
    submission = svc.prepare_upload(F, V, "bad.csv", b"\xff\xfe\x00bad")
    assert submission.parse_status is ParseStatus.FAILED


# -- binary parse lifecycle ------------------------------------------


def test_binary_upload_is_pending_then_ready_and_nulls_bytes():
    extractor = _FakeExtractor("# Extracted")
    svc = _service(extractor)

    submission = svc.prepare_upload(F, V, "spec.pdf", b"%PDF-bytes")
    assert submission.parse_status is ParseStatus.PENDING
    assert submission.should_parse is True
    assert svc._store.read_document_bytes(F, V, "spec.pdf") == b"%PDF-bytes"

    row = svc.parse_pending(F, V, "spec.pdf")
    assert row["status"] == "ready"
    assert extractor.calls == [b"%PDF-bytes"]
    # Transient original cleared once parsed.
    assert svc._store.read_document_bytes(F, V, "spec.pdf") is None
    doc = svc.read_document(F, V, "spec.pdf")
    assert doc["content"] == "# Extracted"


def test_binary_parse_failure_keeps_bytes_and_marks_failed():
    extractor = _FakeExtractor(result=None)
    svc = _service(extractor)
    svc.prepare_upload(F, V, "spec.pdf", b"%PDF-bytes")

    row = svc.parse_pending(F, V, "spec.pdf")
    assert row["status"] == "failed"
    # Bytes retained so a retry can re-run.
    assert svc._store.read_document_bytes(F, V, "spec.pdf") == b"%PDF-bytes"


def test_identical_ready_upload_is_noop():
    svc = _service(_FakeExtractor())
    svc.prepare_upload(F, V, "spec.pdf", b"%PDF-bytes")
    svc.parse_pending(F, V, "spec.pdf")

    submission = svc.prepare_upload(F, V, "spec.pdf", b"%PDF-bytes")
    assert submission.no_op is True
    assert submission.parse_status is ParseStatus.READY


def test_unsupported_binary_is_failed():
    svc = _service(_FakeExtractor())
    submission = svc.prepare_upload(F, V, "archive.zip", b"PK\x03\x04")
    assert submission.parse_status is ParseStatus.FAILED


# -- retry -----------------------------------------------------------


def test_retry_failed_binary_returns_pending():
    extractor = _FakeExtractor(result=None)
    svc = _service(extractor)
    svc.prepare_upload(F, V, "spec.pdf", b"%PDF-bytes")
    svc.parse_pending(F, V, "spec.pdf")  # -> failed

    submission = svc.retry(F, V, "spec.pdf")
    assert submission.parse_status is ParseStatus.PENDING
    assert submission.should_parse is True


def test_retry_ready_document_raises():
    svc = _service(_FakeExtractor())
    svc.prepare_upload(F, V, "spec.pdf", b"%PDF-bytes")
    svc.parse_pending(F, V, "spec.pdf")
    with pytest.raises(ValueError):
        svc.retry(F, V, "spec.pdf")


# -- read never parses -----------------------------------------------


def test_read_pending_document_is_unavailable_without_calling_extractor():
    extractor = _FakeExtractor()
    svc = _service(extractor)
    svc.prepare_upload(F, V, "spec.pdf", b"%PDF-bytes")  # pending, not parsed

    doc = svc.read_document(F, V, "spec.pdf")
    assert doc["parse_status"] == "pending"
    assert "content" not in doc
    assert extractor.calls == []


# -- list / count / delete -------------------------------------------


def test_list_and_count_and_delete():
    svc = _service(_FakeExtractor())
    svc.prepare_upload(F, V, "a.md", b"a")
    svc.prepare_upload(F, V, "b.md", b"b")
    svc.prepare_upload(F, V, "c.md", b"c")

    rows = svc.list_documents(F, V)
    assert {r["filename"] for r in rows} == {"a.md", "b.md", "c.md"}
    assert all(r["parse_status"] == "ready" for r in rows)
    assert "parsed_text" not in rows[0]
    assert svc.count_documents(F, V) == 3

    errors = svc.delete_documents(F, V, ["a.md", "c.md"])
    assert errors == []
    assert svc.count_documents(F, V) == 1
