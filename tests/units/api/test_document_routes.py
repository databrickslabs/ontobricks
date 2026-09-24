"""Route contracts for the Knowledge Store document endpoints (Lakebase)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from api.routers.internal import domain as routes
from back.core.databricks import DocumentParseService, ParseStatus, ParseSubmission

FOLDER, VERSION = "sales", "1"


class _Upload:
    def __init__(self, filename: str, content: bytes) -> None:
        self.filename = filename
        self._content = content

    async def read(self) -> bytes:
        return self._content


class _Request:
    def __init__(self, *, uploads=None, json_body=None) -> None:
        self._uploads = uploads or []
        self._json = json_body or {}

    async def form(self):
        uploads = self._uploads

        class _Form:
            @staticmethod
            def getlist(name):
                return uploads if name == "files" else []

        return _Form()

    async def json(self):
        return self._json


class _FakeService:
    """Store-backed DocumentParseService stand-in keyed by (folder, version)."""

    def __init__(self, submission=None, rows=None, doc=None) -> None:
        self.submission = submission
        self.rows = rows or []
        self.doc = doc
        self.prepared = []
        self.retried = []
        self.deleted = []
        self.parsed = []

    def prepare_upload(self, folder, version, filename, content):
        self.prepared.append((folder, version, filename, content))
        return self.submission

    def parse_pending(self, folder, version, filename):
        self.parsed.append((folder, version, filename))
        return {"status": ParseStatus.READY.value, "filename": filename}

    def retry(self, folder, version, filename):
        self.retried.append((folder, version, filename))
        return self.submission

    def list_documents(self, folder, version):
        return list(self.rows)

    def delete_documents(self, folder, version, filenames):
        self.deleted.append((folder, version, list(filenames)))
        return []

    def read_document(self, folder, version, filename, max_chars=None):
        return self.doc


@pytest.fixture
def route_context(monkeypatch):
    domain = SimpleNamespace(
        uc_domain_folder=FOLDER, domain_folder=FOLDER, current_version=VERSION
    )
    monkeypatch.setattr(routes, "get_domain", lambda _manager: domain)
    return SimpleNamespace(
        domain=domain,
        session=SimpleNamespace(),
        settings=SimpleNamespace(),
    )


def _use_service(monkeypatch, service):
    monkeypatch.setattr(
        routes, "_make_document_parse_service", lambda *_a, **_k: service
    )


@pytest.mark.asyncio
async def test_binary_upload_returns_pending_task(monkeypatch, route_context):
    service = _FakeService(
        ParseSubmission("spec.pdf", True, False, True, ParseStatus.PENDING)
    )
    task_manager = MagicMock()
    task_manager.run_background_task.return_value = SimpleNamespace(id="parse-1")
    _use_service(monkeypatch, service)
    monkeypatch.setattr(routes, "get_task_manager", lambda: task_manager)

    result = await routes.upload_documents(
        _Request(uploads=[_Upload("spec.pdf", b"%PDF")]),
        route_context.session,
        route_context.settings,
    )

    item = result["results"][0]
    assert item["parse_status"] == "pending"
    assert item["task_id"] == "parse-1"
    assert service.prepared == [(FOLDER, VERSION, "spec.pdf", b"%PDF")]
    task_manager.run_background_task.assert_called_once()


@pytest.mark.asyncio
async def test_upload_over_10mb_is_rejected_without_task(monkeypatch, route_context):
    service = _FakeService()
    task_manager = MagicMock()
    _use_service(monkeypatch, service)
    monkeypatch.setattr(routes, "get_task_manager", lambda: task_manager)

    oversize = b"x" * (DocumentParseService.MAX_UPLOAD_BYTES + 1)
    result = await routes.upload_documents(
        _Request(uploads=[_Upload("big.pdf", oversize)]),
        route_context.session,
        route_context.settings,
    )

    item = result["results"][0]
    assert item["success"] is False
    assert item["parse_status"] == "failed"
    assert "10 MB" in item["message"]
    # Neither the service nor a background task was invoked for the oversize file.
    assert service.prepared == []
    task_manager.run_background_task.assert_not_called()


@pytest.mark.asyncio
async def test_identical_ready_upload_returns_noop_without_task(
    monkeypatch, route_context
):
    service = _FakeService(
        ParseSubmission("spec.pdf", False, True, False, ParseStatus.READY)
    )
    task_manager = MagicMock()
    _use_service(monkeypatch, service)
    monkeypatch.setattr(routes, "get_task_manager", lambda: task_manager)

    result = await routes.upload_documents(
        _Request(uploads=[_Upload("spec.pdf", b"%PDF")]),
        route_context.session,
        route_context.settings,
    )

    item = result["results"][0]
    assert item["no_op"] is True
    assert item["parse_status"] == "ready"
    assert "task_id" not in item
    task_manager.run_background_task.assert_not_called()


@pytest.mark.asyncio
async def test_list_returns_rows_without_text(monkeypatch, route_context):
    service = _FakeService(
        rows=[
            {
                "filename": "spec.pdf",
                "status": "ready",
                "parse_status": "ready",
                "parser": "ai_parse_document",
                "size_bytes": 12,
                "error": "",
            }
        ]
    )
    _use_service(monkeypatch, service)

    result = await routes.list_documents(
        route_context.session, route_context.settings
    )

    assert result["success"] is True
    row = result["files"][0]
    assert row["filename"] == "spec.pdf"
    assert row["parse_status"] == "ready"
    assert "parsed_text" not in row


@pytest.mark.asyncio
async def test_retry_failed_binary_schedules_task(monkeypatch, route_context):
    service = _FakeService(
        ParseSubmission("spec.pdf", False, False, True, ParseStatus.PENDING)
    )
    task_manager = MagicMock()
    task_manager.run_background_task.return_value = SimpleNamespace(id="parse-2")
    _use_service(monkeypatch, service)
    monkeypatch.setattr(routes, "get_task_manager", lambda: task_manager)

    result = await routes.retry_document_parse(
        _Request(json_body={"filename": "spec.pdf"}),
        route_context.session,
        route_context.settings,
    )

    assert result["parse_status"] == "pending"
    assert result["task_id"] == "parse-2"
    assert service.retried == [(FOLDER, VERSION, "spec.pdf")]


@pytest.mark.asyncio
async def test_delete_accepts_multiple_filenames(monkeypatch, route_context):
    service = _FakeService()
    _use_service(monkeypatch, service)

    result = await routes.delete_document(
        _Request(json_body={"filenames": ["a.pdf", "b.pdf"]}),
        route_context.session,
        route_context.settings,
    )

    assert result["success"] is True
    assert service.deleted == [(FOLDER, VERSION, ["a.pdf", "b.pdf"])]


@pytest.mark.asyncio
async def test_delete_accepts_legacy_single_filename(monkeypatch, route_context):
    service = _FakeService()
    _use_service(monkeypatch, service)

    result = await routes.delete_document(
        _Request(json_body={"filename": "spec.pdf"}),
        route_context.session,
        route_context.settings,
    )

    assert result["success"] is True
    assert service.deleted == [(FOLDER, VERSION, ["spec.pdf"])]


@pytest.mark.asyncio
async def test_preview_returns_parsed_text_json(monkeypatch, route_context):
    service = _FakeService(
        doc={
            "filename": "spec.pdf",
            "content": "# Parsed",
            "parsed_with": "ai_parse_document",
            "parse_status": "ready",
        }
    )
    _use_service(monkeypatch, service)

    result = await routes.preview_document(
        "spec.pdf", route_context.session, route_context.settings
    )

    assert result["success"] is True
    assert result["content"] == "# Parsed"
    assert result["parser"] == "ai_parse_document"
