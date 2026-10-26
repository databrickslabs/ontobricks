"""Route contracts for the staged ontology Generate workflow (plan task 4 of
``staged-ontology-generate``).

Exercises ``api.routers.internal.ontology``'s Stage 1 (detect), Stage 2
(draft read/update/discard), and Stage 3 (complete) routes by calling the
handler functions directly (mirrors ``tests/units/api/test_document_routes.py``
and ``tests/units/api/test_dq_run_selection.py``): ``threading.Thread`` is
faked to run the background task inline, ``GenerateWorkflow`` is
monkeypatched so this file tests routing/wiring only (workflow logic itself
is covered by ``tests/units/ontology/test_generate_workflow.py``), and a
real ``DomainSession`` (the ``domain_session`` fixture) backs the draft
store.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from back.core.errors import GoneError, OntoBricksError, ValidationError
from back.objects.ontology.generate import GenerateWorkflow as wf
from back.objects.ontology.generate.GenerateDraft import GenerateDraft, GenerateEntity

pytestmark = pytest.mark.unit


class _Request:
    def __init__(self, payload=None):
        self._payload = payload or {}

    async def json(self):
        return self._payload


class _Task:
    def __init__(self, task_id="task-1"):
        self.id = task_id
        self.progress = 0


class _FakeTaskManager:
    def __init__(self):
        self.created = []
        self.completed = []
        self.failed = []
        self.result_patches = []

    def create_task(self, **kw):
        task = _Task()
        self.created.append((task, kw))
        return task

    def start_task(self, task_id, message="Starting..."):
        return True

    def update_progress(self, task_id, progress, message=None):
        return True

    def merge_result(self, task_id, patch):
        self.result_patches.append((task_id, patch))
        return True

    def advance_step(self, task_id, message=None):
        return True

    def complete_task(self, task_id, result=None, message="Completed"):
        self.completed.append((task_id, result, message))
        return True

    def fail_task(self, task_id, error):
        self.failed.append((task_id, error))
        return True


class _FakeThread:
    """Runs the target synchronously — mirrors ``test_dq_run_selection.py``."""

    def __init__(self, target=None, **_kw):
        self._target = target

    def start(self):
        self._target()


@pytest.fixture
def route_ctx(monkeypatch, domain_session):
    from api.routers.internal import ontology as routes

    tm = _FakeTaskManager()
    monkeypatch.setattr(routes, "get_domain", lambda _mgr: domain_session)
    monkeypatch.setattr(
        routes, "require_domain_llm", lambda _domain, _settings: ("h", "t", "e", "chat")
    )
    monkeypatch.setattr(
        routes, "resolve_warehouse_id", lambda _domain, _settings: "wh1"
    )
    monkeypatch.setattr(routes, "get_task_manager", lambda: tm)
    monkeypatch.setattr(routes, "_DETECT_ENTITY_REVEAL_PAUSE_S", 0)
    monkeypatch.setattr(threading, "Thread", _FakeThread)
    return SimpleNamespace(routes=routes, tm=tm, domain=domain_session)


# ---------------------------------------------------------------------------
# Stage 1: POST /ontology/wizard/generate/detect (async)
# ---------------------------------------------------------------------------


class TestStartGenerateDetection:
    async def test_starts_task_and_completes_with_draft_result(
        self, route_ctx, monkeypatch
    ):
        draft = GenerateDraft.new(
            source_fingerprint="sha256:x",
            candidate_entities=[GenerateEntity.new_candidate("Carrier")],
        )
        captured = {}

        def _fake_run_detection(domain, settings, **kw):
            captured.update(kw)
            return draft

        monkeypatch.setattr(wf, "run_detection", _fake_run_detection)

        result = await route_ctx.routes.start_generate_detection(
            _Request(
                {"metadata": {"tables": []}, "guidelines": "g", "documents": ["a.pdf"]}
            ),
            session_mgr=object(),
            settings=object(),
        )

        assert result["success"] is True
        assert result["task_id"] == route_ctx.tm.created[0][0].id
        assert captured["host"] == "h"
        assert captured["token"] == "t"
        assert captured["endpoint_name"] == "e"
        assert captured["selected_docs"] == ["a.pdf"]
        assert captured["warehouse_id"] == "wh1"

        assert len(route_ctx.tm.completed) == 1
        task_id, task_result, _message = route_ctx.tm.completed[0]
        assert task_id == route_ctx.tm.created[0][0].id
        assert (
            task_result["draft"]["candidate_entities"][0]["canonical_label"]
            == "Carrier"
        )
        assert route_ctx.tm.result_patches == [
            (task_id, {"detected_entities": ["Carrier"]})
        ]

    async def test_detection_failure_fails_the_task_not_the_request(
        self, route_ctx, monkeypatch
    ):
        from back.objects.ontology.generate.DraftValidationError import DraftValidationError

        def _boom(domain, settings, **kw):
            raise DraftValidationError("bad output")

        monkeypatch.setattr(wf, "run_detection", _boom)

        result = await route_ctx.routes.start_generate_detection(
            _Request({}), session_mgr=object(), settings=object()
        )

        assert (
            result["success"] is True
        )  # the task was started, not the detection itself
        assert len(route_ctx.tm.failed) == 1
        assert route_ctx.tm.failed[0][1] == "bad output"
        assert not route_ctx.tm.completed


# ---------------------------------------------------------------------------
# Stage 2: GET/POST /ontology/wizard/generate/draft*
# ---------------------------------------------------------------------------


class TestDraftRoutes:
    async def test_get_draft_returns_none_when_absent(self, route_ctx, monkeypatch):
        monkeypatch.setattr(wf, "get_draft_view", lambda *_a, **_kw: None)
        result = await route_ctx.routes.get_generate_draft(
            session_mgr=object(), settings=object()
        )
        assert result == {"success": True, "draft": None}

    async def test_get_draft_returns_view(self, route_ctx, monkeypatch):
        monkeypatch.setattr(
            wf,
            "get_draft_view",
            lambda *_a, **_kw: {"stage": "reviewing", "stale": False},
        )
        result = await route_ctx.routes.get_generate_draft(
            session_mgr=object(), settings=object()
        )
        assert result["draft"]["stage"] == "reviewing"

    async def test_update_draft_requires_revision_and_op(self, route_ctx):
        with pytest.raises(ValidationError):
            await route_ctx.routes.update_generate_draft(
                _Request({}), session_mgr=object()
            )

    # -- Malformed payload shapes must 400, never TypeError/500 (task 4 --
    # -- review finding #7) --------------------------------------------

    async def test_update_draft_non_numeric_revision_is_400(self, route_ctx):
        with pytest.raises(ValidationError):
            await route_ctx.routes.update_generate_draft(
                _Request({"revision": "not-a-number", "op": "exclude"}),
                session_mgr=object(),
            )

    async def test_update_draft_non_dict_entity_is_400(self, route_ctx):
        """``op=add`` with a non-object ``entity`` must not leak an
        AttributeError/TypeError from ``entity.get(...)`` as a 500."""
        with pytest.raises(ValidationError):
            await route_ctx.routes.update_generate_draft(
                _Request({"revision": 0, "op": "add", "entity": "Carrier"}),
                session_mgr=object(),
            )

    async def test_update_draft_non_dict_updates_is_400(self, route_ctx):
        """``op=update`` with a non-object ``updates`` must not leak an
        AttributeError from ``updates.items()`` as a 500."""
        with pytest.raises(ValidationError):
            await route_ctx.routes.update_generate_draft(
                _Request(
                    {
                        "revision": 0,
                        "op": "update",
                        "entity_id": "cand-1",
                        "updates": "canonical_label=Foo",
                    }
                ),
                session_mgr=object(),
            )

    async def test_update_draft_non_string_entity_id_is_400(self, route_ctx):
        with pytest.raises(ValidationError):
            await route_ctx.routes.update_generate_draft(
                _Request({"revision": 0, "op": "exclude", "entity_id": 123}),
                session_mgr=object(),
            )

    async def test_update_draft_unknown_updates_fields_are_ignored_not_500(
        self, route_ctx, monkeypatch
    ):
        """An unknown field inside ``updates`` is filtered out by the
        workflow layer already — the route must still succeed (400 is only
        for malformed *shapes*, not unknown keys within a valid object)."""
        captured = {}

        def _fake_update(domain, **kw):
            captured.update(kw)
            return GenerateDraft.new(source_fingerprint="sha256:x")

        monkeypatch.setattr(wf, "update_draft", _fake_update)

        result = await route_ctx.routes.update_generate_draft(
            _Request(
                {
                    "revision": 2,
                    "op": "update",
                    "entity_id": "cand-1",
                    "updates": {"canonical_label": "Foo", "not_a_real_field": "x"},
                }
            ),
            session_mgr=object(),
        )
        assert result["success"] is True
        assert captured["updates"] == {
            "canonical_label": "Foo",
            "not_a_real_field": "x",
        }

    async def test_update_draft_delegates_to_workflow(self, route_ctx, monkeypatch):
        captured = {}

        def _fake_update(domain, **kw):
            captured.update(kw)
            return GenerateDraft.new(source_fingerprint="sha256:x")

        monkeypatch.setattr(wf, "update_draft", _fake_update)

        result = await route_ctx.routes.update_generate_draft(
            _Request(
                {
                    "revision": 2,
                    "op": "exclude",
                    "entity_id": "cand-1",
                }
            ),
            session_mgr=object(),
        )

        assert result["success"] is True
        assert captured["revision"] == 2
        assert captured["op"] == "exclude"
        assert captured["entity_id"] == "cand-1"

    async def test_discard_draft_delegates_to_workflow(self, route_ctx, monkeypatch):
        calls = []
        monkeypatch.setattr(wf, "discard_draft", lambda domain: calls.append(domain))

        result = await route_ctx.routes.discard_generate_draft(session_mgr=object())

        assert result["success"] is True
        assert calls == [route_ctx.domain]


# ---------------------------------------------------------------------------
# Stage 3: POST /ontology/wizard/generate/complete (async)
# ---------------------------------------------------------------------------


class TestStartGenerateCompletion:
    async def test_starts_task_and_completes_with_merge_result(
        self, route_ctx, monkeypatch
    ):
        outcome = {
            "draft": {"stage": "done"},
            "merge": {
                "classes_added": 1,
                "relations_added": 0,
                "attributes_added": 0,
                "axioms_added": 0,
            },
        }
        captured = {}

        def _fake_run_completion(domain, settings, **kw):
            captured.update(kw)
            return outcome

        monkeypatch.setattr(wf, "run_completion", _fake_run_completion)

        result = await route_ctx.routes.start_generate_completion(
            _Request({"options": {"max_classes": 10}}),
            session_mgr=object(),
            settings=object(),
        )

        assert result["success"] is True
        assert result["task_id"] == route_ctx.tm.created[0][0].id
        assert captured["host"] == "h"
        assert captured["options"] == {"max_classes": 10}
        assert len(route_ctx.tm.completed) == 1
        _task_id, task_result, _message = route_ctx.tm.completed[0]
        assert task_result["merge"]["classes_added"] == 1

    async def test_completion_failure_fails_the_task_not_the_request(
        self, route_ctx, monkeypatch
    ):
        from back.core.errors import NotFoundError

        def _boom(domain, settings, **kw):
            raise NotFoundError("no draft")

        monkeypatch.setattr(wf, "run_completion", _boom)

        result = await route_ctx.routes.start_generate_completion(
            _Request({}), session_mgr=object(), settings=object()
        )

        assert result["success"] is True
        assert len(route_ctx.tm.failed) == 1
        assert route_ctx.tm.failed[0][1] == "no draft"


# ---------------------------------------------------------------------------
# Legacy one-shot route is gone
# ---------------------------------------------------------------------------


class TestLegacyOneShotRouteGone:
    async def test_wizard_generate_async_always_returns_410(self, route_ctx):
        from api.routers.internal import ontology as routes

        with pytest.raises(OntoBricksError) as exc_info:
            await routes.generate_ontology_async()

        assert exc_info.value.status_code == 410
        # Typed subclass, not the raw base error (§4 minor closure).
        assert isinstance(exc_info.value, GoneError)
