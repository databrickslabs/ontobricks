"""Observability of the auto-mapping agent run.

Covers the two surfaces of one run (SPEC §3.1 of ``agent_auto_assignment``):
the live step log republished on ``task.result`` while the task is running, and
the durable ``agent_auto_map_run`` report buffered for the Domain Audit Trail.
"""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.objects.mapping import Mapping
from back.objects.mapping.Mapping import (
    AUTO_MAP_RUN_ACTION,
    build_auto_map_run_event,
)

SESSION_ID = "a" * 32


def _step(step_type="tool_call", tool="submit_entity_mapping", content="A", ms=12):
    return SimpleNamespace(
        step_type=step_type, tool_name=tool, content=content, duration_ms=ms
    )


class _FakeTask:
    def __init__(self, task_id="task-1"):
        self.id = task_id
        self.result = None

    def duration_seconds(self):
        return 1.5


class _FakeTaskManager:
    """Minimal TaskManager stand-in recording the terminal call."""

    def __init__(self):
        self.cancelled = False
        self.completed = None
        self.failed = None
        self.progress = []

    def start_task(self, *_a, **_k):
        pass

    def advance_step(self, *_a, **_k):
        pass

    def update_progress(self, _task_id, pct, msg):
        self.progress.append((pct, msg))

    def is_cancelled(self, _task_id):
        return self.cancelled

    def complete_task(self, _task_id, result=None, message=""):
        self.completed = {"result": result, "message": message}
        return True

    def fail_task(self, _task_id, error):
        self.failed = error
        return True


def _entity(uri):
    return {"uri": uri, "name": uri.rsplit("/", 1)[-1]}


def _agent_result(uris, steps):
    return SimpleNamespace(
        success=True,
        error=None,
        entity_mappings=[{"ontology_class": u, "id_column": "id"} for u in uris],
        relationship_mappings=[],
        steps=list(steps),
        iterations=1,
        usage={"prompt_tokens": 1, "completion_tokens": 1},
        stats={"entities": len(uris), "relationships": 0},
    )


def _run(agent_side_effect, *, entities, tm, task, recorded):
    """Drive ``run_auto_assign_task`` with one entity per chunk."""
    mapping = Mapping(MagicMock())

    def _record(session_id, session_ref, **kwargs):
        recorded.append(kwargs)
        return True

    with (
        patch("back.core.task_manager.get_task_manager", return_value=tm),
        patch("back.objects.mapping.Mapping.AUTO_ASSIGN_CHUNK_SIZE", 1),
        patch("back.objects.mapping.Mapping.AUTO_ASSIGN_CHUNK_COOLDOWN", 0),
        patch.object(Mapping, "fetch_documents_for_agent", return_value=[]),
        patch.object(Mapping, "save_mappings_to_session"),
        patch.object(Mapping, "build_per_item_results", return_value=[]),
        patch.object(Mapping, "record_auto_map_run", side_effect=_record),
        patch.object(
            Mapping, "auto_assign_with_agent", side_effect=agent_side_effect
        ),
    ):
        mapping.run_auto_assign_task(
            task,
            entities=entities,
            relationships=[],
            host="https://h",
            token="t",
            client=MagicMock(),
            llm_endpoint="ep",
            schema_context={},
            session_id=SESSION_ID,
            session_ref={},
            entity_mappings=[],
            relationship_mappings=[],
        )


class TestBuildAutoMapRunEvent:
    def test_shape_matches_record_change(self):
        event = build_auto_map_run_event(
            status="completed",
            task_id="t-7",
            duration_ms=1500,
            summary="Completed: 2 entities",
            steps=[{"type": "output", "tool": "", "content": "x", "ms": 3}],
            stats={"entities": 2},
        )
        assert set(event) == {
            "ts",
            "action",
            "entity_type",
            "entity_ref",
            "summary",
            "source",
            "meta",
        }
        assert event["action"] == AUTO_MAP_RUN_ACTION
        assert event["source"] == "agent"
        assert event["entity_type"] == "agent_run"
        assert event["entity_ref"] == "t-7"

    def test_meta_carries_the_report(self):
        steps = [{"type": "tool_call", "tool": "submit", "content": "A", "ms": 1}]
        event = build_auto_map_run_event(
            status="cancelled",
            task_id="t-7",
            duration_ms=90,
            summary="Cancelled",
            steps=steps,
            stats={"entities": 1, "relationships": 0},
        )
        meta = event["meta"]
        assert meta["status"] == "cancelled"
        assert meta["task_id"] == "t-7"
        assert meta["duration_ms"] == 90
        assert meta["stats"]["entities"] == 1
        assert meta["steps"] == steps

    def test_steps_are_copied_not_aliased(self):
        steps = [{"type": "output", "tool": "", "content": "x", "ms": 0}]
        event = build_auto_map_run_event(
            status="completed",
            task_id="t",
            duration_ms=0,
            summary="",
            steps=steps,
            stats={},
        )
        steps.append({"type": "output", "tool": "", "content": "later", "ms": 0})
        assert len(event["meta"]["steps"]) == 1


class TestAppendChangeEventToSession:
    @pytest.fixture()
    def session_dir(self, tmp_path):
        with patch(
            "back.objects.mapping.Mapping.get_settings",
            return_value=SimpleNamespace(session_dir=str(tmp_path)),
        ):
            yield tmp_path

    def test_appends_to_existing_change_log(self, session_dir):
        path = session_dir / SESSION_ID
        path.write_text(
            json.dumps({"domain_data": {"change_log": [{"action": "class_added"}]}})
        )
        ok = Mapping.append_change_event_to_session(
            SESSION_ID, {}, {"action": AUTO_MAP_RUN_ACTION}
        )
        assert ok is True
        log = json.loads(path.read_text())["domain_data"]["change_log"]
        assert [e["action"] for e in log] == ["class_added", AUTO_MAP_RUN_ACTION]

    def test_creates_bucket_when_absent(self, session_dir):
        path = session_dir / SESSION_ID
        path.write_text(json.dumps({"other": 1}))
        assert Mapping.append_change_event_to_session(
            SESSION_ID, {}, {"action": AUTO_MAP_RUN_ACTION}
        )
        data = json.loads(path.read_text())
        assert data["other"] == 1
        assert len(data["domain_data"]["change_log"]) == 1

    def test_migrates_legacy_project_data_bucket(self, session_dir):
        path = session_dir / SESSION_ID
        path.write_text(json.dumps({"project_data": {"change_log": []}}))
        assert Mapping.append_change_event_to_session(
            SESSION_ID, {}, {"action": AUTO_MAP_RUN_ACTION}
        )
        data = json.loads(path.read_text())
        assert "project_data" not in data
        assert len(data["domain_data"]["change_log"]) == 1

    def test_refreshes_the_in_memory_session_ref(self, session_dir):
        (session_dir / SESSION_ID).write_text(json.dumps({"domain_data": {}}))
        ref = {}
        Mapping.append_change_event_to_session(
            SESSION_ID, ref, {"action": AUTO_MAP_RUN_ACTION}
        )
        assert ref["domain_data"]["change_log"][0]["action"] == AUTO_MAP_RUN_ACTION

    def test_malformed_session_id_is_a_no_op(self, session_dir):
        assert (
            Mapping.append_change_event_to_session("../etc", {}, {"action": "x"})
            is False
        )

    def test_corrupted_session_file_does_not_raise(self, session_dir):
        (session_dir / SESSION_ID).write_text("{not json")
        assert (
            Mapping.append_change_event_to_session(SESSION_ID, {}, {"action": "x"})
            is False
        )


class TestLiveStepPublication:
    def test_steps_are_published_before_the_task_completes(self):
        task = _FakeTask()
        tm = _FakeTaskManager()
        entities = [_entity("http://x/A"), _entity("http://x/B")]
        seen_before_second_chunk = {}

        calls = {"n": 0}

        def agent(**kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                seen_before_second_chunk["result"] = dict(task.result or {})
            uri = kwargs["ontology"]["entities"][0]["uri"]
            return _agent_result([uri], [_step(content=uri)])

        _run(agent, entities=entities, tm=tm, task=task, recorded=[])

        published = seen_before_second_chunk["result"]["agent_steps"]
        assert len(published) == 1
        assert published[0]["content"] == "http://x/A"
        assert set(published[0]) == {"type", "tool", "content", "ms"}

    def test_final_result_holds_every_step(self):
        task = _FakeTask()
        tm = _FakeTaskManager()
        entities = [_entity("http://x/A"), _entity("http://x/B")]

        def agent(**kwargs):
            uri = kwargs["ontology"]["entities"][0]["uri"]
            return _agent_result([uri], [_step(content=uri)])

        _run(agent, entities=entities, tm=tm, task=task, recorded=[])

        steps = tm.completed["result"]["agent_steps"]
        assert [s["content"] for s in steps] == ["http://x/A", "http://x/B"]


class TestAuditReport:
    def test_completed_run_reports_parity_with_the_live_payload(self):
        task = _FakeTask()
        tm = _FakeTaskManager()
        recorded = []

        def agent(**kwargs):
            uri = kwargs["ontology"]["entities"][0]["uri"]
            return _agent_result([uri], [_step(content=uri)])

        _run(agent, entities=[_entity("http://x/A")], tm=tm, task=task, recorded=recorded)

        assert len(recorded) == 1
        report = recorded[0]
        assert report["status"] == "completed"
        assert report["steps"] == tm.completed["result"]["agent_steps"]
        assert report["stats"]["entities"] == 1

    def test_failed_run_is_reported(self):
        task = _FakeTask()
        tm = _FakeTaskManager()
        recorded = []

        def agent(**_kwargs):
            return SimpleNamespace(
                success=False,
                error="endpoint down",
                entity_mappings=[],
                relationship_mappings=[],
                steps=[],
                iterations=0,
                usage={},
                stats={},
            )

        _run(agent, entities=[_entity("http://x/A")], tm=tm, task=task, recorded=recorded)

        assert tm.failed is not None
        assert [r["status"] for r in recorded] == ["failed"]
        assert "endpoint down" in recorded[0]["summary"]

    def test_unexpected_crash_is_reported(self):
        task = _FakeTask()
        tm = _FakeTaskManager()
        recorded = []

        def agent(**_kwargs):
            raise RuntimeError("boom")

        # A chunk-level exception is swallowed as a chunk error, so the crash
        # has to come from outside the chunk loop to hit the outer handler.
        with patch.object(
            Mapping, "fetch_documents_for_agent", side_effect=RuntimeError("boom")
        ):
            _run(
                agent,
                entities=[_entity("http://x/A")],
                tm=tm,
                task=task,
                recorded=recorded,
            )

        assert [r["status"] for r in recorded] == ["failed"]
        assert "boom" in recorded[0]["summary"]


class TestCooperativeCancellation:
    def test_cancel_stops_the_loop_and_reports_cancelled(self):
        task = _FakeTask()
        tm = _FakeTaskManager()
        recorded = []
        entities = [_entity("http://x/A"), _entity("http://x/B"), _entity("http://x/C")]
        calls = []

        def agent(**kwargs):
            uri = kwargs["ontology"]["entities"][0]["uri"]
            calls.append(uri)
            tm.cancelled = True
            return _agent_result([uri], [_step(content=uri)])

        _run(agent, entities=entities, tm=tm, task=task, recorded=recorded)

        assert calls == ["http://x/A"]
        assert tm.completed is None
        assert tm.failed is None
        report = recorded[0]
        assert report["status"] == "cancelled"
        assert report["stats"]["entities"] == 1
        assert [s["content"] for s in report["steps"]] == ["http://x/A"]

    def test_cancel_persists_the_completed_chunks(self):
        task = _FakeTask()
        tm = _FakeTaskManager()
        entities = [_entity("http://x/A"), _entity("http://x/B")]

        def agent(**kwargs):
            uri = kwargs["ontology"]["entities"][0]["uri"]
            tm.cancelled = True
            return _agent_result([uri], [_step(content=uri)])

        mapping = Mapping(MagicMock())
        with (
            patch("back.core.task_manager.get_task_manager", return_value=tm),
            patch("back.objects.mapping.Mapping.AUTO_ASSIGN_CHUNK_SIZE", 1),
            patch("back.objects.mapping.Mapping.AUTO_ASSIGN_CHUNK_COOLDOWN", 0),
            patch.object(Mapping, "fetch_documents_for_agent", return_value=[]),
            patch.object(Mapping, "record_auto_map_run", return_value=True),
            patch.object(Mapping, "save_mappings_to_session") as save,
            patch.object(Mapping, "auto_assign_with_agent", side_effect=agent),
        ):
            mapping.run_auto_assign_task(
                task,
                entities=entities,
                relationships=[],
                host="https://h",
                token="t",
                client=MagicMock(),
                llm_endpoint="ep",
                schema_context={},
                session_id=SESSION_ID,
                session_ref={},
                entity_mappings=[],
                relationship_mappings=[],
            )

        save.assert_called_once()
        saved_entities = save.call_args.args[2]
        assert [m["ontology_class"] for m in saved_entities] == ["http://x/A"]

    def test_cancel_before_the_first_chunk_makes_no_agent_call(self):
        task = _FakeTask()
        tm = _FakeTaskManager()
        tm.cancelled = True
        recorded = []
        calls = []

        def agent(**kwargs):
            calls.append(1)
            return _agent_result(["http://x/A"], [])

        _run(agent, entities=[_entity("http://x/A")], tm=tm, task=task, recorded=recorded)

        assert calls == []
        assert recorded[0]["status"] == "cancelled"
        assert recorded[0]["stats"]["entities"] == 0
