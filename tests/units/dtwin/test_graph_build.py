"""One Build: the Original Graph, then the Certified Graph, in a single task."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from back.core.errors import ConflictError
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.objects.digitaltwin.build import TwinLakehouseBuild
from back.objects.digitaltwin.certify import CertifiedBuild, GraphBuild

pytestmark = pytest.mark.unit

_TASK = SimpleNamespace(id="t-1")


def _original(outcome):
    """Fake Original plan whose run reports *outcome* through the task manager."""
    plan = MagicMock()

    def _run(task, tm, builder, build_kind):
        tm.start_task(task.id, "Preparing...")
        if outcome == "fail":
            tm.fail_task(task.id, "R2RML failed")
            return
        tm.advance_step(task.id, "VIEW")
        count = 0 if outcome == "empty" else 12
        tm.complete_task(task.id, result={"triple_count": count}, message="Original built")

    plan._run.side_effect = _run
    return plan


def _run(outcome, *, certified=None):
    tm = MagicMock()
    certified_cls = MagicMock()
    if certified is not None:
        certified_cls.return_value.run.side_effect = certified
    with patch(
        "back.objects.digitaltwin.certify.GraphBuild.CertifiedBuild", certified_cls
    ):
        GraphBuild._run(
            _TASK, tm, _original(outcome), MagicMock(), MagicMock(), MagicMock(), "session"
        )
    return tm, certified_cls


class TestRun:
    def test_a_built_original_chains_the_certified_build(self):
        _tm, certified = _run("ok")

        certified.return_value.run.assert_called_once_with()

    def test_the_original_completion_does_not_end_the_task(self):
        tm, _certified = _run("ok")

        tm.complete_task.assert_not_called()

    def test_the_certified_completion_ends_the_task_with_both_results(self):
        def _certified_run():
            chained = certified.call_args.args[0]
            chained.start_task("t-1", "Composing the certified graph...")
            chained.complete_task("t-1", result={"triple_count": 14}, message="Certified built")

        tm = MagicMock()
        certified = MagicMock()
        certified.return_value.run.side_effect = _certified_run
        with patch("back.objects.digitaltwin.certify.GraphBuild.CertifiedBuild", certified):
            GraphBuild._run(
                _TASK, tm, _original("ok"), MagicMock(), MagicMock(), MagicMock(), "session"
            )

        tm.start_task.assert_called_once()
        tm.advance_step.assert_any_call("t-1", "Composing the certified graph...")
        result = tm.complete_task.call_args.kwargs["result"]
        assert result["triple_count"] == 14
        assert result["original"] == {"triple_count": 12}

    def test_an_empty_original_completes_without_a_certified_build(self):
        tm, certified = _run("empty")

        certified.assert_not_called()
        assert tm.complete_task.call_args.kwargs["result"] == {"triple_count": 0}

    def test_a_failed_original_stops_the_build(self):
        tm, certified = _run("fail")

        certified.assert_not_called()
        tm.fail_task.assert_called_once_with("t-1", "R2RML failed")
        tm.complete_task.assert_not_called()


class TestStart:
    def _start(self, *, editable=True):
        tm = MagicMock()
        tm.run_background_task.return_value = SimpleNamespace(id="t-7")
        builder = MagicMock()
        builder.build_steps.return_value = [{"name": "prepare"}]
        domain = SimpleNamespace(
            info={"name": "Dom"}, current_version="2", databricks={}, delta={}
        )
        with (
            patch.object(
                CertifiedBuild,
                "ensure_editable",
                side_effect=None if editable else ConflictError("locked"),
            ),
            patch.object(TwinLakehouseBuild, "prepare", return_value="plan"),
            patch.object(GraphDBFactory, "get_builder", return_value=builder),
            patch.object(
                CertifiedBuild, "task_steps", return_value=[{"name": "compose"}]
            ),
            patch(
                "back.core.helpers.get_build_sql_credentials",
                return_value=("https://h", "tok", "wh"),
            ),
            patch("back.core.task_manager.get_task_manager", return_value=tm),
        ):
            return GraphBuild.start(domain, SimpleNamespace()), tm

    def test_one_task_carries_the_original_then_the_certified_steps(self):
        task_id, tm = self._start()

        assert task_id == "t-7"
        kwargs = tm.run_background_task.call_args.kwargs
        assert [s["name"] for s in kwargs["steps"]] == ["prepare", "compose"]

    def test_a_read_only_version_starts_nothing(self):
        with pytest.raises(ConflictError):
            self._start(editable=False)


@pytest.fixture
def api_client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize(
    "path",
    ["/dtwin/sync/start", "/dtwin/databricks-build/start", "/dtwin/certified-build/start"],
)
def test_every_build_route_starts_the_single_build(api_client, path):
    domain = MagicMock()
    with (
        patch("api.routers.internal.dtwin.get_domain", return_value=domain),
        patch.object(GraphBuild, "start", return_value="t-1") as start,
    ):
        response = api_client.post(path, json={})

    assert response.status_code == 200
    assert response.json()["task_id"] == "t-1"
    assert start.call_args.args[0] is domain
