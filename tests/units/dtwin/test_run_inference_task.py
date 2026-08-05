"""Materialisation follows the options, not the caller's identity.

``run_inference_task`` used to gate both write-back branches on
``build_kind == "api"``, so a scheduled run computed its inferences and
then dropped them on the floor. The gate now reads the options, which is
what the caller actually asked for. The interactive UI is unaffected
because it never sends those options — it writes back through
``POST /dtwin/reasoning/materialize`` instead.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.objects.digitaltwin import DigitalTwin

pytestmark = pytest.mark.unit


class _Triple:
    def __init__(self, n: int) -> None:
        self.subject = f"http://ex.org/s{n}"
        self.predicate = "http://ex.org/p"
        self.object = f"http://ex.org/o{n}"


class _Result:
    def __init__(self, count: int = 2) -> None:
        self.inferred_triples = [_Triple(i) for i in range(count)]
        self.violations = []

    def to_dict(self):
        return {"inferred": len(self.inferred_triples)}


def _run(options, *, build_kind, inferred=2):
    """Drive the task with every collaborator stubbed, returning the
    TaskManager mock, the materialise-to-graph mock and the to-Delta mock."""
    tm = MagicMock()
    svc = MagicMock()
    svc.run_full_reasoning.return_value = _Result(inferred)
    svc.materialize_inferred.return_value = inferred

    with patch(
        "back.core.reasoning.ReasoningService", return_value=svc
    ) as svc_cls, patch(
        "back.core.graphdb.get_graphdb", return_value=MagicMock()
    ), patch(
        "back.core.helpers.get_databricks_client", return_value=MagicMock()
    ):
        svc_cls.materialize_to_delta = MagicMock(return_value=inferred)
        DigitalTwin.run_inference_task(
            tm,
            "task-1",
            SimpleNamespace(),
            SimpleNamespace(),
            options,
            build_kind=build_kind,
        )
        return tm, svc, svc_cls.materialize_to_delta


class TestAppendToGraph:
    def test_a_scheduled_run_appends_when_asked(self):
        tm, svc, _delta = _run({"swrl": True, "append_graph": True}, build_kind="scheduled")

        svc.materialize_inferred.assert_called_once()
        result = tm.complete_task.call_args[1]["result"]
        assert result["append_graph_count"] == 2

    def test_an_api_run_still_appends(self):
        _tm, svc, _delta = _run({"swrl": True, "append_graph": True}, build_kind="api")
        svc.materialize_inferred.assert_called_once()

    def test_the_interactive_ui_does_not(self):
        _tm, svc, _delta = _run({"swrl": True}, build_kind="session")
        svc.materialize_inferred.assert_not_called()

    def test_nothing_inferred_means_nothing_to_append(self):
        _tm, svc, _delta = _run(
            {"swrl": True, "append_graph": True}, build_kind="scheduled", inferred=0
        )
        svc.materialize_inferred.assert_not_called()

    def test_a_failed_append_is_reported_not_raised(self):
        tm = MagicMock()
        svc = MagicMock()
        svc.run_full_reasoning.return_value = _Result()
        svc.materialize_inferred.side_effect = RuntimeError("graph read-only")

        with patch("back.core.reasoning.ReasoningService", return_value=svc), patch(
            "back.core.graphdb.get_graphdb", return_value=MagicMock()
        ):
            DigitalTwin.run_inference_task(
                tm,
                "task-1",
                SimpleNamespace(),
                SimpleNamespace(),
                {"swrl": True, "append_graph": True},
                build_kind="scheduled",
            )

        tm.fail_task.assert_not_called()
        assert (
            tm.complete_task.call_args[1]["result"]["append_graph_error"]
            == "graph read-only"
        )


class TestMaterialiseToDelta:
    def _options(self, **over):
        base = {
            "swrl": True,
            "materialize": True,
            "materialize_table": "cat.sch.inferred",
        }
        base.update(over)
        return base

    def test_a_scheduled_run_writes_the_delta_table(self):
        tm, _svc, delta = _run(self._options(), build_kind="scheduled")

        delta.assert_called_once()
        assert delta.call_args[0][1] == "cat.sch.inferred"
        result = tm.complete_task.call_args[1]["result"]
        assert result["materialize_count"] == 2
        assert result["materialize_table"] == "cat.sch.inferred"

    def test_a_table_that_is_not_three_part_is_skipped(self):
        """``normalize_config`` rejects these on save; this is the second
        line of defence for configs written before that existed."""
        _tm, _svc, delta = _run(
            self._options(materialize_table="inferred"), build_kind="scheduled"
        )
        delta.assert_not_called()

    def test_the_interactive_ui_does_not_write(self):
        _tm, _svc, delta = _run({"swrl": True}, build_kind="session")
        delta.assert_not_called()


class TestCompletionMessage:
    def test_it_counts_both_write_backs(self):
        tm, _svc, _delta = _run(
            {
                "swrl": True,
                "append_graph": True,
                "materialize": True,
                "materialize_table": "cat.sch.inferred",
            },
            build_kind="scheduled",
        )

        msg = tm.complete_task.call_args[1]["message"]
        assert "2 inferred" in msg
        assert "2 appended to graph" in msg
        assert "2 written to Delta" in msg

    def test_a_scheduled_failure_keeps_the_real_reason(self):
        """It lands in the schedule's run history, where a generic
        'Inference failed' would be useless."""
        tm = MagicMock()
        svc = MagicMock()
        svc.run_full_reasoning.side_effect = RuntimeError("SWRL parser blew up")

        with patch("back.core.reasoning.ReasoningService", return_value=svc), patch(
            "back.core.graphdb.get_graphdb", return_value=MagicMock()
        ):
            DigitalTwin.run_inference_task(
                tm,
                "task-1",
                SimpleNamespace(),
                SimpleNamespace(),
                {"swrl": True},
                build_kind="scheduled",
            )

        tm.fail_task.assert_called_once_with("task-1", "SWRL parser blew up")

    def test_the_interactive_ui_gets_a_generic_failure(self):
        tm = MagicMock()
        svc = MagicMock()
        svc.run_full_reasoning.side_effect = RuntimeError("SWRL parser blew up")

        with patch("back.core.reasoning.ReasoningService", return_value=svc), patch(
            "back.core.graphdb.get_graphdb", return_value=MagicMock()
        ):
            DigitalTwin.run_inference_task(
                tm,
                "task-1",
                SimpleNamespace(),
                SimpleNamespace(),
                {"swrl": True},
                build_kind="session",
            )

        tm.fail_task.assert_called_once_with("task-1", "Inference failed")
