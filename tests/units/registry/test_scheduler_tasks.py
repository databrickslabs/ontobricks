"""The scheduler's task-type registry and its single execution harness.

The scheduler no longer knows what any job does: it looks a
:class:`TaskTypeSpec` up by ``task_type`` and runs it. These tests cover
the three seams that keeps honest — per-type config validation, the
executors for the two types that delegate to a DigitalTwin service, and
the harness envelope that turns whatever happened into a persisted
status plus a history row.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import ValidationError
from back.core.task_manager.models import TaskStatus
from back.objects.digitaltwin import DigitalTwin
from back.objects.registry.scheduler_tasks import (
    TASK_TYPES,
    RunOutcome,
    get_task_type,
    task_type_catalog,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------
# The registry itself
# ---------------------------------------------------------------------


class TestRegistry:
    def test_the_four_shipped_types_are_registered(self):
        assert set(TASK_TYPES) == {"build", "cohort", "analytics", "reasoning"}

    def test_an_unknown_type_names_the_ones_that_exist(self):
        with pytest.raises(ValidationError) as exc:
            get_task_type("teleport")
        assert "teleport" in str(exc.value)
        assert "analytics" in str(exc.value)

    def test_only_cohort_needs_a_target(self):
        needing = {k for k, spec in TASK_TYPES.items() if spec.needs_target}
        assert needing == {"cohort"}

    def test_the_delegating_types_declare_a_count_key(self):
        """The harness reads the run's headline number out of the finished
        task, so a delegating type must say which key holds it."""
        for spec in TASK_TYPES.values():
            if spec.delegates_task_lifecycle:
                assert spec.count_key, f"{spec.key} has no count_key"

    def test_the_catalog_is_json_serialisable_for_the_settings_ui(self):
        import json

        catalog = task_type_catalog()
        assert {c["key"] for c in catalog} == set(TASK_TYPES)
        json.dumps(catalog)


# ---------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------


class TestCohortConfig:
    def test_defaults_to_both_outputs(self):
        assert get_task_type("cohort").normalize_config({}) == {
            "output_graph": True,
            "output_uc": True,
        }

    def test_no_output_target_is_rejected(self):
        with pytest.raises(ValidationError):
            get_task_type("cohort").normalize_config(
                {"output_graph": False, "output_uc": False}
            )


class TestAnalyticsConfig:
    def test_takes_no_options(self):
        assert get_task_type("analytics").normalize_config({"top_n": 500}) == {}


class TestReasoningConfig:
    def _cfg(self, **over):
        base = {
            "phases": {"swrl": True},
            "materialize_graph": True,
            "materialize_delta": False,
            "materialize_table": "",
        }
        base.update(over)
        return base

    def test_every_phase_is_filled_in(self):
        out = get_task_type("reasoning").normalize_config(self._cfg())
        assert set(out["phases"]) == {
            "tbox",
            "swrl",
            "graph",
            "decision_tables",
            "sparql_rules",
            "aggregate_rules",
        }
        assert out["phases"]["swrl"] is True
        assert out["phases"]["decision_tables"] is False

    def test_no_phase_enabled_is_rejected(self):
        with pytest.raises(ValidationError) as exc:
            get_task_type("reasoning").normalize_config(
                self._cfg(phases={name: False for name in ("tbox", "swrl", "graph")})
            )
        assert "phase" in str(exc.value)

    def test_no_materialise_target_is_rejected(self):
        """A scheduled run has no UI to review results in, so inferring
        without writing anywhere would silently discard the work."""
        with pytest.raises(ValidationError) as exc:
            get_task_type("reasoning").normalize_config(
                self._cfg(materialize_graph=False, materialize_delta=False)
            )
        assert "discard" in str(exc.value)

    def test_a_delta_target_must_be_fully_qualified(self):
        with pytest.raises(ValidationError):
            get_task_type("reasoning").normalize_config(
                self._cfg(materialize_delta=True, materialize_table="just_a_table")
            )

    def test_a_stale_delta_table_is_dropped_when_the_target_is_off(self):
        out = get_task_type("reasoning").normalize_config(
            self._cfg(materialize_delta=False, materialize_table="c.s.t")
        )
        assert out["materialize_table"] == ""


# ---------------------------------------------------------------------
# The two delegating executors
# ---------------------------------------------------------------------


def _ctx(**over):
    base = dict(
        task_type="analytics",
        domain_name="Acme",
        target_key="",
        config={},
        settings=SimpleNamespace(analytics_top_n=25),
        graph_name="Acme_V1",
        domain=object(),
        snapshot=object(),
        tm=MagicMock(),
        task_id="task-1",
        progress=lambda pct, msg: None,
        advance=lambda msg: None,
    )
    base.update(over)
    return SimpleNamespace(**base)


class TestAnalyticsExecutor:
    def test_it_delegates_to_run_metrics_task(self):
        from back.objects.registry.scheduler_tasks import analytics

        ctx = _ctx()
        with patch(
            "back.core.graph_analysis.analytics_job_status", return_value=(True, "")
        ), patch.object(DigitalTwin, "run_metrics_task") as run_metrics:
            analytics.run(ctx)

        run_metrics.assert_called_once()
        args, kwargs = run_metrics.call_args
        assert args[1] == "task-1"
        assert args[4] == "Acme_V1"
        assert kwargs["top_n"] == 25

    def test_a_blocked_domain_never_submits_the_job(self):
        from back.objects.registry.scheduler_tasks import analytics

        with patch(
            "back.core.graph_analysis.analytics_job_status",
            return_value=(False, "Run Knowledge Graph → Build to materialise it"),
        ), patch.object(DigitalTwin, "run_metrics_task") as run_metrics:
            with pytest.raises(ValidationError) as exc:
                analytics.run(_ctx())

        run_metrics.assert_not_called()
        assert "Build" in str(exc.value)


class TestReasoningExecutor:
    def test_the_config_becomes_inference_options(self):
        from back.objects.registry.scheduler_tasks import reasoning

        ctx = _ctx(
            task_type="reasoning",
            config={
                "phases": {"tbox": True, "swrl": True},
                "materialize_graph": True,
                "materialize_delta": True,
                "materialize_table": "cat.sch.inferred",
            },
        )
        with patch.object(DigitalTwin, "run_inference_task") as run_inference:
            reasoning.run(ctx)

        options = run_inference.call_args[0][4]
        assert options["tbox"] is True
        assert options["graph"] is False
        assert options["append_graph"] is True
        assert options["materialize"] is True
        assert options["materialize_table"] == "cat.sch.inferred"
        assert run_inference.call_args[1]["build_kind"] == "scheduled"

    def test_an_invalid_config_stops_before_the_delegate(self):
        from back.objects.registry.scheduler_tasks import reasoning

        ctx = _ctx(
            task_type="reasoning",
            config={"phases": {"swrl": True}, "materialize_graph": False},
        )
        with patch.object(DigitalTwin, "run_inference_task") as run_inference:
            with pytest.raises(ValidationError):
                reasoning.run(ctx)
        run_inference.assert_not_called()


# ---------------------------------------------------------------------
# Harness status derivation
# ---------------------------------------------------------------------


class TestOutcomeFromTask:
    """Delegating types complete their own TaskManager task, so the
    harness reads the finished task rather than completing it twice."""

    def _tm(self, status, *, result=None, message="", error=""):
        task = SimpleNamespace(
            status=status, result=result, message=message, error=error
        )
        tm = MagicMock()
        tm.get_task.return_value = task
        return tm

    def test_a_completed_task_yields_its_count_and_detail(self):
        from back.objects.registry.scheduler import _outcome_from_task

        tm = self._tm(
            TaskStatus.COMPLETED,
            result={"node_count": 314, "duration_ms": 900, "unrelated": 1},
            message="Analytics complete",
        )
        outcome = _outcome_from_task(tm, "task-1", get_task_type("analytics"))

        assert outcome.status == "success"
        assert outcome.count == 314
        assert outcome.detail == {"node_count": 314, "duration_ms": 900}

    def test_a_failed_task_surfaces_its_error(self):
        from back.objects.registry.scheduler import _outcome_from_task

        tm = self._tm(TaskStatus.FAILED, error="warehouse unreachable")
        outcome = _outcome_from_task(tm, "task-1", get_task_type("reasoning"))

        assert outcome.status == "error"
        assert outcome.message == "warehouse unreachable"

    def test_a_vanished_task_is_an_error_not_a_crash(self):
        from back.objects.registry.scheduler import _outcome_from_task

        tm = MagicMock()
        tm.get_task.return_value = None
        outcome = _outcome_from_task(tm, "task-1", get_task_type("analytics"))

        assert outcome.status == "error"
        assert outcome.message


# ---------------------------------------------------------------------
# The harness envelope
# ---------------------------------------------------------------------


class _Harness:
    """Drives ``_run_scheduled_task`` with every collaborator stubbed."""

    def __init__(self, monkeypatch, spec_run, *, delegates=False, on_finish=None):
        from back.objects.registry import scheduler as sched_mod
        from back.objects.registry.scheduler_tasks import TaskTypeSpec

        self.module = sched_mod
        self.status_calls = []
        self.tm = MagicMock()
        self.tm.create_task.return_value = SimpleNamespace(id="task-9")

        spec = TaskTypeSpec(
            key="probe",
            label="Probe",
            task_tag="scheduled_probe",
            steps=[{"name": "a", "description": "a"}],
            normalize_config=lambda cfg: dict(cfg or {}),
            run=spec_run,
            delegates_task_lifecycle=delegates,
            on_finish=on_finish,
            count_key="node_count",
        )
        monkeypatch.setattr(sched_mod, "get_task_type", lambda key: spec)
        monkeypatch.setattr(
            sched_mod, "_update_schedule_status", self._record_status
        )
        monkeypatch.setattr(
            sched_mod.get_scheduler(),
            "_resolve_creds",
            lambda settings: ("https://host", "tok", {"catalog": "c", "schema": "s"}),
        )
        monkeypatch.setattr(
            "back.core.task_manager.get_task_manager", lambda: self.tm
        )

    def _record_status(self, host, token, reg, key, outcome, duration_s=0.0, run_ts=""):
        self.status_calls.append((key, outcome, duration_s))

    def fire(self, config=None):
        self.module._run_scheduled_task(
            "probe",
            "Acme",
            "",
            settings=SimpleNamespace(),
            registry_cfg={"catalog": "c", "schema": "s"},
            version="latest",
            config=config or {},
        )
        return self.status_calls[-1] if self.status_calls else None


apscheduler = pytest.importorskip("apscheduler")


class TestHarness:
    def test_a_success_is_persisted_and_the_task_completed(self, monkeypatch):
        h = _Harness(
            monkeypatch,
            lambda ctx: RunOutcome(status="success", message="did it", count=12),
        )
        key, outcome, duration = h.fire()

        assert key == "probe::Acme::"
        assert (outcome.status, outcome.message, outcome.count) == (
            "success",
            "did it",
            12,
        )
        assert duration >= 0
        h.tm.complete_task.assert_called_once()
        h.tm.fail_task.assert_not_called()

    def test_an_exception_becomes_an_error_row_rather_than_escaping(
        self, monkeypatch
    ):
        def _boom(ctx):
            raise RuntimeError("no warehouse")

        h = _Harness(monkeypatch, _boom)
        _key, outcome, _duration = h.fire()

        assert outcome.status == "error"
        assert outcome.message == "no warehouse"
        h.tm.fail_task.assert_called_once_with("task-9", "no warehouse")

    def test_a_delegating_type_is_not_completed_twice(self, monkeypatch):
        h = _Harness(monkeypatch, lambda ctx: None, delegates=True)
        h.tm.get_task.return_value = SimpleNamespace(
            status=TaskStatus.COMPLETED,
            result={"node_count": 5},
            message="done",
            error="",
        )
        _key, outcome, _duration = h.fire()

        assert outcome.count == 5
        h.tm.complete_task.assert_not_called()
        h.tm.fail_task.assert_not_called()

    def test_a_delegating_type_that_never_started_is_still_failed(self, monkeypatch):
        """A failure raised *before* the delegate ran leaves the task
        running; the harness has to close it."""

        def _boom(ctx):
            raise RuntimeError("preflight said no")

        h = _Harness(monkeypatch, _boom, delegates=True)
        h.fire()

        h.tm.fail_task.assert_called_once_with("task-9", "preflight said no")

    def test_the_on_finish_hook_sees_the_outcome(self, monkeypatch):
        seen = []
        h = _Harness(
            monkeypatch,
            lambda ctx: RunOutcome(status="success", count=3),
            on_finish=lambda ctx, outcome, duration: seen.append(outcome),
        )
        h.fire()

        assert len(seen) == 1
        assert seen[0].count == 3

    def test_a_broken_on_finish_hook_does_not_break_the_run(self, monkeypatch):
        def _boom_hook(ctx, outcome, duration):
            raise RuntimeError("trace write failed")

        h = _Harness(
            monkeypatch,
            lambda ctx: RunOutcome(status="success"),
            on_finish=_boom_hook,
        )
        _key, outcome, _duration = h.fire()

        assert outcome.status == "success"
