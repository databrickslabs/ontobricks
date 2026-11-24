"""Community and Pattern Detect tasks write one run-history row each.

Completed and failed Detect runs are written through
``DetectRunHistory`` into the registry store (``store.record_{community,
pattern}_run``); Materialize and Save are not. Recording is best-effort: a
failing write must never change the task outcome. ``RegistryService.from_context``
is the only seam mocked, the same one the Settings Runs endpoint tests mock, so
the task control flow is real.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import InfrastructureError
from back.objects.digitaltwin.DetectRunHistory import DetectRunHistory
from back.objects.digitaltwin.TwinAnalytics import TwinAnalytics
from back.objects.digitaltwin.PatternBackgroundTasks import PatternBackgroundTasks
from back.objects.digitaltwin.TwinBackgroundTasks import TwinBackgroundTasks

pytestmark = pytest.mark.unit

FROM_CONTEXT = "back.objects.registry.RegistryService.RegistryService.from_context"


def _domain(**over):
    base = dict(uc_domain_folder="Sales Domain", current_version="4", info={"name": "Sales"},
                last_build="")
    base.update(over)
    return SimpleNamespace(**base)


def _settings():
    return SimpleNamespace()


@pytest.fixture
def registry():
    """The registry store the history rows are written to."""
    svc = MagicMock()
    with patch(FROM_CONTEXT, return_value=svc):
        yield svc.store


def _recorded(store, method):
    """Return ``(folder, version, entry)`` of the single recorded row."""
    getattr(store, method).assert_called_once()
    folder, version, entry = getattr(store, method).call_args.args
    return folder, version, entry


# ---------------------------------------------------------------------------
# Row building
# ---------------------------------------------------------------------------


def test_a_failed_community_run_defaults_every_metric_to_zero(registry):
    DetectRunHistory.record_community(
        _domain(), _settings(), task_id="t", class_filter=[], started=0.0, error="boom",
    )

    _, _, entry = _recorded(registry, "record_community_run")
    assert entry["status"] == "failed"
    assert entry["node_count"] == 0
    assert entry["cluster_count"] == 0
    assert entry["converged"] is True
    assert entry["run_page_url"] == ""
    assert entry["error"] == "boom"
    assert entry["computed_at"]


def test_a_pattern_run_falls_back_to_the_requested_parameters(registry):
    DetectRunHistory.record_pattern(
        _domain(), _settings(), task_id="t", anchor_type="http://x/Person",
        hops=3, min_support=2, started=0.0,
        stats={"pattern_count": 7, "anchor_count": 40},
    )

    _, _, entry = _recorded(registry, "record_pattern_run")
    assert entry["anchor_type"] == "http://x/Person"
    assert entry["hop_count"] == 3
    assert entry["min_support"] == 2
    assert entry["pattern_count"] == 7
    assert entry["anchor_count"] == 40
    assert entry["detail_level"] == ""


def test_the_job_elapsed_time_wins_over_wall_clock(registry):
    DetectRunHistory.record_community(
        _domain(), _settings(), task_id="t", class_filter=[], started=0.0,
        stats={"elapsed_ms": 850},
    )

    _, _, entry = _recorded(registry, "record_community_run")
    assert entry["duration_ms"] == 850


# ---------------------------------------------------------------------------
# Community detection
# ---------------------------------------------------------------------------


class TestCommunityRunRecording:
    def test_completed_run_is_recorded_with_the_job_stats(self, registry):
        tm = MagicMock()
        stats = {
            "cluster_count": 4,
            "node_count": 90,
            "converged": False,
            "elapsed_ms": 850,
            "run_page_url": "https://dbx/run/9",
        }
        with patch.object(TwinAnalytics, "detect_clusters", return_value=stats):
            TwinBackgroundTasks.run_community_task(
                tm, "t", _domain(), _settings(), "g", class_filter=["http://x/Person"]
            )

        folder, version, entry = _recorded(registry, "record_community_run")
        assert (folder, version) == ("Sales Domain", "4")
        assert entry["status"] == "completed"
        assert entry["class_filter"] == ["http://x/Person"]
        assert entry["node_count"] == 90
        assert entry["cluster_count"] == 4
        assert entry["converged"] is False
        assert entry["duration_ms"] == 850
        assert entry["run_page_url"] == "https://dbx/run/9"
        assert entry["task_id"] == "t"
        assert entry["error"] == ""
        tm.complete_task.assert_called_once()

    def test_failed_run_is_recorded_with_the_error_and_still_fails_the_task(self, registry):
        tm = MagicMock()
        boom = InfrastructureError("Community detection job failed", detail="run https://x/run/1")
        with patch.object(TwinAnalytics, "detect_clusters", side_effect=boom):
            TwinBackgroundTasks.run_community_task(tm, "t", _domain(), _settings(), "g")

        _, _, entry = _recorded(registry, "record_community_run")
        assert entry["status"] == "failed"
        assert entry["error"] == "Community detection job failed: run https://x/run/1"
        assert entry["node_count"] == 0
        assert tm.fail_task.call_args.args == ("t", entry["error"])
        tm.complete_task.assert_not_called()

    def test_a_failing_history_write_does_not_change_the_outcome(self, registry):
        registry.record_community_run.side_effect = RuntimeError("lakebase down")
        tm = MagicMock()
        with patch.object(
            TwinAnalytics, "detect_clusters",
            return_value={"cluster_count": 2, "node_count": 10},
        ):
            TwinBackgroundTasks.run_community_task(tm, "t", _domain(), _settings(), "g")

        tm.complete_task.assert_called_once()
        tm.fail_task.assert_not_called()

    def test_missing_folder_or_version_skips_the_write(self, registry):
        tm = MagicMock()
        with patch.object(TwinAnalytics, "detect_clusters", return_value={"cluster_count": 1}):
            TwinBackgroundTasks.run_community_task(
                tm, "t", _domain(uc_domain_folder=""), _settings(), "g"
            )

        registry.record_community_run.assert_not_called()
        tm.complete_task.assert_called_once()


# ---------------------------------------------------------------------------
# Pattern detection
# ---------------------------------------------------------------------------


class TestPatternRunRecording:
    def test_completed_run_is_recorded_with_the_job_stats(self, registry):
        tm = MagicMock()
        stats = {
            "pattern_count": 12,
            "anchor_count": 500,
            "detail_level": "neighbour",
            "elapsed_ms": 900,
            "run_page_url": "https://dbx/run/3",
        }
        job = MagicMock()
        job.top_patterns.return_value = [{"id": "pat-1"}]
        with patch.object(TwinAnalytics, "detect_patterns", return_value=stats), patch.object(
            TwinAnalytics, "build_job_patterns", return_value=job
        ), patch.object(
            TwinAnalytics,
            "save_pattern",
            return_value={
                "saved_id": "person-abc",
                "label": "Person — 2026/11/16 16:40:00",
                "pattern_id": "pat-1",
            },
        ) as save:
            PatternBackgroundTasks.run_pattern_task(
                tm, "t", _domain(), _settings(), "g",
                class_filter="http://x/Person", hops=2, min_support=4,
            )

        folder, version, entry = _recorded(registry, "record_pattern_run")
        assert (folder, version) == ("Sales Domain", "4")
        assert entry["status"] == "completed"
        assert entry["anchor_type"] == "http://x/Person"
        assert entry["hop_count"] == 2
        assert entry["min_support"] == 4
        assert entry["detail_level"] == "neighbour"
        assert entry["pattern_count"] == 12
        assert entry["anchor_count"] == 500
        assert entry["duration_ms"] == 900
        assert entry["run_page_url"] == "https://dbx/run/3"
        tm.complete_task.assert_called_once()
        result = tm.complete_task.call_args.kwargs["result"]
        assert result["saved_id"] == "person-abc"
        assert result["pattern_id"] == "pat-1"
        save.assert_called_once()
        assert "Person —" in save.call_args.args[2]

    def test_zero_patterns_skips_auto_save(self, registry):
        tm = MagicMock()
        with patch.object(
            TwinAnalytics, "detect_patterns",
            return_value={"pattern_count": 0, "anchor_count": 10},
        ), patch.object(TwinAnalytics, "save_pattern") as save:
            PatternBackgroundTasks.run_pattern_task(
                tm, "t", _domain(), _settings(), "g", class_filter="http://x/Person"
            )
        save.assert_not_called()
        result = tm.complete_task.call_args.kwargs["result"]
        assert "saved_id" not in result

    def test_failed_run_is_recorded_with_the_error_and_still_fails_the_task(self, registry):
        tm = MagicMock()
        boom = InfrastructureError("Pattern detection job failed", detail="cancelled")
        with patch.object(TwinAnalytics, "detect_patterns", side_effect=boom):
            PatternBackgroundTasks.run_pattern_task(
                tm, "t", _domain(), _settings(), "g", class_filter=" http://x/Person "
            )

        _, _, entry = _recorded(registry, "record_pattern_run")
        assert entry["status"] == "failed"
        assert entry["anchor_type"] == "http://x/Person"
        assert entry["error"] == "Pattern detection job failed: cancelled"
        assert tm.fail_task.call_args.args == ("t", entry["error"])
        tm.complete_task.assert_not_called()

    def test_a_failing_history_write_does_not_change_the_outcome(self, registry):
        registry.record_pattern_run.side_effect = RuntimeError("lakebase down")
        tm = MagicMock()
        job = MagicMock()
        job.top_patterns.return_value = [{"id": "pat-1"}]
        with patch.object(
            TwinAnalytics, "detect_patterns",
            return_value={"pattern_count": 1, "anchor_count": 2},
        ), patch.object(
            TwinAnalytics, "build_job_patterns", return_value=job
        ), patch.object(
            TwinAnalytics,
            "save_pattern",
            return_value={"saved_id": "s1", "label": "L", "pattern_id": "pat-1"},
        ):
            PatternBackgroundTasks.run_pattern_task(
                tm, "t", _domain(), _settings(), "g", class_filter="http://x/Person"
            )

        tm.complete_task.assert_called_once()
        tm.fail_task.assert_not_called()

    def test_save_failure_after_detect_fails_the_task_without_a_second_history_row(
        self, registry
    ):
        tm = MagicMock()
        job = MagicMock()
        job.top_patterns.return_value = [{"id": "pat-1"}]
        with patch.object(
            TwinAnalytics, "detect_patterns",
            return_value={"pattern_count": 1, "anchor_count": 2},
        ), patch.object(
            TwinAnalytics, "build_job_patterns", return_value=job
        ), patch.object(
            TwinAnalytics, "save_pattern", side_effect=RuntimeError("warehouse down")
        ):
            PatternBackgroundTasks.run_pattern_task(
                tm, "t", _domain(), _settings(), "g", class_filter="http://x/Person"
            )

        assert registry.record_pattern_run.call_count == 1
        _, _, entry = _recorded(registry, "record_pattern_run")
        assert entry["status"] == "completed"
        tm.fail_task.assert_called_once()
        tm.complete_task.assert_not_called()

    def test_materialize_style_tasks_do_not_write_history(self, registry):
        """Standalone Save still exists for the API; it does not write Detect history."""
        tm = MagicMock()
        with patch.object(TwinAnalytics, "save_pattern", return_value={"label": "x", "size": 1}):
            PatternBackgroundTasks.run_pattern_save_task(
                tm, "t", _domain(), _settings(), "g", pattern_id="p1", label="x"
            )

        registry.record_pattern_run.assert_not_called()
        registry.record_community_run.assert_not_called()
