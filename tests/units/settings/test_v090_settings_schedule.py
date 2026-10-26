"""ScheduleSettings (0.9.0): scheduled tasks (build, graph cache refresh, ...),
run history, build analytics, scheduler status, and the cohort-rule picker."""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock

import pytest

from back.core.errors import (
    InfrastructureError,
    NotFoundError,
    ValidationError,
)
from back.objects.domain.settings.ScheduleSettings import ScheduleSettings
from back.objects.domain.settings.SettingsService import SettingsService
from back.objects.registry.schedule import TASK_BUILD, TASK_CACHE_REFRESH

pytestmark = pytest.mark.unit

_ss = importlib.import_module("back.objects.domain.settings.SettingsService")

CFG = {"catalog": "cat", "schema": "sch"}


@pytest.fixture
def scheduler(monkeypatch):
    sched = MagicMock()
    monkeypatch.setattr(SettingsService, "_get_scheduler", staticmethod(lambda: sched))
    monkeypatch.setattr(
        SettingsService,
        "_resolve_context",
        staticmethod(lambda sm, st: (MagicMock(), "h", "t", CFG)),
    )
    return sched


@pytest.fixture
def svc(monkeypatch):
    registry = MagicMock()
    registry.cfg.is_configured = True
    rs_cls = MagicMock()
    rs_cls.from_context.return_value = registry
    monkeypatch.setattr(_ss, "RegistryService", rs_cls)
    monkeypatch.setattr(_ss, "get_domain", lambda sm: MagicMock())
    monkeypatch.setattr(
        SettingsService,
        "_resolve_context",
        staticmethod(lambda sm, st: (MagicMock(), "h", "t", CFG)),
    )
    return registry


@pytest.mark.parametrize(
    "nbytes,expected",
    [
        (0, "0 B"),
        (1023, "1023 B"),
        (1024, "1.0 KB"),
        (1536, "1.5 KB"),
        (5 * 1024**2, "5.0 MB"),
        (3 * 1024**3, "3.0 GB"),
        (1024**5, "1.0 PB"),
    ],
)
def test_human_size(nbytes, expected):
    assert ScheduleSettings.human_size(nbytes) == expected


# ---------------------------------------------------------------------------
# Schedules CRUD
# ---------------------------------------------------------------------------


class TestListSchedules:
    def test_returns_entries_and_task_type_catalogue(self, scheduler):
        scheduler.get_all_schedules.return_value = [{"task_type": "build"}]

        result = ScheduleSettings.list_schedules_result(MagicMock(), MagicMock())

        assert result["schedules"] == [{"task_type": "build"}]
        labels = {t["key"]: t["label"] for t in result["task_types"]}
        assert labels[TASK_CACHE_REFRESH] == "Graph Cache Refresh"
        assert labels[TASK_BUILD] == "Knowledge Graph Build"
        scheduler.get_all_schedules.assert_called_once_with("h", "t", CFG)

    def test_unexpected_error_is_wrapped(self, scheduler):
        scheduler.get_all_schedules.side_effect = RuntimeError("db")
        with pytest.raises(InfrastructureError, match="Failed to list schedules"):
            ScheduleSettings.list_schedules_result(MagicMock(), MagicMock())

    def test_domain_errors_propagate(self, scheduler):
        scheduler.get_all_schedules.side_effect = ValidationError("bad")
        with pytest.raises(ValidationError):
            ScheduleSettings.list_schedules_result(MagicMock(), MagicMock())


class TestSaveSchedule:
    def test_defaults_and_legacy_project_name(self, scheduler):
        scheduler.save_schedule.return_value = (True, "saved")
        settings = MagicMock()

        result = ScheduleSettings.save_schedule_result(
            {"project_name": " claims ", "config": "not-a-dict"}, MagicMock(), settings
        )

        assert result == {"success": True, "message": "saved"}
        scheduler.save_schedule.assert_called_once_with(
            "h", "t", CFG, settings, "build", "claims", 60,
            target_key="", enabled=True, version="latest", config={},
        )

    def test_graph_cache_refresh_options_are_forwarded(self, scheduler):
        scheduler.save_schedule.return_value = (True, "ok")
        ScheduleSettings.save_schedule_result(
            {
                "task_type": TASK_CACHE_REFRESH,
                "domain_name": "claims",
                "target_key": "rule-1",
                "interval_minutes": "15",
                "enabled": False,
                "version": "3",
                "config": {"warm": True},
            },
            MagicMock(),
            MagicMock(),
        )
        args, kwargs = scheduler.save_schedule.call_args
        assert args[4:] == (TASK_CACHE_REFRESH, "claims", 15)
        assert kwargs == {"target_key": "rule-1", "enabled": False, "version": "3", "config": {"warm": True}}

    def test_missing_domain_raises_validation(self, scheduler):
        with pytest.raises(ValidationError, match="Domain name is required"):
            ScheduleSettings.save_schedule_result({"task_type": "build"}, MagicMock(), MagicMock())
        scheduler.save_schedule.assert_not_called()

    def test_rejected_schedule_raises_validation_with_message(self, scheduler):
        scheduler.save_schedule.return_value = (False, "interval too small")
        with pytest.raises(ValidationError, match="interval too small"):
            ScheduleSettings.save_schedule_result({"domain_name": "d"}, MagicMock(), MagicMock())

    def test_non_numeric_interval_is_wrapped(self, scheduler):
        with pytest.raises(InfrastructureError, match="Failed to save schedule"):
            ScheduleSettings.save_schedule_result(
                {"domain_name": "d", "interval_minutes": "soon"}, MagicMock(), MagicMock()
            )


class TestScheduleHistory:
    def test_returns_history_with_echoed_keys(self, scheduler):
        scheduler.get_schedule_history.return_value = [{"status": "ok"}]
        result = ScheduleSettings.get_schedule_history_result(
            "build", "claims", MagicMock(), MagicMock(), target_key="k"
        )
        assert result == {
            "success": True,
            "task_type": "build",
            "domain_name": "claims",
            "target_key": "k",
            "history": [{"status": "ok"}],
        }
        scheduler.get_schedule_history.assert_called_once_with("h", "t", CFG, "build", "claims", "k")

    def test_unexpected_error_is_wrapped(self, scheduler):
        scheduler.get_schedule_history.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="schedule history"):
            ScheduleSettings.get_schedule_history_result("build", "c", MagicMock(), MagicMock())

    def test_domain_errors_propagate(self, scheduler):
        scheduler.get_schedule_history.side_effect = NotFoundError("gone")
        with pytest.raises(NotFoundError):
            ScheduleSettings.get_schedule_history_result("build", "c", MagicMock(), MagicMock())


class TestDeleteSchedule:
    def test_success(self, scheduler):
        scheduler.remove_schedule.return_value = (True, "removed")
        assert ScheduleSettings.delete_schedule_result(
            "build", "claims", MagicMock(), MagicMock(), target_key="k"
        ) == {"success": True, "message": "removed"}
        scheduler.remove_schedule.assert_called_once_with("h", "t", CFG, "build", "claims", "k")

    def test_missing_schedule_raises_not_found(self, scheduler):
        scheduler.remove_schedule.return_value = (False, "no such schedule")
        with pytest.raises(NotFoundError, match="no such schedule"):
            ScheduleSettings.delete_schedule_result("build", "c", MagicMock(), MagicMock())

    def test_unexpected_error_is_wrapped(self, scheduler):
        scheduler.remove_schedule.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Failed to remove schedule"):
            ScheduleSettings.delete_schedule_result("build", "c", MagicMock(), MagicMock())


class TestTriggerScheduleNow:
    def test_success(self, scheduler):
        scheduler.run_schedule_now.return_value = (True, "queued")
        settings = MagicMock()
        assert ScheduleSettings.trigger_schedule_now_result(
            TASK_CACHE_REFRESH, "claims", MagicMock(), settings
        ) == {"success": True, "message": "queued"}
        scheduler.run_schedule_now.assert_called_once_with(
            "h", "t", CFG, settings, TASK_CACHE_REFRESH, "claims", ""
        )

    def test_refused_trigger_raises_infrastructure(self, scheduler):
        scheduler.run_schedule_now.return_value = (False, "already running")
        with pytest.raises(InfrastructureError) as exc_info:
            ScheduleSettings.trigger_schedule_now_result("build", "c", MagicMock(), MagicMock())
        assert exc_info.value.detail == "already running"

    def test_unexpected_error_is_wrapped(self, scheduler):
        scheduler.run_schedule_now.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Failed to trigger schedule"):
            ScheduleSettings.trigger_schedule_now_result("build", "c", MagicMock(), MagicMock())


def test_scheduler_status_payload_merges_status(scheduler):
    scheduler.status.return_value = {"running": True, "jobs": 3}
    assert ScheduleSettings.scheduler_status_payload() == {
        "success": True,
        "running": True,
        "jobs": 3,
    }


# ---------------------------------------------------------------------------
# Runs / analytics
# ---------------------------------------------------------------------------


class TestBuildRuns:
    def test_returns_runs(self, svc):
        svc.load_build_runs.return_value = [{"id": 1}]
        result = ScheduleSettings.get_build_runs_result(
            "claims", MagicMock(), MagicMock(), version="2", limit=5
        )
        assert result == {"success": True, "domain_name": "claims", "version": "2", "runs": [{"id": 1}]}
        svc.load_build_runs.assert_called_once_with("claims", version="2", limit=5)

    def test_unconfigured_registry_raises_validation(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError):
            ScheduleSettings.get_build_runs_result("claims", MagicMock(), MagicMock())

    def test_unexpected_error_is_wrapped(self, svc):
        svc.load_build_runs.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Failed to load build runs"):
            ScheduleSettings.get_build_runs_result("claims", MagicMock(), MagicMock())


class TestAllRuns:
    def test_build_kind_reads_build_runs(self, svc):
        svc.load_all_build_runs.return_value = ([{"id": 1}], 7)
        result = ScheduleSettings._all_runs_result(
            "build", MagicMock(), MagicMock(), folder=None, limit=25, offset=0
        )
        assert result == {
            "success": True, "domain": None, "runs": [{"id": 1}],
            "total": 7, "limit": 25, "offset": 0,
        }
        svc.load_all_graph_analytics_runs.assert_not_called()

    def test_analytics_kind_reads_analytics_runs(self, svc):
        svc.load_all_graph_analytics_runs.return_value = ([], 0)
        ScheduleSettings._all_runs_result(
            "analytics", MagicMock(), MagicMock(), folder="claims", limit=10, offset=20
        )
        svc.load_all_graph_analytics_runs.assert_called_once_with(folder="claims", limit=10, offset=20)

    def test_unconfigured_registry_raises_validation(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError):
            ScheduleSettings._all_runs_result("build", MagicMock(), MagicMock(), folder=None, limit=1, offset=0)

    def test_unexpected_error_mentions_kind(self, svc):
        svc.load_all_graph_analytics_runs.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Failed to load analytics runs"):
            ScheduleSettings._all_runs_result(
                "analytics", MagicMock(), MagicMock(), folder=None, limit=1, offset=0
            )

    @pytest.mark.parametrize(
        "method,kind",
        [("get_all_build_runs_result", "build"), ("get_all_analytics_runs_result", "analytics")],
    )
    def test_public_wrappers_select_kind(self, monkeypatch, method, kind):
        spy = MagicMock(return_value={"ok": True})
        monkeypatch.setattr(SettingsService, "_all_runs_result", spy)
        sm, st = MagicMock(), MagicMock()

        assert getattr(ScheduleSettings, method)(sm, st, folder="f", limit=3, offset=6) == {"ok": True}
        spy.assert_called_once_with(kind, sm, st, folder="f", limit=3, offset=6)


class TestBuildAnalytics:
    def test_returns_analytics(self, svc):
        svc.build_analytics.return_value = {"runs": 4}
        assert ScheduleSettings.get_build_analytics_result("claims", MagicMock(), MagicMock(), version="1") == {
            "success": True, "domain_name": "claims", "version": "1", "analytics": {"runs": 4},
        }

    def test_unconfigured_registry_raises_validation(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError):
            ScheduleSettings.get_build_analytics_result("claims", MagicMock(), MagicMock())

    def test_unexpected_error_is_wrapped(self, svc):
        svc.build_analytics.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Failed to load build analytics"):
            ScheduleSettings.get_build_analytics_result("claims", MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# Cohort-rule picker
# ---------------------------------------------------------------------------


def _rule(rid, label="", table=None, graph=None, class_uri="ex:C"):
    output = {}
    if graph is not None:
        output["graph"] = graph
    if table:
        output["uc_table"] = table
    return {"id": rid, "label": label, "class_uri": class_uri, "output": output}


class TestListCohortRulesForDomain:
    def _call(self):
        return ScheduleSettings.list_cohort_rules_for_domain_result("claims", MagicMock(), MagicMock())

    def test_reads_rules_from_versioned_document(self, svc):
        doc = {
            "info": {},
            "versions": {
                "2": {
                    "ontology": {
                        "cohort_rules": [
                            _rule("r1", "High value", table={"catalog": "c", "schema": "s", "table_name": "t"}),
                            _rule("r2", graph=False),
                            {"label": "no id, ignored"},
                        ]
                    }
                }
            },
        }
        svc.load_latest_domain_data.return_value = (True, doc, "2", "")

        result = self._call()

        assert result == {
            "success": True,
            "rules": [
                {
                    "id": "r1",
                    "label": "High value",
                    "class_uri": "ex:C",
                    "output": {
                        "graph": True,
                        "uc_table": {"catalog": "c", "schema": "s", "table_name": "t"},
                    },
                },
                {"id": "r2", "label": "r2", "class_uri": "ex:C", "output": {"graph": False, "uc_table": None}},
            ],
        }

    def test_falls_back_to_highest_version_key(self, svc):
        doc = {
            "versions": {
                "1.9": {"ontology": {"cohort_rules": [_rule("old")]}},
                "1.10": {"ontology": {"cohort_rules": [_rule("new")]}},
            }
        }
        svc.load_latest_domain_data.return_value = (True, doc, "missing", "")
        assert [r["id"] for r in self._call()["rules"]] == ["new"]

    def test_non_numeric_version_keys_use_first_entry(self, svc):
        doc = {
            "versions": {
                "alpha": {"ontology": {"cohort_rules": [_rule("a")]}},
                "beta": {"ontology": {"cohort_rules": [_rule("b")]}},
            }
        }
        svc.load_latest_domain_data.return_value = (True, doc, "zzz", "")
        assert [r["id"] for r in self._call()["rules"]] == ["a"]

    def test_legacy_flat_shapes(self, svc):
        svc.load_latest_domain_data.return_value = (
            True, {"ontology": {"cohort_rules": [_rule("flat")]}}, "1", ""
        )
        assert [r["id"] for r in self._call()["rules"]] == ["flat"]

        svc.load_latest_domain_data.return_value = (True, {"cohort_rules": [_rule("top")]}, "1", "")
        assert [r["id"] for r in self._call()["rules"]] == ["top"]

    def test_non_dict_payload_yields_no_rules(self, svc):
        svc.load_latest_domain_data.return_value = (True, "garbage", "1", "")
        assert self._call() == {"success": True, "rules": []}

    def test_missing_domain_raises_not_found(self, svc):
        svc.load_latest_domain_data.return_value = (False, None, None, "")
        with pytest.raises(NotFoundError, match="'claims' not found"):
            self._call()

    def test_unconfigured_registry_raises_validation(self, svc):
        svc.cfg.is_configured = False
        with pytest.raises(ValidationError):
            self._call()

    def test_unexpected_error_is_wrapped(self, svc):
        svc.load_latest_domain_data.side_effect = RuntimeError("x")
        with pytest.raises(InfrastructureError, match="Failed to list cohort rules"):
            self._call()
