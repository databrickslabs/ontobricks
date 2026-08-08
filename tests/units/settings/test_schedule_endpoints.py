"""The five generic schedule endpoints and their service wrappers.

Ten endpoints (five build, five cohort) collapsed into one surface where
the task type is a path or body field. These tests pin what that surface
promises: the type and target reach the scheduler, per-type options
travel in ``config``, and the type catalogue ships with the listing so
the settings UI does not hardcode the list a second time.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.objects.domain.SettingsService import SettingsService

pytestmark = pytest.mark.unit

MODULE = "back.objects.domain.SettingsService"


@pytest.fixture
def scheduler():
    """A stubbed BuildScheduler behind resolved credentials."""
    sched = MagicMock()
    with patch.object(
        SettingsService,
        "_resolve_context",
        return_value=(None, "https://host", "tok", {"catalog": "c"}),
    ), patch.object(SettingsService, "_get_scheduler", return_value=sched):
        yield sched


class TestListing:
    def test_it_returns_every_type_plus_the_catalogue(self, scheduler):
        scheduler.get_all_schedules.return_value = [
            {"task_type": "analytics", "domain_name": "Acme"}
        ]

        out = SettingsService.list_schedules_result(MagicMock(), MagicMock())

        assert out["success"] is True
        assert out["schedules"][0]["task_type"] == "analytics"
        assert {t["key"] for t in out["task_types"]} == {
            "build",
            "cohort",
            "analytics",
            "reasoning",
        }


class TestSaving:
    def _save(self, scheduler, **body):
        scheduler.save_schedule.return_value = (True, "saved")
        data = {"domain_name": "Acme", "interval_minutes": 30}
        data.update(body)
        return SettingsService.save_schedule_result(data, MagicMock(), MagicMock())

    def test_the_type_target_and_config_reach_the_scheduler(self, scheduler):
        self._save(
            scheduler,
            task_type="cohort",
            target_key="rule_1",
            config={"output_graph": True, "output_uc": False},
        )

        kwargs = scheduler.save_schedule.call_args[1]
        assert kwargs["target_key"] == "rule_1"
        assert kwargs["config"] == {"output_graph": True, "output_uc": False}
        assert scheduler.save_schedule.call_args[0][4] == "cohort"

    def test_it_defaults_to_a_build(self, scheduler):
        self._save(scheduler)
        assert scheduler.save_schedule.call_args[0][4] == "build"

    def test_a_missing_domain_is_a_validation_error(self, scheduler):
        with pytest.raises(ValidationError):
            self._save(scheduler, domain_name="")

    def test_a_rejected_config_surfaces_as_a_validation_error(self, scheduler):
        """The task type validates its own options; a 400 is the honest
        answer, not a 500."""
        scheduler.save_schedule.return_value = (False, "Enable at least one phase")

        with pytest.raises(ValidationError) as exc:
            SettingsService.save_schedule_result(
                {"task_type": "reasoning", "domain_name": "Acme"},
                MagicMock(),
                MagicMock(),
            )
        assert "phase" in str(exc.value)

    def test_a_non_dict_config_is_ignored_rather_than_crashing(self, scheduler):
        self._save(scheduler, config="not a dict")
        assert scheduler.save_schedule.call_args[1]["config"] == {}


class TestHistoryDeleteAndRunNow:
    def test_history_is_scoped_to_the_composite_key(self, scheduler):
        scheduler.get_schedule_history.return_value = [{"status": "success"}]

        out = SettingsService.get_schedule_history_result(
            "cohort", "Acme", MagicMock(), MagicMock(), target_key="rule_1"
        )

        assert scheduler.get_schedule_history.call_args[0][3:] == (
            "cohort",
            "Acme",
            "rule_1",
        )
        assert out["task_type"] == "cohort"
        assert out["target_key"] == "rule_1"

    def test_deleting_an_absent_schedule_is_a_not_found(self, scheduler):
        scheduler.remove_schedule.return_value = (False, "No schedule found")

        with pytest.raises(NotFoundError):
            SettingsService.delete_schedule_result(
                "analytics", "Acme", MagicMock(), MagicMock()
            )

    def test_run_now_forwards_the_type_and_target(self, scheduler):
        scheduler.run_schedule_now.return_value = (True, "queued")

        out = SettingsService.trigger_schedule_now_result(
            "reasoning", "Acme", MagicMock(), MagicMock()
        )

        assert out["success"] is True
        assert scheduler.run_schedule_now.call_args[0][4:] == (
            "reasoning",
            "Acme",
            "",
        )


class TestRoutes:
    """The router shape is part of the contract the front-end codes to."""

    def _paths(self):
        from api.routers.internal import settings as settings_router

        return {
            (tuple(sorted(r.methods)), r.path)
            for r in settings_router.router.routes
            if "schedule" in r.path
        }

    def test_the_five_generic_routes_exist(self):
        paths = self._paths()
        assert (("GET",), "/settings/schedules") in paths
        assert (("POST",), "/settings/schedules") in paths
        assert (
            ("GET",),
            "/settings/schedules/{task_type}/{domain_name}/history",
        ) in paths
        assert (("DELETE",), "/settings/schedules/{task_type}/{domain_name}") in paths
        assert (
            ("POST",),
            "/settings/schedules/{task_type}/{domain_name}/run-now",
        ) in paths

    def test_the_cohort_only_surface_is_gone(self):
        from api.routers.internal import settings as settings_router

        assert not [
            r for r in settings_router.router.routes if "cohort-schedules" in r.path
        ]

    def test_the_static_segments_still_resolve(self):
        """``status`` and ``rules/{domain}`` must be declared before the
        ``{task_type}`` routes or FastAPI would swallow them."""
        from api.routers.internal import settings as settings_router

        order = [r.path for r in settings_router.router.routes if "schedule" in r.path]
        generic = order.index("/settings/schedules/{task_type}/{domain_name}/history")
        assert order.index("/settings/schedules/status") < generic
        assert order.index("/settings/schedules/rules/{domain_name}") < generic
