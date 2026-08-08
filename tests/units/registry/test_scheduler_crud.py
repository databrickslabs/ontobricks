"""The scheduler's one CRUD path, shared by all four task types.

`save_schedule`, `remove_schedule`, `run_schedule_now`,
`get_all_schedules` and the APScheduler job registration used to exist
twice — once for builds, once for cohorts. They now exist once and take
``task_type`` plus ``target_key``, so these tests drive each of them
with more than one type to prove the generalisation actually holds
rather than just compiling.

The store is a dict behind a fake, and APScheduler is real but never
started, so jobs are inspected rather than executed.
"""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from back.objects.registry.scheduler import BuildScheduler
from back.objects.registry.store.base import schedule_key

pytestmark = pytest.mark.unit

REG = {"catalog": "cat", "schema": "sch", "volume": "vol"}
HOST = "https://host"
TOKEN = "tok"


class _FakeStore:
    """Just enough RegistryStore for the scheduler's four store calls."""

    def __init__(self) -> None:
        self.schedules: Dict[str, Dict[str, Any]] = {}
        self.history: Dict[str, List[Dict[str, Any]]] = {}
        self.save_error: str = ""

    def load_schedules(self) -> Dict[str, Dict[str, Any]]:
        return {k: dict(v) for k, v in self.schedules.items()}

    def save_schedules(self, schedules):
        if self.save_error:
            return False, self.save_error
        self.schedules = {k: dict(v) for k, v in schedules.items()}
        return True, "ok"

    def load_schedule_history(self, key):
        return list(self.history.get(key, []))

    def append_schedule_history(self, key, entry, *, max_entries=50):
        bucket = self.history.setdefault(key, [])
        bucket.append(dict(entry))
        del bucket[:-max_entries]


@pytest.fixture
def store() -> _FakeStore:
    return _FakeStore()


@pytest.fixture
def sched(store, monkeypatch):
    """A BuildScheduler wired to the fake store.

    APScheduler is started *paused*: jobs get real triggers and next-run
    times, so the assertions below are about the real thing, but nothing
    is ever executed — including the immediate one-shot that
    ``run_schedule_now`` queues.
    """
    s = BuildScheduler()
    monkeypatch.setattr(
        BuildScheduler, "_store_for", staticmethod(lambda h, t, cfg: store)
    )
    monkeypatch.setattr(
        "back.objects.session.global_config_service", MagicMock(), raising=False
    )
    s._sched.start(paused=True)
    s._started = True
    s._settings = MagicMock()
    yield s
    s._sched.shutdown(wait=False)


def _save(sched, task_type="build", domain="Acme", **over):
    kwargs = dict(target_key="", enabled=True, version="latest", config={})
    kwargs.update(over)
    interval = kwargs.pop("interval_minutes", 60)
    return sched.save_schedule(
        HOST, TOKEN, REG, MagicMock(), task_type, domain, interval, **kwargs
    )


class TestJobIds:
    def test_the_id_carries_the_type_and_target(self):
        assert (
            BuildScheduler._job_id("cohort", "Acme", "rule_1")
            == "sched_cohort_Acme__rule_1"
        )

    def test_two_types_on_one_domain_do_not_collide(self):
        """The old unique key was (registry, domain), which could hold
        only one schedule per domain. Distinct job ids are the in-memory
        half of lifting that limit."""
        ids = {
            BuildScheduler._job_id(t, "Acme", "")
            for t in ("build", "cohort", "analytics", "reasoning")
        }
        assert len(ids) == 4

    def test_two_cohort_rules_on_one_domain_do_not_collide(self):
        assert BuildScheduler._job_id(
            "cohort", "Acme", "rule_1"
        ) != BuildScheduler._job_id("cohort", "Acme", "rule_2")


class TestSaveValidation:
    def test_an_unknown_type_is_refused(self, sched, store):
        ok, msg = _save(sched, task_type="teleport")
        assert ok is False
        assert "teleport" in msg
        assert store.schedules == {}

    def test_a_missing_domain_is_refused(self, sched):
        ok, msg = _save(sched, domain="")
        assert (ok, "required" in msg.lower()) == (False, True)

    def test_the_interval_floor_is_enforced(self, sched):
        ok, msg = _save(sched, interval_minutes=1)
        assert ok is False
        assert "2 minutes" in msg

    def test_the_floor_itself_is_allowed(self, sched):
        assert _save(sched, interval_minutes=2)[0] is True

    def test_a_cohort_without_a_rule_is_refused(self, sched):
        ok, msg = _save(sched, task_type="cohort", target_key="")
        assert ok is False
        assert "rule" in msg.lower()

    def test_a_target_on_a_type_that_has_none_is_dropped(self, sched, store):
        """Otherwise it would land in the composite key and silently
        create a second, unreachable analytics schedule."""
        _save(sched, task_type="analytics", target_key="stowaway")

        assert list(store.schedules) == ["analytics::Acme::"]
        assert store.schedules["analytics::Acme::"]["target_key"] == ""

    def test_the_config_is_validated_by_the_type(self, sched, store):
        ok, msg = _save(
            sched,
            task_type="reasoning",
            config={"phases": {"swrl": True}, "materialize_graph": False},
        )
        assert ok is False
        assert "discard" in msg
        assert store.schedules == {}

    def test_the_config_is_normalised_before_it_is_stored(self, sched, store):
        _save(sched, task_type="cohort", target_key="rule_1", config={})

        assert store.schedules["cohort::Acme::rule_1"]["config"] == {
            "output_graph": True,
            "output_uc": True,
        }


class TestSavePersistenceAndJobs:
    def test_saving_registers_a_job(self, sched):
        _save(sched, task_type="analytics", interval_minutes=180)

        job = sched._sched.get_job("sched_analytics_Acme__")
        assert job is not None
        assert job.kwargs["task_type"] == "analytics"
        assert job.kwargs["version"] == "latest"
        assert job.trigger.interval.total_seconds() == 180 * 60

    def test_saving_disabled_stores_it_but_registers_nothing(self, sched, store):
        _save(sched, enabled=False)

        assert store.schedules["build::Acme::"]["enabled"] is False
        assert sched._sched.get_job("sched_build_Acme__") is None

    def test_disabling_an_existing_schedule_unregisters_its_job(self, sched):
        _save(sched)
        assert sched._sched.get_job("sched_build_Acme__") is not None

        _save(sched, enabled=False)
        assert sched._sched.get_job("sched_build_Acme__") is None

    def test_a_failed_write_registers_no_job(self, sched, store):
        """A job whose definition was never persisted would vanish on the
        next restart while still firing until then."""
        store.save_error = "lakebase unreachable"

        ok, msg = _save(sched)

        assert (ok, msg) == (False, "lakebase unreachable")
        assert sched._sched.get_job("sched_build_Acme__") is None

    def test_updating_preserves_the_last_run_columns(self, sched, store):
        _save(sched)
        store.schedules["build::Acme::"].update(
            last_run="2026-08-04T10:00:00+00:00",
            last_status="success",
            last_message="42 triples",
            last_count=42,
        )

        _save(sched, interval_minutes=15)

        row = store.schedules["build::Acme::"]
        assert row["interval_minutes"] == 15
        assert row["last_status"] == "success"
        assert row["last_count"] == 42

    def test_four_types_coexist_on_one_domain(self, sched, store):
        for task_type, target in (
            ("build", ""),
            ("cohort", "rule_1"),
            ("analytics", ""),
            ("reasoning", ""),
        ):
            cfg = (
                {"phases": {"swrl": True}, "materialize_graph": True}
                if task_type == "reasoning"
                else {}
            )
            ok, msg = _save(sched, task_type=task_type, target_key=target, config=cfg)
            assert ok is True, msg

        assert set(store.schedules) == {
            "build::Acme::",
            "cohort::Acme::rule_1",
            "analytics::Acme::",
            "reasoning::Acme::",
        }
        assert len(sched._sched.get_jobs()) == 4


class TestRemove:
    def test_removing_drops_the_row_and_the_job(self, sched, store):
        _save(sched, task_type="cohort", target_key="rule_1")

        ok, _msg = sched.remove_schedule(HOST, TOKEN, REG, "cohort", "Acme", "rule_1")

        assert ok is True
        assert store.schedules == {}
        assert sched._sched.get_job("sched_cohort_Acme__rule_1") is None

    def test_removing_one_type_leaves_the_others(self, sched, store):
        _save(sched, task_type="build")
        _save(sched, task_type="analytics")

        sched.remove_schedule(HOST, TOKEN, REG, "analytics", "Acme")

        assert list(store.schedules) == ["build::Acme::"]
        assert sched._sched.get_job("sched_build_Acme__") is not None

    def test_removing_an_absent_schedule_reports_it(self, sched):
        ok, msg = sched.remove_schedule(HOST, TOKEN, REG, "reasoning", "Acme")
        assert ok is False
        assert "No schedule found" in msg

    def test_a_failed_write_keeps_the_job(self, sched, store):
        _save(sched)
        store.save_error = "lakebase unreachable"

        ok, _msg = sched.remove_schedule(HOST, TOKEN, REG, "build", "Acme")

        assert ok is False
        assert sched._sched.get_job("sched_build_Acme__") is not None


class TestRunNow:
    def _run_now(self, sched, task_type="build", target=""):
        return sched.run_schedule_now(
            HOST, TOKEN, REG, MagicMock(), task_type, "Acme", target
        )

    def test_it_queues_a_one_shot_job(self, sched):
        _save(sched, task_type="analytics")

        ok, msg = self._run_now(sched, "analytics")

        assert ok is True
        assert "Acme" in msg
        manual = [j for j in sched._sched.get_jobs() if j.id.startswith("manual_")]
        assert len(manual) == 1
        assert manual[0].kwargs["task_type"] == "analytics"

    def test_the_recurring_job_and_the_stored_row_are_untouched(self, sched, store):
        _save(sched, interval_minutes=30)
        before = dict(store.schedules["build::Acme::"])

        self._run_now(sched)

        assert store.schedules["build::Acme::"] == before
        assert sched._sched.get_job("sched_build_Acme__") is not None

    def test_it_carries_the_stored_config_not_the_defaults(self, sched):
        """The manual run has to be the same run the timer would do."""
        _save(
            sched,
            task_type="cohort",
            target_key="rule_1",
            config={"output_graph": True, "output_uc": False},
        )

        self._run_now(sched, "cohort", "rule_1")

        manual = [j for j in sched._sched.get_jobs() if j.id.startswith("manual_")][0]
        assert manual.kwargs["config"] == {"output_graph": True, "output_uc": False}
        assert manual.kwargs["target_key"] == "rule_1"

    def test_an_unscheduled_domain_is_refused(self, sched):
        ok, msg = self._run_now(sched)
        assert ok is False
        assert "No schedule found" in msg

    def test_a_stopped_scheduler_is_refused(self, sched):
        _save(sched)
        sched._started = False

        ok, msg = self._run_now(sched)

        assert ok is False
        assert "not running" in msg

    def test_a_disabled_schedule_can_still_be_run_by_hand(self, sched):
        """Run-now is the way to test a schedule before switching it on."""
        _save(sched, enabled=False)

        assert self._run_now(sched)[0] is True


class TestListing:
    def test_every_type_comes_back_with_its_label_and_next_run(self, sched):
        _save(sched, task_type="analytics", interval_minutes=120)

        entries = sched.get_all_schedules(HOST, TOKEN, REG)

        assert len(entries) == 1
        entry = entries[0]
        assert entry["task_type"] == "analytics"
        assert entry["label"] == "Graph Analytics"
        assert entry["target_key"] == ""
        assert entry["next_run"]

    def test_a_disabled_schedule_is_listed_without_a_next_run(self, sched):
        _save(sched, enabled=False)

        entry = sched.get_all_schedules(HOST, TOKEN, REG)[0]

        assert entry["enabled"] is False
        assert entry["next_run"] is None

    def test_a_missing_job_is_registered_lazily(self, sched, store):
        """Env credentials are often absent at boot, so the first visit to
        the Scheduler tab is what actually arms the jobs."""
        store.schedules["reasoning::Acme::"] = {
            "task_type": "reasoning",
            "domain_name": "Acme",
            "target_key": "",
            "interval_minutes": 60,
            "enabled": True,
            "version": "latest",
            "config": {},
        }
        assert sched._sched.get_job("sched_reasoning_Acme__") is None

        entry = sched.get_all_schedules(HOST, TOKEN, REG)[0]

        assert sched._sched.get_job("sched_reasoning_Acme__") is not None
        assert entry["next_run"]

    def test_a_stopped_scheduler_registers_nothing(self, sched, store):
        store.schedules["build::Acme::"] = {"domain_name": "Acme", "enabled": True}
        sched._started = False

        entry = sched.get_all_schedules(HOST, TOKEN, REG)[0]

        assert entry["next_run"] is None
        assert sched._sched.get_jobs() == []

    def test_missing_credentials_yield_an_empty_list_not_an_error(self, sched):
        assert sched.get_all_schedules("", TOKEN, REG) == []
        assert sched.get_all_schedules(HOST, TOKEN, {}) == []


class TestEntryShape:
    def test_the_key_fills_in_what_the_row_omits(self):
        entry = BuildScheduler._entry_from_config("cohort::Acme::rule_1", {})

        assert entry["task_type"] == "cohort"
        assert entry["domain_name"] == "Acme"
        assert entry["target_key"] == "rule_1"
        assert entry["interval_minutes"] == 60
        assert entry["enabled"] is True
        assert entry["version"] == "latest"

    def test_an_unknown_type_degrades_to_its_own_name(self):
        """A row written by a newer version must not break the listing."""
        entry = BuildScheduler._entry_from_config("teleport::Acme::", {})
        assert entry["label"] == "teleport"


class TestHistory:
    def test_it_comes_back_newest_first(self, sched, store):
        key = schedule_key("analytics", "Acme")
        store.history[key] = [
            {"timestamp": "2026-08-01", "status": "success"},
            {"timestamp": "2026-08-02", "status": "error"},
        ]

        history = sched.get_schedule_history(HOST, TOKEN, REG, "analytics", "Acme")

        assert [h["timestamp"] for h in history] == ["2026-08-02", "2026-08-01"]

    def test_it_is_scoped_to_one_type_on_a_shared_domain(self, sched, store):
        store.history[schedule_key("build", "Acme")] = [{"status": "success"}]
        store.history[schedule_key("analytics", "Acme")] = [{"status": "error"}]

        history = sched.get_schedule_history(HOST, TOKEN, REG, "analytics", "Acme")

        assert [h["status"] for h in history] == ["error"]

    def test_a_store_failure_yields_no_history_rather_than_a_500(self, sched, store):
        store.load_schedule_history = MagicMock(side_effect=RuntimeError("down"))

        assert sched.get_schedule_history(HOST, TOKEN, REG, "build", "Acme") == []


class TestStatusWriteBack:
    def _update(self, sched, outcome, key="analytics::Acme::"):
        from back.objects.registry.scheduler import _update_schedule_status

        with patch(
            "back.objects.registry.scheduler.get_scheduler", return_value=sched
        ):
            _update_schedule_status(
                HOST, TOKEN, REG, key, outcome, duration_s=12.34, run_ts="2026-08-05T06:00:00+00:00"
            )

    def test_a_run_stamps_the_row_and_appends_history(self, sched, store):
        from back.objects.registry.scheduler_tasks import RunOutcome

        _save(sched, task_type="analytics")

        self._update(
            sched,
            RunOutcome(
                status="success",
                message="Analytics complete",
                count=314,
                detail={"node_count": 314},
            ),
        )

        row = store.schedules["analytics::Acme::"]
        assert row["last_status"] == "success"
        assert row["last_count"] == 314
        assert row["last_run"] == "2026-08-05T06:00:00+00:00"

        entry = store.history["analytics::Acme::"][0]
        assert entry["duration_s"] == 12.3
        assert entry["triple_count"] == 314
        assert entry["detail"] == {"node_count": 314}

    def test_a_run_for_a_deleted_schedule_still_records_history(self, sched, store):
        """The schedule can be deleted while a run is in flight; losing
        the history row as well would hide the failure entirely."""
        from back.objects.registry.scheduler_tasks import RunOutcome

        self._update(sched, RunOutcome(status="error", message="domain gone"))

        assert store.schedules == {}
        assert store.history["analytics::Acme::"][0]["status"] == "error"


class TestRestoreOnStartup:
    def _restore(self, sched, settings=None):
        with patch.object(
            BuildScheduler, "_resolve_creds", staticmethod(lambda s: (HOST, TOKEN, REG))
        ):
            sched._restore_jobs(settings or MagicMock())

    def test_enabled_schedules_of_every_type_are_re_armed(self, sched, store):
        for task_type, target in (("build", ""), ("cohort", "rule_1")):
            store.schedules[schedule_key(task_type, "Acme", target)] = {
                "task_type": task_type,
                "domain_name": "Acme",
                "target_key": target,
                "interval_minutes": 60,
                "enabled": True,
            }

        self._restore(sched)

        assert {j.id for j in sched._sched.get_jobs()} == {
            "sched_build_Acme__",
            "sched_cohort_Acme__rule_1",
        }

    def test_disabled_schedules_are_skipped(self, sched, store):
        store.schedules["build::Acme::"] = {"domain_name": "Acme", "enabled": False}

        self._restore(sched)

        assert sched._sched.get_jobs() == []

    def test_a_row_without_a_domain_is_skipped(self, sched, store):
        store.schedules["build::::"] = {"enabled": True, "interval_minutes": 60}

        self._restore(sched)

        assert sched._sched.get_jobs() == []

    def test_no_credentials_means_no_jobs_and_no_crash(self, sched, store):
        store.schedules["build::Acme::"] = {"domain_name": "Acme", "enabled": True}

        with patch.object(
            BuildScheduler, "_resolve_creds", staticmethod(lambda s: ("", "", {}))
        ):
            sched._restore_jobs(MagicMock())

        assert sched._sched.get_jobs() == []
