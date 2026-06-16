"""
Scheduled Build Service for OntoBricks.

Manages per-project scheduled builds using APScheduler's BackgroundScheduler.
Schedule definitions are persisted in ``.global_config.json`` on the UC Volume
alongside other instance-level settings (warehouse_id, etc.).

Each schedule entry contains:
- ``interval_minutes`` -- how often to run (2, 5, 10, 30, 60, 360, 720, 1440)
- ``drop_existing``    -- whether to replace data on each build
- ``enabled``          -- whether the schedule is active
- ``last_run``         -- ISO timestamp of the last execution
- ``last_status``      -- ``"success"`` / ``"error"`` / ``null``
- ``last_message``     -- human-readable outcome of the last run

Jobs are restored at startup from env-var credentials when available.
If registry config is session-only, jobs are lazily registered when a
user opens the Schedule tab (``get_all_schedules``).
"""

from __future__ import annotations

import json
import re
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.events import (
    EVENT_JOB_ADDED,
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_JOB_REMOVED,
    EVENT_JOB_SUBMITTED,
    JobExecutionEvent,
)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from back.core.logging import get_logger
from back.core.errors import ValidationError, NotFoundError, InfrastructureError
from shared.config.constants import DEFAULT_GRAPH_NAME

logger = get_logger(__name__)

_SCHEDULES_KEY = "schedules"
_MAX_HISTORY = 50
_JOB_PREFIX = "scheduled_build_"
_COHORT_JOB_PREFIX = "scheduled_cohort_"

_scheduler_instance: Optional[BuildScheduler] = None
_lock = threading.Lock()


def get_scheduler() -> BuildScheduler:
    """Return the singleton ``BuildScheduler`` (created lazily)."""
    global _scheduler_instance
    if _scheduler_instance is None:
        with _lock:
            if _scheduler_instance is None:
                _scheduler_instance = BuildScheduler()
    return _scheduler_instance


class BuildScheduler:
    """Wraps APScheduler and persists schedule definitions in the global config."""

    _MISFIRE_GRACE = 300  # 5 min – tolerate late wakeups without silently skipping

    def __init__(self):
        self._sched = BackgroundScheduler(
            daemon=True,
            job_defaults={
                "misfire_grace_time": self._MISFIRE_GRACE,
                "coalesce": True,
                "max_instances": 1,
            },
        )
        self._started = False
        self._settings = None

    def start(self, settings) -> None:
        """Start the scheduler and load persisted schedules."""
        if self._started:
            return
        self._settings = settings

        self._sched.add_listener(
            self._on_job_event,
            EVENT_JOB_ADDED
            | EVENT_JOB_REMOVED
            | EVENT_JOB_SUBMITTED
            | EVENT_JOB_EXECUTED
            | EVENT_JOB_ERROR
            | EVENT_JOB_MISSED,
        )

        self._sched.start()
        self._started = True
        logger.info(
            "BuildScheduler started (running=%s, misfire_grace=%ds)",
            self._sched.running,
            self._MISFIRE_GRACE,
        )
        try:
            self._restore_jobs(settings)
        except Exception as e:
            logger.warning("Could not restore scheduled jobs on startup: %s", e)

    @staticmethod
    def _on_job_event(event):
        """APScheduler event listener -- logs every job lifecycle event."""
        job_id = getattr(event, "job_id", "?")
        if isinstance(event, JobExecutionEvent):
            if event.exception:
                logger.error(
                    "APScheduler EVENT_JOB_ERROR  job=%s  exception=%s",
                    job_id,
                    event.exception,
                )
                if event.traceback:
                    logger.error("APScheduler traceback:\n%s", event.traceback)
            else:
                logger.info(
                    "APScheduler EVENT_JOB_EXECUTED  job=%s  retval=%s",
                    job_id,
                    event.retval,
                )
        elif event.code == EVENT_JOB_SUBMITTED:
            logger.info("APScheduler EVENT_JOB_SUBMITTED  job=%s", job_id)
        elif event.code == EVENT_JOB_MISSED:
            logger.warning(
                "APScheduler EVENT_JOB_MISSED  job=%s  scheduled_run_time=%s",
                job_id,
                getattr(event, "scheduled_run_time", "?"),
            )
        elif event.code == EVENT_JOB_ADDED:
            logger.info("APScheduler EVENT_JOB_ADDED  job=%s", job_id)
        elif event.code == EVENT_JOB_REMOVED:
            logger.info("APScheduler EVENT_JOB_REMOVED  job=%s", job_id)

    def stop(self) -> None:
        """Shut down the scheduler gracefully."""
        if self._started:
            self._sched.shutdown(wait=False)
            self._started = False
            logger.info("BuildScheduler stopped")

    def status(self) -> Dict[str, Any]:
        """Return a diagnostic snapshot of the scheduler's internal state."""
        jobs_info = []
        for job in self._sched.get_jobs():
            jobs_info.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": (
                        job.next_run_time.isoformat() if job.next_run_time else None
                    ),
                    "trigger": str(job.trigger),
                    "pending": job.pending,
                }
            )
        return {
            "started": self._started,
            "running": self._sched.running,
            "job_count": len(jobs_info),
            "jobs": jobs_info,
        }

    # ------------------------------------------------------------------
    # Schedule CRUD
    # ------------------------------------------------------------------

    def get_all_schedules(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """Return every schedule definition enriched with next-run info.

        Also lazily registers APScheduler jobs for enabled schedules that
        are missing (e.g. after an app restart when env-var credentials
        were not available at startup).
        """
        schedules = self._load_schedules(host, token, registry_cfg)
        result = []
        for name, cfg in schedules.items():
            job = self._sched.get_job(self._job_id(name))
            if not job and cfg.get("enabled") and self._started:
                self._add_or_update_job(
                    self._settings,
                    name,
                    cfg.get("interval_minutes", 60),
                    cfg.get("drop_existing", True),
                    registry_cfg,
                    cfg.get("version", "latest"),
                )
                job = self._sched.get_job(self._job_id(name))
                logger.info("Lazily registered missing APScheduler job for '%s'", name)

            entry = {"domain_name": name, **cfg}
            if job and job.next_run_time:
                entry["next_run"] = job.next_run_time.isoformat()
            else:
                entry["next_run"] = None
            result.append(entry)
        return result

    def get_schedule_history(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_name: str,
    ) -> List[Dict[str, Any]]:
        """Return the run history for a single domain, newest first."""
        entries = self._load_domain_history(host, token, registry_cfg, domain_name)
        return list(reversed(entries))

    def save_schedule(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        settings,
        domain_name: str,
        interval_minutes: int,
        drop_existing: bool = True,
        enabled: bool = True,
        version: str = "latest",
    ) -> Tuple[bool, str]:
        """Create or update a schedule for *domain_name*."""
        if interval_minutes < 2:
            return False, "Minimum interval is 2 minutes"

        schedules = self._load_schedules(host, token, registry_cfg)
        prev = schedules.get(domain_name) or {}
        schedules[domain_name] = {
            "interval_minutes": interval_minutes,
            "drop_existing": drop_existing,
            "enabled": enabled,
            "version": version or "latest",
            "last_run": prev.get("last_run"),
            "last_status": prev.get("last_status"),
            "last_message": prev.get("last_message"),
        }

        ok, msg = self._persist_schedules(host, token, registry_cfg, schedules)
        if not ok:
            return False, msg

        if enabled and self._started:
            self._add_or_update_job(
                settings,
                domain_name,
                interval_minutes,
                drop_existing,
                registry_cfg,
                version,
            )
        else:
            self._remove_job(domain_name)

        logger.info(
            "Schedule saved for '%s': every %d min, version=%s, enabled=%s",
            domain_name,
            interval_minutes,
            version,
            enabled,
        )
        return True, f"Schedule for '{domain_name}' saved"

    def remove_schedule(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_name: str,
    ) -> Tuple[bool, str]:
        """Delete a schedule for *domain_name*."""
        schedules = self._load_schedules(host, token, registry_cfg)
        if domain_name not in schedules:
            return False, f"No schedule found for '{domain_name}'"

        del schedules[domain_name]
        ok, msg = self._persist_schedules(host, token, registry_cfg, schedules)
        if not ok:
            return False, msg

        self._remove_job(domain_name)
        logger.info("Schedule removed for '%s'", domain_name)
        return True, f"Schedule for '{domain_name}' removed"

    # ------------------------------------------------------------------
    # Cohort schedule CRUD
    # ------------------------------------------------------------------

    @staticmethod
    def _cohort_key(domain_name: str, rule_id: str) -> str:
        """Composite key used to store cohort schedules in a single dict."""
        return f"{domain_name}::{rule_id}"

    def get_all_cohort_schedules(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        """Return every cohort schedule enriched with next-run info.

        Lazily registers APScheduler jobs for enabled schedules that
        are missing (e.g. after an app restart when env-var credentials
        were not available at startup). Mirrors :meth:`get_all_schedules`.
        """
        schedules = self._load_cohort_schedules(host, token, registry_cfg)
        result = []
        for key, cfg in schedules.items():
            domain_name = cfg.get("domain_name") or ""
            rule_id = cfg.get("rule_id") or ""
            if not domain_name or not rule_id:
                continue
            job_id = self._cohort_job_id(domain_name, rule_id)
            job = self._sched.get_job(job_id)
            if not job and cfg.get("enabled") and self._started:
                self._add_or_update_cohort_job(
                    self._settings,
                    domain_name,
                    rule_id,
                    cfg.get("interval_minutes", 60),
                    registry_cfg,
                    cfg.get("version", "latest"),
                    output_graph=bool(cfg.get("output_graph", True)),
                    output_uc=bool(cfg.get("output_uc", True)),
                )
                job = self._sched.get_job(job_id)
                logger.info(
                    "Lazily registered missing cohort APScheduler job for '%s'",
                    key,
                )

            entry = {"key": key, **cfg}
            if job and job.next_run_time:
                entry["next_run"] = job.next_run_time.isoformat()
            else:
                entry["next_run"] = None
            result.append(entry)
        return result

    def get_cohort_schedule_history(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_name: str,
        rule_id: str,
    ) -> List[Dict[str, Any]]:
        """Return run history for a single cohort schedule (newest first)."""
        entries = self._load_cohort_history(
            host, token, registry_cfg, self._cohort_key(domain_name, rule_id)
        )
        return list(reversed(entries))

    def save_cohort_schedule(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        settings,
        domain_name: str,
        rule_id: str,
        interval_minutes: int,
        enabled: bool = True,
        version: str = "latest",
        output_graph: bool = True,
        output_uc: bool = True,
    ) -> Tuple[bool, str]:
        """Create or update a cohort materialisation schedule.

        ``output_graph`` / ``output_uc`` decide which targets the
        scheduled run writes. They override the rule's own ``output``
        config for this schedule only — the saved rule is not mutated.
        """
        if interval_minutes < 2:
            return False, "Minimum interval is 2 minutes"
        if not domain_name:
            return False, "Domain name is required"
        if not rule_id:
            return False, "Cohort rule id is required"

        schedules = self._load_cohort_schedules(host, token, registry_cfg)
        key = self._cohort_key(domain_name, rule_id)
        prev = schedules.get(key) or {}
        schedules[key] = {
            "domain_name": domain_name,
            "rule_id": rule_id,
            "interval_minutes": interval_minutes,
            "enabled": enabled,
            "version": version or "latest",
            "output_graph": bool(output_graph),
            "output_uc": bool(output_uc),
            "last_run": prev.get("last_run"),
            "last_status": prev.get("last_status"),
            "last_message": prev.get("last_message"),
            "last_count": prev.get("last_count"),
        }

        ok, msg = self._persist_cohort_schedules(host, token, registry_cfg, schedules)
        if not ok:
            return False, msg

        if enabled and self._started:
            self._add_or_update_cohort_job(
                settings,
                domain_name,
                rule_id,
                interval_minutes,
                registry_cfg,
                version,
                output_graph=bool(output_graph),
                output_uc=bool(output_uc),
            )
        else:
            self._remove_cohort_job(domain_name, rule_id)

        logger.info(
            "Cohort schedule saved for '%s/%s': every %d min, version=%s, enabled=%s",
            domain_name,
            rule_id,
            interval_minutes,
            version,
            enabled,
        )
        return True, f"Cohort schedule for '{domain_name}/{rule_id}' saved"

    def remove_cohort_schedule(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_name: str,
        rule_id: str,
    ) -> Tuple[bool, str]:
        """Delete a cohort schedule for *(domain_name, rule_id)*."""
        schedules = self._load_cohort_schedules(host, token, registry_cfg)
        key = self._cohort_key(domain_name, rule_id)
        if key not in schedules:
            return False, f"No cohort schedule found for '{domain_name}/{rule_id}'"

        del schedules[key]
        ok, msg = self._persist_cohort_schedules(host, token, registry_cfg, schedules)
        if not ok:
            return False, msg

        self._remove_cohort_job(domain_name, rule_id)
        logger.info("Cohort schedule removed for '%s/%s'", domain_name, rule_id)
        return True, f"Cohort schedule for '{domain_name}/{rule_id}' removed"

    # ------------------------------------------------------------------
    # Manual trigger ("Run now")
    #
    # Both helpers schedule a one-shot APScheduler ``DateTrigger`` job
    # for ``now`` so the materialise / build runs in the same worker
    # thread pool as the recurring schedule (no FastAPI request thread
    # blocked, status / history / TaskManager all updated by the
    # existing ``_run_scheduled_*`` helpers). The persisted schedule is
    # untouched — the next periodic run still fires on its own clock.
    # ------------------------------------------------------------------

    def run_schedule_now(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        settings,
        domain_name: str,
    ) -> Tuple[bool, str]:
        """Fire the build schedule for *domain_name* immediately."""
        from apscheduler.triggers.date import DateTrigger

        schedules = self._load_schedules(host, token, registry_cfg)
        cfg = schedules.get(domain_name)
        if not cfg:
            return False, f"No schedule found for domain '{domain_name}'"

        if not self._started:
            return False, "Scheduler is not running"

        run_id = f"manual_build_{domain_name}_{int(time.time() * 1000)}"
        self._sched.add_job(
            _run_scheduled_build,
            trigger=DateTrigger(run_date=datetime.now(timezone.utc)),
            id=run_id,
            name=f"Manual build {domain_name}",
            kwargs={
                "domain_name": domain_name,
                "drop_existing": bool(cfg.get("drop_existing", True)),
                "settings": settings,
                "registry_cfg": registry_cfg,
                "version": cfg.get("version", "latest"),
            },
            misfire_grace_time=self._MISFIRE_GRACE,
            coalesce=True,
            max_instances=1,
        )
        logger.info("Manual build trigger queued for '%s' (run_id=%s)", domain_name, run_id)
        return True, f"Build for '{domain_name}' queued"

    def run_cohort_schedule_now(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        settings,
        domain_name: str,
        rule_id: str,
    ) -> Tuple[bool, str]:
        """Fire the cohort materialisation schedule for *(domain, rule)* immediately."""
        from apscheduler.triggers.date import DateTrigger

        schedules = self._load_cohort_schedules(host, token, registry_cfg)
        key = self._cohort_key(domain_name, rule_id)
        cfg = schedules.get(key)
        if not cfg:
            return False, f"No cohort schedule found for '{domain_name}/{rule_id}'"

        if not self._started:
            return False, "Scheduler is not running"

        run_id = f"manual_cohort_{domain_name}__{rule_id}_{int(time.time() * 1000)}"
        self._sched.add_job(
            _run_scheduled_cohort_materialize,
            trigger=DateTrigger(run_date=datetime.now(timezone.utc)),
            id=run_id,
            name=f"Manual cohort {domain_name}/{rule_id}",
            kwargs={
                "domain_name": domain_name,
                "rule_id": rule_id,
                "settings": settings,
                "registry_cfg": registry_cfg,
                "version": cfg.get("version", "latest"),
                "output_graph": bool(cfg.get("output_graph", True)),
                "output_uc": bool(cfg.get("output_uc", True)),
            },
            misfire_grace_time=self._MISFIRE_GRACE,
            coalesce=True,
            max_instances=1,
        )
        logger.info(
            "Manual cohort trigger queued for '%s/%s' (run_id=%s)",
            domain_name,
            rule_id,
            run_id,
        )
        return True, f"Cohort materialisation for '{domain_name}/{rule_id}' queued"

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _store_for(host: str, token: str, registry_cfg: Dict[str, str]):
        """Build the Lakebase :class:`RegistryStore` for *registry_cfg*.

        ``host``/``token`` are accepted for signature compatibility with
        the rest of the scheduler plumbing; Lakebase uses its own
        PG*/JWT credentials so they are ignored.
        """
        from back.objects.registry import RegistryCfg
        from back.objects.registry.store import RegistryFactory

        del host, token
        cfg = RegistryCfg.from_dict(registry_cfg)
        return RegistryFactory.from_cfg(cfg)

    def _load_schedules(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> Dict[str, Any]:
        if not host or not registry_cfg.get("catalog"):
            return {}
        try:
            store = self._store_for(host, token, registry_cfg)
            return dict(store.load_schedules() or {})
        except Exception as e:
            logger.debug("Could not load schedules: %s", e)
            return {}

    def _persist_schedules(
        self, host: str, token: str, registry_cfg: Dict[str, str], schedules: Dict
    ) -> Tuple[bool, str]:
        if not host or not registry_cfg.get("catalog"):
            return False, "Databricks credentials or registry not configured"
        try:
            store = self._store_for(host, token, registry_cfg)
            ok, msg = store.save_schedules(schedules)
            if ok:
                # Invalidate the in-process global-config cache so other
                # readers (e.g. settings UI, GlobalConfigService.load) see
                # the schedule changes on next load.
                from back.objects.session.global_config import (
                    global_config_service,
                )

                global_config_service._cache = None
                global_config_service._cache_ts = 0.0
            return ok, msg
        except Exception as e:
            logger.exception("Could not persist schedules: %s", e)
            return False, str(e)

    def _load_domain_history(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_name: str,
    ) -> List[Dict[str, Any]]:
        if not host or not registry_cfg.get("catalog"):
            return []
        try:
            store = self._store_for(host, token, registry_cfg)
            return list(store.load_schedule_history(domain_name))
        except Exception as e:
            logger.debug("Could not load history for '%s': %s", domain_name, e)
            return []

    def _append_history(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        domain_name: str,
        entry: Dict[str, Any],
    ) -> None:
        try:
            store = self._store_for(host, token, registry_cfg)
            store.append_schedule_history(
                domain_name, entry, max_entries=_MAX_HISTORY
            )
        except Exception as e:
            logger.warning("Could not save history for '%s': %s", domain_name, e)

    def _load_cohort_schedules(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> Dict[str, Any]:
        if not host or not registry_cfg.get("catalog"):
            return {}
        try:
            store = self._store_for(host, token, registry_cfg)
            return dict(store.load_cohort_schedules() or {})
        except Exception as e:
            logger.debug("Could not load cohort schedules: %s", e)
            return {}

    def _persist_cohort_schedules(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        schedules: Dict,
    ) -> Tuple[bool, str]:
        if not host or not registry_cfg.get("catalog"):
            return False, "Databricks credentials or registry not configured"
        try:
            store = self._store_for(host, token, registry_cfg)
            ok, msg = store.save_cohort_schedules(schedules)
            if ok:
                from back.objects.session.global_config import (
                    global_config_service,
                )

                global_config_service._cache = None
                global_config_service._cache_ts = 0.0
            return ok, msg
        except Exception as e:
            logger.exception("Could not persist cohort schedules: %s", e)
            return False, str(e)

    def _load_cohort_history(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        key: str,
    ) -> List[Dict[str, Any]]:
        if not host or not registry_cfg.get("catalog"):
            return []
        try:
            store = self._store_for(host, token, registry_cfg)
            return list(store.load_cohort_schedule_history(key))
        except Exception as e:
            logger.debug("Could not load cohort history for '%s': %s", key, e)
            return []

    def _append_cohort_history(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        key: str,
        entry: Dict[str, Any],
    ) -> None:
        try:
            store = self._store_for(host, token, registry_cfg)
            store.append_cohort_schedule_history(
                key, entry, max_entries=_MAX_HISTORY
            )
        except Exception as e:
            logger.warning("Could not save cohort history for '%s': %s", key, e)

    @staticmethod
    def _resolve_creds(settings):
        """Resolve host/token/registry from env-level settings (for startup).

        The returned ``cfg`` carries ``lakebase_schema`` and
        ``lakebase_database`` from *Settings* so that schedule-related
        store calls made *before* the global config has been loaded
        (e.g. on app boot, when restoring jobs) target the right
        Lakebase database and schema from the very first APScheduler
        tick.
        """
        from back.core.databricks import is_databricks_app
        from back.objects.registry.RegistryService import RegistryCfg

        host = settings.databricks_host
        token = settings.databricks_token
        if (not host or not token) and is_databricks_app():
            from back.core.helpers import get_databricks_host_and_token

            class _Stub:
                databricks = {}

            host, token = get_databricks_host_and_token(_Stub(), settings)

        lakebase_schema = (
            getattr(settings, "lakebase_schema", "ontobricks_registry")
            or "ontobricks_registry"
        )
        lakebase_database = getattr(settings, "lakebase_database", "") or ""

        vol_path = (getattr(settings, "registry_volume_path", "") or "").strip()
        if vol_path:
            parsed = RegistryCfg.from_volume_path(
                vol_path,
                lakebase_schema=lakebase_schema,
                lakebase_database=lakebase_database,
            )
            if parsed.catalog and parsed.schema and parsed.volume:
                return host, token, parsed.as_dict()

        cfg = RegistryCfg(
            catalog=settings.registry_catalog,
            schema=settings.registry_schema,
            volume=settings.registry_volume or "OntoBricksRegistry",
            lakebase_schema=lakebase_schema,
            lakebase_database=lakebase_database,
        )
        return host, token, cfg.as_dict()

    # ------------------------------------------------------------------
    # APScheduler job management
    # ------------------------------------------------------------------

    def _job_id(self, domain_name: str) -> str:
        return f"{_JOB_PREFIX}{domain_name}"

    def _add_or_update_job(
        self,
        settings,
        domain_name: str,
        interval_minutes: int,
        drop_existing: bool,
        registry_cfg: Optional[Dict[str, str]] = None,
        version: str = "latest",
    ):
        job_id = self._job_id(domain_name)
        existing = self._sched.get_job(job_id)
        if existing:
            self._sched.remove_job(job_id)

        trigger = IntervalTrigger(minutes=interval_minutes)
        job = self._sched.add_job(
            _run_scheduled_build,
            trigger=trigger,
            id=job_id,
            name=f"Build {domain_name}",
            args=[domain_name, drop_existing, settings, registry_cfg, version],
            replace_existing=True,
            misfire_grace_time=self._MISFIRE_GRACE,
            coalesce=True,
            max_instances=1,
        )
        next_run = job.next_run_time.isoformat() if job.next_run_time else "unknown"
        logger.info(
            "APScheduler job added/updated: %s (every %d min, next_run=%s, misfire_grace=%ds)",
            job_id,
            interval_minutes,
            next_run,
            self._MISFIRE_GRACE,
        )

    def _remove_job(self, domain_name: str):
        job_id = self._job_id(domain_name)
        if self._sched.get_job(job_id):
            self._sched.remove_job(job_id)
            logger.info("APScheduler job removed: %s", job_id)

    def _cohort_job_id(self, domain_name: str, rule_id: str) -> str:
        return f"{_COHORT_JOB_PREFIX}{domain_name}__{rule_id}"

    def _add_or_update_cohort_job(
        self,
        settings,
        domain_name: str,
        rule_id: str,
        interval_minutes: int,
        registry_cfg: Optional[Dict[str, str]] = None,
        version: str = "latest",
        output_graph: bool = True,
        output_uc: bool = True,
    ):
        job_id = self._cohort_job_id(domain_name, rule_id)
        if self._sched.get_job(job_id):
            self._sched.remove_job(job_id)

        trigger = IntervalTrigger(minutes=interval_minutes)
        job = self._sched.add_job(
            _run_scheduled_cohort_materialize,
            trigger=trigger,
            id=job_id,
            name=f"Cohort {domain_name}/{rule_id}",
            kwargs={
                "domain_name": domain_name,
                "rule_id": rule_id,
                "settings": settings,
                "registry_cfg": registry_cfg,
                "version": version,
                "output_graph": bool(output_graph),
                "output_uc": bool(output_uc),
            },
            replace_existing=True,
            misfire_grace_time=self._MISFIRE_GRACE,
            coalesce=True,
            max_instances=1,
        )
        next_run = job.next_run_time.isoformat() if job.next_run_time else "unknown"
        logger.info(
            "Cohort APScheduler job added/updated: %s (every %d min, next_run=%s)",
            job_id,
            interval_minutes,
            next_run,
        )

    def _remove_cohort_job(self, domain_name: str, rule_id: str):
        job_id = self._cohort_job_id(domain_name, rule_id)
        if self._sched.get_job(job_id):
            self._sched.remove_job(job_id)
            logger.info("Cohort APScheduler job removed: %s", job_id)

    def _restore_jobs(self, settings):
        """Re-register APScheduler jobs for all enabled schedules on startup."""
        host, token, reg = self._resolve_creds(settings)
        if not host or not reg.get("catalog"):
            logger.info(
                "No credentials/registry from env at startup; "
                "jobs will be lazily registered when a user opens the Schedule tab"
            )
            return
        schedules = self._load_schedules(host, token, reg)
        count = 0
        for name, cfg in schedules.items():
            if cfg.get("enabled"):
                self._add_or_update_job(
                    settings,
                    name,
                    cfg.get("interval_minutes", 60),
                    cfg.get("drop_existing", True),
                    reg,
                    cfg.get("version", "latest"),
                )
                count += 1
        logger.info("Restored %d scheduled build job(s)", count)

        cohort_schedules = self._load_cohort_schedules(host, token, reg)
        c_count = 0
        for _key, cfg in cohort_schedules.items():
            if not cfg.get("enabled"):
                continue
            domain_name = cfg.get("domain_name") or ""
            rule_id = cfg.get("rule_id") or ""
            if not domain_name or not rule_id:
                continue
            self._add_or_update_cohort_job(
                settings,
                domain_name,
                rule_id,
                cfg.get("interval_minutes", 60),
                reg,
                cfg.get("version", "latest"),
                output_graph=bool(cfg.get("output_graph", True)),
                output_uc=bool(cfg.get("output_uc", True)),
            )
            c_count += 1
        if c_count:
            logger.info("Restored %d scheduled cohort job(s)", c_count)


# ======================================================================
# Build execution (runs in APScheduler's thread pool)
# ======================================================================


def _load_domain_for_build(
    svc,
    domain_name: str,
    version: str,
    host: str,
    token: str,
    reg: dict,
):
    """Load a domain from the registry into a headless DomainSession.

    Returns ``(domain, loaded_version, domain_path, latest_filename)``.
    """
    from back.objects.session.DomainSession import DomainSession

    if version and version != "latest":
        ok, data, err = svc.read_version(domain_name, version)
        loaded_version = version
        if not ok:
            raise NotFoundError(
                err or f"Version '{version}' not found for domain '{domain_name}'"
            )
    else:
        ok, data, loaded_version, err = svc.load_latest_domain_data(domain_name)
        if not ok:
            raise NotFoundError(err or f"Domain '{domain_name}' not found in registry")

    class _FakeSessionMgr:
        """Minimal stand-in so DomainSession can load without a real session."""

        def __init__(self):
            self._store: Dict = {}

        def get(self, key, default=None):
            return self._store.get(key, default)

        def set(self, key, value):
            self._store[key] = value

    domain = DomainSession(_FakeSessionMgr())
    domain.import_from_file(data, version=loaded_version)
    domain.domain_folder = domain_name
    domain.settings["registry"] = reg
    domain.databricks["host"] = host
    domain.ensure_generated_content()

    domain_path = svc.domain_path(domain_name)
    latest = f"v{loaded_version}.json"
    return domain, loaded_version, domain_path, latest


def _generate_sql_from_r2rml(domain, domain_name: str):
    """Generate Spark SQL from R2RML mappings.

    Returns ``(sql_text, view_table, graph_name, base_uri)``.
    """
    from back.core.w3c import sparql
    from back.objects.digitaltwin import (
        augment_mappings_from_config,
        augment_relationships_from_config,
    )

    r2rml = domain.get_r2rml()
    if not r2rml:
        raise ValidationError("No R2RML mapping available")

    delta = domain.delta or {}
    _name = (domain.info or {}).get("name", DEFAULT_GRAPH_NAME)
    _version = getattr(domain, "current_version", "1") or "1"
    _safe = re.sub(r"[^a-z0-9_]", "_", _name.lower())
    _view_name = f"triplestore_{_safe}_V{_version}"
    view_parts = [delta.get("catalog", ""), delta.get("schema", ""), _view_name]
    view_table = ".".join(p for p in view_parts if p)
    if not view_table or len(view_table.split(".")) != 3:
        raise ValidationError(f"View not fully qualified: {view_table}")
    graph_name = f"{_name}_V{_version}"

    base_uri = domain.ontology.get("base_uri", "http://example.org/")
    mapping_config = domain.assignment
    ontology_config = domain.ontology

    logger.info("Scheduled build [%s]: generating SQL from R2RML", domain_name)
    ent, rels = sparql.extract_r2rml_mappings(r2rml)
    ent = augment_mappings_from_config(ent, mapping_config, base_uri, ontology_config)
    rels = augment_relationships_from_config(
        rels, mapping_config, base_uri, ontology_config
    )
    if not ent and not rels:
        raise ValidationError("No valid mappings found")

    sparql_q = (
        f"PREFIX : <{base_uri}>\n"
        "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
        "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n\n"
        "SELECT DISTINCT ?subject ?predicate ?object\n"
        "WHERE {\n    ?subject ?predicate ?object .\n}"
    )
    res = sparql.translate_sparql_to_spark(sparql_q, ent, None, rels, dialect="spark")

    return res["sql"], view_table, graph_name, base_uri, ent, rels


def _stream_into_store(store, src, graph_name: str, select_sql: str, batch: int = 5000) -> int:
    """Stream warehouse rows into ``store.bulk_insert_iter`` (or list fallback)."""
    rows = src.iter_rows(select_sql, batch_size=batch)
    if hasattr(store, "bulk_insert_iter"):
        return store.bulk_insert_iter(graph_name, rows, batch_size=batch)
    return store.insert_triples(graph_name, list(rows), batch_size=min(batch, 500))


def _is_managed_synced(store) -> bool:
    """Lakebase store in managed_synced mode -- bulk goes via Lakeflow."""
    return bool(getattr(store, "is_synced", False))


def _apply_synced_pipeline(
    store,
    src,
    delta_cfg: Dict[str, Any],
    graph_name: str,
    view_table: str,
    *,
    full: bool,
    domain_name: str,
    domain: Any = None,
    settings: Any = None,
) -> None:
    """Trigger the Lakeflow synced-table refresh for *graph_name*.

    Mirrors :meth:`_BuildPipeline._apply_via_synced_pipeline` so scheduled
    builds also keep bulk data movement on the data plane.
    """
    from back.core.graphdb.lakebase.LakebaseFlatStore import (
        resolve_sync_uc_fallback_catalog,
    )
    from back.core.graphdb.lakebase._sync_uc_schema import (
        ensure_uc_schema_for_synced_table_fqn,
    )

    mgr = store.synced_manager()
    if domain is not None and settings is not None:
        fallback_cat = resolve_sync_uc_fallback_catalog(
            domain, settings, delta_cfg
        )
    else:
        fallback_cat = (delta_cfg or {}).get("catalog", "")
    synced_uc = store.synced_uc_name(graph_name, fallback_catalog=fallback_cat)
    logger.info(
        "Scheduled build [%s]: managed-sync UC target %s "
        "(sync_uc_catalog=%r; fallback_catalog=%r; graph_schema=%s)",
        domain_name,
        synced_uc,
        (store.sync_uc_catalog or "").strip() or None,
        fallback_cat or None,
        store.graph_schema,
    )
    ensure_uc_schema_for_synced_table_fqn(
        src,
        synced_uc,
        task_log_prefix=f"Scheduled build [{domain_name}]",
    )
    mgr.ensure(
        synced_uc,
        source_table_full_name=view_table,
        primary_key_columns=["subject", "predicate", "object"],
        sync_mode=store.sync_table_mode,
    )
    store.ensure_synced_companion(graph_name)
    state = mgr.trigger_and_wait(synced_uc, timeout_s=store.sync_timeout_s)
    logger.info(
        "Scheduled build [%s]: synced table %s state=%s",
        domain_name,
        synced_uc,
        state,
    )
    store.ensure_synced_union_view(graph_name)
    if full:
        try:
            store.truncate_companion(graph_name)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Scheduled build [%s]: companion truncate failed (non-fatal): %s",
                domain_name,
                exc,
            )


def _count_view_triples(src, view_table: str) -> int:
    """Return the server-side triple count for *view_table*."""
    try:
        rows = src.execute_query(f"SELECT COUNT(*) AS cnt FROM {view_table}")
        return int(rows[0].get("cnt", 0)) if rows else 0
    except Exception:
        return 0


def _write_graph_triples(
    store,
    src,
    graph_name: str,
    view_table: str,
    domain_name: str,
    delta_cfg: Optional[Dict[str, Any]] = None,
    domain: Any = None,
    settings: Any = None,
) -> int:
    """Write triples to the graph store. Returns the triple count.

    When the store is in Lakebase ``managed_synced`` mode the entire branch is
    replaced by a Lakeflow snapshot refresh — triples never enter this process.
    Otherwise a full drop-and-rebuild is performed.
    """
    if _is_managed_synced(store):
        _apply_synced_pipeline(
            store,
            src,
            delta_cfg or {},
            graph_name,
            view_table,
            full=True,
            domain_name=domain_name,
            domain=domain,
            settings=settings,
        )
        return _count_view_triples(src, view_table)

    triple_count = _count_view_triples(src, view_table)
    logger.info(
        "Scheduled build [%s]: %d triples reported by VIEW",
        domain_name,
        triple_count,
    )

    if triple_count > 0:
        store.drop_table(graph_name)
        store.create_table(graph_name)
        _stream_into_store(
            store,
            src,
            graph_name,
            f"SELECT subject, predicate, object FROM {view_table}",
        )
        store.optimize_table(graph_name)
        logger.info(
            "Scheduled build [%s]: graph '%s' populated with %d triples",
            domain_name,
            graph_name,
            triple_count,
        )
    return triple_count


def _persist_domain_metadata(
    svc, domain, version: str, build_ts: str, domain_name: str
):
    """Stamp last_build and write the domain doc via the active store.

    ``svc`` is the :class:`RegistryService` that owns the
    :class:`RegistryStore` (always Lakebase).
    ``version`` is the numeric version string (e.g. ``"1"``).
    """
    domain.last_build = build_ts
    try:
        domain_data = domain.export_for_save()
        w_ok, w_msg = svc._store.write_version(domain_name, version, domain_data)
        if w_ok:
            logger.info(
                "Scheduled build [%s]: stamped last_build=%s in registry",
                domain_name,
                build_ts,
            )
        else:
            logger.error(
                "Scheduled build [%s]: write_version returned failure: %s",
                domain_name,
                w_msg,
            )
    except Exception as save_exc:
        logger.warning(
            "Scheduled build [%s]: could not stamp last_build: %s",
            domain_name,
            save_exc,
        )


def _run_scheduled_build(
    domain_name: str,
    drop_existing: bool,  # kept for backward-compat with persisted schedule configs
    settings,
    registry_cfg: Optional[Dict[str, str]] = None,
    version: str = "latest",
) -> None:
    """Execute a Digital Twin build for *domain_name* without a user session.

    Loads the domain from the registry, generates SQL from R2RML, creates
    the VIEW, and populates the graph store (full rebuild every run).

    When *version* is ``"latest"`` (default) the newest version is loaded.
    The ``drop_existing`` flag is accepted for backward compatibility with
    persisted schedule configs but has no effect — all scheduled builds are
    full rebuilds.

    The entire function is wrapped in a fail-safe try/except so that
    no exception can silently escape to APScheduler's executor.
    """
    logger.info(
        "Scheduled build FIRED for '%s' version=%s (thread=%s)",
        domain_name,
        version,
        threading.current_thread().name,
    )
    start = time.time()
    build_ts = datetime.now(timezone.utc).isoformat()

    tm = None
    task = None
    host: str = ""
    token: str = ""
    reg: Dict[str, str] = {}
    status = "error"
    message = ""
    triple_count = 0
    # Captured for the build-run trace (see finally). Populated as the
    # build progresses so a mid-build failure still records what it can.
    svc = None
    view_table = ""
    graph_name = ""
    ent_count = 0
    rel_count = 0
    sql_chars = 0
    build_stats: Dict[str, Any] = {}

    try:
        scheduler = get_scheduler()

        from back.core.task_manager import get_task_manager

        tm = get_task_manager()
        task = tm.create_task(
            name=f"Scheduled Build — {domain_name}",
            task_type="scheduled_build",
            steps=[
                {"name": "prepare", "description": "Loading domain and generating SQL"},
                {
                    "name": "view",
                    "description": "Creating Triple-Store VIEW in Unity Catalog",
                },
                {"name": "graph", "description": "Populating Lakebase graph"},
            ],
        )
        tm.start_task(task.id, f"Starting scheduled build for {domain_name}...")

        host, token, env_reg = scheduler._resolve_creds(settings)
        reg = registry_cfg or env_reg
        logger.info(
            "Scheduled build [%s]: creds resolved host=%s reg_catalog=%s",
            domain_name,
            bool(host),
            reg.get("catalog", ""),
        )

        if not host or not token:
            raise InfrastructureError("Databricks host/token not available")

        from back.objects.registry.RegistryService import RegistryCfg, RegistryService

        cfg = RegistryCfg.from_dict(reg)
        if not cfg.catalog or not cfg.schema:
            raise ValidationError("Registry not configured")

        from back.core.databricks.DatabricksClient import DatabricksClient
        from back.core.databricks.VolumeFileService import VolumeFileService
        from back.core.helpers import resolve_warehouse_id

        tm.update_progress(task.id, 5, "Loading domain from registry...")

        uc = VolumeFileService(host=host, token=token)
        svc = RegistryService(cfg, uc)
        domain, version, domain_path, latest = _load_domain_for_build(
            svc,
            domain_name,
            version,
            host,
            token,
            reg,
        )

        warehouse_id = resolve_warehouse_id(domain, settings)
        if not warehouse_id:
            raise InfrastructureError("No SQL warehouse configured")

        tm.update_progress(task.id, 10, "Generating SQL from R2RML mappings...")
        sql_text, view_table, graph_name, base_uri, ent_mappings, rel_mappings = (
            _generate_sql_from_r2rml(
                domain,
                domain_name,
            )
        )
        ent_count = len(ent_mappings or [])
        rel_count = len(rel_mappings or [])
        sql_chars = len(sql_text or "")

        # Ontology + mapping stats for the build-run trace (Cockpit picture).
        from back.objects.digitaltwin._build_pipeline import collect_domain_stats

        build_stats = collect_domain_stats(
            getattr(domain, "ontology", {}),
            getattr(domain, "assignment", {}),
            constraints=getattr(domain, "constraints", None),
            swrl_rules=getattr(domain, "swrl_rules", None),
            axioms=getattr(domain, "axioms", None),
            shacl_shapes=getattr(domain, "shacl_shapes", None),
        )

        # --- Step 2: Create VIEW ---
        tm.advance_step(task.id, f"Creating VIEW {view_table}...")
        src = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)

        cat, sch, vname = view_table.split(".")
        logger.info("Scheduled build [%s]: creating VIEW %s", domain_name, view_table)
        view_ok, view_msg = src.create_or_replace_view(cat, sch, vname, sql_text)
        if not view_ok:
            from back.objects.digitaltwin import DigitalTwin

            detail = DigitalTwin.diagnose_view_error(
                view_msg, ent_mappings, rel_mappings
            )
            logger.error(
                "Scheduled build [%s]: VIEW creation failed:\n%s", domain_name, detail
            )
            raise InfrastructureError(f"Failed to create VIEW: {detail}")
        tm.update_progress(task.id, 40, "VIEW created")

        # --- Step 3: Populate graph ---
        tm.advance_step(task.id, f"Applying to graph {graph_name}...")

        from back.core.triplestore import get_triplestore
        from back.objects.digitaltwin.models import DomainSnapshot

        snap = DomainSnapshot(domain, host=host, token=token)
        store = get_triplestore(snap, settings, backend="graph")
        if not store:
            raise InfrastructureError("Could not initialize graph backend")

        triple_count = _write_graph_triples(
            store,
            src,
            graph_name,
            view_table,
            domain_name,
            delta_cfg=getattr(domain, "delta", None) or {},
            domain=domain,
            settings=settings,
        )

        tm.update_progress(task.id, 95, "Saving domain metadata...")
        _persist_domain_metadata(svc, domain, version, build_ts, domain_name)

        status = "success"
        message = f"Built {triple_count} triples in {time.time() - start:.1f}s"

    except Exception as exc:
        status = "error"
        message = str(exc)
        logger.exception(
            "Scheduled build [%s] failed after %.1fs: %s",
            domain_name,
            time.time() - start,
            exc,
        )

    finally:
        duration = time.time() - start

        try:
            if tm and task:
                if status == "success":
                    tm.complete_task(
                        task.id,
                        result={
                            "triple_count": triple_count,
                            "duration_seconds": duration,
                        },
                        message=message,
                    )
                else:
                    tm.fail_task(task.id, message)
        except Exception as tm_exc:
            logger.error(
                "Scheduled build [%s]: task-manager update failed: %s",
                domain_name,
                tm_exc,
            )

        if host and reg.get("catalog"):
            try:
                _update_schedule_status(
                    host,
                    token,
                    reg,
                    domain_name,
                    status,
                    message,
                    duration_s=duration,
                    triple_count=triple_count,
                    run_ts=build_ts,
                )
            except Exception as status_exc:
                logger.error(
                    "Scheduled build [%s]: failed to update status: %s",
                    domain_name,
                    status_exc,
                )
        else:
            logger.error(
                "Scheduled build [%s]: cannot update status — no host or registry config",
                domain_name,
            )

        # Build-run trace (analytics). Best-effort and independent from
        # the legacy ``schedule_runs`` history written above.
        if svc is not None:
            try:
                from back.objects.digitaltwin._build_pipeline import (
                    step_times_from_task,
                )

                svc.record_build_run(
                    domain_name,
                    {
                        "version": str(version),
                        "build_kind": "scheduled",
                        "status": status,
                        "message": message if status == "success" else "",
                        "error": message if status != "success" else "",
                        "started_at": build_ts,
                        "finished_at": datetime.now(timezone.utc).isoformat(),
                        "duration_s": duration,
                        "triple_count": triple_count,
                        "entity_count": ent_count,
                        "relationship_count": rel_count,
                        "sql_chars": sql_chars,
                        "graph_engine": "",
                        "sync_mode": "",
                        "view_table": view_table,
                        "graph_name": graph_name,
                        "task_id": task.id if task else "",
                        "phase_times": step_times_from_task(task) if task else {},
                        "stats": build_stats,
                    },
                )
            except Exception as trace_exc:  # noqa: BLE001
                logger.warning(
                    "Scheduled build [%s]: could not record build-run trace: %s",
                    domain_name,
                    trace_exc,
                )

        logger.info(
            "Scheduled build [%s]: finished with status=%s in %.1fs",
            domain_name,
            status,
            duration,
        )


def _run_scheduled_cohort_materialize(
    domain_name: str,
    rule_id: str,
    settings,
    registry_cfg: Optional[Dict[str, str]] = None,
    version: str = "latest",
    output_graph: bool = True,
    output_uc: bool = True,
) -> None:
    """Run a cohort materialisation for *(domain_name, rule_id)*.

    Loads the domain headlessly from the registry, resolves the graph
    backend + Databricks client, then delegates to
    :meth:`CohortService.materialize`. Status / history / TaskManager
    are updated on every run, mirroring :func:`_run_scheduled_build`.
    """
    logger.info(
        "Scheduled cohort materialise FIRED for '%s/%s' version=%s (thread=%s)",
        domain_name,
        rule_id,
        version,
        threading.current_thread().name,
    )
    start = time.time()
    run_ts = datetime.now(timezone.utc).isoformat()

    tm = None
    task = None
    host: str = ""
    token: str = ""
    reg: Dict[str, str] = {}
    status = "error"
    message = ""
    materialized_triples = 0
    uc_rows_written = 0

    try:
        scheduler = get_scheduler()
        from back.core.task_manager import get_task_manager

        tm = get_task_manager()
        task = tm.create_task(
            name=f"Scheduled Cohort — {domain_name}/{rule_id}",
            task_type="scheduled_cohort",
            steps=[
                {"name": "prepare", "description": "Loading domain and rule"},
                {"name": "engine", "description": "Running cohort engine"},
                {"name": "write", "description": "Writing cohort outputs"},
            ],
        )
        tm.start_task(task.id, f"Starting cohort materialise for {rule_id}...")

        host, token, env_reg = scheduler._resolve_creds(settings)
        reg = registry_cfg or env_reg

        if not host or not token:
            raise InfrastructureError("Databricks host/token not available")

        from back.objects.registry.RegistryService import RegistryCfg, RegistryService

        cfg = RegistryCfg.from_dict(reg)
        if not cfg.catalog or not cfg.schema:
            raise ValidationError("Registry not configured")

        from back.core.databricks.DatabricksClient import DatabricksClient
        from back.core.databricks.VolumeFileService import VolumeFileService
        from back.core.helpers import resolve_warehouse_id
        from back.core.triplestore import get_triplestore
        from back.objects.digitaltwin import CohortService
        from back.objects.digitaltwin.models import DomainSnapshot

        tm.update_progress(task.id, 5, "Loading domain from registry...")

        uc = VolumeFileService(host=host, token=token)
        svc = RegistryService(cfg, uc)
        domain, loaded_version, _domain_path, _latest = _load_domain_for_build(
            svc,
            domain_name,
            version,
            host,
            token,
            reg,
        )

        warehouse_id = resolve_warehouse_id(domain, settings)
        if not warehouse_id:
            raise InfrastructureError("No SQL warehouse configured")

        rules = list(getattr(domain, "cohort_rules", []) or [])
        if not any((r.get("id") == rule_id) for r in rules):
            raise NotFoundError(
                f"Cohort rule '{rule_id}' not found in domain '{domain_name}'"
            )

        snap = DomainSnapshot(domain, host=host, token=token)
        store = get_triplestore(snap, settings, backend="graph")
        if not store:
            raise InfrastructureError("Could not initialize graph backend")

        graph_name = (
            f"{(domain.info or {}).get('name', DEFAULT_GRAPH_NAME)}"
            f"_V{getattr(domain, 'current_version', loaded_version) or '1'}"
        )

        tm.advance_step(task.id, "Running cohort engine...")
        client = DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)

        def _label_resolver(uris):
            try:
                metadata = store.get_entity_metadata(graph_name, list(uris))
            except Exception:
                return {}
            return {
                row.get("uri", ""): row.get("label", "")
                for row in metadata or []
            }

        cohort_svc = CohortService(domain)

        tm.advance_step(task.id, "Writing cohort outputs...")
        result = cohort_svc.materialize(
            rule_id,
            store,
            graph_name,
            client=client,
            domain_version=str(loaded_version or ""),
            member_label_resolver=_label_resolver,
            output_graph=bool(output_graph),
            output_uc=bool(output_uc),
        )

        materialized_triples = int(result.get("materialized_triples") or 0)
        uc_rows_written = int(result.get("uc_rows_written") or 0)
        status = "success"
        bits: List[str] = []
        if materialized_triples:
            bits.append(f"{materialized_triples} triples")
        if uc_rows_written:
            bits.append(f"{uc_rows_written} UC rows")
        if not bits:
            bits.append("0 outputs (rule produced no cohorts)")
        message = (
            f"Materialised {' / '.join(bits)} in {time.time() - start:.1f}s"
        )
        graph_err = result.get("materialize_graph_error")
        uc_err = result.get("materialize_uc_error")
        if graph_err or uc_err:
            status = "error"
            message = (graph_err or uc_err) or message

    except Exception as exc:
        status = "error"
        message = str(exc)
        logger.exception(
            "Scheduled cohort [%s/%s] failed after %.1fs: %s",
            domain_name,
            rule_id,
            time.time() - start,
            exc,
        )

    finally:
        duration = time.time() - start
        try:
            if tm and task:
                if status == "success":
                    tm.complete_task(
                        task.id,
                        result={
                            "materialized_triples": materialized_triples,
                            "uc_rows_written": uc_rows_written,
                            "duration_seconds": duration,
                        },
                        message=message,
                    )
                else:
                    tm.fail_task(task.id, message)
        except Exception as tm_exc:
            logger.error(
                "Scheduled cohort [%s/%s]: task-manager update failed: %s",
                domain_name,
                rule_id,
                tm_exc,
            )

        if host and reg.get("catalog"):
            try:
                _update_cohort_schedule_status(
                    host,
                    token,
                    reg,
                    domain_name,
                    rule_id,
                    status,
                    message,
                    duration_s=duration,
                    materialized_triples=materialized_triples,
                    uc_rows_written=uc_rows_written,
                    run_ts=run_ts,
                )
            except Exception as status_exc:
                logger.error(
                    "Scheduled cohort [%s/%s]: failed to update status: %s",
                    domain_name,
                    rule_id,
                    status_exc,
                )
        logger.info(
            "Scheduled cohort [%s/%s]: finished with status=%s in %.1fs",
            domain_name,
            rule_id,
            status,
            duration,
        )


def _update_cohort_schedule_status(
    host: str,
    token: str,
    registry_cfg: Dict[str, str],
    domain_name: str,
    rule_id: str,
    status: str,
    message: str,
    duration_s: float = 0.0,
    materialized_triples: int = 0,
    uc_rows_written: int = 0,
    run_ts: str = "",
):
    """Update last_run / last_status / last_message and append to history."""
    try:
        ts = run_ts or datetime.now(timezone.utc).isoformat()
        scheduler = get_scheduler()
        key = scheduler._cohort_key(domain_name, rule_id)

        schedules = scheduler._load_cohort_schedules(host, token, registry_cfg)
        total_count = int(materialized_triples) + int(uc_rows_written)
        if key in schedules:
            schedules[key]["last_run"] = ts
            schedules[key]["last_status"] = status
            schedules[key]["last_message"] = message
            schedules[key]["last_count"] = total_count
            scheduler._persist_cohort_schedules(host, token, registry_cfg, schedules)

        history_entry = {
            "timestamp": ts,
            "status": status,
            "message": message,
            "duration_s": round(duration_s, 1),
            "materialized_triples": int(materialized_triples),
            "uc_rows_written": int(uc_rows_written),
            "triple_count": total_count,
        }
        scheduler._append_cohort_history(
            host, token, registry_cfg, key, history_entry
        )
    except Exception as e:
        logger.warning(
            "Could not update cohort schedule status for '%s/%s': %s",
            domain_name,
            rule_id,
            e,
        )


def _update_schedule_status(
    host: str,
    token: str,
    registry_cfg: Dict[str, str],
    domain_name: str,
    status: str,
    message: str,
    duration_s: float = 0.0,
    triple_count: int = 0,
    run_ts: str = "",
):
    """Update last_run / last_status / last_message and append to run history.

    *run_ts* is the ISO timestamp shared with ``domain.last_build`` so
    that both values always match after a successful scheduled build.
    """
    try:
        ts = run_ts or datetime.now(timezone.utc).isoformat()
        scheduler = get_scheduler()

        schedules = scheduler._load_schedules(host, token, registry_cfg)
        if domain_name in schedules:
            schedules[domain_name]["last_run"] = ts
            schedules[domain_name]["last_status"] = status
            schedules[domain_name]["last_message"] = message
            scheduler._persist_schedules(host, token, registry_cfg, schedules)

        history_entry = {
            "timestamp": ts,
            "status": status,
            "message": message,
            "duration_s": round(duration_s, 1),
            "triple_count": triple_count,
        }
        scheduler._append_history(host, token, registry_cfg, domain_name, history_entry)
    except Exception as e:
        logger.warning("Could not update schedule status for '%s': %s", domain_name, e)
