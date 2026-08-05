"""Scheduled task service for OntoBricks.

Runs recurring per-domain jobs — Knowledge Graph builds, cohort
materialisations, graph analytics, and inference — on APScheduler's
``BackgroundScheduler``. Schedules are persisted in the registry's
``schedules`` table and their run history in ``schedule_runs``.

The scheduler itself knows nothing about what any given job *does*: a
schedule carries a ``task_type`` that selects a
:class:`~back.objects.registry.scheduler_tasks.TaskTypeSpec`, plus a
free-form ``config`` dict that the spec validates. Adding a job kind
means adding a spec, not touching this module.

Each schedule entry contains:

- ``task_type``        -- which executor to run
- ``domain_name``      -- the domain it targets
- ``target_key``       -- sub-object inside the domain (cohort rule id), or ``""``
- ``interval_minutes`` -- how often to run (minimum 2)
- ``version``          -- domain version, or ``"latest"``
- ``config``           -- type-specific options
- ``enabled``          -- whether the schedule is active
- ``last_run`` / ``last_status`` / ``last_message`` / ``last_count``

Jobs are restored at startup from env-var credentials when available.
If registry config is session-only, jobs are lazily registered when a
user opens the Scheduler tab (:meth:`BuildScheduler.get_all_schedules`).
"""

from __future__ import annotations

import threading
import time
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

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger

from .scheduler_tasks import RunOutcome, TaskContext, get_task_type
from .store.base import parse_schedule_key, schedule_key

logger = get_logger(__name__)

_MAX_HISTORY = 50
_MIN_INTERVAL_MINUTES = 2
_JOB_PREFIX = "sched_"

_scheduler_instance: Optional["BuildScheduler"] = None
_lock = threading.Lock()


def get_scheduler() -> "BuildScheduler":
    """Return the singleton ``BuildScheduler`` (created lazily)."""
    global _scheduler_instance
    if _scheduler_instance is None:
        with _lock:
            if _scheduler_instance is None:
                _scheduler_instance = BuildScheduler()
    return _scheduler_instance


class BuildScheduler:
    """Wraps APScheduler and persists schedule definitions in the registry."""

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
        """Return every schedule of every type, enriched with next-run info.

        Also lazily registers APScheduler jobs for enabled schedules that
        are missing (e.g. after an app restart when env-var credentials
        were not available at startup).
        """
        schedules = self._load_schedules(host, token, registry_cfg)
        result = []
        for key, cfg in schedules.items():
            entry = self._entry_from_config(key, cfg)
            job_id = self._job_id(
                entry["task_type"], entry["domain_name"], entry["target_key"]
            )
            job = self._sched.get_job(job_id)
            if not job and entry["enabled"] and self._started:
                self._add_or_update_job(self._settings, entry, registry_cfg)
                job = self._sched.get_job(job_id)
                logger.info("Lazily registered missing APScheduler job '%s'", job_id)

            entry["next_run"] = (
                job.next_run_time.isoformat() if job and job.next_run_time else None
            )
            result.append(entry)
        return result

    def get_schedule_history(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        task_type: str,
        domain_name: str,
        target_key: str = "",
    ) -> List[Dict[str, Any]]:
        """Return the run history for a single schedule, newest first."""
        key = schedule_key(task_type, domain_name, target_key)
        return list(reversed(self._load_history(host, token, registry_cfg, key)))

    def save_schedule(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        settings,
        task_type: str,
        domain_name: str,
        interval_minutes: int,
        *,
        target_key: str = "",
        enabled: bool = True,
        version: str = "latest",
        config: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str]:
        """Create or update a schedule. Returns ``(ok, message)``."""
        try:
            spec = get_task_type(task_type)
            if not domain_name:
                raise ValidationError("Domain name is required")
            if interval_minutes < _MIN_INTERVAL_MINUTES:
                raise ValidationError(
                    f"Minimum interval is {_MIN_INTERVAL_MINUTES} minutes"
                )
            target_key = (target_key or "").strip()
            if spec.needs_target and not target_key:
                raise ValidationError(f"{spec.target_label} is required")
            if not spec.needs_target:
                target_key = ""
            normalized = spec.normalize_config(config or {})
        except ValidationError as exc:
            return False, str(exc)

        schedules = self._load_schedules(host, token, registry_cfg)
        key = schedule_key(spec.key, domain_name, target_key)
        prev = schedules.get(key) or {}
        schedules[key] = {
            "task_type": spec.key,
            "domain_name": domain_name,
            "target_key": target_key,
            "interval_minutes": interval_minutes,
            "enabled": enabled,
            "version": version or "latest",
            "config": normalized,
            "last_run": prev.get("last_run"),
            "last_status": prev.get("last_status"),
            "last_message": prev.get("last_message"),
            "last_count": prev.get("last_count", 0),
        }

        ok, msg = self._persist_schedules(host, token, registry_cfg, schedules)
        if not ok:
            return False, msg

        if enabled and self._started:
            self._add_or_update_job(
                settings, self._entry_from_config(key, schedules[key]), registry_cfg
            )
        else:
            self._remove_job(spec.key, domain_name, target_key)

        logger.info(
            "Schedule saved for %s: every %d min, version=%s, enabled=%s",
            key,
            interval_minutes,
            version,
            enabled,
        )
        return True, f"{spec.label} schedule for '{domain_name}' saved"

    def remove_schedule(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        task_type: str,
        domain_name: str,
        target_key: str = "",
    ) -> Tuple[bool, str]:
        """Delete a schedule."""
        key = schedule_key(task_type, domain_name, target_key)
        schedules = self._load_schedules(host, token, registry_cfg)
        if key not in schedules:
            return False, f"No schedule found for '{domain_name}'"

        del schedules[key]
        ok, msg = self._persist_schedules(host, token, registry_cfg, schedules)
        if not ok:
            return False, msg

        self._remove_job(task_type, domain_name, target_key)
        logger.info("Schedule removed: %s", key)
        return True, f"Schedule for '{domain_name}' removed"

    def run_schedule_now(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        settings,
        task_type: str,
        domain_name: str,
        target_key: str = "",
    ) -> Tuple[bool, str]:
        """Fire a schedule immediately, as a one-shot job.

        Queues a ``DateTrigger`` job for *now* so the run happens in the
        same worker thread pool as the recurring schedule — no FastAPI
        request thread is blocked, and status / history / TaskManager are
        all updated by the usual harness. The persisted schedule is
        untouched: the next periodic run still fires on its own clock.
        """
        from apscheduler.triggers.date import DateTrigger

        key = schedule_key(task_type, domain_name, target_key)
        cfg = self._load_schedules(host, token, registry_cfg).get(key)
        if not cfg:
            return False, f"No schedule found for '{domain_name}'"
        if not self._started:
            return False, "Scheduler is not running"

        spec = get_task_type(task_type)
        label = f"{domain_name}/{target_key}" if target_key else domain_name
        run_id = f"manual_{task_type}_{domain_name}__{target_key}_{int(time.time() * 1000)}"
        self._sched.add_job(
            _run_scheduled_task,
            trigger=DateTrigger(run_date=datetime.now(timezone.utc)),
            id=run_id,
            name=f"Manual {spec.label} {label}",
            kwargs=self._job_kwargs(
                self._entry_from_config(key, cfg), settings, registry_cfg
            ),
            misfire_grace_time=self._MISFIRE_GRACE,
            coalesce=True,
            max_instances=1,
        )
        logger.info("Manual trigger queued for %s (run_id=%s)", key, run_id)
        return True, f"{spec.label} for '{label}' queued"

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _entry_from_config(key: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
        """Normalise a stored schedule row into the API/UI entry shape."""
        task_type, domain_name, target_key = parse_schedule_key(key)
        entry = {
            "key": key,
            "task_type": cfg.get("task_type") or task_type,
            "domain_name": cfg.get("domain_name") or domain_name,
            "target_key": cfg.get("target_key") or target_key,
            "interval_minutes": int(cfg.get("interval_minutes", 60)),
            "enabled": bool(cfg.get("enabled", True)),
            "version": cfg.get("version") or "latest",
            "config": dict(cfg.get("config") or {}),
            "last_run": cfg.get("last_run"),
            "last_status": cfg.get("last_status"),
            "last_message": cfg.get("last_message"),
            "last_count": int(cfg.get("last_count") or 0),
        }
        try:
            entry["label"] = get_task_type(entry["task_type"]).label
        except ValidationError:
            entry["label"] = entry["task_type"]
        return entry

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
                from back.objects.session import global_config_service

                global_config_service._cache = None
                global_config_service._cache_ts = 0.0
            return ok, msg
        except Exception as e:
            logger.exception("Could not persist schedules: %s", e)
            return False, str(e)

    def _load_history(
        self, host: str, token: str, registry_cfg: Dict[str, str], key: str
    ) -> List[Dict[str, Any]]:
        if not host or not registry_cfg.get("catalog"):
            return []
        try:
            store = self._store_for(host, token, registry_cfg)
            return list(store.load_schedule_history(key))
        except Exception as e:
            logger.debug("Could not load history for '%s': %s", key, e)
            return []

    def _append_history(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        key: str,
        entry: Dict[str, Any],
    ) -> None:
        try:
            store = self._store_for(host, token, registry_cfg)
            store.append_schedule_history(key, entry, max_entries=_MAX_HISTORY)
        except Exception as e:
            logger.warning("Could not save history for '%s': %s", key, e)

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

    @staticmethod
    def _job_id(task_type: str, domain_name: str, target_key: str = "") -> str:
        return f"{_JOB_PREFIX}{task_type}_{domain_name}__{target_key or ''}"

    @staticmethod
    def _job_kwargs(
        entry: Dict[str, Any], settings, registry_cfg: Optional[Dict[str, str]]
    ) -> Dict[str, Any]:
        return {
            "task_type": entry["task_type"],
            "domain_name": entry["domain_name"],
            "target_key": entry["target_key"],
            "settings": settings,
            "registry_cfg": registry_cfg,
            "version": entry["version"],
            "config": entry["config"],
        }

    def _add_or_update_job(
        self,
        settings,
        entry: Dict[str, Any],
        registry_cfg: Optional[Dict[str, str]] = None,
    ):
        job_id = self._job_id(
            entry["task_type"], entry["domain_name"], entry["target_key"]
        )
        if self._sched.get_job(job_id):
            self._sched.remove_job(job_id)

        label = entry["domain_name"]
        if entry["target_key"]:
            label = f"{label}/{entry['target_key']}"
        job = self._sched.add_job(
            _run_scheduled_task,
            trigger=IntervalTrigger(minutes=entry["interval_minutes"]),
            id=job_id,
            name=f"{entry.get('label', entry['task_type'])} {label}",
            kwargs=self._job_kwargs(entry, settings, registry_cfg),
            replace_existing=True,
            misfire_grace_time=self._MISFIRE_GRACE,
            coalesce=True,
            max_instances=1,
        )
        next_run = job.next_run_time.isoformat() if job.next_run_time else "unknown"
        logger.info(
            "APScheduler job added/updated: %s (every %d min, next_run=%s, "
            "misfire_grace=%ds)",
            job_id,
            entry["interval_minutes"],
            next_run,
            self._MISFIRE_GRACE,
        )

    def _remove_job(self, task_type: str, domain_name: str, target_key: str = ""):
        job_id = self._job_id(task_type, domain_name, target_key)
        if self._sched.get_job(job_id):
            self._sched.remove_job(job_id)
            logger.info("APScheduler job removed: %s", job_id)

    def _restore_jobs(self, settings):
        """Re-register APScheduler jobs for all enabled schedules on startup."""
        host, token, reg = self._resolve_creds(settings)
        if not host or not reg.get("catalog"):
            logger.info(
                "No credentials/registry from env at startup; "
                "jobs will be lazily registered when a user opens the Scheduler tab"
            )
            return
        count = 0
        for key, cfg in self._load_schedules(host, token, reg).items():
            if not cfg.get("enabled"):
                continue
            entry = self._entry_from_config(key, cfg)
            if not entry["domain_name"]:
                continue
            self._add_or_update_job(settings, entry, reg)
            count += 1
        logger.info("Restored %d scheduled job(s)", count)


# ======================================================================
# Task execution (runs in APScheduler's thread pool)
# ======================================================================


def _outcome_from_task(tm, task_id: str, spec) -> RunOutcome:
    """Derive the outcome from a task the executor completed itself.

    Analytics and inference delegate to services that own the
    TaskManager task, so the harness reads the finished task back rather
    than completing it a second time.
    """
    from back.core.task_manager.models import TaskStatus

    task = tm.get_task(task_id) if tm else None
    if task is None:
        return RunOutcome(status="error", message="Task disappeared before it finished")

    result = task.result if isinstance(task.result, dict) else {}
    if task.status != TaskStatus.COMPLETED:
        return RunOutcome(
            status="error",
            message=task.error or task.message or "Run failed",
        )
    return RunOutcome(
        status="success",
        message=task.message or "Completed",
        count=int(result.get(spec.count_key, 0) or 0) if spec.count_key else 0,
        detail={k: result[k] for k in spec.detail_keys if k in result},
    )


def _run_scheduled_task(
    task_type: str,
    domain_name: str,
    target_key: str = "",
    settings=None,
    registry_cfg: Optional[Dict[str, str]] = None,
    version: str = "latest",
    config: Optional[Dict[str, Any]] = None,
) -> None:
    """Run one scheduled task of any type.

    Owns the whole envelope every task type shares: the TaskManager
    task, credential resolution, the execution context, and the
    status/history write-back. The type-specific work is the
    :attr:`TaskTypeSpec.run` callable.

    Wrapped in a fail-safe try/except so no exception can escape to
    APScheduler's executor.
    """
    key = schedule_key(task_type, domain_name, target_key)
    label = f"{domain_name}/{target_key}" if target_key else domain_name
    logger.info(
        "Scheduled %s FIRED for '%s' version=%s (thread=%s)",
        task_type,
        label,
        version,
        threading.current_thread().name,
    )
    start = time.time()
    run_ts = datetime.now(timezone.utc).isoformat()

    spec = None
    tm = None
    task = None
    ctx = None
    host: str = ""
    token: str = ""
    reg: Dict[str, str] = {}
    outcome = RunOutcome(status="error", message="")

    try:
        spec = get_task_type(task_type)
        scheduler = get_scheduler()

        from back.core.task_manager import get_task_manager

        tm = get_task_manager()
        task = tm.create_task(
            name=f"Scheduled {spec.label} — {label}",
            task_type=spec.task_tag,
            steps=list(spec.steps),
        )
        tm.start_task(task.id, f"Starting {spec.label.lower()} for {label}...")

        host, token, env_reg = scheduler._resolve_creds(settings)
        reg = registry_cfg or env_reg
        if not host or not token:
            raise InfrastructureError("Databricks host/token not available")

        from back.objects.registry.RegistryService import RegistryCfg

        cfg = RegistryCfg.from_dict(reg)
        if not cfg.catalog or not cfg.schema:
            raise ValidationError("Registry not configured")

        ctx = TaskContext(
            task_type=spec.key,
            domain_name=domain_name,
            target_key=target_key or "",
            version=version or "latest",
            config=dict(config or {}),
            settings=settings,
            registry_cfg=reg,
            host=host,
            token=token,
            tm=tm,
            task_id=task.id,
            run_ts=run_ts,
        )

        result = spec.run(ctx)
        if spec.delegates_task_lifecycle:
            outcome = _outcome_from_task(tm, task.id, spec)
        else:
            outcome = result or RunOutcome(status="success", message="Completed")

    except Exception as exc:
        outcome = RunOutcome(status="error", message=str(exc))
        logger.exception(
            "Scheduled %s [%s] failed after %.1fs: %s",
            task_type,
            label,
            time.time() - start,
            exc,
        )

    finally:
        duration = time.time() - start

        try:
            if tm and task:
                if spec is not None and spec.delegates_task_lifecycle:
                    # The delegate already completed or failed the task;
                    # this only catches failures raised before it ran.
                    if outcome.status != "success":
                        tm.fail_task(task.id, outcome.message)
                elif outcome.status == "success":
                    tm.complete_task(
                        task.id,
                        result=outcome.task_result
                        or {"duration_seconds": duration},
                        message=outcome.message,
                    )
                else:
                    tm.fail_task(task.id, outcome.message)
        except Exception as tm_exc:
            logger.error(
                "Scheduled %s [%s]: task-manager update failed: %s",
                task_type,
                label,
                tm_exc,
            )

        if host and reg.get("catalog"):
            try:
                _update_schedule_status(
                    host, token, reg, key, outcome, duration_s=duration, run_ts=run_ts
                )
            except Exception as status_exc:
                logger.error(
                    "Scheduled %s [%s]: failed to update status: %s",
                    task_type,
                    label,
                    status_exc,
                )
        else:
            logger.error(
                "Scheduled %s [%s]: cannot update status — no host or registry config",
                task_type,
                label,
            )

        if spec is not None and spec.on_finish is not None and ctx is not None:
            try:
                spec.on_finish(ctx, outcome, duration)
            except Exception as hook_exc:  # noqa: BLE001
                logger.warning(
                    "Scheduled %s [%s]: on_finish hook failed: %s",
                    task_type,
                    label,
                    hook_exc,
                )

        logger.info(
            "Scheduled %s [%s]: finished with status=%s in %.1fs",
            task_type,
            label,
            outcome.status,
            duration,
        )


def _update_schedule_status(
    host: str,
    token: str,
    registry_cfg: Dict[str, str],
    key: str,
    outcome: RunOutcome,
    duration_s: float = 0.0,
    run_ts: str = "",
):
    """Stamp last_run / last_status / last_message and append to history.

    *run_ts* is the ISO timestamp shared with the run itself (for builds,
    ``domain.last_build``) so both values always match afterwards.
    """
    try:
        ts = run_ts or datetime.now(timezone.utc).isoformat()
        scheduler = get_scheduler()

        schedules = scheduler._load_schedules(host, token, registry_cfg)
        if key in schedules:
            schedules[key]["last_run"] = ts
            schedules[key]["last_status"] = outcome.status
            schedules[key]["last_message"] = outcome.message
            schedules[key]["last_count"] = outcome.count
            scheduler._persist_schedules(host, token, registry_cfg, schedules)

        scheduler._append_history(
            host,
            token,
            registry_cfg,
            key,
            {
                "timestamp": ts,
                "status": outcome.status,
                "message": outcome.message,
                "duration_s": round(duration_s, 1),
                "triple_count": outcome.count,
                "detail": dict(outcome.detail or {}),
            },
        )
    except Exception as e:
        logger.warning("Could not update schedule status for '%s': %s", key, e)
