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
    # Persistence helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _store_for(host: str, token: str, registry_cfg: Dict[str, str]):
        from back.objects.registry import RegistryCfg
        from back.objects.registry.store import RegistryFactory

        cfg = RegistryCfg.from_dict(registry_cfg)
        return RegistryFactory.from_cfg(cfg, host=host, token=token)

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

    @staticmethod
    def _resolve_creds(settings):
        """Resolve host/token/registry from env-level settings (for startup).

        The returned ``cfg`` carries ``backend``, ``lakebase_schema`` and
        ``lakebase_database`` from *Settings* so that schedule-related
        store calls made *before* the global config has been loaded
        (e.g. on app boot, when restoring jobs) target the right
        backend instead of silently defaulting to Volume. This matters
        for Databricks Apps deployments that bind a Lakebase database
        and want their schedules persisted there from the very first
        APScheduler tick.
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

        # ``"auto"`` (the new default) → Lakebase when the runtime has
        # injected PG* env vars and psycopg is importable, otherwise
        # Volume. Centralised in ``resolve_default_backend`` so the
        # scheduler subprocess and the FastAPI request path agree on
        # which backend to use.
        from back.objects.registry import resolve_default_backend

        backend = resolve_default_backend(
            getattr(settings, "registry_backend", None)
        )
        lakebase_schema = (
            getattr(settings, "lakebase_schema", "ontobricks_registry")
            or "ontobricks_registry"
        )
        lakebase_database = getattr(settings, "lakebase_database", "") or ""

        vol_path = (getattr(settings, "registry_volume_path", "") or "").strip()
        if vol_path:
            parsed = RegistryCfg.from_volume_path(
                vol_path,
                backend=backend,
                lakebase_schema=lakebase_schema,
                lakebase_database=lakebase_database,
            )
            if parsed.catalog and parsed.schema and parsed.volume:
                return host, token, parsed.as_dict()

        cfg = RegistryCfg(
            catalog=settings.registry_catalog,
            schema=settings.registry_schema,
            volume=settings.registry_volume or "OntoBricksRegistry",
            backend=backend,
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
        "SELECT ?subject ?predicate ?object\n"
        "WHERE {\n    ?subject ?predicate ?object .\n}"
    )
    res = sparql.translate_sparql_to_spark(sparql_q, ent, None, rels, dialect="spark")

    return res["sql"], view_table, graph_name, base_uri, ent, rels


def _write_graph_triples(
    store,
    src,
    graph_name: str,
    view_table: str,
    actual_mode: str,
    to_add: list,
    to_remove: list,
    incr_svc,
    domain_name: str,
) -> int:
    """Write triples to the graph store. Returns the triple count."""
    if actual_mode == "full":
        logger.info("Scheduled build [%s]: reading triples from VIEW", domain_name)
        triples = src.execute_query(f"SELECT * FROM {view_table}")
        triple_count = len(triples)
        logger.info(
            "Scheduled build [%s]: %d triples from VIEW", domain_name, triple_count
        )

        if triple_count > 0:
            store.drop_table(graph_name)
            store.create_table(graph_name)
            store.insert_triples(graph_name, triples, batch_size=500)
            store.optimize_table(graph_name)
            logger.info(
                "Scheduled build [%s]: graph '%s' populated with %d triples",
                domain_name,
                graph_name,
                triple_count,
            )
    else:
        triple_count = incr_svc.count_view_triples(view_table)
        if to_remove:
            store.delete_triples(graph_name, to_remove, batch_size=500)
            logger.info(
                "Scheduled build [%s]: removed %d triples", domain_name, len(to_remove)
            )
        if to_add:
            store.insert_triples(graph_name, to_add, batch_size=500)
            logger.info(
                "Scheduled build [%s]: added %d triples", domain_name, len(to_add)
            )
        store.optimize_table(graph_name)
    return triple_count


def _persist_domain_metadata(
    svc, domain, version: str, build_ts: str, domain_name: str
):
    """Stamp last_build and write the domain doc via the active store.

    ``svc`` is the :class:`RegistryService` that owns the right
    :class:`RegistryStore` for the current backend (Volume or Lakebase).
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
    drop_existing: bool,
    settings,
    registry_cfg: Optional[Dict[str, str]] = None,
    version: str = "latest",
) -> None:
    """Execute a Digital Twin build for *domain_name* without a user session.

    Loads the domain from the registry, generates SQL from R2RML,
    creates the VIEW, and populates LadybugDB.

    When *version* is ``"latest"`` (default) the newest version is loaded.
    Otherwise the specific version string is used.

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
                {"name": "graph", "description": "Populating LadybugDB graph"},
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

        # --- Incremental logic ---
        from back.core.triplestore import IncrementalBuildService

        incr_svc = IncrementalBuildService(src)

        snapshot_table = incr_svc.snapshot_table_name(
            (domain.info or {}).get("name", DEFAULT_GRAPH_NAME),
            getattr(domain, "delta", None) or {},
            version=getattr(domain, "current_version", "1"),
        )

        actual_mode = "full" if drop_existing else "incremental"
        to_add: list = []
        to_remove: list = []

        if actual_mode == "incremental" and incr_svc.snapshot_exists(snapshot_table):
            logger.info("Scheduled build [%s]: computing incremental diff", domain_name)
            try:
                to_add, to_remove = incr_svc.compute_diff(view_table, snapshot_table)
                view_count = incr_svc.count_view_triples(view_table)
                if incr_svc.should_fallback_to_full(
                    len(to_add), len(to_remove), view_count
                ):
                    actual_mode = "full"
                elif len(to_add) == 0 and len(to_remove) == 0:
                    triple_count = view_count
                    logger.info(
                        "Scheduled build [%s]: no changes, skipping graph write",
                        domain_name,
                    )
                    tm.update_progress(task.id, 90, "No changes detected")
                    try:
                        incr_svc.refresh_snapshot(view_table, snapshot_table)
                    except Exception as snap_e:
                        logger.warning(
                            "Scheduled build [%s]: snapshot refresh failed: %s",
                            domain_name,
                            snap_e,
                        )
                    status = "success"
                    message = f"No changes — {triple_count} triples unchanged"
                    _persist_domain_metadata(
                        svc, domain, version, build_ts, domain_name
                    )
                    return
            except Exception as e:
                logger.warning(
                    "Scheduled build [%s]: incremental diff failed, falling back to full: %s",
                    domain_name,
                    e,
                )
                actual_mode = "full"
        else:
            actual_mode = "full"

        # --- Step 3: Populate graph ---
        tm.advance_step(task.id, f"Applying changes to graph {graph_name}...")

        from back.core.triplestore import get_triplestore
        from back.objects.digitaltwin.models import DomainSnapshot

        snap = DomainSnapshot(domain, host=host, token=token)
        store = get_triplestore(snap, settings, backend="graph")
        if not store:
            raise InfrastructureError("Could not initialize LadybugDB backend")

        triple_count = _write_graph_triples(
            store,
            src,
            graph_name,
            view_table,
            actual_mode,
            to_add,
            to_remove,
            incr_svc,
            domain_name,
        )

        try:
            incr_svc.refresh_snapshot(view_table, snapshot_table)
            logger.info("Scheduled build [%s]: snapshot refreshed", domain_name)
        except Exception as snap_e:
            logger.warning(
                "Scheduled build [%s]: snapshot refresh failed: %s", domain_name, snap_e
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
        logger.info(
            "Scheduled build [%s]: finished with status=%s in %.1fs",
            domain_name,
            status,
            duration,
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
