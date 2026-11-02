"""Start, stop and report the realtime stream job of a published domain version."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from back.core.errors import InfrastructureError, NotFoundError, OntoBricksError, ValidationError
from back.core.logging import get_logger

logger = get_logger(__name__)

#: Run life-cycle states in which a stream is (about to be) running.
_ACTIVE_STATES = ("PENDING", "QUEUED", "BLOCKED", "RUNNING", "TERMINATING", "WAITING_FOR_RETRY")


class TwinRealtimeStream:
    """The ``realtime`` mode's stream job (``resources/triple_rtm.job.yml``) for one domain version.

    :meth:`start` checks the prerequisites (the deployment setting, a
    published version, a complete ``realtime_source``, a Lakebase store, the
    sink credentials and checkpoint root), creates the ``_live`` table,
    uploads the mappings the job projects with, resolves the job by name and
    ``run_now``\\ s it with ``job_parameters``. A run is found again by its
    ``domain`` / ``version`` parameters among the job's active runs, so a
    second start reuses it and :meth:`stop` / :meth:`status` need no stored
    state. The build records the run in its trace (``realtime_run_id``,
    ``realtime_state``). Every call goes through *client_factory* (a
    ``WorkspaceClient`` by default).
    """

    def __init__(self, settings: Any, *, client_factory: Callable[[], Any] | None = None) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_factory
        self._client: Any = None
        self._job_id: int | None = None

    @staticmethod
    def _default_factory() -> Any:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient()

    def _call(self, what: str, call: Callable[[Any], Any]) -> Any:
        try:
            if self._client is None:
                self._client = self._client_factory()
            return call(self._client)
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(f"Could not {what}", detail=str(exc)) from exc

    # ------------------------------------------------------------------
    # Job and runs
    # ------------------------------------------------------------------

    @staticmethod
    def job_name(settings: Any) -> str:
        """``ONTOBRICKS_REALTIME_JOB_NAME``, else ``<app>-triple-rtm`` (``""`` without an app name)."""
        explicit = (getattr(settings, "realtime_job_name", "") or "").strip()
        if explicit:
            return explicit
        app_name = (getattr(settings, "ontobricks_app_name", "") or "").strip()
        return f"{app_name}-triple-rtm" if app_name else ""

    def resolve_job_id(self) -> int:
        """The deployed job's id: an exact name, else a ``[dev <user>] `` prefixed one."""
        if self._job_id is not None:
            return self._job_id
        name = self.job_name(self._settings)
        if not name:
            raise NotFoundError(
                "No realtime stream job name is configured (ONTOBRICKS_REALTIME_JOB_NAME)"
            )
        jobs = self._call("list the Databricks jobs", lambda w: list(w.jobs.list()))
        names = [(getattr(getattr(j, "settings", None), "name", "") or "", j) for j in jobs]
        match = [j for n, j in names if n == name] or [
            j for n, j in names if n.endswith(f"] {name}")
        ]
        if not match:
            raise NotFoundError(
                f"Databricks job {name!r} was not found: deploy the bundle (make deploy) and "
                "grant the app CAN_MANAGE_RUN on it (make bootstrap-perms)"
            )
        self._job_id = int(match[0].job_id)
        return self._job_id

    def active_run(self, domain: Any) -> Any:
        """The job's active run streaming *domain*'s version, or ``None``."""
        name, version = self._domain_key(domain)
        job_id = self.resolve_job_id()
        runs = self._call(
            "list the stream job runs",
            lambda w: list(w.jobs.list_runs(job_id=job_id, active_only=True)),
        )
        for run in runs:
            params = {
                getattr(p, "name", ""): getattr(p, "value", "")
                for p in (getattr(run, "job_parameters", None) or [])
            }
            if params.get("domain") == name and params.get("version") == version:
                return run
        return None

    @staticmethod
    def run_state(run: Any) -> str:
        """The life-cycle state of *run* (``RUNNING``, ``PENDING``, …)."""
        state = getattr(run, "state", None)
        value = getattr(state, "life_cycle_state", "") or ""
        return str(getattr(value, "value", value) or "UNKNOWN")

    # ------------------------------------------------------------------
    # Start / stop / status
    # ------------------------------------------------------------------

    def start(self, domain: Any, *, store: Any = None) -> dict:
        """Start (or reuse) the stream of *domain*'s published version.

        Returns ``{"run_id", "state", "reused"}``. Prerequisites that do not
        hold raise :class:`ValidationError`; Databricks API errors
        :class:`InfrastructureError`; a job that is not deployed
        :class:`NotFoundError`.
        """
        config = self._require_ready(domain)
        existing = self.active_run(domain)
        if existing is not None:
            return {"run_id": int(existing.run_id), "state": self.run_state(existing), "reused": True}
        store = store if store is not None else self._store(domain)
        live = self._live_table(store, domain)
        params = self.job_parameters(
            domain,
            self._settings,
            config,
            live_table=f"{live.connection()['schema']}.{live.table_name(self._graph(domain))}",
            connection=live.connection(),
            mappings_path=self._upload_mappings(domain),
        )
        job_id = self.resolve_job_id()
        started = self._call(
            "start the realtime stream job",
            lambda w: w.jobs.run_now(job_id=job_id, job_parameters=params),
        )
        run_id = getattr(started, "run_id", None) or getattr(
            getattr(started, "response", None), "run_id", None
        )
        if run_id is None:
            raise InfrastructureError("The stream job was started but the SDK returned no run id")
        logger.info("Started realtime stream run %s for %s", run_id, params["live_table"])
        return {"run_id": int(run_id), "state": "PENDING", "reused": False}

    def stop(self, domain: Any) -> dict:
        """Cancel *domain*'s active stream run; ``{"run_id", "state"}`` (``not_running`` when none)."""
        run = self.active_run(domain)
        if run is None:
            return {"run_id": None, "state": "not_running"}
        run_id = int(run.run_id)
        self._call("cancel the realtime stream run", lambda w: w.jobs.cancel_run(run_id=run_id))
        logger.info("Cancelled realtime stream run %s", run_id)
        return {"run_id": run_id, "state": "CANCELLING"}

    def status(self, domain: Any) -> dict:
        """``{"run_id", "state"}`` of *domain*'s stream; ``not_configured`` / ``not_running`` / ``unknown``."""
        if not self.job_name(self._settings):
            return {"run_id": None, "state": "not_configured"}
        try:
            run = self.active_run(domain)
        except OntoBricksError as exc:
            return {"run_id": None, "state": "unknown", "error": str(exc)}
        if run is None:
            return {"run_id": None, "state": "not_running"}
        return {"run_id": int(run.run_id), "state": self.run_state(run)}

    # ------------------------------------------------------------------
    # Build hooks
    # ------------------------------------------------------------------

    @staticmethod
    def after_build(pipe: Any, *, factory: Callable[[Any], Any] | None = None) -> None:
        """Start the stream of an effective ``realtime`` build; stop a leftover one otherwise.

        Best-effort: the build has already produced the microbatch graph, so a
        stream that cannot start is recorded (``realtime_state =
        failed:<Error>``) and logged, never raised. A non-realtime build of a
        deployment with realtime enabled looks for a stream only when the
        recent build runs recorded one; with realtime disabled it looks for
        none (stop a leftover stream from the domain's status card).
        """
        from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory

        if getattr(pipe.settings, "realtime_enabled", False) is not True:
            return
        decision = getattr(pipe, "mode_decision", None)
        realtime = getattr(decision, "effective", "") == "realtime"
        if not realtime and not TwinRealtimeStream._streamed_before(
            TwinBuildRunHistory.recent_runs(pipe)
        ):
            return
        stream = (factory or TwinRealtimeStream)(pipe.settings)
        try:
            if realtime:
                started = stream.start(pipe.domain, store=getattr(pipe, "store", None))
                pipe.realtime_run_id, pipe.realtime_state = str(started["run_id"]), started["state"]
            else:
                stopped = stream.stop(pipe.domain)
                pipe.realtime_run_id = str(stopped["run_id"] or "")
                pipe.realtime_state = stopped["state"]
        except OntoBricksError as exc:
            logger.warning("[DT-BUILD %s] realtime stream: %s", pipe.task_id, exc)
            pipe.realtime_state = f"failed:{type(exc).__name__}"

    @staticmethod
    def _streamed_before(runs: Any) -> bool:
        """True when a recent build run recorded a stream run."""
        from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory

        return any(
            (TwinBuildRunHistory.facts_of(run) or {}).get("realtime_run_id")
            for run in (runs if isinstance(runs, list) else [])
        )

    # ------------------------------------------------------------------
    # Parameters and prerequisites
    # ------------------------------------------------------------------

    @staticmethod
    def job_parameters(
        domain: Any,
        settings: Any,
        config: dict,
        *,
        live_table: str,
        connection: dict,
        mappings_path: str,
    ) -> dict:
        """The ``job_parameters`` of one run (every argument the job parses; no secret value)."""
        from back.core.graphdb.delta import _table_naming

        name, version = TwinRealtimeStream._domain_key(domain)
        root = str(getattr(settings, "realtime_checkpoint_root", "") or "").rstrip("/")
        return {
            "domain": name,
            "version": version,
            "source_kind": config["kind"],
            "bootstrap_servers": config["bootstrap_servers"],
            "topic": config["topic"],
            "secret_scope": config["secret_scope"],
            "secret_key": config["secret_key"],
            "slots_table": _table_naming.slots_table_fqn(domain, settings),
            "mappings_json_path": mappings_path,
            "lakebase_host": connection.get("host", ""),
            "lakebase_db": connection.get("database", ""),
            "lakebase_user": str(getattr(settings, "realtime_lakebase_user", "") or ""),
            "lakebase_secret": str(getattr(settings, "realtime_lakebase_secret", "") or ""),
            "live_table": live_table,
            "checkpoint": f"{root}/{TwinRealtimeStream._run_name(domain)}",
        }

    def _require_ready(self, domain: Any) -> dict:
        """The domain's realtime config once every prerequisite holds."""
        from back.core.graphdb import RealtimeSource

        settings = self._settings
        if getattr(settings, "realtime_enabled", False) is not True:
            raise ValidationError("Realtime is disabled on this deployment (ONTOBRICKS_REALTIME_ENABLED)")
        if str((getattr(domain, "info", None) or {}).get("status") or "").upper() != "PUBLISHED":
            raise ValidationError("Only a published domain version streams in realtime")
        config = RealtimeSource.of(domain)
        if not RealtimeSource.is_complete(config):
            raise ValidationError("The domain's realtime source (topic and secret names) is incomplete")
        missing = [
            env
            for attr, env in (
                ("realtime_lakebase_user", "ONTOBRICKS_REALTIME_LAKEBASE_USER"),
                ("realtime_lakebase_secret", "ONTOBRICKS_REALTIME_LAKEBASE_SECRET"),
                ("realtime_checkpoint_root", "ONTOBRICKS_REALTIME_CHECKPOINT_ROOT"),
            )
            if not str(getattr(settings, attr, "") or "").strip()
        ]
        if missing:
            raise ValidationError(f"Realtime needs {', '.join(missing)}")
        return config

    def _store(self, domain: Any) -> Any:
        from back.core.graphdb import get_graphdb

        return get_graphdb(domain, self._settings)

    def _live_table(self, store: Any, domain: Any) -> Any:
        """The store's ``_live`` table, created; its union view refreshed when synced."""
        if store is None or not hasattr(store, "live_table"):
            raise ValidationError("Realtime serving needs the Lakebase graph backend")
        live = store.live_table()
        graph = self._graph(domain)
        live.ensure(graph)
        if getattr(store, "is_synced", False) is True:
            try:
                store.ensure_synced_union_view(graph)
            except Exception as exc:  # noqa: BLE001 — the next sync recreates the view
                logger.warning("Union view of %s not refreshed yet: %s", graph, exc)
        return live

    def _upload_mappings(self, domain: Any) -> str:
        """Upload the mappings the job projects with; return the workspace file path."""
        from back.objects.digitaltwin.build.TwinIncrementalBuild import TwinIncrementalBuild

        mappings = TwinIncrementalBuild.mappings_for(domain)
        if mappings is None:
            raise ValidationError("The domain has no R2RML mappings to stream")
        entities, relationships, base_uri = mappings
        payload = json.dumps(
            {"entity_mappings": entities, "relationship_mappings": relationships, "base_uri": base_uri},
            default=str,
        ).encode("utf-8")
        me = self._call("resolve the current user", lambda w: w.current_user.me())
        folder = f"/Workspace/Users/{getattr(me, 'user_name', '')}/.ontobricks/realtime"
        path = f"{folder}/{self._run_name(domain)}.json"
        self._call(f"create {folder}", lambda w: w.workspace.mkdirs(folder))
        self._call(
            f"upload {path}",
            lambda w: w.workspace.upload(path, payload, format=self._auto_format(), overwrite=True),
        )
        return path

    @staticmethod
    def _auto_format() -> Any:
        try:
            from databricks.sdk.service.workspace import ImportFormat
        except ImportError:  # pragma: no cover — every supported SDK has it
            return None
        return ImportFormat.AUTO

    @staticmethod
    def _domain_key(domain: Any) -> tuple[str, str]:
        """``(domain name, version)`` a run is started with and found again by."""
        name = str((getattr(domain, "info", None) or {}).get("name") or "")
        return name, str(getattr(domain, "current_version", "") or "")

    @staticmethod
    def _graph(domain: Any) -> str:
        from back.core.helpers import effective_graph_name

        return effective_graph_name(domain)

    @staticmethod
    def _run_name(domain: Any) -> str:
        from back.core.streaming import SlotPipelineRunner

        name, version = TwinRealtimeStream._domain_key(domain)
        return SlotPipelineRunner.pipeline_name(name, version).replace(
            "ontobricks-slots-", "ontobricks-rtm-", 1
        )
