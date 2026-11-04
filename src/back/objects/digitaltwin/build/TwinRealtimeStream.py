"""Start, stop and report the realtime stream of a published domain version."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from back.core.errors import OntoBricksError, ValidationError
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinLiveServing import TwinLiveServing
from back.objects.digitaltwin.build.TwinRealtimeJob import TwinRealtimeJob

logger = get_logger(__name__)


class TwinRealtimeStream:
    """The ``realtime`` mode's stream for one domain version: its job run and its ``_live``.

    :meth:`start` checks the prerequisites, cancels a run of the same domain
    at another version (one stream per domain; the job allows one run at a
    time) and retires that version's ``_live``, creates ``_live``, seeds it
    from ``…_slots`` at a pinned version and serves it (the union view reads it
    from then on), uploads the mappings and starts the job
    (:class:`TwinRealtimeJob`) on the same snapshot version, replaying the
    topic from just before it. :meth:`stop` cancels the run and retires
    ``_live`` (emptied, no longer served), so a stopped stream never leaves a
    frozen ``_live`` in the view. The build and the scheduled tick call
    :meth:`after_build` / :meth:`after_tick`.
    """

    def __init__(self, settings: Any, *, client_factory: Callable[[], Any] | None = None) -> None:
        self._settings = settings
        self._job = TwinRealtimeJob(settings, client_factory=client_factory)

    # ------------------------------------------------------------------
    # Start / stop / status
    # ------------------------------------------------------------------

    def start(self, domain: Any, *, store: Any = None, client: Any = None) -> dict:
        """Start (or reuse) the stream of *domain*'s published version.

        Returns ``{"run_id", "state", "reused", "seeded", "slots_version"}``
        (``slots_version``: the ``_slots`` version ``_live`` was seeded at and
        the job starts from; ``None`` for a reused run). *client* is the
        warehouse client the ``_slots`` snapshot is read on (default: the
        domain's build warehouse). Prerequisites that do not hold raise
        :class:`ValidationError`; Databricks API errors
        :class:`InfrastructureError`; a job that is not deployed
        :class:`NotFoundError`. A failure after the seed retires ``_live``
        again before it is raised.
        """
        config = self._require_ready(domain)
        name, version = self._domain_key(domain)
        store = store if store is not None else self._store(domain)
        reused = None
        for run_version, run in self._job.active_runs(name):
            if run_version == version and reused is None:
                reused = run
            else:
                logger.info("Stopping the stream of %s v%s for v%s", name, run_version, version)
                self._job.cancel(int(run.run_id))
                self._retire_quietly(store, self._at_version(domain, run_version))
        if reused is not None:
            return {
                "run_id": int(reused.run_id),
                "state": self._job.run_state(reused),
                "reused": True,
                "seeded": 0,
                "slots_version": None,
            }
        live = TwinLiveServing.live(store)
        seed = TwinLiveServing.start(
            store, domain, self._settings, client if client is not None else self._client(domain)
        )
        graph = self._graph(domain)
        try:
            params = self.job_parameters(
                domain,
                self._settings,
                config,
                live_table=f"{live.connection()['schema']}.{live.table_name(graph)}",
                connection=live.connection(),
                mappings_path=self._job.upload_mappings(self._run_name(domain), self._mappings(domain)),
                seed=seed,
            )
            run_id = self._job.run_now(params)
        except Exception:
            self._retire_quietly(store, domain)
            raise
        logger.info(
            "Started realtime stream run %s for %s (%d seeded at slots v%s)",
            run_id,
            graph,
            seed["seeded"],
            seed["slots_version"],
        )
        return {
            "run_id": run_id,
            "state": "PENDING",
            "reused": False,
            "seeded": seed["seeded"],
            "slots_version": seed["slots_version"],
        }

    def stop(self, domain: Any, *, store: Any = None) -> dict:
        """Cancel *domain*'s stream run and retire its ``_live``; ``{"run_id", "state"}``."""
        name, version = self._domain_key(domain)
        run_id = None
        for run_version, run in self._job.active_runs(name):
            if run_version == version:
                run_id = int(run.run_id)
                self._job.cancel(run_id)
        TwinLiveServing.retire(store if store is not None else self._store(domain), domain, self._settings)
        return {"run_id": run_id, "state": "CANCELLING" if run_id else "not_running"}

    def status(self, domain: Any) -> dict:
        """``{"run_id", "state"}`` of *domain*'s stream; ``not_configured`` / ``not_running`` / ``unknown``."""
        if not TwinRealtimeJob.job_name(self._settings):
            return {"run_id": None, "state": "not_configured"}
        name, version = self._domain_key(domain)
        try:
            runs = [run for v, run in self._job.active_runs(name) if v == version]
        except OntoBricksError as exc:
            return {"run_id": None, "state": "unknown", "error": str(exc)}
        if not runs:
            return {"run_id": None, "state": "not_running"}
        return {"run_id": int(runs[0].run_id), "state": self._job.run_state(runs[0])}

    # ------------------------------------------------------------------
    # Build and tick hooks
    # ------------------------------------------------------------------

    @staticmethod
    def after_build(pipe: Any, *, factory: Callable[[Any], Any] | None = None) -> None:
        """Start the stream of an effective ``realtime`` build; retire a served ``_live`` otherwise.

        Best-effort: the build has already produced the microbatch graph, so a
        stream that cannot start or stop is recorded (``realtime_state =
        failed:<Error>``) and logged, never raised. A started stream records the
        ``_slots`` version ``_live`` was seeded at (``slots_seed_version``). A
        build that is not ``realtime``, on a deployment with realtime enabled
        and a Lakebase store whose ``_live`` is still served, cancels the stream
        (when the job is configured) and retires ``_live``; with realtime off
        no Postgres query is made.
        """
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        decision = getattr(pipe, "mode_decision", None)
        realtime = getattr(decision, "effective", "") == "realtime"
        enabled = getattr(pipe.settings, "realtime_enabled", False) is True
        store = getattr(pipe, "store", None)
        stream = (factory or TwinRealtimeStream)(pipe.settings)
        try:
            if realtime:
                started = stream.start(pipe.domain, store=store, client=pipe.source_client)
                pipe.realtime_run_id, pipe.realtime_state = str(started["run_id"]), started["state"]
                pipe.slots_seed_version = started.get("slots_version")
            elif (
                enabled
                and GraphDBFactory.classify_store(store) == "lakebase"
                and TwinLiveServing.serving(store, pipe.domain)
            ):
                stopped = stream.stop_quietly(pipe.domain, store=store)
                pipe.realtime_run_id = str(stopped["run_id"] or "")
                pipe.realtime_state = stopped["state"]
        except Exception as exc:  # noqa: BLE001 — Postgres / SDK errors too; the build has succeeded
            logger.warning("[DT-BUILD %s] realtime stream: %s", pipe.task_id, exc)
            pipe.realtime_state = f"failed:{type(exc).__name__}"

    def stop_quietly(self, domain: Any, *, store: Any) -> dict:
        """:meth:`stop`, with a job that cannot be reached still retiring ``_live``."""
        try:
            return self.stop(domain, store=store)
        except OntoBricksError as exc:
            logger.warning("Realtime stream not cancelled (%s); retiring its live table", exc)
            TwinLiveServing.retire(store, domain, self._settings)
            return {"run_id": None, "state": "retired"}

    def _retire_quietly(self, store: Any, domain: Any) -> None:
        """:meth:`TwinLiveServing.retire`, logged instead of raised (the caller's error wins)."""
        try:
            TwinLiveServing.retire(store, domain, self._settings)
        except Exception as exc:  # noqa: BLE001 — best-effort clean-up
            logger.warning("Live table of %s not retired: %s", self._graph(domain), exc)

    @staticmethod
    def _at_version(domain: Any, version: str) -> Any:
        """*domain* as seen at another *version* (its graph name and ``_live`` table)."""
        from types import SimpleNamespace

        return SimpleNamespace(
            info=getattr(domain, "info", None) or {},
            current_version=version,
            delta=getattr(domain, "delta", None) or {},
        )

    def after_tick(self, domain: Any, store: Any) -> dict:
        """A realtime tick's housekeeping: retire a dead stream's ``_live``, purge tombstones.

        A served ``_live`` whose stream no longer runs (crashed past its
        retries) is retired, so readers fall back to the microbatch graph
        until the next build restarts the stream; otherwise tombstones older
        than the state TTL (at least 24 hours) are purged. Returns the facts
        the tick records.
        """
        from back.core.graphdb import RealtimeSource

        if not TwinLiveServing.serving(store, domain):
            return {}
        state = self.status(domain)["state"]
        if state in ("not_running", "not_configured"):
            TwinLiveServing.retire(store, domain, self._settings)
            logger.warning("Realtime stream of %s is not running: _live retired", self._graph(domain))
            return {"realtime_state": "retired"}
        hours = max(24, int(RealtimeSource.of(domain).get("state_ttl_hours") or 24))
        return {"live_tombstones_purged": TwinLiveServing.purge(store, domain, hours)}

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
        seed: dict,
    ) -> dict:
        """The ``job_parameters`` of one run (every argument the job parses; no secret value).

        *seed* is :meth:`TwinLiveServing.start`'s result: the job reads its
        initial state at ``slots_version`` and replays the topic from
        ``start_timestamp_ms``.
        """
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
            "state_ttl_hours": str(config.get("state_ttl_hours", 72)),
            "slots_version": str(int(seed["slots_version"])),
            "start_timestamp_ms": str(int(seed["start_timestamp_ms"])),
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
        if not str(settings.realtime_checkpoint_root).strip().startswith("/Volumes/"):
            raise ValidationError(
                "ONTOBRICKS_REALTIME_CHECKPOINT_ROOT must be a Unity Catalog volume path (/Volumes/…)"
            )
        return config

    def _store(self, domain: Any) -> Any:
        from back.core.graphdb import get_graphdb

        return get_graphdb(domain, self._settings)

    def _client(self, domain: Any) -> Any:
        """The domain's client on its build warehouse (the ``_slots`` snapshot read)."""
        from back.core.graphdb.GraphDBFactory import GraphDBFactory

        return GraphDBFactory.build_warehouse_client(domain, self._settings)

    @staticmethod
    def _mappings(domain: Any) -> tuple:
        from back.objects.digitaltwin.build.TwinIncrementalBuild import TwinIncrementalBuild

        mappings = TwinIncrementalBuild.mappings_for(domain)
        if mappings is None:
            raise ValidationError("The domain has no R2RML mappings to stream")
        return mappings

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
