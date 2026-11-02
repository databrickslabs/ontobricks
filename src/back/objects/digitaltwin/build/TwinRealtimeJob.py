"""The Databricks Jobs side of the realtime stream: the deployed job, its runs, the mappings file."""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from typing import Any

from back.core.errors import InfrastructureError, NotFoundError, OntoBricksError
from back.core.logging import get_logger

logger = get_logger(__name__)


class TwinRealtimeJob:
    """The stream job (``resources/triple_rtm.job.yml``) and its runs, through the Databricks SDK.

    A run is identified by its ``domain`` / ``version`` job parameters, so the
    job's active runs say which domain versions stream; nothing is stored.
    Every call goes through *client_factory* (a ``WorkspaceClient`` by
    default); SDK errors become :class:`InfrastructureError`.
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

    def call(self, what: str, call: Callable[[Any], Any]) -> Any:
        try:
            if self._client is None:
                self._client = self._client_factory()
            return call(self._client)
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(f"Could not {what}", detail=str(exc)) from exc

    @staticmethod
    def job_name(settings: Any) -> str:
        """``ONTOBRICKS_REALTIME_JOB_NAME``, else ``<app>-triple-rtm`` (``""`` without an app name)."""
        explicit = (getattr(settings, "realtime_job_name", "") or "").strip()
        if explicit:
            return explicit
        app_name = (getattr(settings, "ontobricks_app_name", "") or "").strip()
        return f"{app_name}-triple-rtm" if app_name else ""

    def resolve_job_id(self) -> int:
        """The deployed job's id: listed by name first, else a ``[dev <user>] `` prefixed one."""
        if self._job_id is not None:
            return self._job_id
        name = self.job_name(self._settings)
        if not name:
            raise NotFoundError(
                "No realtime stream job name is configured (ONTOBRICKS_REALTIME_JOB_NAME)"
            )
        jobs = self.call("list the Databricks jobs", lambda w: list(w.jobs.list(name=name)))
        exact = [j for j in jobs if self._name_of(j) == name]
        if not exact:
            everything = self.call("list the Databricks jobs", lambda w: list(w.jobs.list()))
            exact = [j for j in everything if self._name_of(j).endswith(f"] {name}")]
        if not exact:
            raise NotFoundError(
                f"Databricks job {name!r} was not found: deploy the bundle (make deploy) and "
                "grant the app CAN_MANAGE_RUN on it (make bootstrap-perms)"
            )
        self._job_id = int(exact[0].job_id)
        return self._job_id

    @staticmethod
    def _name_of(job: Any) -> str:
        return getattr(getattr(job, "settings", None), "name", "") or ""

    def active_runs(self, domain_name: str) -> list[tuple[str, Any]]:
        """``(version, run)`` of the job's active runs streaming *domain_name*."""
        job_id = self.resolve_job_id()
        runs = self.call(
            "list the stream job runs",
            lambda w: list(w.jobs.list_runs(job_id=job_id, active_only=True)),
        )
        found = []
        for run in runs:
            params = {
                getattr(p, "name", ""): getattr(p, "value", "")
                for p in (getattr(run, "job_parameters", None) or [])
            }
            if params.get("domain") == domain_name:
                found.append((str(params.get("version", "")), run))
        return found

    def run_now(self, params: dict) -> int:
        job_id = self.resolve_job_id()
        started = self.call(
            "start the realtime stream job",
            lambda w: w.jobs.run_now(job_id=job_id, job_parameters=params),
        )
        run_id = getattr(started, "run_id", None) or getattr(
            getattr(started, "response", None), "run_id", None
        )
        if run_id is None:
            raise InfrastructureError("The stream job was started but the SDK returned no run id")
        return int(run_id)

    def cancel(self, run_id: int) -> None:
        self.call("cancel the realtime stream run", lambda w: w.jobs.cancel_run(run_id=run_id))
        logger.info("Cancelled realtime stream run %s", run_id)

    @staticmethod
    def run_state(run: Any) -> str:
        """The life-cycle state of *run* (``RUNNING``, ``PENDING``, …)."""
        state = getattr(run, "state", None)
        value = getattr(state, "life_cycle_state", "") or ""
        return str(getattr(value, "value", value) or "UNKNOWN")

    def upload_mappings(self, name: str, mappings: tuple) -> str:
        """Upload ``(entities, relationships, base URI)`` next to the checkpoints; the file path.

        The file goes to ``<checkpoint root>/_mappings/<name>.json`` on the
        checkpoint volume, which the app (WRITE VOLUME) and the job's run-as
        identity (READ / WRITE VOLUME, for its checkpoints) both reach; a file in
        the app principal's workspace home would not be readable by the job.
        """
        entities, relationships, base_uri = mappings
        payload = json.dumps(
            {"entity_mappings": entities, "relationship_mappings": relationships, "base_uri": base_uri},
            default=str,
        ).encode("utf-8")
        root = str(getattr(self._settings, "realtime_checkpoint_root", "") or "").rstrip("/")
        folder = f"{root}/_mappings"
        path = f"{folder}/{name}.json"
        self.call(f"create {folder}", lambda w: w.files.create_directory(folder))
        self.call(
            f"upload {path}",
            lambda w: w.files.upload(path, io.BytesIO(payload), overwrite=True),
        )
        return path
