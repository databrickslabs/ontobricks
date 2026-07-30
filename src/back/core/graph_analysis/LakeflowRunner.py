"""Trigger and track the serverless graph-analytics Lakeflow job.

Wraps ``WorkspaceClient.jobs`` behind a narrow surface so
:meth:`DigitalTwin.run_metrics_task` can launch the job defined in
``resources/graph_analytics.job.yml`` and poll it from the existing
``TaskManager`` worker thread. Mapping run state onto ``tm.update_progress``
keeps ``/tasks/{id}`` polling unchanged, so the front-end task tracker needs no
changes.

The job is resolved **by name**, not by a hard-coded id, because the id is
assigned at deploy time. DAB in ``mode: development`` also prefixes the
deployed name with ``[dev <user>] ``, so the lookup suffix-matches rather than
comparing for equality — see :meth:`LakeflowRunner.resolve_job_id`.

``client_factory`` and ``sleep`` are injectable so the trigger/poll logic can be
tested against a fake ``WorkspaceClient`` without a workspace or real waiting.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

from back.core.errors import InfrastructureError, NotFoundError
from back.core.logging import get_logger

logger = get_logger(__name__)

DEFAULT_POLL_INTERVAL_S = 5.0
DEFAULT_TIMEOUT_S = 3600

#: Terminal life-cycle states.
_TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def _enum_name(value: Any) -> str:
    """Normalise an SDK enum / string / None into a bare upper-case name."""
    if value is None:
        return ""
    name = getattr(value, "value", None) or getattr(value, "name", None) or str(value)
    # ``str(SomeEnum.FOO)`` renders as ``RunLifeCycleState.FOO``.
    return str(name).rsplit(".", 1)[-1].upper()


class LakeflowRunner:
    """Launch the graph-analytics job and follow a run to completion."""

    def __init__(
        self,
        job_name: str,
        *,
        client_factory: Optional[Callable[[], Any]] = None,
        sleep: Optional[Callable[[float], None]] = None,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        timeout_s: int = DEFAULT_TIMEOUT_S,
    ) -> None:
        self._job_name = (job_name or "").strip()
        self._client_factory = client_factory or self._default_factory
        self._sleep = sleep or time.sleep
        self._poll_interval_s = max(0.0, float(poll_interval_s))
        self._timeout_s = max(1, int(timeout_s))
        self._client: Optional[Any] = None
        self._job_id: Optional[int] = None

    @staticmethod
    def _default_factory() -> Any:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient()

    def _w(self) -> Any:
        if self._client is None:
            try:
                self._client = self._client_factory()
            except Exception as exc:  # noqa: BLE001
                raise InfrastructureError(
                    "Cannot initialise the Databricks SDK to run the graph "
                    "analytics job",
                    detail=str(exc),
                ) from exc
        return self._client

    # ------------------------------------------------------------------
    # Job resolution
    # ------------------------------------------------------------------

    def resolve_job_id(self) -> int:
        """Return the job id for the configured name, caching the result.

        Prefers an exact name match, then a ``[dev <user>] `` prefixed match,
        which is what a bundle deployed in development mode produces. Raises
        :class:`NotFoundError` when nothing matches — usually because the
        bundle has not been deployed since the job was added.
        """
        if self._job_id is not None:
            return self._job_id
        if not self._job_name:
            raise NotFoundError(
                "No graph-analytics job name is configured "
                "(ONTOBRICKS_ANALYTICS_JOB_NAME)"
            )

        try:
            jobs = list(self._w().jobs.list())
        except Exception as exc:  # noqa: BLE001
            raise InfrastructureError(
                "Could not list Databricks jobs to find the graph analytics job",
                detail=str(exc),
            ) from exc

        exact: List[Any] = []
        suffixed: List[Any] = []
        for job in jobs:
            name = (getattr(getattr(job, "settings", None), "name", "") or "").strip()
            if not name:
                continue
            if name == self._job_name:
                exact.append(job)
            elif name.endswith(f"] {self._job_name}"):
                suffixed.append(job)

        match = (exact or suffixed)
        if not match:
            raise NotFoundError(
                f"Databricks job {self._job_name!r} was not found. Deploy the "
                f"bundle (make deploy) so the graph analytics job exists."
            )
        if len(match) > 1:
            logger.warning(
                "%d jobs match %r — using the first (%s)",
                len(match),
                self._job_name,
                getattr(match[0], "job_id", "?"),
            )

        self._job_id = int(getattr(match[0], "job_id"))
        logger.info("Resolved graph analytics job %r -> %s", self._job_name, self._job_id)
        return self._job_id

    # ------------------------------------------------------------------
    # Trigger and poll
    # ------------------------------------------------------------------

    def submit(
        self,
        *,
        source_table: str,
        output_table: str,
        exclude_predicates: Optional[List[str]] = None,
        pagerank_iterations: int = 20,
    ) -> int:
        """Start a run and return its run id."""
        job_id = self.resolve_job_id()
        params = {
            "source_table": source_table,
            "output_table": output_table,
            "exclude_predicates": ",".join(exclude_predicates or []),
            "pagerank_iterations": str(int(pagerank_iterations)),
        }
        try:
            started = self._w().jobs.run_now(job_id=job_id, job_parameters=params)
        except Exception as exc:  # noqa: BLE001
            raise InfrastructureError(
                f"Could not start the graph analytics job (id {job_id})",
                detail=str(exc),
            ) from exc

        run_id = getattr(started, "run_id", None)
        if run_id is None:
            # ``run_now`` returns a waiter in most SDK versions; unwrap it.
            run_id = getattr(getattr(started, "response", None), "run_id", None)
        if run_id is None:
            raise InfrastructureError(
                "The graph analytics job was triggered but the SDK returned no run id"
            )
        logger.info(
            "Started graph analytics run %s (job %s) for %s -> %s",
            run_id,
            job_id,
            source_table,
            output_table,
        )
        return int(run_id)

    def wait_for(
        self,
        run_id: int,
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> Dict[str, Any]:
        """Poll *run_id* until it finishes.

        Calls *on_progress* with ``(percent, message)`` on each state change so
        the caller can forward it to the task tracker. Returns a dict with
        ``success``, ``life_cycle_state``, ``result_state``, ``message`` and
        ``run_page_url``. Raises :class:`InfrastructureError` on timeout.
        """
        deadline = time.time() + self._timeout_s
        last_state = ""
        run_page_url = ""

        while True:
            try:
                run = self._w().jobs.get_run(run_id=run_id)
            except Exception as exc:  # noqa: BLE001
                raise InfrastructureError(
                    f"Lost track of graph analytics run {run_id}", detail=str(exc)
                ) from exc

            run_page_url = getattr(run, "run_page_url", "") or run_page_url
            state = getattr(run, "state", None)
            life_cycle = _enum_name(getattr(state, "life_cycle_state", None))
            result = _enum_name(getattr(state, "result_state", None))
            message = getattr(state, "state_message", "") or ""

            if life_cycle != last_state:
                last_state = life_cycle
                percent, label = self._progress_for(life_cycle)
                logger.info("Graph analytics run %s: %s %s", run_id, life_cycle, message)
                if on_progress:
                    on_progress(percent, label)

            if life_cycle in _TERMINAL_STATES:
                success = life_cycle == "TERMINATED" and result == "SUCCESS"
                return {
                    "success": success,
                    "life_cycle_state": life_cycle,
                    "result_state": result,
                    "message": message,
                    "run_page_url": run_page_url,
                    "run_id": run_id,
                }

            if time.time() >= deadline:
                raise InfrastructureError(
                    f"Graph analytics run {run_id} did not finish within "
                    f"{self._timeout_s}s (last state {life_cycle or 'UNKNOWN'}). "
                    f"It may still be running: {run_page_url}"
                )
            self._sleep(self._poll_interval_s)

    def run_and_wait(
        self,
        *,
        source_table: str,
        output_table: str,
        exclude_predicates: Optional[List[str]] = None,
        pagerank_iterations: int = 20,
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> Dict[str, Any]:
        """Convenience wrapper: submit, then follow the run to completion."""
        run_id = self.submit(
            source_table=source_table,
            output_table=output_table,
            exclude_predicates=exclude_predicates,
            pagerank_iterations=pagerank_iterations,
        )
        return self.wait_for(run_id, on_progress=on_progress)

    @staticmethod
    def _progress_for(life_cycle: str) -> tuple:
        """Map a run life-cycle state onto a task-tracker percentage."""
        return {
            "PENDING": (30, "Waiting for serverless compute"),
            "QUEUED": (30, "Job queued"),
            "RUNNING": (45, "Computing graph metrics on Databricks"),
            "TERMINATING": (75, "Finishing up"),
            "TERMINATED": (80, "Job finished"),
            "SKIPPED": (80, "Job skipped"),
            "INTERNAL_ERROR": (80, "Job failed"),
        }.get(life_cycle, (35, "Running graph analytics job"))
