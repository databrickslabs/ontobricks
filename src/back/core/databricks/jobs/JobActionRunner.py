"""Trigger and follow Databricks Jobs bound as ontology class actions.

A class action of ``kind=job`` is an existing workspace Job that declares an
``entity_id`` job parameter. OntoBricks only ``run_now``s it; the notebook or
wheel owns cluster, libraries, and Spark ``run_as``.

On a Lakehouse domain (``graph_backend=databricks``) in App mode, list and
trigger use the caller's forwarded token (OBO, fail-closed). Elsewhere the
control-plane client is used. Spark inside the Job is still the Job's
``run_as``.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional

from back.core.databricks.request_identity import get_request_identity
from back.core.errors import AuthorizationError, InfrastructureError, ValidationError
from back.core.logging import get_logger

logger = get_logger(__name__)

ENTITY_ID_PARAM = "entity_id"
JOB_KIND = "job"
JOB_ACTION_PREFIX = "job:"
TASK_TYPE = "entity_job_action"

DEFAULT_POLL_INTERVAL_S = 5.0
DEFAULT_TIMEOUT_S = 3600
_TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def job_action_full_name(job_id: int) -> str:
    """Return the allow-list identifier ``job:<id>``."""
    return f"{JOB_ACTION_PREFIX}{int(job_id)}"


def parse_job_action_id(value: Any) -> Optional[int]:
    """Return a positive job id from ``job:<id>`` or a raw int, else None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    text = str(value).strip()
    if text.startswith(JOB_ACTION_PREFIX):
        text = text[len(JOB_ACTION_PREFIX) :]
    if not text.isdigit():
        return None
    job_id = int(text)
    return job_id if job_id > 0 else None


def _enum_name(value: Any) -> str:
    if value is None:
        return ""
    name = getattr(value, "value", None) or getattr(value, "name", None) or str(value)
    return str(name).rsplit(".", 1)[-1].upper()


def _domain_info(domain: Any) -> Dict[str, Any]:
    info = getattr(domain, "info", None)
    if callable(info):
        info = info()
    return info if isinstance(info, dict) else {}


def is_lakehouse_domain(domain: Any) -> bool:
    """True when the domain graph backend is Lakehouse (Unity Catalog Delta)."""
    from back.core.graphdb.GraphDBFactory import normalize_graph_backend

    return normalize_graph_backend(_domain_info(domain).get("graph_backend")) == (
        "databricks"
    )


def class_job_entry(action: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a class Job action, or None if it is not a usable Job."""
    if not isinstance(action, dict):
        return None
    kind = str(action.get("kind") or "").strip().lower()
    is_job = kind == JOB_KIND or (
        action.get("job_id") is not None and kind != "uc_function"
    )
    if not is_job:
        return None
    if action.get("notebook_path"):
        logger.warning(
            "Skipping job action with notebook_path: %r",
            action.get("notebook_path"),
        )
        return None
    job_id = parse_job_action_id(action.get("job_id"))
    if job_id is None:
        logger.warning(
            "Skipping job action with invalid job_id: %r", action.get("job_id")
        )
        return None
    job_name = str(action.get("job_name") or action.get("function") or job_id)
    return {
        "kind": JOB_KIND,
        "job_id": job_id,
        "job_name": job_name,
        "fullName": job_action_full_name(job_id),
        "function": job_name,
        "description": str(action.get("description") or "").strip() or None,
        "returns_table": False,
    }


def _param_name(param: Any) -> str:
    name = getattr(param, "name", None)
    if name is None and isinstance(param, dict):
        name = param.get("name")
    return str(name or "")


def _seq_declares_entity_id(params: Any) -> bool:
    """True when a task argument list or map names ``entity_id``."""
    if isinstance(params, dict):
        return ENTITY_ID_PARAM in params
    if not isinstance(params, (list, tuple)):
        return False
    needle = "{{job.parameters.%s}}" % ENTITY_ID_PARAM
    for item in params:
        text = str(item or "")
        if text in (ENTITY_ID_PARAM, "--entity-id", f"--{ENTITY_ID_PARAM}"):
            return True
        if needle in text:
            return True
    return False


def job_declares_entity_id(job: Any) -> bool:
    """True when the Job exposes an ``entity_id`` parameter.

    ``jobs.list`` omits ``settings.parameters`` unless the caller then
    ``jobs.get``s. ``expand_tasks=True`` does return Spark/notebook task
    args, so a ``--entity-id`` / ``{{job.parameters.entity_id}}`` task
    argument is enough to list the Job in the class-action picker.
    """
    settings = getattr(job, "settings", None)
    if settings is None:
        return False
    for param in getattr(settings, "parameters", None) or []:
        if _param_name(param) == ENTITY_ID_PARAM:
            return True
    for task in getattr(settings, "tasks", None) or []:
        notebook = getattr(task, "notebook_task", None)
        if notebook is not None:
            base = getattr(notebook, "base_parameters", None)
            if base is None and isinstance(notebook, dict):
                base = notebook.get("base_parameters")
            if _seq_declares_entity_id(base or {}):
                return True
        spark = getattr(task, "spark_python_task", None)
        if spark is not None:
            params = getattr(spark, "parameters", None)
            if params is None and isinstance(spark, dict):
                params = spark.get("parameters")
            if _seq_declares_entity_id(params):
                return True
        if isinstance(task, dict):
            notebook = task.get("notebook_task") or {}
            if _seq_declares_entity_id(notebook.get("base_parameters") or {}):
                return True
            spark = task.get("spark_python_task") or {}
            if _seq_declares_entity_id(spark.get("parameters")):
                return True
    return False


def _notebook_exit(run: Any) -> Optional[str]:
    tasks = getattr(run, "tasks", None) or []
    for task in tasks:
        output = getattr(task, "notebook_output", None)
        if output is None:
            continue
        result = getattr(output, "result", None)
        if result:
            return str(result)
    return None


class JobActionRunner:
    """List bindable Jobs, ``run_now``, and poll ``get_run``."""

    def __init__(
        self,
        *,
        client_factory: Callable[[], Any],
        sleep: Optional[Callable[[float], None]] = None,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        timeout_s: int = DEFAULT_TIMEOUT_S,
    ) -> None:
        self._client_factory = client_factory
        self._sleep = sleep or time.sleep
        self._poll_interval_s = max(0.0, float(poll_interval_s))
        self._timeout_s = max(1, int(timeout_s))
        self._client: Optional[Any] = None

    @classmethod
    def from_domain(
        cls,
        domain: Any,
        settings: Any,
        *,
        user_token: Optional[str] = None,
        **kwargs: Any,
    ) -> "JobActionRunner":
        """Bind a runner; snapshot *user_token* for TaskManager workers."""
        token = (
            user_token
            if user_token is not None
            else get_request_identity().user_token
        )
        return cls(
            client_factory=lambda: cls.workspace_client(
                domain, settings, user_token=token
            ),
            **kwargs,
        )

    @staticmethod
    def workspace_client(
        domain: Any,
        settings: Any,
        *,
        user_token: str = "",
    ) -> Any:
        """Return a ``WorkspaceClient`` honouring Lakehouse OBO."""
        from databricks.sdk import WorkspaceClient

        from back.core.databricks import is_databricks_app
        from back.core.helpers import get_databricks_client

        if is_lakehouse_domain(domain) and is_databricks_app():
            if not (user_token or "").strip():
                raise AuthorizationError(
                    "User authorization is required for this data access. "
                    "The Databricks Apps proxy did not forward a user token."
                )
            host = (
                getattr(settings, "databricks_host", None)
                or ""
            ).rstrip("/")
            if not host:
                raise InfrastructureError("Databricks host is not configured")
            return WorkspaceClient(host=host, token=user_token.strip())

        client = get_databricks_client(domain, settings)
        if client is None:
            raise InfrastructureError("Databricks client is not configured")
        host = (getattr(client, "host", None) or "").rstrip("/")
        token = (getattr(client, "token", None) or "").strip()
        if token:
            return WorkspaceClient(host=host or None, token=token)
        return WorkspaceClient(host=host or None)

    def _w(self) -> Any:
        if self._client is None:
            try:
                self._client = self._client_factory()
            except (AuthorizationError, InfrastructureError, ValidationError):
                raise
            except Exception as exc:  # noqa: BLE001
                raise InfrastructureError(
                    "Cannot initialise the Databricks SDK to run the job",
                    detail=str(exc),
                ) from exc
        return self._client

    def list_entity_jobs(self) -> List[Dict[str, Any]]:
        """Jobs the caller can see that declare ``entity_id``."""
        try:
            listed = list(self._w().jobs.list(expand_tasks=True))
        except (AuthorizationError, InfrastructureError, ValidationError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise InfrastructureError(
                "Could not list Databricks jobs", detail=str(exc)
            ) from exc
        out: List[Dict[str, Any]] = []
        for job in listed:
            if not job_declares_entity_id(job):
                continue
            job_id = getattr(job, "job_id", None)
            if parse_job_action_id(job_id) is None:
                continue
            name = (
                getattr(getattr(job, "settings", None), "name", None) or ""
            ).strip()
            out.append(
                {
                    "job_id": int(job_id),
                    "name": name or f"job-{job_id}",
                    "fullName": job_action_full_name(int(job_id)),
                }
            )
        out.sort(key=lambda row: row["name"].lower())
        return out

    def run_now(
        self,
        job_id: int,
        *,
        entity_id: str,
        entity_uri: str = "",
        class_name: str = "",
        domain: str = "",
    ) -> int:
        """Start a run; return the Databricks run id."""
        params = {ENTITY_ID_PARAM: str(entity_id)}
        if entity_uri:
            params["entity_uri"] = str(entity_uri)
        if class_name:
            params["class_name"] = str(class_name)
        if domain:
            params["domain"] = str(domain)
        try:
            started = self._w().jobs.run_now(
                job_id=int(job_id), job_parameters=params
            )
        except (AuthorizationError, InfrastructureError, ValidationError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise InfrastructureError(
                f"Could not start Databricks job {job_id}", detail=str(exc)
            ) from exc
        run_id = getattr(started, "run_id", None)
        if run_id is None:
            run_id = getattr(getattr(started, "response", None), "run_id", None)
        if run_id is None:
            raise InfrastructureError(
                "The job was triggered but the SDK returned no run id"
            )
        logger.info("Started entity job run %s (job %s) for %s", run_id, job_id, entity_id)
        return int(run_id)

    def run_state(self, run_id: int) -> Dict[str, str]:
        """Read the run's current state once (no polling)."""
        try:
            run = self._w().jobs.get_run(run_id=run_id)
        except (AuthorizationError, InfrastructureError, ValidationError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise InfrastructureError(
                f"Could not read the state of job run {run_id}", detail=str(exc)
            ) from exc
        state = getattr(run, "state", None)
        return {
            "life_cycle_state": _enum_name(getattr(state, "life_cycle_state", None)),
            "result_state": _enum_name(getattr(state, "result_state", None)),
            "message": getattr(state, "state_message", "") or "",
            "run_page_url": getattr(run, "run_page_url", "") or "",
        }

    def wait_for(
        self,
        run_id: int,
        on_progress: Optional[Callable[[int, str], None]] = None,
    ) -> Dict[str, Any]:
        """Poll until the run terminals. Raises on timeout."""
        deadline = time.time() + self._timeout_s
        last_state = ""
        run_page_url = ""
        notebook_output: Optional[str] = None

        while True:
            try:
                run = self._w().jobs.get_run(run_id=run_id)
            except Exception as exc:  # noqa: BLE001
                raise InfrastructureError(
                    f"Lost track of job run {run_id}", detail=str(exc)
                ) from exc

            run_page_url = getattr(run, "run_page_url", "") or run_page_url
            notebook_output = _notebook_exit(run) or notebook_output
            state = getattr(run, "state", None)
            life_cycle = _enum_name(getattr(state, "life_cycle_state", None))
            result = _enum_name(getattr(state, "result_state", None))
            message = getattr(state, "state_message", "") or ""

            if life_cycle != last_state:
                last_state = life_cycle
                percent = {
                    "PENDING": 10,
                    "RUNNING": 50,
                    "TERMINATING": 80,
                    "TERMINATED": 100,
                    "SKIPPED": 100,
                    "INTERNAL_ERROR": 100,
                }.get(life_cycle, 20)
                if on_progress:
                    on_progress(percent, life_cycle or "UNKNOWN")

            if life_cycle in _TERMINAL_STATES:
                success = life_cycle == "TERMINATED" and result == "SUCCESS"
                return {
                    "success": success,
                    "life_cycle_state": life_cycle,
                    "result_state": result,
                    "message": message,
                    "run_page_url": run_page_url,
                    "run_id": run_id,
                    "output": notebook_output,
                }

            if time.time() >= deadline:
                raise InfrastructureError(
                    f"Job run {run_id} did not finish within {self._timeout_s}s "
                    f"(last state {life_cycle or 'UNKNOWN'}). "
                    f"It may still be running: {run_page_url}"
                )
            self._sleep(self._poll_interval_s)
