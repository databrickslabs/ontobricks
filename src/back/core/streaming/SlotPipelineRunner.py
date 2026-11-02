"""Create, update and run the per-version Lakeflow slot pipeline.

Wraps ``WorkspaceClient.pipelines`` (and ``workspace`` for the SQL file) behind
the narrow surface the incremental build needs, in the style of
:class:`~back.core.graph_analysis.LakeflowRunner.LakeflowRunner`: the client is
built lazily through an injectable factory, every SDK call runs under a
timeout guard, and every SDK failure becomes :class:`InfrastructureError` so
the build can fall back to a full rebuild.

One pipeline per (domain, version), named by :meth:`pipeline_name`. Its single
library is a ``.sql`` workspace file holding the compiled
:class:`~back.core.streaming.SlotPipelineSpec.SlotPipelineSpec`. The SHA-256
of that SQL is stored in the pipeline ``configuration`` under
:attr:`FINGERPRINT_KEY`, so a caller can tell an unchanged definition (an
incremental update is enough) from a changed one (flows were added or
removed, so ``_slots`` needs a full refresh).
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import re
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from back.core.errors import (
    ConflictError,
    InfrastructureError,
    OntoBricksError,
    OperationCancelledError,
    ValidationError,
)
from back.core.logging import get_logger
from back.core.streaming.SlotUpdateMetrics import SlotUpdateMetrics

logger = get_logger(__name__)

DEFAULT_API_TIMEOUT_S = 90
#: The Lakeflow API's answer when a (continuous) pipeline already runs an update.
_ACTIVE_UPDATE = re.compile(r"active update .* already exists", re.IGNORECASE)
#: DAB development mode deploys ``<name>`` as ``[dev <user>] <name>``.
_DEV_PREFIX_END = "] "
DEFAULT_STOP_TIMEOUT_S = 600

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_UNSAFE_NAME_CHARS = re.compile(r"[^a-z0-9]+")

_DONE = "COMPLETED"
_RUNNING = "RUNNING"
_FAILED = {"FAILED", "CANCELED"}


def _enum_name(value: Any) -> str:
    """Normalise an SDK enum / string / None into a bare upper-case name."""
    if value is None:
        return ""
    name = getattr(value, "value", None) or getattr(value, "name", None) or str(value)
    return str(name).rsplit(".", 1)[-1].upper()


def _slug(value: Any, fallback: str) -> str:
    return _UNSAFE_NAME_CHARS.sub("-", str(value or "").lower()).strip("-") or fallback


class SlotPipelineRunner:
    """Lakeflow SDP pipeline life cycle for one domain version's ``_slots`` table."""

    FINGERPRINT_KEY = "ontobricks.slot_sql_sha256"

    def __init__(
        self,
        *,
        client_factory: Callable[[], Any] | None = None,
        sleep: Callable[[float], None] | None = None,
        clock: Callable[[], float] | None = None,
        api_timeout_s: float = DEFAULT_API_TIMEOUT_S,
        stop_timeout_s: int = DEFAULT_STOP_TIMEOUT_S,
    ) -> None:
        self._client_factory = client_factory or self._default_factory
        self._sleep = sleep or time.sleep
        self._clock = clock or time.monotonic
        self._api_timeout_s = max(0.01, float(api_timeout_s))
        self._stop_timeout_s = max(1, int(stop_timeout_s))
        self._client: Any | None = None
        self._user_name: str | None = None
        self._found: dict[str, tuple[str, str]] = {}

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
                    "Cannot initialise the Databricks SDK to run the slot pipeline",
                    detail=str(exc),
                ) from exc
        return self._client

    def _call(self, label: str, fn: Callable[[Any], Any]) -> Any:
        """Run ``fn(client)`` under the API timeout; SDK errors become InfrastructureError.

        Same guard as ``SyncedTableManager._call_with_timeout``:
        ``shutdown(wait=False)`` so a hung call never blocks the build past the
        timeout (the idempotent call is left to finish in the background).
        """
        client = self._w()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(fn, client)
        try:
            return future.result(timeout=self._api_timeout_s)
        except concurrent.futures.TimeoutError as exc:
            raise InfrastructureError(
                f"Lakeflow pipelines API call timed out after "
                f"{self._api_timeout_s:g}s ({label})"
            ) from exc
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(
                f"Lakeflow pipelines API call failed ({label}): {exc}", detail=str(exc)
            ) from exc
        finally:
            executor.shutdown(wait=False)

    # ------------------------------------------------------------------
    # Naming and definition
    # ------------------------------------------------------------------

    @staticmethod
    def pipeline_name(domain_name: str, version: Any) -> str:
        """``ontobricks-slots-<domain>-v<version>``, lower-case and dash-separated."""
        return f"ontobricks-slots-{_slug(domain_name, 'domain')}-v{_slug(version, '')}"

    @staticmethod
    def fingerprint(sql_text: str) -> str:
        """SHA-256 (hex) of the pipeline SQL, as stored under :attr:`FINGERPRINT_KEY`."""
        return hashlib.sha256((sql_text or "").encode("utf-8")).hexdigest()

    def find_pipeline_id(self, name: str) -> str | None:
        """Id of the pipeline called *name* (see :meth:`find_pipeline`), or ``None``."""
        found = self.find_pipeline(name)
        return found[0] if found else None

    def find_pipeline(self, name: str) -> tuple[str, str] | None:
        """``(id, deployed name)`` of the pipeline *name*, or ``None`` (cached once found).

        A bundle in development mode deploys the pipeline as
        ``[dev <user>] <name>``, so that name matches too; an exact match wins.
        Two distinct pipelines that match the same way (two exact names, or two
        prefixed ones and no exact one) are a :class:`ConflictError`: either may
        own the ``_slots`` table, so neither is picked.
        """
        self._require_name(name)
        if name in self._found:
            return self._found[name]
        listed = self._call(
            f"list pipelines {name}",
            lambda w: list(w.pipelines.list_pipelines(filter=f"name LIKE '%{name}'")),
        )
        exact = [p for p in listed if getattr(p, "name", None) == name]
        prefixed = [
            p for p in listed if str(getattr(p, "name", "") or "").endswith(_DEV_PREFIX_END + name)
        ]
        matches = {getattr(p, "pipeline_id", None): p for p in exact or prefixed}
        matches.pop(None, None)
        if len(matches) > 1:
            names = ", ".join(sorted(f"{p.name!r} ({pid})" for pid, p in matches.items()))
            raise ConflictError(
                f"{len(matches)} pipelines match the slot pipeline {name}: {names}; delete the stray one"
            )
        if not matches:
            return None
        if exact and prefixed:
            logger.warning(
                "Slot pipeline %s exists both as itself and with a development prefix; using %s",
                name,
                next(iter(matches)),
            )
        pipeline_id, match = next(iter(matches.items()))
        self._found[name] = (pipeline_id, str(match.name))
        return self._found[name]

    def stored_fingerprint(self, pipeline_id: str) -> str:
        """The SQL fingerprint in the pipeline's configuration (``""`` when absent)."""
        response = self._call(
            f"get pipeline {pipeline_id}", lambda w: w.pipelines.get(pipeline_id=pipeline_id)
        )
        spec = getattr(response, "spec", None)
        configuration = getattr(spec, "configuration", None) or {}
        return str(configuration.get(self.FINGERPRINT_KEY, "") or "")

    def bundle_managed(self, pipeline_id: str) -> bool:
        """True when a bundle deployed the pipeline (``spec.deployment.kind == BUNDLE``)."""
        response = self._call(
            f"get pipeline {pipeline_id}", lambda w: w.pipelines.get(pipeline_id=pipeline_id)
        )
        deployment = getattr(getattr(response, "spec", None), "deployment", None)
        kind = getattr(deployment, "kind", None)
        return str(getattr(kind, "value", kind) or "").upper() == "BUNDLE"

    def ensure(
        self,
        *,
        name: str,
        catalog: str,
        schema: str,
        sql_text: str,
        continuous: bool,
        serverless: bool = True,
        channel: str = "PREVIEW",
        fingerprint: str | None = None,
    ) -> str:
        """Upload *sql_text* and create or update the pipeline; return its id.

        The SQL goes to ``/Workspace/Users/<me>/.ontobricks/pipelines/<name>.sql``
        (overwritten every time). The pipeline publishes into *catalog* /
        *schema*, is never in development mode, and runs *continuous* or
        triggered on the given *channel*.

        *fingerprint* is what goes under :attr:`FINGERPRINT_KEY`; default the
        hash of *sql_text*. A caller with a full refresh still pending passes
        the previous one (or ``""``) and stamps the new one once it completed,
        so a failed refresh is retried by the next build.
        """
        self._require_name(name)
        path = self._upload(name, sql_text)
        spec = {
            "name": name,
            "catalog": catalog,
            "schema": schema,
            "serverless": bool(serverless),
            "continuous": bool(continuous),
            "channel": channel,
            "development": False,
            "libraries": [self._file_library(path)],
            "configuration": {
                self.FINGERPRINT_KEY: (
                    self.fingerprint(sql_text) if fingerprint is None else fingerprint
                )
            },
        }
        found = self.find_pipeline(name)
        if found:
            existing, deployed_name = found
            # A bundle-deployed pipeline keeps the name the bundle gave it.
            spec["name"] = deployed_name
            self._call(
                f"update pipeline {name}", lambda w: w.pipelines.update(existing, **spec)
            )
            logger.info("Updated slot pipeline %s (%s)", deployed_name, existing)
            return existing
        created = self._call(f"create pipeline {name}", lambda w: w.pipelines.create(**spec))
        pipeline_id = getattr(created, "pipeline_id", None)
        if not pipeline_id:
            raise InfrastructureError(
                f"Slot pipeline {name} was created but the SDK returned no id"
            )
        self._found[name] = (pipeline_id, name)
        logger.info("Created slot pipeline %s (%s)", name, pipeline_id)
        return pipeline_id

    # ------------------------------------------------------------------
    # Updates
    # ------------------------------------------------------------------

    def start_update(self, pipeline_id: str, *, full_refresh: bool = False) -> str:
        """Start an update and return its id.

        A continuous pipeline starts its own update as soon as it is switched
        to continuous (verified live, 2026-11-02: the next ``start_update``
        answers ``An active update … already exists``). That active update is
        the one wanted, so it is returned instead of raising — unless a full
        refresh was asked for, which the running update is not.
        """
        try:
            response = self._call(
                f"start update on {pipeline_id}",
                lambda w: w.pipelines.start_update(
                    pipeline_id=pipeline_id, full_refresh=bool(full_refresh)
                ),
            )
        except InfrastructureError as exc:
            active = self._active_update(pipeline_id) if _ACTIVE_UPDATE.search(str(exc)) else ""
            if not active or full_refresh:
                raise
            logger.info(
                "Slot pipeline %s already runs update %s; adopting it instead of starting one",
                pipeline_id,
                active,
            )
            return active
        update_id = getattr(response, "update_id", None)
        if not update_id:
            raise InfrastructureError(
                f"Slot pipeline {pipeline_id} started but the SDK returned no update id"
            )
        logger.info(
            "Started slot pipeline update %s on %s (full_refresh=%s)",
            update_id,
            pipeline_id,
            bool(full_refresh),
        )
        return update_id

    def _active_update(self, pipeline_id: str) -> str:
        """The newest update id the pipeline reports, or ``""``."""
        try:
            info = self._call(f"get pipeline {pipeline_id}", lambda w: w.pipelines.get(pipeline_id=pipeline_id))
        except InfrastructureError:
            return ""
        latest = getattr(info, "latest_updates", None) or []
        return str(getattr(latest[0], "update_id", "") or "") if latest else ""

    def wait_for_update(
        self,
        pipeline_id: str,
        update_id: str,
        *,
        timeout_s: int,
        poll_s: float = 5.0,
        cancel_check: Callable[[], bool] | None = None,
        on_state: Callable[[str], None] | None = None,
        until_running: bool = False,
    ) -> str:
        """Poll the update until ``COMPLETED`` and return that state.

        With *until_running* (a continuous pipeline never completes) the first
        ``RUNNING`` state is enough. ``FAILED`` / ``CANCELED`` and the
        *timeout_s* deadline raise :class:`InfrastructureError`; *cancel_check*
        returning true raises :class:`OperationCancelledError`. *on_state* is
        called on every state change.
        """
        deadline = self._clock() + max(1, int(timeout_s))
        last = ""
        while True:
            self._raise_if_cancelled(cancel_check, update_id)
            response = self._call(
                f"get update {update_id}",
                lambda w: w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id),
            )
            state = _enum_name(getattr(getattr(response, "update", None), "state", None))
            if state != last:
                last = state
                logger.info("Slot pipeline %s update %s: %s", pipeline_id, update_id, state)
                if on_state:
                    on_state(state)
            if state == _DONE or (until_running and state == _RUNNING):
                return state
            if state in _FAILED:
                raise InfrastructureError(
                    f"Slot pipeline update {update_id} on {pipeline_id} ended with {state}"
                )
            if self._clock() >= deadline:
                raise InfrastructureError(
                    f"Slot pipeline update {update_id} on {pipeline_id} did not finish "
                    f"within {int(timeout_s)}s (last state {state or 'UNKNOWN'})"
                )
            self._sleep(poll_s)

    def stop(self, pipeline_id: str) -> None:
        """Stop the pipeline and wait (bounded) until it is idle."""
        waiter = self._call(
            f"stop pipeline {pipeline_id}", lambda w: w.pipelines.stop(pipeline_id=pipeline_id)
        )
        result = getattr(waiter, "result", None)
        if not callable(result):
            return
        try:
            result(timeout=timedelta(seconds=self._stop_timeout_s))
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(
                f"Slot pipeline {pipeline_id} did not stop", detail=str(exc)
            ) from exc
        logger.info("Stopped slot pipeline %s", pipeline_id)

    def get_state(self, pipeline_id: str) -> str:
        """Pipeline state name (``IDLE``, ``RUNNING``, ...; ``""`` when unknown)."""
        response = self._call(
            f"get pipeline {pipeline_id}", lambda w: w.pipelines.get(pipeline_id=pipeline_id)
        )
        return _enum_name(getattr(response, "state", None))

    def update_metrics(self, pipeline_id: str, update_id: str) -> dict | None:
        """``{rows_output, rows_upserted, rows_deleted}`` the update wrote (:class:`SlotUpdateMetrics`).

        ``None`` when no metric was found. Best effort: any API error is
        ``None`` too (logged at warning), never raised.
        """
        try:
            return self._call(
                f"list events of {pipeline_id}",
                lambda w: SlotUpdateMetrics.read(w, pipeline_id, update_id),
            )
        except OntoBricksError as exc:
            logger.warning(
                "Slot pipeline %s update %s: no row metrics (%s)", pipeline_id, update_id, exc
            )
            return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _require_name(name: str) -> None:
        if not isinstance(name, str) or _SAFE_NAME.fullmatch(name) is None:
            raise ValidationError(f"Invalid slot pipeline name: {name!r}")

    @staticmethod
    def _raise_if_cancelled(cancel_check: Callable[[], bool] | None, update_id: str) -> None:
        if cancel_check is not None and cancel_check():
            raise OperationCancelledError(
                f"Cancelled while waiting for slot pipeline update {update_id}"
            )

    def _upload(self, name: str, sql_text: str) -> str:
        """Write the pipeline SQL as a workspace file and return its path."""
        if self._user_name is None:
            me = self._call("current user", lambda w: w.current_user.me())
            self._user_name = str(getattr(me, "user_name", "") or "")
            if not self._user_name:
                raise InfrastructureError("Could not resolve the current Databricks user")
        folder = f"/Workspace/Users/{self._user_name}/.ontobricks/pipelines"
        path = f"{folder}/{name}.sql"
        content = (sql_text or "").encode("utf-8")
        self._call(f"mkdirs {folder}", lambda w: w.workspace.mkdirs(folder))
        self._call(
            f"upload {path}",
            lambda w: w.workspace.upload(
                path, content, format=self._file_format(), overwrite=True
            ),
        )
        return path

    @staticmethod
    def _file_format() -> Any:
        """``ImportFormat.AUTO``: a ``.sql`` without a notebook header stays a file."""
        try:
            from databricks.sdk.service.workspace import ImportFormat
        except ImportError:  # pragma: no cover — every supported SDK has it
            return None
        return ImportFormat.AUTO

    @staticmethod
    def _file_library(path: str) -> Any:
        """A single-file pipeline library (SDK model when available, else a dict)."""
        try:
            from databricks.sdk.service.pipelines import FileLibrary, PipelineLibrary
        except ImportError:  # pragma: no cover — every supported SDK has them
            return {"file": {"path": path}}
        return PipelineLibrary(file=FileLibrary(path=path))
