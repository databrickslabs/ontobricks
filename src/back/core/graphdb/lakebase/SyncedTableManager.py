"""Manage Lakebase synced tables (Lakeflow snapshot pipelines).

Wraps the ``WorkspaceClient.database`` SDK behind a narrow surface so the
build pipeline can drive bulk R2RML data movement entirely on the data
plane (Databricks Delta -> Lakebase Postgres) without iterating triples
in the FastAPI process.

The full architecture is described in
:doc:`/guides/graphdb-integration` (Lakebase managed-synced section).

SDK compatibility
-----------------
``databricks.sdk.service.database`` is a relatively new module that may be
absent from the SDK version bundled into a Databricks Apps container.  When
the module is missing the manager falls back to:

* **REST calls** — direct ``requests`` calls to
  ``/api/2.0/database/synced_tables/…`` authenticated with
  ``DATABRICKS_TOKEN`` / ``DATABRICKS_HOST`` (auto-injected by Apps).
* **Plain-dict payloads** — synced-table specs are built as ``dict``
  objects rather than SDK dataclasses.
* **``_DictNamespace`` responses** — API responses are wrapped in a
  simple attribute-access shim so the same ``getattr``-based helpers
  (``_extract_state``, ``_extract_pipeline_id``) work for both paths.
"""

from __future__ import annotations

import concurrent.futures
import time
from datetime import timedelta
from typing import Any, Callable, List, Optional

from back.core.errors import (
    InfrastructureError,
    OperationCancelledError,
    ValidationError,
)
from back.core.logging import get_logger
from shared.config.constants import HTTP_USER_AGENT


def _raise_if_cancelled(
    cancel_check: Optional[Callable[[], bool]], context: str
) -> None:
    """Raise ``OperationCancelledError`` if ``cancel_check`` reports True.

    Centralised so every wait loop has the same cancellation semantics
    and the same human-readable message format.
    """
    if cancel_check is None:
        return
    try:
        if cancel_check():
            raise OperationCancelledError(
                f"Cancelled while {context}"
            )
    except OperationCancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.debug("cancel_check raised %s — treating as not-cancelled", exc)

logger = get_logger(__name__)

DEFAULT_TIMEOUT_S = 600
_POLL_INTERVAL_S = 5


def _env_int(name: str, default: int) -> int:
    """Read a positive int env var with a fallback default.

    Used so deployments can override post-create / missing-bail timeouts
    without a code change when UC propagation is unusually slow.
    """
    import os

    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
        return v if v > 0 else default
    except ValueError:
        return default


# How long to wait for a just-created synced table to become visible via GET
# before considering registration a failure (UC propagation lag).
# UC metastore propagation can take 2-3 minutes on first creation when the
# schema is also being created on demand; default to 240s and allow override
# via ONTOBRICKS_SYNC_POST_CREATE_S.
_POST_CREATE_VISIBILITY_S = _env_int("ONTOBRICKS_SYNC_POST_CREATE_S", 240)
# How long MISSING state is tolerated in wait_for_completion before we give up
# with a clear error rather than looping until the global timeout.
# Allow override via ONTOBRICKS_SYNC_MISSING_BAIL_S.
_MISSING_BAIL_S = _env_int("ONTOBRICKS_SYNC_MISSING_BAIL_S", 240)

# State names are normalised by ``_extract_state`` — the ``SYNCED_TABLE_``
# SDK prefix is stripped, and the SDK typo ``SYNCED_TABLED_OFFLINE`` becomes
# ``TABLED_OFFLINE`` (included in _TERMINAL_FAIL below).
_TERMINAL_OK = {"ONLINE", "ONLINE_NO_PENDING_UPDATE"}
_TERMINAL_FAIL = {"FAILED", "OFFLINE_FAILED", "TABLED_OFFLINE"}
_IN_PROGRESS = {
    "PROVISIONING",
    "PROVISIONING_INITIAL_SNAPSHOT",
    "PROVISIONING_PIPELINE_RESOURCES",
    "ONLINE_TRIGGERED_UPDATE",
    "ONLINE_PIPELINE_FAILED",
    "ONLINE_UPDATING_PIPELINE_RESOURCES",
    "ONLINE_CONTINUOUS_UPDATE",
    "OFFLINE",
}

# ---------------------------------------------------------------------------
# SDK availability probe
# ---------------------------------------------------------------------------

try:
    from databricks.sdk.service import database as _db_svc  # noqa: F401

    _SDK_DATABASE_AVAILABLE = True
except ImportError:
    _db_svc = None  # type: ignore[assignment]
    _SDK_DATABASE_AVAILABLE = False
    logger.debug(
        "databricks.sdk.service.database not available — "
        "SyncedTableManager will use direct REST calls"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _DictNamespace:
    """Recursively wrap a JSON dict so nested keys are accessible as attributes.

    Used to normalise SDK-object vs. plain-dict responses so the same
    ``getattr``-based helpers work regardless of which code path ran.
    """

    def __init__(self, data: Any) -> None:
        self._data = data if isinstance(data, dict) else {}

    def __getattr__(self, name: str) -> Any:
        val = self._data.get(name)
        if isinstance(val, dict):
            return _DictNamespace(val)
        if isinstance(val, list):
            return [_DictNamespace(v) if isinstance(v, dict) else v for v in val]
        return val

    def __repr__(self) -> str:  # pragma: no cover
        return f"_DictNamespace({self._data!r})"


def _to_dict(obj: Any) -> dict:
    """Convert an SDK dataclass *or* a plain dict to ``dict``."""
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "as_dict"):
        return obj.as_dict()
    return {}


# ---------------------------------------------------------------------------
# REST-only database API (no databricks.sdk.service.database required)
# ---------------------------------------------------------------------------


# Timeout (seconds) for a single synced-table API call (GET or POST).
# The SDK's http client has no built-in call-level timeout; we enforce one
# by running each call in a thread and waiting with a deadline.
# Override via ONTOBRICKS_SYNC_API_TIMEOUT_S (default 90s).
_API_CALL_TIMEOUT_S = _env_int("ONTOBRICKS_SYNC_API_TIMEOUT_S", 90)


def _call_with_timeout(fn: Callable, timeout_s: int, label: str) -> Any:
    """Run *fn()* in a thread; raise InfrastructureError on timeout.

    IMPORTANT: Uses ``shutdown(wait=False)`` so a hung API call does not
    block the caller indefinitely after the timeout fires.  The background
    thread is left to finish on its own (or be reaped when the process
    exits) — acceptable for idempotent REST calls.
    """
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = ex.submit(fn)
    try:
        return future.result(timeout=timeout_s)
    except concurrent.futures.TimeoutError:
        raise InfrastructureError(
            f"Synced-table API call timed out after {timeout_s}s ({label}). "
            f"The Lakebase synced-table API may be unavailable or rate-limited "
            f"in this workspace. Override timeout with "
            f"ONTOBRICKS_SYNC_API_TIMEOUT_S env var."
        )
    finally:
        # shutdown(wait=False): don't block waiting for the background thread.
        # If the API call is hung, we propagate the error immediately.
        ex.shutdown(wait=False)


class _RestDatabaseAPI:
    """Direct REST implementation of the three synced-table endpoints.

    Uses the SDK ``WorkspaceClient.api_client.do()`` for auth so the same
    credential flow works in local dev (PAT), Databricks Apps (OIDC M2M),
    and CI (env-var token).  Falls back to a raw ``requests`` call with
    ``DATABRICKS_TOKEN`` only when the SDK's ``api_client`` is unavailable.

    Every outbound call is wrapped in ``_call_with_timeout`` so a hung API
    connection fails fast rather than blocking the build thread indefinitely.
    """

    def __init__(self, workspace_client: Any) -> None:
        self._wc = workspace_client

    def _do(self, method: str, path: str, **kwargs: Any) -> Any:
        """Route through SDK's ApiClient when possible; raw requests otherwise."""
        api = getattr(self._wc, "api_client", None)
        if api is not None and hasattr(api, "do"):
            # SDK ApiClient handles auth (PAT / OIDC / Apps credential injection).
            return api.do(method, path, **kwargs)

        # Last-resort fallback: raw requests with DATABRICKS_TOKEN.
        import os

        import requests

        host = (os.environ.get("DATABRICKS_HOST") or "").strip().rstrip("/")
        if host and not host.startswith(("http://", "https://")):
            host = "https://" + host
        token = os.environ.get("DATABRICKS_TOKEN") or ""
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": HTTP_USER_AGENT,
        }
        body = kwargs.get("body")
        resp = requests.request(
            method,
            f"{host}{path}",
            json=body,
            params=kwargs.get("query"),
            headers=headers,
            timeout=_API_CALL_TIMEOUT_S,
        )
        if resp.status_code == 404:
            raise ValueError(f"404 Not Found: {path}")
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    def get_synced_database_table(self, *, name: str) -> Any:
        def _call():
            return self._do("GET", f"/api/2.0/database/synced_tables/{name}")

        try:
            data = _call_with_timeout(_call, _API_CALL_TIMEOUT_S, f"GET synced_tables/{name}")
        except InfrastructureError:
            raise
        except Exception as exc:
            if "404" in str(exc) or "not found" in str(exc).lower():
                raise ValueError(f"Synced table {name!r} not found") from exc
            raise
        if _SDK_DATABASE_AVAILABLE and _db_svc is not None:
            return _db_svc.SyncedDatabaseTable.from_dict(data)
        return _DictNamespace(data)

    def create_synced_database_table(self, *, synced_table: Any) -> Any:
        body = _to_dict(synced_table)
        logger.info(
            "Creating synced table via API (timeout=%ss): %s",
            _API_CALL_TIMEOUT_S,
            body.get("name", "?"),
        )
        data = _call_with_timeout(
            lambda: self._do("POST", "/api/2.0/database/synced_tables", body=body),
            _API_CALL_TIMEOUT_S,
            f"POST synced_tables/{body.get('name', '?')}",
        )
        if _SDK_DATABASE_AVAILABLE and _db_svc is not None:
            return _db_svc.SyncedDatabaseTable.from_dict(data)
        return _DictNamespace(data)

    def delete_synced_database_table(self, *, name: str, purge_data: bool = True) -> None:
        try:
            _call_with_timeout(
                lambda: self._do(
                    "DELETE",
                    f"/api/2.0/database/synced_tables/{name}",
                    query={"purge_data": "true" if purge_data else "false"},
                ),
                _API_CALL_TIMEOUT_S,
                f"DELETE synced_tables/{name}",
            )
        except InfrastructureError:
            raise
        except Exception as exc:
            if "404" in str(exc) or "not found" in str(exc).lower():
                return
            raise


class _CompatWorkspaceClient:
    """Thin wrapper that injects a REST-based ``.database`` when the SDK lacks one."""

    def __init__(self, workspace_client: Any) -> None:
        self._wc = workspace_client
        self.database = _RestDatabaseAPI(workspace_client)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._wc, name)


# ---------------------------------------------------------------------------
# SyncedTableManager
# ---------------------------------------------------------------------------


class SyncedTableManager:
    """Idempotent wrapper for Lakebase synced-table CRUD + refresh.

    The manager is bound to a single Lakebase project + logical database.
    The synced table's UC fully-qualified name (``<catalog>.<schema>.<table>``)
    must include a *schema* segment that matches the Postgres-side schema
    where the companion table will live, so both end up in the same
    ``search_path``.
    """

    def __init__(
        self,
        *,
        project_name: str = "",
        branch_name: str = "",
        logical_db: str = "",
        client_factory: Optional[Any] = None,
        sleep: Optional[Any] = None,
    ) -> None:
        self._project = project_name
        self._branch = branch_name
        self._logical_db = logical_db
        self._client_factory = client_factory or self._default_factory
        self._client: Optional[Any] = None
        self._sleep = sleep or time.sleep

    @staticmethod
    def _default_factory() -> Any:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        # Always wrap in _CompatWorkspaceClient so that every call to
        # .database goes through _RestDatabaseAPI → _call_with_timeout.
        # The SDK's native WorkspaceClient.database (when present) has no
        # per-call timeout and hangs indefinitely on slow/unavailable APIs.
        if hasattr(w, "database"):
            logger.debug(
                "WorkspaceClient.database available — wrapping with "
                "_CompatWorkspaceClient for per-call timeout enforcement "
                "(timeout=%ss, override via ONTOBRICKS_SYNC_API_TIMEOUT_S)",
                _API_CALL_TIMEOUT_S,
            )
        else:
            logger.debug(
                "WorkspaceClient.database not available — using REST fallback "
                "for synced-table API (timeout=%ss)",
                _API_CALL_TIMEOUT_S,
            )
        return _CompatWorkspaceClient(w)

    def _w(self) -> Any:
        if self._client is None:
            self._client = self._client_factory()
        return self._client

    # -- CRUD -------------------------------------------------------------

    def get(self, name: str) -> Optional[Any]:
        """Return the ``SyncedDatabaseTable`` (or dict-namespace) or *None*."""
        try:
            return self._w().database.get_synced_database_table(name=name)
        except Exception as exc:  # noqa: BLE001
            if self._is_not_found(exc):
                return None
            raise

    def exists(self, name: str) -> bool:
        return self.get(name) is not None

    # How long to wait for first visibility before attempting a delete+recreate when
    # the table was reported "already exists" but GET returns None.  This handles the
    # common case where the table was created by a *different* principal (e.g. the
    # developer running the app locally) and the app service principal lacks SELECT on it.
    _STALE_OWNERSHIP_PROBE_S: int = 30

    # Fallback UC-name suffixes tried when the Lakebase control-plane has a ghost
    # reservation for the primary name (POST → "Destination table already exists in
    # schema X" but GET → 404).  The control plane tracks reservations internally and
    # they can outlive the UC registration when the latter is deleted without going
    # through the synced-tables DELETE API (e.g. DROP TABLE in UC or domain reset).
    # Suffixes are appended to the last component of the UC FQN:
    #   primary:  catalog.schema.table_sync
    #   fallback: catalog.schema.table_sync_b, ..._c, ..._d
    _GHOST_FALLBACK_SUFFIXES: tuple = ("_b", "_c", "_d")

    def ensure(
        self,
        name: str,
        *,
        source_table_full_name: str,
        primary_key_columns: List[str],
        sync_mode: str = "snapshot",
        create_database_objects_if_missing: bool = True,
    ) -> Any:
        """Create the synced table if missing; otherwise return the existing one.

        Returns the synced-table object.  When the primary name has a ghost
        control-plane reservation (see ``_GHOST_FALLBACK_SUFFIXES``), the
        returned object's ``.name`` attribute may differ from *name* — callers
        must use it downstream (union-view DDL, trigger, wait).

        After a successful CREATE, polls until the table is visible via GET to
        handle the UC registration propagation delay (~seconds).  Raises
        ``InfrastructureError`` if visibility is not confirmed within
        ``_POST_CREATE_VISIBILITY_S`` seconds.

        **Stale-ownership recovery**: when POST returns 409 "already exists"
        but GET keeps returning 404 for ``_STALE_OWNERSHIP_PROBE_S`` seconds,
        the table was almost certainly created by a different principal (the
        deploying user running the app locally).  In that case we delete the
        stale table and re-create it so the app service principal becomes the
        owner and the subsequent GET succeeds.

        **Ghost control-plane recovery**: when POST returns the Lakebase
        "Destination table already exists in schema" error but GET returns 404,
        the Lakebase control-plane has a stale reservation from a previous build
        that was cleaned up at the UC level without going through the
        synced-tables DELETE API.  The reservation cannot be cleared externally;
        instead, ``ensure`` retries with ``_b``, ``_c``, ``_d`` name suffixes.
        """
        existing = self.get(name)
        if existing is not None:
            logger.info("Synced table %s already exists — reusing", name)
            return existing

        # ── Candidate names: primary first, then ghost-state fallbacks ────────
        candidates = [name] + [
            f"{name}{sfx}" for sfx in self._GHOST_FALLBACK_SUFFIXES
        ]
        actual_name = name  # updated when a fallback is used

        for candidate in candidates:
            synced = self._build_synced_table_payload(
                name=candidate,
                source_table_full_name=source_table_full_name,
                primary_key_columns=primary_key_columns,
                sync_mode=sync_mode,
                create_database_objects_if_missing=create_database_objects_if_missing,
            )
            already_existed = False

            # Check if this fallback candidate already exists.
            if candidate != name:
                existing_fallback = self.get(candidate)
                if existing_fallback is not None:
                    logger.info(
                        "Synced table fallback %s already exists — reusing "
                        "(primary %s has a ghost control-plane reservation).",
                        candidate,
                        name,
                    )
                    return existing_fallback

            try:
                self._w().database.create_synced_database_table(  # noqa: F841
                    synced_table=synced
                )
                actual_name = candidate
                if candidate != name:
                    logger.warning(
                        "Ghost control-plane reservation detected for %s — "
                        "registered synced table under fallback name %s.  "
                        "The primary name is permanently blocked until the "
                        "Lakebase control-plane reservation is cleared.  "
                        "To unblock: contact Databricks support or delete and "
                        "recreate the Lakebase branch.",
                        name,
                        candidate,
                    )
                else:
                    logger.info(
                        "Created synced table %s (source=%s, mode=%s)",
                        candidate,
                        source_table_full_name,
                        sync_mode,
                    )
                break  # success — exit candidate loop

            except Exception as exc:  # noqa: BLE001
                if self._is_ghost_control_plane_state(exc):
                    # Try to release the reservation via DELETE — succeeds when
                    # this principal owns it; silently ignored otherwise.
                    try:
                        self._w().database.delete_synced_database_table(
                            name=candidate, purge_data=True
                        )
                        logger.info(
                            "Ghost-state DELETE for %s succeeded — retrying CREATE",
                            candidate,
                        )
                        try:
                            self._w().database.create_synced_database_table(
                                synced_table=synced
                            )
                            actual_name = candidate
                            logger.info(
                                "Re-created %s after ghost-state DELETE", candidate
                            )
                            break  # success after delete+recreate
                        except Exception:  # noqa: BLE001
                            pass  # still blocked — fall through to next candidate
                    except Exception:  # noqa: BLE001
                        pass  # DELETE also failed — fall through to next candidate

                    if candidate == candidates[-1]:
                        tried = ", ".join(candidates)
                        raise InfrastructureError(
                            f"Lakebase control-plane ghost reservation blocks synced "
                            f"table creation.  Tried all candidates: {tried}.  "
                            f"Root cause: a previous build registered the Postgres "
                            f"destination (schema + table) at the control-plane level "
                            f"but the UC registration was later deleted without going "
                            f"through the synced-tables DELETE API (e.g. via UC DROP "
                            f"TABLE or domain reset).  The reservation persists "
                            f"independently of UC and Postgres state.\n"
                            f"  Fix option 1 — Databricks support: ask them to clear "
                            f"the control-plane reservation for {name!r}.\n"
                            f"  Fix option 2 — recreate branch: delete and recreate "
                            f"the Lakebase branch (all synced tables will need rebuild)."
                        )
                    logger.warning(
                        "Ghost control-plane reservation for %s — trying next "
                        "candidate (exc: %s)",
                        candidate,
                        exc,
                    )
                    continue  # next candidate

                elif self._is_already_exists(exc):
                    logger.warning(
                        "Synced table %s: POST returned 409 'already exists' but "
                        "the initial GET returned None — the table exists but this "
                        "service principal cannot read it.  Will wait %ss; if still "
                        "invisible, will attempt delete + re-create to take ownership.",
                        candidate,
                        self._STALE_OWNERSHIP_PROBE_S,
                    )
                    actual_name = candidate
                    already_existed = True
                    break  # exit candidate loop — use stale-ownership recovery below

                else:
                    raise InfrastructureError(
                        f"Failed to create synced table {candidate}: {exc}"
                    ) from exc

        # ── Early probe when table "already existed" but is not readable ──────
        # Quick poll for _STALE_OWNERSHIP_PROBE_S seconds.  If still invisible,
        # the table is owned by another principal → delete and recreate.
        if already_existed:
            probe_deadline = time.time() + self._STALE_OWNERSHIP_PROBE_S
            probe_visible: Optional[Any] = None
            while time.time() < probe_deadline:
                probe_visible = self.get(actual_name)
                if probe_visible is not None:
                    break
                self._sleep(min(_POLL_INTERVAL_S, probe_deadline - time.time()))

            if probe_visible is not None:
                logger.info(
                    "Synced table %s became visible during ownership probe",
                    actual_name,
                )
                return probe_visible

            # Still invisible → ownership conflict → attempt delete + recreate.
            logger.warning(
                "Synced table %s still invisible after %ss probe — "
                "deleting stale table and re-creating under SP ownership.",
                actual_name,
                self._STALE_OWNERSHIP_PROBE_S,
            )
            synced_for_recreate = self._build_synced_table_payload(
                name=actual_name,
                source_table_full_name=source_table_full_name,
                primary_key_columns=primary_key_columns,
                sync_mode=sync_mode,
                create_database_objects_if_missing=create_database_objects_if_missing,
            )
            try:
                self._w().database.delete_synced_database_table(name=actual_name)
                logger.info("Deleted stale synced table %s", actual_name)
            except Exception as del_exc:  # noqa: BLE001
                logger.warning(
                    "Could not delete stale synced table %s: %s — "
                    "proceeding to visibility poll anyway (delete may require "
                    "CAN_MANAGE on the Lakebase project; see docs).",
                    actual_name,
                    del_exc,
                )
            else:
                try:
                    self._w().database.create_synced_database_table(
                        synced_table=synced_for_recreate
                    )
                    logger.info(
                        "Re-created synced table %s under SP ownership", actual_name
                    )
                except Exception as rc_exc:  # noqa: BLE001
                    if not self._is_already_exists(rc_exc):
                        raise InfrastructureError(
                            f"Failed to re-create synced table {actual_name} after "
                            f"deleting stale copy: {rc_exc}"
                        ) from rc_exc

        # ── Visibility poll (UC propagation delay after CREATE) ───────────────
        deadline = time.time() + _POST_CREATE_VISIBILITY_S
        attempt = 0
        while True:
            attempt += 1
            visible = self.get(actual_name)
            if visible is not None:
                logger.info(
                    "Synced table %s confirmed visible after %d GET attempt(s)",
                    actual_name,
                    attempt,
                )
                return visible
            remaining = deadline - time.time()
            if remaining <= 0:
                raise InfrastructureError(
                    f"Synced table {actual_name!r} was created (or already existed) "
                    f"but is not visible via GET after {_POST_CREATE_VISIBILITY_S}s. "
                    f"Root causes in order of likelihood:\n"
                    f"  1. The app SP lacks CAN_USE on the Lakebase Autoscaling "
                    f"project — the /api/2.0/permissions/database-instances grant "
                    f"used by bootstrap-lakebase-perms.sh may not apply to "
                    f"Autoscaling projects.  Re-run the bootstrap script and verify "
                    f"the grant appears in the project's ACL.\n"
                    f"  2. The SP lacks SELECT on the Unity Catalog table — run: "
                    f"  databricks grants update TABLE {actual_name} --json "
                    f"'{{\"changes\":[{{\"principal\":\"<sp-client-id>\","
                    f"\"add\":[\"SELECT\"]}}]}}'\n"
                    f"  3. The Lakebase synced-table API is temporarily unavailable."
                )
            logger.info(
                "Synced table %s not yet visible (attempt %d) — "
                "retrying in %.1fs (%.0fs remaining)",
                actual_name,
                attempt,
                min(_POLL_INTERVAL_S, remaining),
                remaining,
            )
            self._sleep(min(_POLL_INTERVAL_S, remaining))

    def trigger_refresh(self, name: str) -> str:
        """Kick off a Lakeflow update for this synced table; return the ``update_id``."""
        synced = self.get(name)
        if synced is None:
            raise ValidationError(f"Synced table {name} does not exist")
        pipeline_id = self._extract_pipeline_id(synced)
        if not pipeline_id:
            raise InfrastructureError(
                f"Synced table {name} has no pipeline_id yet — initial provisioning "
                f"is still in progress"
            )
        try:
            from databricks.sdk.service.pipelines import StartUpdateCause

            update = self._w().pipelines.start_update(
                pipeline_id=pipeline_id,
                full_refresh=True,
                cause=StartUpdateCause.API_CALL,
            )
            return getattr(update, "update_id", "") or ""
        except Exception as exc:  # noqa: BLE001
            if self._is_active_pipeline_update_conflict(exc):
                logger.info(
                    "Skipping start_update for %s — pipeline %s already has an "
                    "active update (will poll sync state instead)",
                    name,
                    pipeline_id,
                )
                return ""
            raise

    def wait_for_completion(
        self,
        name: str,
        *,
        timeout_s: int = DEFAULT_TIMEOUT_S,
        pipeline_update_id: Optional[str] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
        on_state_change: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Block until the sync reaches a terminal state; raise on failure / timeout.

        ``cancel_check`` is invoked between polls. When it returns ``True``
        the wait stops promptly with :class:`OperationCancelledError` so
        callers can flip the surrounding task to ``cancelled`` instead of
        observing a stale ``FAILED`` state from a pipeline that the user
        already abandoned.

        ``on_state_change`` is called with the new state string whenever the
        pipeline state transitions.  Callers can use this to forward progress
        updates to the task manager UI without coupling this module to it.
        """
        deadline = time.time() + max(1, int(timeout_s))
        last_state = ""
        idle_pipeline_wait_done = False
        # Track when we first saw MISSING/empty so we can bail out fast.
        missing_since: Optional[float] = None

        uid = (pipeline_update_id or "").strip()
        if uid:
            synced = self.get(name)
            pid = self._extract_pipeline_id(synced) if synced else ""
            if not pid:
                raise InfrastructureError(
                    f"Synced table {name} has no pipeline_id — cannot track update {uid}"
                )
            remaining = max(0.0, deadline - time.time())
            if remaining > 1:
                logger.info(
                    "Waiting for Lakeflow pipeline update %s (pipeline %s · synced %s)",
                    uid,
                    pid,
                    name,
                )
                self._wait_for_pipeline_update(
                    pid, uid, remaining, cancel_check=cancel_check
                )
            idle_pipeline_wait_done = True

        while True:
            _raise_if_cancelled(
                cancel_check, f"waiting for synced table {name}"
            )
            now = time.time()
            if now >= deadline:
                raise InfrastructureError(
                    f"Timed out after {timeout_s}s waiting for synced table {name} "
                    f"(last state={last_state or '?'})"
                )

            synced = self.get(name)
            state = self._extract_state(synced) if synced else "MISSING"

            # Track how long the table has been absent / state-less.
            if not state or state == "MISSING":
                if missing_since is None:
                    missing_since = time.time()
                    logger.warning(
                        "Synced table %s is %s — "
                        "will bail out if still missing after %ds",
                        name,
                        state or "state-unknown",
                        _MISSING_BAIL_S,
                    )
                elif time.time() - missing_since >= _MISSING_BAIL_S:
                    raise InfrastructureError(
                        f"Synced table {name!r} is still not visible "
                        f"after {_MISSING_BAIL_S}s (state={state!r}). "
                        f"The service principal may lack permission to create or "
                        f"read the synced table, or the Lakebase API is "
                        f"temporarily unavailable. "
                        f"Check the Databricks workspace audit logs for the "
                        f"synced-table API call."
                    )
            else:
                missing_since = None  # reset if we got a real state

            if state and state != last_state:
                logger.info("Synced table %s state=%s", name, state)
                last_state = state
                if on_state_change is not None:
                    try:
                        on_state_change(state)
                    except Exception:  # noqa: BLE001
                        pass

            if state in _TERMINAL_FAIL:
                raise InfrastructureError(
                    f"Synced table {name} reached terminal failure state: {state}"
                )

            if not idle_pipeline_wait_done:
                pid = self._extract_pipeline_id(synced) if synced else ""
                if pid:
                    remaining = deadline - time.time()
                    if remaining > 1:
                        logger.info(
                            "Waiting for Lakeflow pipeline %s to finish "
                            "(Databricks · synced table %s)",
                            pid,
                            name,
                        )
                        # wait_get_pipeline_idle blocks for ``remaining`` seconds
                        # with no cancellation hook. Chunk it so we can observe
                        # the cancel signal between SDK calls.
                        chunk = max(1.0, float(_POLL_INTERVAL_S))
                        chunk_deadline = deadline
                        while time.time() < chunk_deadline:
                            _raise_if_cancelled(
                                cancel_check,
                                f"waiting for pipeline {pid} to become idle",
                            )
                            slice_remaining = min(
                                chunk, chunk_deadline - time.time()
                            )
                            if slice_remaining <= 0:
                                break
                            try:
                                self._w().pipelines.wait_get_pipeline_idle(
                                    pipeline_id=pid,
                                    timeout=timedelta(seconds=slice_remaining),
                                )
                                break
                            except TimeoutError:
                                continue
                            except Exception as exc:  # noqa: BLE001
                                msg = str(exc).lower()
                                if "timeout" in msg or "timed out" in msg:
                                    continue
                                raise InfrastructureError(
                                    f"Lakeflow pipeline {pid} did not become idle "
                                    f"while syncing {name}: {exc}"
                                ) from exc
                    idle_pipeline_wait_done = True
                    continue

            if state in _TERMINAL_OK:
                return state

            self._sleep(_POLL_INTERVAL_S)

    def trigger_and_wait(
        self,
        name: str,
        *,
        timeout_s: int = DEFAULT_TIMEOUT_S,
        skip_initial_trigger: bool = False,
        cancel_check: Optional[Callable[[], bool]] = None,
        on_state_change: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Convenience: ``trigger_refresh`` then ``wait_for_completion``.

        ``cancel_check`` is forwarded to :meth:`wait_for_completion` so
        long-running pipeline waits bail out promptly when the surrounding
        task is cancelled.

        ``on_state_change`` is forwarded to :meth:`wait_for_completion` and
        called on every pipeline state transition so callers can surface
        live progress without polling.
        """
        update_id = ""
        if not skip_initial_trigger:
            try:
                update_id = self.trigger_refresh(name)
                logger.info(
                    "Lakeflow refresh triggered for %s (update_id=%s)",
                    name,
                    update_id or "n/a",
                )
            except (ValidationError, InfrastructureError) as exc:
                logger.warning(
                    "Could not trigger refresh on %s — will fall back to "
                    "polling the synced-table state directly: %s",
                    name,
                    exc,
                )
        uid = (update_id or "").strip()
        return self.wait_for_completion(
            name,
            timeout_s=timeout_s,
            pipeline_update_id=uid or None,
            cancel_check=cancel_check,
            on_state_change=on_state_change,
        )

    def _wait_for_pipeline_update(
        self,
        pipeline_id: str,
        update_id: str,
        timeout_s: float,
        *,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> None:
        """Poll ``pipelines.get_update`` until the update completes or fails."""
        deadline = time.time() + max(1.0, float(timeout_s))
        terminal_ok = {"COMPLETED"}
        terminal_bad = {"FAILED", "CANCELED"}
        while time.time() < deadline:
            _raise_if_cancelled(
                cancel_check,
                f"waiting for pipeline update {update_id}",
            )
            try:
                resp = self._w().pipelines.get_update(
                    pipeline_id=pipeline_id,
                    update_id=update_id,
                )
            except Exception as exc:  # noqa: BLE001
                raise InfrastructureError(
                    f"Could not read pipeline update {update_id} for {pipeline_id}: {exc}"
                ) from exc
            upd = getattr(resp, "update", None)
            st = getattr(upd, "state", None) if upd else None
            raw = getattr(st, "name", st) if st is not None else None
            name_st = str(raw).upper() if raw is not None else ""
            if "." in name_st:
                name_st = name_st.split(".")[-1]
            if name_st in terminal_ok:
                logger.info(
                    "Pipeline update %s finished successfully (pipeline %s)",
                    update_id,
                    pipeline_id,
                )
                return
            if name_st in terminal_bad:
                raise InfrastructureError(
                    f"Pipeline update {update_id} for {pipeline_id} ended with {name_st}"
                )
            self._sleep(_POLL_INTERVAL_S)
        raise InfrastructureError(
            f"Timed out waiting for pipeline update {update_id} (pipeline {pipeline_id})"
        )

    def delete(self, name: str, *, purge_data: bool = True) -> None:
        """Delete the synced table from UC; optionally drop the underlying PG table."""
        try:
            self._w().database.delete_synced_database_table(
                name=name, purge_data=purge_data
            )
            logger.info("Deleted synced table %s (purge_data=%s)", name, purge_data)
        except Exception as exc:  # noqa: BLE001
            if self._is_not_found(exc):
                return
            raise InfrastructureError(
                f"Failed to delete synced table {name}: {exc}"
            ) from exc

    # -- Payload builders -------------------------------------------------

    def _build_synced_table_payload(
        self,
        *,
        name: str,
        source_table_full_name: str,
        primary_key_columns: List[str],
        sync_mode: str,
        create_database_objects_if_missing: bool,
    ) -> Any:
        """Return a synced-table payload as a plain dict.

        We always use the dict form (not the SDK dataclass) because the
        Lakebase Autoscaling API requires ``database_project`` +
        ``database_branch`` for correct branch targeting, but the
        ``databricks-sdk`` ``SyncedDatabaseTable`` model does not expose
        those fields.  Passing an SDK object through ``_to_dict`` would
        silently strip them via ``as_dict()``, causing the API to fall
        back to the project's default (production) branch.  A plain dict
        is passed straight through ``_to_dict`` with all fields intact.
        """
        return self._build_synced_table_dict(
            name=name,
            source_table_full_name=source_table_full_name,
            primary_key_columns=primary_key_columns,
            sync_mode=sync_mode,
            create_database_objects_if_missing=create_database_objects_if_missing,
        )

    def _build_synced_table_sdk(
        self,
        *,
        name: str,
        source_table_full_name: str,
        primary_key_columns: List[str],
        sync_mode: str,
        create_database_objects_if_missing: bool,
    ) -> Any:
        sync_mode_norm = (sync_mode or "snapshot").upper()
        scheduling_policy_cls = getattr(_db_svc, "SyncedTableSchedulingPolicy", None)
        if scheduling_policy_cls is not None:
            policy = getattr(scheduling_policy_cls, sync_mode_norm, None) or getattr(
                scheduling_policy_cls, "SNAPSHOT"
            )
        else:
            policy = sync_mode_norm

        spec = _db_svc.SyncedTableSpec(  # type: ignore[union-attr]
            source_table_full_name=source_table_full_name,
            primary_key_columns=list(primary_key_columns),
            scheduling_policy=policy,
            create_database_objects_if_missing=create_database_objects_if_missing,
        )
        # NOTE: _build_synced_table_payload always returns a plain dict now
        # (see its docstring), so this method is never called from the hot path.
        # It is kept for completeness.  The SDK SyncedDatabaseTable object does
        # not expose database_branch, so we cannot use it for branch-targeted
        # creation.
        return _db_svc.SyncedDatabaseTable(  # type: ignore[union-attr]
            name=name,
            database_instance_name=self._project or None,
            logical_database_name=self._logical_db or None,
            spec=spec,
        )

    def _build_synced_table_dict(
        self,
        *,
        name: str,
        source_table_full_name: str,
        primary_key_columns: List[str],
        sync_mode: str,
        create_database_objects_if_missing: bool,
    ) -> dict:
        payload: dict = {"name": name}
        if self._project:
            payload["database_instance_name"] = self._project
        if self._branch:
            payload["database_branch"] = self._branch
        if self._logical_db:
            payload["logical_database_name"] = self._logical_db
        payload["spec"] = {
            "source_table_full_name": source_table_full_name,
            "primary_key_columns": list(primary_key_columns),
            "scheduling_policy": (sync_mode or "snapshot").upper(),
            "create_database_objects_if_missing": create_database_objects_if_missing,
        }
        return payload

    # -- SDK shape helpers ------------------------------------------------

    @staticmethod
    def _extract_pipeline_id(synced: Any) -> str:
        status = getattr(synced, "data_synchronization_status", None)
        if status is None:
            return ""
        return getattr(status, "pipeline_id", "") or ""

    @staticmethod
    def _extract_state(synced: Any) -> str:
        status = getattr(synced, "data_synchronization_status", None)
        if status is None:
            return ""
        state = (
            getattr(status, "detailed_state", None)
            or getattr(status, "state", None)
            or ""
        )
        raw = str(getattr(state, "name", state)).upper()
        return raw.removeprefix("SYNCED_TABLE_")

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        msg = str(exc).lower()
        return "not found" in msg or "does not exist" in msg or "404" in msg

    @staticmethod
    def _is_ghost_control_plane_state(exc: Exception) -> bool:
        """Return True for the Lakebase control-plane "destination reserved" error.

        This fires when the Postgres destination (schema + table) was reserved
        by a previous synced-table setup that did not clean up properly at the
        control-plane level.  GET and DELETE both return 404 (no UC registration
        exists), but POST returns this error.  It is distinct from the normal
        UC-registration 409 "already exists" response.
        """
        msg = str(exc).lower()
        return "destination table" in msg and "already exists in schema" in msg

    @staticmethod
    def _is_already_exists(exc: Exception) -> bool:
        msg = str(exc).lower()
        # Exclude the Lakebase control-plane ghost-reservation error
        # ("Destination table X already exists in schema Y") — that is handled
        # by _is_ghost_control_plane_state / _GHOST_FALLBACK_SUFFIXES logic.
        if "destination table" in msg and "already exists in schema" in msg:
            return False
        return (
            "already exists" in msg
            or "already_exists" in msg
            or "alreadyexists" in msg
        )

    @staticmethod
    def _is_active_pipeline_update_conflict(exc: BaseException) -> bool:
        msg = str(exc).lower()
        return (
            "active update" in msg
            and "already exists" in msg
            and "pipeline" in msg
        )
