"""Postgres-on-Lakebase implementation of :class:`RegistryStore`.

Storage layout (one Postgres schema, default ``ontobricks_registry``):

- ``registries``        — one row per OntoBricks instance
- ``global_config``     — single-row JSONB blob (warehouse_id, …)
- ``domains``           — one row per domain folder
- ``domain_versions``   — one row per domain version, full document split
                          into JSONB columns + a few hot scalar fields
- ``domain_permissions``— Viewer/Editor/Builder per principal/domain
- ``schedules``         — one row per scheduled domain
- ``schedule_runs``     — append-only, capped per domain
- ``build_runs``        — append-only build-run trace, one row per
                          Knowledge Graph build (all paths), keyed by
                          ``(domain_id, version)``

Authentication:
- Connection params (host/port/db/user) come from ``PG*`` env vars
  injected by the Apps ``postgres`` resource binding (Lakebase
  Autoscaling — the only tier supported by OntoBricks).
- The Postgres password is a short-lived OAuth token minted by
  :class:`back.core.databricks.LakebaseAuth`.

Cold start:
- Lakebase Autoscaling scales-to-zero when idle. Initial calls
  retry with exponential backoff on SQLSTATE ``57P03``
  ("cannot_connect_now") and on ``connection refused``.

Connection pooling:
- A process-wide LIFO pool (``_LakebasePool``) keeps a small handful
  of warm psycopg connections, keyed by host/db/user/schema. This
  avoids the 200-500 ms TCP+TLS+JWT handshake per call and turns
  hot-path operations like *Load Domain from Registry* into a single
  network round-trip per query. Connections are recycled before the
  1 h JWT expiry (``_POOL_MAX_LIFETIME_S``), so token rotation stays
  invisible to callers.

Token expiry:
- Authentication failures (SQLSTATE ``28P01``) trigger a single
  invalidate-and-retry cycle when *opening* a fresh connection. Pooled
  connections that hit auth failure mid-flight are discarded by the
  ``_connect`` context manager.

The whole module is import-safe even without ``psycopg`` installed —
it raises a clear error only when the class is instantiated.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

from back.core.databricks import get_lakebase_auth
from back.core.errors import InfrastructureError
from back.core.logging import get_logger
from back.objects.registry.registry_cache import invalidate_registry_cache

from ..base import (
    BuildRunEntry,
    DomainComment,
    DomainSummary,
    DomainTask,
    RegistryStore,
    ReviewEvent,
    ScheduleHistoryEntry,
    StoreError,
)

logger = get_logger(__name__)

_SCHEDULES_KEY = "schedules"
_DDL_FILENAME = "schema.sql"
_SCHEMA_TOKEN = "__SCHEMA__"
_SAFE_SCHEMA_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

_COLD_START_SQLSTATES = {"57P03"}  # cannot_connect_now
_AUTH_FAILURE_SQLSTATES = {"28P01"}  # invalid_password / token expired
_MAX_COLD_START_ATTEMPTS = 6
_INITIAL_BACKOFF_S = 1.0
_MAX_BACKOFF_S = 16.0

# Connection pool tuning. ``_POOL_MAX_LIFETIME_S`` is comfortably below
# the Lakebase JWT TTL (~1 h) so a connection is always retired before
# its credentials would expire mid-query. ``_POOL_MAX_SIZE`` is small on
# purpose: the registry is admin-traffic only, and Postgres connections
# are not cheap on the Lakebase side either.
_POOL_MAX_SIZE = 4
_POOL_MAX_LIFETIME_S = 45 * 60.0  # 45 min
_POOL_ACQUIRE_TIMEOUT_S = 30.0

# Whitelist used by ``table_row_counts``; keeps the dynamic SQL safe
# even though identifiers are also quoted via ``_q``.
_KNOWN_TABLES = frozenset(
    {
        "registries",
        "global_config",
        "domains",
        "domain_versions",
        "domain_permissions",
        "schedules",
        "schedule_runs",
        "build_runs",
        "domain_review_events",
        "domain_comments",
        "domain_tasks",
    }
)


def _require_psycopg():
    """Lazy import psycopg + psycopg.rows. Clear error when missing."""
    try:
        import psycopg  # noqa: F401
        from psycopg.rows import dict_row  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        raise InfrastructureError(
            "psycopg is required for the Lakebase backend. Install with "
            "``uv sync --extra lakebase`` (or ``pip install .[lakebase]``)."
        ) from exc
    return psycopg, dict_row


class _LakebasePool:
    """Tiny thread-safe LIFO connection pool for Lakebase.

    The pool is intentionally minimal — just enough plumbing to
    avoid the per-call TCP+TLS+JWT handshake while keeping all the
    bespoke behaviour (cold-start retries, OAuth token rotation,
    ``search_path`` setup) that we already had on the unpooled path.

    A single instance is shared by every :class:`LakebaseRegistryStore`
    pointing at the same host/db/user/schema (see :func:`_get_pool`).
    """

    def __init__(
        self,
        *,
        auth: Any,
        schema: str,
        database: str = "",
        max_size: int = _POOL_MAX_SIZE,
        max_lifetime: float = _POOL_MAX_LIFETIME_S,
    ) -> None:
        self._auth = auth
        self._schema = schema
        # Empty string means "use whatever PGDATABASE is bound to the
        # app". A non-empty value points the store at a different
        # database on the same Lakebase instance (the JWT scope is
        # per-instance so the cached token still authenticates).
        self._database = database or ""
        self._max_size = max_size
        self._max_lifetime = max_lifetime
        self._cv = threading.Condition()
        self._idle: List[Tuple[Any, float]] = []  # (conn, opened_at)
        self._size = 0  # checked-out + idle
        self._closed = False

    # -- public API --------------------------------------------------

    @contextmanager
    def connection(self):
        """Yield a healthy Lakebase connection from the pool."""
        conn, opened_at = self._acquire()
        try:
            yield conn
        except Exception:
            self._discard(conn)
            raise
        else:
            self._release(conn, opened_at)

    def close(self) -> None:
        """Drain the pool, closing every idle connection."""
        with self._cv:
            self._closed = True
            idle = list(self._idle)
            self._idle.clear()
            self._size = 0
            self._cv.notify_all()
        for conn, _ in idle:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    def stats(self) -> Dict[str, int]:
        with self._cv:
            return {
                "size": self._size,
                "idle": len(self._idle),
                "max_size": self._max_size,
            }

    # -- internals ---------------------------------------------------

    def _is_alive(self, conn: Any, opened_at: float) -> bool:
        if (time.monotonic() - opened_at) >= self._max_lifetime:
            return False
        try:
            return not conn.closed
        except Exception:  # noqa: BLE001
            return False

    def _acquire(self, timeout: float = _POOL_ACQUIRE_TIMEOUT_S) -> Tuple[Any, float]:
        deadline = time.monotonic() + timeout
        with self._cv:
            while True:
                if self._closed:
                    raise StoreError("Lakebase pool is closed")
                # Re-use an idle connection (LIFO keeps the hottest
                # connection on top — friendliest to TCP keep-alive).
                while self._idle:
                    conn, opened_at = self._idle.pop()
                    if self._is_alive(conn, opened_at):
                        return conn, opened_at
                    # Stale or closed: drop and keep looking.
                    self._size -= 1
                    try:
                        conn.close()
                    except Exception:  # noqa: BLE001
                        pass
                # No idle: open a fresh one if we are under cap. We
                # reserve the slot here, then release the lock to do
                # the (potentially slow) open.
                if self._size < self._max_size:
                    self._size += 1
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise StoreError(
                        f"Lakebase pool exhausted after waiting "
                        f"{timeout:.1f}s for a connection"
                    )
                self._cv.wait(remaining)
        # Open outside the lock. On failure, give the slot back so
        # other waiters are not starved by a transient outage.
        try:
            conn = self._open_one()
        except Exception:
            with self._cv:
                self._size -= 1
                self._cv.notify()
            raise
        return conn, time.monotonic()

    def _release(self, conn: Any, opened_at: float) -> None:
        with self._cv:
            if self._closed or not self._is_alive(conn, opened_at):
                self._size -= 1
                self._cv.notify()
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
                return
            self._idle.append((conn, opened_at))
            self._cv.notify()

    def _discard(self, conn: Any) -> None:
        with self._cv:
            self._size -= 1
            self._cv.notify()
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    def _open_one(self) -> Any:
        """Open one new psycopg connection, with cold-start + auth retry."""
        psycopg, _ = _require_psycopg()
        attempts = 0
        backoff = _INITIAL_BACKOFF_S
        retried_auth = False
        while True:
            try:
                kwargs = self._auth.kwargs(
                    application_name="ontobricks-registry"
                )
                if self._database:
                    kwargs["dbname"] = self._database
                conn = psycopg.connect(autocommit=True, **kwargs)
                with conn.cursor() as cur:
                    cur.execute(f'SET search_path TO "{self._schema}", public')
                return conn
            except Exception as exc:  # noqa: BLE001
                sqlstate = getattr(exc, "sqlstate", "") or ""
                msg = str(exc).lower()
                cold = (
                    sqlstate in _COLD_START_SQLSTATES
                    or "starting up" in msg
                    or "connection refused" in msg
                )
                auth_failed = (
                    sqlstate in _AUTH_FAILURE_SQLSTATES
                    or "authentication failed" in msg
                )
                if auth_failed and not retried_auth:
                    self._auth.invalidate()
                    retried_auth = True
                    logger.info("Lakebase auth failed; rotating token and retrying")
                    continue
                if cold and attempts < _MAX_COLD_START_ATTEMPTS:
                    attempts += 1
                    sleep_for = min(backoff, _MAX_BACKOFF_S)
                    logger.info(
                        "Lakebase cold start (sqlstate=%s, attempt=%d/%d); "
                        "sleeping %.1fs",
                        sqlstate or "?",
                        attempts,
                        _MAX_COLD_START_ATTEMPTS,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                    backoff *= 2
                    continue
                raise StoreError(f"Lakebase connection failed: {exc}") from exc


# Process-wide pool registry. ``LakebaseRegistryStore`` is rebuilt on
# every request through :class:`RegistryFactory`, so the pool itself
# must outlive any single store instance.
_pools_lock = threading.Lock()
_pools: Dict[Tuple[str, str, str, str, str, str, str], _LakebasePool] = {}


def _safe_attr(obj: Any, name: str) -> str:
    """Read an attribute that may raise ``ValidationError`` lazily."""
    try:
        return str(getattr(obj, name, "") or "")
    except Exception:  # noqa: BLE001
        return ""


def _get_pool(auth: Any, schema: str, database: str = "") -> _LakebasePool:
    """Return (and lazily create) the shared pool for *auth* + *schema* + *database*.

    The ``database`` arg is the optional override that points the store
    at a different Postgres database on the same Lakebase instance. The
    empty string means "use the bound PGDATABASE". Two stores that
    differ only by ``database`` get distinct pools (and distinct
    connections), which is what we want — a Postgres connection only
    ever talks to a single database.
    """
    bound_db = _safe_attr(auth, "database")
    effective_db = database or bound_db
    key = (
        _safe_attr(auth, "host"),
        _safe_attr(auth, "port"),
        bound_db,
        effective_db,
        _safe_attr(auth, "user"),
        _safe_attr(auth, "instance_name"),
        schema,
    )
    with _pools_lock:
        pool = _pools.get(key)
        if pool is None:
            pool = _LakebasePool(auth=auth, schema=schema, database=database)
            _pools[key] = pool
            logger.info(
                "Created Lakebase connection pool for %s/%s (schema=%s, max_size=%d)",
                key[0],
                effective_db,
                schema,
                _POOL_MAX_SIZE,
            )
        return pool


# ---------------------------------------------------------------------------
# Public helper: fetch the (catalog, schema, volume) of the Lakebase row
# without instantiating a full ``LakebaseRegistryStore``. Used by
# ``RegistryCfg.from_domain`` so the active registry triplet matches what
# is stored *in Lakebase* (where binary artifacts were originally archived)
# rather than whatever Volume the Apps runtime happens to bind. Without
# this, a deployment whose ``volume`` resource points at a different
# Volume than the one referenced by the Lakebase row resolves
# ``effective_view_table`` and ``uc_version_path`` to paths where no
# artefact exists — every existence badge on the Build page goes red even
# though the underlying data is intact.
# ---------------------------------------------------------------------------

_TRIPLET_CACHE: Dict[Tuple[str, str], Optional[Tuple[str, str, str]]] = {}
_TRIPLET_LOCK = threading.Lock()
_TRIPLET_NEGATIVE_TTL_S = 60.0
_TRIPLET_NEG_TS: Dict[Tuple[str, str], float] = {}


def fetch_lakebase_registry_triplet(
    schema: str,
    database: str = "",
) -> Optional[Tuple[str, str, str]]:
    """Return the ``(catalog, schema, volume)`` stored in the Lakebase ``registries`` row.

    Returns ``None`` when Lakebase is unavailable, the row doesn't exist
    yet, or any error occurs — callers must fall back gracefully (e.g.
    to the bound Volume resource path).

    Positive results are cached for the lifetime of the process keyed by
    ``(schema, database)``. Negative results are cached for
    :data:`_TRIPLET_NEGATIVE_TTL_S` so a transient cold-start failure
    doesn't stick around forever, but we also don't hammer the database
    on every page render. Restart the app to invalidate after editing
    the row directly in Postgres.
    """
    key = (schema or "", database or "")
    with _TRIPLET_LOCK:
        if key in _TRIPLET_CACHE:
            cached = _TRIPLET_CACHE[key]
            if cached is not None:
                return cached
            ts = _TRIPLET_NEG_TS.get(key, 0.0)
            if (time.time() - ts) < _TRIPLET_NEGATIVE_TTL_S:
                return None

    try:
        auth = get_lakebase_auth()
    except Exception as exc:  # noqa: BLE001
        logger.debug("Lakebase auth unavailable for triplet probe: %s", exc)
        with _TRIPLET_LOCK:
            _TRIPLET_CACHE[key] = None
            _TRIPLET_NEG_TS[key] = time.time()
        return None

    try:
        with _get_pool(auth, schema, database).connection() as conn, conn.cursor() as cur:
            # Identifiers can't be parameterised in psycopg, so we use the
            # same _quote-via-double-replace trick as the rest of the
            # store. ``schema`` is operator-controlled config, never user
            # input, but escape defensively.
            quoted = '"' + schema.replace('"', '""') + '"'
            cur.execute(
                f"SELECT catalog, schema, volume FROM {quoted}.registries "
                "ORDER BY created_at ASC LIMIT 1"
            )
            row = cur.fetchone()
        if not row:
            with _TRIPLET_LOCK:
                _TRIPLET_CACHE[key] = None
                _TRIPLET_NEG_TS[key] = time.time()
            return None
        triplet = (str(row[0]), str(row[1]), str(row[2]))
        with _TRIPLET_LOCK:
            _TRIPLET_CACHE[key] = triplet
        return triplet
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not fetch Lakebase registry triplet: %s", exc)
        with _TRIPLET_LOCK:
            _TRIPLET_CACHE[key] = None
            _TRIPLET_NEG_TS[key] = time.time()
        return None


def reset_lakebase_triplet_cache() -> None:
    """Clear the cached registry triplet — call after admin-side edits in Postgres or in tests."""
    with _TRIPLET_LOCK:
        _TRIPLET_CACHE.clear()
        _TRIPLET_NEG_TS.clear()


class LakebaseRegistryStore(RegistryStore):
    """Postgres-backed registry store. Optional backend.

    Parameters
    ----------
    registry_cfg:
        :class:`back.objects.registry.RegistryService.RegistryCfg` —
        used as the registry identity (the catalog/schema/volume
        triplet still matters because binaries live on the Volume).
    schema:
        Postgres schema where registry tables live. Defaults to
        ``"ontobricks_registry"``.
    database:
        Optional Postgres database name. Empty (the default) means
        "use whatever ``PGDATABASE`` is bound to the app". A non-empty
        value lets the admin point the registry at any other database
        that lives on the *same* Lakebase instance — provided the
        service principal has ``CONNECT`` on it. The Lakebase JWT
        scope is per-instance, so the cached token still authenticates
        without a re-mint.
    """

    def __init__(
        self,
        *,
        registry_cfg,
        schema: str = "ontobricks_registry",
        database: str = "",
    ):
        if not _SAFE_SCHEMA_RE.match(schema or ""):
            raise InfrastructureError(
                f"Invalid Lakebase schema name {schema!r}; must match "
                f"[a-zA-Z_][a-zA-Z0-9_]*"
            )
        self._cfg = registry_cfg
        self._schema = schema
        self._database = database or ""
        self._auth = get_lakebase_auth()
        self._registry_id: Optional[str] = None  # cached after initialize()
        # Guards the lazy ``CREATE TABLE IF NOT EXISTS build_runs`` used to
        # self-heal deployments created before the build-run trace existed
        # (the full DDL only runs from the Settings "Initialize" action).
        self._build_runs_ready = False
        # Guards the lazy ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS status``
        # used to self-heal deployments created before the lifecycle status
        # column existed (same pattern as ``_build_runs_ready``).
        self._status_column_ready = False
        # Guards the lazy ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS
        # review_quorum`` used to self-heal deployments created before the
        # per-domain sign-off quorum existed (same pattern as
        # ``_status_column_ready``).
        self._quorum_column_ready = False
        # Guards the lazy ``CREATE TABLE IF NOT EXISTS domain_review_events``
        # used to self-heal deployments created before the review/validation
        # audit log existed (same pattern as ``_build_runs_ready``).
        self._review_events_ready = False
        # Guards the lazy ``CREATE TABLE IF NOT EXISTS domain_comments /
        # domain_tasks`` used to self-heal deployments created before the
        # collaborative comments + tasks feature existed (same pattern as
        # ``_review_events_ready``).
        self._collab_tables_ready = False

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def backend(self) -> str:
        return "lakebase"

    @property
    def cache_key(self) -> str:
        c = self._cfg
        # Include the backend tag so a switch at runtime invalidates the
        # registry-level TTL cache automatically. The database override
        # (when set) is part of the key so swapping it busts the cache.
        db = self._effective_database
        return (
            f"lakebase:{self._auth.host}:{db}:{self._schema}:"
            f"{c.catalog}.{c.schema}.{c.volume}"
        )

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def _effective_database(self) -> str:
        """Resolve the Postgres database name actually used by the store.

        Returns the explicit override when set (admin chose a database
        from the UI), otherwise the auto-injected ``PGDATABASE`` from
        the Apps runtime via :class:`LakebaseAuth`.
        """
        return self._database or self._auth.database

    def is_initialized(self) -> bool:
        """Cheap boolean probe — silent on errors (matches base contract).

        Most callers only need a yes/no answer (e.g. *Initialize*
        button gating). Use :meth:`init_status` when you also want
        the *reason* an initialised schema looks empty (missing
        ``USAGE`` on the schema, no registry row, …) — that's what
        the admin Registry Location panel surfaces to operators.
        """
        return self.init_status()["initialized"]

    def init_status(self) -> Dict[str, Any]:
        """Detailed initialise-probe with explicit failure reasons.

        Returns ``{initialized: bool, reason: str, error: Optional[str]}``.
        ``reason`` is a short stable token (``"ok"``, ``"no_usage"``,
        ``"no_registries_table"``, ``"no_registry_row"``,
        ``"connect_failed"``) suitable for log filtering; ``error``
        is a human-readable explanation suitable for the admin UI.

        The reason ``no_usage`` is the most common silent-failure
        mode: when the app's service principal lacks ``USAGE`` on
        the registry schema, ``to_regclass`` returns NULL even
        though the tables exist and hold data — turning the panel
        into a misleading "not initialised, 0 rows everywhere"
        screen. Surfacing the explicit reason lets the operator
        run ``scripts/bootstrap-lakebase-perms.sh`` and move on
        instead of hunting for a phantom data loss.
        """
        try:
            with self._connect() as conn, conn.cursor() as cur:
                # Probe the live session context so the error message can
                # tell the operator exactly which (database, role, schema)
                # the check ran against — this is the only reliable way
                # to spot grants that landed on a different database
                # than the one the Apps ``postgres`` resource binds.
                cur.execute(
                    "SELECT current_database(), current_user, "
                    "       has_schema_privilege(current_user, %s, 'USAGE'), "
                    "       EXISTS (SELECT 1 FROM pg_namespace "
                    "               WHERE nspname = %s)",
                    (self._schema, self._schema),
                )
                row = cur.fetchone()
                if not row:
                    has_usage = False
                    cur_db = self._effective_database
                    cur_user = "?"
                    schema_exists = False
                else:
                    cur_db, cur_user, has_usage_raw, schema_exists = row
                    has_usage = bool(has_usage_raw)
                if not has_usage:
                    if schema_exists:
                        msg = (
                            f"Role '{cur_user}' lacks USAGE on schema "
                            f"'{self._schema}' in database '{cur_db}'. "
                            f"Run scripts/bootstrap-lakebase-perms.sh "
                            f"-i <instance> -d {cur_db} -s {self._schema} "
                            f"-a <app-name>, or GRANT USAGE ON SCHEMA "
                            f"\"{self._schema}\" TO \"{cur_user}\" "
                            f"directly in database '{cur_db}'."
                        )
                    else:
                        msg = (
                            f"Schema '{self._schema}' does not exist in "
                            f"database '{cur_db}' (role '{cur_user}'). "
                            f"Either initialize it from Settings > "
                            f"Registry, or check that bundle "
                            f"``lakebase_*`` Postgres binding points at "
                            f"the database where the schema actually "
                            f"lives."
                        )
                    logger.warning("Lakebase init probe: %s", msg)
                    return {
                        "initialized": False,
                        "reason": "no_usage",
                        "error": msg,
                    }
                cur.execute(
                    "SELECT to_regclass(%s) IS NOT NULL",
                    (f"{self._schema}.registries",),
                )
                ok = bool(cur.fetchone()[0])
            if not ok:
                return {
                    "initialized": False,
                    "reason": "no_registries_table",
                    "error": (
                        f"Schema '{self._schema}' has no 'registries' "
                        f"table — run *Initialize* to create it."
                    ),
                }
            if self._registry_id is None:
                self._registry_id = self._fetch_registry_id()
            if self._registry_id is None:
                return {
                    "initialized": False,
                    "reason": "no_registry_row",
                    "error": (
                        f"Schema '{self._schema}' has no registry row "
                        f"yet — run *Initialize* to seed it."
                    ),
                }
            return {"initialized": True, "reason": "ok", "error": None}
        except Exception as exc:  # noqa: BLE001
            logger.warning("Lakebase init probe failed: %s", exc)
            return {
                "initialized": False,
                "reason": "connect_failed",
                "error": f"Lakebase probe failed: {exc}",
            }

    def initialize(self, *, client: Any = None) -> Tuple[bool, str]:
        del client  # not used: Lakebase instance is provisioned out of band
        try:
            self._apply_ddl()
            self._registry_id = self._ensure_registry_row()
            self._scrub_global_config_legacy_keys()
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")  # wake probe
            logger.info(
                "Lakebase registry initialised (schema=%s, host=%s)",
                self._schema,
                self._auth.host,
            )
            return True, (
                f"Lakebase registry initialized at "
                f"{self._auth.host}/{self._effective_database} "
                f"(schema={self._schema})"
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Lakebase initialise failed")
            return False, f"Failed to initialise Lakebase registry: {exc}"

    # ------------------------------------------------------------------
    # Domain listings
    # ------------------------------------------------------------------

    def list_domain_folders(self) -> Tuple[bool, List[str], str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT folder FROM {self._q(self._schema)}.domains "
                    "WHERE registry_id = %s ORDER BY folder",
                    (self._registry(),),
                )
                names = [r[0] for r in cur.fetchall()]
            return True, names, ""
        except Exception as exc:  # noqa: BLE001
            return False, [], str(exc)

    def list_domains_with_metadata(self) -> Tuple[bool, List[DomainSummary], str]:
        try:
            self._ensure_domain_versions_status_column()
            self._ensure_domains_review_quorum_column()
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        f"""
                        SELECT d.id, d.folder, d.description, d.base_uri,
                               d.review_quorum
                        FROM {self._q(self._schema)}.domains d
                        WHERE d.registry_id = %s
                        ORDER BY d.folder
                        """,
                        (self._registry(),),
                    )
                    domain_rows = cur.fetchall()
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        f"""
                        SELECT v.domain_id, v.version, v.mcp_enabled, v.status,
                               v.last_update, v.last_build, v.info, v.ontology
                        FROM {self._q(self._schema)}.domain_versions v
                        JOIN {self._q(self._schema)}.domains d ON d.id = v.domain_id
                        WHERE d.registry_id = %s
                        ORDER BY v.domain_id,
                                 string_to_array(v.version, '.')::int[] DESC
                        """,
                        (self._registry(),),
                    )
                    version_rows = cur.fetchall()

            by_domain: Dict[str, List[Dict[str, Any]]] = {}
            for v in version_rows:
                by_domain.setdefault(str(v["domain_id"]), []).append(v)

            result: List[DomainSummary] = []
            for d in domain_rows:
                versions = by_domain.get(str(d["id"]), [])
                description = d["description"] or ""
                base_uri = d["base_uri"] or ""
                if versions:
                    latest = versions[0]
                    info = latest["info"] or {}
                    description = description or info.get("description", "")
                    ont = latest["ontology"] or {}
                    base_uri = base_uri or ont.get("base_uri", "")
                result.append(
                    {
                        "name": d["folder"],
                        "base_uri": base_uri,
                        "description": description,
                        "review_quorum": max(1, int(d.get("review_quorum") or 1)),
                        "versions": [
                            {
                                "version": v["version"],
                                "active": bool(v["mcp_enabled"]),
                                "status": v["status"] or "DRAFT",
                                "last_update": v["last_update"] or "",
                                "last_build": v["last_build"] or "",
                            }
                            for v in versions
                        ],
                    }
                )
            return True, result, ""
        except Exception as exc:  # noqa: BLE001
            logger.exception("list_domains_with_metadata failed")
            return False, [], str(exc)

    def domain_exists(self, folder: str) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {self._q(self._schema)}.domains "
                    "WHERE registry_id = %s AND folder = %s",
                    (self._registry(), folder),
                )
                return cur.fetchone() is not None
        except Exception as exc:  # noqa: BLE001
            logger.debug("domain_exists(%s) failed: %s", folder, exc)
            return False

    def get_domain_quorum(self, folder: str) -> int:
        """Per-domain review sign-off quorum (>= 1). Default ``1`` when the
        domain is missing or the column has not been provisioned yet.
        """
        try:
            self._ensure_domains_review_quorum_column()
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT review_quorum FROM {self._q(self._schema)}.domains "
                    "WHERE registry_id = %s AND folder = %s",
                    (self._registry(), folder),
                )
                row = cur.fetchone()
            if not row or row[0] is None:
                return 1
            return max(1, int(row[0]))
        except Exception as exc:  # noqa: BLE001
            logger.debug("get_domain_quorum(%s) failed: %s", folder, exc)
            return 1

    def delete_domain(self, folder: str) -> List[str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self._q(self._schema)}.domains "
                    "WHERE registry_id = %s AND folder = %s",
                    (self._registry(), folder),
                )
            invalidate_registry_cache(self.cache_key)
            return []
        except Exception as exc:  # noqa: BLE001
            return [str(exc)]

    # ------------------------------------------------------------------
    # Versions
    # ------------------------------------------------------------------

    def list_versions(self, folder: str) -> Tuple[bool, List[str], str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT v.version
                    FROM {self._q(self._schema)}.domain_versions v
                    JOIN {self._q(self._schema)}.domains d ON d.id = v.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s
                    ORDER BY string_to_array(v.version, '.')::int[]
                    """,
                    (self._registry(), folder),
                )
                versions = [r[0] for r in cur.fetchall()]
            return True, versions, ""
        except Exception as exc:  # noqa: BLE001
            return False, [], str(exc)

    def read_version(
        self, folder: str, version: str
    ) -> Tuple[bool, Dict[str, Any], str]:
        try:
            self._ensure_domain_versions_status_column()
            self._ensure_domains_review_quorum_column()
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT v.info, v.ontology, v.assignment, v.design_layout,
                           v.metadata, v.version, v.mcp_enabled, v.status,
                           v.last_update, v.last_build, d.review_quorum
                    FROM {self._q(self._schema)}.domain_versions v
                    JOIN {self._q(self._schema)}.domains d ON d.id = v.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s AND v.version = %s
                    """,
                    (self._registry(), folder, version),
                )
                row = cur.fetchone()
            if not row:
                return False, {}, f"Version {version} not found for domain {folder}"
            info = row["info"] or {}
            info.setdefault("mcp_enabled", bool(row["mcp_enabled"]))
            info["review_quorum"] = max(1, int(row.get("review_quorum") or 1))
            info["status"] = row["status"] or "DRAFT"
            if row["last_update"]:
                info["last_update"] = row["last_update"]
            if row["last_build"]:
                info["last_build"] = row["last_build"]
            doc = {
                "info": info,
                "versions": {
                    row["version"]: {
                        "ontology": row["ontology"] or {},
                        "assignment": row["assignment"] or {},
                        "design_layout": row["design_layout"] or {},
                        "metadata": row["metadata"] or {},
                    }
                },
            }
            return True, doc, ""
        except Exception as exc:  # noqa: BLE001
            return False, {}, str(exc)

    def write_version(
        self, folder: str, version: str, data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        try:
            self._ensure_domain_versions_status_column()
            self._ensure_domains_review_quorum_column()
            info = data.get("info", {}) or {}
            ver_blob = (data.get("versions") or {}).get(version, {}) or {}
            ontology = ver_blob.get("ontology", data.get("ontology", {})) or {}
            assignment = ver_blob.get("assignment", data.get("assignment", {})) or {}
            design = ver_blob.get("design_layout", data.get("design_layout", {})) or {}
            metadata = ver_blob.get("metadata", data.get("metadata", {})) or {}
            mcp_enabled = bool(info.get("mcp_enabled"))
            status = info.get("status") or "DRAFT"
            last_update = info.get("last_update", "") or ""
            last_build = info.get("last_build", "") or ""
            description = info.get("description", "") or ""
            base_uri = ontology.get("base_uri", "") or ""
            review_quorum = max(1, int(info.get("review_quorum") or 1))

            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._q(self._schema)}.domains
                        (registry_id, folder, description, base_uri,
                         review_quorum)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (registry_id, folder)
                    DO UPDATE SET description   = EXCLUDED.description,
                                  base_uri      = EXCLUDED.base_uri,
                                  review_quorum = EXCLUDED.review_quorum,
                                  updated_at    = now()
                    RETURNING id
                    """,
                    (
                        self._registry(),
                        folder,
                        description,
                        base_uri,
                        review_quorum,
                    ),
                )
                domain_id = cur.fetchone()[0]
                cur.execute(
                    f"""
                    INSERT INTO {self._q(self._schema)}.domain_versions
                        (domain_id, version, info, ontology, assignment,
                         design_layout, metadata, mcp_enabled, status,
                         last_update, last_build)
                    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                            %s::jsonb, %s::jsonb, %s, %s, %s, %s)
                    ON CONFLICT (domain_id, version)
                    DO UPDATE SET info          = EXCLUDED.info,
                                  ontology      = EXCLUDED.ontology,
                                  assignment    = EXCLUDED.assignment,
                                  design_layout = EXCLUDED.design_layout,
                                  metadata      = EXCLUDED.metadata,
                                  mcp_enabled   = EXCLUDED.mcp_enabled,
                                  status        = EXCLUDED.status,
                                  last_update   = EXCLUDED.last_update,
                                  last_build    = EXCLUDED.last_build,
                                  updated_at    = now()
                    """,
                    (
                        domain_id,
                        version,
                        json.dumps(info),
                        json.dumps(ontology),
                        json.dumps(assignment),
                        json.dumps(design),
                        json.dumps(metadata),
                        mcp_enabled,
                        status,
                        last_update,
                        last_build,
                    ),
                )
            invalidate_registry_cache(self.cache_key)
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.exception("write_version failed for %s/%s", folder, version)
            return False, str(exc)

    def delete_version(self, folder: str, version: str) -> Tuple[bool, str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    DELETE FROM {self._q(self._schema)}.domain_versions
                    WHERE version = %s
                      AND domain_id IN (
                          SELECT id FROM {self._q(self._schema)}.domains
                          WHERE registry_id = %s AND folder = %s
                      )
                    """,
                    (version, self._registry(), folder),
                )
            invalidate_registry_cache(self.cache_key)
            return True, ""
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def update_version_status(
        self, folder: str, version: str, status: str
    ) -> Tuple[bool, str]:
        """Set the lifecycle ``status`` of a single (domain, version).

        Targeted single-row UPDATE so a status transition never rewrites
        the full version document. Also mirrors ``status`` into the
        version ``info`` blob so cached reads stay consistent.
        """
        try:
            self._ensure_domain_versions_status_column()
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self._q(self._schema)}.domain_versions v
                    SET status = %s,
                        info = jsonb_set(v.info, '{{status}}', to_jsonb(%s::text)),
                        updated_at = now()
                    FROM {self._q(self._schema)}.domains d
                    WHERE v.domain_id = d.id
                      AND d.registry_id = %s AND d.folder = %s
                      AND v.version = %s
                    """,
                    (status, status, self._registry(), folder, version),
                )
                if cur.rowcount == 0:
                    return False, (
                        f"Version {version} not found for domain {folder}"
                    )
            invalidate_registry_cache(self.cache_key)
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "update_version_status failed for %s/%s", folder, version
            )
            return False, str(exc)

    # ------------------------------------------------------------------
    # Permissions
    # ------------------------------------------------------------------

    def load_domain_permissions(self, folder: str) -> Dict[str, Any]:
        try:
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT p.principal, p.principal_type, p.display_name, p.role
                    FROM {self._q(self._schema)}.domain_permissions p
                    JOIN {self._q(self._schema)}.domains d ON d.id = p.domain_id
                    WHERE d.registry_id = %s AND d.folder = %s
                    ORDER BY lower(p.principal)
                    """,
                    (self._registry(), folder),
                )
                rows = cur.fetchall()
            return {"version": 1, "permissions": [dict(r) for r in rows]}
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_domain_permissions(%s) failed: %s", folder, exc)
            return {"version": 1, "permissions": []}

    def save_domain_permissions(
        self, folder: str, data: Dict[str, Any]
    ) -> Tuple[bool, str]:
        entries = data.get("permissions") or []
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id FROM {self._q(self._schema)}.domains
                    WHERE registry_id = %s AND folder = %s
                    """,
                    (self._registry(), folder),
                )
                row = cur.fetchone()
                if not row:
                    return False, f"Domain '{folder}' not found"
                domain_id = row[0]
                cur.execute(
                    f"DELETE FROM {self._q(self._schema)}.domain_permissions "
                    "WHERE domain_id = %s",
                    (domain_id,),
                )
                for e in entries:
                    cur.execute(
                        f"""
                        INSERT INTO {self._q(self._schema)}.domain_permissions
                            (domain_id, principal, principal_type,
                             display_name, role)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            domain_id,
                            e.get("principal", ""),
                            e.get("principal_type", "user"),
                            e.get("display_name", ""),
                            e.get("role", "viewer"),
                        ),
                    )
            return True, "Domain permissions saved"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    # ------------------------------------------------------------------
    # Schedules + history
    # ------------------------------------------------------------------

    def load_schedules(self) -> Dict[str, Dict[str, Any]]:
        try:
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT domain_name, interval_minutes, drop_existing,
                           enabled, version, last_run, last_status, last_message
                    FROM {self._q(self._schema)}.schedules
                    WHERE registry_id = %s
                    """,
                    (self._registry(),),
                )
                rows = cur.fetchall()
            out: Dict[str, Dict[str, Any]] = {}
            for r in rows:
                out[r["domain_name"]] = {
                    "interval_minutes": r["interval_minutes"],
                    "drop_existing": r["drop_existing"],
                    "enabled": r["enabled"],
                    "version": r["version"] or "latest",
                    "last_run": r["last_run"].isoformat() if r["last_run"] else None,
                    "last_status": r["last_status"],
                    "last_message": r["last_message"],
                }
            return out
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_schedules failed: %s", exc)
            return {}

    def save_schedules(
        self, schedules: Dict[str, Dict[str, Any]]
    ) -> Tuple[bool, str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    DELETE FROM {self._q(self._schema)}.schedules
                    WHERE registry_id = %s
                    """,
                    (self._registry(),),
                )
                for name, cfg in schedules.items():
                    cur.execute(
                        f"""
                        INSERT INTO {self._q(self._schema)}.schedules
                            (registry_id, domain_name, interval_minutes,
                             drop_existing, enabled, version, last_run,
                             last_status, last_message)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            self._registry(),
                            name,
                            int(cfg.get("interval_minutes", 60)),
                            bool(cfg.get("drop_existing", True)),
                            bool(cfg.get("enabled", True)),
                            cfg.get("version", "latest") or "latest",
                            cfg.get("last_run"),
                            cfg.get("last_status"),
                            cfg.get("last_message"),
                        ),
                    )
            return True, "Schedules saved"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def load_schedule_history(self, folder: str) -> List[ScheduleHistoryEntry]:
        try:
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT run_ts, status, message, duration_s, triple_count
                    FROM {self._q(self._schema)}.schedule_runs
                    WHERE registry_id = %s AND domain_name = %s
                    ORDER BY run_ts ASC
                    """,
                    (self._registry(), folder),
                )
                rows = cur.fetchall()
            return [
                {
                    "timestamp": r["run_ts"].isoformat(),
                    "status": r["status"],
                    "message": r["message"] or "",
                    "duration_s": float(r["duration_s"] or 0),
                    "triple_count": int(r["triple_count"] or 0),
                }
                for r in rows
            ]
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_schedule_history(%s) failed: %s", folder, exc)
            return []

    def append_schedule_history(
        self, folder: str, entry: ScheduleHistoryEntry, *, max_entries: int = 50
    ) -> None:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._q(self._schema)}.schedule_runs
                        (registry_id, domain_name, run_ts, status, message,
                         duration_s, triple_count)
                    VALUES (%s, %s, COALESCE(%s::timestamptz, now()),
                            %s, %s, %s, %s)
                    """,
                    (
                        self._registry(),
                        folder,
                        entry.get("timestamp"),
                        entry.get("status", ""),
                        entry.get("message", ""),
                        float(entry.get("duration_s", 0) or 0),
                        int(entry.get("triple_count", 0) or 0),
                    ),
                )
                cur.execute(
                    f"""
                    DELETE FROM {self._q(self._schema)}.schedule_runs
                    WHERE registry_id = %s AND domain_name = %s
                      AND id NOT IN (
                          SELECT id FROM {self._q(self._schema)}.schedule_runs
                          WHERE registry_id = %s AND domain_name = %s
                          ORDER BY run_ts DESC
                          LIMIT %s
                      )
                    """,
                    (
                        self._registry(),
                        folder,
                        self._registry(),
                        folder,
                        max_entries,
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("append_schedule_history(%s) failed: %s", folder, exc)

    # ------------------------------------------------------------------
    # Lifecycle status column (self-heal)
    # ------------------------------------------------------------------

    def _ensure_domain_versions_status_column(self) -> bool:
        """Lazily add ``domain_versions.status`` (+ index) if missing.

        Self-heals deployments created before the lifecycle status column
        existed: the full DDL only runs from the Settings *Initialize*
        action. Idempotent (``ADD COLUMN IF NOT EXISTS`` /
        ``CREATE INDEX IF NOT EXISTS``) and guarded by a per-instance flag
        so we only pay the round-trip once per store. Best-effort: on
        failure it logs and returns ``False`` so callers can no-op.
        """
        if self._status_column_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                # Check first: if the column already exists (created by
                # bootstrap as the schema owner), skip all DDL.  Both
                # ALTER TABLE and CREATE INDEX require table ownership in
                # Postgres — attempting them as the SP (who doesn't own
                # domain_versions) raises "must be owner of table …"
                # even with IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.
                cur.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'domain_versions' "
                    "AND column_name = 'status'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._status_column_ready = True
                    return True
                # Column absent — attempt DDL (requires schema owner to
                # have not yet run bootstrap-lakebase-perms.sh).
                cur.execute(
                    f"""
                    ALTER TABLE {sch}.domain_versions
                        ADD COLUMN IF NOT EXISTS status text NOT NULL
                        DEFAULT 'DRAFT'
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_domain_versions_status
                        ON {sch}.domain_versions(domain_id, status)
                    """
                )
            self._status_column_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not add domain_versions.status column — "
                "run `make bootstrap-lakebase` (or scripts/bootstrap-lakebase-perms.sh) "
                "as the schema owner to apply the migration: %s",
                exc,
            )
            return False

    def _ensure_domains_review_quorum_column(self) -> bool:
        """Lazily add ``domains.review_quorum`` if missing.

        Self-heals deployments created before the per-domain sign-off
        quorum existed. Same idempotent, ownership-aware pattern as
        :meth:`_ensure_domain_versions_status_column`. Best-effort: on
        failure it logs and returns ``False`` so callers can fall back to
        the default quorum.
        """
        if self._quorum_column_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = 'domains' "
                    "AND column_name = 'review_quorum'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._quorum_column_ready = True
                    return True
                cur.execute(
                    f"""
                    ALTER TABLE {sch}.domains
                        ADD COLUMN IF NOT EXISTS review_quorum integer
                        NOT NULL DEFAULT 1
                    """
                )
            self._quorum_column_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not add domains.review_quorum column — "
                "run `make bootstrap-lakebase` (or scripts/bootstrap-lakebase-perms.sh) "
                "as the schema owner to apply the migration: %s",
                exc,
            )
            return False

    # ------------------------------------------------------------------
    # Build-run trace (analytics)
    # ------------------------------------------------------------------

    def _ensure_build_runs_table(self) -> bool:
        """Lazily create ``build_runs`` (+ index) if it is missing.

        Self-heals deployments created before the build-run trace
        existed: the full DDL only runs from the Settings *Initialize*
        action, so without this an upgraded instance would have no
        table until an admin re-ran Initialize. Idempotent (every
        statement uses ``IF NOT EXISTS``) and guarded by a per-instance
        flag so we only pay the round-trip once per store. Best-effort:
        on failure (e.g. missing GRANT) it logs and returns ``False``
        so callers can no-op instead of breaking a build.
        """
        if self._build_runs_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                # Check first: if the table already exists (created by
                # bootstrap as the schema owner), skip all DDL.  CREATE
                # INDEX requires table ownership in Postgres — running it
                # when we don't own the table raises "must be owner of
                # table build_runs" even with IF NOT EXISTS.
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = 'build_runs'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._build_runs_ready = True
                    return True
                # Table is absent — SP has CREATE ON SCHEMA so it can
                # create the table (and will own it, allowing the index).
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.build_runs (
                        id                  bigserial PRIMARY KEY,
                        domain_id           uuid NOT NULL
                                            REFERENCES {sch}.domains(id)
                                            ON DELETE CASCADE,
                        version             text NOT NULL,
                        build_kind          text NOT NULL DEFAULT 'session',
                        status              text NOT NULL,
                        message             text NOT NULL DEFAULT '',
                        error               text NOT NULL DEFAULT '',
                        started_at          timestamptz NOT NULL DEFAULT now(),
                        finished_at         timestamptz,
                        duration_s          double precision NOT NULL DEFAULT 0,
                        triple_count        bigint NOT NULL DEFAULT 0,
                        entity_count        integer NOT NULL DEFAULT 0,
                        relationship_count  integer NOT NULL DEFAULT 0,
                        sql_chars           integer NOT NULL DEFAULT 0,
                        graph_engine        text NOT NULL DEFAULT '',
                        sync_mode           text NOT NULL DEFAULT '',
                        view_table          text NOT NULL DEFAULT '',
                        graph_name          text NOT NULL DEFAULT '',
                        task_id             text NOT NULL DEFAULT '',
                        phase_times         jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        stats               jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at          timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_build_runs_domain_version
                        ON {sch}.build_runs(domain_id, version, started_at DESC)
                    """
                )
            self._build_runs_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create build_runs table — "
                "run `make bootstrap-lakebase` as the schema owner to apply the migration: %s",
                exc,
            )
            return False

    def record_build_run(self, folder: str, entry: BuildRunEntry) -> None:
        if not self._ensure_build_runs_table():
            return
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.build_runs
                        (domain_id, version, build_kind, status, message,
                         error, started_at, finished_at, duration_s,
                         triple_count, entity_count, relationship_count,
                         sql_chars, graph_engine, sync_mode, view_table,
                         graph_name, task_id, phase_times, stats)
                    SELECT d.id, %s, %s, %s, %s, %s,
                           COALESCE(%s::timestamptz, now()),
                           %s::timestamptz, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s::jsonb, %s::jsonb
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    """,
                    (
                        str(entry.get("version", "")),
                        str(entry.get("build_kind", "session")),
                        str(entry.get("status", "")),
                        str(entry.get("message", "") or ""),
                        str(entry.get("error", "") or ""),
                        entry.get("started_at"),
                        entry.get("finished_at"),
                        float(entry.get("duration_s", 0) or 0),
                        int(entry.get("triple_count", 0) or 0),
                        int(entry.get("entity_count", 0) or 0),
                        int(entry.get("relationship_count", 0) or 0),
                        int(entry.get("sql_chars", 0) or 0),
                        str(entry.get("graph_engine", "") or ""),
                        str(entry.get("sync_mode", "") or ""),
                        str(entry.get("view_table", "") or ""),
                        str(entry.get("graph_name", "") or ""),
                        str(entry.get("task_id", "") or ""),
                        json.dumps(entry.get("phase_times") or {}),
                        json.dumps(entry.get("stats") or {}),
                        self._registry(),
                        folder,
                    ),
                )
                if cur.rowcount == 0:
                    logger.warning(
                        "record_build_run(%s): no domain row matched — "
                        "build trace not stored",
                        folder,
                    )
        except Exception as exc:  # noqa: BLE001
            logger.warning("record_build_run(%s) failed: %s", folder, exc)

    @staticmethod
    def _build_run_row_to_entry(r: Dict[str, Any]) -> BuildRunEntry:
        return {
            "id": int(r.get("id") or 0),
            "version": r["version"],
            "build_kind": r["build_kind"],
            "status": r["status"],
            "message": r["message"] or "",
            "error": r["error"] or "",
            "started_at": (
                r["started_at"].isoformat() if r.get("started_at") else ""
            ),
            "finished_at": (
                r["finished_at"].isoformat() if r.get("finished_at") else ""
            ),
            "duration_s": float(r["duration_s"] or 0),
            "triple_count": int(r["triple_count"] or 0),
            "entity_count": int(r["entity_count"] or 0),
            "relationship_count": int(r["relationship_count"] or 0),
            "sql_chars": int(r["sql_chars"] or 0),
            "graph_engine": r["graph_engine"] or "",
            "sync_mode": r["sync_mode"] or "",
            "view_table": r["view_table"] or "",
            "graph_name": r["graph_name"] or "",
            "task_id": r["task_id"] or "",
            "phase_times": dict(r["phase_times"] or {}),
            "stats": dict(r["stats"] or {}),
        }

    def stamp_last_build(
        self, folder: str, version: str, ts: str
    ) -> Tuple[bool, str]:
        """Targeted UPDATE for ``domain_versions.last_build``.

        Avoids a full read + re-write of the JSONB blobs: only the scalar
        ``last_build`` column is touched.  Returns ``(ok, message)``.
        """
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self._q(self._schema)}.domain_versions v
                       SET last_build = %s
                      FROM {self._q(self._schema)}.domains d
                     WHERE d.id         = v.domain_id
                       AND d.registry_id = %s
                       AND d.folder     = %s
                       AND v.version    = %s
                    """,
                    (ts, self._registry(), folder, version),
                )
            return True, ""
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def load_build_runs(
        self,
        folder: str,
        *,
        version: Optional[str] = None,
        limit: int = 100,
    ) -> List[BuildRunEntry]:
        if not self._ensure_build_runs_table():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            clauses = ["d.registry_id = %s", "d.folder = %s"]
            params: List[Any] = [self._registry(), folder]
            if version:
                clauses.append("b.version = %s")
                params.append(version)
            params.append(int(limit))
            where = " AND ".join(clauses)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT b.id, b.version, b.build_kind, b.status, b.message,
                           b.error, b.started_at, b.finished_at, b.duration_s,
                           b.triple_count, b.entity_count, b.relationship_count,
                           b.sql_chars, b.graph_engine, b.sync_mode,
                           b.view_table, b.graph_name, b.task_id,
                           b.phase_times, b.stats
                    FROM {sch}.build_runs b
                    JOIN {sch}.domains d ON d.id = b.domain_id
                    WHERE {where}
                    ORDER BY b.started_at DESC, b.id DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._build_run_row_to_entry(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_build_runs(%s) failed: %s", folder, exc)
            return []

    @staticmethod
    def _empty_analytics() -> Dict[str, Any]:
        return {
            "total_runs": 0,
            "success_runs": 0,
            "failed_runs": 0,
            "success_rate": 0.0,
            "avg_duration_s": 0.0,
            "min_duration_s": 0.0,
            "max_duration_s": 0.0,
            "last_triple_count": 0,
            "active_build": None,
            "per_version": [],
        }

    def build_analytics(
        self, folder: str, *, version: Optional[str] = None
    ) -> Dict[str, Any]:
        if not self._ensure_build_runs_table():
            return self._empty_analytics()
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            scope = ["d.registry_id = %s", "d.folder = %s"]
            scope_params: List[Any] = [self._registry(), folder]
            if version:
                scope.append("b.version = %s")
                scope_params.append(version)
            where = " AND ".join(scope)

            result = self._empty_analytics()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                # Headline aggregates.
                cur.execute(
                    f"""
                    SELECT
                        count(*)                                   AS total_runs,
                        count(*) FILTER (WHERE b.status = 'success') AS success_runs,
                        count(*) FILTER (WHERE b.status <> 'success') AS failed_runs,
                        COALESCE(avg(b.duration_s)
                                 FILTER (WHERE b.status = 'success'), 0) AS avg_duration_s,
                        COALESCE(min(b.duration_s)
                                 FILTER (WHERE b.status = 'success'), 0) AS min_duration_s,
                        COALESCE(max(b.duration_s)
                                 FILTER (WHERE b.status = 'success'), 0) AS max_duration_s
                    FROM {sch}.build_runs b
                    JOIN {sch}.domains d ON d.id = b.domain_id
                    WHERE {where}
                    """,
                    tuple(scope_params),
                )
                agg = cur.fetchone() or {}
                total = int(agg.get("total_runs") or 0)
                success = int(agg.get("success_runs") or 0)
                result.update(
                    {
                        "total_runs": total,
                        "success_runs": success,
                        "failed_runs": int(agg.get("failed_runs") or 0),
                        "success_rate": (success / total) if total else 0.0,
                        "avg_duration_s": float(agg.get("avg_duration_s") or 0),
                        "min_duration_s": float(agg.get("min_duration_s") or 0),
                        "max_duration_s": float(agg.get("max_duration_s") or 0),
                    }
                )

                # Active build = latest successful run in scope.
                cur.execute(
                    f"""
                    SELECT b.version, b.build_kind, b.status, b.message,
                           b.error, b.started_at, b.finished_at, b.duration_s,
                           b.triple_count, b.entity_count, b.relationship_count,
                           b.sql_chars, b.graph_engine, b.sync_mode,
                           b.view_table, b.graph_name, b.task_id,
                           b.phase_times, b.stats
                    FROM {sch}.build_runs b
                    JOIN {sch}.domains d ON d.id = b.domain_id
                    WHERE {where} AND b.status = 'success'
                    ORDER BY b.started_at DESC, b.id DESC
                    LIMIT 1
                    """,
                    tuple(scope_params),
                )
                active = cur.fetchone()
                if active:
                    entry = self._build_run_row_to_entry(active)
                    result["active_build"] = entry
                    result["last_triple_count"] = entry["triple_count"]

                # Per-version rollup (newest version first).
                cur.execute(
                    f"""
                    SELECT b.version,
                           count(*) AS total_runs,
                           count(*) FILTER (WHERE b.status = 'success')
                               AS success_runs,
                           max(b.started_at) AS last_run,
                           (array_agg(b.status ORDER BY b.started_at DESC,
                                      b.id DESC))[1] AS last_status,
                           (array_agg(b.triple_count ORDER BY b.started_at DESC,
                                      b.id DESC))[1] AS last_triple_count
                    FROM {sch}.build_runs b
                    JOIN {sch}.domains d ON d.id = b.domain_id
                    WHERE {where}
                    GROUP BY b.version
                    ORDER BY max(b.started_at) DESC
                    """,
                    tuple(scope_params),
                )
                result["per_version"] = [
                    {
                        "version": r["version"],
                        "total_runs": int(r["total_runs"] or 0),
                        "success_runs": int(r["success_runs"] or 0),
                        "last_status": r["last_status"] or "",
                        "last_triple_count": int(r["last_triple_count"] or 0),
                        "last_run": (
                            r["last_run"].isoformat() if r.get("last_run") else ""
                        ),
                    }
                    for r in cur.fetchall()
                ]
            return result
        except Exception as exc:  # noqa: BLE001
            logger.debug("build_analytics(%s) failed: %s", folder, exc)
            return self._empty_analytics()

    # ------------------------------------------------------------------
    # Review / validation audit log
    # ------------------------------------------------------------------

    def _ensure_review_events_table(self) -> bool:
        """Lazily create ``domain_review_events`` (+ index) if missing.

        Self-heals deployments created before the review/validation audit
        log existed — same ownership-safe pattern as
        :meth:`_ensure_build_runs_table`: check first, only attempt DDL
        when the table is genuinely absent. Best-effort: on failure it
        logs and returns ``False`` so callers no-op rather than breaking
        a transition.
        """
        if self._review_events_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s "
                    "AND table_name = 'domain_review_events'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._review_events_ready = True
                    return True
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.domain_review_events (
                        id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                        domain_id       uuid NOT NULL
                                        REFERENCES {sch}.domains(id)
                                        ON DELETE CASCADE,
                        version         text NOT NULL,
                        actor           text NOT NULL,
                        action          text NOT NULL,
                        from_status     text NOT NULL DEFAULT '',
                        to_status       text NOT NULL DEFAULT '',
                        comment         text NOT NULL DEFAULT '',
                        meta            jsonb NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at      timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_review_events_domain_version
                        ON {sch}.domain_review_events
                           (domain_id, version, created_at)
                    """
                )
            self._review_events_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create domain_review_events table — "
                "run `make bootstrap-lakebase` as the schema owner to "
                "apply the migration: %s",
                exc,
            )
            return False

    def record_review_event(
        self,
        folder: str,
        version: str,
        actor: str,
        action: str,
        *,
        from_status: str = "",
        to_status: str = "",
        comment: str = "",
        meta: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str]:
        if not self._ensure_review_events_table():
            return False, "review audit log unavailable"
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.domain_review_events
                        (domain_id, version, actor, action, from_status,
                         to_status, comment, meta)
                    SELECT d.id, %s, %s, %s, %s, %s, %s, %s::jsonb
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    """,
                    (
                        version,
                        actor or "",
                        action or "",
                        from_status or "",
                        to_status or "",
                        comment or "",
                        json.dumps(meta or {}),
                        self._registry(),
                        folder,
                    ),
                )
                if cur.rowcount == 0:
                    return False, f"Domain '{folder}' not found"
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "record_review_event(%s/%s) failed: %s", folder, version, exc
            )
            return False, str(exc)

    @staticmethod
    def _review_row_to_event(r: Dict[str, Any]) -> ReviewEvent:
        return {
            "id": str(r.get("id") or ""),
            "folder": r.get("folder", "") or "",
            "version": r["version"],
            "actor": r["actor"] or "",
            "action": r["action"] or "",
            "from_status": r["from_status"] or "",
            "to_status": r["to_status"] or "",
            "comment": r["comment"] or "",
            "meta": dict(r["meta"] or {}),
            "created_at": (
                r["created_at"].isoformat() if r.get("created_at") else ""
            ),
        }

    def list_review_events(
        self, folder: str, version: Optional[str] = None
    ) -> List[ReviewEvent]:
        if not self._ensure_review_events_table():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            clauses = ["d.registry_id = %s", "d.folder = %s"]
            params: List[Any] = [self._registry(), folder]
            if version:
                clauses.append("e.version = %s")
                params.append(version)
            where = " AND ".join(clauses)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT e.id, d.folder, e.version, e.actor, e.action,
                           e.from_status, e.to_status, e.comment, e.meta,
                           e.created_at
                    FROM {sch}.domain_review_events e
                    JOIN {sch}.domains d ON d.id = e.domain_id
                    WHERE {where}
                    ORDER BY e.created_at ASC, e.id ASC
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._review_row_to_event(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_review_events(%s) failed: %s", folder, exc)
            return []

    def list_all_review_events(self) -> List[ReviewEvent]:
        if not self._ensure_review_events_table():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT e.id, d.folder, e.version, e.actor, e.action,
                           e.from_status, e.to_status, e.comment, e.meta,
                           e.created_at
                    FROM {sch}.domain_review_events e
                    JOIN {sch}.domains d ON d.id = e.domain_id
                    WHERE d.registry_id = %s
                    ORDER BY e.created_at ASC, e.id ASC
                    """,
                    (self._registry(),),
                )
                rows = cur.fetchall()
            return [self._review_row_to_event(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_all_review_events failed: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Collaborative comments + tasks
    # ------------------------------------------------------------------

    def _ensure_collab_tables(self) -> bool:
        """Lazily create ``domain_comments`` + ``domain_tasks`` (+ indexes).

        Self-heals deployments created before the collaborative comments
        and tasks feature existed — same ownership-safe pattern as
        :meth:`_ensure_review_events_table`. Best-effort: on failure it
        logs and returns ``False`` so callers no-op rather than breaking.
        """
        if self._collab_tables_ready:
            return True
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = %s "
                    "AND table_name = 'domain_comments'",
                    (self._schema,),
                )
                if cur.fetchone():
                    self._collab_tables_ready = True
                    return True
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.domain_comments (
                        id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                        domain_id   uuid NOT NULL
                                    REFERENCES {sch}.domains(id)
                                    ON DELETE CASCADE,
                        version     text NOT NULL,
                        parent_id   uuid
                                    REFERENCES {sch}.domain_comments(id)
                                    ON DELETE CASCADE,
                        author      text NOT NULL,
                        body        text NOT NULL DEFAULT '',
                        resolved    boolean NOT NULL DEFAULT false,
                        created_at  timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_domain_comments_lookup
                        ON {sch}.domain_comments (domain_id, version, created_at)
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {sch}.domain_tasks (
                        id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                        domain_id   uuid NOT NULL
                                    REFERENCES {sch}.domains(id)
                                    ON DELETE CASCADE,
                        version     text NOT NULL,
                        assignee    text NOT NULL,
                        created_by  text NOT NULL,
                        title       text NOT NULL,
                        description text NOT NULL DEFAULT '',
                        status      text NOT NULL DEFAULT 'open',
                        due_date    date,
                        comment_id  uuid
                                    REFERENCES {sch}.domain_comments(id)
                                    ON DELETE SET NULL,
                        created_at  timestamptz NOT NULL DEFAULT now(),
                        updated_at  timestamptz NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_domain_tasks_assignee
                        ON {sch}.domain_tasks (lower(assignee), status)
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_domain_tasks_domain
                        ON {sch}.domain_tasks (domain_id, version)
                    """
                )
            self._collab_tables_ready = True
            return True
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "could not create domain_comments/domain_tasks tables — "
                "run `make bootstrap-lakebase` as the schema owner to "
                "apply the migration: %s",
                exc,
            )
            return False

    @staticmethod
    def _comment_row_to_dict(
        r: Dict[str, Any], folder: str = ""
    ) -> DomainComment:
        return {
            "id": str(r.get("id") or ""),
            "folder": r.get("folder", folder) or folder,
            "version": r["version"],
            "parent_id": str(r["parent_id"]) if r.get("parent_id") else "",
            "author": r["author"] or "",
            "body": r["body"] or "",
            "resolved": bool(r["resolved"]),
            "created_at": (
                r["created_at"].isoformat() if r.get("created_at") else ""
            ),
        }

    def insert_comment(
        self,
        folder: str,
        version: str,
        *,
        author: str,
        body: str,
        parent_id: Optional[str] = None,
    ) -> Optional[DomainComment]:
        if not self._ensure_collab_tables():
            return None
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.domain_comments
                        (domain_id, version, parent_id, author, body)
                    SELECT d.id, %s, %s, %s, %s
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    RETURNING id, version, parent_id, author, body,
                              resolved, created_at
                    """,
                    (
                        version,
                        parent_id or None,
                        author or "",
                        body or "",
                        self._registry(),
                        folder,
                    ),
                )
                row = cur.fetchone()
            if not row:
                return None
            return self._comment_row_to_dict(row, folder)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "insert_comment(%s/%s) failed: %s", folder, version, exc
            )
            return None

    def list_comments(
        self,
        folder: str,
        version: Optional[str] = None,
        *,
        include_resolved: bool = True,
    ) -> List[DomainComment]:
        if not self._ensure_collab_tables():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            clauses = ["d.registry_id = %s", "d.folder = %s"]
            params: List[Any] = [self._registry(), folder]
            if version:
                clauses.append("c.version = %s")
                params.append(version)
            if not include_resolved:
                clauses.append("c.resolved = false")
            where = " AND ".join(clauses)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT c.id, d.folder, c.version, c.parent_id,
                           c.author, c.body, c.resolved, c.created_at
                    FROM {sch}.domain_comments c
                    JOIN {sch}.domains d ON d.id = c.domain_id
                    WHERE {where}
                    ORDER BY c.created_at ASC, c.id ASC
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._comment_row_to_dict(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_comments(%s) failed: %s", folder, exc)
            return []

    def resolve_comment(
        self, folder: str, comment_id: str, *, resolved: bool = True
    ) -> Tuple[bool, str]:
        if not self._ensure_collab_tables():
            return False, "comments backend unavailable"
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {sch}.domain_comments c
                    SET resolved = %s
                    FROM {sch}.domains d
                    WHERE c.domain_id = d.id
                      AND d.registry_id = %s AND d.folder = %s
                      AND c.id = %s
                    """,
                    (resolved, self._registry(), folder, comment_id),
                )
                if cur.rowcount == 0:
                    return False, "Comment not found"
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "resolve_comment(%s/%s) failed: %s", folder, comment_id, exc
            )
            return False, str(exc)

    @staticmethod
    def _task_row_to_dict(r: Dict[str, Any], folder: str = "") -> DomainTask:
        return {
            "id": str(r.get("id") or ""),
            "folder": r.get("folder", folder) or folder,
            "version": r["version"],
            "assignee": r["assignee"] or "",
            "created_by": r["created_by"] or "",
            "title": r["title"] or "",
            "description": r["description"] or "",
            "status": r["status"] or "open",
            "due_date": r["due_date"].isoformat() if r.get("due_date") else "",
            "comment_id": str(r["comment_id"]) if r.get("comment_id") else "",
            "created_at": (
                r["created_at"].isoformat() if r.get("created_at") else ""
            ),
            "updated_at": (
                r["updated_at"].isoformat() if r.get("updated_at") else ""
            ),
        }

    def insert_task(
        self,
        folder: str,
        version: str,
        *,
        assignee: str,
        created_by: str,
        title: str,
        description: str = "",
        due_date: Optional[str] = None,
        comment_id: Optional[str] = None,
    ) -> Optional[DomainTask]:
        if not self._ensure_collab_tables():
            return None
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    INSERT INTO {sch}.domain_tasks
                        (domain_id, version, assignee, created_by, title,
                         description, due_date, comment_id)
                    SELECT d.id, %s, %s, %s, %s, %s, %s, %s
                    FROM {sch}.domains d
                    WHERE d.registry_id = %s AND d.folder = %s
                    RETURNING id, version, assignee, created_by, title,
                              description, status, due_date, comment_id,
                              created_at, updated_at
                    """,
                    (
                        version,
                        assignee or "",
                        created_by or "",
                        title or "",
                        description or "",
                        due_date or None,
                        comment_id or None,
                        self._registry(),
                        folder,
                    ),
                )
                row = cur.fetchone()
            if not row:
                return None
            return self._task_row_to_dict(row, folder)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "insert_task(%s/%s) failed: %s", folder, version, exc
            )
            return None

    def list_tasks(
        self, folder: str, version: Optional[str] = None
    ) -> List[DomainTask]:
        if not self._ensure_collab_tables():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            clauses = ["d.registry_id = %s", "d.folder = %s"]
            params: List[Any] = [self._registry(), folder]
            if version:
                clauses.append("t.version = %s")
                params.append(version)
            where = " AND ".join(clauses)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT t.id, d.folder, t.version, t.assignee, t.created_by,
                           t.title, t.description, t.status, t.due_date,
                           t.comment_id, t.created_at, t.updated_at
                    FROM {sch}.domain_tasks t
                    JOIN {sch}.domains d ON d.id = t.domain_id
                    WHERE {where}
                    ORDER BY t.created_at DESC, t.id DESC
                    """,
                    tuple(params),
                )
                rows = cur.fetchall()
            return [self._task_row_to_dict(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_tasks(%s) failed: %s", folder, exc)
            return []

    def list_tasks_for_assignee(self, assignee: str) -> List[DomainTask]:
        if not self._ensure_collab_tables():
            return []
        try:
            psycopg, dict_row = _require_psycopg()
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT t.id, d.folder, t.version, t.assignee, t.created_by,
                           t.title, t.description, t.status, t.due_date,
                           t.comment_id, t.created_at, t.updated_at
                    FROM {sch}.domain_tasks t
                    JOIN {sch}.domains d ON d.id = t.domain_id
                    WHERE d.registry_id = %s AND lower(t.assignee) = lower(%s)
                    ORDER BY t.created_at DESC, t.id DESC
                    """,
                    (self._registry(), assignee or ""),
                )
                rows = cur.fetchall()
            return [self._task_row_to_dict(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            logger.debug("list_tasks_for_assignee(%s) failed: %s", assignee, exc)
            return []

    def update_task_status(
        self, folder: str, task_id: str, status: str
    ) -> Tuple[bool, str]:
        if not self._ensure_collab_tables():
            return False, "tasks backend unavailable"
        try:
            sch = self._q(self._schema)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {sch}.domain_tasks t
                    SET status = %s, updated_at = now()
                    FROM {sch}.domains d
                    WHERE t.domain_id = d.id
                      AND d.registry_id = %s AND d.folder = %s
                      AND t.id = %s
                    """,
                    (status, self._registry(), folder, task_id),
                )
                if cur.rowcount == 0:
                    return False, "Task not found"
            return True, ""
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "update_task_status(%s/%s) failed: %s", folder, task_id, exc
            )
            return False, str(exc)

    # ------------------------------------------------------------------
    # Global config
    # ------------------------------------------------------------------

    def load_global_config(self) -> Dict[str, Any]:
        try:
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT config FROM {self._q(self._schema)}.global_config
                    WHERE registry_id = %s
                    """,
                    (self._registry(),),
                )
                row = cur.fetchone()
            if not row:
                return {}
            data = dict(row["config"] or {})
            # ``schedules`` lives in its own table on Lakebase; ``schedule_history``
            # in ``schedule_runs``. Strip both so the JSONB blob is the single
            # source of truth only for instance-wide settings.
            data.pop(_SCHEDULES_KEY, None)
            data.pop("schedule_history", None)
            return data
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_global_config failed: %s", exc)
            return {}

    def save_global_config(self, updates: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            data = self.load_global_config()
            data["version"] = data.get("version", 1)
            sanitized_updates = {
                k: v
                for k, v in (updates or {}).items()
                if k not in (_SCHEDULES_KEY, "schedule_history")
            }
            data.pop(_SCHEDULES_KEY, None)
            data.pop("schedule_history", None)
            data.update(sanitized_updates)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._q(self._schema)}.global_config
                        (registry_id, config)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (registry_id)
                    DO UPDATE SET config = EXCLUDED.config,
                                  updated_at = now()
                    """,
                    (self._registry(), json.dumps(data)),
                )
            return True, "Global configuration saved"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def domain_folder_id(self, folder: str) -> Optional[str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id FROM {self._q(self._schema)}.domains
                    WHERE registry_id = %s AND folder = %s
                    """,
                    (self._registry(), folder),
                )
                row = cur.fetchone()
            return str(row[0]) if row else None
        except Exception:  # noqa: BLE001
            return None

    def describe(self) -> Dict[str, Any]:
        c = self._cfg
        try:
            host = self._auth.host
            bound_db = self._auth.database
            user = self._auth.user
        except Exception:  # noqa: BLE001
            host = bound_db = user = ""
        return {
            "backend": self.backend,
            "cache_key": self.cache_key,
            "schema": self._schema,
            "host": host,
            "database": bound_db,
            "database_override": self._database,
            "effective_database": self._database or bound_db,
            "user": user,
            "volume_catalog": c.catalog,
            "volume_schema": c.schema,
            "volume_volume": c.volume,
        }

    def table_row_counts(self, tables: Tuple[str, ...]) -> Dict[str, int]:
        """Return ``{table_name: row_count}`` for tables in this schema.

        Tables that do not exist (schema not yet initialised, or table
        renamed) are reported as ``0``. Connection / permission /
        unknown errors are *raised* — silent zeros mask broken
        deployments (e.g. service principal missing ``USAGE`` on the
        schema) and are surfaced by the admin UI. Whitelist-only:
        *tables* is matched against :data:`_KNOWN_TABLES` to keep the
        dynamic SQL safe.
        """
        result: Dict[str, int] = {t: 0 for t in tables}
        wanted = [t for t in tables if t in _KNOWN_TABLES]
        if not wanted:
            return result
        with self._connect() as conn, conn.cursor() as cur:
            # First, find which of the requested tables actually
            # exist — that way we never blow up on partial schemas
            # (e.g. mid-migration or before initialise()).
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_name = ANY(%s)
                """,
                (self._schema, wanted),
            )
            present = {row[0] for row in cur.fetchall()}
            for tname in wanted:
                if tname not in present:
                    continue
                cur.execute(
                    f"SELECT count(*) FROM "
                    f"{self._q(self._schema)}.{self._q(tname)}"
                )
                row = cur.fetchone()
                result[tname] = int(row[0]) if row else 0
        return result

    # ------------------------------------------------------------------
    # Connection plumbing
    # ------------------------------------------------------------------

    def _connect(self):
        """Acquire a Lakebase connection from the shared process-wide pool.

        Returns a context manager: callers keep the existing
        ``with self._connect() as conn`` idiom unchanged. On clean
        exit the connection goes back to the pool; on exception it
        is discarded so that broken sessions are never reused.

        The pool itself owns cold-start retry and OAuth token
        rotation — see :class:`_LakebasePool._open_one`.
        """
        return _get_pool(
            self._auth, self._schema, self._database
        ).connection()

    def _registry(self) -> str:
        if self._registry_id is None:
            self._registry_id = self._fetch_registry_id() or self._ensure_registry_row()
        return self._registry_id

    def _fetch_registry_id(self) -> Optional[str]:
        """Find the singleton registry row for this Lakebase schema.

        Identity model: **one Postgres schema = one OntoBricks
        registry**. The ``registries.name`` is the schema name, so two
        apps that share a Lakebase resource binding (instance +
        database + schema) naturally see the same registry. The Volume
        triplet (``catalog/schema/volume``) is no longer part of the
        identity — it's just where domain-scoped binary artefacts
        (``documents/`` uploads) live for whichever app is currently
        reading.

        Backward-compat: pre-existing schemas migrated under the legacy
        ``"<catalog>.<schema>.<volume>"`` naming are *adopted* on first
        access. If no row matches the new schema-based name but exactly
        one legacy row is present, we transparently rename it so the
        next lookup is O(1). When more than one legacy row is present,
        we adopt the oldest by ``created_at`` and log a warning so the
        admin can clean up duplicates.
        """
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT id FROM {self._q(self._schema)}.registries "
                    "WHERE name = %s",
                    (self._registry_name(),),
                )
                row = cur.fetchone()
                if row:
                    return str(row[0])
                # No row keyed by the new (schema-based) name. Try to
                # adopt a legacy row. We pick the oldest row to be
                # deterministic when more than one is present.
                cur.execute(
                    f"""
                    SELECT id, name, count(*) OVER () AS total
                    FROM {self._q(self._schema)}.registries
                    ORDER BY created_at ASC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if not row:
                    return None
                legacy_id, legacy_name, total = row
                if total > 1:
                    logger.warning(
                        "Lakebase schema %r contains %d registry rows; "
                        "adopting the oldest (%s) under the new "
                        "schema-keyed name. Drop the unused rows when "
                        "you are sure they are no longer needed.",
                        self._schema,
                        total,
                        legacy_name,
                    )
                else:
                    logger.info(
                        "Adopting legacy Lakebase registry row %r as "
                        "the singleton for schema %r.",
                        legacy_name,
                        self._schema,
                    )
                cur.execute(
                    f"UPDATE {self._q(self._schema)}.registries "
                    "SET name = %s, updated_at = now() WHERE id = %s",
                    (self._registry_name(), legacy_id),
                )
                return str(legacy_id)
        except Exception:  # noqa: BLE001
            return None

    def _ensure_registry_row(self) -> str:
        c = self._cfg
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._q(self._schema)}.registries
                    (name, catalog, schema, volume)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name)
                DO UPDATE SET catalog    = EXCLUDED.catalog,
                              schema     = EXCLUDED.schema,
                              volume     = EXCLUDED.volume,
                              updated_at = now()
                RETURNING id
                """,
                (self._registry_name(), c.catalog, c.schema, c.volume),
            )
            row = cur.fetchone()
        return str(row[0])

    def _registry_name(self) -> str:
        """Registry identity for the Lakebase backend.

        The Postgres schema *is* the registry namespace. Pointing two
        apps at the same Lakebase ``(instance, database, schema)``
        triple makes them share the registry; pointing them at
        different schemas isolates them. The Volume triplet from
        :class:`RegistryCfg` is intentionally *not* part of the
        identity here — Volume bindings only matter for domain-scoped
        binary artefacts (``documents/`` uploads) and can differ per
        app without forking the metadata.
        """
        return self._schema

    def _apply_ddl(self) -> None:
        ddl_path = os.path.join(os.path.dirname(__file__), _DDL_FILENAME)
        with open(ddl_path, "r", encoding="utf-8") as fh:
            ddl = fh.read()
        ddl = ddl.replace(_SCHEMA_TOKEN, self._schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(ddl)

    def _scrub_global_config_legacy_keys(self) -> None:
        """Remove ``schedules`` / ``schedule_history`` from the JSONB blob.

        Both keys belong to dedicated tables on Lakebase (``schedules`` and
        ``schedule_runs``). They used to leak into ``global_config.config``
        through the Volume → Lakebase migration path, which fed the entire
        Volume ``.global_config.json`` blob — schedules included — into
        ``save_global_config``. The duplicated state was harmless at read
        time (callers go through ``load_schedules``) but caused the JSONB
        blob to grow unbounded and confused operators inspecting the row
        directly. This one-shot ``UPDATE`` runs at every ``initialize()``
        so existing deployments self-heal on next app start. The ``WHERE``
        clause keeps the scrub a no-op once the blob is clean.
        """
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self._q(self._schema)}.global_config
                    SET config = (config - 'schedules') - 'schedule_history',
                        updated_at = now()
                    WHERE config ? 'schedules' OR config ? 'schedule_history'
                    """
                )
                scrubbed = cur.rowcount or 0
            if scrubbed:
                logger.info(
                    "Scrubbed legacy schedules/schedule_history keys from "
                    "global_config (%d row(s)) — Lakebase keeps schedules "
                    "in the dedicated 'schedules' table.",
                    scrubbed,
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Could not scrub legacy keys from global_config: %s", exc
            )

    @staticmethod
    def _q(name: str) -> str:
        """Quote an SQL identifier safely (validated at construction time)."""
        return f'"{name}"'
