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

Authentication:
- Connection params (host/port/db/user) come from ``PG*`` env vars
  injected by the Apps ``database`` resource binding.
- The Postgres password is a short-lived OAuth token minted by
  :class:`back.core.databricks.LakebaseAuth`.

Cold start (Autoscaling tier):
- Initial calls retry with exponential backoff on SQLSTATE ``57P03``
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

from ..base import DomainSummary, RegistryStore, ScheduleHistoryEntry, StoreError

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
            "``uv sync --extra lakebase`` (or ``pip install .[lakebase]``) or "
            "set REGISTRY_BACKEND=volume."
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
        max_size: int = _POOL_MAX_SIZE,
        max_lifetime: float = _POOL_MAX_LIFETIME_S,
    ) -> None:
        self._auth = auth
        self._schema = schema
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
                conn = psycopg.connect(
                    autocommit=True,
                    **self._auth.kwargs(application_name="ontobricks-registry"),
                )
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
_pools: Dict[Tuple[str, str, str, str, str, str], _LakebasePool] = {}


def _safe_attr(obj: Any, name: str) -> str:
    """Read an attribute that may raise ``ValidationError`` lazily."""
    try:
        return str(getattr(obj, name, "") or "")
    except Exception:  # noqa: BLE001
        return ""


def _get_pool(auth: Any, schema: str) -> _LakebasePool:
    """Return (and lazily create) the shared pool for *auth* + *schema*."""
    key = (
        _safe_attr(auth, "host"),
        _safe_attr(auth, "port"),
        _safe_attr(auth, "database"),
        _safe_attr(auth, "user"),
        _safe_attr(auth, "instance_name"),
        schema,
    )
    with _pools_lock:
        pool = _pools.get(key)
        if pool is None:
            pool = _LakebasePool(auth=auth, schema=schema)
            _pools[key] = pool
            logger.info(
                "Created Lakebase connection pool for %s/%s (schema=%s, max_size=%d)",
                key[0],
                key[2],
                schema,
                _POOL_MAX_SIZE,
            )
        return pool


class LakebaseRegistryStore(RegistryStore):
    """Postgres-backed registry store. Optional backend."""

    def __init__(self, *, registry_cfg, schema: str = "ontobricks_registry"):
        if not _SAFE_SCHEMA_RE.match(schema or ""):
            raise InfrastructureError(
                f"Invalid Lakebase schema name {schema!r}; must match "
                f"[a-zA-Z_][a-zA-Z0-9_]*"
            )
        self._cfg = registry_cfg
        self._schema = schema
        self._auth = get_lakebase_auth()
        self._registry_id: Optional[str] = None  # cached after initialize()

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
        # registry-level TTL cache automatically.
        return f"lakebase:{self._auth.host}:{self._schema}:{c.catalog}.{c.schema}.{c.volume}"

    @property
    def schema(self) -> str:
        return self._schema

    def is_initialized(self) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT to_regclass(%s) IS NOT NULL",
                    (f"{self._schema}.registries",),
                )
                ok = bool(cur.fetchone()[0])
            if ok and self._registry_id is None:
                self._registry_id = self._fetch_registry_id()
            return ok and self._registry_id is not None
        except Exception as exc:  # noqa: BLE001
            logger.debug("Lakebase is_initialized probe failed: %s", exc)
            return False

    def initialize(self, *, client: Any = None) -> Tuple[bool, str]:
        del client  # not used: Lakebase instance is provisioned out of band
        try:
            self._apply_ddl()
            self._registry_id = self._ensure_registry_row()
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")  # wake probe
            logger.info(
                "Lakebase registry initialised (schema=%s, host=%s)",
                self._schema,
                self._auth.host,
            )
            return True, (
                f"Lakebase registry initialized at "
                f"{self._auth.host}/{self._auth.database} (schema={self._schema})"
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
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(
                        f"""
                        SELECT d.id, d.folder, d.description, d.base_uri
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
                        SELECT v.domain_id, v.version, v.mcp_enabled,
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
                        "versions": [
                            {
                                "version": v["version"],
                                "active": bool(v["mcp_enabled"]),
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
            psycopg, dict_row = _require_psycopg()
            with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT v.info, v.ontology, v.assignment, v.design_layout,
                           v.metadata, v.version, v.mcp_enabled,
                           v.last_update, v.last_build
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
            info = data.get("info", {}) or {}
            ver_blob = (data.get("versions") or {}).get(version, {}) or {}
            ontology = ver_blob.get("ontology", data.get("ontology", {})) or {}
            assignment = ver_blob.get("assignment", data.get("assignment", {})) or {}
            design = ver_blob.get("design_layout", data.get("design_layout", {})) or {}
            metadata = ver_blob.get("metadata", data.get("metadata", {})) or {}
            mcp_enabled = bool(info.get("mcp_enabled"))
            last_update = info.get("last_update", "") or ""
            last_build = info.get("last_build", "") or ""
            description = info.get("description", "") or ""
            base_uri = ontology.get("base_uri", "") or ""

            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._q(self._schema)}.domains
                        (registry_id, folder, description, base_uri)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (registry_id, folder)
                    DO UPDATE SET description = EXCLUDED.description,
                                  base_uri    = EXCLUDED.base_uri,
                                  updated_at  = now()
                    RETURNING id
                    """,
                    (self._registry(), folder, description, base_uri),
                )
                domain_id = cur.fetchone()[0]
                cur.execute(
                    f"""
                    INSERT INTO {self._q(self._schema)}.domain_versions
                        (domain_id, version, info, ontology, assignment,
                         design_layout, metadata, mcp_enabled,
                         last_update, last_build)
                    VALUES (%s, %s, %s::jsonb, %s::jsonb, %s::jsonb,
                            %s::jsonb, %s::jsonb, %s, %s, %s)
                    ON CONFLICT (domain_id, version)
                    DO UPDATE SET info          = EXCLUDED.info,
                                  ontology      = EXCLUDED.ontology,
                                  assignment    = EXCLUDED.assignment,
                                  design_layout = EXCLUDED.design_layout,
                                  metadata      = EXCLUDED.metadata,
                                  mcp_enabled   = EXCLUDED.mcp_enabled,
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
            data.pop("schedule_history", None)
            return data
        except Exception as exc:  # noqa: BLE001
            logger.debug("load_global_config failed: %s", exc)
            return {}

    def save_global_config(self, updates: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            data = self.load_global_config()
            data["version"] = data.get("version", 1)
            data.pop("schedule_history", None)
            data.update(updates)
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
            db = self._auth.database
            user = self._auth.user
        except Exception as exc:  # noqa: BLE001
            host = db = user = ""
        return {
            "backend": self.backend,
            "cache_key": self.cache_key,
            "schema": self._schema,
            "host": host,
            "database": db,
            "user": user,
            "volume_catalog": c.catalog,
            "volume_schema": c.schema,
            "volume_volume": c.volume,
        }

    def table_row_counts(self, tables: Tuple[str, ...]) -> Dict[str, int]:
        """Return ``{table_name: row_count}`` for tables in this schema.

        Tables that do not exist (schema not yet initialised, or table
        renamed) are reported as ``0`` rather than raising — this lets
        the admin UI render a consistent inventory grid even on an
        empty Lakebase. Whitelist-only: *tables* is matched against
        :data:`_KNOWN_TABLES` to keep the dynamic SQL safe.
        """
        result: Dict[str, int] = {t: 0 for t in tables}
        wanted = [t for t in tables if t in _KNOWN_TABLES]
        if not wanted:
            return result
        try:
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
        except Exception as exc:  # noqa: BLE001
            logger.debug("Lakebase table_row_counts failed: %s", exc)
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
        return _get_pool(self._auth, self._schema).connection()

    def _registry(self) -> str:
        if self._registry_id is None:
            self._registry_id = self._fetch_registry_id() or self._ensure_registry_row()
        return self._registry_id

    def _fetch_registry_id(self) -> Optional[str]:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT id FROM {self._q(self._schema)}.registries "
                    "WHERE name = %s",
                    (self._registry_name(),),
                )
                row = cur.fetchone()
            return str(row[0]) if row else None
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
        c = self._cfg
        return f"{c.catalog}.{c.schema}.{c.volume}"

    def _apply_ddl(self) -> None:
        ddl_path = os.path.join(os.path.dirname(__file__), _DDL_FILENAME)
        with open(ddl_path, "r", encoding="utf-8") as fh:
            ddl = fh.read()
        ddl = ddl.replace(_SCHEMA_TOKEN, self._schema)
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(ddl)

    @staticmethod
    def _q(name: str) -> str:
        """Quote an SQL identifier safely (validated at construction time)."""
        return f'"{name}"'
