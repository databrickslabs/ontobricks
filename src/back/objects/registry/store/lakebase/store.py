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
  :class:`back.core.databricks.lakebase.LakebaseAuth`.

Cold start:
- Lakebase Autoscaling scales-to-zero when idle. Initial calls
  retry with exponential backoff on SQLSTATE ``57P03``
  ("cannot_connect_now") and on ``connection refused``.

Connection pooling:
- The connection machinery is shared with the graph triple store and
  lives in :mod:`back.core.databricks.lakebase`. A process-wide LIFO
  pool keeps a small handful of warm psycopg connections, keyed by the
  full connection identity (host/port/db/user/instance/schema) plus the
  ``ontobricks-registry`` workload label. This avoids the 200-500 ms
  TCP+TLS+JWT handshake per call and turns hot-path operations like
  *Load Domain from Registry* into a single network round-trip per
  query. Connections are recycled before the 1 h JWT expiry so token
  rotation stays invisible to callers. The registry and the graph store
  remain independent databases — they never share a pool.

Token expiry:
- Authentication failures (SQLSTATE ``28P01``) trigger a single
  invalidate-and-retry cycle when *opening* a fresh connection. Pooled
  connections that hit auth failure mid-flight are discarded by the
  ``_connect`` context manager.

The whole module is import-safe even without ``psycopg`` installed —
it raises a clear error only when the class is instantiated.
"""

from __future__ import annotations

import os
import re
import threading
import time
from typing import Any, Dict, Optional, Tuple

from back.core.databricks import get_lakebase_auth
from back.core.databricks.lakebase import get_lakebase_pool
from back.core.databricks.lakebase import require_psycopg as _shared_require_psycopg
from back.core.databricks.lakebase.constants import APPLICATION_NAME_REGISTRY
from back.core.errors import InfrastructureError
from back.core.logging import get_logger
from back.objects.registry.gitsync.GitHubMirrorHooks import GitHubMirrorHooks

from .Collab import Collab
from .Documents import Documents
from .Analytics import Analytics
from .Bootstrap import Bootstrap
from .BuildRuns import BuildRuns
from .GlobalConfig import GlobalConfig
from .Catalog import Catalog
from .Schedules import Schedules
from .ChangeEvents import ChangeEvents
from .ReviewEvents import ReviewEvents
from .EditLocks import EditLocks
from ..base import (
    RegistryStore,
    StoreError,
)

logger = get_logger(__name__)

# Keys the global-config blob must never carry: schedules and their run
# history are owned by the ``schedules`` / ``schedule_runs`` tables. The
# cohort pair predates the generic scheduled-task table and is imported
# out of the blob by ``_import_legacy_cohort_schedules``.
_LEGACY_SCHEDULE_KEYS = (
    "schedules",
    "schedule_history",
    "cohort_schedules",
    "cohort_schedule_history",
)
_DDL_FILENAME = "schema.sql"
_SCHEMA_TOKEN = "__SCHEMA__"
_SAFE_SCHEMA_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

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
        "graph_analytics",
        "graph_analytics_runs",
        "domain_review_events",
        "domain_change_events",
        "domain_comments",
        "domain_tasks",
        "domain_edit_locks",
        "domain_documents",
    }
)

# Single-editor lock for DRAFT (domain, version) versions. The lock is held
# until the holder explicitly *closes* the domain (release), an admin *takes
# over* (force), the version leaves DRAFT, or — when a lease TTL is configured
# (``ttl_seconds``) — its ``heartbeat_at`` lease lapses, at which point the
# next opener silently reclaims it. The holder keeps the lease alive by
# renewing ``heartbeat_at`` (``renew_edit_lock`` + the per-page acquire). A
# TTL of 0 disables the lease (held until explicit release / take-over).


def _require_psycopg():
    """Lazy import psycopg + psycopg.rows. Clear error when missing.

    Delegates to the shared gate (:func:`back.core.databricks.lakebase.
    require_psycopg`) but re-raises as :class:`InfrastructureError` to keep
    the registry's error surface unchanged. Kept as a module-level function
    so the many ``psycopg, dict_row = _require_psycopg()`` call sites (and the
    tests that monkeypatch this name) keep working.
    """
    try:
        return _shared_require_psycopg()
    except ImportError as exc:  # pragma: no cover
        raise InfrastructureError(str(exc)) from exc


def _get_pool(auth: Any, schema: str, database: str = ""):
    """Return the shared Lakebase pool for *auth* + *schema* + *database*.

    Thin wrapper over :func:`back.core.databricks.lakebase.get_lakebase_pool`
    with the registry workload label and error type. Kept as a module-level
    function because ``fetch_lakebase_registry_triplet`` and
    :meth:`LakebaseRegistryStore._connect` call it (and tests monkeypatch it).

    The ``database`` arg is the optional override that points the store at a
    different Postgres database on the same Lakebase instance. The empty
    string means "use the bound PGDATABASE".
    """
    return get_lakebase_pool(
        auth,
        schema,
        database,
        application_name=APPLICATION_NAME_REGISTRY,
        error_factory=StoreError,
    )


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


class LakebaseRegistryStore(
    GitHubMirrorHooks,
    Documents,
    Collab,
    EditLocks,
    Analytics,
    ReviewEvents,
    ChangeEvents,
    BuildRuns,
    Catalog,
    Schedules,
    Bootstrap,
    GlobalConfig,
    RegistryStore,
):
    """Postgres-backed registry store. Optional backend.

    Parameters
    ----------
    registry_cfg:
        :class:`back.objects.registry.RegistryCfg.RegistryCfg` —
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
        # Guards the lazy ``CREATE TABLE IF NOT EXISTS graph_analytics`` used
        # to self-heal deployments created before the async graph-analytics
        # cache existed (same pattern as ``_build_runs_ready``).
        self._graph_analytics_ready = False
        # Guards the lazy ``CREATE TABLE IF NOT EXISTS graph_analytics_runs``
        # (append-only analysis run history; same pattern as above).
        self._graph_analytics_runs_ready = False
        # Guards the lazy ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS status``
        # used to self-heal deployments created before the lifecycle status
        # column existed (same pattern as ``_build_runs_ready``).
        self._status_column_ready = False
        # Guards the lazy ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS
        # review_quorum`` used to self-heal deployments created before the
        # per-domain sign-off quorum existed (same pattern as
        # ``_status_column_ready``).
        self._quorum_column_ready = False
        # Guards the lazy ``ALTER TABLE ... ADD COLUMN IF NOT EXISTS
        # mcp_policy`` used to self-heal deployments created before the
        # per-domain MCP policy existed (same pattern as
        # ``_quorum_column_ready``).
        self._mcp_policy_column_ready = False
        # Guards the lazy ``CREATE TABLE IF NOT EXISTS domain_review_events``
        # used to self-heal deployments created before the review/validation
        # audit log existed (same pattern as ``_build_runs_ready``).
        self._review_events_ready = False
        # Guards the lazy ``CREATE TABLE IF NOT EXISTS domain_change_events``
        # used to self-heal deployments created before the ontology/mapping
        # change audit log existed (same pattern as ``_review_events_ready``).
        self._change_events_ready = False
        # Guards the lazy ``CREATE TABLE IF NOT EXISTS domain_comments /
        # domain_tasks`` used to self-heal deployments created before the
        # collaborative comments + tasks feature existed (same pattern as
        # ``_review_events_ready``).
        self._collab_tables_ready = False
        # Guards the lazy ``CREATE TABLE IF NOT EXISTS domain_edit_locks``
        # used to self-heal deployments created before the single-editor
        # lock feature existed (same pattern as ``_collab_tables_ready``).
        self._edit_locks_ready = False
        # Guards the lazy migration that turns the build-only ``schedules``
        # table into the generic scheduled-task table (task_type /
        # target_key / config / detail + the widened unique constraint).
        self._schedule_columns_ready = False
        # Guards the one-shot import of cohort schedules out of the
        # ``global_config`` JSONB blob into the ``schedules`` table.
        self._cohort_schedules_imported = False
        # Guards the lazy ``CREATE TABLE IF NOT EXISTS domain_documents``
        # used to self-heal deployments created before the Lakebase
        # Knowledge Store existed (same pattern as ``_build_runs_ready``).
        self._domain_documents_ready = False

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

    @staticmethod
    def _scalar_total(row: Any) -> int:
        """Read a ``COUNT(*) AS total`` result whatever the row factory is."""
        if row is None:
            return 0
        if isinstance(row, dict):
            return int(row.get("total") or 0)
        return int(row[0] or 0)

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
        rotation — see
        :class:`back.core.databricks.lakebase.LakebaseConnectionPool`.
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


    @staticmethod
    def _q(name: str) -> str:
        """Quote an SQL identifier safely (validated at construction time)."""
        return f'"{name}"'
