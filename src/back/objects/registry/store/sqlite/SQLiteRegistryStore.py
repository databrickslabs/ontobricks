"""Registry store on an embedded SQLite file, for workspaces without Lakebase.

Lakebase isn't offered in every cloud region yet, and
the registry is the only part of OntoBricks that needs it: the graph and the
Explorer run on Delta with the ``databricks`` (Lakehouse) graph backend, and
the MCP server goes through the app's REST API.

This store reuses every Lakebase registry mixin unchanged. Only three
concerns are replaced:

- the connection: :class:`SQLiteConnection` speaks the psycopg subset the
  mixins use and translates their Postgres SQL (:class:`SQLiteDialect`);
- the schema: a SQLite copy of the Lakebase DDL, applied when the database
  file is new, so the store needs no *Initialize* step and none of the
  Postgres self-healing migrations (their ``_*_ready`` guards start True);
- the Postgres-only diagnostics (``information_schema``, grants), which have
  no meaning for a local file.

Durability comes from :class:`SQLiteSnapshot` (local file mirrored to a UC
Volume). Only one App instance may write the file.
"""

from __future__ import annotations

import os
import re
import tempfile
from typing import Any, Dict, List, Optional, Tuple

from back.core.errors import InfrastructureError
from back.core.logging import get_logger
from back.objects.registry.store.lakebase.store import LakebaseRegistryStore
from back.objects.registry.store.sqlite.SQLiteConnection import SQLiteConnection
from back.objects.registry.store.sqlite.SQLiteSnapshot import SQLiteSnapshot

logger = get_logger(__name__)

_SCHEMA_SQL = os.path.join(os.path.dirname(__file__), "schema.sql")
_SAFE_SCHEMA_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_UUID_DEFAULT = (
    "lower(hex(randomblob(4)))||'-'||lower(hex(randomblob(2)))||'-4'||"
    "substr(lower(hex(randomblob(2))),2)||'-'||substr('89ab',1+(abs(random())%4),1)||"
    "substr(lower(hex(randomblob(2))),2)||'-'||lower(hex(randomblob(6)))"
)
_NOW_DEFAULT = "strftime('%Y-%m-%dT%H:%M:%f+00:00','now')"

# Postgres self-healing migrations guarded by these flags never apply to a
# SQLite file: its schema is created complete.
_READY_FLAGS = (
    "_build_runs_ready",
    "_graph_analytics_ready",
    "_graph_analytics_runs_ready",
    "_status_column_ready",
    "_quorum_column_ready",
    "_mcp_policy_column_ready",
    "_review_events_ready",
    "_change_events_ready",
    "_collab_tables_ready",
    "_edit_locks_ready",
    "_schedule_columns_ready",
    "_cohort_schedules_imported",
    "_domain_documents_ready",
)


class SQLiteRegistryStore(LakebaseRegistryStore):
    """Lakebase registry logic over a local SQLite file mirrored to a UC Volume."""

    def __init__(
        self,
        *,
        registry_cfg,
        schema: str = "ontobricks_registry",
        local_path: str = "",
        snapshot: Optional[SQLiteSnapshot] = None,
    ) -> None:
        # The Lakebase constructor resolves Lakebase credentials, so it is
        # deliberately not called; only the attributes the mixins read are set.
        if not _SAFE_SCHEMA_RE.match(schema or ""):
            raise InfrastructureError(
                "Invalid registry schema name %r; must match [a-zA-Z_][a-zA-Z0-9_]*" % schema
            )
        self._cfg = registry_cfg
        self._schema = schema
        self._database = ""
        self._registry_id: Optional[str] = None
        for flag in _READY_FLAGS:
            setattr(self, flag, True)
        self._local_path = local_path or self.default_local_path(schema)
        self._snapshot = snapshot
        self._prepare_file()

    @staticmethod
    def default_local_path(schema: str) -> str:
        """Local file for *schema*: ``$REGISTRY_SQLITE_PATH`` or a temp-dir default."""
        configured = os.environ.get("REGISTRY_SQLITE_PATH", "").strip()
        if configured:
            return configured
        return os.path.join(tempfile.gettempdir(), "ontobricks-registry", "%s.sqlite" % schema)

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    @property
    def backend(self) -> str:
        return "sqlite"

    @property
    def cache_key(self) -> str:
        c = self._cfg
        return "sqlite:%s:%s:%s.%s.%s" % (
            self._local_path, self._schema, c.catalog, c.schema, c.volume
        )

    @property
    def local_path(self) -> str:
        return self._local_path

    def describe(self) -> Dict[str, Any]:
        c = self._cfg
        return {
            "backend": self.backend,
            "cache_key": self.cache_key,
            "schema": self._schema,
            "local_path": self._local_path,
            "snapshot_path": self._snapshot.volume_path if self._snapshot else "",
            "volume_catalog": c.catalog,
            "volume_schema": c.schema,
            "volume_volume": c.volume,
        }

    # ------------------------------------------------------------------
    # Connection and schema
    # ------------------------------------------------------------------

    def _connect(self) -> SQLiteConnection:
        on_commit = self._snapshot.mark_dirty if self._snapshot else None
        return SQLiteConnection(self._local_path, self._schema, on_commit=on_commit)

    def _prepare_file(self) -> None:
        """Restore the snapshot if needed, then create the schema when the file is new."""
        if self._snapshot is not None:
            self._snapshot.restore()
        if not os.path.exists(self._local_path):
            os.makedirs(os.path.dirname(self._local_path) or ".", exist_ok=True)
            self._apply_ddl()
            logger.info("Created SQLite registry %s (schema=%s)", self._local_path, self._schema)

    def _apply_ddl(self) -> None:
        with open(_SCHEMA_SQL, "r", encoding="utf-8") as fh:
            ddl = fh.read()
        ddl = (
            ddl.replace("__SCHEMA__", self._q(self._schema))
            .replace("__UUID__", _UUID_DEFAULT)
            .replace("__NOW__", _NOW_DEFAULT)
        )
        self._connect().run_script(ddl)

    def table_row_counts(self, tables: Tuple[str, ...]) -> Dict[str, int]:
        result: Dict[str, int] = {t: 0 for t in tables}
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT name FROM %s.sqlite_master WHERE type = 'table'" % self._q(self._schema))
            present = {row[0] for row in cur.fetchall()}
            for name in tables:
                if name in present:
                    cur.execute("SELECT count(*) FROM %s.%s" % (self._q(self._schema), self._q(name)))
                    result[name] = int(cur.fetchone()[0])
        return result

    # ------------------------------------------------------------------
    # Bootstrap (Postgres catalogs and grants have no SQLite meaning)
    # ------------------------------------------------------------------

    def init_status(self) -> Dict[str, Any]:
        """The local file needs no *Initialize* step: the registry row is seeded on first use."""
        try:
            self._registry()
        except Exception as exc:  # noqa: BLE001 -- surfaced to the admin UI
            return {"initialized": False, "reason": "connect_failed", "error": str(exc)}
        return {"initialized": True, "reason": "ok", "error": None}

    def initialize(self, *, client: Any = None) -> Tuple[bool, str]:
        del client
        try:
            self._apply_ddl()
            self._registry_id = self._ensure_registry_row()
            self._scrub_global_config_legacy_keys()
        except Exception as exc:  # noqa: BLE001 -- reported to the admin UI
            logger.exception("SQLite registry initialise failed")
            return False, "Failed to initialise SQLite registry: %s" % exc
        if self._snapshot is not None:
            self._snapshot.flush()
        return True, "SQLite registry initialized at %s (schema=%s)" % (self._local_path, self._schema)

    def check_permissions(self) -> Dict[str, Any]:
        checks: List[Dict[str, Any]] = []
        writable = os.access(os.path.dirname(self._local_path) or ".", os.W_OK)
        checks.append({
            "id": "local_file",
            "label": "Local SQLite file is writable",
            "status": "ok" if writable else "error",
            "detail": self._local_path,
        })
        if self._snapshot is None:
            checks.append({
                "id": "snapshot",
                "label": "Snapshot to a UC Volume",
                "status": "warning",
                "detail": "No Volume configured: the registry is lost when the app restarts.",
            })
        else:
            checks.append({
                "id": "snapshot",
                "label": "Snapshot to a UC Volume",
                "status": "ok" if self._snapshot.flush() else "error",
                "detail": self._snapshot.volume_path,
            })
        return {
            "success": all(c["status"] != "error" for c in checks),
            "database": self._local_path,
            "user": "",
            "schema": self._schema,
            "checks": checks,
        }

    def grant_app_permissions(self, *, app_names: List[str], uc_catalog: str = "") -> Dict[str, Any]:
        del app_names, uc_catalog
        return {
            "success": True,
            "granted": [],
            "warnings": ["The SQLite registry is a local file: there are no database grants to apply."],
            "error": None,
        }
