"""Shared infrastructure for LadybugDB graph-database backends.

Provides connection management, path helpers, and the common abstract
base class that ``LadybugFlatStore`` and ``LadybugGraphStore`` inherit.
"""
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from back.core.graphdb.GraphDBBackend import GraphDBBackend
from shared.config.constants import DEFAULT_GRAPH_NAME

logger = get_logger(__name__)

try:
    import real_ladybug as lb
except ImportError:
    lb = None  # type: ignore[assignment]

AutoRestoreCallback = Callable[[], Tuple[bool, str]]


class LadybugBase(GraphDBBackend):
    """Abstract base for LadybugDB backends.

    Manages the on-disk database connection and exposes helpers shared
    by both flat-model and graph-model subclasses.

    Parameters
    ----------
    auto_restore:
        Optional callback ``() -> (ok, message)`` invoked when the
        ``.lbug`` file is missing from *db_path*.  The callback should
        restore the file (e.g. download from the registry) and return
        ``(True, ...)`` on success.  When *None* or when the callback
        fails, a fresh empty database is created instead.
    """

    @staticmethod
    def _require_ladybug() -> None:
        if lb is None:
            raise ImportError(
                "real_ladybug is required for LadybugDB backends. "
                "Install it with: pip install real_ladybug"
            )

    @staticmethod
    def safe_table_id(name: str) -> str:
        """Convert a table name to a valid LadybugDB node-table identifier."""
        from back.core.helpers import safe_identifier
        base = name.split(".")[-1] if "." in name else name
        if not base:
            return "triples"
        return safe_identifier(base) or "triples"

    def __init__(
        self,
        db_path: str = "/tmp/ontobricks",
        db_name: str = DEFAULT_GRAPH_NAME,
        ontology: Optional[Dict[str, Any]] = None,
        auto_restore: Optional[AutoRestoreCallback] = None,
    ) -> None:
        LadybugBase._require_ladybug()
        self.db_path = db_path
        self.db_name = db_name
        self._db: Optional[Any] = None
        self._conn: Optional[Any] = None
        self._next_id: int = 0
        self._table_registry: Dict[str, bool] = {}
        self._ontology = ontology
        self._graph_schema: Optional[Any] = None
        self._graph_schema_checked: bool = False
        self._auto_restore = auto_restore

    def _get_db_path(self) -> str:
        safe = re.sub(r"[^a-zA-Z0-9_]", "_", self.db_name)
        return os.path.join(self.db_path, f"{safe}.lbug")

    def _try_auto_restore(self, path: str) -> None:
        """Attempt to restore the .lbug file from the registry if missing."""
        if self._auto_restore is None:
            return
        logger.info(
            "LadybugDB file not found locally (%s) — attempting auto-restore from registry",
            path,
        )
        try:
            ok, msg = self._auto_restore()
            if ok:
                logger.info("Auto-restore from registry succeeded: %s", msg)
            else:
                logger.warning("Auto-restore from registry failed: %s", msg)
        except Exception as exc:
            logger.warning("Auto-restore from registry error: %s", exc)

    def _get_connection(self) -> Any:
        if self._conn is not None:
            return self._conn
        path = self._get_db_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            self._try_auto_restore(path)
        self._db = lb.Database(path)
        logger.info("LadybugDB on-disk database: %s", path)
        self._conn = lb.Connection(self._db)
        return self._conn

    # -- GraphDBBackend capability flags ------------------------------------

    @property
    def supports_cypher(self) -> bool:
        return True

    @property
    def query_dialect(self) -> str:
        return "cypher"

    # -- GraphDBBackend connection management --------------------------------

    def get_connection(self) -> Any:
        """Return (and lazily open) the native LadybugDB connection."""
        return self._get_connection()

    def close(self) -> None:
        self._conn = None
        self._db = None
        self._table_registry.clear()
        logger.debug("LadybugDB connection closed")

    # -- GraphDBBackend schema helpers ---------------------------------------

    def get_node_table(self, table_name: str) -> str:
        return LadybugBase.safe_table_id(table_name)

    def get_graph_schema(self) -> Optional[Any]:
        return self._graph_schema

    # -- GraphDBBackend sync -------------------------------------------------

    def sync_to_remote(
        self, uc_path: str, volume_service: Any,
    ) -> Tuple[bool, str]:
        from back.core.graphdb.ladybugdb.GraphSyncService import GraphSyncService
        svc = GraphSyncService(volume_service, self.db_name, self.db_path)
        return svc.sync_to_volume(uc_path)

    def sync_from_remote(
        self, uc_path: str, volume_service: Any,
    ) -> Tuple[bool, str]:
        from back.core.graphdb.ladybugdb.GraphSyncService import GraphSyncService
        svc = GraphSyncService(volume_service, self.db_name, self.db_path)
        return svc.sync_from_volume(uc_path)

    def local_path(self) -> Optional[str]:
        return self._get_db_path()

    def remote_archive_path(self, uc_domain_path: str) -> Optional[str]:
        from back.core.graphdb.ladybugdb.GraphSyncService import GraphSyncService
        return GraphSyncService.volume_archive_path(uc_domain_path, self.db_name)

    # -- GraphDBBackend reasoning support ------------------------------------

    def get_query_translator(self, table_name: str = "") -> Any:
        from back.core.reasoning.SWRLFlatCypherTranslator import SWRLFlatCypherTranslator
        node_table = LadybugBase.safe_table_id(table_name) if table_name else "Triple"
        return SWRLFlatCypherTranslator(node_table=node_table)

    # -- Internal helpers (backward compat) ----------------------------------

    def _node_table(self, table_name: str) -> str:
        return LadybugBase.safe_table_id(table_name)

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """LadybugDB does not support raw SQL."""
        raise NotImplementedError(
            "LadybugDB uses Cypher, not SQL. "
            "Use the named query methods on TripleStoreBackend instead."
        )

    @staticmethod
    def _translate_conditions(conditions: List[str], alias: str) -> List[str]:
        """Best-effort translation of simple SQL conditions to Cypher.

        Handles patterns like ``column = 'value'``, ``column LIKE '%x%'``,
        and ``subject IN (SELECT ...)``.  Unsupported patterns are dropped
        with a warning.
        """
        import re as _re

        cypher: List[str] = []
        for cond in conditions:
            cond = cond.strip()

            eq = _re.match(r"^(\w+)\s*=\s*'([^']*)'$", cond)
            if eq:
                col, val = eq.group(1), eq.group(2)
                cypher.append(f"{alias}.{col} = '{val}'")
                continue

            like_both = _re.match(r"^(\w+)\s+LIKE\s+'%([^']+)%'$", cond, _re.IGNORECASE)
            if like_both:
                col, val = like_both.group(1), like_both.group(2)
                cypher.append(f"{alias}.{col} CONTAINS '{val}'")
                continue

            like_end = _re.match(r"^(\w+)\s+LIKE\s+'%([^']+)'$", cond, _re.IGNORECASE)
            if like_end:
                col, val = like_end.group(1), like_end.group(2)
                cypher.append(f"{alias}.{col} ENDS WITH '{val}'")
                continue

            like_start = _re.match(r"^(\w+)\s+LIKE\s+'([^']+)%'$", cond, _re.IGNORECASE)
            if like_start:
                col, val = like_start.group(1), like_start.group(2)
                cypher.append(f"{alias}.{col} STARTS WITH '{val}'")
                continue

            logger.warning(
                "LadybugDB: cannot translate SQL condition to Cypher: %s", cond
            )
        return cypher
