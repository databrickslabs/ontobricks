"""The Lakebase ``_live`` serving table a ``realtime`` domain's stream job writes."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

from back.core.graphdb.lakebase import _companion_ddl
from back.core.logging import get_logger

logger = get_logger(__name__)

#: Tombstones kept this long by default before :meth:`LakebaseLiveTable.purge_tombstones`.
DEFAULT_TOMBSTONE_HOURS = 24


class LakebaseLiveTable:
    """Create, purge and count a graph's ``g_<dom>_v<n>_live`` table.

    A collaborator of the Lakebase store (``LakebaseBase.live_table``): it runs
    on the store's pooled cursor (search path = the graph schema) and knows the
    connection facts the stream job needs. The table's DDL and the union view
    that serves it live in ``_companion_ddl``.
    """

    def __init__(
        self,
        cursor: Callable[[], AbstractContextManager[Any]],
        *,
        schema: str,
        auth: Any = None,
    ) -> None:
        self._cursor = cursor
        self._schema = schema
        self._auth = auth

    @staticmethod
    def table_name(graph: str) -> str:
        """The ``_live`` table of *graph* (bare: the cursor's search path is the graph schema)."""
        return _companion_ddl.live_phy(graph)

    def ensure(self, graph: str) -> str:
        """Create the ``_live`` table of *graph* (idempotent); return its name.

        The union view picks it up the next time it is (re)created
        (``ensure_synced_union_view``).
        """
        table = self.table_name(graph)
        with self._cursor() as cur:
            for statement in _companion_ddl.live_table_ddl(table):
                cur.execute(statement)
        logger.info("Live serving table %s.%s ensured", self._schema, table)
        return table

    def purge_tombstones(self, graph: str, older_than_hours: int = DEFAULT_TOMBSTONE_HOURS) -> int:
        """Delete *graph*'s tombstones older than *older_than_hours*; return how many."""
        sql = _companion_ddl.purge_live_tombstones_sql(self.table_name(graph), older_than_hours)
        with self._cursor() as cur:
            cur.execute(sql)
            purged = int(getattr(cur, "rowcount", 0) or 0)
        logger.info("Purged %d tombstone(s) from %s", purged, self.table_name(graph))
        return purged

    def count(self, graph: str) -> int:
        """Slots *graph*'s ``_live`` table serves (not deleted, non-blank object)."""
        with self._cursor() as cur:
            cur.execute(_companion_ddl.count_live_rows_sql(self.table_name(graph)))
            row = cur.fetchone()
        if isinstance(row, dict):
            return int(row.get("cnt") or 0)
        return int(row[0]) if row else 0

    def connection(self) -> dict:
        """``host`` / ``database`` / ``schema`` the stream job connects to (no credential)."""
        return {
            "host": getattr(self._auth, "host", "") or "",
            "database": getattr(self._auth, "database", "") or "",
            "schema": self._schema,
        }
