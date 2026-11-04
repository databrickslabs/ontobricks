"""The Lakebase ``_live`` serving table a ``realtime`` domain's stream job writes."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import AbstractContextManager
from itertools import islice
from typing import Any

from back.core.errors import InfrastructureError
from back.core.graphdb.lakebase import _companion_ddl
from back.core.logging import get_logger

logger = get_logger(__name__)

#: Tombstones kept this long by default before :meth:`LakebaseLiveTable.purge_tombstones`.
DEFAULT_TOMBSTONE_HOURS = 24
_SEED_BATCH = 2000


class LakebaseLiveTable:
    """Create, seed, serve, retire, purge and count a graph's ``g_<dom>_v<n>_live`` table.

    A collaborator of the Lakebase store (``Base.live_table``): it runs
    on the store's pooled cursor (search path = the graph schema). The union
    view reads the table only while it is *served* (a table comment,
    ``_companion_ddl.LIVE_SERVING_COMMENT``): :meth:`attach` seeds it from the
    ``_slots`` snapshot and marks it served when a stream starts, :meth:`detach`
    empties and unmarks it when the stream stops or the domain stops being
    ``realtime``, so a frozen ``_live`` never lingers in the view. The caller
    re-creates the view after either.
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

    def ensure(self, graph: str, *, writer_role: str = "") -> str:
        """Create the ``_live`` table of *graph* (idempotent) and grant *writer_role*; its name.

        *writer_role* is the Postgres role the stream job upserts as
        (``USAGE`` on the schema, ``SELECT, INSERT, UPDATE`` on the table).
        """
        table = self.table_name(graph)
        statements = _companion_ddl.live_table_ddl(table)
        if writer_role:
            statements += _companion_ddl.live_grants_sql(table, self._schema, writer_role)
        try:
            with self._cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
        except Exception as exc:  # noqa: BLE001 — the driver's error, e.g. a writer role that does not exist
            raise InfrastructureError(
                f"Could not prepare the live table {self._schema}.{table}"
                + (f" for role {writer_role!r}" if writer_role else ""),
                detail=str(exc),
            ) from exc
        logger.info("Live serving table %s.%s ensured", self._schema, table)
        return table

    @staticmethod
    def seed_rows(slots_rows: Iterable[Mapping]) -> Iterator[tuple]:
        """``_live`` rows of ``_slots`` snapshot rows: non-blank slots at the snapshot ``seq``.

        The same slots the stream's initial state starts from
        (``SlotDiffProcessor.initial_state``), so the first change of a row
        diffs against what ``_live`` already serves.
        """
        from back.core.streaming.SlotChange import SlotChange
        from back.core.streaming.SlotDiffProcessor import SlotDiffProcessor

        for row in slots_rows:
            obj, subject = row.get("object"), row.get("subject")
            if subject is None or SlotDiffProcessor.is_blank(obj):
                continue
            yield (
                str(row["map_id"]),
                str(row["source_key"]),
                str(row["predicate"]),
                subject,
                obj,
                SlotChange.hash_object(obj),
                SlotDiffProcessor.SNAPSHOT_SEQ,
            )

    def attach(self, graph: str, slots_rows: Iterable[Mapping]) -> int:
        """Empty *graph*'s ``_live``, seed it from *slots_rows*, mark it served; rows seeded."""
        table = self.table_name(graph)
        rows = self.seed_rows(slots_rows)
        seeded = 0
        with self._cursor() as cur:
            cur.execute(_companion_ddl.truncate_live_sql(table))
            while batch := list(islice(rows, _SEED_BATCH)):
                cur.executemany(_companion_ddl.seed_live_sql(table), batch)
                seeded += len(batch)
            cur.execute(_companion_ddl.live_serving_sql(table, True))
        logger.info("Live serving table %s seeded with %d slot(s) and served", table, seeded)
        return seeded

    def detach(self, graph: str) -> None:
        """Empty *graph*'s ``_live`` (when it exists) and stop serving it."""
        table = self.table_name(graph)
        with self._cursor() as cur:
            if not _companion_ddl._table_exists(cur, table):
                return
            cur.execute(_companion_ddl.truncate_live_sql(table))
            cur.execute(_companion_ddl.live_serving_sql(table, False))
        logger.info("Live serving table %s emptied and no longer served", table)

    def serving(self, graph: str) -> bool:
        """True while the union view reads *graph*'s ``_live``."""
        with self._cursor() as cur:
            return _companion_ddl.is_live_serving(cur, self.table_name(graph))

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
