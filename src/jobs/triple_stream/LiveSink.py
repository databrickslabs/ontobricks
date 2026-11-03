"""The realtime stream's ``foreach`` sink: every slot change upserted into Lakebase ``_live`` at once."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Any

#: Columns of a ``_live`` row, in insert order.
LIVE_COLUMNS = (
    "map_id",
    "source_key",
    "predicate",
    "subject",
    "object",
    "object_hash",
    "deleted",
    "seq",
)
_TABLE = re.compile(r"[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?")


class LiveSink:
    """``writeStream.foreach`` writer of the ``_live`` table (open / process / close).

    Real-Time Mode keeps one batch open for a whole checkpoint interval, so a
    writer that buffered until ``close`` would hold every change back for
    minutes: :meth:`process` upserts each slot change as it arrives, on a
    connection opened in :meth:`open` (autocommit), and :meth:`close` closes
    it. The upsert's ``WHERE EXCLUDED.seq > live.seq`` is the ordering guard
    at the sink — the reason this is a ``foreach`` writer and not the native
    ``postgresql`` sink, which has no such guard. A replayed row (a retried
    epoch) is a no-op for the same reason.
    """

    def __init__(self, connect: Callable[[], Any], live_table: str) -> None:
        if not _TABLE.fullmatch(live_table or ""):
            raise ValueError(f"Not a table name: {live_table!r}")
        self._connect = connect
        self._sql = LiveSink.upsert_sql(live_table)
        self._connection: Any = None

    @staticmethod
    def upsert_sql(live_table: str) -> str:
        """One slot change into *live_table*: inserted, or replacing an older ``seq``."""
        if not _TABLE.fullmatch(live_table or ""):
            raise ValueError(f"Not a table name: {live_table!r}")
        columns = ", ".join(LIVE_COLUMNS)
        placeholders = ", ".join(["%s"] * len(LIVE_COLUMNS))
        updates = ", ".join(
            f"{column} = EXCLUDED.{column}"
            for column in ("subject", "object", "object_hash", "deleted", "seq")
        )
        return (
            f"INSERT INTO {live_table} AS live ({columns}, updated_at) "
            f"VALUES ({placeholders}, now()) "
            "ON CONFLICT (map_id, source_key, predicate) DO UPDATE SET "
            f"{updates}, updated_at = now() WHERE EXCLUDED.seq > live.seq"
        )

    @staticmethod
    def values(change: Any) -> tuple:
        """The row's values in :data:`LIVE_COLUMNS` order.

        A micro-batch hands the writer a Spark ``Row`` (or a mapping); Real-Time
        Mode hands it a plain tuple in the output schema's column order, which is
        :data:`LIVE_COLUMNS` (measured live: ``dict(tuple)`` was the sink's first
        failure under the real-time trigger).
        """
        if hasattr(change, "asDict"):
            record = change.asDict()
        elif isinstance(change, Mapping):
            record = dict(change)
        else:
            record = dict(zip(LIVE_COLUMNS, tuple(change), strict=False))
        return tuple(record.get(column) for column in LIVE_COLUMNS)

    def open(self, partition_id: int, epoch_id: int) -> bool:
        """Connect for this partition (autocommit: each upsert lands at once)."""
        self._connection = self._connect()
        self._connection.autocommit = True
        return True

    def process(self, row: Any) -> None:
        """Upsert one slot change now."""
        with self._connection.cursor() as cur:
            cur.execute(self._sql, LiveSink.values(row))

    def close(self, error: BaseException | None) -> None:
        """Close the connection (the upserts are already committed)."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
