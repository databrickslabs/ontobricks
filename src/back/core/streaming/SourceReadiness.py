"""Readiness probes on the Delta source tables an incremental build streams from."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from back.core.helpers import WarehouseStatements
from back.core.logging import get_logger

logger = get_logger(__name__)

_ROW_TRACKING = "delta.enableRowTracking"


class SourceReadiness:
    """Whether a source table can feed an incremental build.

    Enzyme refreshes a materialized view incrementally only from sources that
    track row identity (``delta.enableRowTracking``).

    The only I/O is ``client.execute_query``. Failures raise
    :class:`InfrastructureError`: an unknown answer is not a "no".
    """

    @staticmethod
    def row_tracking_enabled(client: Any, table_fqn: str) -> bool:
        """True when *table_fqn* has ``delta.enableRowTracking = true``.

        ``SHOW TBLPROPERTIES`` answers a missing key with a row whose value is
        a "does not have property" message, which reads as not tracked.
        """
        SourceReadiness._require_fqn(table_fqn)
        rows = WarehouseStatements.query(
            client,
            f"SHOW TBLPROPERTIES {table_fqn} ('{_ROW_TRACKING}')",
            f"read row tracking on {table_fqn}",
        )
        return any(
            str(row.get("key", "")) == _ROW_TRACKING
            and str(row.get("value", "")).strip().lower() == "true"
            for row in rows
            if isinstance(row, Mapping)
        )

    @staticmethod
    def check(client: Any, source_tables: Iterable[str]) -> dict[str, bool]:
        """Row-tracking state of every table in *source_tables*."""
        return {
            table: SourceReadiness.row_tracking_enabled(client, table)
            for table in source_tables
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _require_fqn(table_fqn: str) -> None:
        WarehouseStatements.require_fqn(table_fqn, what="source table name")
