"""Readiness probes on the Delta source tables an incremental build streams from."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from back.core.helpers import WarehouseStatements
from back.core.logging import get_logger

logger = get_logger(__name__)

_ROW_TRACKING = "delta.enableRowTracking"

#: ``DESCRIBE HISTORY`` operations that rewrite the table instead of changing rows.
_REWRITING_OPERATIONS = frozenset(
    {
        "CREATE OR REPLACE TABLE AS SELECT",
        "REPLACE TABLE AS SELECT",
        "TRUNCATE",
        "RESTORE",
    }
)


class SourceReadiness:
    """Whether a source table can feed an incremental build.

    Enzyme refreshes a materialized view incrementally only from sources that
    track row identity (``delta.enableRowTracking``). A source that was
    *rewritten* (overwrite, replace, truncate, restore) since a known version
    breaks a change-feed reader, which then needs a full refresh.

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

    @staticmethod
    def enable_sql(table_fqn: str) -> str:
        """``ALTER TABLE`` that makes *table_fqn* ready (row tracking + deletion vectors)."""
        SourceReadiness._require_fqn(table_fqn)
        return (
            f"ALTER TABLE {table_fqn} SET TBLPROPERTIES "
            "(delta.enableRowTracking = true, delta.enableDeletionVectors = true)"
        )

    @staticmethod
    def source_overwritten_since(client: Any, table_fqn: str, since_version: int) -> bool:
        """True when *table_fqn* was rewritten after Delta version *since_version*.

        A ``DESCRIBE HISTORY`` row counts when its ``version`` is greater than
        *since_version* and its ``operation`` replaces the data wholesale: one
        of the rewriting operations, or a ``WRITE`` whose ``mode`` parameter
        is ``Overwrite``.
        """
        SourceReadiness._require_fqn(table_fqn)
        rows = WarehouseStatements.query(
            client, f"DESCRIBE HISTORY {table_fqn}", f"read the history of {table_fqn}"
        )
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            if int(row.get("version") or 0) <= int(since_version):
                continue
            operation = str(row.get("operation") or "").upper()
            if operation in _REWRITING_OPERATIONS:
                return True
            params = SourceReadiness._params(row.get("operationParameters"))
            if operation == "WRITE" and str(params.get("mode", "")).lower() == "overwrite":
                return True
        return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _require_fqn(table_fqn: str) -> None:
        WarehouseStatements.require_fqn(table_fqn, what="source table name")

    @staticmethod
    def _params(value: Any) -> dict:
        """``operationParameters`` as a dict (the connector returns a map, pairs or JSON)."""
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except ValueError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        if isinstance(value, (list, tuple)):
            return {
                pair[0]: pair[1]
                for pair in value
                if isinstance(pair, (list, tuple)) and len(pair) == 2
            }
        return {}
