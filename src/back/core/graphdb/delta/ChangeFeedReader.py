"""Net triple changes of a Delta relation, read from its change data feed."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from back.core.helpers import SAFE_SQL_IDENT, validate_table_name
from back.core.logging import get_logger

logger = get_logger(__name__)

#: Change types counted as an insert (+1) and as a delete (-1).
_INSERTS = "('insert', 'update_postimage')"
_ALL_CHANGES = "('insert', 'update_postimage', 'delete', 'update_preimage')"


class ChangeFeedReader:
    """Reads the net insert / delete triple changes of a Delta relation since a version.

    ``table_changes`` lists every change of every commit; a triple inserted and
    deleted again in the window cancels out. The reduction groups the feed by
    ``(subject, predicate, object)`` and keeps the sign of the sum (inserts and
    post-images +1, deletes and pre-images -1), so each triple appears at most
    once, as an ``insert`` or a ``delete``.

    The only I/O is the warehouse ``client`` (``execute_query`` /
    ``iter_rows``). Driver errors become :class:`InfrastructureError` so a
    caller can fall back to a full read.
    """

    @staticmethod
    def latest_version(client: Any, fqn: str) -> int:
        """The newest Delta version of *fqn* (``DESCRIBE HISTORY … LIMIT 1``)."""
        ChangeFeedReader._require_fqn(fqn)
        rows = ChangeFeedReader._query(
            client, f"DESCRIBE HISTORY {fqn} LIMIT 1", f"read the history of {fqn}"
        )
        version = rows[0].get("version") if rows and isinstance(rows[0], dict) else None
        if version is None:
            raise InfrastructureError(f"Could not read the history of {fqn}: no version")
        return int(version)

    @staticmethod
    def net_changes_sql(fqn: str, since_version: int) -> str:
        """The net changes after *since_version*, deletes first (``'delete' < 'insert'``).

        Applying the deletes before the inserts matters for single-valued
        stores (Neo4j properties): an updated value arrives as the delete of
        the old triple and the insert of the new one.
        """
        return ChangeFeedReader._net_select(fqn, since_version) + "\nORDER BY op"

    @staticmethod
    def iter_net_changes(
        client: Any, fqn: str, since_version: int, batch_size: int = 5000
    ) -> Iterator[dict]:
        """Stream :meth:`net_changes_sql` rows (``subject``, ``predicate``, ``object``, ``op``)."""
        sql = ChangeFeedReader.net_changes_sql(fqn, since_version)
        try:
            yield from client.iter_rows(sql, batch_size=batch_size)
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(
                f"Could not read the change feed of {fqn}", detail=str(exc)
            ) from exc

    @staticmethod
    def changeset_ratio(client: Any, fqn: str, since_version: int, total_rows: int) -> float:
        """Net changes after *since_version* as a fraction of *total_rows*.

        An empty total makes any change infinitely large (and no change 0.0).
        """
        net = ChangeFeedReader._net_select(fqn, since_version)
        rows = ChangeFeedReader._query(
            client,
            f"SELECT COUNT(*) AS cnt FROM (\n{net}\n) n",
            f"count the changes of {fqn}",
        )
        changes = int((rows[0].get("cnt") if rows else 0) or 0)
        if total_rows <= 0:
            return float("inf") if changes else 0.0
        return changes / total_rows

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _net_select(fqn: str, since_version: int) -> str:
        ChangeFeedReader._require_fqn(fqn)
        is_int = isinstance(since_version, int) and not isinstance(since_version, bool)
        if not is_int or since_version < 0:
            raise ValidationError(f"Invalid change feed version: {since_version!r}")
        return (
            "SELECT subject, predicate, object, "
            "CASE WHEN net_change > 0 THEN 'insert' ELSE 'delete' END AS op\n"
            "FROM (\n"
            "  SELECT subject, predicate, object,\n"
            f"    SUM(CASE WHEN _change_type IN {_INSERTS} THEN 1 ELSE -1 END) AS net_change\n"
            f"  FROM table_changes('{fqn}', {since_version + 1})\n"
            f"  WHERE _change_type IN {_ALL_CHANGES}\n"
            "  GROUP BY subject, predicate, object\n"
            ") c\n"
            "WHERE net_change <> 0"
        )

    @staticmethod
    def _require_fqn(fqn: str) -> None:
        """Validated name; the safe-identifier check keeps quotes out of the literal."""
        validate_table_name(fqn)
        if SAFE_SQL_IDENT.fullmatch(fqn) is None:
            raise ValidationError(f"Invalid table name: {fqn!r}")

    @staticmethod
    def _query(client: Any, sql: str, what: str) -> list:
        try:
            return list(client.execute_query(sql) or [])
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(f"Could not {what}", detail=str(exc)) from exc
