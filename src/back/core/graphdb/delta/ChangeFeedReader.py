"""Net triple changes of a Delta relation, read from its change data feed."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from back.core.errors import InfrastructureError, OntoBricksError, ValidationError
from back.core.helpers import WarehouseStatements
from back.core.logging import get_logger

logger = get_logger(__name__)

#: Change types counted as an insert (+1) and as a delete (-1).
_INSERTS = "('insert', 'update_postimage')"
_ALL_CHANGES = "('insert', 'update_postimage', 'delete', 'update_preimage')"


class ChangeFeedReader:
    """Reads the net insert / delete triple changes of a Delta relation in a version window.

    ``table_changes`` lists every change of every commit; a triple inserted and
    deleted again in the window cancels out. The window runs from the version
    after the last applied one up to a recorded version (inclusive), so a
    commit that lands while a build reads is left for the next build instead of
    being applied twice. The reduction groups the feed by
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
        WarehouseStatements.require_fqn(fqn)
        rows = WarehouseStatements.query(
            client, f"DESCRIBE HISTORY {fqn} LIMIT 1", f"read the history of {fqn}"
        )
        version = rows[0].get("version") if rows and isinstance(rows[0], dict) else None
        if version is None:
            raise InfrastructureError(f"Could not read the history of {fqn}: no version")
        return int(version)

    @staticmethod
    def table_id(client: Any, fqn: str) -> str:
        """The Delta table id of *fqn* (``DESCRIBE DETAIL``), or ``""`` when unknown.

        A relation dropped and created again (a TABLE replaced by a materialized
        view, or the reverse) gets a new id and restarts its history at version
        0, so equal versions only compare within one id. Best-effort: the id is
        a guard on top of the version checks, so a failed read is ``""``.
        """
        WarehouseStatements.require_fqn(fqn)
        try:
            rows = WarehouseStatements.query(
                client, f"DESCRIBE DETAIL {fqn}", f"describe {fqn}"
            )
        except InfrastructureError as exc:
            logger.debug("No table id for %s: %s", fqn, exc)
            return ""
        row = rows[0] if rows and isinstance(rows[0], dict) else {}
        return str(row.get("id") or "")

    @staticmethod
    def net_changes_sql(fqn: str, since_version: int, until_version: int) -> str:
        """The net changes in ``(since_version, until_version]``, deletes first.

        ``'delete' < 'insert'``: deletes stream before inserts.

        Applying the deletes before the inserts matters for single-valued
        stores (Neo4j properties): an updated value arrives as the delete of
        the old triple and the insert of the new one.
        """
        net = ChangeFeedReader._net_select(fqn, since_version, until_version)
        return net + "\nORDER BY op"

    @staticmethod
    def iter_net_changes(
        client: Any, fqn: str, since_version: int, until_version: int, batch_size: int = 5000
    ) -> Iterator[dict]:
        """Stream :meth:`net_changes_sql` rows (``subject``, ``predicate``, ``object``, ``op``)."""
        sql = ChangeFeedReader.net_changes_sql(fqn, since_version, until_version)
        try:
            yield from client.iter_rows(sql, batch_size=batch_size)
        except OntoBricksError:
            raise
        except Exception as exc:  # noqa: BLE001 — translated, not swallowed
            raise InfrastructureError(
                f"Could not read the change feed of {fqn}", detail=str(exc)
            ) from exc

    @staticmethod
    def changeset_ratio(
        client: Any, fqn: str, since_version: int, until_version: int, total_rows: int
    ) -> float:
        """Net changes in ``(since_version, until_version]`` as a fraction of *total_rows*.

        An empty total makes any change infinitely large (and no change 0.0).
        """
        net = ChangeFeedReader._net_select(fqn, since_version, until_version)
        rows = WarehouseStatements.query(
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
    def _net_select(fqn: str, since_version: int, until_version: int) -> str:
        WarehouseStatements.require_fqn(fqn)
        ChangeFeedReader._require_version(since_version, minimum=0)
        ChangeFeedReader._require_version(until_version, minimum=since_version + 1)
        return (
            "SELECT subject, predicate, object, "
            "CASE WHEN net_change > 0 THEN 'insert' ELSE 'delete' END AS op\n"
            "FROM (\n"
            "  SELECT subject, predicate, object,\n"
            f"    SUM(CASE WHEN _change_type IN {_INSERTS} THEN 1 ELSE -1 END) AS net_change\n"
            f"  FROM table_changes('{fqn}', {since_version + 1}, {until_version})\n"
            f"  WHERE _change_type IN {_ALL_CHANGES}\n"
            "  GROUP BY subject, predicate, object\n"
            ") c\n"
            "WHERE net_change <> 0"
        )

    @staticmethod
    def _require_version(version: Any, *, minimum: int) -> None:
        """An integer (not a bool) of at least *minimum*, else :class:`ValidationError`."""
        is_int = isinstance(version, int) and not isinstance(version, bool)
        if not is_int or version < minimum:
            raise ValidationError(f"Invalid change feed version: {version!r}")
