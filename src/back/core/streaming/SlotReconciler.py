"""Drift check between the ``_slots`` streaming table and the ``_data`` relation."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from back.core.errors import InfrastructureError
from back.core.helpers import WarehouseStatements
from back.core.helpers.SQLHelpers import NON_BLANK_OBJECT_SQL
from back.core.logging import get_logger

logger = get_logger(__name__)

#: One 64-bit hash per triple, summed as a 38-digit decimal: hashes are
#: uniform 64-bit values, so a BIGINT ``sum`` overflows after a handful of rows
#: and ANSI mode raises ``ARITHMETIC_OVERFLOW`` instead of wrapping.
_TRIPLE_HASH_SUM = "sum(CAST(xxhash64(subject, predicate, object) AS DECIMAL(38, 0))) AS h"

#: The triples reconciled, on every side: non-blank objects (as the MV reads
#: the slots) and a subject. A NULL source key gives the gateway VIEW (and a
#: batch branch) a NULL-subject triple the slot view drops; such garbage rows
#: would otherwise show as drift forever. The MV and VIEW SQL keep them.
_RECONCILED_TRIPLES = "WHERE subject IS NOT NULL AND " + NON_BLANK_OBJECT_SQL.removeprefix("WHERE ")


class SlotReconciler:
    """Row counts and triple hashes of ``_slots`` versus ``_data``.

    The incremental ``_data`` materialized view is the union of the slot table
    and the batch-mapping branches, so its rows and hashes equal the slot
    table's only when every mapping is row-local. With batch mappings present
    a mismatch is expected and ``drift`` is not meaningful; the caller
    (:class:`TwinSlotReconciliation`) reports ``batch_branches_present`` so a
    reader can tell the two apart.

    Both sides are counted the way the MV reads the slots: distinct triples
    (two slots may project the same triple; the MV groups them) with blank
    objects left out (the MV's slots branch filters ``NULL`` / ``''`` objects,
    which unset attributes produce; ``_slots`` keeps them as upserts), and
    without NULL-subject rows (:data:`_RECONCILED_TRIPLES`).
    ``by_map_id`` is slots-only: ``_data`` carries no ``map_id``. The only I/O
    is ``client.execute_query``; driver errors and unreadable answers become
    :class:`InfrastructureError`, an unsafe name :class:`ValidationError`.
    """

    @staticmethod
    def counts_sql(fqn: str, *, group_by_map_id: bool = False) -> str:
        """``count(*)`` and the triple-hash sum of *fqn*, per ``map_id`` on request."""
        WarehouseStatements.require_fqn(fqn, what="relation name")
        if group_by_map_id:
            return (
                f"SELECT map_id, count(*) AS n, {_TRIPLE_HASH_SUM} "
                f"FROM {fqn} {_RECONCILED_TRIPLES} GROUP BY map_id ORDER BY map_id"
            )
        return (
            f"SELECT count(*) AS n, {_TRIPLE_HASH_SUM} "
            f"FROM (SELECT DISTINCT subject, predicate, object FROM {fqn} {_RECONCILED_TRIPLES})"
        )

    @staticmethod
    def totals(client: Any, fqn: str) -> dict:
        """``{"rows": int, "hash": int}`` of the distinct non-blank triples in *fqn*."""
        rows = WarehouseStatements.query(
            client, SlotReconciler.counts_sql(fqn), f"count the triples of {fqn}"
        )
        if not rows or not isinstance(rows[0], Mapping):
            raise InfrastructureError(f"Could not count the triples of {fqn}: no row returned")
        return {"rows": SlotReconciler._int(rows[0].get("n")), "hash": SlotReconciler._int(rows[0].get("h"))}

    @staticmethod
    def by_map_id(client: Any, slots_fqn: str) -> list[dict]:
        """Per-mapping ``{"map_id", "rows", "hash"}`` of the slots table."""
        rows = WarehouseStatements.query(
            client,
            SlotReconciler.counts_sql(slots_fqn, group_by_map_id=True),
            f"count the slots of {slots_fqn} per mapping",
        )
        return [
            {
                "map_id": str(row.get("map_id", "")),
                "rows": SlotReconciler._int(row.get("n")),
                "hash": SlotReconciler._int(row.get("h")),
            }
            for row in rows
            if isinstance(row, Mapping)
        ]

    @staticmethod
    def compare(
        client: Any,
        slots_fqn: str | None,
        data_fqn: str,
        *,
        view_fqn: str | None = None,
        sync_rows: int | None = None,
    ) -> dict:
        """Totals of both relations, ``drift`` when they differ, and the slots per mapping.

        With *view_fqn* — the gateway VIEW, i.e. a batch recompute of every
        triple from the sources — its totals are counted the same way; with
        *sync_rows* (the row count of the Lakebase synced table) that count is
        added. ``pairs`` says per pair whether it differs: ``slots_data``,
        ``view_data`` (rows and hash) and ``sync_data`` (rows only); a pair
        whose side was not given is left out. Without *slots_fqn* (a domain
        whose mappings are all batch has no ``_slots``) ``slots`` is ``None``,
        ``drift`` false and nothing is listed per mapping.
        """
        data = SlotReconciler.totals(client, data_fqn)
        slots = SlotReconciler.totals(client, slots_fqn) if slots_fqn else None
        result: dict = {"slots": slots, "data": data, "drift": slots is not None and slots != data}
        pairs = {"slots_data": result["drift"]} if slots is not None else {}
        if view_fqn:
            result["view"] = SlotReconciler.totals(client, view_fqn)
            pairs["view_data"] = result["view"] != data
        if sync_rows is not None:
            result["sync"] = {"rows": int(sync_rows)}
            pairs["sync_data"] = int(sync_rows) != data["rows"]
        if any(pairs.values()):
            logger.info(
                "Slot reconciliation of %s: drift %s (data %s, slots %s, view %s, sync %s)",
                data_fqn,
                sorted(name for name, differs in pairs.items() if differs),
                data,
                slots,
                result.get("view"),
                result.get("sync"),
            )
        result["pairs"] = pairs
        result["by_map_id"] = SlotReconciler.by_map_id(client, slots_fqn) if slots_fqn else []
        return result

    @staticmethod
    def _int(value: Any) -> int:
        """A warehouse count or decimal sum as ``int``; ``NULL`` (empty relation) is 0.

        Anything else that does not read as a number is an error, not a zero:
        two garbled answers must not compare as "no drift".
        """
        if value is None:
            return 0
        try:
            return int(Decimal(str(value)))
        except (InvalidOperation, ValueError) as exc:
            raise InfrastructureError(f"Unreadable warehouse count: {value!r}") from exc
