"""Drift check between the ``_slots`` streaming table and the ``_data`` relation."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from back.core.helpers import WarehouseStatements
from back.core.logging import get_logger

logger = get_logger(__name__)

#: One 64-bit hash per triple, summed as a 38-digit decimal so a large graph
#: cannot overflow the BIGINT ``sum`` (ANSI mode would raise instead of wrap).
_TRIPLE_HASH_SUM = "sum(CAST(xxhash64(subject, predicate, object) AS DECIMAL(38, 0))) AS h"


class SlotReconciler:
    """Row counts and triple hashes of ``_slots`` versus ``_data``.

    The incremental ``_data`` materialized view is the union of the slot table
    and the batch-mapping branches, so its rows and hashes equal the slot
    table's only when every mapping is row-local. With batch mappings present
    a mismatch is expected and ``drift`` is not meaningful; the caller
    (:class:`TwinSlotReconciliation`) reports ``batch_branches_present`` so a
    reader can tell the two apart.

    Totals are computed over distinct triples (two slots may project the same
    triple; the MV groups them). ``by_map_id`` is slots-only: ``_data`` carries
    no ``map_id``. The only I/O is ``client.execute_query``; driver errors
    become :class:`InfrastructureError` and an unsafe name
    :class:`ValidationError` (both via :class:`WarehouseStatements`).
    """

    @staticmethod
    def counts_sql(fqn: str, *, group_by_map_id: bool = False) -> str:
        """``count(*)`` and the triple-hash sum of *fqn*, per ``map_id`` on request."""
        WarehouseStatements.require_fqn(fqn, what="relation name")
        if group_by_map_id:
            return (
                f"SELECT map_id, count(*) AS n, {_TRIPLE_HASH_SUM} "
                f"FROM {fqn} GROUP BY map_id ORDER BY map_id"
            )
        return (
            f"SELECT count(*) AS n, {_TRIPLE_HASH_SUM} "
            f"FROM (SELECT DISTINCT subject, predicate, object FROM {fqn})"
        )

    @staticmethod
    def totals(client: Any, fqn: str) -> dict:
        """``{"rows": int, "hash": int}`` of the distinct triples in *fqn*."""
        rows = WarehouseStatements.query(
            client, SlotReconciler.counts_sql(fqn), f"count the triples of {fqn}"
        )
        row = rows[0] if rows and isinstance(rows[0], Mapping) else {}
        return {"rows": SlotReconciler._int(row.get("n")), "hash": SlotReconciler._int(row.get("h"))}

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
    def compare(client: Any, slots_fqn: str, data_fqn: str) -> dict:
        """Totals of both relations, ``drift`` when they differ, and the slots per mapping."""
        slots = SlotReconciler.totals(client, slots_fqn)
        data = SlotReconciler.totals(client, data_fqn)
        drift = slots != data
        if drift:
            logger.info(
                "Slot reconciliation: %s has %d rows / hash %s, %s has %d rows / hash %s",
                slots_fqn,
                slots["rows"],
                slots["hash"],
                data_fqn,
                data["rows"],
                data["hash"],
            )
        return {
            "slots": slots,
            "data": data,
            "drift": drift,
            "by_map_id": SlotReconciler.by_map_id(client, slots_fqn),
        }

    @staticmethod
    def _int(value: Any) -> int:
        """A warehouse count or decimal sum as ``int``; ``NULL`` (empty relation) is 0."""
        if value is None:
            return 0
        try:
            return int(Decimal(str(value)))
        except (InvalidOperation, ValueError):
            return 0
