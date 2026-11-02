"""What a Lakebase synced table reads, which key it syncs on, and how often."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SyncPlan:
    """One ``SyncedTableManager.ensure`` registration, decided by :class:`TwinLakebaseSync`.

    The source is ``…_data`` (table or materialized view) or the R2RML gateway
    VIEW — ``source_kind`` ``triples`` — or the ``…_slots`` streaming table
    (``slots``) when the slots carry the whole graph; the Postgres union view
    and the asserted view give both the triples shape.
    """

    source_fqn: str
    primary_key: tuple[str, ...]
    sync_mode: str
    source_kind: str = "triples"

    def ensure_kwargs(self) -> dict:
        """Keyword arguments of ``SyncedTableManager.ensure`` for this plan."""
        return {
            "source_table_full_name": self.source_fqn,
            "primary_key_columns": list(self.primary_key),
            "sync_mode": self.sync_mode,
        }
