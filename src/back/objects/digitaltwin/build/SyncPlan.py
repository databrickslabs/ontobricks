"""What a Lakebase synced table reads, which key it syncs on, and how often."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SyncPlan:
    """One ``SyncedTableManager.ensure`` registration, decided by :class:`TwinLakebaseSync`.

    ``source_kind`` is ``"slots"`` for a ``…_slots`` source (the Postgres union
    view must project three columns and de-duplicate) and ``"triples"`` for
    ``…_data`` or the R2RML VIEW.
    """

    source_fqn: str
    primary_key: tuple[str, ...]
    sync_mode: str
    source_kind: str

    def ensure_kwargs(self) -> dict:
        """Keyword arguments of ``SyncedTableManager.ensure`` for this plan."""
        return {
            "source_table_full_name": self.source_fqn,
            "primary_key_columns": list(self.primary_key),
            "sync_mode": self.sync_mode,
        }
