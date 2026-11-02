"""The name a Lakebase synced table is registered under: its own, or a ghost-state fallback."""

from __future__ import annotations

from typing import Any


class SyncedTableName:
    """Which registration of a synced table is in use, in ``SyncedTableManager.ensure``'s order.

    Collaborator of :class:`~back.core.graphdb.lakebase.SyncedTableManager.SyncedTableManager`
    (like ``ContinuousSyncWait``): ``ensure`` falls back to ``<name>_b`` /
    ``_c`` / ``_d`` when the primary name has a ghost control-plane
    reservation, and a caller that re-creates the union view later (without
    the ``ensure`` result at hand) must point it at the same table.
    """

    @staticmethod
    def registered(mgr: Any, name: str) -> str:
        """The first of *name*, *name*``_b`` … with a usable registration; *name* when none has."""
        from back.core.graphdb.lakebase.SyncedTableManager import (
            _TERMINAL_FAIL,
            SyncedTableManager,
        )

        fallbacks = (f"{name}{suffix}" for suffix in SyncedTableManager._GHOST_FALLBACK_SUFFIXES)
        for candidate in (name, *fallbacks):
            synced = mgr.get(candidate)
            if synced is not None and mgr._extract_state(synced) not in _TERMINAL_FAIL:
                return candidate
        return name
