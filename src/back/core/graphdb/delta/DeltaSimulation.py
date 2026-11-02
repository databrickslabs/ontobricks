"""Delta implementation of the :class:`GraphSimulation` capability.

The sandbox table + Explorer union view are built by the simulation sandbox
class itself (UC view DDL); this capability only vends the raw materialized
store so sandbox writes cannot be redirected into the live domain's inferred
companion.
"""

from __future__ import annotations

from typing import Any

from back.core.graphdb.interfaces.GraphSimulation import GraphSimulation


class DeltaSimulation(GraphSimulation):
    """Copy-on-write UC Delta sandbox for a Databricks domain."""

    def simulation_store(self, live_store: Any) -> Any:
        from back.core.graphdb.delta.DeltaMaterializedStore import (
            DeltaMaterializedStore,
        )

        return DeltaMaterializedStore(live_store._client)

    def is_native_store(self, live_store: Any) -> bool:
        from back.core.graphdb.delta.DeltaReadStore import DeltaReadStore

        return isinstance(live_store, DeltaReadStore)
