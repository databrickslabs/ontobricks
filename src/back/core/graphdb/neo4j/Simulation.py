"""Neo4j implementation of the :class:`GraphSimulation` capability.

Neo4j isolates a sandbox by marker label on the same store, so the simulation
store is the live store itself; the sandbox class handles label isolation.
"""

from __future__ import annotations

from typing import Any

from back.core.graphdb.interfaces.GraphSimulation import GraphSimulation


class Simulation(GraphSimulation):
    """Marker-label-isolated sandbox for a Neo4j domain."""

    def simulation_store(self, live_store: Any) -> Any:
        return live_store

    def is_native_store(self, live_store: Any) -> bool:
        from back.core.graphdb.neo4j.Store import Store

        return isinstance(live_store, Store)
