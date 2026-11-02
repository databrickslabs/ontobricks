"""Lakebase implementation of the :class:`GraphSimulation` capability.

Facade over ``LakebaseSimulationStore`` / ``LakebaseSimulationUnion`` /
``_companion_ddl`` so ``simulation/**`` never imports ``lakebase.*`` directly.
"""

from __future__ import annotations

from typing import Any

from back.core.graphdb.GraphSimulation import GraphSimulation


class LakebaseSimulation(GraphSimulation):
    """Postgres overlay sandbox for a Lakebase domain."""

    def simulation_store(self, live_store: Any) -> Any:
        from back.core.graphdb.lakebase import LakebaseSimulationStore

        return LakebaseSimulationStore.from_store(live_store)

    def is_native_store(self, live_store: Any) -> bool:
        from back.core.graphdb.lakebase import LakebaseFlatStore

        return LakebaseFlatStore is not None and isinstance(
            live_store, LakebaseFlatStore
        )

    def sandbox_table_name(self, graph_name: str, sim_id: str) -> str:
        from back.core.graphdb.lakebase import LakebaseSimulationStore

        return LakebaseSimulationStore.sandbox_table_name(graph_name, sim_id)

    def sim_union_view_name(self, graph_name: str, sim_id: str) -> str:
        from back.core.graphdb.lakebase import _companion_ddl

        return _companion_ddl.sim_union_phy(graph_name, sim_id)

    def sim_table_name(self, graph_name: str, sim_id: str) -> str:
        from back.core.graphdb.lakebase import _companion_ddl

        return _companion_ddl.sim_phy(graph_name, sim_id)

    def drop_union(self, live_store: Any, graph_name: str, sim_id: str) -> None:
        from back.core.graphdb.lakebase import LakebaseSimulationUnion

        LakebaseSimulationUnion(live_store).drop_all(graph_name, sim_id)

    def ensure_union(
        self, live_store: Any, view: str, live_union: str, sandbox_table: str
    ) -> None:
        from back.core.graphdb.lakebase import LakebaseSimulationUnion

        LakebaseSimulationUnion(live_store).ensure(view, live_union, sandbox_table)
