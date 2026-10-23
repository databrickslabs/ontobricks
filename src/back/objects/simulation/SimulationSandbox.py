"""Lifecycle of one ``<graph>__sim_<id>`` sandbox table."""

from __future__ import annotations

from typing import Any, Dict, List, Set

from back.core.errors import ValidationError
from back.core.graphdb.lakebase import LakebaseFlatStore, LakebaseSimulationStore
from back.core.logging import get_logger
from back.objects.simulation.SimulationDiff import SimulationDiff, TripleKey
from back.objects.simulation.SimulationPatch import SimulationPatch

logger = get_logger(__name__)


class SimulationSandbox:
    """Seed, patch and read a standalone sandbox copy of a live graph slice.

    The live store is only read (``copy_subjects_from``); every write goes
    through a :class:`LakebaseSimulationStore`, which refuses non-sandbox
    targets.
    """

    MAX_TRIPLES = 250_000

    def __init__(self, live_store: Any, graph_name: str, sim_id: str) -> None:
        if LakebaseFlatStore is None or not isinstance(live_store, LakebaseFlatStore):
            raise ValidationError("Simulations require the Lakebase graph backend")
        self._live = live_store
        self._graph_name = graph_name
        self.table = LakebaseSimulationStore.sandbox_table_name(graph_name, sim_id)
        self.store = LakebaseSimulationStore.from_store(live_store)

    def reset(self) -> None:
        self.store.drop_table(self.table)
        self.store.create_table(self.table)

    def drop(self) -> None:
        self.store.drop_table(self.table)

    def seed(self, entities: List[str]) -> int:
        source = self._live.sql_table_reference(self._graph_name)
        copied = self.store.copy_subjects_from(self.table, source, entities)
        total = self.store.count_triples(self.table)
        if total > self.MAX_TRIPLES:
            self.drop()
            raise ValidationError(
                f"Simulation slice has {total} triples, above the "
                f"{self.MAX_TRIPLES} limit — narrow the scope"
            )
        return copied

    def triples(self) -> List[Dict[str, str]]:
        return self.store.query_triples(self.table)

    def apply(self, patches: List[SimulationPatch]) -> Set[TripleKey]:
        """Apply *patches* in order; return the keys asserted by add/set patches."""
        asserted: Set[TripleKey] = set()
        for patch in patches:
            if patch.op == "set":
                self.store.delete_subject_predicate(
                    self.table, [{"subject": patch.subject, "predicate": patch.predicate}]
                )
                self.store.insert_triples(self.table, [patch.triple()])
                asserted = {
                    k for k in asserted if (k[0], k[1]) != (patch.subject, patch.predicate)
                }
                asserted.add(SimulationDiff.key(patch.triple()))
            elif patch.op == "add":
                self.store.insert_triples(self.table, [patch.triple()])
                asserted.add(SimulationDiff.key(patch.triple()))
            else:
                self.store.delete_triples(self.table, [patch.triple()])
                asserted.discard(SimulationDiff.key(patch.triple()))
        logger.info("Applied %d patches to sandbox %s", len(patches), self.table)
        return asserted
