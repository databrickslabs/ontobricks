"""Lifecycle of one ``<graph>__sim_<id>`` sandbox table."""

from __future__ import annotations

from typing import Any, Dict, List, Set

from back.core.errors import ValidationError
from back.core.graphdb.lakebase import LakebaseFlatStore, LakebaseSimulationStore
from back.core.helpers import is_uri
from back.core.logging import get_logger
from back.objects.digitaltwin.constants import RDF_TYPE
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
        self._sim_id = sim_id
        self.table = LakebaseSimulationStore.sandbox_table_name(graph_name, sim_id)
        self.store = LakebaseSimulationStore.from_store(live_store)

    def reset(self) -> None:
        self._drop_explorer_views()
        self.store.drop_table(self.table)
        self.store.create_table(self.table)

    def drop(self) -> None:
        self._drop_explorer_views()
        self.store.drop_table(self.table)

    def _drop_explorer_views(self) -> None:
        """Remove Explorer views before their sandbox dependency is dropped."""
        from back.core.graphdb.lakebase import LakebaseSimulationUnion

        LakebaseSimulationUnion(self._live).drop_all(self._graph_name, self._sim_id)

    def ensure_explorer_views(self, live_union: str, live_asserted: str) -> None:
        """Materialise inferred and asserted Explorer union views after a run."""
        from back.core.graphdb.lakebase import LakebaseSimulationUnion, _companion_ddl

        union = LakebaseSimulationUnion(self._live)
        for include_inferred, live in ((True, live_union), (False, live_asserted)):
            view = _companion_ddl.sim_union_phy(
                self._graph_name, self._sim_id, include_inferred=include_inferred
            )
            union.ensure(view, live, self.table)

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

    def seed_neighbourhood(self, uris: List[str], hops: int = 1) -> int:
        """Copy *uris* from live plus *hops* of related subjects.

        Patch objects (e.g. extra contracts on a customer) are often outside
        the original scope slice. Without their outgoing triples — and the
        meter subjects those triples point at — SWRL cannot chain
        ``hasContract`` to ``hasMeter``.
        """
        unique = list(dict.fromkeys(u for u in uris if u))
        if not unique:
            return 0
        copied = self.seed(unique)
        seen: Dict[str, None] = {u: None for u in unique}
        copied += self._seed_live_expand(unique, hops, seen)
        copied += self._seed_sandbox_walk(list(seen), max(hops, 1), seen)
        return copied

    def _seed_live_expand(
        self, unique: List[str], hops: int, seen: Dict[str, None]
    ) -> int:
        expand = getattr(self._live, "expand_and_fetch_subgraph", None)
        if hops < 1 or not callable(expand):
            return 0
        cap_entities = min(max(len(unique) * 40, 200), 5000)
        try:
            sub = expand(
                self._graph_name,
                unique,
                hops,
                cap_entities,
                cap_entities * 50,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Live neighbourhood expand failed: %s", exc)
            return 0
        extra: List[str] = []
        for triple in sub.get("results") or []:
            subj = (triple or {}).get("subject") or ""
            obj = (triple or {}).get("object") or ""
            if subj and subj not in seen:
                extra.append(subj)
            if (triple or {}).get("predicate") != RDF_TYPE and is_uri(obj) and obj not in seen:
                extra.append(obj)
        more = [u for u in dict.fromkeys(extra) if u not in seen]
        for u in more:
            seen[u] = None
        return self.seed(more) if more else 0

    def _seed_sandbox_walk(
        self, start: List[str], hops: int, seen: Dict[str, None]
    ) -> int:
        """Copy URI objects already stored on seeded subjects (e.g. meters)."""
        copied = 0
        frontier = list(start)
        for _ in range(max(hops, 1)):
            nxt: List[str] = []
            for uri in frontier:
                for obj in self.uri_objects_of(uri):
                    if obj not in seen:
                        seen[obj] = None
                        nxt.append(obj)
            if not nxt:
                break
            copied += self.seed(nxt)
            frontier = nxt
        return copied

    def uri_objects_of(self, subject: str) -> List[str]:
        """Sandbox URI objects of *subject*, excluding ``rdf:type`` class objects."""
        from back.core.helpers import SQL_DIALECT_POSTGRES, sql_literal

        store = getattr(self, "store", None)
        execute = getattr(store, "execute_query", None)
        table_ref = getattr(store, "sql_table_reference", None)
        if not subject or not callable(execute) or not callable(table_ref):
            return []
        rows = execute(
            f"SELECT object, predicate FROM {table_ref(self.table)} "
            f"WHERE subject = {sql_literal(subject, SQL_DIALECT_POSTGRES)}"
        ) or []
        seen: Dict[str, None] = {}
        for row in rows:
            obj = (row or {}).get("object") or ""
            if (row or {}).get("predicate") == RDF_TYPE or not is_uri(obj):
                continue
            seen.setdefault(obj, None)
        return list(seen)

    def seed_focus_for_swrl(self, entity_uri: str, hops: int = 2) -> int:
        """Copy the entity neighbourhood plus objects already linked in the sandbox.

        Explorer SWRL runs against the sandbox table, not the union view.
        Extra contracts added by a patch are already in the sandbox as
        objects of the focus entity; their live meters still need copying.
        """
        copied = self.seed_neighbourhood([entity_uri], hops=hops)
        extra = [u for u in self.uri_objects_of(entity_uri) if u != entity_uri]
        if extra:
            copied += self.seed_neighbourhood(extra, hops=1)
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
