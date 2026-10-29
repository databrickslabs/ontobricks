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

class _LakebaseSimulationSandbox:
    """Seed, patch and read a standalone sandbox copy of a live graph slice.

    The live store is only read (``copy_subjects_from``); every write goes
    through a :class:`LakebaseSimulationStore`, which refuses non-sandbox
    targets.
    """

    MAX_TRIPLES = 250_000

    def __init__(self, live_store: Any, graph_name: str, sim_id: str) -> None:
        if LakebaseFlatStore is None or not isinstance(live_store, LakebaseFlatStore):
            raise ValidationError("Lakebase simulation sandbox requires Lakebase")
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

    def seed_neighbourhood(
        self, uris: List[str], hops: int = 1, inbound: bool = True
    ) -> int:
        """Copy *uris* from live plus *hops* of related subjects.

        Patch objects (e.g. extra contracts on a customer) are often outside
        the original scope slice. Without their outgoing triples — and the
        meter subjects those triples point at — SWRL cannot chain
        ``hasContract`` to ``hasMeter``.

        *inbound* follows live reverse edges (who already points at this
        contract). Leave it off for hypothesis objects: those contracts
        still belong to other customers in the live graph, and pulling
        their claims into the sandbox makes SWRL fire on the wrong people.
        """
        unique = list(dict.fromkeys(u for u in uris if u))
        if not unique:
            return 0
        copied = self.seed(unique)
        seen: Dict[str, None] = {u: None for u in unique}
        if inbound:
            copied += self._seed_live_expand(unique, hops, seen)
        # Walk only the URIs we were asked to seed — not every live-expand
        # neighbour (that is one SQL round-trip per subject and freezes the
        # run at "30% seeding neighbours").
        copied += self._seed_sandbox_walk(unique, max(hops, 1), seen)
        return copied

    def seed_referrers(self, uris: List[str]) -> int:
        """Copy live subjects that point at *uris* (incoming edges only).

        The scoped customer's claims are often ``Claim → customer`` rather
        than ``customer → hasClaim``. Hypothesis seeding must not pull
        every inbound neighbour of a patched contract, but the focus
        entity still needs those claim subjects in the sandbox.
        """
        unique = list(dict.fromkeys(u for u in uris if u))
        if not unique:
            return 0
        expand = getattr(self._live, "expand_and_fetch_subgraph", None)
        if not callable(expand):
            return 0
        cap_entities = min(max(len(unique) * 40, 200), 5000)
        try:
            sub = expand(
                self._graph_name,
                unique,
                1,
                cap_entities,
                cap_entities * 50,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Live referrer expand failed: %s", exc)
            return 0
        wanted = set(unique)
        extra: List[str] = []
        for triple in sub.get("results") or []:
            pred = (triple or {}).get("predicate") or ""
            obj = (triple or {}).get("object") or ""
            subj = (triple or {}).get("subject") or ""
            if pred == RDF_TYPE or obj not in wanted or not subj or subj in wanted:
                continue
            extra.append(subj)
        more = list(dict.fromkeys(extra))
        return self.seed(more) if more else 0

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
            nxt = [
                obj
                for obj in self.uri_objects_of_many(frontier)
                if obj not in seen
            ]
            if not nxt:
                break
            for obj in nxt:
                seen[obj] = None
            copied += self.seed(nxt)
            frontier = nxt
        return copied

    def uri_objects_of(self, subject: str) -> List[str]:
        """Sandbox URI objects of *subject*, excluding ``rdf:type`` class objects."""
        return self.uri_objects_of_many([subject] if subject else [])

    def uri_objects_of_many(self, subjects: List[str]) -> List[str]:
        """Sandbox URI objects of *subjects*, excluding ``rdf:type`` class objects."""
        from back.core.helpers import SQL_DIALECT_POSTGRES, sql_literal

        wanted = list(dict.fromkeys(s for s in subjects if s))
        store = getattr(self, "store", None)
        execute = getattr(store, "execute_query", None)
        table_ref = getattr(store, "sql_table_reference", None)
        if not wanted or not callable(execute) or not callable(table_ref):
            return []
        out: Dict[str, None] = {}
        chunk = 400
        for i in range(0, len(wanted), chunk):
            literals = ", ".join(
                sql_literal(s, SQL_DIALECT_POSTGRES) for s in wanted[i : i + chunk]
            )
            rows = execute(
                f"SELECT object, predicate FROM {table_ref(self.table)} "
                f"WHERE subject IN ({literals})"
            ) or []
            for row in rows:
                obj = (row or {}).get("object") or ""
                if (row or {}).get("predicate") == RDF_TYPE or not is_uri(obj):
                    continue
                out.setdefault(obj, None)
        return list(out)

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


class _StoreSimulationSandbox:
    """Backend-neutral copy-on-write sandbox for Delta and Neo4j.

    Both engines support an independently named SPO graph.  The source graph
    is read only; all mutations target ``table``.  Delta receives a raw store
    so its normal inferred-companion write routing cannot redirect sandbox
    mutations into the live domain.
    """

    MAX_TRIPLES = _LakebaseSimulationSandbox.MAX_TRIPLES

    def __init__(self, live_store: Any, graph_name: str, sim_id: str) -> None:
        self._live = live_store
        self._graph_name = graph_name
        self._sim_id = sim_id
        self.table = self._sandbox_name(graph_name, sim_id)
        self.store = self._sandbox_store(live_store)

    @staticmethod
    def _token(value: str) -> str:
        import re

        token = re.sub(r"[^a-zA-Z0-9_]", "_", value or "")
        return token.strip("_") or "simulation"

    def _sandbox_name(self, graph_name: str, sim_id: str) -> str:
        return f"{self._token(graph_name)}__sim_{self._token(sim_id)}"

    def _sandbox_store(self, live_store: Any) -> Any:
        return live_store

    def reset(self) -> None:
        self.drop()
        self.store.create_table(self.table)

    def drop(self) -> None:
        self._drop_explorer_views()
        self.store.drop_table(self.table)

    def _drop_explorer_views(self) -> None:
        return

    def ensure_explorer_views(self, live_union: str, live_asserted: str) -> None:
        return

    def seed(self, entities: List[str]) -> int:
        unique = list(dict.fromkeys(entity for entity in entities if entity))
        if not unique:
            return 0
        triples = self._live.get_triples_for_subjects(self._graph_name, unique)
        copied = self.store.insert_triples(self.table, triples) if triples else 0
        if self.store.count_triples(self.table) > self.MAX_TRIPLES:
            self.drop()
            raise ValidationError(
                f"Simulation slice has more than {self.MAX_TRIPLES} triples — narrow the scope"
            )
        return copied

    def seed_neighbourhood(
        self, uris: List[str], hops: int = 1, inbound: bool = True
    ) -> int:
        unique = list(dict.fromkeys(uri for uri in uris if uri))
        if not unique:
            return 0
        copied = self.seed(unique)
        seen = set(unique)
        frontier = unique
        for _ in range(max(hops, 0)):
            next_uris = [
                triple.get("object", "")
                for triple in self.store.query_triples(self.table)
                if triple.get("subject") in frontier
                and triple.get("predicate") != RDF_TYPE
                and is_uri(triple.get("object", ""))
                and triple.get("object") not in seen
            ]
            frontier = list(dict.fromkeys(next_uris))
            if not frontier:
                break
            seen.update(frontier)
            copied += self.seed(frontier)
        return copied

    def seed_referrers(self, uris: List[str]) -> int:
        unique = list(dict.fromkeys(uri for uri in uris if uri))
        if not unique:
            return 0
        expand = getattr(self._live, "expand_and_fetch_subgraph", None)
        if not callable(expand):
            return 0
        try:
            sub = expand(self._graph_name, unique, 1, 200, 10_000)
        except Exception:  # noqa: BLE001
            return 0
        wanted = set(unique)
        extra = [
            (triple or {}).get("subject") or ""
            for triple in sub.get("results") or []
            if (triple or {}).get("predicate") != RDF_TYPE
            and (triple or {}).get("object") in wanted
            and (triple or {}).get("subject") not in wanted
        ]
        more = [uri for uri in dict.fromkeys(extra) if uri]
        return self.seed(more) if more else 0

    def seed_focus_for_swrl(self, entity_uri: str, hops: int = 2) -> int:
        return self.seed_neighbourhood([entity_uri], hops=hops)

    def uri_objects_of(self, subject: str) -> List[str]:
        return [
            triple["object"]
            for triple in self.store.query_triples(self.table)
            if triple.get("subject") == subject
            and triple.get("predicate") != RDF_TYPE
            and is_uri(triple.get("object", ""))
        ]

    def uri_objects_of_many(self, subjects: List[str]) -> List[str]:
        wanted = set(subjects)
        return list(
            dict.fromkeys(
                triple["object"]
                for triple in self.store.query_triples(self.table)
                if triple.get("subject") in wanted
                and triple.get("predicate") != RDF_TYPE
                and is_uri(triple.get("object", ""))
            )
        )

    def triples(self) -> List[Dict[str, str]]:
        return self.store.query_triples(self.table)

    def apply(self, patches: List[SimulationPatch]) -> Set[TripleKey]:
        asserted: Set[TripleKey] = set()
        for patch in patches:
            if patch.op == "set":
                current = [
                    triple for triple in self.triples()
                    if triple.get("subject") == patch.subject
                    and triple.get("predicate") == patch.predicate
                ]
                if current:
                    self.store.delete_triples(self.table, current)
                self.store.insert_triples(self.table, [patch.triple()])
                asserted = {
                    key for key in asserted
                    if (key[0], key[1]) != (patch.subject, patch.predicate)
                }
                asserted.add(SimulationDiff.key(patch.triple()))
            elif patch.op == "add":
                self.store.insert_triples(self.table, [patch.triple()])
                asserted.add(SimulationDiff.key(patch.triple()))
            else:
                self.store.delete_triples(self.table, [patch.triple()])
                asserted.discard(SimulationDiff.key(patch.triple()))
        return asserted


class _DeltaSimulationSandbox(_StoreSimulationSandbox):
    """UC Delta sandbox table and predicate-overlay Explorer views."""

    def _sandbox_name(self, graph_name: str, sim_id: str) -> str:
        source = self._live._sql_relation(graph_name)
        if source.count(".") != 2:
            raise ValidationError("Delta simulations require a Unity Catalog graph table")
        return f"{source}__sim_{self._token(sim_id)}"

    def _sandbox_store(self, live_store: Any) -> Any:
        from back.core.graphdb.delta import DeltaFlatStore

        return DeltaFlatStore(live_store._client)

    def _view_name(self, inferred: bool) -> str:
        suffix = "smu" if inferred else "sms"
        return self.table.replace("__sim_", f"__{suffix}_")

    def _drop_explorer_views(self) -> None:
        for inferred in (True, False):
            self.store._client.execute_statement(f"DROP VIEW IF EXISTS {self._view_name(inferred)}")

    def ensure_explorer_views(self, live_union: str, live_asserted: str) -> None:
        for inferred, live in ((True, live_union), (False, live_asserted)):
            view = self._view_name(inferred)
            self.store._client.execute_statement(
                f"CREATE OR REPLACE VIEW {view} AS "
                f"SELECT l.subject, l.predicate, l.object FROM {live} l "
                f"WHERE NOT EXISTS (SELECT 1 FROM {self.table} s "
                f"WHERE s.subject = l.subject AND s.predicate = l.predicate) "
                f"UNION ALL SELECT subject, predicate, object FROM {self.table}"
            )


class _Neo4jSimulationSandbox(_StoreSimulationSandbox):
    """Marker-label-isolated Neo4j sandbox graph."""


class SimulationSandbox(_LakebaseSimulationSandbox):
    """Create the isolation implementation for the active graph backend."""

    def __new__(cls, live_store: Any, graph_name: str, sim_id: str) -> Any:
        from back.core.graphdb.delta import DeltaFlatStore
        from back.core.graphdb.neo4j import Neo4jStore

        if LakebaseFlatStore is not None and isinstance(live_store, LakebaseFlatStore):
            return super().__new__(cls)
        if isinstance(live_store, DeltaFlatStore):
            return _DeltaSimulationSandbox(live_store, graph_name, sim_id)
        if isinstance(live_store, Neo4jStore):
            return _Neo4jSimulationSandbox(live_store, graph_name, sim_id)
        raise ValidationError(
            f"Simulations are unsupported for graph store {type(live_store).__name__}"
        )
