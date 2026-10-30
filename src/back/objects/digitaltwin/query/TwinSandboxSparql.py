"""SPARQL over a simulation sandbox snapshot (copied slice + patches)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from rdflib import Graph, Literal, URIRef
from rdflib.term import BNode

from back.core.errors import ValidationError
from back.core.graphdb import get_graphdb
from back.core.w3c import sparql as sparql_mod
from back.objects.digitaltwin.TwinGraphAccess import TwinGraphAccess
from back.objects.simulation import Simulation


class TwinSandboxSparql:
    """Run a read-only SPARQL query against a ready sandbox table.

    The physical sandbox (not the Explorer union view) is loaded into RDFLib
    so SELECT queries see hypothetical facts. Large snapshots are refused.
    """

    MAX_TRIPLES = 20000

    def __init__(self, domain: Any, settings: Any) -> None:
        self._domain = domain
        self._settings = settings

    def execute(
        self,
        query: str,
        limit: Optional[int],
        simulation_id: str,
    ) -> Dict[str, Any]:
        sid = TwinGraphAccess.sandbox_id(simulation_id)
        if not sid:
            raise ValidationError("simulation_id is required for sandbox SPARQL")
        query = sparql_mod.require_read_only_sparql(query)
        store = get_graphdb(self._domain, self._settings)
        if store is None:
            raise ValidationError("Graph backend not configured")
        table = Simulation(self._domain, self._settings).sandbox_relation(
            sid, store=store
        )
        total = store.paginated_count(table, [])
        if total > self.MAX_TRIPLES:
            raise ValidationError(
                "This sandbox is too large for SPARQL in-memory. "
                "Use GET /api/v1/digitaltwin/triples or GraphQL with simulation_id."
            )
        graph = self._load(store, table, total)
        rows, columns = self._run(graph, query, limit)
        return {
            "success": True,
            "results": rows,
            "columns": columns,
            "count": len(rows),
            "engine": "sandbox",
            "generated_sql": f"-- SPARQL on sandbox table {table} ({total} triples)",
            "simulation_id": sid,
        }

    def _load(self, store: Any, table: str, total: int) -> Graph:
        graph = Graph()
        for row in store.paginated_triples(table, [], max(total, 1), 0):
            graph.add(self._triple(row))
        return graph

    @staticmethod
    def _triple(row: dict) -> tuple:
        subject = TwinSandboxSparql._term(row.get("subject") or "")
        predicate = TwinSandboxSparql._term(row.get("predicate") or "")
        obj = TwinSandboxSparql._term(row.get("object") or "")
        return (subject, predicate, obj)

    @staticmethod
    def _term(raw: str):
        text = str(raw or "")
        if text.startswith("_:"):
            return BNode(text[2:])
        if text.startswith("http://") or text.startswith("https://") or text.startswith("urn:"):
            return URIRef(text)
        return Literal(text)

    @staticmethod
    def _run(graph: Graph, query: str, limit: Optional[int]) -> tuple[List[dict], List[str]]:
        result = graph.query(query)
        vars_ = list(result.vars or [])
        names = [str(v).lstrip("?") for v in vars_]
        rows: List[dict] = []
        cap = int(limit) if limit else None
        for binding in result:
            if cap is not None and len(rows) >= cap:
                break
            if names:
                rows.append(
                    {
                        name: TwinSandboxSparql._n3(binding[var])
                        for name, var in zip(names, vars_)
                    }
                )
            else:
                rows.append({"value": TwinSandboxSparql._n3(binding)})
        return rows, names or ["value"]

    @staticmethod
    def _n3(term: Any) -> str:
        if term is None:
            return ""
        to_n3 = getattr(term, "n3", None)
        if callable(to_n3):
            return str(term)
        return str(term)
