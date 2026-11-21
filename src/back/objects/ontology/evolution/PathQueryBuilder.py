"""Turn a typed intent into deterministic path plans for one arm.

The plan is built from the arm's own ontology with the same path rule the
coverage check uses (shortest unique path within the edge budget, narrowed
by ``path_hint``), so the parent arm of a missing-relationship question has
no plan at all and is reported *unresolved* rather than guessed.

Each plan names the predicate URIs the build emits, following the
``GraphQLSchemaBuilder`` rules (``<base>/<name>`` with ``#`` normalised to
``/``, class URIs as declared). An attribute mapped to the entity's label
column is matched on ``rdfs:label``, because the build emits it only there.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.objects.ontology.evolution.Intent import Intent
from back.objects.ontology.evolution.IntentFilter import IntentFilter
from back.objects.ontology.evolution.OntologyIndex import OntologyIndex

RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


class PathQueryBuilder:
    """One plan per filter; the arm answer is the intersection of their answers."""

    def __init__(self, index: OntologyIndex):
        from back.core.graphql.GraphQLSchemaBuilder import GraphQLSchemaBuilder

        self._index = index
        self._norm = GraphQLSchemaBuilder.normalize_base(index.base_uri)

    def _uri(self, item: Dict[str, Any]) -> str:
        uri = str(item.get("uri") or "")
        return uri if uri.startswith(self._norm) else self._norm + str(item.get("name"))

    def class_uri(self, cls: str) -> str:
        return str(self._index.classes[cls].get("uri") or self._norm + cls)

    def plan(self, intent: Intent) -> Dict[str, Any]:
        """``{"queries": [...], "unresolved": [{"filter", "reason"}]}``."""
        output = intent.output_class
        if not output or output not in self._index.classes:
            return {
                "queries": [],
                "unresolved": [{"filter": "output", "reason": "unknown_output_class"}],
            }
        queries: List[Dict[str, Any]] = []
        unresolved: List[Dict[str, str]] = []
        for flt in list(intent.filters) or [None]:
            item, reason = self._one(output, flt, intent)
            if item:
                queries.append(item)
            else:
                unresolved.append({"filter": self._key(flt), "reason": reason})
        return {"queries": queries, "unresolved": unresolved}

    @staticmethod
    def _key(flt: Optional[IntentFilter]) -> str:
        return f"{flt.cls}.{flt.attribute}={flt.value}" if flt else "all"

    def _one(self, output: str, flt: Optional[IntentFilter], intent: Intent):
        base = {"filter": self._key(flt), "output_uri": self.class_uri(output)}
        if flt is None:
            return {
                **base,
                "filter_uri": base["output_uri"],
                "edges": [],
                "match": None,
            }, ""
        if flt.op != "eq":
            return None, "unsupported_operator"
        if flt.cls not in self._index.classes or not (
            flt.attribute == "id" or self._index.attribute(flt.cls, flt.attribute)
        ):
            return None, "unknown_attribute"
        edges: List[Dict[str, Any]] = []
        if flt.cls != output:
            path = self._path(output, flt.cls, intent)
            if path is None:
                return None, "no_unique_path"
            for name, a, _b in path:
                rel = self._index.relationships[name]
                edges.append(
                    {"predicate": self._uri(rel), "forward": rel["domain"] == a}
                )
        return {
            **base,
            "filter_uri": self.class_uri(flt.cls),
            "edges": edges,
            "match": self._match(flt),
        }, ""

    def _match(self, flt: IntentFilter) -> Dict[str, str]:
        if flt.attribute == "id":
            return {"kind": "id", "value": str(flt.value)}
        entity = self._index.entity(flt.cls) or {}
        column = self._index.attribute_column(flt.cls, flt.attribute)
        if column and column == entity.get("label_column"):
            return {
                "kind": "predicate",
                "predicate": RDFS_LABEL,
                "value": str(flt.value),
            }
        attr = self._index.attribute(flt.cls, flt.attribute) or {}
        return {
            "kind": "predicate",
            "predicate": self._uri({"uri": attr.get("uri"), "name": flt.attribute}),
            "value": str(flt.value),
        }

    def _path(self, start: str, goal: str, intent: Intent):
        paths = self._index.paths(start, goal, intent.max_edges)
        if intent.path_hint:
            paths = [
                p for p in paths if set(intent.path_hint) <= {e[0] for e in p}
            ] or paths
        if not paths:
            return None
        shortest = [p for p in paths if len(p) == len(paths[0])]
        return shortest[0] if len(shortest) == 1 else None
