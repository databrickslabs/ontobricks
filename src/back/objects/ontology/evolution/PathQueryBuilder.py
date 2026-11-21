"""Turn a typed intent into deterministic GraphQL path queries for one arm.

The query is built from the arm's own ontology with the same path rule the
coverage check uses (shortest unique path within the edge budget, narrowed
by ``path_hint``), so the parent arm of a missing-relationship question has
no plan at all and is reported *unresolved* rather than guessed.

GraphQL object fields only follow a relationship from its domain to its
range, so a path is queried from the output class when every edge points
away from it, from the filter class when every edge points towards it, and
is unresolved when the directions are mixed. Field names follow
``GraphQLSchemaBuilder`` (plural root field, camel-cased attributes).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.objects.ontology.evolution.Intent import Intent
from back.objects.ontology.evolution.IntentFilter import IntentFilter
from back.objects.ontology.evolution.OntologyIndex import OntologyIndex


def _field(name: str) -> str:
    from strawberry.utils.str_converters import to_camel_case

    from back.core.graphql.GraphQLSchemaBuilder import GraphQLSchemaBuilder

    return to_camel_case(GraphQLSchemaBuilder.safe_name(name))


def _root_field(cls: str) -> str:
    from back.core.graphql.GraphQLSchemaBuilder import GraphQLSchemaBuilder

    return GraphQLSchemaBuilder.pluralize(cls)


class PathQueryBuilder:
    """Plans one GraphQL query per filter; the arm answer is their intersection."""

    LIMIT = 1000

    def __init__(self, index: OntologyIndex):
        self._index = index

    def plan(self, intent: Intent) -> Dict[str, Any]:
        """``{"queries": [...], "unresolved": [{"filter", "reason"}], "depth"}``."""
        output = intent.output_class
        queries: List[Dict[str, Any]] = []
        unresolved: List[Dict[str, str]] = []
        if not output or output not in self._index.classes:
            return {
                "queries": [],
                "unresolved": [{"filter": "output", "reason": "unknown_output_class"}],
                "depth": 1,
            }
        filters = list(intent.filters) or [None]
        for flt in filters:
            item, reason = self._one(output, flt, intent)
            if item:
                queries.append(item)
            else:
                unresolved.append({"filter": self._key(flt), "reason": reason})
        depth = max([len(q["steps"]) for q in queries] + [1])
        return {"queries": queries, "unresolved": unresolved, "depth": depth}

    @staticmethod
    def _key(flt: Optional[IntentFilter]) -> str:
        return f"{flt.cls}.{flt.attribute}={flt.value}" if flt else "all"

    def _one(self, output: str, flt: Optional[IntentFilter], intent: Intent):
        if flt is None:
            return self._query(output, [], None, "root"), ""
        if flt.op != "eq":
            return None, "unsupported_operator"
        if flt.cls not in self._index.classes or not (
            flt.attribute == "id" or self._index.attribute(flt.cls, flt.attribute)
        ):
            return None, "unknown_attribute"
        if flt.cls == output:
            return self._query(output, [], flt, "root"), ""
        path = self._path(output, flt.cls, intent)
        if path is None:
            return None, "no_unique_path"
        forward = [self._index.relationships[n]["domain"] == a for n, a, _ in path]
        if all(forward):
            return (
                self._query(output, [n for n, _, _ in path], flt, "forward"),
                "",
            )
        if not any(forward):
            steps = [n for n, _, _ in reversed(path)]
            return self._query(flt.cls, steps, flt, "reverse"), ""
        return None, "mixed_direction_path"

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

    def _attribute_field(self, flt: IntentFilter) -> str:
        """GraphQL field holding the filter attribute's value.

        ``id`` is the entity id. An attribute mapped to the entity's label
        column is emitted only as ``rdfs:label`` by the build, so GraphQL
        exposes it as ``label`` and the attribute field itself is null.
        """
        if flt.attribute == "id":
            return "id"
        entity = self._index.entity(flt.cls) or {}
        column = self._index.attribute_column(flt.cls, flt.attribute)
        if column and column == entity.get("label_column"):
            return "label"
        return _field(flt.attribute)

    def _query(
        self,
        root: str,
        steps: List[str],
        flt: Optional[IntentFilter],
        mode: str,
    ) -> Dict[str, Any]:
        attribute = self._attribute_field(flt) if flt else ""
        fields = [_field(s) for s in steps]
        # Forward: the filter attribute sits on the leaf. Reverse and root:
        # it sits on the root, and ids are collected at the leaf.
        leaf = "id" + (f" {attribute}" if attribute and mode == "forward" else "")
        body = leaf
        for name in reversed(fields):
            body = f"{name} {{ {body} }}"
        root_attr = f" {attribute}" if attribute and mode in ("root", "reverse") else ""
        if fields:
            selection = f"id{root_attr} {body}"
        else:
            selection = f"id{root_attr}"
        root_field = _root_field(root)
        return {
            "filter": self._key(flt),
            "mode": mode,
            "root": root,
            "root_field": root_field,
            "steps": fields,
            "attribute": attribute,
            "value": flt.value if flt else None,
            "query": f"{{ {root_field}(limit: {self.LIMIT}) {{ {selection} }} }}",
        }

    # -- evaluation of a query result -----------------------------------

    @classmethod
    def answer(cls, item: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
        """Ids of the output class selected by *item*; flags a truncated root."""
        rows = (data or {}).get(item["root_field"]) or []
        truncated = len(rows) >= cls.LIMIT
        ids: set = set()
        attr, value, steps = item["attribute"], item["value"], item["steps"]
        for row in rows:
            if item["mode"] == "root":
                if not attr or str(row.get(attr)) == str(value):
                    ids.add(str(row.get("id")))
            elif item["mode"] == "forward":
                if any(str(leaf.get(attr)) == str(value) for leaf in _walk(row, steps)):
                    ids.add(str(row.get("id")))
            elif str(row.get(attr)) == str(value):
                ids.update(str(leaf.get("id")) for leaf in _walk(row, steps))
        ids.discard("None")
        return {"ids": sorted(ids), "truncated": truncated, "root_rows": len(rows)}


def _walk(node: Dict[str, Any], steps: List[str]):
    if not steps:
        yield node
        return
    for child in node.get(steps[0]) or []:
        yield from _walk(child, steps[1:])
