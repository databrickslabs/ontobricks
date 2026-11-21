"""Answer a :class:`PathQueryBuilder` plan with SQL over one arm's graph relation.

Each plan compiles to nested semi-joins over the arm's ``…_graph`` view
(``subject, predicate, object``): the filter class's matching nodes, then
one hop per edge back towards the output class, then the output type. The
statement runs through the arm's graph store on the query warehouse
(Lakehouse//RT when configured), bounded by the graph statement timeout.

Why not the GraphQL list resolvers: they list the output class from the
root (``limit`` rows) and only follow relationships forwards, so a
selective question over a large graph either truncates or cannot be
expressed. Store errors also raise here instead of reading as an empty
answer.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from back.core.errors import InfrastructureError, OntoBricksError
from back.core.helpers.SQLHelpers import SQLHelpers

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

Execute = Callable[[str], List[Dict[str, Any]]]


def _lit(value: str) -> str:
    return SQLHelpers.sql_literal(str(value))


def local_id(uri: str) -> str:
    return str(uri).rstrip("/").rsplit("/", 1)[-1].rsplit("#", 1)[-1]


class ArmSqlQuery:
    """Compiles and runs plans on one arm; ``run`` intersects a plan's answers."""

    MAX_IDS = 200_000

    def __init__(
        self,
        domain: Any,
        settings: Any,
        *,
        execute: Optional[Execute] = None,
        table: str = "",
    ):
        """*execute* / *table* default to the arm's graph store and union relation."""
        if execute is None or not table:
            from back.core.graphdb import get_graphdb
            from back.objects.digitaltwin import TwinGraphAccess

            store = get_graphdb(domain, settings)
            if not store:
                raise InfrastructureError("Graph backend not configured or unreachable")
            execute = execute or store.execute_query
            table = table or TwinGraphAccess.graph_query_table(
                domain, settings, store, include_inferred=True
            )
        self._execute = execute
        self._table = table

    @property
    def table(self) -> str:
        return self._table

    def _typed(self, class_uri: str) -> str:
        return (
            f"SELECT subject FROM {self._table} "
            f"WHERE predicate = {_lit(RDF_TYPE)} AND object = {_lit(class_uri)}"
        )

    def sql(self, item: Dict[str, Any]) -> str:
        t = self._table
        match = item.get("match")
        typed = self._typed(item["filter_uri"])
        if match is None:
            cur = f"SELECT subject AS n FROM {t} WHERE subject IN ({typed})"
        elif match["kind"] == "id":
            tail = "/" + match["value"]
            cur = (
                f"SELECT subject AS n FROM {t} WHERE subject IN ({typed}) "
                f"AND substr(subject, -{len(tail)}) = {_lit(tail)}"
            )
        else:
            cur = (
                f"SELECT subject AS n FROM {t} WHERE predicate = {_lit(match['predicate'])} "
                f"AND object = {_lit(match['value'])} "
                f"AND subject IN ({self._typed(item['filter_uri'])})"
            )
        for edge in reversed(item["edges"]):
            p = _lit(edge["predicate"])
            if edge["forward"]:
                cur = f"SELECT subject AS n FROM {t} WHERE predicate = {p} AND object IN ({cur})"
            else:
                cur = f"SELECT object AS n FROM {t} WHERE predicate = {p} AND subject IN ({cur})"
        return (
            f"SELECT DISTINCT n FROM ({cur}) q "
            f"WHERE n IN ({self._typed(item['output_uri'])}) LIMIT {self.MAX_IDS + 1}"
        )

    def run(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """``{"status": answered|unresolved|error, "ids", "truncated", "queries", "detail"}``."""
        sqls = [self.sql(q) for q in plan["queries"]]
        if plan["unresolved"]:
            return {
                "status": "unresolved",
                "ids": [],
                "truncated": False,
                "queries": sqls,
                "detail": plan["unresolved"],
            }
        ids = None
        truncated = False
        try:
            for statement in sqls:
                rows = self._execute(statement)
                truncated = truncated or len(rows) > self.MAX_IDS
                found = {local_id(r["n"]) for r in rows[: self.MAX_IDS]}
                ids = found if ids is None else ids & found
        except OntoBricksError as exc:
            return {
                "status": "error",
                "ids": [],
                "truncated": truncated,
                "queries": sqls,
                "detail": str(exc.detail or exc.message)[:300],
            }
        except Exception as exc:  # noqa: BLE001 — a failed read is not an empty answer
            return {
                "status": "error",
                "ids": [],
                "truncated": truncated,
                "queries": sqls,
                "detail": str(exc)[:300],
            }
        return {
            "status": "answered",
            "ids": sorted(ids or []),
            "truncated": truncated,
            "queries": sqls,
            "detail": "",
        }
