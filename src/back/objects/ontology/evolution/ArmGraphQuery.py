"""Run a :class:`PathQueryBuilder` plan against one arm's GraphQL schema.

Uses the same pieces as ``/graphql/{domain}``: the schema generated from the
arm's ontology (cached under the arm's run-owned display name), the arm's
graph store and its union graph relation. Store reads are probed directly
first because the GraphQL list resolver logs and swallows store errors,
which would otherwise read as an empty, plausible answer.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from back.core.errors import InfrastructureError, ValidationError
from back.objects.ontology.evolution.PathQueryBuilder import PathQueryBuilder

Execute = Callable[[str, int], Dict[str, Any]]


class ArmGraphQuery:
    """GraphQL execution for one arm domain; ``run`` combines a plan's queries."""

    def __init__(
        self, domain: Any, settings: Any, *, store: Any = None, table: str = ""
    ):
        """*store* / *table* default to the arm's graph store and union relation."""
        from back.core.graphql import build_schema_for_domain
        from shared.config.constants import DEFAULT_BASE_URI

        ontology = domain.ontology or {}
        self._base_uri = ontology.get("base_uri", DEFAULT_BASE_URI)
        self._class_uris = {
            c.get("name"): c.get("uri") for c in ontology.get("classes") or []
        }
        built = build_schema_for_domain(
            ontology.get("classes", []),
            ontology.get("properties", []),
            self._base_uri,
            (domain.info or {}).get("name", ""),
        )
        if not built:
            raise ValidationError("The arm ontology cannot back a GraphQL schema")
        self._schema = built[0]
        if store is None:
            from back.core.graphdb import get_graphdb

            store = get_graphdb(domain, settings)
        if not store:
            raise InfrastructureError("Graph backend not configured or unreachable")
        if not table:
            from back.objects.digitaltwin import TwinGraphAccess

            table = TwinGraphAccess.graph_query_table(
                domain, settings, store, include_inferred=True
            )
        # Arms are built without search companions (``…_props``, adjacency),
        # so read the graph relation directly, like ``?cache=false``.
        from back.core.graphdb.SearchCachePolicy import SearchCachePolicy

        SearchCachePolicy.apply_search_cache_to_store(store, domain, False)
        self._store, self._table = store, table

    @property
    def table(self) -> str:
        return self._table

    def probe(self, cls: str) -> None:
        """Raise when the store cannot list or load *cls* entities of the arm graph."""
        try:
            subjects = self._store.find_subjects_by_type(
                self._table, self._class_uris.get(cls, ""), 1, 0, None
            )
            if subjects:
                # Triple loading is swallowed by the resolver too; read one.
                self._store.get_triples_for_subjects(self._table, list(subjects))
        except Exception as exc:  # noqa: BLE001
            raise InfrastructureError(
                "The arm graph could not be read", detail=str(exc)[:300]
            ) from exc

    def execute(self, query: str, depth: int) -> Dict[str, Any]:
        result = self._schema.execute_sync(
            query,
            context_value={
                "triplestore": self._store,
                "table_name": self._table,
                "base_uri": self._base_uri,
                "depth": depth,
            },
        )
        if result.errors:
            raise InfrastructureError(
                "GraphQL query failed", detail=str(result.errors[0])[:300]
            )
        return result.data or {}

    @staticmethod
    def run(
        plan: Dict[str, Any],
        execute: Execute,
        probe: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """``{"status": answered|unresolved|error, "ids", "truncated", "queries", "detail"}``."""
        if plan["unresolved"]:
            return {
                "status": "unresolved",
                "ids": [],
                "truncated": False,
                "queries": [q["query"] for q in plan["queries"]],
                "detail": plan["unresolved"],
            }
        ids = None
        truncated = False
        try:
            for item in plan["queries"]:
                if probe is not None:
                    probe(item["root"])
                answer = PathQueryBuilder.answer(
                    item, execute(item["query"], plan["depth"])
                )
                truncated = truncated or answer["truncated"]
                found = set(answer["ids"])
                ids = found if ids is None else ids & found
        except InfrastructureError as exc:
            return {
                "status": "error",
                "ids": [],
                "truncated": truncated,
                "queries": [q["query"] for q in plan["queries"]],
                "detail": str(exc.detail or exc.message),
            }
        return {
            "status": "answered",
            "ids": sorted(ids or []),
            "truncated": truncated,
            "queries": [q["query"] for q in plan["queries"]],
            "detail": "",
        }
