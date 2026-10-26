"""Seed + BFS neighbourhood walk for Chat, MCP, and public triples/find.

Extracted from :class:`DigitalTwin` so find/seed SQL stays in one place.
``DigitalTwin`` methods remain thin delegators for existing callers.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from back.core.helpers import LIKE_ESCAPE, sql_like_escape
from back.core.helpers import sql_escape as escape_sql_value, extract_local_name
from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL


class GraphFind:
    """Build find seeds, expand URI aliases, and page BFS neighbourhood triples."""

    @staticmethod
    def local_id(uri: str) -> str:
        """Local id after ``#`` / last path segment (class/id fragments stripped)."""
        local = extract_local_name(uri)
        if "/" in local:
            local = local.rsplit("/", 1)[-1]
        return local or uri

    @staticmethod
    def expand_uri_aliases(store, table_name: str, uris: Set[str]) -> Set[str]:
        """Find alternate URI forms for a set of entity URIs."""
        if not uris:
            return uris
        local_ids = {GraphFind.local_id(u) for u in uris}
        local_ids.discard("")
        if not local_ids:
            return uris
        patterns = [f"%/{lid}" for lid in local_ids]
        expanded = set(uris) | store.find_subjects_by_patterns(table_name, patterns)
        return expanded

    @staticmethod
    def build_find_seed_where(
        table: str,
        *,
        entity_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> str:
        """Build the SQL ``WHERE`` clause that seeds a triples/find BFS.

        Shared by the public ``/api/v1/digitaltwin/triples/find`` route and the
        session-aware ``/dtwin/triples/find`` Graph Chat route so the two
        surfaces cannot drift on seed semantics.
        """
        seed_conditions: List[str] = []
        if entity_type:
            esc = escape_sql_value(sql_like_escape(entity_type)).lower()
            seed_conditions.append(
                f"subject IN (SELECT subject FROM {table} "
                f"WHERE predicate = '{RDF_TYPE}' AND "
                f"(LOWER(object) LIKE '%#{esc}' {LIKE_ESCAPE} "
                f"OR LOWER(object) LIKE '%/{esc}' {LIKE_ESCAPE}))"
            )
        if search:
            esc = escape_sql_value(sql_like_escape(search)).lower()
            seed_conditions.append(
                f"(subject IN (SELECT subject FROM {table} "
                f"WHERE (predicate = '{RDFS_LABEL}' "
                f"OR predicate LIKE '%#label' OR predicate LIKE '%/label' "
                f"OR predicate LIKE '%#name' OR predicate LIKE '%/name') "
                f"AND LOWER(object) LIKE '%{esc}%' {LIKE_ESCAPE}) "
                f"OR LOWER(subject) LIKE '%/{esc}%' {LIKE_ESCAPE} "
                f"OR LOWER(subject) LIKE '%#{esc}%' {LIKE_ESCAPE})"
            )
        return " WHERE " + " AND ".join(seed_conditions)

    @staticmethod
    def find_triples_bfs(
        store: Any,
        table: str,
        *,
        entity_type: Optional[str] = None,
        search: Optional[str] = None,
        depth: int = 1,
        limit: int = 1000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Seed + BFS neighbourhood walk used by both find endpoints.

        Returns a normalized payload dict (``seed_count``, ``triples``,
        pagination fields). An empty match yields ``message`` and empty lists.
        """
        seed_where = GraphFind.build_find_seed_where(
            table, entity_type=entity_type, search=search
        )
        bfs_rows = store.bfs_traversal(
            table,
            seed_where,
            depth,
            search=search or "",
            entity_type=entity_type or "",
        )
        if not bfs_rows:
            return {
                "seed_count": 0,
                "depth": depth,
                "message": "No matching entities found",
                "triples": [],
                "count": 0,
                "total": 0,
                "limit": limit,
                "offset": offset,
                "entity_count": 0,
                "has_more": False,
            }

        all_entities = {r["entity"] for r in bfs_rows}
        seed_count = sum(1 for r in bfs_rows if int(r.get("min_lvl", 0)) == 0)
        all_entities = GraphFind.expand_uri_aliases(store, table, all_entities)
        page_result = store.get_triples_page_for_subjects(
            table,
            list(all_entities),
            limit=limit,
            offset=offset,
        )
        page = page_result.get("rows", [])
        total = int(page_result.get("total", 0))
        has_more = offset + len(page) < total
        return {
            "seed_count": seed_count,
            "depth": depth,
            "triples": page,
            "count": len(page),
            "total": total,
            "limit": limit,
            "offset": offset,
            "entity_count": len(all_entities),
            "has_more": has_more,
        }
