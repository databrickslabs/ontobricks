"""Default SQL implementations: entity lookup, preview seeds, pagination.

Implementation detail of :class:`~back.core.graphdb.interfaces.GraphBackend`;
mixed into it, not a public interface. Reads the entity-search / property
companions when ready, falling back to the live SPO relation otherwise.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from back.core.logging import get_logger
from back.core.helpers import LIKE_ESCAPE, sql_like_escape
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graphdb.props import execute_expand_with_props_fallback, props_page_sql
from back.core.graphdb.entity_search import (
    entity_search_uri_search_sql,
    is_asserted_only_relation,
    is_missing_relation_error,
    preview_select_sql,
    sort_preview_rows,
)

logger = get_logger(__name__)


class EntityQueryMixin:
    """Subject/entity lookup, Explorer Preview seeding, and triple pagination."""

    def find_subjects_by_type(
        self,
        table_name: str,
        type_uri: str,
        limit: int = 50,
        offset: int = 0,
        search: Optional[str] = None,
    ) -> List[str]:
        """Return distinct subject URIs that are ``rdf:type`` *type_uri*.

        When *search* is given, matches against all literal values for the
        subject (label, data properties, etc.) — not just ``rdfs:label``.

        When the entity-search companion is ready, reads it instead — a
        Build/Refresh-cache snapshot rather than the live graph, and
        *search* then matches only ``rdfs:label``/URI (same fields Explorer
        Preview searches). Falls back to the SPO query below when the
        companion is missing or a missing-table error occurs mid-query.
        """
        if self.entity_search_ready(table_name):
            if is_asserted_only_relation(table_name):
                search_table = self.entity_search_asserted_table_id(table_name)
            else:
                search_table = self.entity_search_table_id(table_name)
            sql = entity_search_uri_search_sql(
                search_table=self._sql_relation(search_table),
                type_uri=type_uri,
                search=search or "",
                limit=limit,
                offset=offset,
                escape=self._sql_escape,
            )
            try:
                rows = self.execute_query(sql)
                return [r["uri"] for r in rows]
            except Exception as exc:  # noqa: BLE001
                if not is_missing_relation_error(exc):
                    raise
                logger.info(
                    "Entity-search table is unavailable for typed lookup; "
                    "using SPO fallback: %s",
                    exc,
                )

        esc_type = self._sql_escape(type_uri)
        conditions = [
            f"predicate = '{RDF_TYPE}'",
            f"object = '{esc_type}'",
        ]
        if search:
            esc = self._sql_escape(sql_like_escape(search)).lower()
            conditions.append(
                f"subject IN ("
                f"SELECT DISTINCT subject FROM {self._sql_relation(table_name)} "
                f"WHERE predicate != '{RDF_TYPE}' "
                f"AND LOWER(object) LIKE '%{esc}%' {LIKE_ESCAPE})"
            )
        sql = (
            f"SELECT DISTINCT subject FROM {self._sql_relation(table_name)} "
            f"WHERE {' AND '.join(conditions)} "
            f"ORDER BY subject "
            f"LIMIT {int(limit)} OFFSET {int(offset)}"
        )
        rows = self.execute_query(sql)
        return [r["subject"] for r in rows]

    def resolve_subject_by_id(
        self, table_name: str, type_uri: str, id_fragment: str
    ) -> Optional[str]:
        """Find a subject URI by type and trailing local-name fragment."""
        esc_type = self._sql_escape(type_uri)
        esc_id = self._sql_escape(id_fragment)
        sql = (
            f"SELECT DISTINCT subject FROM {self._sql_relation(table_name)} "
            f"WHERE predicate = '{RDF_TYPE}' "
            f"AND object = '{esc_type}' "
            f"AND (subject LIKE '%/{esc_id}' OR subject LIKE '%#{esc_id}')"
        )
        rows = self.execute_query(sql)
        return rows[0]["subject"] if rows else None

    def get_entity_metadata(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        """Return ``rdf:type`` and ``rdfs:label`` for each subject.

        Returns a list of dicts with keys ``uri``, ``type`` (full URI),
        and ``label`` (literal value or empty string).
        """
        if not subjects:
            return []
        in_clause = ", ".join(f"'{self._sql_escape(u)}'" for u in subjects)

        # Fetch type and label in a single round-trip. Two separate queries
        # doubled the warehouse latency on the preview path (each scan of the
        # union graph view is several seconds on a serverless warehouse); one
        # predicate-IN scan returns both and is split in Python.
        rows = self.execute_query(
            f"SELECT subject, predicate, object FROM {self._sql_relation(table_name)} "
            f"WHERE subject IN ({in_clause}) "
            f"AND predicate IN ('{RDF_TYPE}', '{RDFS_LABEL}')"
        ) or []

        types: Dict[str, str] = {}
        labels: Dict[str, str] = {}
        for r in rows:
            if r["predicate"] == RDF_TYPE:
                types.setdefault(r["subject"], r["object"])
            elif r["predicate"] == RDFS_LABEL:
                labels.setdefault(r["subject"], r["object"])

        return [
            {"uri": uri, "type": types.get(uri, ""), "label": labels.get(uri, "")}
            for uri in subjects
            if uri in types
        ]

    def get_triples_for_subjects(
        self, table_name: str, subjects: List[str]
    ) -> List[Dict[str, str]]:
        """Return all triples whose subject is in *subjects*.

        Reads the property companion (``_props``) when this backend
        supports it — every outgoing triple of a **typed** subject — falling
        back to the SPO relation for a companion-missing graph or a
        missing-table error mid-query (see ``back.core.graphdb.props``).
        """
        if not subjects:
            return []
        in_clause = ", ".join(f"'{self._sql_escape(u)}'" for u in subjects)
        fallback_sql = (
            f"SELECT subject, predicate, object FROM {self._sql_relation(table_name)} "
            f"WHERE subject IN ({in_clause})"
        )
        props = self._props_table_for_read(table_name)
        if not props:
            return self.execute_query(fallback_sql)
        sql = (
            f"SELECT subject, predicate, object FROM {self._sql_relation(props)} "
            f"WHERE subject IN ({in_clause})"
        )
        return execute_expand_with_props_fallback(
            execute_query=self.execute_query,
            sql=sql,
            props_table=props,
            fallback_sql=fallback_sql,
        )

    def get_triples_page_for_subjects(
        self,
        table_name: str,
        subjects: List[str],
        *,
        limit: int,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return a paged triple list plus exact total for the subject set."""
        if not subjects:
            return {"rows": [], "total": 0}

        fallback_sql = props_page_sql(
            payload_relation=self._sql_relation(table_name),
            uris=subjects,
            limit=limit,
            offset=offset,
            escape=self._sql_escape,
        )

        props = self._props_table_for_read(table_name)
        if props:
            sql = props_page_sql(
                payload_relation=self._sql_relation(props),
                uris=subjects,
                limit=limit,
                offset=offset,
                escape=self._sql_escape,
            )
            rows = execute_expand_with_props_fallback(
                execute_query=self.execute_query,
                sql=sql,
                props_table=props,
                fallback_sql=fallback_sql,
            )
        else:
            rows = self.execute_query(fallback_sql) or []

        total = int(rows[0].get("_ob_total") or 0) if rows else 0
        triples: List[Dict[str, str]] = []
        for row in rows:
            if (
                row.get("subject") is None
                and row.get("predicate") is None
                and row.get("object") is None
            ):
                continue
            triples.append(
                {
                    "subject": row.get("subject") or "",
                    "predicate": row.get("predicate") or "",
                    "object": row.get("object") or "",
                }
            )
        return {"rows": triples, "total": total}

    def get_predicates_for_type(self, table_name: str, type_uri: str) -> List[str]:
        """Return distinct predicates used by instances of *type_uri*."""
        esc_type = self._sql_escape(type_uri)
        sql = (
            f"SELECT DISTINCT predicate FROM {self._sql_relation(table_name)} "
            f"WHERE subject IN ("
            f"  SELECT subject FROM {self._sql_relation(table_name)} "
            f"  WHERE predicate = '{RDF_TYPE}' "
            f"  AND object = '{esc_type}' LIMIT 1"
            f")"
        )
        rows = self.execute_query(sql)
        return [r["predicate"] for r in rows]

    def paginated_triples(
        self,
        table_name: str,
        conditions: List[str],
        limit: int,
        offset: int,
    ) -> List[Dict[str, str]]:
        """Return triples matching *conditions* with LIMIT/OFFSET pagination."""
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = (
            f"SELECT subject, predicate, object "
            f"FROM {self._sql_relation(table_name)}{where} LIMIT {limit} OFFSET {offset}"
        )
        return self.execute_query(sql)

    def paginated_count(self, table_name: str, conditions: List[str]) -> int:
        """Return count of triples matching *conditions*."""
        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT COUNT(*) AS cnt FROM {self._sql_relation(table_name)}{where}"
        rows = self.execute_query(sql)
        return int(rows[0]["cnt"]) if rows else 0

    def find_seed_subjects(
        self,
        table_name: str,
        entity_type: str = "",
        field: str = "any",
        match_type: str = "contains",
        value: str = "",
        limit: int = 0,
    ) -> Set[str]:
        """Return distinct subjects matching type and/or value criteria.

        *field* is ``"label"`` (match on ``rdfs:label``), ``"id"`` (match on
        the subject URI itself), or ``"any"`` (match either).
        *match_type* is ``"contains"``, ``"exact"``, ``"starts"``, or
        ``"ends"``. ``limit`` (when > 0) caps returned subjects for responsive
        preview queries.
        """
        esc_type = self._sql_escape(entity_type) if entity_type else ""
        safe_val = self._sql_escape(value.lower()) if value else ""
        like_val = self._sql_escape(sql_like_escape(value.lower())) if value else ""
        rel = self._sql_relation(table_name)

        search_label = field in ("label", "any")
        search_id = field in ("id", "any")

        def _like(column: str) -> str:
            if match_type == "exact":
                return f"{column} = '{safe_val}'"
            if match_type == "starts":
                return f"{column} LIKE '{like_val}%' {LIKE_ESCAPE}"
            if match_type == "ends":
                return f"{column} LIKE '%{like_val}' {LIKE_ESCAPE}"
            return f"{column} LIKE '%{like_val}%' {LIKE_ESCAPE}"

        if entity_type and value:
            # Build a set of candidate subjects matching the text filter,
            # then intersect with the typed subjects.
            parts = []
            if search_id:
                parts.append(
                    f"SELECT DISTINCT subject FROM {rel} "
                    f"WHERE predicate = '{RDF_TYPE}' AND object = '{esc_type}' "
                    f"AND {_like('LOWER(subject)')}"
                )
            if search_label:
                parts.append(
                    f"SELECT DISTINCT subject FROM {rel} "
                    f"WHERE predicate = '{RDF_TYPE}' AND object = '{esc_type}' "
                    f"AND subject IN ("
                    f"SELECT subject FROM {rel} "
                    f"WHERE predicate = '{RDFS_LABEL}' AND {_like('LOWER(object)')})"
                )
            sql = " UNION ".join(parts)

        elif entity_type:
            sql = (
                f"SELECT DISTINCT subject FROM {rel} "
                f"WHERE predicate = '{RDF_TYPE}' AND object = '{esc_type}'"
            )

        else:
            # value only — search by label and/or URI fragment
            parts = []
            if search_label:
                parts.append(
                    f"SELECT DISTINCT subject FROM {rel} "
                    f"WHERE predicate = '{RDFS_LABEL}' AND {_like('LOWER(object)')}"
                )
            if search_id:
                parts.append(
                    f"SELECT DISTINCT subject FROM {rel} "
                    f"WHERE predicate = '{RDF_TYPE}' AND {_like('LOWER(subject)')}"
                )
            sql = " UNION ".join(parts)

        # Cap server-side. Without this the warehouse materialises *every*
        # matching subject — for a bare entity-type filter that is every
        # instance of the type (potentially millions), all serialised back over
        # SEA before the caller slices to the preview size. Wrapping in a
        # derived table applies the bound to the whole (possibly UNION-ed) set
        # in both Spark and Postgres. The caller asks for max_preview+1 so it can
        # still detect capping.
        if limit and int(limit) > 0:
            sql = f"SELECT subject FROM ({sql}) AS _seeds LIMIT {int(limit)}"

        rows = self.execute_query(sql)
        return {r["subject"] for r in rows}

    def find_preview_seeds(
        self,
        table_name: str,
        entity_type: str = "",
        field: str = "any",
        match_type: str = "contains",
        value: str = "",
        limit: int = 0,
    ) -> List[Dict[str, str]]:
        """Return Preview rows with URI, type URI, and label."""
        probe_limit = int(limit) if limit else 0
        search_table = ""
        if probe_limit > 0 and self.entity_search_ready(table_name):
            if is_asserted_only_relation(table_name):
                search_table = self.entity_search_asserted_table_id(table_name)
            else:
                search_table = self.entity_search_table_id(table_name)
        if search_table:
            try:
                rows = self.execute_query(
                    preview_select_sql(
                        search_table=self._sql_relation(search_table),
                        entity_type=entity_type,
                        field=field,
                        match_type=match_type,
                        value=value,
                        limit=probe_limit,
                        escape=self._sql_escape,
                    )
                ) or []
                return sort_preview_rows(
                    [
                        {
                            "uri": row["uri"],
                            "type": row.get("type_uri") or "",
                            "label": row.get("label") or "",
                        }
                        for row in rows
                    ]
                )
            except Exception as exc:  # noqa: BLE001
                if not is_missing_relation_error(exc):
                    raise
                logger.info(
                    "Entity-search table is unavailable; using SPO Preview fallback: %s",
                    exc,
                )

        subjects = list(
            self.find_seed_subjects(
                table_name,
                entity_type=entity_type,
                field=field,
                match_type=match_type,
                value=value,
                limit=probe_limit,
            )
        )
        return sort_preview_rows(self.get_entity_metadata(table_name, subjects))

    def find_subjects_by_patterns(
        self, table_name: str, like_patterns: List[str]
    ) -> Set[str]:
        """Return subjects matching any of the given SQL LIKE patterns."""
        if not like_patterns:
            return set()
        like_clauses = " OR ".join(
            f"subject LIKE '{self._sql_escape(p)}'" for p in like_patterns
        )
        sql = f"SELECT DISTINCT subject FROM {self._sql_relation(table_name)} WHERE {like_clauses}"
        rows = self.execute_query(sql)
        return {r["subject"] for r in rows}
