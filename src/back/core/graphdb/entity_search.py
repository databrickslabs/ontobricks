"""Shared entity-directory SQL for Lakehouse and Lakebase."""

from __future__ import annotations

from typing import Callable

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.helpers.SQLHelpers import SQLHelpers

sql_like_escape = SQLHelpers.sql_like_escape
LIKE_ESCAPE = SQLHelpers.LIKE_ESCAPE

Escape = Callable[[str], str]

__all__ = [
    "Escape",
    "entity_search_seed_sql",
    "entity_search_select",
    "entity_search_uri_search_sql",
    "is_asserted_only_relation",
    "is_missing_relation_error",
    "is_view_not_table_error",
    "preview_select_sql",
    "sort_preview_rows",
]


def is_asserted_only_relation(table_name: str) -> bool:
    """Whether *table_name* excludes the inferred companion."""
    leaf = table_name.rsplit(".", 1)[-1]
    return leaf.endswith("_data") or leaf.endswith("_sync")


def _exception_haystack(exc: Exception) -> str:
    """Message plus ``detail`` — SEA keeps warehouse text only in ``detail``."""
    chunks = [str(exc)]
    detail = getattr(exc, "detail", None)
    if detail:
        chunks.append(str(detail))
    return " ".join(chunks).lower()


def is_missing_relation_error(exc: Exception) -> bool:
    """Whether *exc* reports that a referenced table/view does not exist."""
    message = _exception_haystack(exc)
    return (
        "table_or_view_not_found" in message
        or "does not exist" in message
        or "undefined table" in message
    )


def is_view_not_table_error(exc: Exception) -> bool:
    """Whether *exc* is a table-only statement (``DESCRIBE DETAIL``) run on a view."""
    return "expect_table_not_view" in _exception_haystack(exc)


def entity_search_select(spo: str) -> str:
    """Return one deterministic type/label row per typed subject."""
    return (
        f"SELECT typed.subject AS uri, typed.object AS type_uri, "
        f"COALESCE(lab.object, '') AS label, "
        f"LOWER(typed.subject) AS uri_lc, "
        f"LOWER(COALESCE(lab.object, '')) AS label_lc "
        f"FROM ("
        f"SELECT subject, MIN(object) AS object FROM {spo} "
        f"WHERE predicate = '{RDF_TYPE}' GROUP BY subject"
        f") typed "
        f"LEFT JOIN ("
        f"SELECT subject, MIN(object) AS object FROM {spo} "
        f"WHERE predicate = '{RDFS_LABEL}' GROUP BY subject"
        f") lab ON lab.subject = typed.subject"
    )


def _entity_search_text_clause(
    *,
    field: str,
    match_type: str,
    value: str,
    escape: Escape,
) -> str:
    """Return a ``label_lc``/``uri_lc`` LIKE/equality clause, or ``""``.

    Shared by ``preview_select_sql`` (Explorer Preview), and — from this
    plan's Task 3/4 — the GraphQL and MCP find seed builders. Only searches
    ``rdfs:label`` and the subject URI, same fields Explorer Preview already
    searches.
    """
    if not value:
        return ""
    safe_value = escape(value.lower())
    like_value = escape(sql_like_escape(value.lower()))
    search_label = field in ("label", "any")
    search_id = field in ("id", "any")

    def _match(column: str) -> str:
        if match_type == "exact":
            return f"{column} = '{safe_value}'"
        if match_type == "starts":
            return f"{column} LIKE '{like_value}%' {LIKE_ESCAPE}"
        if match_type == "ends":
            return f"{column} LIKE '%{like_value}' {LIKE_ESCAPE}"
        return f"{column} LIKE '%{like_value}%' {LIKE_ESCAPE}"

    text_clauses = []
    if search_label:
        text_clauses.append(_match("label_lc"))
    if search_id:
        text_clauses.append(_match("uri_lc"))
    if not text_clauses:
        return ""
    return f"({' OR '.join(text_clauses)})"


def entity_search_seed_sql(
    *,
    search_table: str,
    entity_type: str = "",
    search: str = "",
    escape: Escape,
) -> str:
    """Unbounded seed lookup for MCP/Graph Chat find, mirroring the legacy
    ``build_find_seed_where`` SPO seed clause.

    *entity_type* is matched as a **local-name suffix** against ``type_uri``
    (case-insensitive ``#name`` / ``/name``) — MCP's ``describe_entity`` and
    ``/triples/find`` document ``entity_type`` as a bare local name, not a
    full URI (unlike :func:`entity_search_uri_search_sql`, used by GraphQL
    and Explorer Preview, which both take a full class URI).
    """
    clauses: list[str] = []
    if entity_type:
        esc = escape(sql_like_escape(entity_type.lower()))
        clauses.append(
            f"(LOWER(type_uri) LIKE '%#{esc}' {LIKE_ESCAPE} "
            f"OR LOWER(type_uri) LIKE '%/{esc}' {LIKE_ESCAPE})"
        )
    text_clause = _entity_search_text_clause(
        field="any", match_type="contains", value=search, escape=escape
    )
    if text_clause:
        clauses.append(text_clause)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return f"SELECT uri FROM {search_table}{where}"


def entity_search_uri_search_sql(
    *,
    search_table: str,
    type_uri: str = "",
    search: str = "",
    limit: int,
    offset: int = 0,
    escape: Escape,
) -> str:
    """Bounded, ordered subject lookup for GraphQL's typed list resolver.

    *type_uri* is matched by exact equality (a full class URI, same
    semantics as Explorer Preview's ``entity_type``) — unlike
    :func:`entity_search_seed_sql`, which matches a bare local name.
    """
    clauses: list[str] = []
    if type_uri:
        clauses.append(f"type_uri = '{escape(type_uri)}'")
    text_clause = _entity_search_text_clause(
        field="any", match_type="contains", value=search, escape=escape
    )
    if text_clause:
        clauses.append(text_clause)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return (
        f"SELECT uri FROM {search_table}{where} "
        f"ORDER BY uri LIMIT {int(limit)} OFFSET {int(offset)}"
    )


def preview_select_sql(
    *,
    search_table: str,
    entity_type: str,
    field: str,
    match_type: str,
    value: str,
    limit: int,
    escape: Escape,
) -> str:
    """Build a bounded Preview lookup against an entity-search table."""
    if int(limit) <= 0:
        raise ValueError("limit must be greater than zero")

    clauses: list[str] = []
    if entity_type:
        clauses.append(f"type_uri = '{escape(entity_type)}'")
    text_clause = _entity_search_text_clause(
        field=field, match_type=match_type, value=value, escape=escape
    )
    if text_clause:
        clauses.append(text_clause)

    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    return (
        f"SELECT uri, type_uri, label FROM {search_table}{where} "
        f"LIMIT {int(limit)}"
    )


def sort_preview_rows(rows: list[dict]) -> list[dict]:
    """Stable UI order after a warehouse LIMIT (no ORDER BY in SQL)."""
    return sorted(
        rows,
        key=lambda r: (r.get("type") or "", r.get("label") or "", r.get("uri") or ""),
    )
