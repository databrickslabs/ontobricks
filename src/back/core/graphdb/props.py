"""Shared property-companion SQL for Lakehouse and Lakebase."""

from __future__ import annotations

from typing import Any, Callable

from back.core.graphdb.constants import RDF_TYPE
from back.core.logging import get_logger

logger = get_logger(__name__)

ExecuteQuery = Callable[[str], list[dict[str, Any]]]
_missing_props_tables: set[str] = set()

__all__ = [
    "execute_expand_with_props_fallback",
    "forget_missing_props",
    "is_missing_props_error",
    "known_missing_props",
    "props_page_sql",
    "props_select",
    "remember_missing_props",
    "reset_missing_props_cache",
]


def remember_missing_props(props_table: str) -> None:
    """Remember that a property companion is unavailable in this process."""
    if props_table:
        _missing_props_tables.add(props_table)


def known_missing_props(props_table: str) -> bool:
    """Whether a property companion previously returned a missing-table error."""
    return bool(props_table) and props_table in _missing_props_tables


def forget_missing_props(props_table: str) -> None:
    """Forget a stale negative after a successful companion rebuild."""
    _missing_props_tables.discard(props_table)


def reset_missing_props_cache() -> None:
    """Clear property companion negatives."""
    _missing_props_tables.clear()


def is_missing_props_error(exc: Exception, props_table: str) -> bool:
    """Whether *exc* reports that the attempted property table is missing."""
    message = str(exc).lower()
    return props_table.lower() in message and any(
        marker in message
        for marker in (
            "table_or_view_not_found",
            "does not exist",
            "undefined table",
        )
    )


def execute_expand_with_props_fallback(
    *,
    execute_query: ExecuteQuery,
    sql: str,
    props_table: str,
    fallback_sql: str,
) -> list[dict[str, Any]]:
    """Execute property-backed expansion, remembering a missing companion."""
    if not props_table:
        return execute_query(sql) or []
    if known_missing_props(props_table):
        return execute_query(fallback_sql) or []
    try:
        return execute_query(sql) or []
    except Exception as exc:  # noqa: BLE001
        if not is_missing_props_error(exc, props_table):
            raise
        remember_missing_props(props_table)
        logger.info(
            "Property table is unavailable; using SPO expansion fallback: %s",
            exc,
        )
        return execute_query(fallback_sql) or []


def props_select(spo: str) -> str:
    """Return all outgoing triples whose subject is a typed instance."""
    return (
        f"SELECT t.subject, t.predicate, t.object "
        f"FROM {spo} t "
        f"INNER JOIN ("
        f"SELECT DISTINCT subject FROM {spo} "
        f"WHERE predicate = '{RDF_TYPE}'"
        f") typed ON typed.subject = t.subject"
    )


def props_page_sql(
    *,
    payload_relation: str,
    uris: list[str],
    limit: int,
    offset: int,
    escape: Callable[[str], str],
) -> str:
    """Return deterministic page SQL with exact count metadata."""
    if not uris:
        raise ValueError("At least one URI is required")
    subject_literals = ", ".join(
        f"'{escape(uri)}'" for uri in dict.fromkeys(uris)
    )
    page_limit = int(limit)
    page_offset = int(offset)
    return (
        "WITH base AS ("
        " SELECT DISTINCT subject, predicate, object "
        f"FROM {payload_relation} "
        f"WHERE subject IN ({subject_literals})"
        "), stats AS ("
        " SELECT COUNT(*) AS _ob_total FROM base"
        "), page AS ("
        " SELECT subject, predicate, object FROM base "
        "ORDER BY subject, predicate, object "
        f"LIMIT {page_limit} OFFSET {page_offset}"
        ") "
        "SELECT page.subject, page.predicate, page.object, stats._ob_total "
        "FROM stats LEFT JOIN page ON TRUE "
        "ORDER BY page.subject, page.predicate, page.object"
    )
