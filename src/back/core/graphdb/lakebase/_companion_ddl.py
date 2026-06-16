"""DDL helpers for the synced table, writable companion table, and union view.

Both ``managed_synced`` and ``app_managed`` modes use the same three-object
Postgres layout per graph version:

- ``g_<dom>_v<n>_sync`` -- bulk-data table.
  In ``managed_synced`` it is read-only, populated by Lakeflow.
  In ``app_managed`` it is populated by the app during build (streaming from
  the Delta warehouse view) and is otherwise read-only post-build.
- ``g_<dom>_v<n>__app``  -- writable companion (reasoning + cohort writes).
- ``g_<dom>_v<n>``       -- union view that readers query (back-compat name).

The synced table mirrors the source Delta view's columns
(``subject``, ``predicate``, ``object``); the companion carries the full
``(subject, predicate, object, datatype, lang)`` shape used by reasoning
output. The union view casts NULL ``datatype`` / ``lang`` for the synced
side so SPARQL / KG-search readers see a uniform schema.
"""

from __future__ import annotations

from typing import Any

from back.core.helpers import safe_identifier


def _safe(name: str) -> str:
    return (safe_identifier(name) or "triples").lower()


def synced_phy(name: str) -> str:
    """Postgres table name for the read-only synced table.

    Uses a ``_sync`` suffix (not ``__sync``) so it does not collide with the union
    view identifier (:func:`view_phy`, the legacy reader-facing name).
    """
    return f"{_safe(name)}_sync"


def companion_phy(name: str) -> str:
    """Postgres table name for the writable companion table."""
    return f"{_safe(name)}__app"


def view_phy(name: str) -> str:
    """Postgres view name readers see (matches the legacy single-table name)."""
    return _safe(name)


def _idx_name(table: str, suffix: str) -> str:
    base = f"g_{table}_{suffix}".lower()
    return base[:63]


def ensure_synced(cur: Any, schema: str, synced: str) -> None:
    """Create the *_sync bulk-data table + standard B-tree indexes if absent.

    Used by the ``app_managed`` build path to provision the table that receives
    warehouse-streamed triples.  In ``managed_synced`` mode this table is
    created by Lakebase/Lakeflow instead.
    """
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {synced} (
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object TEXT NOT NULL,
            datatype TEXT,
            lang TEXT,
            PRIMARY KEY (subject, predicate, object)
        )
        """
    )
    for sfx, cols in (
        ("sp", "subject, predicate"),
        ("po", "predicate, object"),
        ("ops", "object, predicate"),
    ):
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {_idx_name(synced, sfx)} "
            f"ON {synced} ({cols})"
        )


def drop_synced(cur: Any, synced: str) -> None:
    """Drop the *_sync bulk-data table (app_managed cleanup path).

    Uses a DO block to skip the DROP when the current session does not own the
    table.  This prevents ``PSQLException: must be owner of table`` when a
    previous ``managed_synced`` build left behind a ``_sync`` table created by
    Lakeflow under a different service principal, and the next build runs in
    ``app_managed`` mode (e.g. after a config change or a transient mode-
    resolution fallback).
    """
    bare = synced.split(".")[-1].strip('"')
    cur.execute(
        "DO $$ BEGIN "
        "  IF EXISTS ("
        "    SELECT 1 FROM pg_class c "
        "    JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"    WHERE c.relname = {bare!r} "
        "      AND n.nspname = ANY(current_schemas(false)) "
        "      AND c.relkind = 'r' "
        "      AND pg_has_role(session_user, c.relowner, 'MEMBER')"
        "  ) THEN "
        f"    EXECUTE 'DROP TABLE IF EXISTS {synced}'; "
        "  END IF; "
        "END $$"
    )


def ensure_companion(cur: Any, schema: str, companion: str) -> None:
    """Create the writable companion table + standard B-tree indexes if absent."""
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {companion} (
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object TEXT NOT NULL,
            datatype TEXT,
            lang TEXT,
            PRIMARY KEY (subject, predicate, object)
        )
        """
    )
    for sfx, cols in (
        ("sp", "subject, predicate"),
        ("po", "predicate, object"),
        ("ops", "object, predicate"),
    ):
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {_idx_name(companion, sfx)} "
            f"ON {companion} ({cols})"
        )


def ensure_union_view(
    cur: Any,
    view: str,
    synced: str,
    companion: str,
) -> None:
    """``CREATE OR REPLACE`` the union view that readers query.

    The synced side is NULL-padded for ``datatype`` / ``lang`` so the view
    has a uniform 5-column shape regardless of which side a row came from.

    If ``view`` already exists as a TABLE (e.g. from an old app-managed build
    before the managed_synced migration), it is dropped first — Postgres's
    ``CREATE OR REPLACE VIEW`` cannot replace a table with a view.
    """
    # Drop any stale TABLE that occupies the view name before (re)creating the view.
    # ``CREATE OR REPLACE VIEW`` cannot replace a table — it only replaces views.
    # We check pg_class using the unqualified name and the current search_path schema
    # so this works regardless of whether the caller schema-qualifies the name.
    bare_name = view.split(".")[-1].strip('"')
    cur.execute(
        "DO $$ BEGIN "
        "  IF EXISTS ("
        "    SELECT 1 FROM pg_class c "
        "    JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"    WHERE c.relname = {bare_name!r} "
        "      AND n.nspname = ANY(current_schemas(false)) "
        "      AND c.relkind = 'r' "
        "      AND pg_has_role(session_user, c.relowner, 'MEMBER')"
        "  ) THEN "
        f"    EXECUTE 'DROP TABLE IF EXISTS {view} CASCADE'; "
        "  END IF; "
        "END $$"
    )
    sql = (
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT subject, predicate, object, "
        f"NULL::TEXT AS datatype, NULL::TEXT AS lang "
        f"FROM {synced} "
        f"UNION ALL "
        f"SELECT subject, predicate, object, datatype, lang FROM {companion}"
    )
    cur.execute(sql)


def truncate_companion(cur: Any, companion: str) -> None:
    """Drop all rows from the companion table (used on full rebuild)."""
    cur.execute(f"TRUNCATE TABLE {companion}")


def drop_companion(cur: Any, companion: str) -> None:
    cur.execute(f"DROP TABLE IF EXISTS {companion}")


def drop_view(cur: Any, view: str) -> None:
    cur.execute(f"DROP VIEW IF EXISTS {view}")
