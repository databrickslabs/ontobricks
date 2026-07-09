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
from back.core.logging import get_logger

logger = get_logger(__name__)

# SQLSTATE 42501 (insufficient_privilege) is what Postgres raises for
# ``must be owner of table`` / ``permission denied``.  These are the only
# migration failures we treat as non-fatal: a triples table owned by a
# different role (e.g. a Lakeflow-created ``_sync`` from a previous
# ``managed_synced`` build) simply cannot be altered by the app principal.
_OWNERSHIP_SQLSTATES = frozenset({"42501"})

# Generated column carrying a fixed-width SHA-256 digest of ``object``.
# Postgres B-tree index entries cannot exceed ~2704 bytes, so a triple whose
# literal ``object`` is larger than that aborts any index that includes the raw
# column (the primary key and the object-bearing secondary indexes).  Keying the
# primary key on this 32-byte digest instead of the unbounded ``object`` lets
# arbitrarily long literals load while preserving triple uniqueness.  ``sha256``
# / ``convert_to`` are Postgres built-ins (no ``pgcrypto`` extension needed).
_OBJECT_HASH_COL = (
    "object_hash bytea GENERATED ALWAYS AS "
    "(sha256(convert_to(object, 'UTF8'))) STORED"
)

# Secondary indexes that include the raw ``object`` are made partial so they
# still serve normal-sized data (identical query plans) but simply skip the rare
# oversized literal rather than failing the insert.  2000 bytes leaves head-room
# under the 2704 limit for the accompanying ``predicate`` / ``subject`` columns.
_LONG_OBJECT_PARTIAL = " WHERE octet_length(object) <= 2000"


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
            {_OBJECT_HASH_COL},
            PRIMARY KEY (subject, predicate, object_hash)
        )
        """
    )
    ensure_object_hash_pk(cur, synced)
    _ensure_triple_indexes(cur, synced)


def _ensure_triple_indexes(cur: Any, table: str) -> None:
    """Create the standard triple-lookup indexes.

    ``sp`` covers subject/predicate access; ``po`` / ``ops`` cover object-bearing
    access but are made partial (:data:`_LONG_OBJECT_PARTIAL`) so an oversized
    literal never trips the B-tree row-size limit at insert time.
    """
    for sfx, cols, partial in (
        ("sp", "subject, predicate", ""),
        ("po", "predicate, object", _LONG_OBJECT_PARTIAL),
        ("ops", "object, predicate", _LONG_OBJECT_PARTIAL),
    ):
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {_idx_name(table, sfx)} "
            f"ON {table} ({cols}){partial}"
        )


def _is_ownership_error(exc: Exception) -> bool:
    """Return ``True`` only for the expected ownership/permission failure.

    Prefers the psycopg ``sqlstate`` (42501) and falls back to a message probe
    so the check still works if the driver wraps or re-raises the error without
    preserving the SQLSTATE.
    """
    sqlstate = getattr(exc, "sqlstate", None) or getattr(
        getattr(exc, "diag", None), "sqlstate", None
    )
    if sqlstate in _OWNERSHIP_SQLSTATES:
        return True
    msg = str(exc).lower()
    return "must be owner" in msg or "permission denied" in msg


def ensure_object_hash_pk(cur: Any, table: str) -> None:
    """Idempotently migrate an existing triples table to the hashed-object PK.

    Adds the generated ``object_hash`` column, swaps a legacy
    ``(subject, predicate, object)`` primary key for
    ``(subject, predicate, object_hash)``, and drops the legacy full-object
    B-tree indexes so the size-guarded partial ones created by
    :func:`_ensure_triple_indexes` take over.  A brand-new table created with the
    current DDL is already in the target shape, so every step is a no-op.

    Best-effort: a table owned by a different role (e.g. a Lakeflow-created
    ``_sync`` left behind by a previous ``managed_synced`` build) cannot be
    altered by the app service principal; the failure is logged and swallowed so
    a build is never aborted by the migration itself.
    """
    bare = table.split(".")[-1].strip("\"")
    try:
        cur.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {_OBJECT_HASH_COL}"
        )
        cur.execute(
            "DO $$ "
            "DECLARE pk_name text; pk_cols text; "
            "BEGIN "
            "  SELECT c.conname, "
            "         string_agg(a.attname, ',' ORDER BY k.ord) "
            "    INTO pk_name, pk_cols "
            "  FROM pg_constraint c "
            "  JOIN pg_class t ON t.oid = c.conrelid "
            "  JOIN pg_namespace n ON n.oid = t.relnamespace "
            "  JOIN unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) ON true "
            "  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum "
            f"  WHERE t.relname = {bare!r} "
            "    AND n.nspname = ANY(current_schemas(false)) "
            "    AND c.contype = 'p' "
            "  GROUP BY c.conname; "
            "  IF pk_cols = 'subject,predicate,object' THEN "
            f"    EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', {bare!r}, pk_name); "
            f"    EXECUTE format('ALTER TABLE %I ADD PRIMARY KEY "
            f"(subject, predicate, object_hash)', {bare!r}); "
            "  END IF; "
            "END $$"
        )
        cur.execute(f"DROP INDEX IF EXISTS {_idx_name(bare, 'po')}")
        cur.execute(f"DROP INDEX IF EXISTS {_idx_name(bare, 'ops')}")
    except Exception as exc:  # noqa: BLE001
        # Only ownership/permission failures are expected and non-fatal; any
        # other error (undefined function, syntax error, ...) is a real defect
        # that must surface rather than silently leave the legacy PK/index shape
        # in place and re-trigger the original ProgramLimitExceeded later.
        if not _is_ownership_error(exc):
            raise
        logger.warning(
            "object_hash migration skipped for %s (non-fatal; table is owned by "
            "another role and new tables already use the hashed PK): %s",
            table,
            exc,
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
            {_OBJECT_HASH_COL},
            PRIMARY KEY (subject, predicate, object_hash)
        )
        """
    )
    ensure_object_hash_pk(cur, companion)
    _ensure_triple_indexes(cur, companion)


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
