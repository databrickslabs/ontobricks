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

# Lakeflow synced-table PK (object_hash comes from the Delta warehouse view).
LAKEFLOW_SYNC_PRIMARY_KEY: tuple[str, ...] = (
    "subject",
    "predicate",
    "object_hash",
)

_TRIPLE_TABLE_COLUMNS = """
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object TEXT NOT NULL,
            object_hash BYTEA GENERATED ALWAYS AS (
                digest(coalesce(object, ''), 'sha256')
            ) STORED,
            datatype TEXT,
            lang TEXT,
            PRIMARY KEY (subject, predicate, object_hash)
"""

_TRIPLE_TABLE_INDEXES: tuple[tuple[str, str], ...] = (
    ("sp", "subject, predicate"),
    ("po", "predicate, object_hash"),
    ("oph", "object_hash, predicate"),
)


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


def ensure_pgcrypto(cur: Any) -> None:
    """Enable ``digest()`` for generated ``object_hash`` columns."""
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")


def wrap_triple_view_sql_for_lakeflow(spark_sql: str) -> str:
    """Wrap translated Spark SQL so the view exposes ``object_hash`` for Lakeflow."""
    inner = spark_sql.strip().rstrip(";")
    return (
        "SELECT subject, predicate, object, "
        "sha2(cast(object AS string), 256) AS object_hash "
        f"FROM ({inner}) AS _ob_triples"
    )


def _table_bare_name(table_ref: str) -> str:
    return table_ref.split(".")[-1].strip('"')


def _table_exists(cur: Any, bare_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = %s
          AND n.nspname = ANY(current_schemas(false))
          AND c.relkind = 'r'
        LIMIT 1
        """,
        (bare_name,),
    )
    return cur.fetchone() is not None


def _has_object_hash_column(cur: Any, bare_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = ANY(current_schemas(false))
          AND table_name = %s
          AND column_name = 'object_hash'
        LIMIT 1
        """,
        (bare_name,),
    )
    return cur.fetchone() is not None


def upgrade_legacy_triple_table_to_object_hash(cur: Any, table_ref: str) -> None:
    """Migrate pre-0.6.2 triple tables that still key on full ``object`` text.

    ``CREATE TABLE IF NOT EXISTS`` leaves legacy companions in place; without
    this step ``ensure_graph_indexes`` fails with ``column "object_hash" does
    not exist`` when (re)building managed_synced graphs.
    """
    bare = _table_bare_name(table_ref)
    if not _table_exists(cur, bare) or _has_object_hash_column(cur, bare):
        return

    ensure_pgcrypto(cur)

    for sfx, _ in _TRIPLE_TABLE_INDEXES:
        cur.execute(f"DROP INDEX IF EXISTS {_idx_name(bare, sfx)}")

    cur.execute(
        f"ALTER TABLE {table_ref} "
        "ADD COLUMN IF NOT EXISTS object_hash BYTEA GENERATED ALWAYS AS "
        "(digest(coalesce(object, ''), 'sha256')) STORED"
    )
    cur.execute(f"ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS datatype TEXT")
    cur.execute(f"ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS lang TEXT")

    cur.execute(
        f"""
        DO $$ DECLARE pk_name text;
        BEGIN
          SELECT c.conname INTO pk_name
          FROM pg_constraint c
          JOIN pg_class t ON t.oid = c.conrelid
          JOIN pg_namespace n ON n.oid = t.relnamespace
          WHERE c.contype = 'p'
            AND t.relname = {bare!r}
            AND n.nspname = ANY(current_schemas(false))
          LIMIT 1;
          IF pk_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', {bare!r}, pk_name);
          END IF;
        END $$;
        """
    )
    cur.execute(
        f"ALTER TABLE {table_ref} "
        "ADD PRIMARY KEY (subject, predicate, object_hash)"
    )


def ensure_graph_indexes(cur: Any, table_ref: str) -> None:
    """Create standard graph lookup indexes on *table_ref* if absent.

    ``table_ref`` may be bare (``g_x_v1_sync``) or schema-qualified
    (``"schema".g_x_v1_sync``). Index names use the physical table name only.
    """
    table_name = table_ref.split(".")[-1].strip('"')
    for sfx, cols in _TRIPLE_TABLE_INDEXES:
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {_idx_name(table_name, sfx)} "
            f"ON {table_ref} ({cols})"
        )


def _create_triple_table(cur: Any, schema: str, table: str) -> None:
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
        {_TRIPLE_TABLE_COLUMNS}
        )
        """
    )
    upgrade_legacy_triple_table_to_object_hash(cur, table)
    ensure_graph_indexes(cur, table)


def ensure_synced(cur: Any, schema: str, synced: str) -> None:
    """Create the *_sync bulk-data table + standard B-tree indexes if absent.

    Used by the ``app_managed`` build path to provision the table that receives
    warehouse-streamed triples.  In ``managed_synced`` mode this table is
    created by Lakebase/Lakeflow instead.
    """
    ensure_pgcrypto(cur)
    _create_triple_table(cur, schema, synced)


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
    ensure_pgcrypto(cur)
    _create_triple_table(cur, schema, companion)


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
