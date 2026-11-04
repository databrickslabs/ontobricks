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

import re
from typing import Any

from back.core.errors import InfrastructureError, ValidationError
from back.core.helpers import SQLHelpers, safe_identifier, sql_cast

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

#: What a Lakeflow synced table was registered on: a triples relation
#: (``…_data`` or the R2RML VIEW, keyed on ``LAKEFLOW_SYNC_PRIMARY_KEY``) or the
#: ``…_slots`` streaming table (one row per ``map_id, source_key, predicate``).
SOURCE_KIND_TRIPLES = "triples"
SOURCE_KIND_SLOTS = "slots"

#: Synced-table key of a ``…_slots`` source: one row per slot, unique by construction.
SLOT_SYNC_PRIMARY_KEY: tuple[str, ...] = ("map_id", "source_key", "predicate")

# A ``…_slots``-sourced synced table has no ``object_hash`` column.
_SLOTS_SYNC_INDEXES: tuple[tuple[str, str], ...] = (("sp", "subject, predicate"),)


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


def asserted_view_phy(name: str) -> str:
    """Postgres view of the synced (asserted) triples alone: distinct, non-blank.

    Asserted-only readers (``include_inferred=False`` reads, the asserted
    entity-search rebuild, BFS expansion) query it instead of the ``_sync``
    table, so a ``…_slots``-sourced sync — one row per slot, blank attribute
    objects kept — reads like the ``…_data`` relation.
    """
    return f"{_safe(name)}_asserted"


def live_phy(name: str) -> str:
    """Postgres table name of a ``realtime`` domain's ``_live`` serving table."""
    return f"{_safe(name)}_live"
SIM_TABLE_MARKER = "__sim_"
SIM_UNION_INFERRED_MARKER = "__smu_"
SIM_OVERLAY_MARKER = "__smp_"
_PG_IDENTIFIER_MAX_BYTES = 63


def _sim_named(name: str, sim_id: str, marker: str) -> str:
    if not (sim_id or "").strip():
        raise ValueError("Simulation id is required")
    sid = safe_identifier(sim_id.strip()).lower()
    suffix = f"{marker}{sid}"
    budget = _PG_IDENTIFIER_MAX_BYTES - len(suffix)
    if budget < 1:
        raise ValueError(f"Simulation id too long: {sim_id!r}")
    stem = _safe(name)[:budget].rstrip("_") or "g"
    return f"{stem}{suffix}"


def sim_phy(name: str, sim_id: str) -> str:
    """Postgres table name for one simulation sandbox of graph *name*.

    The result is its own physical identifier (``physical_table_id`` maps it
    to itself), so it can be passed to the store as a logical table name.
    The graph stem is trimmed so the identifier stays within Postgres's
    63-byte limit while the ``__sim_<id>`` suffix is always preserved.
    """
    return _sim_named(name, sim_id, SIM_TABLE_MARKER)


def sim_union_phy(name: str, sim_id: str) -> str:
    """Read-only view: live inferred graph with sandbox after-picture per patched predicate."""
    return _sim_named(name, sim_id, SIM_UNION_INFERRED_MARKER)


def sim_overlay_phy(name: str, sim_id: str) -> str:
    """Companion of ``(subject, predicate)`` pairs the sandbox has owned."""
    return _sim_named(name, sim_id, SIM_OVERLAY_MARKER)


def overlay_phy_from(relation: str) -> str:
    """Map a ``__sim_`` / ``__smu_`` identifier to its ``__smp_`` companion."""
    name = relation or ""
    leaf = name.split(".")[-1].strip('"')
    if SIM_OVERLAY_MARKER in leaf:
        return name
    if SIM_UNION_INFERRED_MARKER in name:
        return name.replace(SIM_UNION_INFERRED_MARKER, SIM_OVERLAY_MARKER, 1)
    if SIM_TABLE_MARKER in name:
        return name.replace(SIM_TABLE_MARKER, SIM_OVERLAY_MARKER, 1)
    return ""


def is_sim_phy(table: str) -> bool:
    """True when *table* is a simulation sandbox table name."""
    return SIM_TABLE_MARKER in (table or "").split(".")[-1].strip('"')


def is_sim_overlay_phy(table: str) -> bool:
    """True when *table* is a simulation overlay-key companion."""
    return SIM_OVERLAY_MARKER in (table or "").split(".")[-1].strip('"')


def ensure_sim_table(cur: Any, schema: str, table: str) -> None:
    """Create a standalone simulation triple table + standard indexes."""
    ensure_pgcrypto(cur)
    _create_triple_table(cur, schema, table)


def drop_sim_table(cur: Any, table: str) -> None:
    cur.execute(f"DROP TABLE IF EXISTS {table}")


def backfill_sim_overlay_keys(cur: Any, sandbox: str, overlay: str) -> None:
    """Append-only: remember every ``(subject, predicate)`` currently in *sandbox*."""
    cur.execute(
        f"INSERT INTO {overlay} (subject, predicate, object) "
        f"SELECT DISTINCT subject, predicate, '' FROM {sandbox} "
        f"ON CONFLICT DO NOTHING"
    )


def ensure_sim_union_view(
    cur: Any, view: str, live: str, sandbox: str, overlay: str
) -> None:
    """Live graph UNION sandbox, overlaying predicates the sandbox has owned.

    Overlay keys live in *overlay* (``__smp_``), not remaining sandbox rows, so
    a last-value ``remove`` still hides the live triple.
    """
    sql = (
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT l.subject, l.predicate, l.object, "
        f"NULL::TEXT AS datatype, NULL::TEXT AS lang "
        f"FROM {live} l "
        f"WHERE NOT EXISTS ("
        f"SELECT 1 FROM {overlay} k "
        f"WHERE k.subject = l.subject AND k.predicate = l.predicate"
        f") "
        f"UNION ALL "
        f"SELECT s.subject, s.predicate, s.object, s.datatype, s.lang "
        f"FROM {sandbox} s"
    )
    cur.execute(sql)


def relation_exists(cur: Any, relation: str) -> bool:
    """True when *relation* (a physical table/view id) exists in the database.

    ``to_regclass`` resolves the identifier against the session ``search_path``
    and returns ``NULL`` when the relation is absent — a cheap existence probe
    that works for both tables and views without catalog remapping.  The
    ``SELECT 1 WHERE … IS NOT NULL`` shape yields a row only when it exists, so
    the check is independent of the cursor's row factory (tuple vs dict_row).
    """
    cur.execute("SELECT 1 WHERE to_regclass(%s) IS NOT NULL", (relation,))
    return cur.fetchone() is not None


def _idx_name(table: str, suffix: str) -> str:
    base = f"g_{table}_{suffix}".lower()
    return base[:63]


# ``CREATE EXTENSION`` without a SCHEMA clause installs into the first schema
# on the session ``search_path`` — for pooled connections that is the *graph*
# schema, not ``public``. Because ``IF NOT EXISTS`` then becomes a permanent
# no-op, renaming the graph schema strands ``digest()`` out of reach forever.
# Install into ``public`` (always on the pool's search_path) and relocate a
# stranded extension, falling back to the current schema when the role may not
# write to ``public``. Nested EXCEPTION blocks run as subtransactions, so a
# permission failure cannot abort the caller's transaction.
_PGCRYPTO_ENSURE_REACHABLE = """
DO $$
DECLARE ext_schema text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgcrypto') THEN
        BEGIN
            EXECUTE 'CREATE EXTENSION pgcrypto WITH SCHEMA public';
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                EXECUTE 'CREATE EXTENSION pgcrypto';
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END;
    END IF;

    SELECT n.nspname INTO ext_schema
    FROM pg_extension e
    JOIN pg_namespace n ON n.oid = e.extnamespace
    WHERE e.extname = 'pgcrypto';

    IF ext_schema IS NOT NULL
       AND NOT (ext_schema = ANY (current_schemas(true))) THEN
        BEGIN
            EXECUTE 'ALTER EXTENSION pgcrypto SET SCHEMA public';
        EXCEPTION WHEN OTHERS THEN
            BEGIN
                EXECUTE format(
                    'ALTER EXTENSION pgcrypto SET SCHEMA %I', current_schema()
                );
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END;
    END IF;
END $$
"""


def ensure_pgcrypto(cur: Any) -> None:
    """Make ``digest()`` resolvable for generated ``object_hash`` columns.

    Installs ``pgcrypto`` into ``public`` and relocates a previously
    misplaced install, then verifies ``digest`` resolves on the current
    ``search_path`` so Builds fail with an actionable message instead of
    ``function digest(text, unknown) does not exist``.
    """
    cur.execute(_PGCRYPTO_ENSURE_REACHABLE)
    cur.execute(
        "SELECT 1 FROM pg_proc p "
        "JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE p.proname = 'digest' "
        "  AND pg_catalog.has_function_privilege(p.oid, 'EXECUTE') "
        "  AND n.nspname = ANY (current_schemas(true)) "
        "LIMIT 1"
    )
    if cur.fetchone() is None:
        raise InfrastructureError(
            "Postgres function digest() is unavailable — pgcrypto is missing, or "
            "installed in a schema outside the connection search_path and could "
            "not be relocated to public. Run `make bootstrap-lakebase` as a "
            "workspace admin (Step 1b installs and pins pgcrypto to public) on "
            "the graph database, then retry the Build."
        )


def wrap_triple_view_sql_for_lakeflow(spark_sql: str) -> str:
    """Wrap translated Spark SQL so the view exposes ``object_hash`` for Lakeflow."""
    inner = spark_sql.strip().rstrip(";")
    return (
        "SELECT subject, predicate, object, "
        f"{SQLHelpers.object_hash_sql(sql_cast('object', 'STRING'))} AS object_hash "
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


def ensure_graph_indexes(
    cur: Any, table_ref: str, *, source_kind: str = SOURCE_KIND_TRIPLES
) -> None:
    """Create standard graph lookup indexes on *table_ref* if absent.

    ``table_ref`` may be bare (``g_x_v1_sync``) or schema-qualified
    (``"schema".g_x_v1_sync``). Index names use the physical table name only.
    A ``…_slots``-sourced synced table (*source_kind* ``"slots"``) has no
    ``object_hash`` and gets the ``(subject, predicate)`` index only.
    """
    table_name = table_ref.split(".")[-1].strip('"')
    indexes = _SLOTS_SYNC_INDEXES if source_kind == SOURCE_KIND_SLOTS else _TRIPLE_TABLE_INDEXES
    for sfx, cols in indexes:
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
    """Create the ``_sync`` bulk-data table + standard B-tree indexes if absent.

    Used by the ``app_managed`` build path to provision the table that receives
    warehouse-streamed triples.  In ``managed_synced`` mode this table is
    created by Lakebase/Lakeflow instead.
    """
    ensure_pgcrypto(cur)
    _create_triple_table(cur, schema, synced)


_LAKEFLOW_SYNC_OWNER_PREFIX = "databricks_writer"


def is_lakeflow_sync_owner(owner: str) -> bool:
    """Return True when *owner* looks like a Lakeflow synced-table writer role."""
    return str(owner or "").startswith(_LAKEFLOW_SYNC_OWNER_PREFIX)


def drop_synced(cur: Any, synced: str) -> None:
    """Drop the ``_sync`` bulk-data table (``app_managed`` cleanup path).

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


def build_union_view_sql(view: str, synced: str, companion: str) -> str:
    """Union view over a triples-sourced synced table (``…_data`` / VIEW) and the companion.

    The synced side is NULL-padded for ``datatype`` / ``lang`` so the view
    has a uniform 5-column shape regardless of which side a row came from.
    """
    return (
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT subject, predicate, object, "
        f"NULL::TEXT AS datatype, NULL::TEXT AS lang "
        f"FROM {synced} "
        f"UNION ALL "
        f"SELECT subject, predicate, object, datatype, lang FROM {companion}"
    )


def build_union_view_sql_for_slots(view: str, synced: str, companion: str) -> str:
    """Union view over a ``…_slots``-sourced synced table and the companion.

    The slots table carries ``map_id`` / ``source_key`` next to the triple and
    one row per slot, so the synced side projects the three triple columns and
    de-duplicates them (classes that share a TriplesMap emit the same triple).
    Blank attribute objects are dropped exactly as the ``…_data`` relation drops
    them. The output columns match :func:`build_union_view_sql`, so ``CREATE OR
    REPLACE`` switches between the two.
    """
    return (
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT DISTINCT subject, predicate, object, "
        f"NULL::TEXT AS datatype, NULL::TEXT AS lang "
        f"FROM {synced} WHERE object IS NOT NULL AND TRIM(object) <> '' "
        f"UNION ALL "
        f"SELECT subject, predicate, object, datatype, lang FROM {companion}"
    )


# -- realtime ``_live`` serving table ---------------------------------------

#: The stream job's sink: one row per slot ``(map_id, source_key, predicate)``;
#: a delete keeps a tombstone (``deleted``) so a late event cannot win, and the
#: newest ``seq`` per slot wins at the sink (``INSERT … ON CONFLICT … WHERE``).
_LIVE_TABLE_COLUMNS = (
    "map_id TEXT NOT NULL, source_key TEXT NOT NULL, predicate TEXT NOT NULL, "
    "subject TEXT, object TEXT, object_hash TEXT, "
    "deleted BOOLEAN NOT NULL DEFAULT FALSE, seq BIGINT NOT NULL, "
    "updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
    "PRIMARY KEY (map_id, source_key, predicate)"
)

#: Partial indexes over the live (not deleted) rows the union view reads.
_LIVE_TABLE_INDEXES: tuple[tuple[str, str], ...] = (
    ("ls", "subject"),
    ("lps", "predicate, subject"),
    ("loh", "object_hash"),
)

#: A tombstone-heavy upsert table: vacuum and analyse it early.
_LIVE_AUTOVACUUM = "autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.01"

#: The live rows the union view serves (blank objects dropped like ``…_data``).
_LIVE_ROWS = "WHERE NOT deleted AND object IS NOT NULL AND TRIM(object) <> ''"


def live_table_ddl(table: str) -> list[str]:
    """``CREATE TABLE`` / partial indexes / autovacuum settings of the ``_live`` *table*."""
    bare = _table_bare_name(table)
    return [
        f"CREATE TABLE IF NOT EXISTS {table} ({_LIVE_TABLE_COLUMNS})",
        *(
            f"CREATE INDEX IF NOT EXISTS {_idx_name(bare, sfx)} ON {table} ({cols}) "
            "WHERE NOT deleted"
            for sfx, cols in _LIVE_TABLE_INDEXES
        ),
        f"ALTER TABLE {table} SET ({_LIVE_AUTOVACUUM})",
    ]


def purge_live_tombstones_sql(table: str, older_than_hours: int) -> str:
    """``DELETE`` the tombstones of *table* older than *older_than_hours* (a whole number ≥ 0).

    Keep them at least as long as an event can arrive late: a purged tombstone
    no longer guards its slot against an older ``seq``.
    """
    if isinstance(older_than_hours, bool) or not isinstance(older_than_hours, int):
        raise ValidationError(f"older_than_hours must be an integer, got {older_than_hours!r}")
    if older_than_hours < 0:
        raise ValidationError("older_than_hours must not be negative")
    return (
        f"DELETE FROM {table} WHERE deleted "
        f"AND updated_at < now() - interval '{older_than_hours} hours'"
    )


#: Table comment that marks a ``_live`` table as served by the union view: set
#: when a stream starts (``TwinRealtimeStream``), cleared — with the rows — when
#: it stops or the domain's build is not ``realtime``, so a frozen ``_live`` never
#: lingers in the view.
LIVE_SERVING_COMMENT = "ontobricks:live:serving"
#: A Postgres role the grants quote: a native role, a user e-mail or a principal id.
_ROLE_NAME = re.compile(r"[A-Za-z0-9_.@\-]{1,63}")


def live_serving_sql(table: str, serving: bool) -> str:
    """``COMMENT ON TABLE`` marking *table* served (or not) by the union view."""
    value = f"'{LIVE_SERVING_COMMENT}'" if serving else "NULL"
    return f"COMMENT ON TABLE {table} IS {value}"


def truncate_live_sql(table: str) -> str:
    return f"TRUNCATE TABLE {table}"


def seed_live_sql(table: str) -> str:
    """Insert one snapshot slot into *table*; a slot already there is kept."""
    return (
        f"INSERT INTO {table} (map_id, source_key, predicate, subject, object, object_hash, "
        "deleted, seq) VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s) "
        "ON CONFLICT (map_id, source_key, predicate) DO NOTHING"
    )


def live_grants_sql(table: str, schema: str, role: str) -> list[str]:
    """The privileges the stream job's Postgres *role* needs to upsert *table*."""
    if not _ROLE_NAME.fullmatch(role or ""):
        raise ValidationError(f"Not a Postgres role name: {role!r}")
    return [
        f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"',
        f'GRANT SELECT, INSERT, UPDATE ON {table} TO "{role}"',
    ]


def is_live_serving(cur: Any, bare_name: str) -> bool:
    """True when the ``_live`` table *bare_name* exists and is marked served."""
    cur.execute(
        """
        SELECT obj_description(c.oid, 'pg_class') AS comment
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = %s
          AND n.nspname = ANY(current_schemas(false))
          AND c.relkind = 'r'
        LIMIT 1
        """,
        (bare_name,),
    )
    row = cur.fetchone()
    value = row.get("comment") if isinstance(row, dict) else (row[0] if row else None)
    return value == LIVE_SERVING_COMMENT


def count_live_rows_sql(table: str) -> str:
    """``count(*)`` of the slots *table* serves (not deleted, non-blank object)."""
    return f"SELECT count(*) AS cnt FROM {table} {_LIVE_ROWS}"


def build_union_view_sql_with_live(
    view: str, synced: str, companion: str, live: str, *, source_kind: str = SOURCE_KIND_TRIPLES
) -> str:
    """Union view of a ``realtime`` domain: the synced table, the companion and ``_live``.

    The companion holds both app writes and inferred triples, so the three
    relations are the synced, app, inferred and live sides. ``UNION``
    (distinct) rather than ``UNION ALL``: the stream's ``_live`` rows repeat
    triples the synced table already carries until its next sync. A slot
    deleted by the stream stays visible through the synced table until that
    table syncs the delete.
    """
    base = (
        build_union_view_sql_for_slots if source_kind == SOURCE_KIND_SLOTS else build_union_view_sql
    )(view, synced, companion)
    return (
        base.replace(" UNION ALL ", " UNION ")
        + " UNION SELECT subject, predicate, object, NULL::TEXT AS datatype, NULL::TEXT AS lang "
        + f"FROM {live} {_LIVE_ROWS}"
    )


def _live_ref(view: str) -> str:
    """The ``_live`` table next to *view* (same schema qualification)."""
    head, sep, leaf = view.rpartition(".")
    return f"{head}{sep}{leaf.strip(chr(34))}_live"


def _asserted_ref(view: str) -> str:
    """The asserted view next to *view* (same schema qualification; :func:`asserted_view_phy`)."""
    head, sep, leaf = view.rpartition(".")
    return f"{head}{sep}{leaf.strip(chr(34))}_asserted"


def build_asserted_view_sql(
    asserted: str, synced: str, *, source_kind: str = SOURCE_KIND_TRIPLES
) -> str:
    """``CREATE OR REPLACE`` the asserted view: the synced triples alone, as ``…_data`` reads.

    Over a ``…_slots``-sourced synced table the view de-duplicates and drops
    blank attribute objects (one row per slot otherwise); over a triples source
    it projects the three triple columns. Readers that leave inferred triples
    out query this view (``FlatStore.synced_table_name``).
    """
    if source_kind == SOURCE_KIND_SLOTS:
        return (
            f"CREATE OR REPLACE VIEW {asserted} AS "
            f"SELECT DISTINCT subject, predicate, object FROM {synced} "
            f"WHERE object IS NOT NULL AND TRIM(object) <> ''"
        )
    return f"CREATE OR REPLACE VIEW {asserted} AS SELECT subject, predicate, object FROM {synced}"


def ensure_union_view(
    cur: Any,
    view: str,
    synced: str,
    companion: str,
    *,
    source_kind: str = SOURCE_KIND_TRIPLES,
) -> None:
    """``CREATE OR REPLACE`` the union view that readers query.

    *source_kind* picks :func:`build_union_view_sql_for_slots` for a
    ``…_slots``-sourced synced table and :func:`build_union_view_sql` otherwise.
    When the view's ``_live`` table is marked served (:data:`LIVE_SERVING_COMMENT`
    — a ``realtime`` domain whose stream runs), the view also unions its live
    rows (:func:`build_union_view_sql_with_live`); an unmarked or absent one
    is left out.

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
    live = _live_ref(view)
    if is_live_serving(cur, _table_bare_name(live)):
        cur.execute(
            build_union_view_sql_with_live(view, synced, companion, live, source_kind=source_kind)
        )
    else:
        build = (
            build_union_view_sql_for_slots
            if source_kind == SOURCE_KIND_SLOTS
            else build_union_view_sql
        )
        cur.execute(build(view, synced, companion))
    cur.execute(build_asserted_view_sql(_asserted_ref(view), synced, source_kind=source_kind))


def truncate_companion(cur: Any, companion: str) -> None:
    """Drop all rows from the companion table (used on full rebuild)."""
    cur.execute(f"TRUNCATE TABLE {companion}")


def drop_companion(cur: Any, companion: str) -> None:
    cur.execute(f"DROP TABLE IF EXISTS {companion}")


def drop_view(cur: Any, view: str) -> None:
    """Drop the union view and, for a graph's view, the asserted view next to it.

    Both depend on the ``_sync`` table. A simulation union (``__smu_``) serves a
    sandbox and has no asserted companion.
    """
    cur.execute(f"DROP VIEW IF EXISTS {view}")
    if SIM_UNION_INFERRED_MARKER not in view.split(".")[-1]:
        cur.execute(f"DROP VIEW IF EXISTS {_asserted_ref(view)}")
