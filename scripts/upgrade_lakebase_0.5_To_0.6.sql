-- ============================================================================
-- OntoBricks Lakebase registry upgrade: 0.5.x  ->  0.6
-- ----------------------------------------------------------------------------
-- Single, clean, one-shot migration to the FINAL 0.6 schema. This is the ONE
-- script to run to bring a 0.5.x (or partially-migrated 0.6) registry up to
-- the complete 0.6 shape — it folds in every 0.6-cycle addition:
--
--   * new table   domain_comments  — domain-wide threaded discussion: every
--                 comment belongs to the single per-(domain, version) thread.
--                 A non-empty ``parent_id`` makes the row a reply; ``resolved``
--                 closes a thread without losing history.
--   * new table   domain_tasks     — personalised work items, usually born
--                 from a comment (``comment_id``), surfaced in the assignee's
--                 "My Tasks" worklist.
--   * new table   graph_analytics  — per-(domain, version) cache of the LAST
--                 async Knowledge-Graph Analytics result (UPSERT, one row).
--   * new table   graph_analytics_runs — append-only per-run analytics history.
--   * new table   domain_change_events — append-only ontology/mapping change
--                 audit ("who changed what, and when"); ``source`` tags human
--                 vs AI-assistant edits, ``occurred_at`` is the real edit time.
--   * new table   domain_edit_locks — single-editor DRAFT lock (owner-created
--                 here; the app cannot self-heal its FK to ``domains``).
--   * indexes     idx_domain_comments_lookup, idx_domain_tasks_assignee,
--                 idx_domain_tasks_domain, idx_graph_analytics_runs_domain_version,
--                 idx_change_events_domain_version.
--
-- These tables mirror the canonical
-- ``src/back/objects/registry/store/lakebase/schema.sql``, so the registry
-- stays fully constrained (task status + change-source CHECKs).
--
-- NOTE — supersedes the piecemeal 0.6.x patch scripts. Discussions became
-- domain-wide during the 0.6 cycle, so the early per-anchor columns
-- (``anchor_type`` / ``anchor_ref``) were dropped. This script lands the final
-- shape directly: if it finds those columns (left by an earlier draft, the
-- app's lazy self-heal, or ``make bootstrap-lakebase``), it drops them and the
-- stale ``idx_domain_comments_anchor`` index. It therefore fully replaces the
-- earlier standalone 0.6.x scripts (comment-anchor drop, graph_analytics,
-- change_events) — you only need this one.
--
-- The app self-heals these tables lazily on first use
-- (``_ensure_collab_tables`` / ``_ensure_graph_analytics_table`` /
-- ``_ensure_change_events_table``), and ``make bootstrap-lakebase`` provisions
-- them as the schema owner. Run this script when you prefer an explicit,
-- auditable one-shot migration (e.g. a DBA applying it out-of-band). The only
-- data discarded is the dead ``anchor_type`` / ``anchor_ref`` values (if
-- present); no comment bodies, authors, threading or resolved state are touched.
--
-- Idempotent: safe to run multiple times.
-- ----------------------------------------------------------------------------
-- Usage (psql):
--   # default schema (ontobricks_registry):
--   psql "$PGURL" -f scripts/upgrade_lakebase_0.5_To_0.6.sql
--
--   # custom registry schema (matches LAKEBASE_SCHEMA / REGISTRY_SCHEMA):
--   psql "$PGURL" -v reg_schema=my_registry_schema \
--        -f scripts/upgrade_lakebase_0.5_To_0.6.sql
-- ============================================================================

\set ON_ERROR_STOP on

-- Resolve the target schema (override with  -v reg_schema=...  ; default below).
\if :{?reg_schema}
\else
  \set reg_schema ontobricks_registry
\endif

SET search_path TO :"reg_schema";

\echo 'Upgrading OntoBricks registry schema:' :reg_schema

BEGIN;

-- 1. Collaborative comments (domain-wide thread; final 0.6 shape) ------------
CREATE TABLE IF NOT EXISTS domain_comments (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id   uuid NOT NULL
                REFERENCES domains(id) ON DELETE CASCADE,
    version     text NOT NULL,
    parent_id   uuid REFERENCES domain_comments(id) ON DELETE CASCADE,
    author      text NOT NULL,
    body        text NOT NULL DEFAULT '',
    resolved    boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- 1b. Converge pre-existing tables on the final shape: drop the dead per-anchor
--     columns + their index (left by an earlier anchored 0.6 draft / lazy
--     self-heal). The anchor_type CHECK constraint drops with its column.
DROP INDEX IF EXISTS idx_domain_comments_anchor;
ALTER TABLE IF EXISTS domain_comments DROP COLUMN IF EXISTS anchor_type;
ALTER TABLE IF EXISTS domain_comments DROP COLUMN IF EXISTS anchor_ref;

CREATE INDEX IF NOT EXISTS idx_domain_comments_lookup
    ON domain_comments(domain_id, version, created_at);

-- 2. Collaborative tasks -----------------------------------------------------
CREATE TABLE IF NOT EXISTS domain_tasks (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id   uuid NOT NULL
                REFERENCES domains(id) ON DELETE CASCADE,
    version     text NOT NULL,
    assignee    text NOT NULL,
    created_by  text NOT NULL,
    title       text NOT NULL,
    description text NOT NULL DEFAULT '',
    status      text NOT NULL DEFAULT 'open'
                CHECK (status IN ('open', 'in_progress', 'done', 'cancelled')),
    due_date    date,
    comment_id  uuid REFERENCES domain_comments(id) ON DELETE SET NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_domain_tasks_assignee
    ON domain_tasks(lower(assignee), status);
CREATE INDEX IF NOT EXISTS idx_domain_tasks_domain
    ON domain_tasks(domain_id, version);

-- 2b. Backfill the status CHECK on tables created by the app's lazy self-heal
--     path (which omits the constraint). ----------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'domain_tasks_status_check'
          AND conrelid = 'domain_tasks'::regclass
    ) THEN
        ALTER TABLE domain_tasks
            ADD CONSTRAINT domain_tasks_status_check
            CHECK (status IN ('open', 'in_progress', 'done', 'cancelled'));
    END IF;
END$$;

-- 3. Graph Analytics cache + append-only run history -------------------------
--    The Analytics page and the Domain Validation "Graph Structure" card
--    render from ``graph_analytics`` (one UPSERT row per (domain, version))
--    instead of recomputing NetworkX metrics on every request.
CREATE TABLE IF NOT EXISTS graph_analytics (
    domain_id    uuid NOT NULL
                 REFERENCES domains(id) ON DELETE CASCADE,
    version      text NOT NULL,
    status       text NOT NULL DEFAULT 'completed',  -- completed|failed
    graph_name   text NOT NULL DEFAULT '',
    class_filter jsonb NOT NULL DEFAULT '[]'::jsonb,  -- entity types used ([]=all)
    stats        jsonb NOT NULL DEFAULT '{}'::jsonb,
    top_pagerank jsonb NOT NULL DEFAULT '[]'::jsonb,
    result       jsonb NOT NULL DEFAULT '{}'::jsonb,  -- full compute payload
    error        text NOT NULL DEFAULT '',
    task_id      text NOT NULL DEFAULT '',
    duration_ms  bigint NOT NULL DEFAULT 0,
    computed_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (domain_id, version)
);

CREATE TABLE IF NOT EXISTS graph_analytics_runs (
    id                  bigserial PRIMARY KEY,
    domain_id           uuid NOT NULL
                        REFERENCES domains(id) ON DELETE CASCADE,
    version             text NOT NULL,
    status              text NOT NULL DEFAULT 'completed',
    class_filter        jsonb NOT NULL DEFAULT '[]'::jsonb,
    node_count          bigint NOT NULL DEFAULT 0,
    edge_count          bigint NOT NULL DEFAULT 0,
    connected_components integer NOT NULL DEFAULT 0,
    avg_degree          double precision NOT NULL DEFAULT 0,
    density             double precision NOT NULL DEFAULT 0,
    duration_ms         bigint NOT NULL DEFAULT 0,
    task_id             text NOT NULL DEFAULT '',
    error               text NOT NULL DEFAULT '',
    computed_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_graph_analytics_runs_domain_version
    ON graph_analytics_runs(domain_id, version, computed_at DESC);

-- 4. Ontology / mapping change audit (append-only "who changed what, when") --
--    Flushed from the editing session's buffered change_log on "Save to
--    registry". ``source`` distinguishes human vs AI-assistant edits.
CREATE TABLE IF NOT EXISTS domain_change_events (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id       uuid NOT NULL
                    REFERENCES domains(id) ON DELETE CASCADE,
    version         text NOT NULL,
    actor           text NOT NULL DEFAULT '',
    source          text NOT NULL DEFAULT 'user'
                    CHECK (source IN ('user', 'agent')),
    action          text NOT NULL,
    entity_type     text NOT NULL DEFAULT '',
    entity_ref      text NOT NULL DEFAULT '',
    summary         text NOT NULL DEFAULT '',
    meta            jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_change_events_domain_version
    ON domain_change_events(domain_id, version, occurred_at);

-- domain_edit_locks — single-editor DRAFT lock. The app's lazy self-heal
-- cannot create it (the FK to domains needs a privilege the app service
-- principal lacks), so provision it here as the schema owner. Without it,
-- every opener is falsely shown as a read-only viewer and admin take-over
-- fails.
CREATE TABLE IF NOT EXISTS domain_edit_locks (
    domain_id      uuid NOT NULL
                   REFERENCES domains(id) ON DELETE CASCADE,
    version        text NOT NULL,
    holder_email   text NOT NULL,
    holder_name    text NOT NULL DEFAULT '',
    holder_session text NOT NULL DEFAULT '',
    acquired_at    timestamptz NOT NULL DEFAULT now(),
    heartbeat_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (domain_id, version)
);

COMMIT;

-- Summary -------------------------------------------------------------------
\echo 'Done. Registry tables present:'
SELECT table_name
FROM information_schema.tables
WHERE table_schema = :'reg_schema'
  AND table_name IN ('domain_comments', 'domain_tasks',
                     'graph_analytics', 'graph_analytics_runs',
                     'domain_change_events', 'domain_edit_locks')
ORDER BY table_name;

-- ============================================================================
-- Part 2 — Graph schema migration (0.5 → 0.6)
-- ----------------------------------------------------------------------------
-- Adds the ``datatype`` and ``lang`` columns to all ``*_sync`` and ``*__app``
-- graph triple-store tables that were created before those columns existed.
--
-- These tables live in the **graph schema** (default: ``ontobricks_graph``),
-- NOT in the registry schema above.
--
-- Also drops stale ``*_sync`` tables that may have been left behind by an
-- earlier run under a different role (Lakeflow or a previous session user).
-- The DROP is unconditional so you need superuser or ownership.  Run as the
-- Lakebase schema owner.
--
-- Idempotent: ``ADD COLUMN IF NOT EXISTS`` and ``DROP TABLE IF EXISTS`` are
-- safe to re-run.
--
-- Usage:
--   # default graph schema (ontobricks_graph):
--   psql "$PGURL" -f scripts/upgrade_lakebase_0.5_To_0.6.sql
--
--   # custom graph schema:
--   psql "$PGURL" -v graph_schema=my_graph_schema \
--        -f scripts/upgrade_lakebase_0.5_To_0.6.sql
-- ============================================================================

\if :{?graph_schema}
\else
  \set graph_schema ontobricks_graph
\endif

\echo ''
\echo 'Upgrading OntoBricks graph schema:' :graph_schema

SET search_path TO :"graph_schema";

BEGIN;

-- 5. Backfill datatype + lang on every *_sync table that is missing them -----
--    (tables created by the app before v0.6 only had subject/predicate/object)
DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relkind = 'r'
          AND c.relname LIKE '%_sync'
    LOOP
        EXECUTE format(
            'ALTER TABLE %I ADD COLUMN IF NOT EXISTS datatype TEXT', tbl);
        EXECUTE format(
            'ALTER TABLE %I ADD COLUMN IF NOT EXISTS lang    TEXT', tbl);
        RAISE NOTICE 'Patched table: %', tbl;
    END LOOP;
END$$;

-- 6. Same for *__app companion tables ----------------------------------------
DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relkind = 'r'
          AND c.relname LIKE '%__app'
    LOOP
        EXECUTE format(
            'ALTER TABLE %I ADD COLUMN IF NOT EXISTS datatype TEXT', tbl);
        EXECUTE format(
            'ALTER TABLE %I ADD COLUMN IF NOT EXISTS lang    TEXT', tbl);
        RAISE NOTICE 'Patched companion table: %', tbl;
    END LOOP;
END$$;

-- 7. Drop stale *_sync tables not owned by the current role ------------------
--    Use this block when the previous owner was a Lakeflow service principal
--    or a different session user. Requires superuser or explicit ownership.
--    Review the table list below before running!
--
--    Uncomment and adjust as needed:
-- DO $$
-- DECLARE tbl text;
-- BEGIN
--     FOR tbl IN
--         SELECT c.relname
--         FROM pg_class c
--         JOIN pg_namespace n ON n.oid = c.relnamespace
--         WHERE n.nspname = current_schema()
--           AND c.relkind = 'r'
--           AND c.relname LIKE '%_sync'
--           AND NOT pg_has_role(session_user, c.relowner, 'MEMBER')
--     LOOP
--         EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', tbl);
--         RAISE NOTICE 'Dropped stale table: %', tbl;
--     END LOOP;
-- END$$;

COMMIT;

\echo 'Done. Graph schema columns:'
SELECT
    c.relname                          AS table_name,
    array_agg(a.attname ORDER BY a.attnum) AS columns
FROM pg_class c
JOIN pg_namespace n  ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
WHERE n.nspname = :'graph_schema'
  AND c.relkind = 'r'
  AND (c.relname LIKE '%_sync' OR c.relname LIKE '%__app')
GROUP BY c.relname
ORDER BY c.relname;
