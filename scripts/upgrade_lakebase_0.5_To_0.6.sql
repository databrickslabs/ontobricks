-- ============================================================================
-- OntoBricks Lakebase registry upgrade: 0.5.x  ->  0.6.x
-- ----------------------------------------------------------------------------
-- Adds the collaborative *comments & tasks* (the "Discussions" feature):
--
--   * new table   domain_comments  — contextual threaded discussion anchored
--                 to a domain version (ontology class/property, mapping, graph
--                 node/edge, or the whole domain). A non-empty ``parent_id``
--                 makes the row a reply; ``resolved`` closes a thread without
--                 losing history.
--   * new table   domain_tasks     — personalised work items, usually born
--                 from a comment (``comment_id``), surfaced in the assignee's
--                 "My Tasks" worklist.
--   * indexes     idx_domain_comments_anchor, idx_domain_tasks_assignee,
--                 idx_domain_tasks_domain.
--
-- These tables carry the same CHECK constraints as the canonical
-- ``src/back/objects/registry/store/lakebase/schema.sql`` (anchor_type and
-- task status), so the registry stays fully constrained.
--
-- The app self-heals these tables lazily on first comment/task write
-- (``_ensure_collab_tables``), and ``make bootstrap-lakebase`` provisions them
-- as the schema owner. Run this script when you prefer an explicit, auditable
-- one-shot migration (e.g. a DBA applying it out-of-band). Nothing here is
-- destructive — no existing data is touched, no columns are dropped.
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

-- 1. Collaborative comments --------------------------------------------------
CREATE TABLE IF NOT EXISTS domain_comments (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id   uuid NOT NULL
                REFERENCES domains(id) ON DELETE CASCADE,
    version     text NOT NULL,
    anchor_type text NOT NULL DEFAULT 'domain'
                CHECK (anchor_type IN ('ontology_class', 'ontology_property',
                                       'mapping', 'graph_node', 'graph_edge',
                                       'domain')),
    anchor_ref  text NOT NULL DEFAULT '',
    parent_id   uuid REFERENCES domain_comments(id) ON DELETE CASCADE,
    author      text NOT NULL,
    body        text NOT NULL DEFAULT '',
    resolved    boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_domain_comments_anchor
    ON domain_comments(domain_id, version, anchor_type, anchor_ref);

-- 1b. Backfill the anchor_type CHECK on registries whose table was created by
--     the app's lazy self-heal path (which omits the constraint).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'domain_comments_anchor_type_check'
          AND conrelid = 'domain_comments'::regclass
    ) THEN
        ALTER TABLE domain_comments
            ADD CONSTRAINT domain_comments_anchor_type_check
            CHECK (anchor_type IN ('ontology_class', 'ontology_property',
                                   'mapping', 'graph_node', 'graph_edge',
                                   'domain'));
    END IF;
END$$;

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

-- 2b. Backfill the status CHECK on lazily-created tables (see 1b). ------------
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

COMMIT;

-- Summary -------------------------------------------------------------------
\echo 'Done. Collaboration tables present:'
SELECT table_name
FROM information_schema.tables
WHERE table_schema = :'reg_schema'
  AND table_name IN ('domain_comments', 'domain_tasks')
ORDER BY table_name;
