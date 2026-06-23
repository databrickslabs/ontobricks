-- ============================================================================
-- OntoBricks Lakebase registry upgrade: drop comment anchors (0.6.x)
-- ----------------------------------------------------------------------------
-- Discussions are now domain-wide: every comment belongs to the single
-- per-(domain, version) thread, so the per-anchor columns on
-- ``domain_comments`` are dead weight. This migration removes them:
--
--   * drop columns  anchor_type, anchor_ref  (the anchor_type CHECK
--                   constraint drops with its column)
--   * drop index    idx_domain_comments_anchor (referenced those columns)
--   * create index  idx_domain_comments_lookup (domain_id, version, created_at)
--                   to keep the domain-thread listing query fast
--
-- DESTRUCTIVE: the anchor_type / anchor_ref values are discarded. No comment
-- bodies, authors, threading (parent_id) or resolved state are touched.
--
-- The app self-heals new installs without these columns
-- (``_ensure_collab_tables``) and the canonical
-- ``src/back/objects/registry/store/lakebase/schema.sql`` no longer declares
-- them, so this script is only needed to clean up registries provisioned
-- before the change. Run it as the schema owner.
--
-- Idempotent: safe to run multiple times (IF EXISTS guards throughout).
-- ----------------------------------------------------------------------------
-- Usage (psql):
--   # default schema (ontobricks_registry):
--   psql "$PGURL" -f scripts/upgrade_lakebase_0.6_drop_comment_anchor.sql
--
--   # custom registry schema (matches LAKEBASE_SCHEMA / REGISTRY_SCHEMA):
--   psql "$PGURL" -v reg_schema=my_registry_schema \
--        -f scripts/upgrade_lakebase_0.6_drop_comment_anchor.sql
-- ============================================================================

\set ON_ERROR_STOP on

-- Resolve the target schema (override with  -v reg_schema=...  ; default below).
\if :{?reg_schema}
\else
  \set reg_schema ontobricks_registry
\endif

SET search_path TO :"reg_schema";

\echo 'Dropping comment anchors on OntoBricks registry schema:' :reg_schema

BEGIN;

-- 1. Drop the anchor lookup index (replaced below). ------------------------
DROP INDEX IF EXISTS idx_domain_comments_anchor;

-- 2. Drop the anchor columns (CHECK constraint drops with anchor_type). -----
ALTER TABLE IF EXISTS domain_comments DROP COLUMN IF EXISTS anchor_type;
ALTER TABLE IF EXISTS domain_comments DROP COLUMN IF EXISTS anchor_ref;

-- 3. Domain-thread listing index. ------------------------------------------
CREATE INDEX IF NOT EXISTS idx_domain_comments_lookup
    ON domain_comments(domain_id, version, created_at);

COMMIT;

-- Summary -------------------------------------------------------------------
\echo 'Done. domain_comments columns:'
SELECT column_name
FROM information_schema.columns
WHERE table_schema = :'reg_schema'
  AND table_name = 'domain_comments'
ORDER BY ordinal_position;
