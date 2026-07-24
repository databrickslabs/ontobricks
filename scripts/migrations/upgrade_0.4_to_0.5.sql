-- ============================================================================
-- OntoBricks Lakebase registry upgrade: 0.4.x  ->  0.5.x
-- ----------------------------------------------------------------------------
-- Adds the per-version lifecycle status (DRAFT / IN-REVIEW / PUBLISHED) that
-- replaces the old "Active (API / MCP)" toggle:
--
--   * new column   domain_versions.status        (NOT NULL, default 'DRAFT')
--   * new CHECK     status IN ('DRAFT','IN-REVIEW','PUBLISHED')
--   * new index     idx_domain_versions_status (domain_id, status)
--   * backfill      mcp_enabled=true  ->  status='PUBLISHED'  (preserves which
--                   version the external API / MCP serves), then mirrors the
--                   value into the version ``info`` JSONB blob so reads stay
--                   consistent.
--
-- The legacy ``mcp_enabled`` column is intentionally left in place (dormant);
-- it is no longer read by the application and will be dropped in a later
-- migration. Nothing here is destructive.
--
-- The app self-heals the *column* + *index* lazily on first read, but it does
-- NOT add the CHECK constraint and does NOT backfill PUBLISHED from the old
-- mcp_enabled flag. Run this script once after upgrading so existing "active"
-- versions stay exposed on the API/MCP and the data is fully constrained.
--
-- Idempotent: safe to run multiple times.
-- ----------------------------------------------------------------------------
-- Usage (psql):
--   # default schema (ontobricks_registry):
--   psql "$PGURL" -f scripts/upgrade_lakebase_0.4_To_0.5.sql
--
--   # custom registry schema (matches LAKEBASE_SCHEMA / REGISTRY_SCHEMA):
--   psql "$PGURL" -v reg_schema=my_registry_schema \
--        -f scripts/upgrade_lakebase_0.4_To_0.5.sql
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

-- 1. Lifecycle status column ------------------------------------------------
ALTER TABLE domain_versions
    ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'DRAFT';

-- 2. CHECK constraint (guarded; the inline name from CREATE TABLE) -----------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'domain_versions_status_check'
          AND conrelid = 'domain_versions'::regclass
    ) THEN
        ALTER TABLE domain_versions
            ADD CONSTRAINT domain_versions_status_check
            CHECK (status IN ('DRAFT', 'IN-REVIEW', 'PUBLISHED'));
    END IF;
END$$;

-- 3. Lookup index -----------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_domain_versions_status
    ON domain_versions(domain_id, status);

-- 4. Backfill: preserve the old "Active (MCP / API)" semantics ---------------
--    Every version that was mcp_enabled becomes PUBLISHED so the external
--    API / GraphQL / MCP keep serving the same version after the upgrade.
--    All other versions remain DRAFT (the column default).
UPDATE domain_versions
   SET status = 'PUBLISHED'
 WHERE mcp_enabled = true
   AND status <> 'PUBLISHED';

-- 5. Mirror status into the ``info`` JSONB blob ------------------------------
--    Reads merge ``status`` from the column, but keeping the blob in sync
--    avoids surprises for any path that inspects ``info`` directly and keeps
--    exported snapshots self-describing.
UPDATE domain_versions
   SET info = jsonb_set(COALESCE(info, '{}'::jsonb), '{status}', to_jsonb(status))
 WHERE COALESCE(info ->> 'status', '') <> status;

COMMIT;

-- Summary -------------------------------------------------------------------
\echo 'Done. Lifecycle status distribution:'
SELECT status, count(*) AS versions
FROM domain_versions
GROUP BY status
ORDER BY status;
