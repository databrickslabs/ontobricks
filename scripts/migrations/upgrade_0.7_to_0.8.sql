-- ============================================================================
-- OntoBricks Lakebase registry upgrade: 0.7.x  ->  0.8
-- ----------------------------------------------------------------------------
-- Single, clean, one-shot migration to the FINAL 0.8 registry schema. This is
-- the ONE script to run to bring a 0.7.x registry up to the complete 0.8
-- shape — it folds in the only registry DDL delta of the 0.8 cycle:
--
--   * domains — per-domain ``mcp_policy`` jsonb column holding the MCP
--                surface configuration edited in Domain → Information → MCP:
--                which MCP tools the domain publishes, and how the three
--                ontology attachments (dataset, bridges, actions) are
--                surfaced. Shape:
--                  {"disabled_tools": ["query_graphql"],
--                   "context": {"bridges": "preferred",
--                               "actions": "disabled"}}
--
-- No backfill is required. The column defaults to the empty blob ``{}``,
-- which means "every tool exposed, every attachment normal" — exactly the
-- pre-0.8 behaviour. Storing *disabled* tools rather than enabled ones is
-- what makes that default safe.
--
-- These changes mirror the canonical
-- ``src/back/objects/registry/store/lakebase/schema.sql`` and
-- ``LakebaseRegistryStore._ensure_domains_mcp_policy_column``. The app
-- self-heals the column lazily (and eagerly on Settings → Registry →
-- Initialize), and ``make bootstrap-lakebase`` /
-- ``scripts/bootstrap/lakebase-perms.sh`` Step 2b provisions it as the
-- schema owner on every Lakebase deploy. Run this script when you prefer
-- an explicit, auditable one-shot migration (e.g. a DBA applying it
-- out-of-band).
--
-- Idempotent: safe to run multiple times. No data discarded.
-- ----------------------------------------------------------------------------
-- Usage (psql):
--   # default schema (ontobricks_registry):
--   psql "$PGURL" -f scripts/migrations/upgrade_0.7_to_0.8.sql
--
--   # custom registry schema (matches LAKEBASE_SCHEMA / REGISTRY_SCHEMA):
--   psql "$PGURL" -v reg_schema=my_registry_schema \
--        -f scripts/migrations/upgrade_0.7_to_0.8.sql
-- ============================================================================

\set ON_ERROR_STOP on

-- Resolve the target schema (override with  -v reg_schema=...  ; default below).
\if :{?reg_schema}
\else
  \set reg_schema ontobricks_registry
\endif

SET search_path TO :"reg_schema";

\echo 'Upgrading OntoBricks registry schema to 0.8:' :reg_schema

BEGIN;

-- 1. domains — per-domain MCP policy -----------------------------------------
ALTER TABLE domains
    ADD COLUMN IF NOT EXISTS mcp_policy jsonb NOT NULL DEFAULT '{}'::jsonb;

-- 2. Sanity check -------------------------------------------------------------
DO $check$
DECLARE
    missing text;
BEGIN
    SELECT string_agg(w.tbl || '.' || w.col, ', ' ORDER BY w.tbl, w.col)
      INTO missing
    FROM (
        VALUES
            ('domains', 'mcp_policy')
    ) AS w(tbl, col)
    WHERE NOT EXISTS (
        SELECT 1
        FROM information_schema.columns c
        WHERE c.table_schema = current_schema()
          AND c.table_name = w.tbl
          AND c.column_name = w.col
    );

    IF missing IS NOT NULL THEN
        RAISE EXCEPTION '0.7→0.8 upgrade incomplete — missing columns: %', missing;
    END IF;
END
$check$;

COMMIT;

\echo 'Done. Registry columns present:'
SELECT table_name, column_name, data_type, column_default
FROM information_schema.columns
WHERE table_schema = :'reg_schema'
  AND table_name = 'domains'
  AND column_name = 'mcp_policy';

\echo 'Done — registry schema' :reg_schema 'is at the 0.8 shape.'
