-- ============================================================================
-- OntoBricks Lakebase registry upgrade: 0.6.x  ->  0.7
-- ----------------------------------------------------------------------------
-- Single, clean, one-shot migration to the FINAL 0.7 registry schema. This is
-- the ONE script to run to bring a 0.6.x registry up to the complete 0.7
-- shape — it folds in the only registry DDL delta of the 0.7 cycle:
--
--   * schedules     — generic scheduled-task columns ``task_type``,
--                     ``target_key``, ``config``, ``last_count``; legacy
--                     ``drop_existing`` folded into ``config`` for builds;
--                     unique constraint swapped from
--                     UNIQUE (registry_id, domain_name) to
--                     schedules_type_domain_target_key
--                     UNIQUE (registry_id, task_type, domain_name, target_key).
--   * schedule_runs — matching ``task_type`` / ``target_key`` / ``detail``
--                     columns; ``idx_schedule_runs_domain`` widened to the
--                     new grain.
--
-- These changes mirror the canonical
-- ``src/back/objects/registry/store/lakebase/schema.sql`` and
-- ``LakebaseRegistryStore._ensure_schedule_task_columns``. Existing rows
-- default to ``task_type = 'build'`` / empty ``target_key``, so builds keep
-- working untouched.
--
-- The app self-heals these columns lazily (and eagerly on Settings →
-- Registry → Initialize), and ``make bootstrap-lakebase`` /
-- ``scripts/bootstrap/lakebase-perms.sh`` Step 2b provisions them as the
-- schema owner on every Lakebase deploy. Run this script when you prefer
-- an explicit, auditable one-shot migration (e.g. a DBA applying it
-- out-of-band).
--
-- Idempotent: safe to run multiple times. No data discarded.
-- ----------------------------------------------------------------------------
-- Usage (psql):
--   # default schema (ontobricks_registry):
--   psql "$PGURL" -f scripts/migrations/upgrade_0.6_to_0.7.sql
--
--   # custom registry schema (matches LAKEBASE_SCHEMA / REGISTRY_SCHEMA):
--   psql "$PGURL" -v reg_schema=my_registry_schema \
--        -f scripts/migrations/upgrade_0.6_to_0.7.sql
-- ============================================================================

\set ON_ERROR_STOP on

-- Resolve the target schema (override with  -v reg_schema=...  ; default below).
\if :{?reg_schema}
\else
  \set reg_schema ontobricks_registry
\endif

SET search_path TO :"reg_schema";

\echo 'Upgrading OntoBricks registry schema to 0.7:' :reg_schema

BEGIN;

-- 1. schedules — generic task columns ---------------------------------------
ALTER TABLE schedules
    ADD COLUMN IF NOT EXISTS task_type text NOT NULL DEFAULT 'build',
    ADD COLUMN IF NOT EXISTS target_key text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS config jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS last_count bigint NOT NULL DEFAULT 0;

-- 2. schedule_runs — matching columns ---------------------------------------
ALTER TABLE schedule_runs
    ADD COLUMN IF NOT EXISTS task_type text NOT NULL DEFAULT 'build',
    ADD COLUMN IF NOT EXISTS target_key text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS detail jsonb NOT NULL DEFAULT '{}'::jsonb;

-- 3. Fold legacy drop_existing into config (build rows only) ----------------
UPDATE schedules
SET config = jsonb_build_object('drop_existing', COALESCE(drop_existing, true))
WHERE config = '{}'::jsonb AND task_type = 'build';

-- 4. Swap unique constraint to (task_type, domain, target_key) --------------
DO $sched$
DECLARE
    cname text;
BEGIN
    FOR cname IN
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace ns ON ns.oid = rel.relnamespace
        WHERE ns.nspname = current_schema()
          AND rel.relname = 'schedules'
          AND con.contype = 'u'
          AND pg_get_constraintdef(con.oid)
              = 'UNIQUE (registry_id, domain_name)'
    LOOP
        EXECUTE format(
            'ALTER TABLE schedules DROP CONSTRAINT IF EXISTS %I',
            cname
        );
    END LOOP;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace ns ON ns.oid = rel.relnamespace
        WHERE ns.nspname = current_schema()
          AND rel.relname = 'schedules'
          AND con.conname = 'schedules_type_domain_target_key'
    ) THEN
        ALTER TABLE schedules
            ADD CONSTRAINT schedules_type_domain_target_key
            UNIQUE (registry_id, task_type, domain_name, target_key);
    END IF;
END
$sched$;

-- 5. Widen runs index to the new grain --------------------------------------
DROP INDEX IF EXISTS idx_schedule_runs_domain;
CREATE INDEX IF NOT EXISTS idx_schedule_runs_domain
    ON schedule_runs(
        registry_id, task_type, domain_name, target_key, run_ts DESC
    );

-- 6. Sanity check -----------------------------------------------------------
DO $check$
DECLARE
    missing text;
BEGIN
    SELECT string_agg(w.tbl || '.' || w.col, ', ' ORDER BY w.tbl, w.col)
      INTO missing
    FROM (
        VALUES
            ('schedules', 'task_type'),
            ('schedules', 'target_key'),
            ('schedules', 'config'),
            ('schedules', 'last_count'),
            ('schedule_runs', 'task_type'),
            ('schedule_runs', 'target_key'),
            ('schedule_runs', 'detail')
    ) AS w(tbl, col)
    WHERE NOT EXISTS (
        SELECT 1
        FROM information_schema.columns c
        WHERE c.table_schema = current_schema()
          AND c.table_name = w.tbl
          AND c.column_name = w.col
    );

    IF missing IS NOT NULL THEN
        RAISE EXCEPTION '0.6→0.7 upgrade incomplete — missing columns: %', missing;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace ns ON ns.oid = rel.relnamespace
        WHERE ns.nspname = current_schema()
          AND rel.relname = 'schedules'
          AND con.conname = 'schedules_type_domain_target_key'
    ) THEN
        RAISE EXCEPTION
            '0.6→0.7 upgrade incomplete — constraint schedules_type_domain_target_key missing';
    END IF;
END
$check$;

COMMIT;

\echo 'Done. Registry columns present:'
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = :'reg_schema'
  AND (
      (table_name = 'schedules'
       AND column_name IN ('task_type', 'target_key', 'config', 'last_count'))
      OR (table_name = 'schedule_runs'
          AND column_name IN ('task_type', 'target_key', 'detail'))
  )
ORDER BY table_name, column_name;

\echo 'Done — registry schema' :reg_schema 'is at the 0.7 shape.'
