-- OntoBricks Lakebase registry schema (idempotent).
--
-- Applied on first ``LakebaseRegistryStore.initialize()`` call. Every
-- statement uses ``IF NOT EXISTS`` so re-applying the schema is safe.
--
-- Schema name is parameterised at runtime via psycopg's
-- ``sql.Identifier`` substitution; the literal ``__SCHEMA__`` token below
-- is replaced before execution. The default value is
-- ``ontobricks_registry``.

CREATE SCHEMA IF NOT EXISTS __SCHEMA__;
SET search_path TO __SCHEMA__;

-- ----------------------------------------------------------------
-- Registry identity (one row per OntoBricks instance/registry)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS registries (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text NOT NULL UNIQUE,
    catalog         text NOT NULL,
    schema          text NOT NULL,
    volume          text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------
-- Global configuration (single-row blob; warehouse_id, base_uri, …)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS global_config (
    registry_id     uuid PRIMARY KEY
                    REFERENCES registries(id) ON DELETE CASCADE,
    config          jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------
-- Domains
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS domains (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    registry_id     uuid NOT NULL
                    REFERENCES registries(id) ON DELETE CASCADE,
    folder          text NOT NULL,
    description     text NOT NULL DEFAULT '',
    base_uri        text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (registry_id, folder)
);

CREATE INDEX IF NOT EXISTS idx_domains_registry ON domains(registry_id);

-- ----------------------------------------------------------------
-- Domain versions
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS domain_versions (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id       uuid NOT NULL
                    REFERENCES domains(id) ON DELETE CASCADE,
    version         text NOT NULL,
    info            jsonb NOT NULL DEFAULT '{}'::jsonb,
    ontology        jsonb NOT NULL DEFAULT '{}'::jsonb,
    assignment      jsonb NOT NULL DEFAULT '{}'::jsonb,
    design_layout   jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata        jsonb NOT NULL DEFAULT '{}'::jsonb,
    -- Hot fields denormalised from ``info`` for cheap listing queries.
    mcp_enabled     boolean NOT NULL DEFAULT false,
    last_update     text NOT NULL DEFAULT '',
    last_build      text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (domain_id, version)
);

CREATE INDEX IF NOT EXISTS idx_domain_versions_domain
    ON domain_versions(domain_id);
CREATE INDEX IF NOT EXISTS idx_domain_versions_mcp
    ON domain_versions(domain_id) WHERE mcp_enabled;

-- ----------------------------------------------------------------
-- Domain-level permissions (Viewer / Editor / Builder per principal)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS domain_permissions (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id       uuid NOT NULL
                    REFERENCES domains(id) ON DELETE CASCADE,
    principal       text NOT NULL,
    principal_type  text NOT NULL,            -- 'user' | 'group'
    display_name    text NOT NULL DEFAULT '',
    role            text NOT NULL,            -- 'viewer'|'editor'|'builder'
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (domain_id, principal)
);

CREATE INDEX IF NOT EXISTS idx_domain_permissions_principal
    ON domain_permissions(lower(principal));

-- ----------------------------------------------------------------
-- Scheduled builds
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schedules (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    registry_id         uuid NOT NULL
                        REFERENCES registries(id) ON DELETE CASCADE,
    domain_name         text NOT NULL,
    interval_minutes    integer NOT NULL,
    drop_existing       boolean NOT NULL DEFAULT true,
    enabled             boolean NOT NULL DEFAULT true,
    version             text NOT NULL DEFAULT 'latest',
    last_run            timestamptz,
    last_status         text,
    last_message        text,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (registry_id, domain_name)
);

-- ----------------------------------------------------------------
-- Scheduled-build run history (capped server-side per domain)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schedule_runs (
    id              bigserial PRIMARY KEY,
    registry_id     uuid NOT NULL
                    REFERENCES registries(id) ON DELETE CASCADE,
    domain_name     text NOT NULL,
    run_ts          timestamptz NOT NULL DEFAULT now(),
    status          text NOT NULL,
    message         text NOT NULL DEFAULT '',
    duration_s      double precision NOT NULL DEFAULT 0,
    triple_count    bigint NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_schedule_runs_domain
    ON schedule_runs(registry_id, domain_name, run_ts DESC);
