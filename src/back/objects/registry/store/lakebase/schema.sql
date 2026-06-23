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
    -- Per-domain review sign-off quorum: how many distinct approvals are
    -- required before an IN-REVIEW version can be published. Always >= 1.
    review_quorum   integer NOT NULL DEFAULT 1
                    CHECK (review_quorum >= 1),
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
    -- Lifecycle status gating editability and API access.
    status          text NOT NULL DEFAULT 'DRAFT'
                    CHECK (status IN ('DRAFT', 'IN-REVIEW', 'PUBLISHED')),
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
CREATE INDEX IF NOT EXISTS idx_domain_versions_status
    ON domain_versions(domain_id, status);

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

-- ----------------------------------------------------------------
-- Build-run trace (one immutable row per Knowledge Graph build, all
-- paths: UI session / external API / scheduler). Linked to the
-- domain row; grain is the tuple (domain_id, version). Many rows per
-- tuple are expected — the "active" build for a (domain, version) is
-- the most recent successful row by ``started_at`` (derived, no flag).
-- Powers the registry Build Analytics panel.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS build_runs (
    id                  bigserial PRIMARY KEY,
    domain_id           uuid NOT NULL
                        REFERENCES domains(id) ON DELETE CASCADE,
    version             text NOT NULL,
    build_kind          text NOT NULL DEFAULT 'session',  -- session|api|scheduled
    status              text NOT NULL,                    -- success|error|cancelled
    message             text NOT NULL DEFAULT '',
    error               text NOT NULL DEFAULT '',
    started_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    duration_s          double precision NOT NULL DEFAULT 0,
    triple_count        bigint NOT NULL DEFAULT 0,
    entity_count        integer NOT NULL DEFAULT 0,
    relationship_count  integer NOT NULL DEFAULT 0,
    sql_chars           integer NOT NULL DEFAULT 0,
    graph_engine        text NOT NULL DEFAULT '',
    sync_mode           text NOT NULL DEFAULT '',
    view_table          text NOT NULL DEFAULT '',
    graph_name          text NOT NULL DEFAULT '',
    task_id             text NOT NULL DEFAULT '',
    phase_times         jsonb NOT NULL DEFAULT '{}'::jsonb,
    stats               jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_build_runs_domain_version
    ON build_runs(domain_id, version, started_at DESC);

-- ----------------------------------------------------------------
-- Domain-version review / validation audit log (append-only).
-- One immutable row per workflow decision or lifecycle change:
-- submit-for-review, business-user sign-off (approve), request
-- changes, publish, reopen, or a free-text comment. ``from_status``
-- / ``to_status`` snapshot the lifecycle transition the event drove
-- ('' on pure sign-off / comment rows). The grain is the tuple
-- (domain_id, version); many rows per tuple are expected — together
-- they form the full validation history surfaced in the Domain
-- Validation page and the Registry "My Tasks" worklist.
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS domain_review_events (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id       uuid NOT NULL
                    REFERENCES domains(id) ON DELETE CASCADE,
    version         text NOT NULL,
    actor           text NOT NULL,
    action          text NOT NULL
                    CHECK (action IN ('submitted', 'approved',
                                      'changes_requested', 'published',
                                      'reopened', 'commented')),
    from_status     text NOT NULL DEFAULT '',   -- lifecycle status before the event
    to_status       text NOT NULL DEFAULT '',   -- lifecycle status after the event
    comment         text NOT NULL DEFAULT '',
    meta            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_review_events_domain_version
    ON domain_review_events(domain_id, version, created_at);

-- ----------------------------------------------------------------
-- Collaborative comments — domain-wide threaded discussion. Every
-- comment belongs to the single per-(domain, version) thread. A
-- non-empty ``parent_id`` makes the row a reply within a thread.
-- Append-only; ``resolved`` closes a thread without losing history.
-- Grain: (domain_id, version).
-- ----------------------------------------------------------------
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

CREATE INDEX IF NOT EXISTS idx_domain_comments_lookup
    ON domain_comments(domain_id, version, created_at);

-- ----------------------------------------------------------------
-- Collaborative tasks — a personalised work item assigned to a
-- teammate, usually born from a comment (``comment_id``). Surfaced in
-- the assignee's "My Tasks" worklist. ``status`` walks
-- open -> in_progress -> done (or cancelled). Grain: (domain_id, version).
-- ----------------------------------------------------------------
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
