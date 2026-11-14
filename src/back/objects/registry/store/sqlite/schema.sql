-- OntoBricks SQLite registry schema (idempotent).
--
-- SQLite counterpart of ../lakebase/schema.sql: same tables, columns and
-- constraints, so the Lakebase registry mixins run against it unchanged.
-- The database file is ATTACHed under the registry schema name, which the
-- __SCHEMA__ token stands for. Postgres type names are kept as declared
-- types (jsonb, timestamptz, boolean, uuid) because SQLiteConnection reads
-- them to convert values back the way psycopg returns them.
--
-- Defaults: gen_random_uuid() becomes a random UUID v4 expression and now()
-- becomes an ISO-8601 UTC timestamp with milliseconds, the exact format the
-- now() SQL function registered by SQLiteConnection returns, so timestamps
-- sort correctly as text.

CREATE TABLE IF NOT EXISTS __SCHEMA__.registries (
    id              uuid PRIMARY KEY DEFAULT (__UUID__),
    name            text NOT NULL UNIQUE,
    catalog         text NOT NULL,
    schema          text NOT NULL,
    volume          text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT (__NOW__),
    updated_at      timestamptz NOT NULL DEFAULT (__NOW__)
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.global_config (
    registry_id     uuid PRIMARY KEY REFERENCES registries(id) ON DELETE CASCADE,
    config          jsonb NOT NULL DEFAULT '{}',
    updated_at      timestamptz NOT NULL DEFAULT (__NOW__)
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.domains (
    id              uuid PRIMARY KEY DEFAULT (__UUID__),
    registry_id     uuid NOT NULL REFERENCES registries(id) ON DELETE CASCADE,
    folder          text NOT NULL,
    description     text NOT NULL DEFAULT '',
    base_uri        text NOT NULL DEFAULT '',
    review_quorum   integer NOT NULL DEFAULT 1 CHECK (review_quorum >= 1),
    mcp_policy      jsonb NOT NULL DEFAULT '{}',
    created_at      timestamptz NOT NULL DEFAULT (__NOW__),
    updated_at      timestamptz NOT NULL DEFAULT (__NOW__),
    UNIQUE (registry_id, folder)
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_domains_registry ON domains(registry_id);

CREATE TABLE IF NOT EXISTS __SCHEMA__.domain_versions (
    id              uuid PRIMARY KEY DEFAULT (__UUID__),
    domain_id       uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    version         text NOT NULL,
    info            jsonb NOT NULL DEFAULT '{}',
    ontology        jsonb NOT NULL DEFAULT '{}',
    assignment      jsonb NOT NULL DEFAULT '{}',
    design_layout   jsonb NOT NULL DEFAULT '{}',
    metadata        jsonb NOT NULL DEFAULT '{}',
    mcp_enabled     boolean NOT NULL DEFAULT 0,
    status          text NOT NULL DEFAULT 'DRAFT'
                    CHECK (status IN ('DRAFT', 'IN-REVIEW', 'PUBLISHED')),
    last_update     text NOT NULL DEFAULT '',
    last_build      text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT (__NOW__),
    updated_at      timestamptz NOT NULL DEFAULT (__NOW__),
    UNIQUE (domain_id, version)
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_domain_versions_domain ON domain_versions(domain_id);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_domain_versions_mcp ON domain_versions(domain_id) WHERE mcp_enabled;
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_domain_versions_status ON domain_versions(domain_id, status);

CREATE TABLE IF NOT EXISTS __SCHEMA__.domain_permissions (
    id              uuid PRIMARY KEY DEFAULT (__UUID__),
    domain_id       uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    principal       text NOT NULL,
    principal_type  text NOT NULL,
    display_name    text NOT NULL DEFAULT '',
    role            text NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT (__NOW__),
    UNIQUE (domain_id, principal)
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_domain_permissions_principal ON domain_permissions(lower(principal));

CREATE TABLE IF NOT EXISTS __SCHEMA__.schedules (
    id                  uuid PRIMARY KEY DEFAULT (__UUID__),
    registry_id         uuid NOT NULL REFERENCES registries(id) ON DELETE CASCADE,
    task_type           text NOT NULL DEFAULT 'build',
    domain_name         text NOT NULL,
    target_key          text NOT NULL DEFAULT '',
    interval_minutes    integer NOT NULL,
    drop_existing       boolean NOT NULL DEFAULT 1,
    enabled             boolean NOT NULL DEFAULT 1,
    version             text NOT NULL DEFAULT 'latest',
    config              jsonb NOT NULL DEFAULT '{}',
    last_run            timestamptz,
    last_status         text,
    last_message        text,
    last_count          bigint NOT NULL DEFAULT 0,
    updated_at          timestamptz NOT NULL DEFAULT (__NOW__),
    CONSTRAINT schedules_type_domain_target_key
        UNIQUE (registry_id, task_type, domain_name, target_key)
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.schedule_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    registry_id     uuid NOT NULL REFERENCES registries(id) ON DELETE CASCADE,
    task_type       text NOT NULL DEFAULT 'build',
    domain_name     text NOT NULL,
    target_key      text NOT NULL DEFAULT '',
    run_ts          timestamptz NOT NULL DEFAULT (__NOW__),
    status          text NOT NULL,
    message         text NOT NULL DEFAULT '',
    duration_s      double precision NOT NULL DEFAULT 0,
    triple_count    bigint NOT NULL DEFAULT 0,
    detail          jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_schedule_runs_domain
    ON schedule_runs(registry_id, task_type, domain_name, target_key, run_ts DESC);

CREATE TABLE IF NOT EXISTS __SCHEMA__.build_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    domain_id           uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    version             text NOT NULL,
    build_kind          text NOT NULL DEFAULT 'session',
    status              text NOT NULL,
    message             text NOT NULL DEFAULT '',
    error               text NOT NULL DEFAULT '',
    started_at          timestamptz NOT NULL DEFAULT (__NOW__),
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
    phase_times         jsonb NOT NULL DEFAULT '{}',
    stats               jsonb NOT NULL DEFAULT '{}',
    created_at          timestamptz NOT NULL DEFAULT (__NOW__)
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_build_runs_domain_version
    ON build_runs(domain_id, version, started_at DESC);

CREATE TABLE IF NOT EXISTS __SCHEMA__.graph_analytics (
    domain_id    uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    version      text NOT NULL,
    status       text NOT NULL DEFAULT 'completed',
    graph_name   text NOT NULL DEFAULT '',
    class_filter jsonb NOT NULL DEFAULT '[]',
    stats        jsonb NOT NULL DEFAULT '{}',
    top_pagerank jsonb NOT NULL DEFAULT '[]',
    result       jsonb NOT NULL DEFAULT '{}',
    error        text NOT NULL DEFAULT '',
    task_id      text NOT NULL DEFAULT '',
    duration_ms  bigint NOT NULL DEFAULT 0,
    computed_at  timestamptz NOT NULL DEFAULT (__NOW__),
    PRIMARY KEY (domain_id, version)
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.graph_analytics_runs (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    domain_id            uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    version              text NOT NULL,
    status               text NOT NULL DEFAULT 'completed',
    class_filter         jsonb NOT NULL DEFAULT '[]',
    node_count           bigint NOT NULL DEFAULT 0,
    edge_count           bigint NOT NULL DEFAULT 0,
    connected_components integer NOT NULL DEFAULT 0,
    avg_degree           double precision NOT NULL DEFAULT 0,
    density              double precision NOT NULL DEFAULT 0,
    duration_ms          bigint NOT NULL DEFAULT 0,
    task_id              text NOT NULL DEFAULT '',
    error                text NOT NULL DEFAULT '',
    computed_at          timestamptz NOT NULL DEFAULT (__NOW__)
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_graph_analytics_runs_domain_version
    ON graph_analytics_runs(domain_id, version, computed_at DESC);

CREATE TABLE IF NOT EXISTS __SCHEMA__.domain_review_events (
    id              uuid PRIMARY KEY DEFAULT (__UUID__),
    domain_id       uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    version         text NOT NULL,
    actor           text NOT NULL,
    action          text NOT NULL
                    CHECK (action IN ('submitted', 'approved', 'changes_requested',
                                      'published', 'reopened', 'commented')),
    from_status     text NOT NULL DEFAULT '',
    to_status       text NOT NULL DEFAULT '',
    comment         text NOT NULL DEFAULT '',
    meta            jsonb NOT NULL DEFAULT '{}',
    created_at      timestamptz NOT NULL DEFAULT (__NOW__)
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_review_events_domain_version
    ON domain_review_events(domain_id, version, created_at);

CREATE TABLE IF NOT EXISTS __SCHEMA__.domain_change_events (
    id              uuid PRIMARY KEY DEFAULT (__UUID__),
    domain_id       uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    version         text NOT NULL,
    actor           text NOT NULL DEFAULT '',
    source          text NOT NULL DEFAULT 'user' CHECK (source IN ('user', 'agent')),
    action          text NOT NULL,
    entity_type     text NOT NULL DEFAULT '',
    entity_ref      text NOT NULL DEFAULT '',
    summary         text NOT NULL DEFAULT '',
    meta            jsonb NOT NULL DEFAULT '{}',
    occurred_at     timestamptz NOT NULL DEFAULT (__NOW__),
    created_at      timestamptz NOT NULL DEFAULT (__NOW__)
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_change_events_domain_version
    ON domain_change_events(domain_id, version, occurred_at);

CREATE TABLE IF NOT EXISTS __SCHEMA__.domain_comments (
    id          uuid PRIMARY KEY DEFAULT (__UUID__),
    domain_id   uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    version     text NOT NULL,
    parent_id   uuid REFERENCES domain_comments(id) ON DELETE CASCADE,
    author      text NOT NULL,
    body        text NOT NULL DEFAULT '',
    resolved    boolean NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT (__NOW__)
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_domain_comments_lookup
    ON domain_comments(domain_id, version, created_at);

CREATE TABLE IF NOT EXISTS __SCHEMA__.domain_tasks (
    id          uuid PRIMARY KEY DEFAULT (__UUID__),
    domain_id   uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    version     text NOT NULL,
    assignee    text NOT NULL,
    created_by  text NOT NULL,
    title       text NOT NULL,
    description text NOT NULL DEFAULT '',
    status      text NOT NULL DEFAULT 'open'
                CHECK (status IN ('open', 'in_progress', 'done', 'cancelled')),
    due_date    date,
    comment_id  uuid REFERENCES domain_comments(id) ON DELETE SET NULL,
    created_at  timestamptz NOT NULL DEFAULT (__NOW__),
    updated_at  timestamptz NOT NULL DEFAULT (__NOW__)
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_domain_tasks_assignee ON domain_tasks(lower(assignee), status);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_domain_tasks_domain ON domain_tasks(domain_id, version);

CREATE TABLE IF NOT EXISTS __SCHEMA__.domain_edit_locks (
    domain_id      uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    version        text NOT NULL,
    holder_email   text NOT NULL,
    holder_name    text NOT NULL DEFAULT '',
    holder_session text NOT NULL DEFAULT '',
    acquired_at    timestamptz NOT NULL DEFAULT (__NOW__),
    heartbeat_at   timestamptz NOT NULL DEFAULT (__NOW__),
    PRIMARY KEY (domain_id, version)
);

CREATE TABLE IF NOT EXISTS __SCHEMA__.domain_documents (
    id            uuid PRIMARY KEY DEFAULT (__UUID__),
    domain_id     uuid NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    version       text NOT NULL,
    filename      text NOT NULL,
    source_hash   text NOT NULL,
    parser        text NOT NULL,
    status        text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'ready', 'failed')),
    parsed_text   text NOT NULL DEFAULT '',
    source_bytes  bytea,
    size_bytes    bigint NOT NULL DEFAULT 0,
    output_schema text NOT NULL DEFAULT '',
    error         text NOT NULL DEFAULT '',
    parsed_at     timestamptz,
    created_at    timestamptz NOT NULL DEFAULT (__NOW__),
    updated_at    timestamptz NOT NULL DEFAULT (__NOW__),
    UNIQUE (domain_id, version, filename)
);
CREATE INDEX IF NOT EXISTS __SCHEMA__.idx_domain_documents_lookup ON domain_documents(domain_id, version);
