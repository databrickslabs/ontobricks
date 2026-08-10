# Neo4j Named Connections Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline) or superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single flat Neo4j Settings profile with a named `connections[]` list; require Domain → Neo4j to pick a connection by name; block delete/rename while domains reference it.

**Architecture:** Persist `graph_engine_config.neo4j.connections[]`. Resolve at runtime via `domain.info.neo4j_connection` → matching connection profile. Settings UI becomes master–detail; Domain dropdown lists connection names. No auto-migration of legacy flat keys.

**Tech Stack:** Python (engine_config, SettingsService, GraphDBFactory, DomainSession), FastAPI routes, Jinja/JS Settings + Domain UI, pytest.

**Spec:** `documentation/superpowers/specs/2026-08-07-neo4j-named-connections-design.md`

## Global Constraints

- No auto-migration of flat `uri`/`username` Neo4j config into a named connection.
- Password never persisted; `auth_method` remains `databricks_secret` in the UI.
- Domain Neo4j requires `neo4j_connection`; no blank “use default”.
- Block delete/rename when any domain references the connection name.
- Objects/Health/Test use the selected connection (or draft fields for Test).
- Run `uv run --frozen pytest -q -m "not scenario"` after changes.
- Update `changelogs/v0.7.0/benoitcayladbx_2026-08-07.log`.
- Do not create a Git commit unless the user explicitly requests one.

---

## File Map

- `src/back/core/graphdb/engine_config.py` — `connections[]` finalize + `resolve_neo4j_connection(cfg, name)`
- `src/back/core/graphdb/GraphDBFactory.py` — resolve named connection for Neo4jStore
- `src/back/core/graphdb/neo4j/Neo4jConnection.py` / `Neo4jStore.py` — accept profile dict (already do)
- `src/back/objects/session/DomainSession.py` / `Domain.py` — `neo4j_connection` field
- `src/back/objects/domain/SettingsService.py` — list/resolve connections; strip passwords per entry; referential integrity on save; connection_name for test/health/labels/drop
- `src/api/routers/internal/settings.py` — `GET neo4j-connections`; pass `connection_name` query/body
- `src/front/templates/settings.html` + `settings.js` — master–detail connections UI
- `src/front/templates/partials/domain/_domain_information.html` + `domain-information.js` / `domain.js` — connection dropdown
- Tests under `tests/units/graphdb/`, `tests/units/settings/`, `tests/units/domain/`

### Task 1: Engine config — connections helpers

**Files:**
- Modify: `src/back/core/graphdb/engine_config.py`
- Modify: `tests/units/graphdb/test_graph_engine_config_keys.py`

**Produces:**
- `list_neo4j_connections(cfg) -> list[dict]`
- `resolve_neo4j_connection(cfg, name: str) -> dict` (raises/returns empty on miss)
- `_finalize_neo4j_bucket` keeps `connections` list; does not invent connections from flat keys

- [ ] Add helpers + tests for list/resolve/unique names; ensure flat uri alone does not become a connection
- [ ] Implement finalize + helpers
- [ ] Tests pass

### Task 2: Domain field `neo4j_connection`

**Files:**
- Modify: `DomainSession.py`, `Domain.py`
- Modify: `tests/units/domain/test_domain_session.py`

- [ ] Replace `neo4j_database` read/write with `neo4j_connection` (stop writing `neo4j_database`)
- [ ] Update domain session tests

### Task 3: Runtime resolve in GraphDBFactory

**Files:**
- Modify: `GraphDBFactory._create_neo4j`
- Modify: `tests/units/graphdb/test_neo4j_store.py`

- [ ] Require `info.neo4j_connection`; resolve profile from `connections[]`; pass profile as engine_config
- [ ] Remove domain `neo4j_database` override behaviour
- [ ] Update factory tests

### Task 4: SettingsService + API

**Files:**
- Modify: `SettingsService.py`, `settings.py` router
- Test: new/updated unit tests for connections list, password strip, referential check

- [ ] `graph_engine_neo4j_connections_result` — list connections (no passwords)
- [ ] `set_graph_engine_config_result` — strip password per connection; detect removed/renamed names vs previous config; scan domains (latest version `info.neo4j_connection`) and reject with domain list
- [ ] `_neo4j_connection_from_config(..., connection_name=)` + update test/labels/health/drop
- [ ] Routes: GET `/neo4j-connections`; pass `connection_name` on test/health/labels/drop

### Task 5: Settings UI master–detail

**Files:**
- Modify: `settings.html`, `settings.js`
- Modify: `tests/units/settings/test_neo4j_settings_persistence.py`

- [ ] Replace single form with list + detail form; merge writes `neo4j.connections[]`
- [ ] Test/Objects/Health use selected connection name
- [ ] Update persistence contract tests

### Task 6: Domain UI dropdown

**Files:**
- Modify: `_domain_information.html`, `domain-information.js`, `domain.js`
- Optional: small front contract test

- [ ] Relabel to Neo4j Connection; load `/neo4j-connections`; required; no empty default
- [ ] Save `neo4j_connection`

### Task 7: Changelog + full test run

- [ ] Append changelog section
- [ ] `uv run --frozen pytest -q -m "not scenario"`
- [ ] Record result in changelog

---

## Spec coverage checklist

| Spec requirement | Task |
|------------------|------|
| `connections[]` data model | 1, 4, 5 |
| Domain `neo4j_connection` required | 2, 3, 6 |
| No flat auto-migration | 1 |
| Block delete/rename if referenced | 4 |
| Master–detail Settings UI | 5 |
| Domain dropdown lists names | 6 |
| Objects/Health on selected row | 4, 5 |
| Tests | 1–6, 7 |
