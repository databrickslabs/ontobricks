# OntoBricks — Release Notes V0.6.0

**Release date:** 2026-06-27 (updated 2026-07-02)
**Type:** Major feature release
**Test status:** 2967 passed, 275 skipped, 5 deselected (scenario), 0 failed
(`uv run pytest -q -m "not scenario"`). The former full-suite async-endpoint
ordering failures are gone: the nightly Playwright e2e browser suite now
auto-skips in routine runs, so it no longer poisons the in-process event loop.

---

## Summary

v0.6.0 is the largest feature release since v0.5.0. It ships three independent pillars
of new capability:

1. **Collaborative Comments & Tasks** — domain-scoped discussion threads, task management,
   with a shared team Discussion accessible from every surface (Ontology, Mapping, Graph
   Explorer, Validation, and the new Domain → Collaboration page).

2. **Graph Analytics** — server-side centrality metrics (PageRank, betweenness, degree,
   closeness, clustering), a Data Model Health card, and an AI interpretation agent that
   explains and recommends improvements grounded in live entity data.

3. **Mapping Designer depth** — per-attribute include/exclude, always-quoted column names,
   append-mode OWL/RDFS/SKOS/SHACL import with conflict detection, and a richer
   Ontology Designer (neighbourhood highlight, entity search, inheritance toggle, Business
   View auto-generation).

In addition the release includes a suite of UX improvements (sidebar user panel, App Logs
viewer, Registry access check, domain menu restructure) and a significant set of bug fixes.

The 2026-06-30 update adds three operational-hardening items on top of the same
version: **Domain Edit-Lock** (single-editor concurrency control for DRAFT versions),
**In-app Registry Permission Grants** (apply Lakebase grants from the UI, no `psql`),
and a **deploy-script dependency preflight**.

The 2026-07-01 update adds an **Ontology & Mapping Audit Trail** — a fine-grained,
buffered-then-flushed record of every design change (who / what / when), surfaced as a
third stream in the Domain audit-trail timeline, with AI-assistant edits tagged. It also
reworks the Domain Edit-Lock (release-based, then leased — see below), ships an admin
**Settings → Locks** panel (registry-wide lock overview + force-unlock), a
**close-before-open** switch so you never hold two DRAFT locks, and a `make
scenario-campaign` entrypoint for the live end-to-end journey suites.

The 2026-07-02 update gives the Domain Edit-Lock a **renew-only lease** so abandoned
locks (crashed browser / closed tab) **auto-expire** instead of needing an admin
force-unlock. The holder's browser keeps the lease alive with a background renew ping;
the lock is *never* released on page unload (multi-page navigation can't steal it), and
it only lapses after a full TTL with no renew — at which point the next opener silently
reclaims it. TTL is configurable via `ONTOBRICKS_EDIT_LOCK_TTL_S` (default 600 s; `0`
disables → prior hold-until-close). The navbar domain badge grows a **hover countdown
tooltip** of the remaining lease, and a lost lease flips the page to read-only with a
"session expired" banner. The edit-lock service also moved into a dedicated
`back/objects/registry/lockmgt` subpackage.

No breaking changes. All v0.5.x features are fully intact.

---

## New Features

### 1. Collaborative Comments & Tasks

Domain versions can now carry a shared Discussion thread accessible from every surface
(Ontology Designer, Mapping Designer, Graph Explorer, Domain → Validation, and the new
Domain → Collaboration page). Comments are threaded; any comment can be turned into a
task assigned to a domain member.

- **Domain → Collaboration page** — My Tasks worklist (inline Start/Done transitions)
  + activity timeline with day grouping, anchor badges, and a live tag filter.
- **Discussion offcanvas** — rendered markdown (full `marked` support), per-thread status
  chips, and a standalone "New task" button in the panel header.
- **Per-surface anchoring** — discussion buttons on the Ontology Designer toolbar (whole
  diagram), on entity/relationship edit panels, on mapping modals, and on Graph Explorer
  node/edge detail panels.
- **Lakebase schema** — two new tables (`domain_comments`, `domain_tasks`) with the
  canonical upgrade script `scripts/upgrade_lakebase_0.5_To_0.6.sql`.

### 2. Graph Analytics

A new **Analytics** tab in the Knowledge Graph Explorer computes and renders:

- **Centrality metrics**: PageRank (with formula card), Betweenness, Degree, Closeness,
  Clustering — each with a bar chart, clickable bars (navigate to Graph Viewer), metric
  explanation popup, and a detail ranking table.
- **Entity type filter**: single-select dropdown; filters the returned node set while
  computing metrics on the full connected subgraph for accuracy.
- **Data Model Health card**: detects flat/time-series entity types via four heuristics
  (low degree, low clustering, temporal predicates, high predicate count) and surfaces
  them with reasoning badges.
- **AI Interpretation** (`agent_graph_interpreter`): tool-enabled agent (up to 8 tool
  iterations) that calls `get_entity_details` to ground its insights in live data; returns
  Key Findings, Notable Entities, and Recommendations as styled Bootstrap sub-cards.
- **Graph Structure cockpit card** on Domain → Validation: node/edge count, connected
  components, avg degree, density, and top-5 PageRank nodes.

### 3. Mapping Designer — Per-Attribute Include/Exclude

Attributes (data properties) can be individually excluded from a mapping without deleting
the entity's SQL configuration:

- Checkbox column in the Status tab; "Exclude all / Include all" toggle button.
- Excluded attributes are suppressed from: gap reporting, KPI gauges, Auto-Map page, the
  agent's attribute list, R2RML output, and the Mapping Information page.
- Exclusion state survives Unmap + re-map cycles, agent auto-map updates, and server
  restarts (persisted in the registry).

### 4. Append-mode OWL/RDFS Import with Conflict Detection

The OWL and RDFS import tabs now offer a **Replace / Append** mode selector. Append mode
uses a two-phase flow:

1. `POST /ontology/analyze-import` — read-only conflict analysis (new / duplicate /
   uri_conflict / name_conflict) displayed in a persistent step timeline panel.
2. `POST /ontology/merge-import` — per-entity resolution (skip / overwrite / rename)
   before merge.

Extended format support:
- **SKOS** — `skos:Concept` / `skos:prefLabel` / `skos:broader` vocabularies.
- **Alignment files** — SKOS alignment predicates + labeled-resource fallback.
- **SHACL shapes** — auto-detected and silently routed to Data Quality instead of the
  ontology class list.

### 5. Ontology Designer Improvements

- **Neighbourhood highlight**: clicking a node or relationship dims everything else and
  applies a blue glow ring to connected neighbours/endpoints.
- **Inheritance toggle**: a persistent toolbar button hides/shows all dashed inheritance
  arrows; state preserved in `sessionStorage`.
- **Entity search popup**: sorted dropdown + Focus button that pans/zooms the SVG to the
  selected entity and triggers the neighbourhood highlight.
- **Create Business View from right-click**: builds a 1-hop layout (centre entity + all
  direct neighbours) and saves it as a new Design View (`Auto_<EntityName>`); navigates
  directly to Business Views.

### 6. Application Logs Viewer (Settings → Admin)

A new **Logs** section under Settings → Admin streams a live tail of `ontobricks.log`
with level filtering (ALL / DEBUG / INFO / WARNING / ERROR), text search, configurable
line count (default 200, max 5 000), and 10-second auto-refresh.

### 7. Registry Access Check (Settings)

The "Registry Catalog & Volume" panel now includes a **Check Access** button that runs
two parallel probes:

- **Unity Catalog**: existence + accessible status for the UC schema and Volume via REST.
- **Lakebase**: single Postgres round-trip verifying `USAGE`/`CREATE` on schema, and
  `SELECT`/`INSERT`/`UPDATE`/`DELETE` on all 10 registry tables; failing checks include
  the exact `GRANT` SQL to fix them.

### 9. User / Role Info Panel in Sidebar

All pages using the shared sidebar nav show the connected user's email and role badges
(Admin in red, domain role in blue when it differs) pinned to the sidebar footer. The
redundant role pill in the top-right navbar is hidden.

### 10. Domain Edit-Lock (renew-only lease single-editor + Close button)

Only one browser may **edit** a given DRAFT `(domain, version)` at a time. The first
opener acquires the lock and edits; every later opener lands **read-only** with a banner
naming the current editor. The lock lives in Lakebase (multi-replica safe).

The concurrency model is a **renew-only lease** (updated 2026-07-02). Once acquired, the
holder's browser keeps the lease alive with a background renew ping (~TTL/3, and on tab
re-focus); the lock is **never** released on page unload, so in a multi-page app — where
every navigation is a full unload — nothing can steal it mid-session. A lease only lapses
when **no renew arrives for a full TTL**, at which point the next opener silently reclaims
the stale lease. TTL is configurable via `ONTOBRICKS_EDIT_LOCK_TTL_S` (seconds, default
`600`; `0` disables the lease → the prior "held until explicit Close / take-over" model).
The lease clock reuses the long-dormant `heartbeat_at` column, so **no migration** is
required. This keeps ownership crisp (no release-then-reacquire churn) while ensuring a
genuinely abandoned lock (crashed browser, closed tab) frees itself instead of needing an
admin.

- **Auto-acquire on load** — `POST /domain/load-from-uc` acquires the lock for a loaded
  DRAFT version and returns a `lock` block for an immediate toast. Opening a *different*
  domain closes the previous one first (releases its lock **before** the new one loads,
  so you never hold two DRAFT locks); a same-domain version switch releases the old
  version's lock after the new one loads.
- **Background renew + expiry UX** — while editing, `edit-lock.js` pings
  `POST /domain/edit-lock/renew` on a timer. If a renew reports the lease was lost
  (`renewed === false`), the page flips to read-only and shows a **"your editing session
  expired — read-only, saving disabled"** banner with a **Reload** button; transient
  network blips are ignored (the next tick retries).
- **Lease countdown tooltip** — the navbar domain badge (`#currentDomainName`) shows a
  hover tooltip with a **live `M:SS` countdown** of the remaining lease — only while this
  browser is the editor and a lease is configured. Frontend-only, reusing the Bootstrap
  `Tooltip`.
- **Explicit close** — the L2 subnav **Save** button (renamed from *Save Domain*) now sits
  beside a **Close** button. Close prompts *"Save before closing?"* (Save & Close / Close
  without saving / Cancel), then `POST /domain/close` releases the lock and resets the
  session, returning to the Home page. Any user (viewers included) can close a domain they
  have open.
- **Switch version** — a **Switch** button between **Save** and **Close** opens a popup that
  reloads the current domain from the Registry or loads another of its versions (listed from
  `GET /domain/versions-list`, current version flagged), reusing `POST /domain/load-from-uc`.
  A "Save my changes before switching" box (ticked by default) persists edits first; unticking
  discards them.
- **Admin take-over** — an app-admin viewing a locked version gets a **Take over editing**
  button (`POST /domain/edit-lock/acquire` with `force`); the evicted editor is read-only
  on its next page load.
- **Admin Settings → Locks panel** — a registry-wide overview of every active edit-lock
  (domain, version, lifecycle status, holder, acquired time, `is_stale`) with a
  **force-unlock** action (`GET /settings/locks`, `POST /settings/locks/release`;
  admin-gated).
- **Authoritative server enforcement** — `PermissionMiddleware` blocks a non-holder's
  mutating request on a DRAFT version with a 403 ("being edited by …"); a **stale** lease
  is treated as *not blocking*, so an abandoned lock never locks out a new editor. Admins
  are *not* auto-exempt (they must take over first). Defence-in-depth re-check in
  `Domain.save_domain_to_uc`.
- **UI re-uses the read-only gating** — a new `body.read-only-locked` class is folded into
  the existing read-only CSS selector group, so every write surface locks down with no
  per-control changes; `edit-lock.js` renders the read-only banner.
- **Lakebase schema** — table `domain_edit_locks` (keyed by `(domain_id, version)`,
  `ON DELETE CASCADE`), provisioned as schema owner by `bootstrap-lakebase-perms.sh` and
  lazily self-healed by the store (`_ensure_domain_edit_locks_table`). `heartbeat_at` is
  the lease clock; `acquire_edit_lock`'s `ON CONFLICT` reclaims a stale lease via
  `make_interval`. No migration required.
- **Code organisation** — the edit-lock service lives in the
  `back/objects/registry/lockmgt` subpackage (`from back.objects.registry.lockmgt import
  EditLockService`); persistence stays on `LakebaseRegistryStore`.

### 11. In-app Registry Permission Grants (no `psql` required)

Lakebase registry-schema permissions can now be applied directly from the UI, removing the
`psql`-dependent manual `scripts/bootstrap-lakebase-perms.sh` step for the common case.

- **Self-apply on Initialize** — Settings → Initialize now applies the project `CAN_USE`,
  schema `USAGE`/`CREATE`/`DML`, and UC `ALL_PRIVILEGES` grants to the app + MCP service
  principals, returning a `permissions` summary (best-effort, never fatal to Initialize).
- **Repair permissions button** — an admin-gated button in the Lakebase Connection panel
  re-applies the grants on demand (`POST /settings/registry/grant-permissions`) and renders
  a per-grant results panel with any warnings.
- **Shared grant primitives** — extracted into `back/core/databricks/lakebase_grants.py`
  and reused by both the graph-DB provisioner and the registry store (no duplication).

### 12. Deploy-script dependency preflight

`scripts/deploy.sh` now checks external tooling **up front**: hard dependencies
(`databricks`, `python3`) abort immediately, while soft dependencies (`psql`, only when a
Lakebase-target run will reach the GRANT bootstrap) surface a warning and an interactive
"continue anyway?" prompt — instead of failing deep into the deploy.

### 13. Ontology & Mapping Audit Trail (who / what / when)

Every ontology and mapping change is now traced: **who** changed it, **what** changed,
and **when**. Fine-grained edits (class/property/mapping add/update/remove, SHACL/SWRL/
group edits, imports, resets) are buffered in the working session as they happen and
flushed to the registry in one batch on **Save to registry**, so the trail survives the
save round-trip while adding no per-edit database chatter.

- **Unified timeline** — the Domain → Audit trail page gains a third interleaved stream
  ("Ontology & mapping") alongside status/comments and build runs, with a filter toggle
  and version dropdown. Each entry shows the action, affected entity, actor and real edit
  time; AI-assistant edits carry an **AI** badge.
- **AI-assistant edits tagged** — changes applied by the Ontology Assistant are recorded
  with `source=agent`, distinguishing them from human edits.
- **Lakebase schema** — new append-only `domain_change_events` table
  (`source`, `action`, `entity_type`, `entity_ref`, `summary`, `meta`, `occurred_at`,
  `created_at`), folded into the consolidated `scripts/upgrade_lakebase_0.5_To_0.6.sql`
  migration (new installs self-heal).

---

## Bug Fixes

### Domain & Registry

- **last_build reset on domain switch** — `_record_build_run` now calls
  `stamp_last_build` on every successful UI/API build, writing directly to
  `domain_versions.last_build` in Lakebase so the "Last Built" indicator survives domain
  reloads.
- **Base URI persistence** — `read_version` now reads the canonical `domains.base_uri`
  column; `write_version` uses a `CASE WHEN` guard so an empty incoming value never
  overwrites a stored one; empty-string `base_uri` now triggers the `DEFAULT_BASE_URI`
  fallback consistently across the session, OWL generator, R2RML generator, and external
  API.

### Mapping & R2RML

- **Column names with spaces / special characters** — `R2RMLGenerator` now
  unconditionally double-quotes every `rr:column` and `rr:template` column reference per
  R2RML spec §7.4; `R2RMLParser` strips these quotes on read-back. The auto-mapping agent
  always backtick-quotes column names in SQL and never submits quoted names as mapping
  keys (backtick stripping in `tool_submit_entity_mapping`).
- **Auto-map overwrites attribute exclusions** — `tool_submit_entity_mapping` / `_relationship`
  preserve `excluded_attributes` from the existing mapping; `tool_get_ontology` hides
  excluded attributes from the agent entirely.

### Knowledge Graph

- **Lakeflow silently skipped on cold start** — `TripleStoreFactory._resolve_graph_engine`
  and `_resolve_graph_engine_config` now accept `force=True`; the build pipeline and the
  sync start endpoint always pass `force=True` so the `managed_synced` mode is read from
  Lakebase even immediately after a cold start.
- **Explorer blocked on PUBLISHED/IN-REVIEW domains** — `_STATUS_GATE_EDIT_PATHS` was
  narrowed from the broad `/dtwin/sync/` prefix to `/dtwin/sync/start` and
  `/dtwin/sync/load` only; read-only POSTs (`/dtwin/sync/filter` etc.) are now accessible
  regardless of lifecycle status.

### Ontology Designer

- **Relationships in opposite directions overlapping** — canonical-direction vector
  normalization (smaller entity ID → larger) ensures opposite-direction links offset to
  opposite sides; Bezier midpoint placement corrected to `t=0.5` (offset × 0.5, not × 1).
  Affects the Ontology Designer, Mapping Designer, and Business Views (OntoViz).
- **OWL export uses domain name as namespace prefix** — the default namespace is now bound
  to a clean lowercase prefix derived from the ontology name, instead of the empty string.

### UI & Deployment

- **Settings → Lakebase Connection tab dropdowns not pre-selected** — `SettingsService`
  now overlays env-var fallbacks (`LAKEBASE_PROJECT`, `LAKEBASE_BRANCH`,
  `PGDATABASE`/`LAKEBASE_DATABASE`) for empty fields before returning the config.
- **Lakebase Bulk loading tab — UC catalog not pre-selected** — `prefillLakebaseConnectionFromConfig`
  extended to also populate `lakebaseUcCatalog`; engine change handler triggers
  `loadUcCatalogsForGraphEngine` when saved mode is `managed_synced`.
- **App startup crash — `Any` not imported in `UnityCatalog.py`** — import fixed.
- **Runs page JS crash on dtwin page** — `runDetailsModal` and `domain-runs.js` added to
  `dtwin.html`; null guard added in `showRunDetailsObj`.
- **Knowledge Graph Explorer context-menu hover invisible** — hover rule corrected to
  blue-tint (`#e8f4fd` / `#1a73e8`) in `query-sigmagraph.css`.
- **deploy.sh — `lakebase_database_resource_segment` empty in DAB command** — the resolved
  `db-…` segment is now patched back into `_dab_var_overrides` before the bundle deploy.
- **Collaborative comments — MCP 502/503 retry** — `_get` and `_post` in the MCP server
  retry up to 3 times (5 s → 10 s → 20 s back-off) on 502/503 cold-start responses.

---

## Upgrade Notes

### New deploys (v0.6.0 from scratch)

No special action required beyond the standard `make bootstrap-lakebase` which creates all
required tables including `domain_comments`, `domain_tasks`, `domain_edit_locks`, and
`domain_change_events`. Registry-schema grants can also be applied straight from
Settings → Initialize (no `psql`).

### Upgrading from v0.5.x

Run the single consolidated migration script as schema owner. It folds in every
0.6-cycle registry addition (collaboration, graph analytics, change audit):

```bash
psql -v reg_schema=ontobricks_registry -f scripts/upgrade_lakebase_0.5_To_0.6.sql
```

The script creates `domain_comments`, `domain_tasks`, `graph_analytics`,
`graph_analytics_runs`, and `domain_change_events` (with their indexes + CHECK
constraints), drops the dead comment-anchor columns if present, and patches the graph
schema's `*_sync` / `*__app` tables with `datatype` / `lang`. It is fully idempotent and
safe to re-run. Every table is also lazily self-healed by the app on first use
(`_ensure_collab_tables`, `_ensure_graph_analytics_table`, `_ensure_change_events_table`),
so running the script is optional if you prefer the in-app path.

The `domain_edit_locks` table is now provisioned **as schema owner** by
`scripts/bootstrap-lakebase-perms.sh` (run by `make deploy`) and is also listed in the
consolidated `upgrade_lakebase_0.5_To_0.6.sql`. Owner-provisioning is required because the
app service principal lacks the `REFERENCES` privilege the table's FK needs, so the app's
lazy `_ensure_domain_edit_locks_table` self-heal cannot create it on its own. The in-app
grant flow (Initialize / Repair permissions) covers its DML grants automatically. The
2026-07-02 **renew-only lease** needs **no migration** — it reuses the existing
`heartbeat_at` column as the lease clock — and is tuned purely via the
`ONTOBRICKS_EDIT_LOCK_TTL_S` environment variable (default 600 s).

No other schema changes. No data migrations required.

---

## Changes Summary

| Area | Files changed | Change type |
|------|---------------|-------------|
| Collaboration | `registry/CommentService.py`, `registry/store/lakebase/store.py`, `api/routers/internal/comments.py`, `scripts/upgrade_lakebase_0.5_To_0.6.sql` | New feature |
| Graph Analytics | `back/core/graph_analysis/GraphMetrics.py`, `api/routers/internal/dtwin.py`, `partials/dtwin/_query_analytics.html` | New feature |
| AI Graph Interpreter | `agents/agent_graph_interpreter/`, `back/objects/digitaltwin/DigitalTwin.py` | New feature |
| Mapping attr exclusion | `front/static/mapping/js/mapping-design.js`, `back/objects/mapping/Mapping.py`, `agents/tools/mapping.py` | New feature |
| OWL append import | `back/core/w3c/owl/OntologyConflictDetector.py`, `back/objects/ontology/Ontology.py`, `api/routers/internal/ontology.py` | New feature |
| SKOS/SHACL/Alignment import | `back/core/w3c/rdfs/RDFSParser.py` | New feature |
| Ontology Designer | `front/static/ontology/js/ontology-map.js`, `ontology-map.css`, `_ontology_map.html` | New feature |
| App Logs viewer | `api/routers/internal/settings.py`, `front/static/config/js/settings-logs.js` | New feature |
| Registry access check | `back/core/databricks/UnityCatalog.py`, `back/objects/domain/SettingsService.py` | New feature |
| Sidebar user panel | `front/templates/partials/layout/_sidebar_nav.html`, `css/sidebar-layout.css` | Enhancement |
| R2RML column quoting | `back/core/w3c/r2rml/R2RMLGenerator.py`, `R2RMLParser.py` | Fix |
| Base URI persistence | `registry/store/lakebase/store.py`, `session/DomainSession.py` | Fix |
| last_build persistence | `registry/store/base.py`, `digitaltwin/_build_pipeline.py` | Fix |
| Lakeflow cold-start | `back/core/triplestore/TripleStoreFactory.py`, `digitaltwin/_build_pipeline.py` | Fix |
| Status-gate Explorer fix | `src/shared/fastapi/main.py` | Fix |
| Designer overlap fix | `ontology-map.js`, `mapping-design.js`, `ontoviz.js` | Fix |
| Makefile test isolation | `Makefile` | Infrastructure |
| Nightly e2e auto-skip in routine runs | `tests/e2e/conftest.py`, `.cursor/08-testing-and-deployment.mdc` | Infrastructure |
| Domain edit-lock | `registry/lockmgt/EditLockService.py`, `registry/store/lakebase/store.py`, `registry/store/lakebase/schema.sql`, `api/routers/internal/domain.py`, `api/routers/internal/home.py`, `src/shared/fastapi/main.py`, `objects/domain/Domain.py`, `front/static/global/js/edit-lock.js`, `permissions.css`, `permissions.js`, `base.html` | New feature |
| Release-based edit-lock + Close/Switch button | `registry/lockmgt/EditLockService.py`, `registry/store/lakebase/store.py`, `api/routers/internal/domain.py`, `src/shared/fastapi/main.py`, `objects/domain/Domain.py`, `front/static/global/js/edit-lock.js`, `navbar.js`, `base-ui-handlers.js`, `main.css`, `menu_config.json`, `base.html` | Enhancement |
| Renew-only edit-lock lease + countdown tooltip | `registry/lockmgt/EditLockService.py`, `registry/store/lakebase/store.py`, `api/routers/internal/domain.py`, `front/static/global/js/edit-lock.js` | Enhancement |
| Admin Settings › Locks panel | `registry/lockmgt/EditLockService.py`, `registry/store/lakebase/store.py`, `api/routers/internal/settings.py`, `partials/settings/_settings_locks.html`, `config/js/settings-locks.js`, `menu_config.json`, `settings.html` | New feature |
| Edit-lock service → `lockmgt` subpackage | `registry/lockmgt/__init__.py`, `registry/lockmgt/EditLockService.py` (moved), import sites (`domain.py`, `settings.py`, `home.py`, `main.py`), `docs/sphinx/api/app.objects.registry.rst` | Refactor |
| In-app registry grants | `back/core/databricks/lakebase_grants.py`, `registry/store/lakebase/store.py`, `objects/domain/SettingsService.py`, `api/routers/internal/settings.py`, `partials/registry/_registry_configuration.html`, `registry/js/registry.js`, `graphdb/lakebase/provisioner.py` | New feature |
| Live scenario campaign | `Makefile`, `tests/e2e/scenarios/_harness.py`, `conftest.py`, `README.md`, `test_scenario_0{1,2,3}_*.py`, `test_scenario_validation.py`, `pyproject.toml` | Infrastructure |
| Deploy dependency preflight + `uv.lock` public-CDN re-pin | `scripts/deploy.sh`, `uv.lock` | Enhancement / Fix |
| Version | `pyproject.toml` | Bumped to `0.6.0` |

---

## What is NOT changed

- External API contract — all `/api/v1/` endpoints are backward-compatible.
- MCP tool contracts — no MCP tool signatures changed.
- Existing v0.5.x registry tables — no columns dropped or modified.
- R2RML semantic content — column quoting only affects serialization; all triples and
  mappings are logically identical.
- Any Databricks workspace configuration (warehouses, catalogs, volumes, Lakebase
  settings).
