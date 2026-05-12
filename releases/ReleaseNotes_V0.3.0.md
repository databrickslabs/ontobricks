# OntoBricks — Release Notes V0.3.0

**Release window:** May, 2026
**Test status:** all changes shipped with the suite green (2045 passing, 80 CloudFetch probe tests conditionally skipped in CI).

---

## Highlights

- **Cohort Discovery** — new end-to-end feature for business-friendly entity grouping: rule-based linkage (shared resources via predicates), compatibility constraints, a 6-stage deterministic engine, full Volume + Lakebase persistence, graph-triple + Unity Catalog Delta materialisation, and a natural-language Stage 2 agent that translates free-text prompts into validated `CohortRule` JSON.
- **Live Digital Twin build log** — the Build page now shows a real-time per-step log panel with elapsed timers, phase descriptions, a one-click export to `.log`, and honest background-archive handling. `TaskManager` gains `skip_step` / `complete_current_step` so skipped phases are labelled correctly.
- **Real `/health` readiness probe** — 11 checks covering filesystem, Databricks auth, SQL warehouse, registry Volume read/write, registry UC DDL permissions, Lakebase schema/table/sequence grants, and CloudFetch capability. `/health/detailed` retired. New admin **Health** tab in Settings surfaces the same payload in-app.
- **Mapping diagnostics: source table permissions** — new third section runs a non-destructive `SELECT … LIMIT 0` against every Unity Catalog table referenced by the mapping and reports `ok` / `PERMISSION_DENIED` / `TABLE_OR_VIEW_NOT_FOUND` per table, surfacing missing grants before a build attempt.
- **Knowledge Graph — right-click Expand neighbours** — any node can be expanded N hops in place without re-running a full SPARQL query. Non-blocking spinner, depth picker, camera zoom + highlight on new nodes. KG preview/expand also hardened against timeouts and 502 errors on large graphs.
- **Deployment: single source of truth** — `scripts/deploy.config.sh` + `app.yaml.template` replace scattered literals across `Makefile`, `deploy.sh`, and bootstrap scripts. `app.yaml` is now a generated artifact. App name `ontobricks-030` unified across all tooling.
- **CloudFetch — runtime capability probe** — `DatabricksAuth` detects at runtime whether the Apps sandbox can actually reach the CloudFetch storage host, sets `use_cloud_fetch` accordingly, and exposes the verdict in `/health`. A new Settings → Global toggle lets admins override the default.
- **Task duration & UTC timestamps** — `TaskManager` emits `START task` / `END task` log lines with compact durations, serialises `duration_seconds` in `to_dict()`, and uses UTC-aware ISO timestamps throughout to prevent timezone-drift bugs in the browser.

---

## Cohort Discovery

### Stage 1 — deterministic engine, UI, persistence, materialisation

- New `CohortRule` model with `validate()`, CRUD endpoints, dry-run + materialise, and a 6-stage pure-Python engine (`CohortBuilder`) that works against both Delta/Spark SQL and LadybugDB/Cypher backends.
- Materialise to graph (idempotent `DELETE` then `INSERT`, per-rule `:inCohort<RuleId>` predicate) and to Unity Catalog Delta (partitioned by `rule_id`, chunked INSERT).
- Content-hash cohort URIs (`<base>/cohort/<rule_id>/c-sha256(…)[:8]`).
- Live preview helpers: class stats, edge count, node count, sample property values, `explain_membership` (Why? / Why not?).
- 59 new tests across `test_cohort_models.py`, `test_cohort_builder.py`, `test_dtwin_cohort.py`.
- New `docs/cohort_discovery.md` with mental model, UX walkthrough, 4 worked examples, API summary.

### Stage 2 — NL agent for rule generation

- `agents/agent_cohort/` — six read-only tools (`list_classes`, `list_properties_of`, `count_class_members`, `sample_values_of`, `propose_rule`, `dry_run`) wired to Stage 1 endpoints.
- One-shot agent loop with 10-iteration cap; never writes — saving still goes through the Builder-protected endpoint.
- UI: **Describe** tab (NL prompt + agent trace with tool calls, durations, iterations) auto-switches to **Build rule** tab when a rule is proposed.
- Fix: `list_properties_of` was returning empty arrays when `rdfs:domain` was stored as a local name or property URIs were missing — resolved with `_domain_matches` + `_ensure_uri` helpers and a fallback to the full object-property list.
- 22 new tests across `test_agent_cohort_tools.py` (19) and `test_agent_cohort_engine.py` (3).

### Cohort designer — UX refinements

- **Three-tab layout** (Describe / Build rule / Preview) replaces the full-width drawer; the Preview tab carries a live cohort-count badge.
- **Saved rules pane** promoted to a persistent right/left rail, always reachable regardless of active tab.
- **Dependent dropdowns** in the "Link members" section: `via` property narrows to predicates whose `domain` is the source class and `range` is the chosen shared class; falls back to the full list when ontology metadata is incomplete.
- **camelCase rule name enforcement**: live input sanitisation, paste-to-camelCase, `_isValidRuleName` validator, `_toCamelCase` helper applied to agent-generated names. `id = label` (no more slug fork).
- **Rule-scoped predicate and UC table**: membership triples use `:inCohort<RuleId>`; Auto-pick proposes `cohorts_<snake_rule_name>`. Both the graph-triples hint and the UC-table hint in the Configure-outputs modal now show the *actual* predicate / table name for the active rule.
- **Clickable entity badge** in the Preview pane: each cohort member renders as a pill that links to the Sigma graph focused on that node. URI is demoted to inline parenthetical muted text.
- **Configure-outputs modal — visible feedback**: `Auto-pick` and `Test write access` were silently swallowing errors. Both are now four-state (`idle → working → success | error`) with inline status lines, spinner, toast, and error-envelope surfacing.
- Clearer step labels in the designer: "Link members via a shared entity" (was "When are two members linked?"), "Conditions every member must satisfy" (was "Compatibility policies"). Terminology switched from "class" to "entity" throughout user-facing strings.
- **Cohort explain fix** — namespace drift: `CohortBuilder._members_of_class` now checks all URI variants (ontology form, data form, raw) so `explain_membership` works regardless of which normalisation path the loader used. Enhanced "not in class" diagnostics report *typed-as*, *untyped*, or *URI not found* with actionable advice.
- Rule summary card: removed the redundant `rule_id` chip (equal to `label` for camelCase names); fixed binary UC/graph output display to enumerate all four states (graph + UC / graph only / UC only / no outputs).

---

## Digital Twin Build

- **Live build log panel** (`#syncBuildLogCard`): appears on Build click, grows row-by-row with icon, description, live sub-message, and per-step elapsed timer. Step labels rewritten to plain English ("Preparing mappings…", "Detecting what changed since last build", etc.).
- **`TaskManager.skip_step()`** marks a step `skipped` and advances `current_step` so the label array stays aligned with execution when phases are conditionally bypassed (e.g. *Detecting what changed* on first build, *Checking source tables* on forced full rebuild).
- **Export build log**: one-click export to `digital-twin-build_<timestamp>.log` (plain text with a header block, per-step table, and result block). Button enabled from the first poll onwards.
- **Fast gzip + background archive**: `GraphSyncService.sync_to_volume` now uses `compresslevel=1` (≈ 6–10× faster than the previous level 9). For session builds the Volume upload runs in a daemon thread; the build task completes immediately after the snapshot step with the note *"Registry backup continues in the background."*
- **Archive checkbox**: new "Archive graph to registry" checkbox on the Build page (default on); when off, the archive step is marked skipped with a plain-English reason.
- **Honest timing**: archive row shows "Continues after build" with a tooltip instead of a misleading 0ms duration when the upload is backgrounded.
- Background archive task tracked as a separate `registry_archive` `TaskManager` task with its own navbar hourglass entry.

---

## Task Manager & Notifications

- `Task.duration_seconds()` — computed live for running/pending, frozen at `completed_at − started_at` for terminal tasks; serialised in `to_dict()`.
- `TaskManager` lifecycle log lines unified to `START task <id> [<type>] — <name>` / `END task <id> [<type>] completed|failed|cancelled in <duration>`.
- `_format_duration` produces compact strings: `450ms`, `2.40s`, `1m 23.5s`, `1h 5m`.
- Navbar hourglass: running rows show a live 1 s ticking elapsed time; bell toasts append `(in 1m 23s)`.
- All task/step timestamps emitted in UTC-aware ISO format (`+00:00`); `duration_seconds()` tolerates mixed naive/aware legacy timestamps.
- Removed the broken "Open" link from terminal task completion toasts.

---

## Health & Observability

### `/health` readiness probe (replaces static `{"status":"healthy"}`)

11 probes, each timed and wrapped in `_safely_run` so one failure never breaks the overall response:

| Probe | What it checks |
|---|---|
| `runtime` | Python + OntoBricks version |
| `filesystem.tmp` | Write sentinel + `shutil.disk_usage` thresholds (warn < 1 GB, error < 100 MB) |
| `filesystem.session_dir` | Same check against the session directory |
| `filesystem.log_dir` | Same check against the log directory |
| `databricks.auth` | `has_valid_auth` + OAuth token mint in App mode |
| `databricks.warehouse` | `SELECT 1` against the configured warehouse |
| `databricks.cloudfetch` | Real SQL probe with `use_cloud_fetch=True`; cached 5 min |
| `registry.cfg` | Resolved catalog / schema / volume |
| `registry.volume_read` | `VolumeFileService.list_directory` on the registry volume |
| `registry.volume_write` | Write + delete of a sentinel file via the Files API |
| `registry.uc_schema_ddl` | `CREATE OR REPLACE VIEW … AS SELECT 1` + `DROP VIEW` |
| `lakebase` | `init_status()` with fine-grained warning/error mapping |
| `lakebase.permissions` | Schema `USAGE`/`CREATE`, table DML, sequence grants |

- `/health/detailed` retired; its information is included in the unified response.
- HTTP 200 is intentional even when `status: error` — load-balancers should inspect `summary.errors`.

### Settings → Health tab

- Admin-only tab calls `GET /health` on open (lazy-load) and on **Refresh**.
- KPI tiles (Total / Passed / Warnings / Errors) + sortable table (errors → warnings → ok) with status icon, label, name, detail (long details collapse into `<details>`), and per-check duration badge.

---

## Mapping Diagnostics — source table permissions

- New third accordion section **Source table permissions** probes every distinct UC table referenced by the mapping with `SELECT * FROM … LIMIT 0`.
- Classifies warehouse errors as `PERMISSION_DENIED`, `TABLE_OR_VIEW_NOT_FOUND`, `SCHEMA_NOT_FOUND`, `CATALOG_NOT_FOUND`, or "Probe failed".
- When no warehouse client is available (local dev without PAT), returns a `warning` advisory row so the section explains the gap rather than appearing broken.
- Global KPI tiles at the top of Mapping Diagnostics now include permission check results.
- 23 new tests in `test_mapping_service.py`.

---

## Knowledge Graph

- **Right-click Expand neighbours**: new context-menu entry on any non-cluster node. Fetches the N-hop induced subgraph via `GET /dtwin/neighbors`, de-duplicates against existing results, highlights newly-added nodes for 6 s, and frames the camera around them. Default depth reads the current slider value.
- **Non-blocking expand spinner** (`#sgExpandSpinner`): replaces the blocking "Expanding…" toast; positioned top-right over the canvas with `pointer-events: none`.
- **Depth slider default** lowered from 3 to 2.
- **KG preview / expand hardening**:
  - `_fetchWithTimeout` with `AbortController` applied to preview and expand requests — users see a clear timeout message instead of an indefinite spinner.
  - `find_seed_subjects` now accepts an optional `limit` so the API requests at most `max_preview + 1` subjects, avoiding unbounded scans.
  - In Apps mode: expansion depth capped to 3, max entities capped to 3000, triple fetch batched (250 subjects/batch), 40 s time budget returning partial results rather than failing.
  - 502 error path now shows an actionable message ("reduce Depth / Max entities and retry").
  - `_parseJsonResponse` safely handles non-JSON upstream errors that previously caused `Unexpected token 'u'`.

---

## Deployment & Infrastructure

### Single source of truth

- **`scripts/deploy.config.sh`** (new): one file for app names, DAB target, warehouse ID, registry coords, Lakebase project/branch/database, and `app.yaml` runtime fallbacks. Four sections; `DEFAULT_*` block at the top so changing a default is a one-line edit.
- **`app.yaml.template`** (new): `app.yaml` is now generated by `scripts/_render-app-yaml.py` from the template and config. `app.yaml` added to `.gitignore`.
- `deploy.sh` sources `deploy.config.sh` before doing anything; prints a config summary before validation; accepts `--skip-app-yaml` and `--no-bootstrap`.
- `Makefile` slimmed: removed hard-coded `APP_NAME` variable; `deploy*` targets just call `deploy.sh`; new `make render-app-yaml` target.
- `databricks.yml` app name unified to `ontobricks-030` (was `ontobricks-020` in the DAB resource but `ontobricks-030` in the Makefile — now consistent everywhere).
- `bootstrap-app-permissions.sh` and `bootstrap-lakebase-perms.sh` read `APP_NAME` / `MCP_APP_NAME` from env with documented override patterns; `bootstrap-app-permissions.sh` automatically grants `CAN_USE` from the MCP SP to the main app.
- `bootstrap-lakebase-perms.sh` gains `--branch` support for branch-accurate grant targeting.
- Post-deploy resource-binding verification for both apps (warehouse, volume, postgres branch/database on lakebase targets).

### Dependency / startup hardening

- `requirements.txt` reduced to `uv` + `pydantic-settings>=2.1.0` bootstrap + `-e ".[lakebase]"` editable install so `pyproject.toml` remains the single source of Python dependencies.
- `docs/` and `*.md` excluded from Databricks bundle sync via `.databricksignore` and `databricks.yml` sync exclusions.
- Credential helpers (`get_databricks_client`, `get_databricks_host_and_token`, `resolve_warehouse_id`) now accept `domain=None` via `_domain_databricks()` helper — fixes `/health` warehouse probe crash.
- 4 new regression tests in `test_helpers.py` for the `domain=None` safety contract.

### Scripts cleanup

- Deleted orphaned `scripts/run_tests.sh` and `scripts/test_UI.sh` (superseded by `make test`).
- Deleted stale `tests/reports/` artifacts (2026-02-25 snapshots when the suite was 306 tests).

---

## CloudFetch

- `DatabricksAuth` initially disabled CloudFetch unconditionally in Apps mode to fix a `Connection refused` crash on the `*.storage.cloud.databricks.com` storage host.
- Replaced with a **runtime capability probe** (`probe_cloud_fetch_capability()`) that issues a bounded `SELECT 1` with `use_cloud_fetch=True` and caches the `(capable, reason)` outcome for ~5 minutes. `can_use_cloud_fetch()` reads the cached verdict; `get_sql_connection_params()` sets the flag accordingly.
- `cloud_fetch_status(force=False)` for diagnostic callers.
- `/health` probe `databricks.cloudfetch` calls the real probe and surfaces the actual outcome.
- New **Settings → Global → "Enable CloudFetch for SQL queries"** toggle persisted via `GlobalConfigService`; propagated to `DatabricksClient` and `DatabricksAuth` through a new `resolve_use_cloud_fetch` helper.
- `DATABRICKS_DISABLE_CLOUD_FETCH` / `DATABRICKS_FORCE_CLOUD_FETCH` env vars for CI isolation.

---

## Lakebase connection resilience

- **TCP keepalives** added to `LakebaseAuth.kwargs()` / `conninfo()`: `keepalives_idle=10`, `keepalives_interval=5`, `keepalives_count=3`. Zombie sockets now surface in ~25 s instead of ~130 s.
- **Stale cache on Lakebase failure**: `GlobalConfigService.load()` now returns the previously-cached config (up to 30-minute stale window) instead of falling back to `_empty()` on a blip. `_CACHE_TTL` bumped from 60 s → 300 s.
- Cohort `preview()` wrapped in a 60 s `AbortController` budget with a clear "Preview timed out" message.

---

## Documentation

- **`docs/cohort_discovery.md`** (new) — mental model, 5-section UX walkthrough, output destinations, persistence in both registry backends, 6-stage algorithm summary, 4 worked examples, API summary, Stage 2 walkthrough.
- **`docs/deployment.md`** — updated for `deploy.config.sh`, `app.yaml.template`, Lakebase `--branch` bootstrap, and the real `/health` probe payload.
- **`docs/development.md`** — `TestHealthRoutes` row updated; `/health/detailed` retirement noted.
- **`docs/user-guide.md`** — "Right-click on a node (Expand neighbours)" section added.
- **`README.md`** — new bullet for cohort discovery + right-click expand.

---

## Upgrade notes

- **App name**: the dev sandbox is now **`ontobricks-030`** in all tooling (`databricks.yml`, `deploy.config.sh`, `scripts/bootstrap-*.sh`, `Makefile`). Update any scripts or docs that still reference `ontobricks-020`.
- **`app.yaml` is generated**: do not edit it directly — it is gitignored. Edit `app.yaml.template` and run `make render-app-yaml` (or `make deploy`, which runs the renderer automatically). Commit `app.yaml.template` and `scripts/deploy.config.sh` instead.
- **`deploy.config.sh` required**: `scripts/deploy.sh` aborts if the config file is missing. Copy `scripts/deploy.config.sh.example` and fill in your workspace values before the first deploy on a new checkout.
- **Cohort predicate migration**: graphs that hold the old global `<base>/inCohort` membership triples (v0.3.0-dev only — the feature was never deployed externally) will not be cleaned up by a re-materialise with the new rule-scoped predicate. Run `DELETE FROM … WHERE predicate = '<base>/inCohort'` manually before re-running Materialise.
- **`/health/detailed` removed**: external probes that hit `/health/detailed` should switch to `GET /health` and inspect `status` + `summary.errors`. The route returns `404`.
- **`make test`**: `scripts/run_tests.sh` and `scripts/test_UI.sh` are deleted — use `make test` or `uv run pytest` directly.
- **Lakebase bootstrap**: `bootstrap-lakebase-perms.sh` now accepts `--branch <branch>` to target the correct Lakebase branch endpoint. Pass `LAKEBASE_BOOTSTRAP_BRANCH` via env or let `deploy.sh` handle it automatically.
