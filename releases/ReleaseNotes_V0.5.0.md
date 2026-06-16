# OntoBricks — Release Notes V0.5.0

**Release window:** May–June, 2026
**Test status:** all changes shipped with the suite green (≥ 2532 passing on the unit/integration tiers; the full multi-tier run — unit + integration + property + live + e2e — reached ≥ 2660 passing).

---

## Highlights

- **Domain version lifecycle (`DRAFT → IN-REVIEW → PUBLISHED`)**: a proper, server-enforced state machine replaces the old single "Active" (`mcp_enabled`) toggle. Status gates editability (only DRAFT is editable, *any* DRAFT version, not just the latest), gates API/MCP data access (only PUBLISHED is served, numeric-latest wins), and is surfaced as a colour-coded badge everywhere a domain + version appears.
- **Ontology Validation & Review workflow**: a new Domain → **Validation** workspace and a cross-domain **My Tasks** worklist (Registry + Home) let business users run soft consistency checks, sign off through a per-domain **quorum**-gated review, and keep a full audit trail. Every lifecycle decision is persisted with actor, transition, and comment; admins can override the quorum and drive any transition.
- **Graph Chat streaming (SSE)**: the agent loop now streams tool calls and the final reply token-by-token instead of blocking until the whole turn finishes — no more static "Thinking…" placeholder.
- **Binary-document ingestion for ontology generation**: PDFs, Office docs, and images uploaded for OWL / business-rules generation are now converted to markdown on the fly via Databricks `ai_parse_document`, exposed as a reusable core `DocumentExtractor`.
- **Ontology Pitfalls as a first-class agent tool**: the OWL generator now owns the validate → fix → re-validate loop through a `check_owl_pitfalls` tool and iterates to a 100 %-clean ontology by default; the precision score was corrected so minor warnings actually move the needle.
- **Business Views overhaul**: a guided **New Assistant** (build a view from seed entities + their ontology neighbours with a 1–3 hop control), collapse / expand entities, right-click "Hide from view", delete-the-last-view support, and an icon-only toolbar.
- **Build-run tracing & Build Analytics**: an append-only `build_runs` registry table records one immutable row per build (UI / API / scheduler), surfaced through a Registry "Build Analytics" panel and a domain-wide **Audit trail**.
- **Graph / registry Lakebase separation**: the graph DB can now live in a different Lakebase project from the registry, via a new `BranchLakebaseAuth`, an in-app **Create Graph DB** provisioner (with auto-granted Postgres superuser for managers), and a Settings → **Permissions** tab.
- **CNS test foundations & quality engineering**: a comprehensive test strategy landed — coverage gates, test factories, in-process MCP integration tests, Hypothesis property tests, an LLM-agent eval harness + CI gate, ruff + mypy baselines, pre-commit hooks, and a changelog-presence gate. The suite grew from ~1900 to ≥ 2660 cases across multiple tiers.
- **Deploy simplified to a single knob**: the Lakebase deploy-config surface collapsed from 13 variables to its irreducible core; a second instance now needs only `DEFAULT_APP_NAME` changed. `deploy.sh` gained colourised step logging, an `ERR` trap, preflight + resource-existence checks, and a `--dry-run` mode.

---

## Audit Trail (Cross-Cutting Summary)

> Consolidated view of the audit-trail work shipped in 0.5.0. Full detail lives in the **Ontology Validation & Review Workflow** and **Build-Run Tracing & Analytics** sections below.

- **Review audit log** — new append-only `domain_review_events` table (actor, action, `from → to`, comment, meta, timestamp) written by `ReviewService` on every `submit / signoff / publish / reopen`, plus a chat-style "all comments" history viewer reachable from the worklists.
- **Audit trail viewer (Domain → Audit trail)** — a single domain-wide feed interleaving review events (with comments) and build runs, with **All / Status / Builds** filter pills and a version dropdown (defaults to the current version).
- **Build-run tracing (Runs)** — append-only `build_runs` table (one immutable row per UI / API / scheduler build; "active" = most recent successful run), surfaced as a per-domain **Runs** tab and a Registry → Automation **Build Analytics** panel.
- **Lifecycle attribution** — direct status-dropdown changes (`/domain/set-version-status`) also write an audit row tagged `meta.source="lifecycle"`; local-dev sign-offs are attributed via a cached SCIM `/Me` lookup so quorum counts correctly without the proxy header.
- **Schema provisioning** — `bootstrap-lakebase-perms.sh` / `make bootstrap-lakebase` create `domain_review_events` and `build_runs` (+ indexes) idempotently as the schema owner.

---

## Domain Version Lifecycle

- New per-version status `DRAFT → IN-REVIEW → PUBLISHED`, enforced server-side by a single source of truth (`registry/version_lifecycle.py`: `ALLOWED_TRANSITIONS`, `is_editable`, `check_status_transition`).
  - DRAFT → IN-REVIEW (admin/builder; precondition: version has been built; locks editing).
  - IN-REVIEW → DRAFT (admin/builder; re-enables editing).
  - IN-REVIEW → PUBLISHED (admin/builder).
  - PUBLISHED → DRAFT (admin only; reversible publish).
  - No direct DRAFT → PUBLISHED; new versions are always DRAFT.
- **Editability is now status-only**: any DRAFT version is fully editable (older DRAFT versions included); the previous "only the latest version is editable" frontend restriction was removed. `PermissionMiddleware` blocks mutating edits unless the session version is DRAFT (non-mutating compute / validate / generate stay open).
- **API/MCP serve PUBLISHED only**: `find_published_version` / `load_published_domain_data` (numeric-latest PUBLISHED, no fallback); the external `/api/v1/graphql` mount is strict PUBLISHED-only; `DigitalTwin.resolve_domain` rejects a non-PUBLISHED explicit version.
- `domain_versions.status` column (CHECK-constrained + indexed) with a lazy, owner-aware self-heal migration; the retired `mcp_enabled` toggle is left dormant.
- Colour-coded status badge wired across navbar, Registry → Browse, Domain → Versions, the Digital Twin / Ontology query headers, and the Load-Domain modal (which now shows `v<n> — Draft/In Review/Published` instead of "Latest / Read-Only").
- Digital Twin pages stay fully interactive on PUBLISHED/IN-REVIEW versions (read/analysis surface) — the read-only form gate excludes `body[data-page="digitaltwin"]`; real mutations remain server-gated.

## Ontology Validation & Review Workflow

- **[Audit]** New `domain_review_events` **append-only audit table** (actor, action, `from → to`, comment, meta, timestamp) and a stateless `ReviewService` orchestrator: `my_tasks`, `review_detail`, `submit`, `signoff`, `publish`, `reopen`. Approvals reset on resubmit / change-request / publish.
- New `/review` router: `GET /my-tasks`, `GET /{folder}/{version}`, and POST `submit | signoff | publish | reopen`, all resolving the caller's role against the *target* domain.
- **Domain → Validation** workspace: a visual lifecycle diagram (Draft → In Review → Published with the people involved at each stage), status banner with live quorum progress, a soft (advisory) consistency-check summary, header-mounted action buttons, and the audit timeline.
- **My Tasks** worklist on Registry → Review and on the **Home** page (revealed only when tasks exist), each handing off to the Validation workspace via a single **Validate** button rather than driving the workflow inline.
- **Per-domain sign-off quorum**: stored as a typed `review_quorum` column on `domains` (default 1), set at domain creation and editable on Domain → Information → Global. The old registry-wide `global_config.review_quorum` is no longer read.
- **Admin quorum override**: admins (app- or domain-level) can publish regardless of quorum; the override is recorded in the published event's meta and surfaced in the UI.
- **Review comments everywhere**: a shared `ReviewModals` helper adds a comment prompt to *every* status switch and a chat-style "all comments" history viewer reachable from the worklists.
- **[Audit] Domain → Audit trail (viewer)**: a single domain-wide feed interleaving review events (with comments) and build runs, with All / Status / Builds filter pills and a version dropdown (defaults to the current version).
- **[Audit] Attribution**: direct lifecycle dropdown changes (`/domain/set-version-status`) now also write an audit row tagged `meta.source="lifecycle"`; local-dev sign-offs are attributed via a cached SCIM `/Me` lookup so quorum counts correctly without the proxy header.

## Graph Chat — Streaming (SSE)

- `run_agent()` gained an `on_event` callback fired per `tool_call` / `tool_result` / final `output`; the legacy `on_step` string callback is preserved.
- New `POST /dtwin/assistant/chat/stream` SSE endpoint bridges the sync agent thread to an async generator (`asyncio.Queue` + `run_coroutine_threadsafe`), streaming `step` events then a final `done` event (reply, tool trace, usage, iterations); `error` events carry the exception message.
- Frontend renders a live streaming bubble (`createStreamingBubble` / `updateStreamingBubble` / `finalizeStreamingBubble` / `errorStreamingBubble`) consuming the `ReadableStream` and parsing SSE frames.

## Ontology Generation — Pitfalls Tool & Iteration Loop

- New `src/agents/tools/pitfalls.py`: `check_owl_pitfalls` agent tool returns `{score, is_clean, total_warnings, warnings[…], fix_instruction}`; the agent now owns the validate → fix → re-validate cycle in-loop (with a `max_fix_rounds` cap forcing final output when the budget is exhausted).
- Default convergence tightened: `score_threshold` 70 → **100**, `stop_on_no_critical` True → **False**, `max_fix_rounds` 3 → **5** — the loop now targets a zero-warning ontology.
- **Precision-score fix**: replaced the size-based penalty normalisation (which rounded any non-trivial ontology straight to 100 %) with a fixed `_MAX_OCCURRENCES_PER_PITFALL = 5` cap, so a single minor warning yields 99, not 100.
- Pitfalls UI: progress bar repositioned below the tab strip (visible across tabs), restyled to match the Data Quality run, and the iteration overlay now shows each warning's ID, title, affected elements, fix hint, and an explicit "Asking agent to fix these warnings…" notice.

## Binary-Document Ingestion (`ai_parse_document`)

- Uploaded PDFs / Office / images are converted to markdown at read time via the Databricks `ai_parse_document` SQL function (output schema pinned to v2.0), with a graceful fallback to the previous behaviour when no warehouse is available.
- Extraction logic was refactored (Fowler: Extract Class → Move Class) into a generic, reusable `back.core.databricks.DocumentExtractor` (agent-independent): `supports()`, `file_extension()`, `is_available()`, `extract()`, `extract_text_from_parsed()` (elements-first, page / markdown fallbacks), per-`ToolContext` caching.
- Warehouse id threaded through `run_agent` → `ToolContext` for the OWL-generator and business-rules agents; `/ontology/wizard/generate-async` and `/ontology/business-rules/generate-async` resolve it. Verified live (`ai_parse_document` v2.0 / Since 4.0.0).

## Business Views (Visual Ontology Designer)

- **New Assistant**: bootstraps a view from chosen seed entities plus their ontology neighbours (object-property + inheritance adjacency, BFS by 1–3 hops), drops would-be orphan neighbours, and starts collapsed. Frontend-only — reuses the existing per-view `visibility` plumbing.
- **Collapse / expand entities**: per-entity chevron toggle and a global collapse-all / expand-all; state persists per view via `visibility.collapsedEntities` and survives re-render (including after hiding).
- **Right-click "Hide from view"** for entities and relationships, available even in view-only mode (a business view is view-only); re-show from the palette.
- **Delete the last Business View** is now allowed (falls back to the empty state); fixed the related ontology-wipe-on-delete, stray dropdown entry, and view-scoped control gating.
- Toolbar tidy-up: New Assistant, Collapse-all, and View/Edit toggle are now icon-only with tooltips; entity header background set to light red; collapse toggle switched to a Bootstrap chevron (cache-busting `@import` fix).

## Build-Run Tracing & Analytics

- **[Audit]** New append-only `build_runs` registry table (FK to `domains`, keyed by `(domain_id, version)`): one immutable row per build across UI / API / scheduler paths; "active" build derived as the most recent successful run.
- `record_build_run` / `load_build_runs` / `build_analytics` on the store + `RegistryService` facade; wired into `_build_pipeline` (success / early-complete / error / cancel / phase-failure) and the scheduler.
- **[Audit]** `GET /settings/build-runs/{domain}` and `GET /settings/build-analytics/{domain}`; a Registry → Automation **Build Analytics** panel (domain + version selectors, summary cards, runs table with the active build highlighted) and a per-domain **Runs** tab.

## Graph DB / Registry Lakebase Separation

- **`BranchLakebaseAuth`** (new): a drop-in auth that resolves the Postgres host and mints JWTs for an explicit `projects/<proj>/branches/<branch>` resource path, so the graph DB can live in a *different* Lakebase project than the registry. `GraphDBFactory`, the Health probe, the Objects / schema / drop endpoints, and `health.py` all select it when `lakebase_branch` is configured (bound auth otherwise).
- **Create Graph DB provisioner**: provisions instance → database → schema → grants, with a DB-reachability poll to dodge the "database does not exist" control-plane race, and a "superusers" step that grants `DATABRICKS_SUPERUSER` to CAN_MANAGE / admin users (seeded by the authenticated operator's email, SCIM as best-effort supplement). The provisioner no longer auto-overwrites the saved active connection config.
- **Settings → Permissions tab**: list Postgres roles + app users on the graph branch and grant `DATABRICKS_SUPERUSER` on demand from a user dropdown; `LakebaseAuth` / `BranchLakebaseAuth` gained a `branch_path` property.
- **Objects tab** corrected to query the bound graph host/database (not provisioner metadata), to show schemas the SP has USAGE on (not just owns), and to filter out `__*` Databricks-internal schemas; **Health tab** relabelled to make the registry-vs-graph database distinction explicit.

## CNS Test Foundations & Quality Engineering

- **Test foundations (T-M0)**: `pytest` markers (`unit`, `integration`, `contract`, `e2e`, `eval`, `mcp`, `db`, `spark`, `external`, `property`, `live_integration`, …), coverage config + per-package thresholds (`scripts/check_coverage.py`, `ci/coverage_thresholds.yaml`), dataclass test factories (ontology / mapping / triple / domain / SHACL), Databricks mocks, MLflow trace-capture, an in-process MCP client, and a redaction fixture.
- **MCP integration tests**: schema + parametrized + smoke tests over the 40+ MCP tools via an in-process FastMCP client.
- **Contract tests**: OpenAPI path contract (locks the MCP server's hard-coded route constants) and a GraphQL schema contract.
- **Property tests (Hypothesis)**: OWL roundtrip, SHACL conformance, R2RML idempotency.
- **AI feature lifecycle (CNS M2)**: `.cursor/12-ai-feature-lifecycle.mdc` rule + `ai-feature` skill mandate a SPEC.md, eval dataset, and MLflow eval run for any `src/agents/**` change; SPEC scaffolds + starter `baseline.jsonl` datasets for all 6 agents; `eval-gate` / `eval-drift` CI workflows (calibration mode initially).
- **Quality enforcement (CNS M3)**: ruff + mypy config with NEW-violation-only gates against committed baselines (`mypy_baseline.txt`, `check-mypy-diff.py`), pre-commit hooks (black / ruff / secrets / changelog-presence / forbid-gsd-imports), Conventional-Commit PR-title lint, and a changelog-presence CI gate.
- **Live & e2e**: a `tests/live_integration/` httpx + JSON-RPC smoke suite against a deployed app, and a dual-mode `tests/e2e/` harness that runs the 258 Playwright user-journeys either locally or against the deployed Databricks App (bearer-header auth + a server-host redirect-fix), gated by `ONTOBRICKS_LIVE_BASE`, with mutating flows opt-in.

## Mapping Designer

- **R2RML import URI fix**: imported slash-separated class/predicate URIs are now canonicalised to the ontology's hash-terminated URIs (by local name) at import time, so designer / diagnostics / export / KG build all agree and imported mappings no longer show as "Not Mapped".
- **Auto-Exclude** smart bulk action (replaces the earlier "Exclude unmapped"): in one click excludes unmapped entities, orphans (no ObjectProperty relationships), and pure-parent abstract classes, plus unmapped ObjectProperties — with the ObjectProperty-only filter and `sql_query`-based "mapped" definition matching the graph.
- **Include excluded** bulk action to re-enable everything previously excluded.

## Deployment & Operations

- **Lakebase deploy-config collapsed** from 13 variables to its irreducible surface, then made strictly **registry-scoped** (graph schema grants moved to the in-app flow), with section-4 runtime fallbacks tracking the section-3 bound values and the dead `DATABRICKS_CATALOG` / `DATABRICKS_SCHEMA` chain removed end-to-end.
- **Single-knob multi-instance**: `deploy.config.sh` restructured so only `DEFAULT_APP_NAME` need change for a new instance (MCP name, registry schema, Lakebase schema/datname, DAB target all derived), with a `DEFAULT_SCHEMA_OVERRIDE` escape hatch for legacy deployments.
- **`deploy.sh` hardening**: colourised step logging + `ERR` trap (failing step, exact command, line, hint), a preflight step (required tooling / files / non-empty config), promoted auth verification, resource-existence checks (warehouse / volume / Postgres DB), and a `--dry-run` (`--check`) mode (+ `make deploy-dry-run`).
- **MCP companion `app.yaml`** templated end-to-end (no stale hardcoded URL / schema, no volume resource); `deploy.sh` auto-resolves `ONTOBRICKS_URL` from the live app before rendering.
- **Self-healing migrations as schema owner**: `bootstrap-lakebase-perms.sh` Step 2b now provisions `domain_versions.status` (+ index), `build_runs` (+ index), `domains.review_quorum`, and `domain_review_events` idempotently as the schema owner — fixing the Postgres ownership bug where the app SP's `ALTER TABLE` / `CREATE INDEX` silently failed (the store now skips DDL when the object already exists). `bootstrap-app-permissions.sh` grants `ALL_PRIVILEGES` on the UC schema to both SPs. One-shot `scripts/upgrade_lakebase_0.4_To_0.5.sql` migrates an existing 0.4.x registry and backfills PUBLISHED from `mcp_enabled`.

## Documentation

- `README.md`, `docs/user-guide.md` — Validation & Review workflow, per-domain quorum (+ admin override).
- `docs/data-access.md` — binary-document conversion via `ai_parse_document`, warehouse requirement, supported formats.
- `docs/deployment.md`, `docs/lakebase-graphdb.md`, `README.md` — two-schema registry-scoped permission model, collapsed deploy-config variables, dry-run, removed "separate instance" guidance.
- `CONTRIBUTING.md` — live-integration (deployed Databricks App) run commands and the mutating opt-in.

---

## Upgrade Notes

- **Run the 0.4 → 0.5 Lakebase migration.** Either `psql -f scripts/upgrade_lakebase_0.4_To_0.5.sql` (adds `domain_versions.status` + CHECK + index, **backfills `mcp_enabled=true → status='PUBLISHED'`**, mirrors status into the `info` JSONB) or `make bootstrap-lakebase` (applies the `status`, `build_runs`, `domains.review_quorum`, and `domain_review_events` migrations as the schema owner). Without the backfill, no version will be served by the API/MCP until you publish one.
- **The "Active" toggle is gone.** API/MCP now serve the numeric-latest **PUBLISHED** version. After upgrade, set the version you want exposed to PUBLISHED (Domain → Validation, or Registry → Browse → Validate). `mcp_enabled` is left dormant.
- **Editability is status-only.** Only DRAFT versions are editable (any DRAFT, not just the latest). If a version looks read-only, send it Back to Draft to edit it.
- **Sign-off quorum is per-domain.** Set it on Domain → Information → Global (default 1); the old registry-wide `global_config.review_quorum` is no longer read. Admins can publish regardless of quorum.
- **Lakebase deploy variables changed.** The datname is now `LAKEBASE_REGISTRY_DATABASE`; the removed `LAKEBASE_BOOTSTRAP_*` / `LAKEBASE_GRAPH_*` / `LAKEBASE_DATABASE` (deploy-time) / `LAKEBASE_SYNC_SCHEMA` overrides are silently lost — grant the **graph** schema via the in-app **Create Graph DB** flow or a manual `bootstrap-lakebase-perms.sh` run. The app-runtime `LAKEBASE_DATABASE` env var is unchanged.
- **Multi-instance deploys**: change only `DEFAULT_APP_NAME` in `deploy.config.sh` (set `DEFAULT_SCHEMA_OVERRIDE=""` for new deployments; leave it set for legacy schemas). App names are workspace-global.
- **Binary-document parsing requires a SQL warehouse.** PDF/Office/image uploads only feed ontology generation when a warehouse is configured; text uploads are unaffected.
- **Pitfall-clean ontologies by default.** The OWL generator now iterates to a zero-warning ontology (up to 5 fix rounds) — generation may take a little longer but produces cleaner output.
- **Dev environment**: `psycopg[binary]` / `psycopg-pool` are now in the dev dependency group (a plain `uv sync` installs them), so the registry-config gate no longer trips in tests. Pitfalls ML checks still need `uv sync --extra pitfalls`.
