# OntoBricks — Release Notes V0.7.0

**Release window:** July – August, 2026
**Test status:** all changes shipped with the suite green (4688 passed, 276 skipped, 5 deselected, 1 xfailed).

---

## Highlights

- **Neo4j graph backend (GA)**: Neo4j (Community, Enterprise, AuraDB) joins Delta Lake and Lakebase as a full **typed property-graph** engine — nodes/relationships instead of flat triples, a per-domain database selector, Settings → Neo4j (Connection / Objects / Health tabs with a live Bolt + Cypher **Test connection** probe), and `documentation/neo4j-requirements.md` for flavor compatibility.
- **Graph backend selection moved per-domain**: choosing Lakebase / Lakehouse (Delta) / Neo4j is now a mandatory per-domain setting on Domain → Information → Knowledge Graph (large selectable cards), not a workspace-wide Settings toggle; the old Settings → Back End page is gone.
- **Knowledge Graph Analytics rebuilt on a Lakeflow job**: PageRank, connected components, clustering, and (pivot-sampled, clearly labelled) betweenness/closeness are now computed by a serverless Databricks job reading the same `_data` snapshot every graph engine's Build now materialises — one compute path, identical numbers on Lakebase, Lakehouse, and Neo4j, no more silent "0.0000" for uncomputed metrics. A new three-tab **Analytics dashboard** (Dashboard / Data Model Health / AI Insights) replaces the old seven-tab layout, with global metric distributions and a scope-picker dialog.
- **Run history reorganized**: per-domain Build + Analytics run history now lives on Knowledge Graph → Management → **Runs** (two tabs, no version filter — always spans every version), and a new admin-only **Settings → Automation → Runs** page shows the same history across every domain with server-side pagination.
- **Scheduler generalized**: the scheduler's two hardcoded job kinds (Build, Cohort) become four data-driven task types — Build, Cohort, **Graph Analytics**, and **Inference/Reasoning** — all schedulable, sharing one CRUD/executor stack instead of four parallel copies.
- **Conditional SHACL rules ("IF" guards)**: Data Quality conformance/consistency rules can now be scoped to instances matching an attribute condition, plus new **numeric range** and **string length** conformance checks that actually execute (they previously showed a false 100% pass rate).
- **Ontology Class Actions**: bind a Unity Catalog function (one required parameter) to an ontology class as an invokable "Action", surfaced in the entity panel, the Graph Explorer (details pane + right-click), and the MCP server (`invoke_entity_action`).
- **Agent Bricks — PGE mapping & routing supervisor**: a Planner→Generator→Evaluator mapping engine (`agent_mapping_pge`) and a complexity-based routing supervisor (`agent_supervisor`) join the existing auto-mapping agent, plus a PGE evaluator stage for OWL generation (orphan/dangling/naming/duplicate checks with bounded retries).
- **Registry becomes a navbar modal**: Registry is no longer a full page — it's an icon-only navbar button opening a modal with Create/Browse/Bridges tabs; Developer/API, Automation (Scheduler, Runs), and Teams moved into Settings.
- **Security & reliability hardening**: Unity Catalog SQL identifier validation (injection guards), a session-file lifecycle fix (an unbounded-growth + path-traversal issue: 82,200 stray session files, and a raw cookie value used as a filename), and dependency floor raises across both the app and MCP server lockfiles (GitPython, MCP, NLTK, Pillow, pip, pyasn1, setuptools, torch, Click).
- **`TRY_CAST` everywhere**: every remaining Spark SQL emitter (SWRL, decision tables, aggregate rules, SHACL guards, SPARQL translation) now uses `TRY_CAST` instead of a bare `CAST`, so one non-numeric value in a triple can no longer abort an entire rule/query on ANSI-mode warehouses.

---

## Neo4j Graph Backend

- **Typed property graph** (not flat triples): `rdf:type` → node label, `rdfs:label` → `name` property, literal predicates → node properties, URI-object predicates → relationships (`Neo4jGraphModel.py`). Reads reconstruct the exact original `{subject, predicate, object}` triples via a per-graph reverse-map (`:__GraphSchema` node), so the Knowledge Graph view, GraphQL, and reasoning behave identically across Delta / Lakebase / Neo4j.
- Neo4j 5.x server required (4.x unsupported); **no APOC required** — the write path inlines sanitised labels and batches with `UNWIND` so it runs on Aura Free / Community with zero plugins.
- Per-domain **Neo4j database selector** (parity with Lakebase's picker), with graceful degradation on Community (single-database) servers via `SHOW DATABASES`.
- Settings → Neo4j: Connection / **Objects** (list + drop every OntoBricks graph in the connected database, with counts) / **Health** (Bolt handshake) tabs; **Test connection** button runs a real handshake plus a `RETURN 1` Cypher probe and reports latency + credential source.
- Nested per-backend connection config (`{lakebase: {}, neo4j: {}, lakehouse: {}}`) so Neo4j and Lakebase can never collide on shared keys (e.g. `database`).
- `documentation/neo4j-requirements.md` documents server requirements, flavor compatibility (Aura / Enterprise / Community), the per-domain database selector, and the breaking storage-format change from earlier pre-release flat-graph builds (old flat graphs must be rebuilt).

## Knowledge Graph Analytics

- **Phase A → Phase B → Lakeflow-only**: the 500k-triple in-memory cap was first lifted with SQL pushdown aggregation, then a serverless Lakeflow job added PageRank / components / clustering at scale, and finally the two intermediate compute paths (NetworkX in-memory, SQL pushdown) were deleted — **the Lakeflow job is now the only compute path**, so every backend produces identical numbers and a failed run raises instead of silently degrading to a different algorithm.
- **Betweenness & closeness** are approximated via Brandes-Pich pivot sampling (exact when pivots ≥ node count), clearly marked as estimates (`≈`) in the UI rather than presented as exact values.
- **Analytics dashboard redesign**: seven tabs collapsed to three (Dashboard, Data Model Health, AI Insights); a KPI strip, a five-tile metric-distribution strip (with a **Log scale** toggle), and an analysis-scope picker dialog (asked at launch instead of a toolbar dropdown that silently went stale).
- **Admin toggle** (Settings → Global) turns job-mode analytics on/off at runtime, replacing a redeploy-only env var; the UI now names the *specific* reason job mode is unavailable (toggle off / no job name / graph unreadable from Spark) instead of generic advice.
- Every graph engine's Build pipeline now materialises the `…_data` UC snapshot the job reads, closing the gap where a Lakebase or Neo4j domain built cleanly but could not be analysed.
- Uncomputed / approximate metrics render as an em-dash or `≈`-prefixed value instead of a misleading `0.0000`.
- The Databricks graph-analytics job is deployed via the Asset Bundle (`resources/graph_analytics.job.yml`), with a preflight test suite validating the bundle YAML, job parameters, and CLI wiring so a mis-indentation cannot silently break every `databricks` CLI command again.

## Run History (Runs page)

- Per-domain **Knowledge Graph → Management → Runs** now shows Build runs and Analytics runs as two tabs (was: one table, with Analytics history buried on a since-removed Analytics tab), spanning every version with no filter, each loading and failing independently.
- New admin-only **Settings → Automation → Runs**: the same two-tab history across **every** domain, with a Domain filter and real server-side pagination — replacing the old single-domain "Build Analytics" panel.
- A failed analytics run shows a red "Failed" badge with dashed-out metric cells instead of printing the stored zeros as if they were real results.

## Scheduler

- Collapsed two hardcoded job kinds (Build, Cohort — ~1800 lines, mostly duplicated) into four **data-driven task types**: Build, Cohort, **Graph Analytics**, and **Inference/Reasoning** (SWRL/OWL 2 RL materialization), sharing one CRUD path, one job-registration harness, and one executor contract.
- Fixed a latent bug where a scheduled Inference run computed its results and silently discarded them (write-back was gated on an "API vs UI" flag instead of the caller's actual options).

## Data Quality & Reasoning

- **Conditional SHACL rules ("IF" guards)**: conformance/consistency rules can now carry attribute conditions (built the same way as Business Rules decision-table conditions), compiled to SQL (subquery filter) or evaluated in-memory, and exported/imported as a `sh:SPARQLTarget`.
- **Numeric range & string length** conformance checks now actually execute (previously silently reported a false 100% pass rate); a check the engine genuinely cannot run now shows a dash with a "did not run" tooltip instead of a misleading percentage.
- **Single execution target**: Data Quality checks (SHACL, SWRL, decision tables, aggregate rules) always run as SQL against the build-time triple-store VIEW — the old "Triple-Store vs Graph" backend toggle (which silently ran different rule subsets over different data, and couldn't evaluate `sh:datatype`/`sh:sparql` at all) is gone.
- Running a subset of dimensions/rules now genuinely runs only that subset — SWRL rules, decision tables, and aggregate rules previously had no per-rule id and ran in full whenever their dimension was selected.
- Property dropdowns in Data Quality and SWRL rule editors now correctly scope to the selected entity's own + inherited attributes (previously leaked properties from unrelated entities, or silently misclassified relationships as attributes).
- New **SWRL text pane** (Business Rules) and the existing SHACL Turtle pane both moved from an inline toggle to a modal-xl viewer with Refresh / Export / Import.
- `TRY_CAST` (instead of bare `CAST`) across every remaining rule SQL emitter (SWRL builtins, decision tables, aggregates, SHACL condition guards) — a single non-numeric triple value can no longer abort an entire rule on ANSI-mode Databricks SQL warehouses; failed checks now surface the underlying engine error message instead of a generic failure.

## Ontology & Mapping

- **Class Actions**: bind a Unity Catalog function (exactly one parameter — the entity's local ID) to an ontology class; round-trips through OWL, surfaced via MCP (`invoke_entity_action`), and invokable from the Graph Explorer (details pane + right-click, with the function's description shown in the result popup).
- **Dataset preview**: linked-dataset key-column is now a validated dropdown (not free text) with an editable description (auto-filled from the UC comment, refillable on demand), and a "Preview rows" action in the Graph Explorer showing up to 10 live UC rows.
- **Designer external-config badge**: a small lightning-bolt badge on the D3 Ontology Designer marks classes that already have a Dashboard, Dataset, Actions, or Bridge configured.
- **Designer context menu**: right-click an entity or relationship to jump straight into a specific tab of the shared edit panel (Details / Attributes / External / Constraints).
- **Create Relationship dialog** split into separate Label and ID fields (ID mirrors the Label as camelCase until manually edited, with a global uniqueness check).
- Backported and merged **PGE (Planner→Generator→Evaluator) engines**: `agent_mapping_pge` for entity/relationship SQL mapping, a PGE evaluator stage for `agent_owl_generator` (deterministic orphan-class / dangling-domain-range / naming / duplicate-class checks with bounded retry hints), and `agent_supervisor`, a complexity-based router between the legacy auto-mapping agent and the new PGE engine.
- Terminology consistency pass: user-facing UI text now says "Entity/Entities" everywhere instead of a mix of "Class(es)"/"Entity" (genuine OWL/RDF/SHACL technical identifiers such as `owl:Class`/`sh:class` are untouched).
- Relationship Constraints tab now shows a visible caption under each cardinality/characteristic option instead of relying on hover tooltips.
- SWRL "Attribute conditions" now offers every selected IF entity as a condition subject (previously filtered out entities whose attributes couldn't be resolved, including via inheritance).
- New domain names are restricted to CamelCase alphanumeric (no spaces/special characters), matching the existing Domain Information validation.
- Fixed R2RML Turtle generation failing (502) when the domain/ontology name contained spaces.

## Registry, Settings & Navigation

- **Registry is now a navbar modal** (Create a New Domain / Browse / Bridges tabs) instead of a full page with its own sidebar; Load Domain moved out of the toolbar into Browse.
- **Developer → API**, **Automation → Scheduler / Runs**, and **Teams** moved from Registry into Settings, alongside the removal of the now-redundant read-only Admin → Permissions page.
- **Stacked modal fix**: opening a confirmation dialog (e.g. Load Domain) over an already-open modal (e.g. the Registry modal) now dims/blurs the parent instead of rendering white-on-white.
- Icon consistency pass across the top-level navbar: Registry → `bi-boxes`, Domain → `bi-box`, Knowledge Graph → its own `bi-radar` (previously shared `bi-box`).
- Breadcrumb simplified to just the current main menu + sub-menu (dropped the Registry/domain-name ancestor crumbs).
- Ontology Designer / Mapping Designer bottom-pane "Save" renamed to **"Apply"** (it only writes into the session; Registry publish remains the durable save) — same change applied to the Mapping Manual panel.
- Domain Save no longer shows a confirmation popup before writing to the Registry.

## Security & Reliability

- **Unity Catalog SQL identifier validation**: centralized validation/quoting of catalog/schema/table/column identifiers before any warehouse SQL is built, closing an f-string SQL injection surface; restored a CloudFetch recursion guard.
- **Session file lifecycle fix**: the file-based session store had grown to 327 MB / 82,200 files on a dev machine (no expiry, and a missing-file cookie always minted a brand-new session instead of reusing the one the client held) — and, separately, a malformed cookie value was used verbatim as a filename, which would have let a crafted cookie write a session file outside the session directory. Both are fixed: session IDs are now validated against a strict 32-hex-char pattern before ever touching the filesystem, and a startup sweep reaps files idle longer than the configured max age.
- **Dependency floor raises** across both the app and standalone MCP server lockfiles: GitPython (two advisories), MCP, NLTK, Pillow, pip, pyasn1, setuptools, torch, and Click.
- `uv run` (bare, without `--frozen`) is now explicitly called out everywhere as a lock-poisoning command — a bare `uv run pytest` during a routine test pass silently rewrote `uv.lock` to point at an internal PyPI proxy and broke the next deploy; the documented and mandatory test command is now `uv run --frozen pytest -q -m "not scenario"`.

## Deploy & Infrastructure

- **Per-instance DAB targets** (`DEFAULT_INSTANCE_ID`): a single knob derives a distinct app name and Databricks bundle target per instance, so changing only the app name no longer risks Terraform destroying the running app as a "rename".
- Fixed a `uv.lock` fully proxied against an internal Databricks PyPI mirror (0 public-CDN URLs), which crashes a container build; rewritten back to public PyPI.
- `scripts/` reorganized into `bootstrap/`, `migrations/`, and `_internal/` subfolders; CI/dev tooling extracted into a new `ci/` folder.
- GitHub Pages **marketing site** added under `docs/` (Home, Features, Get Started, Docs hub, About, custom 404); the former Markdown/Sphinx product documentation moved to `documentation/` so `docs/` is free for the Pages publish root.
- Lakebase connection diagnostics, deploy-script dependency preflight, and a deploy-time fix for Lakebase's underscore-vs-hyphen database naming ambiguity.
- Analytics-job `CAN_MANAGE_RUN` permission now granted automatically to both app service principals on every `make deploy` (previously required a manual grant, since `jobs.list()` is ACL-filtered).

## Bug Fixes (selected)

- **Delta triple-store backend**: fixed inference/SWRL materialization writing to the wrong logical table name, Graph Explorer querying the wrong UC table FQN, a failing `CREATE TABLE ... AS SELECT` (RTAS cannot declare an explicit schema), and silent build-failure detection in the KG Build UI (wrong polling endpoint).
- **MCP server latency**: a 6-phase pass removed redundant per-request domain reloads, OWL/R2RML regeneration, and session saves on every read-only tool call; added a TTL-cached PUBLISHED-domain store, a pooled shared HTTP client, and tuned retry backoff.
- **Graph Chat**: structured (non-string) model responses no longer render as `[object Object]`.
- **Lakebase**: fixed sync failures on literal objects exceeding the Postgres btree index limit (new `object_hash` generated column), and a stale `_sync` table ownership conflict that blocked Lakeflow's managed-sync pipeline.
- **Auto Mapping**: the "chunk errors" count in the completion message no longer conflates genuine LLM/agent failures with entities the agent cleanly declined to map.
- **Navbar / domain switching**: the top-navbar domain name/version now refreshes reliably after a cross-domain bridge switch (previously stuck on the stale identity for up to 15 seconds).
- **"Unmap all"** renamed and added to the Mapping Designer toolbar (previously only on Mapping Information), wired via a reliable `addEventListener` confirm instead of an inline `onclick`.
- Fixed the Switch-domain modal offering "Save my changes before switching" on read-only (IN-REVIEW/PUBLISHED) versions, which cannot be saved.
- Fixed an unreachable graph engine (e.g. a slow Neo4j Aura probe) being reported as "Graph not built" instead of "Status unavailable" — a transient connectivity failure is no longer cached and presented as a confirmed absent graph.

---

## Upgrade Notes

### New deploys (v0.7.0 from scratch)

No special action beyond the standard `make bootstrap-lakebase` (which now also self-heals the scheduler's generalized `task_type`/`target_key` columns and analytics run-history tables) and `make deploy`, which additionally grants `CAN_MANAGE_RUN` on the graph-analytics job to both app service principals automatically.

### Upgrading from v0.6.x

- **Neo4j is a breaking storage change for pre-release testers only**: any Neo4j graph written by an earlier v0.7 pre-release build (flat triple nodes, no relationships) is **not** migrated automatically. Drop the old graph via Settings → Neo4j → Objects and rebuild the domain. Domains that have never used Neo4j are unaffected.
- **Graph backend selection moved**: the workspace-global Settings → Back End page is gone. Every domain must have a `graph_backend` (Lakebase / Lakehouse / Neo4j) set on Domain → Information → Knowledge Graph; existing domains default to Lakebase.
- **Analytics compute path changed**: domains built before this version have no `…_data` snapshot for the Lakeflow analytics job to read. Analytics hard-fails on those domains with a "Run Build" remedy — rebuild the domain once to pick up the snapshot. The old in-memory and SQL-pushdown compute paths (and the `analytics_pushdown_enabled` setting) are removed; the job is opt-in via Settings → Global (admin) and requires `make deploy` to have shipped the job bundle.
- **Scheduler schema migration**: `schedules` / `schedule_runs` gain `task_type`, `target_key`, `config` (jsonb), and (for runs) `detail` (jsonb); the unique key widens from `(registry_id, domain_name)` to `(registry_id, task_type, domain_name, target_key)`. Applied lazily and idempotently by the app (`_ensure_schedule_task_columns`), or eagerly via `make bootstrap-lakebase`. Legacy cohort schedules stored in the `global_config` JSONB blob are migrated into the real tables automatically, once.
- **Session files**: existing session directories accumulated before this release are not automatically cleaned; the new reaper only prunes files older than the configured max age going forward. A one-time manual cleanup of `session_dir` is optional but harmless.
- **Documentation paths moved**: product documentation (Markdown + Sphinx) moved from `docs/` to `documentation/`; any bookmarked links or scripts referencing `docs/user-guide.md` etc. should be updated to `documentation/user-guide.md`. `docs/` is now the GitHub Pages marketing site.
- **`uv run` without `--frozen` is no longer supported for routine test/dev commands** — always use `uv run --frozen pytest ...` to avoid rewriting `uv.lock` against the internal proxy.
- Data Quality's "Triple-Store vs Graph" backend toggle is gone; every check now runs against the build-time VIEW. The public `DataQualityRequest.backend` API field is deprecated (ignored, not removed) for backward compatibility.
