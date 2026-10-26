# OntoBricks — Release Notes V0.9.0

**Release window:** September–October 2026<br>
**Test status:** all changes shipped with the non-scenario suite green: 9478 passed, 326 skipped, 7 deselected, 1 xfailed.

---

## Highlights

- **Lakehouse//RT graph queries** — administrators can point Knowledge Graph reads at a serverless Lakehouse//RT warehouse over the Statement Execution API, while builds keep a classic or serverless warehouse that still accepts `CREATE VIEW`, CTAS, and writes.
- **Three-stage Ontology Generate** — detect entities, review and lock the catalog, then complete relations, attributes, and axioms with checkpoints. The merge **appends** to the existing ontology instead of replacing it.
- **Knowledge Store** — parsed documents live in Lakebase Postgres, not a Unity Catalog Volume. Generate and Mapping share the same corpus; no `READ VOLUME` / `WRITE VOLUME` grant is required.
- **Unity Catalog Metric Views** — Metric Views load and map as data assets next to tables and views. Columns carry dimension/measure roles; Auto-Map emits `MEASURE(...)` SQL instead of `SELECT *`.
- **GitHub domain mirror** — each instance can push domain configuration (OWL, R2RML, SHACL, layouts, rules) to one GitHub repository. Lakebase stays the source of truth; Load from GitHub restores domains through the OBX import flow.
- **OBX Import as** — Registry import can rename every incoming domain (CamelCase target) and safely resets graph runtime on a renamed copy.
- **Faster Explorer first paint** — graph status no longer blocks the Knowledge Graph page on full `COUNT(*)` scans; adjacency and entity-search indexes, paged BFS, and server-side seed limits cut preview and expansion time. The canvas reports Preview, Expansion, Display, and Total duration.
- **SPARQL playground** — run SPARQL against the graph in the Knowledge Graph UI and inspect the generated SQL after execution.
- **Entity business rules** — attach SWRL rules (including aggregate rules and decision tables) to a class in Studio; run them scoped to one entity from Explorer or MCP.
- **Entity Data Quality Checks** — right-click a node in Explorer (or call MCP `check_entity_shacl_facets`) to check that one entity against every SHACL shape on its class, its parent classes, and the global ones. A popup shows per-dimension gauges and the pass/fail detail with the values found; checked nodes get a quality ring on the canvas.
- **Class actions include Databricks Jobs** — the Studio entity panel's **Behaviour** tab binds Unity Catalog functions **and** workspace Jobs that declare `entity_id`. Triggering one from Explorer opens a popup that verifies the run started (Running / Queued) and follows it; the run is tracked in navbar Background Tasks with a link to the Databricks run. MCP `invoke_entity_action` accepts `job:<id>`.
- **Ontology Studio** — Designer is renamed Studio. Multi-select, group move, selected-only Business Views, import entity from another domain, inherited outgoing relations on subclasses, and OWL/R2RML export on the canvas. The entity panel is regrouped into General / Attrs. / Model / Behaviour / Links tabs with one consistent section layout.
- **Load opens Domain → General** — Home Open, navbar Load Domain, Registry Load, Versions load/reload, and domain import all navigate to Domain → General. Publication and Validation jumps stay on their own sections.
- **OBO and MCP Team gate** — per-backend on-behalf-of identity, MCP Team gate fail-closed on public graph reads, and admin-only domain creation.
- **AI Gateway and No LLM** — domain LLM target comes from the AI Gateway picker; a global No-LLM gate turns off ontology, mapping, and Graph AI routes.
- **Safer Databricks Apps result download** — CloudFetch can be turned off globally when Apps cannot reach the CloudFetch storage host.
- **Unity Catalog names that start with a digit** — data assets such as `5_g_subscribers` load when backtick-quoted, matching Unity Catalog rules.
- **Dependabot security floors** — PyJWT, GitPython, urllib3, anyio, cryptography, MLflow, and sentence-transformers raised to patched releases. oauthlib 4.x and the remaining NLTK pathsec advisory are blocked upstream.

This release is additive on v0.8.0. Ontology-only domains, MCP policy, virtual attributes, branding, and the Clarity UI remain as documented in `releases/ReleaseNotes_V0.8.0.md`.

---

## Lakehouse compute and Statement Execution API

### Build warehouse vs Query warehouse

Lakehouse//RT warehouses accept reads and reject build DDL (`UNSUPPORTED_FEATURE.CREATE_VIEW`). Settings therefore split the roles:

- **Settings → Lakehouse → SQL Warehouse → Build** — classic or serverless warehouse used for mapping views, materialization, and other writes. RT warehouses are excluded from this selector. An administrator can override the Databricks App `sql-warehouse` resource default.
- **Query** — disabled by default and mirrored from Build. Enable **Use Lakehouse//RT for queries** to pick a distinct query warehouse (standard or RT) from the same workspace list. Turning the option off and applying clears the override so reads fall back to Build.

Builds never fall back to the Lakehouse query warehouse. Analytics job SQL always uses the Build warehouse. RT DDL failures map to this guidance instead of a generic source-table error.

### Statement Execution API

Lakehouse//RT rejects the legacy Thrift protocol. When the query (or build) warehouse uses SEA:

- SQL clients connect with `use_sea=True` (Kernel extra on `databricks-sql-connector`).
- Transient SEA 5xx / connection-reset failures retry with reconnect; genuine SQL and Thrift-not-supported errors do not.
- `SET STATEMENT_TIMEOUT` is skipped on SEA (the deprecated SEA backend 500s on those statements when CloudFetch is off). Client-side socket timeout still applies. Thrift warehouses keep the server-side bound.

### CloudFetch

**Settings → Databricks → Use CloudFetch** (administrators) controls whether SQL clients download result files via CloudFetch. Leave it on unless Databricks Apps cannot reach the CloudFetch storage host; in that case Explorer searches can time out after SQL has already finished. Saving CloudFetch no longer validates an unloaded Lakehouse Query selector.

---

## Knowledge Graph Explorer

- Async graph handlers (`/sync/stats`, load, BFS find, neighbour expand) run blocking SQL on the sized thread pool so a slow warehouse query does not stall the rest of the page.
- `/sync/stats` runs independent warehouse round-trips concurrently. Lakehouse distinct counts use `approx_count_distinct` for the Insights overview; Lakebase keeps exact counts.
- Graph existence uses `SELECT 1 … LIMIT 1`. `/sync/info` returns a pending skeleton on cache miss so the page paints immediately; the triple-count badge fills in the background.
- **Adjacency index** and a compact **entity-search index** back Preview and expansion. Explorer Phase-1 preview applies `LIMIT` in SQL and fetches `rdf:type` plus `rdfs:label` in one scan. Neighbour expansion for Delta collapses into one RT statement where possible.
- BFS payloads are paged server-side (`has_more`) for Lakehouse, Lakebase, and Neo4j. GraphQL and MCP `find` read through the same graph-cache companions.
- Knowledge Graph Explorer, GraphQL, and Chat headers no longer repeat Domain / version / status (the navbar already shows it). Graph DB name and Switch domain remain.
- The canvas stopwatch is a button: Preview request, Expansion request, Display, and Total. Seed-selection dwell is excluded. Empty results clear stale times.
- **Find Path** highlights a path on the canvas (From / To from the entity context menu; group super-nodes are walked). The Filter tab is renamed **Search**. The camera can fit a node plus its neighbors.
- Every section of the entity **Details** tab starts collapsed, so the panel stays short on rich classes; click a header to open it.
- Opening the entity-type filter no longer recounts the graph. Explorer honors max-entities in Databricks Apps.

### Entity Data Quality Checks

The Data Quality tab checks the whole graph; Explorer can now check one entity.

- **Which shapes apply** — every enabled SHACL shape targeting the entity's class, inherited from its parent classes (`parent` lineage), and the global shapes. Inherited shapes are re-targeted to the entity's own class and keep an "inherited from" badge.
- **Entry points** — right-click a node → **Data quality → Data Quality Checks (N)**, shown only when at least one shape applies; **Check** in the node detail panel's **Data quality** section; MCP `check_entity_shacl_facets(entity_uri)`.
- **Evaluation** — the shapes are translated to SQL restricted to that entity and run in one batch (with a per-shape fallback). Each shape reports passed, failed (with severity), not applicable (guard false), not evaluated (no SQL translation, such as `sh:closed`), or error, plus the values the entity carries on the constrained property.
- **Popup** — one gauge per dimension (completeness, cardinality, uniqueness, consistency, conformance, structural) scoring passed / (passed + failed), coloured with the Data Quality tab thresholds and always labelled with the percentage and "P of C passed". Below, the shapes are grouped by dimension in sections that start collapsed; clicking a gauge or a status filter chip opens the matching sections. **Re-check** recomputes; **Open Data Quality** jumps to the tab.
- **Canvas** — checked nodes get a ring: red when a Violation failed, amber when a Warning/Info failed or a shape errored, green when everything passed or did not apply. The **Quality** toolbar toggle hides the rings. Results are cached per entity for the page's lifetime.

---

## SPARQL playground

Knowledge Graph includes an in-app SPARQL playground with a documented runtime lifecycle, mobile results, and keyboard focus. After execution, generated SQL stays below the editor so operators can inspect what the translator produced.

---

## Ontology Generate and Knowledge Store

### Three-stage Generate

Ontology Generate is no longer a one-shot replacement of the ontology:

1. **Detect** — structured entity candidates from the Knowledge Store and optional data assets; locked existing classes stay anchors.
2. **Review** — add, remove, include, exclude, edit, and alternate labels; at least one entity must remain.
3. **Complete** — relations, then attributes, then axioms, with per-substage checkpoints and resume.

The final merge is append-only and idempotent. Inverse relations collapse to one direction; paraphrased and inverse duplicates are dropped. Stage 1 and Stage 3 use transport-level structured output (`response_format`). The wizard is resumable, can Start Over, and uses Entity / Relationship / Attribute wording.

### Knowledge Store

Domain document management no longer copies binaries or `_parsed` sidecars into a Unity Catalog Volume. `ai_parse_document` runs on inline base64; only parsed text is stored in Lakebase (`domain_documents`). Users can view parsed content, upload files (hard 10 MB per-file cap), and purge one or several documents. The registry UC Volume is removed from settings, DAB, health probes, and the MCP server. Generate and Mapping agents read the same store.

Data Sources UI labels are renamed **Data Assets**. Asset imports are capped (default 40). Unity Catalog **Metric Views** are first-class assets (`object_kind`: table / view / metric_view). Measure columns must be queried with `MEASURE(col) AS col` and `GROUP BY` dimensions; the mapping agent is evaluated against that contract, and the picker shows a Metric View badge.

---

## Ontology Studio

Designer is **Studio**. OWL and R2RML export live on the canvas.

- Multi-select (marquee, toolbar Select, macOS Control-click), group move, multi-delete, and Business Views created from the current selection.
- **Import entity** from another domain.
- Subclasses show inherited outgoing relations.
- Metadata saves (label, icon, reference, relationship direction) update visible D3 elements in place; topology changes still rebuild the map.
- Entity right pane tabs: **General / Attrs. / Model / Behaviour / Links**. Attrs. holds mapped attributes, then virtual attributes (computed when viewed). Model holds inheritance, equivalent/disjoint axioms, and bridges. Behaviour holds what the Knowledge Graph can run on one entity, each tagged with its effect: functions (return a result), jobs (run in background), and inference rules (SWRL, write to the graph). Links holds dashboard and dataset. Entity SWRL triggers are called **Inference rules** in the Studio and the Graph Explorer.
- Every section in those tabs shares one layout: a header with an item count, the effect chip, and **+ Add** (a *From data assets* / *Manually* menu on mapped attributes; **Change** on Dashboard and Dataset once linked), a one-line explanation with the longer help behind an ⓘ button, flat item rows, and a dashed empty state with a direct "Add …" link. Descriptions are edited in place (borderless until hovered or focused). **Equivalent to** and **Disjoint with** are chip pickers instead of Ctrl/Cmd-click multi-selects. Icon and name share one row on General.
- The first item of the **Domain**, **Ontology**, and **Mapping** menus is renamed **Information → General**; URLs (`?section=information`) are unchanged.
- OWL import and save preserve labels, language tags, resource IRIs, leftover RDF annotations, non-OWL triples, and session extras. Invalid IRIs are percent-encoded instead of failing generation. Renames stay graph-consistent.
- Mapping Diagnostics lists issues only and skips excluded relationship columns.

---

## Business rules and class actions

### SWRL on an entity

Ontology authors attach SWRL business rules to a class (including parent-class lineage). Builders run a matching rule on one entity from Graph Explorer (confirmation modal) or through MCP `run_entity_business_rule` (no confirmation). Inference is scoped to that entity; inferred triples materialize; Explorer refreshes. Aggregate Rule and Decision Table editors are documented; decision tables support optional columns, Unique hit policy, and Assign entity. Neo4j Cypher translation remains scaffolding (Neo4j domains infer no facts from this path). SWRL built-ins translate during graph inference. Databricks Apps graph writes for SWRL apply and inference purge are fixed.

### Jobs as class actions

The Studio entity panel's **Behaviour** tab already bound Unity Catalog functions (section **Functions**). Authors can also bind existing workspace Jobs that declare `entity_id`. OntoBricks only calls `run_now`, tracks the run in navbar Background Tasks, and allow-lists `job:<job_id>` from Explorer and MCP. Lakehouse App mode triggers OBO with the forwarded user token.

---

## GitHub mirror, versions, and registry

### GitHub mirror

Administrators can enable a per-instance GitHub repository. Each registry write queues an async, coalesced push. A GitHub failure never fails the registry write. Pushes appear in the domain Audit trail. Each domain folder stores split JSON plus generated OWL, R2RML, and SHACL Turtle; PUBLISHED versions are tagged `<Folder>/V<n>`. **Load from GitHub** in the Registry import modal lists domains from the Git tree and restores them through OBX preview/import. Deleting a registry domain can optionally delete the GitHub domain. Test connection, spinner, and “enable mirror before configure” UX are in Settings.

OBX import exposes an **Import as** name for every domain, not only on folder conflicts. A renamed copy resets graph runtime so the new identity does not reuse the previous warehouse objects.

### Versions

- **New Version** from the navbar Versions popup (same ungated create-version workflow as Domain → Versions). After confirmation the popup closes and the branded loading overlay stays until the new version loads.
- Domain Versions is an action-card workspace with server-computed capabilities and guarded deletion (including last-version delete when policy allows).
- Navbar Save shows a spinner while the domain is being saved.
- **Loading a domain always opens Domain → General** (`/domain/?section=information`) instead of reloading the current page. That covers Home → Open, navbar **Load Domain**, **Registry → Browse → Load**, Domain → Versions load/reload, and domain import. Intentional Publication / Validation navigations (`?section=review`) are unchanged. Reloading while already on General still refreshes that page.

The navbar domain badge restores the last confirmed loaded domain on page start and keeps it across transient `/navbar/state` failures. Navigating to Settings no longer flashes an empty badge. Closing the domain still clears it. Unloaded workspace pages redirect to Home. Domain creation is restricted to administrators.

---

## Identity, MCP, and AI gates

- Per-backend OBO / permissions surface on Domain → General **Backend** (renamed from Knowledge Graph). MCP OBO on Lakehouse carries the forwarded user token on the bypass path.
- MCP Team gate is fail-closed on public graph-read handlers. MCP sessions stay isolated.
- **MCP client reference implementation and optimization guide** (PR #193) ships with tests.
- AI Gateway model services appear in the LLM picker. Ontology Assistant and Graph AI honor the saved domain LLM target. A global **No LLM** UI gate turns those routes off; inherited privileges apply to Gateway discovery.

Data Quality `suggest_from_ontology` now uses parsed OWL characteristics (`owl:FunctionalProperty`, `owl:InverseFunctionalProperty`, cardinality restrictions) and `domain.constraints` instead of inventing `sh:minCount 1` on every object property.

---

## Lakebase operations

- Auto-provision the Lakebase registry on deploy. Existing registries skip recreate and owner-only initialize.
- Settings → Lakebase proposes **Managed sync** first. Lakeflow schedule is **Snapshot-only** (mapped R2RML views have no Change Data Feed; Triggered/Continuous coerce to Snapshot).
- Initialize DML still runs when the app service principal cannot apply DDL.
- Lakebase authentication, Settings discovery, and provisioning follow every Postgres List Projects page, so projects beyond the first API page resolve.
- Default Lakehouse access type is **views only**.
- Default deploy instance identity for this line is **09x** (`scripts/deploy.config.sh`).

---

## Reliability, architecture, and CI

- `GET /api/v1/digitaltwin/triples` no longer raises `NameError` when selecting the graph or view table; it uses the documented `backend` parameter.
- Remaining undefined-name defects in ontology evaluation, the pitfalls runner, and Delta warehouse resolution are fixed. CI runs a blocking, frozen, repository-wide Ruff **F821** gate.
- Knowledge Graph builds no longer wipe the registry ontology.
- Knowledge Store listing failures are distinguished during version deletion.
- Large route and settings modules are split into topic packages and class-per-file collaborators (graph engine, registry mixins, DigitalTwin, Generate). This is an internal layout change; public HTTP and MCP contracts stay.
- Compact Domain audit trail with before → after values, including SWRL, SHACL, relationships, and axioms.
- Live scenario campaigns (`tests/e2e/scenarios/`) can run against a deployed Databricks App (the Apps gateway bearer is forwarded). New **scenario 5** covers a Lakehouse zero-copy domain end to end: virtual attributes, a Unity Catalog function action, and an entity SWRL inference rule.

---

## Documentation

Operator guides now describe the Build / Query split, Lakehouse//RT, CloudFetch, Explorer timing, Versions-popup creation, staged Generate, Knowledge Store, Metric Views, GitHub mirror, OBX Import as, Studio, business rules, Job actions, entity Data Quality Checks, SPARQL playground, OBO, No LLM, and load navigation to Domain → General:

- `docs/getting-started.md`
- `docs/deployment.md`
- `docs/architecture.md`
- `docs/user-guide.md` (feature inventory)
- `docs/mcp.md`
- `docs/api.md`
- `docs/README.md` (documentation hub)
- `README.md`

The documentation hub and Help Center were recategorized; small guides fold into Graph backends / Advanced features with per-guide TOCs.

---

## Bug Fixes (selected)

- Fixed Knowledge Graph builds on Lakehouse//RT failing because Thrift was used even when SEA was configured, then failing again when build SQL was sent to a read-only RT warehouse.
- Fixed persistent `Error querying graph` on SEA with CloudFetch off caused by `SET STATEMENT_TIMEOUT`.
- Fixed Explorer timeouts in Databricks Apps after fast RT SQL when CloudFetch could not download result files.
- Fixed Settings → Databricks saves raising a false “Select a Query SQL Warehouse” error before Lakehouse controls had loaded.
- Fixed Data Assets rejecting valid Unity Catalog tables whose names start with a digit.
- Fixed Ontology Studio flashing a full-map spinner on metadata-only saves.
- Fixed the navbar hiding a still-loaded domain after a failed or slow navbar refresh.
- Fixed `GET /api/v1/digitaltwin/triples` crashing with `NameError` on every request.
- Fixed Load from GitHub doing nothing on Import, hanging on Reading…, and Test connection 404 on GET repo.
- Fixed GitHub import listing only registry folders instead of the Git tree.
- Fixed last-version domain delete being blocked when policy should allow it.
- Fixed Apps graph writes for SWRL apply and inference purge.
- Fixed entity inference rules and build-time inferred triples failing on Lakehouse (`DELTA_CLUSTERING_NOT_SUPPORTED`) because they wrote through the read-only Query warehouse; they now use the Build warehouse.
- Fixed Lakehouse zero-copy status probes failing with `DESCRIBE DETAIL` on the `…_data` VIEW; views are treated as existing.
- Fixed a domain loaded with `version: 1` (JSON number) falling through to the latest version, and Overview / status timing out on `COUNT(*)` over the pass-through `…_graph` VIEW; they now read the companion tables.
- Fixed Graph Explorer right-click menus opening partly off-screen near the viewport edges.
- Fixed attribute names and inherited relationship labels being rendered unescaped in the Studio entity panel.
- Fixed an entity quality check on a domain whose graph is not built returning a bare 500; it now returns a readable 502 ("Could not read the knowledge graph — is it built?").
- Fixed `suggest_from_ontology` inventing cardinality and calling a missing `domain.get_constraints()`.
- Fixed Generate Stage-1 false rejection when every selected entity is already a locked anchor, and Stage-3 id-bracketing / JSON double-encoding under `response_format`.
- Fixed loading a domain from Home (or the navbar Load dialog) leaving the user on the homepage with only the top menus updated. Generic loads now open Domain → General.

---

## Security hardening (code review)

- **CSRF:** unsafe requests to the web app now need both the `csrf_token` cookie and a matching `X-CSRF-Token` header (constant-time comparison); a missing cookie no longer slips through. `/api/` and `/graphql/` stay exempt.
- **`/health`:** now a public liveness probe that leaks no paths, catalog names or exception text. The full readiness probes moved to the admin-only `/settings/diagnostics/readiness`. A failed startup no longer shows the traceback to anonymous callers.
- **SQL literals:** one dialect-aware escape (Spark doubles backslashes, Postgres doesn't) across SPARQL rules, decision tables, aggregate rules, SHACL, the SPARQL translator and the graph stores. A value ending in a backslash can no longer close a literal early; `nan`, `inf` and `1_000` are no longer emitted as bare numbers; a non-numeric aggregate `threshold` is rejected instead of pasted into `HAVING`.
- **Front-end XSS:** one DOMPurify-sanitised `renderMarkdown` for comments, chat and audit; `escapeHtml` escapes quotes and is the only definition; untrusted ontology and metadata values are escaped before `innerHTML`.
- **Tokens out of the session:** the warehouse settings no longer persist a posted token; a token left in an old session file is dropped on load and masked in the session debug view.
- **Permissions and reviews:** permission updates lock the row (no lost update); a Lakebase read failure raises instead of reading as "no permissions"; review quorum fails closed.
- **MCP:** GraphQL paths URL-encode the domain name and forward the caller's identity; HTTP failures in tools and resources return a short message instead of a stack trace.
- **Honest errors:** an unparseable SHACL report and failed Unity Catalog listings now raise instead of reading as "conforms" or an empty schema.

---

## Security (Dependabot)

Raised uv constraint floors and relocked `uv.lock` / `src/mcp-server/uv.lock` against patched releases:

| Package | Floor | Notes |
|---------|-------|--------|
| PyJWT | ≥2.15.0 | HMAC/JWK confusion series, RecursionError, options-dict mutation |
| GitPython | ≥3.1.62 | submodule path traversal, ReDoS, git-dir impersonation |
| urllib3 | ≥2.8.0 | proxy TLS override, unbounded chunk-size, Deflate loop |
| anyio | ≥4.14.2 | TLSStream IDNA 2003 spoofing, process-pool stderr stall |
| cryptography | ≥50.0.0 | PKCS#7 oracle + path-building; now allowed by MLflow 3.16.1 (`cryptography<51`) |
| MLflow | ≥3.16.1 | outside AI Gateway SSRF range (3.13.0–3.15.2) |
| sentence-transformers | ≥5.6.0 | `trust_remote_code` bypass on local model load (pitfalls extra) |

**Not patched (upstream):**

- **oauthlib 4.0.0** — `databricks-sql-connector` 4.4–4.6 still requires `oauthlib>=3.1.0,<4.0.0`.
- **NLTK GHSA-8mgp-746c-j5xp** — latest published is 3.10.3; no patched release. Pitfalls extra only.

---

## Upgrade Notes

### No registry schema migration required for warehouses

v0.9.0 adds global-config keys (`warehouse_use_sea`, Lakehouse query warehouse / `use_sea`, `use_cloud_fetch`, GitHub mirror settings). Existing v0.8.0 domains keep working: Query mirrors Build, SEA defaults off, CloudFetch defaults on, GitHub mirror defaults off.

Knowledge Store uses a new Lakebase `domain_documents` table created by the standard registry bootstrap / initialize path. The registry Unity Catalog Volume is no longer used for documents; previously stored Volume binaries are not migrated automatically — re-upload documents that must remain in Generate or Mapping context.

### Behaviour changes from the security hardening

- **Health probes:** anything that parsed the `checks` list from `GET /health` must call the admin-only `GET /settings/diagnostics/readiness` instead. Probes that only need HTTP 200 are unaffected.
- **Scripts posting to web routes:** non-browser clients must use `/api/v1/` or `/graphql/`; internal routes now reject requests without the CSRF cookie and header.
- **SHACL `sh:pattern`:** backslashes now reach Spark `RLIKE` as written. Before, `\d+` was silently read as `d+`, so pattern rules may report different (correct) violation counts after the upgrade.
- **Unity Catalog errors:** a failed table or column listing now surfaces as an error (502) instead of an empty list.

### Menu rename

The **Information** menu item is now **General** in Domain, Ontology, and Mapping. Section ids and URLs are unchanged. The No-LLM guidance now reads `No LLM selected. Select one in Domain → General → AI.` — clients matching the old `Domain Information → AI` text must update.

### Load navigation

Scripts or bookmarks that assumed **Load Domain** would `location.reload()` the current page (Home, Registry Browse, Versions) must follow the redirect to `/domain/?section=information`. Publication and Validation entry points that already set `?section=review` are unchanged.

### Enabling Lakehouse//RT reads

1. Keep **Build** on a classic or serverless SQL Warehouse (not Lakehouse//RT).
2. Under **Settings → Lakehouse → SQL Warehouse**, enable **Use Lakehouse//RT for queries** and select a distinct query warehouse.
3. If Explorer times out in Databricks Apps after SQL has finished, uncheck **Use CloudFetch** and save.

Rebuilds are not required solely to turn RT queries on. Builds still run on the Build warehouse.

### GitHub mirror

Configure owner, repository, branch, and a Databricks secret for the token under Settings. Lakebase remains authoritative. Enable the mirror before filling repository fields. First push claims the repository via `.ontobricks/instance.json` unless take-over is explicit.

### Instance suffix

Deploy configs and tests for this line use `INSTANCE_ID=09x`. Align `scripts/deploy.config.sh`, app names, and DAB targets before deploying alongside an 08x instance.

### External API

`GET /api/v1/digitaltwin/triples` remains the triples listing endpoint. Callers that already passed `backend` are unchanged; the server now honors that parameter instead of raising.

New: `GET /api/v1/digitaltwin/nodes/shacl?uri=…` returns the entity's SHACL facets, and `/nodes/context` plus `/domain/classes` carry a `shacl_shape_count`.

MCP: `run_entity_business_rule` and `check_entity_shacl_facets` are new (the latter is a graph tool that can be disabled per domain in the MCP policy); `select_domain` and `get_entity_context` hint at the applicable shape count when it is enabled; `invoke_entity_action` accepts `job:<job_id>` in addition to a Unity Catalog function FQN. Domain MCP policy from v0.8.0 still controls which tools are published.
