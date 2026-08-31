# OntoBricks — Release Notes V0.8.0

**Release window:** August 2026<br>
**Test status:** all changes shipped with the non-scenario suite green: 5756 passed, 305 skipped, 6 deselected, 1 xfailed, 23 warnings.

---

## Highlights

- **Ontology-only domains** — a domain can now use **No Backend**, move through review and publication with an ontology but no materialized graph, and expose `describe_ontology` through MCP while graph-dependent UI and tools stay unavailable.
- **Per-domain MCP policy** — each domain controls which graph tools it publishes and whether datasets, bridges, actions, and virtual attributes are normal, preferred, or disabled context.
- **Virtual attributes** — ontology entities can declare values computed on demand by Unity Catalog functions. Values appear in Graph Explorer, REST, and MCP without entering mappings, R2RML, SHACL, cohorts, or graph materialization.
- **Data-source and mapping integrity** — metadata refresh now shows column-level changes before applying them, source deletion lists affected mappings, and schema drift is visible in diagnostics and on the Mapping canvas.
- **Lakehouse view-only materialization** — Lakehouse domains can expose a live pass-through view instead of copying mapped triples into a Delta table; analytics automatically creates and removes a temporary snapshot when required.
- **Safer graph operations** — Builders can refresh frozen IN-REVIEW and PUBLISHED graphs, inspect and purge generated reasoning/cohort triples without touching mapped source triples, and rely on bounded Graph Chat reads.
- **Configurable product branding** — administrators can change the application title, primary color, logo, and default entity icon with server-first rendering and immediate accessible preview.
- **Unified application experience** — navigation, sidebars, tabs, buttons, Designer panels, responsive layouts, and page rhythm now use one documented Clarity design system.

---

## Ontology and Mapping

### Stronger metadata and mapping integrity

- Removing or clearing a referenced data source now opens a detailed confirmation listing affected entity and relationship mappings.
- Generated R2RML and SQL are invalidated when a referenced source disappears, preventing a stale build artifact from surviving the removal.
- **Update from UC** computes added, removed, type-changed, and unchanged columns and presents the diff before the refreshed metadata is saved.
- Mapping Diagnostics and the Designer surface advisory schema-drift warnings, including missing columns and affected relationship sides.
- Aliased SQL projections are excluded from physical-column drift checks to prevent false positives, and warehouse checks run in the background without blocking the Designer.

### Virtual attributes

- Entity classes can bind a Unity Catalog scalar or table function as a group of virtual attributes.
- Function output columns become ontology-visible attribute declarations; name collisions are resolved safely.
- Graph Explorer computes values on demand, caches them for the page session, and isolates failures by function.
- External REST and MCP expose declarations separately from computed values, preserving the distinction between “not computed” and a computed null.
- A dedicated MCP `compute_virtual_attributes` tool complements `describe_entity` and `get_entity_context`.
- Virtual attributes intentionally remain outside mappings, R2RML, SPARQL, GraphQL, cohorts, SHACL, and Data Quality materialization.

### Reliable Designer persistence

- Ontology and Mapping Designer edits auto-save when leaving their Design section or navigating away.
- Entity and relationship panels no longer require Apply/Cancel actions; closing or switching context flushes pending edits.
- The ontology load path now waits for authoritative ontology state before rebuilding the canvas, preventing stale design-layout copies from overwriting saved changes.
- Design views are reconciled with the ontology, and orphaned datatype-property mirrors are pruned so deleted attributes do not reappear.
- Relationship direction updates immediately across the Ontology Designer, ontology map, and Mapping Designer.
- R2RML exports now emit standards-compliant bare column names while retaining compatibility with older quoted exports.
- The Manual Mapping panel correctly owns its shared detail pane and no longer leaves previews spinning indefinitely.

### Auto-Mapping observability

- The Auto-Map progress overlay shows the agent's live ordered activity without adding a separate streaming endpoint.
- Cancel is cooperative: no new agent chunk starts after cancellation, while completed chunks remain saved.
- Every completed, failed, crashed, or cancelled run records a durable execution report in the Domain Audit Trail.

---

## Knowledge Graph and Backends

### Ontology-only and per-domain backend choice

- **No Backend** joins Lakehouse, Lakebase, and Neo4j as a first-class domain type.
- Mapping and Knowledge Graph navigation is disabled for ontology-only domains, including direct-route protection.
- New domains default to Lakehouse and explain all backend choices; backend selection remains editable under Domain → Information.
- Backend selection is presented as accessible branded cards and is natively frozen for viewers, frozen versions, and edit-locked sessions.

### Lakehouse view-only materialization

- Lakehouse domains can choose:
  - **TABLE — materialized copy** for a build-time snapshot.
  - **VIEW — no data copy** for live reads over the mapping gateway.
- The Build page, health summary, run records, and domain payloads report the active materialization mode.
- Switching modes safely replaces the previous relation type.
- Graph Analytics uses a disposable Delta snapshot for view-only domains and always removes it after the run.
- Lakebase and Neo4j continue to resolve to table materialization for their UC analytics snapshot.

### Generated inference management

- Builders and administrators can inspect the combined count of materialized reasoning and cohort triples.
- **Purge Inferences** is available from Lakehouse/Lakebase Build, Inference, and Cohorts.
- Purging truncates only the generated companion; mapped source triples remain untouched.
- Unsupported backends, including Neo4j, reject the operation safely.

### Frozen-version refresh

- Builders can rebuild graph data and materialize reasoning for IN-REVIEW and PUBLISHED versions without reopening ontology or mapping edits.
- The UI skips domain-design persistence for frozen versions while retaining viewer and edit-lock enforcement.

### Graph reliability

- Lakebase and SQL Warehouse reads have scoped statement timeouts.
- Graph Chat result size is bounded and reports blocking-pool pressure.
- Analytics no longer displays historical charts as if they described a graph whose live objects were deleted; run history remains available under Management → Runs.

---

## Registry, Workflow, and Permissions

- DRAFT versions can enter review when they contain either a successful graph build or a valid ontology; truly empty versions remain blocked.
- Published domain listings expose `has_graph`, enabling ontology-only MCP selection without advertising graph tools.
- Databricks group grants now participate in application and domain role resolution using the current user's SCIM membership; the most privileged direct or group match wins.
- Editors and Builders can discover Unity Catalog catalogs and schemas for Add Data Source without requiring application `CAN_MANAGE`; shared Settings writes remain administrator-only.
- Registry configuration now prefers an explicit environment volume over the seeded session default.
- Starting **New Domain** first resolves the currently loaded domain's save/discard/cancel flow, and the creation fields remain editable even if the previous domain was read-only.

---

## External API and MCP

### Domain-aware MCP publication

- Domain Information adds an MCP tab with four always-on registry tools and configurable domain tools.
- Tool visibility resets when selecting a domain and is enforced again at call time, preventing cached clients from invoking disabled tools.
- Datasets, bridges, actions, and virtual attributes can be **Preferred**, **Normal**, or **Disabled**.
- Cross-domain bridges include the target-domain description and direct the model to `select_domain` for a real hop; unavailable MCP targets are filtered from external context.

### Ontology-only publication

- `describe_ontology` returns ontology classes and OWL content without requiring a graph.
- Graph tools are hidden and refused for a domain whose latest PUBLISHED version has no graph.
- Per-domain policy can still disable `describe_ontology`.

### REST, GraphQL, and Swagger

- `GET /api/v1/domains` exposes normalized `graph_backend`, `has_graph`, and `mcp_policy`.
- `POST /api/v1/domain/info` includes normalized backend information and typed file/statistics response models.
- GraphQL domain discovery lists only materialized graph domains.
- Settings → Developer → API now selects only API-visible domains and PUBLISHED versions, shows backend readiness, and disables graph-dependent Try-it actions when no graph exists.
- Swagger/ReDoc preserve descriptive tags, support contact, license metadata, and accurate typed response schemas.
- External endpoints expose virtual-attribute declarations and on-demand computation.

---

## UI and Settings

### Configurable UI branding

- Settings → Configuration → UI manages application title, primary color, application logo, and default entity icon.
- Branding is stored atomically in registry global configuration with compatibility for the legacy navbar logo.
- Server-first rendering applies title, favicon, logo, and derived CSS tokens on the initial HTML response, avoiding a default-theme flash.
- Settings provides immediate preview, validation, accessible swatches, Reset icon, Default, Save, and Discard actions with an unsaved-change guard.
- GraphiQL, Help Center, Ontology Assistant, navbar state, and graph highlights use the configured brand.

### Clarity design-system refresh

- The sidebar is a framed card, user identity moved to the navbar, and level-two workspace navigation became a compact segmented control.
- Page and panel tabs use a shared underline rail and density model; card-integrated tabs are now the default page pattern.
- Ontology and Mapping Designers use persistent two-card split layouts, manual resize handles, placeholders, and optional session-persisted dotted grids.
- Shared button focus, hierarchy, grouping, responsive wrapping, and tokenized Graph Chat controls were standardized.
- Sidebar content roots, scrolling ownership, mobile natural flow, and bottom alignment were normalized across Domain, Ontology, Mapping, Knowledge Graph, and Settings.

---

## Security and Reliability

- Dependency floors were raised for MLflow, GitPython, aiohttp, and sqlparse, and the lockfile remains public-index deploy safe.
- The remaining cryptography advisory cannot yet be raised to its patched major version because MLflow constrains `cryptography<50`; the constraint is documented for follow-up.
- GraphiQL branding output is escaped and serialized safely to prevent stored-content injection.
- Settings branding endpoints require the administrator role at both route and service layers.
- Session persistence guards prevent concurrent unload writers and stale design-layout data from reverting ontology edits.
- Read-only MCP policy controls are disabled by stable classes rather than tool-name-derived IDs.
- Project rules now require English for comments, docstrings, changelogs, logs, audit-trail text, and traces.

---

## Deployment and Documentation

- Lakebase bootstrap installs or relocates `pgcrypto` in `public`, verifies `digest()`, and documents that extensions are database-scoped.
- Deploy preflight detects missing or misplaced `pgcrypto`.
- Optional Neo4j secrets are no longer forced into the DAB deployment; connections remain configured through Settings → Neo4j.
- Instance-suffixed MCP application names are resolved consistently, and first-deploy Unity Catalog grants run before the Lakebase schema-existence guard.
- The 0.7 → 0.8 Lakebase migration adds the persisted `mcp_policy` column with bootstrap and lazy self-healing support.
- Documentation now covers UI branding, ontology-only domains, MCP domain policy, virtual attributes, graph limits, Lakehouse materialization, inference purging, and the Unity Catalog HTTP connection used to attach OntoBricks MCP to Genie.
- Generated Sphinx HTML is no longer tracked.
- The retired GitHub Pages copy was removed; `https://ontobricks.org/` is the canonical product site.

---

## Bug Fixes (selected)

- Fixed table-comment and schema probes using unsupported `%s` placeholders instead of Databricks SQL qmark bindings.
- Fixed missing MCP service-principal grants on first instance-suffixed deployment.
- Fixed graph-limit fields loading blank and potentially clearing stored overrides on Settings save.
- Fixed relationship direction changes producing duplicate-name errors or stale canvas arrows.
- Fixed Mapping Designer schema-drift checks repeatedly querying the warehouse.
- Fixed stale Analytics results appearing after the graph objects were deleted.
- Fixed New Domain fields being disabled by the previously loaded domain's view-only state.
- Fixed ontology attributes, relationships, inheritance, and other structural edits being restored from stale mirrors after navigation.
- Fixed Lakehouse Build storage labels and a JavaScript scope error on populated graphs.
- Fixed mobile Graph Chat, Data Quality, and Logs panes blocking natural page scrolling.

---

## Upgrade Notes

### Registry migration

Run the standard Lakebase bootstrap or the idempotent `scripts/migrations/upgrade_0.7_to_0.8.sql` migration so the registry `domains` table contains `mcp_policy jsonb NOT NULL DEFAULT '{}'::jsonb`. Deploy preflight validates the migration asset and expected column.

Existing domains require no policy backfill: an empty policy preserves the pre-v0.8 MCP tool and context behavior.

### Graph backend behavior

- Existing domains with no stored backend continue to normalize to Lakebase.
- Newly created domains default to Lakehouse.
- Choose **No Backend** only for ontology-only publication; Mapping, Knowledge Graph, graph API operations, and graph MCP tools are intentionally unavailable.
- Rebuild a Lakehouse domain after changing TABLE/VIEW materialization so the target relation matches the selected mode.

### Lakebase `pgcrypto`

`pgcrypto` must be installed in every Lakebase database used by the graph store, not only the registry database, and should live in the `public` schema so `digest()` is resolvable. Run `make bootstrap-lakebase` or the documented bootstrap script with the correct graph database coordinates.

### Neo4j configuration

The Asset Bundle no longer binds an optional `neo4j-password` secret. Configure named Neo4j connections in Settings → Neo4j using Databricks secret scope/key references.

### MCP publication

Review each domain's new MCP tab after upgrade if you want a reduced tool set or preferred/disabled context. Ontology-only domains automatically force graph tools off. Re-selecting a domain refreshes the visible tool set for the MCP session.

### Branding

Application branding is instance-wide and stored in registry `global_config`. Administrators should use Settings → Configuration → UI; **Default** restores factory branding, while **Reset icon** changes only the draft icon until Save.
