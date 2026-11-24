# OntoBricks — Release Notes V0.10.0

**Release window:** October 22 – November 20, 2026<br>
**Test status:** all changes shipped with the non-scenario suite green: 11091 passed, 334 skipped, 8 deselected (up from 9518 passed on the first v0.10.0 change).

---

## Highlights

- **Intelligence workspace** — a new top-level **Intelligence** menu groups graph analysis into **Configuration**, **Discovery**, and **Simulation**: Communities, Cohorts, Patterns, and what-if Sandbox scenarios, with Simulation, Community, and Pattern run history under **Settings → Runs**.
- **What-if Simulation sandbox** — assert hypothetical facts on a sandbox copy of a graph slice, re-run reasoning there, and compare **Live vs Sandbox** before and after. The live graph is never written. Sandboxes work on Lakebase, Lakehouse Delta, and Neo4j.
- **Pattern detection** — discover recurring typed neighbourhoods for an entity type on a dedicated serverless job, then save, reopen, and explore them with layered diagrams, KPIs (Patterns, Entities, Coverage), and a Matching-entities modal.
- **SHACL-AF rule engine** — business rules now execute as canonical SHACL-AF rules (`sh:TripleRule` / `sh:SPARQLRule`) while keeping SQL-at-scale execution. The editor offers two rule families, **Simple** (IF/THEN) and **Complex** (SPARQL CONSTRUCT), with a fail-closed SPARQL-to-SQL compiler and a conversational SPARQL assistant.
- **Ontology Sanity Checks** — advisory schema lint (integrity, hygiene, naming, hierarchy) with click-through to the offending entity or attribute and a propose-then-apply **Auto-fix**.
- **JSON-LD interoperability** — OWL export/import, SPARQL `CONSTRUCT`/`DESCRIBE`, and the MCP `describe_ontology` tool can use W3C JSON-LD next to Turtle.
- **Genie Pages and Documents** — browse and select Unity Catalog Knowledge Pages under **Context → Pages** (search-first Genie picker) and feed them into **Ontology → Generate**.
- **Home as the Registry** — the Home page is now the Registry (Browse · Bridges · My tasks), with a feature selector and per-workspace Home pages made of launch tiles.
- **Databricks-style chrome** — unified navbar, Activity panel, user menu, feature selector, Du Bois tabs, buttons, and kebab menus, with configurable Create / Share button colours.
- **Graph backend cloisonnement** — the graph database layer is split into a shared read root, a materialized hat, and capability interfaces per engine family (Delta, Lakebase, Neo4j).
- **Context terminology** — the user-facing "Domain" is now **Context**. Routes, API fields, and stored formats are unchanged.
- **Faster Context open** — loads land on **Context → Home**, skip simulations and OWL generation, and hydrate the Configuration section lazily.
- **Dependabot security floors** — Pillow, certifi, authlib, dnspython, msgpack, nltk, pyasn1, graphql-core, joserfc, oauthlib, and opentelemetry-api raised to patched releases.

This release is additive on v0.9.x for data and external contracts. Several UI surfaces were removed or moved; see **Upgrade Notes**.

---

## Intelligence workspace

### Menu and navigation

- **Simulation → Intelligence.** The top-level menu is **Intelligence** (`/intelligence/`), organised as **Configuration / Discovery / Simulation**. Backend packages in `back/core/graph_analysis` are split by analysis type (community, cohort, pattern, metrics). The old `/simulation/` page URL now returns 404; external contracts (`/api/v1/simulations`, MCP tools) keep their names.
- **Cohorts and Runs** share one page with **Design / Runs** tabs; Analytics moved under Intelligence.
- **Communities, Patterns, and Scenarios** each have a **How it works** tab with business-friendly diagrams and a **Discuss** button.
- Write CTAs are disabled in read-only mode.
- The Help Center, Starter Guide, Sphinx docs, and User Guide cover every Intelligence feature.

### What-if Simulation (Sandbox)

- **Scenarios and workbench.** Scenarios list and editor share one URL; the workbench has **General**, **Hypothetical facts**, and **Reasoning** tabs, with Results shown on the workbench. Scope can be an entity neighbourhood (with a search picker) or a cohort; individual inference rules can be ticked per scenario.
- **Hypothetical facts.** A fact wizard scopes subject, predicate, and value to the selected entity type, previews the outcome triple, and shows operation and type icons. Facts can be edited, multi-selected, and purged.
- **Propose facts agent.** A natural-language agent (**Describe the facts**) proposes facts, streams its steps into the workbench, appends to existing facts, and remembers the last prompt on the scenario.
- **Explorer overlay.** Open a sandbox in Graph Explorer as the same view retargeted at **Live UNION sandbox**; Explorer defaults to the live inferred graph except for deep links.
- **Reasoning in the sandbox.** Ticked business rules fire after hypotheses, including entity-scoped rules and claim-removal facts generated from live relationships.
- **Compare → Evaluate.** A **Business insights** agent compares the Live and Sandbox answers against the scenario's fact changes and writes four short business sections; a printable report is available.
- **Public Simulation API.** `/api/v1/simulations` offers scenario CRUD and run, and every query surface (SPARQL playground, Knowledge Graph reads, MCP) can use a sandbox as a graph source. Simulation endpoints are listed under **Settings → API**.
- **Lifecycle.** Scenarios and runs persist with the context version (branch, delete, GitHub/OBX export), runs appear in Background Tasks, and Lakebase sandbox objects are grouped under the context card and deleted as a block.

### Patterns and Communities

- **Pattern detection** runs on a dedicated serverless DAB job (`<app>-pattern-detection`) with egocentric signatures, attribute bins, `min_support`, automatic signature detail levels, and forward-only walks. Detect and catalog save run as one background task and auto-save the run.
- **Details view.** Pattern picker with ordinal names, run-level KPIs (Patterns, Entities, Coverage against the true type population), a per-pattern bar chart showing each bar's share of the population, and a full-height layered neighbourhood diagram with hover lineage highlight, relationship tooltips (min/max/count/avg), and a Matching entities modal linked to Graph Explorer.
- **Saved patterns** mirror the Scenarios list, snapshot KPIs and diagrams, and reopen in Discover.
- **Community detection** adds a preview with entity label, type, and class icon, detection duration, Purge and Materialize actions, and run history under **Settings → Automation → Runs**.

---

## Business rules and reasoning

- **Engine swap.** SWRL execution is replaced by SHACL-AF rules. Authoring, the stored model, provenance (`swrl:<rule>`), and result stats are preserved; SQL execution on Delta and Lakebase stays byte-for-byte compatible.
- **Naming.** SWRL naming is removed from the UI, docs, code, and routes in favour of **IF-THEN**; legacy persisted keys and `swrlb:` prefixes are still read.
- **Two rule families.** Decision Tables and Aggregate rules are removed. The former If-Then family is **Simple**; the former SPARQL family is **Complex**.
- **Set-attribute consequents.** IF/THEN rules can set data-property values; numeric literals are materialised correctly on Postgres Lakebase.
- **Complex rules.** A compiler built on rdflib's SPARQL algebra accepts an explicit subset and refuses anything else, on save and at run time (no more silent zero-triple results). Typed SPARQL value `FILTER`s compile to SQL (#210).
- **SPARQL assistant.** A conversational agent builds Complex rules with Graph-chat look and feel, a draft view, and fullscreen mode.
- **Editor polish.** Collapsible generated-code panes; Explorer shows IF/THEN text instead of raw rule code; business-rule, Data Quality, and scenario lists share one card style.

---

## Ontology and Mapping

### Sanity Checks

- **Design checks** `DC-E01` to `DC-I05` run on the session ontology and surface advisory counts on the Context Home.
- **Orphan attributes** can be attached to or removed from an entity from the findings list.
- **Auto-fix** proposes changes, then applies them on confirmation. Wording uses entity / attribute.
- Sanity Checks moves to the end of the **Design** group of the Ontology menu.

### Ontology Studio

- **Business Views and groups.** OntoViz is removed; Studio (D3) is the only canvas and owns Business Views (View dropdown, rename) and group collapse/expand with exact layout preservation. Groups are managed from the Studio context menu with exclusive membership, coloured badges, and a minus badge on expanded members.
- **IDs and URIs.** "Name" labels are renamed "ID"; renaming an entity or relationship re-derives its URI (DC-E06). Studio entity import keeps source URIs.
- **Hierarchy instance IRI scope.** An opt-in setting mints root-class URIs so a parent class and its subclass mapped with the same business id land on one graph node (#228).
- **Multiple datasets per class.** A class can map to several Unity Catalog datasets, including metric views.
- **Auto-Map** teaches the agent to emit `MEASURE()` and `GROUP BY` for Unity Catalog metric views, with a submit-time guard, and is proposed when Build mapping is not ready.

### Mapping moves under Graph

- The top-level **Mapping** menu is removed. **Graph → Build → Mapping** hosts the Mapping Studio with **Visual**, **List**, and **Informations** tabs, an **Import R2RML** button, Batch Auto-Map in a modal, and a Mapping Diagnostics popup. The Spark SQL page is removed and legacy `/mapping/` URLs redirect to `/dtwin/?section=…`.
- Hidden entities and relationships are kept when unmapping; ghost "Not Mapped" paths no longer appear on the Visual diagram.

### Genie Pages and Documents

- **Context → Pages** lets operators search Genie Knowledge Pages, preview them in a modal (rendered as HTML), and persist the selection on the context. Ontology → Generate adds a **Pages** tab where each page can be unselected, and the Generate detect step allowlists the selected pages.
- **Context → Knowledge Store** is renamed **Documents** and lists supported import types.
- **Add Data Assets** replaces three stacked popups with one dialog (catalog, schema, tables, Import). Already-loaded assets have no checkbox and the action becomes Refresh.

### Interoperability

- **JSON-LD** is an opt-in format for `GET /export-owl?format=json-ld`, `output_format=json-ld` on SPARQL `CONSTRUCT`/`DESCRIBE`, the external `/api/v1` query model, and MCP `describe_ontology`. Default Turtle and tabular behaviour is unchanged.
- **Ontology Generate.** Paged Stage-3 completion is hardened against truncation loops (issue #173), with constant-size page requests, duplicate-page stall detection, user-visible paging failures, and a large-schema stress harness.

---

## Graph, Build, and Explorer

- **Build page.** New **Runs**, **Overrides**, and **Cache** tabs. **Overrides** shows a breakdown of materialized inferred triples with **Purge**; **Cache** shows status, companions, and an Activate link. A header **⋮** menu holds **Purge inferences** and **Refresh cache**; the header Refresh button is removed.
- **Graph DB cloisonnement.** The god-class `GraphDBBackend` is replaced by a read root (`GraphBackend`), a `MaterializedGraphBackend` hat, and capability interfaces per family, with a guardrail test and updated architecture docs. `DeltaFlatStore` is split into materialized and virtual stores.
- **Explorer.** Entity behaviour is grouped into one section, a groups toggle is added, canvas toggles move under **View** and **Graph**, and neighbourhoods stay connected when the entity cap hits.
- **Search and chat.** Viewers can search the graph and use Graph Chat; Graph Chat shows the full, collapsible thinking process.
- **Data Quality.** Bulk Data Quality uses a VIEW-sourced snapshot, and entity checks show a live, scrollable activity log. Lakebase quality SQL stays on the Build warehouse.
- **Settings → Lakebase & Lakehouse** groups the `__smb_`, `__smo_`, and `_entity_search_asserted` objects.

---

## Home, navigation, and chrome

- **Home is the Registry.** One card with **Browse · Bridges · My tasks**; the navbar Registry popup and icon are removed. Launch tiles (Databricks Jobs create-tile style) show live counts and open an 80% Registry modal; the Home page also carries Help, Website, and GitHub resources and a *Databricks Labs project* credit.
- **Workspace Home pages** for Context, Ontology, Graph, and Intelligence group tiles by sidebar section.
- **Feature selector** (Databricks-style app switcher) replaces the workspace dropdown rail. **Settings** moves into the user menu, and **Activity** combines tasks and notifications into one panel.
- **Navbar.** Unified chrome bar, Databricks-style user avatar, name and email header, Help and Report issue in the user menu, and Save / Close / Resume editing styled as Genie Code chips. The Settings gear and the SQL Warehouse status icon are removed.
- **Context menu.** Governance (Versions, Publication), Collaboration (Audit trail), Metadata (Unity Catalog, Genie Pages); Genie and Unity Catalog use official icons.
- **Context loading.** Loads land on **Context → Home**; the loading overlay persists across open, create, and validate navigation; Configuration data hydrates per section with spinners.
- **Versions list** is redesigned in the Intelligence / Patterns card style with a left status badge, and the workspace map combines Switch Version.
- **Design system.** Du Bois tabs, buttons, and kebab menus (shared `.ob-dbx-menu` component), reduced corner rounding, tighter spacing, and configurable **Create** (filled) and **Share** (outlined) colours under **Settings → Configuration → UI → Theme**.
- **Dates** display as `YYYY/MM/DD HH:MM:SS` everywhere in the UI. The JSON API is unchanged.

---

## Registry, identity, and MCP

- **Registry import consistency (#229).** Renamed contexts rewrite namespaces fully, Published copies import as Draft, existing versions are skipped unless overwritten, and **Add missing versions** is exposed in the UI.
- **Every version on the external API.** API, MCP, and GraphQL can target DRAFT, IN-REVIEW, and PUBLISHED versions.
- **MCP end-user identity (#208).** The MCP app forwards the end-user identity past the Apps proxy overwrite, so graph tools authorise the real caller instead of the app service principal. MCP graph tools no longer need a sticky session.
- **Graph Chat `max_tokens`** fix ported from PR #223.

---

## Documentation

- `docs/user-guide.md` — full Intelligence coverage, Purge inferences in the Build ⋮ menu, Load lands on Context → Home.
- `docs/intelligence.md` and the Sphinx Intelligence guide — operator guide for Communities, Cohorts, Patterns, and Simulation.
- `docs/architecture.md` and `.cursor` rules — graph backend cloisonnement and the minimal-surface checklist for agents.
- `docs/mcp.md` — Genie One connection setup delegated to official Databricks documentation, plus MCP example scripts moved to `resources/examples`.
- Help Center, Starter Guide, `docs/api.md`, and `README.md` (Ontology Studio screenshot) refreshed; third-party licenses updated; `CONTRIBUTORS.md` credits @rickynho-rccl.
- `docs/` removes the undocumented `registry_transfer` CLI (issue #227).
- The frontend rules (`.cursor/11-frontend-design.mdc`) document the shared kebab menu, sidebar section card layout, and the Databricks button styles.

---

## Bug Fixes (selected)

- Fixed the ontology Generate truncation loop in paged Stage-3 completion (#173).
- Fixed SWRL / IF-THEN built-in atoms over datatype properties in inference, materialization, and Lakebase.
- Fixed the Studio Group and View dropdown edit-pencil border alignment.
- Fixed Explorer sandbox load SQL, stuck simulation progress at 30%, and the Save lock after a stuck run.
- Fixed pattern detection failing on Spark ANSI casts and returning "0 patterns".
- Fixed the Analytics ranked chart crashing by bucketing the metric series on the warehouse.
- Fixed a shared Contract wrongly inventing a reverse `hasContract` relationship in patterns.
- Fixed numeric set-attribute literals for Postgres Lakebase.
- Fixed collapsed Unity Catalog FQNs in mapping SQL.
- Fixed the Mapping Visual spinner that never stopped.
- Fixed the sidebar expand-then-collapse flash on page load.
- Fixed the Home `Illegal invocation` error from Bootstrap Tab on a button group.
- Fixed `uv.lock` package URLs by rewriting them onto the public CDN.

---

## Security (Dependabot)

Raised dependency floors to patched releases for Pillow, certifi, authlib, dnspython, msgpack, nltk, pyasn1, graphql-core, joserfc, oauthlib, and opentelemetry-api, and aligned `uv.lock` with the public package index.

---

## Upgrade Notes

### Removed or moved features

- **Intelligence URL.** `/simulation/` returns 404; use `/intelligence/`. `/api/v1/simulations` and the MCP tools are unchanged.
- **Mapping menu.** The top-level Mapping menu and the Spark SQL page are gone. `/mapping/` URLs redirect into Graph.
- **Decision Tables and Aggregate rules** are removed. Saved data is no longer read or written but older versioned files are left untouched.
- **Ontology Pitfalls** is removed, with its REST routes, the `check_pitfalls` / `check_owl_pitfalls` agent tools, and the optional `[pitfalls]` extra. `sentence-transformers`, NLTK, torch, and transformers are no longer pulled by that extra.
- **OntoViz and the Groups sidebar** are removed; use Ontology Studio.
- **Registry popup and navbar Registry icon** are removed; use Home.
- **Studio Export (OWL)** toolbar button is removed.

### Naming and behaviour

- **Context vs Domain.** Only user-visible text changed. Routes, `domain_name` fields, the GitHub `domains/<Domain>/` layout, MCP tools, and agent prompts are unchanged.
- **SWRL naming.** New writes use IF-THEN names; legacy persisted keys and `swrlb:` prefixes are still accepted on read.
- **Load navigation.** Open, Load, Registry Load, and Versions reload now land on **Context → Home** (previously **Context → General**).
- **Backend class names.** Graph backend classes are renamed (`GraphDBBackend` → `GraphBackend`, Lakebase/Neo4j prefixes dropped). No compatibility shims; update any custom code that imports them.
- **Dates** in the UI use `YYYY/MM/DD HH:MM:SS` in browser local time.

### Lakebase bootstrap

Run the Lakebase bootstrap on upgrade (the standard `make deploy` does this). It now creates the `community_runs` and `pattern_runs` tables and their indexes explicitly; the app still creates them lazily if the bootstrap is skipped.

### Pattern detection job

Pattern detection runs on a dedicated serverless job `<app>-pattern-detection`, deployed by the bundle and gated by the same analytics toggle as Community detection.

### Databricks Apps deployment

`user_api_scopes` now declares only `sql`. Databricks Apps rejects `iam.current-user:read` and `iam.access-control:read` when listed explicitly, because they are platform defaults. Fresh instances now deploy cleanly; the platform still applies the identity defaults automatically.

### Hierarchy instance IRI scope

The new scope is opt-in and off by default. Enabling it regenerates R2RML for the context, and rebuilding the graph is required for parent and subclass instances to merge.

### Simulation sandboxes

Sandbox objects (`__smb_`, `__smo_`, simulation views and marker graphs) are listed under Settings → Lakebase & Lakehouse, where each sandbox block can be dropped in a single action.
