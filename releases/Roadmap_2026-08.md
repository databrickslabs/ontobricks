# OntoBricks — Product Roadmap

> **Version:** 0.7.x → beyond  
> **Last updated:** 2026-08-05  
> **Status:** Living document — updated after each release

> **Disclaimer:** This roadmap represents the current product direction and planned investments as of the date above. It is provided for informational purposes only and is subject to change at any time without notice. The features, timelines, and priorities described here are aspirational and do not constitute a commitment, promise, or legal obligation to deliver any specific functionality by any specific date. Actual releases may differ materially from what is described here.

---

## Executive Summary

OntoBricks is the only Databricks-native knowledge graph builder that combines ontology design, LLM-powered automation, formal reasoning, and interactive graph exploration in a single deployable App. Versions 0.4.0 (Lakebase as primary triple store), 0.5.0 (UX, workflow & governance), 0.6.0 (collaborative comments & AI agents, graph analytics, mapping depth), and 0.7.0 (Neo4j connector) have shipped; **v0.7.0 is the current stable line**.

The next phase of the roadmap focuses on four strategic axes:

1. **Data & mapping integrity hardening** — close two gaps surfaced by real usage: deleting a data source still referenced by a mapping today produces a silent, dangling reference (broken R2RML at build time, no warning at delete time), and metadata refresh applies column changes without a preview (**v0.8.0**).
2. **External surface expansion** — a GraphRAG-inspired retrieval layer for the MCP Server, embedded-dashboard/action/dataset endpoints published on the external API, and a first design pass on exposing draft domains and agentic modeling externally (**v0.8.0**).
3. **Workflow completeness** — close the remaining v0.6.0-deferred UX and automation items (ontology version diff, mapping multi-select & orphan validation, scheduled reasoning, temporal & recursive Datalog) folded into the **v0.8.0** release.
4. **Enterprise hardening** — fine-grained RBAC, multi-workspace federation, audit log, large-graph pagination, and one-command deployment (**v0.9.0**).

---

## Market Context

### Knowledge graph adoption trends

The knowledge graph market is growing rapidly, driven by:

- **AI grounding**: LLMs need structured, governed knowledge bases to avoid hallucinations. Knowledge graphs provide exactly that.
- **Data product thinking**: organizations are shifting from raw tables to versioned, semantic data products — ontologies are the schema layer.
- **Regulatory pressure**: FIBO (finance), CDISC (pharma), HL7 FHIR (healthcare), GDPR/data lineage requirements all push toward formal semantics.
- **Graph-native query demand**: dedicated graph databases are growing — customers want graph traversal without leaving the Lakehouse.

### Where competitors fall short

Every existing solution leaves at least one critical gap for Databricks users:

- **Proprietary ontology platforms** lock organizations into vendor-specific formats (no OWL/W3C standards), carry heavy licensing costs, and require separate infrastructure outside the Lakehouse.
- **Dedicated graph databases** deliver excellent traversal performance but force a data copy out of Unity Catalog, breaking lineage and governance, and adding operational overhead.
- **Managed cloud triple stores** offer SPARQL 1.1 compliance but are tied to a single cloud provider and have no native Databricks or Unity Catalog integration.
- **SQL semantic layers** cover dimensional modeling (metrics, dimensions) but have no concept of OWL ontologies, graph visualization, or formal reasoning.
- **Desktop ontology editors** support OWL design but cannot map entities to Databricks tables, generate SQL, or deploy as a Databricks App.

No existing tool combines ontology design, W3C standards, LLM automation, graph visualization, formal reasoning, unstructured-to-ontology document ingestion, and native Databricks deployment in a single open-source application.

### OntoBricks strategic position

OntoBricks can be positioned as the **semantic layer for the Databricks Lakehouse**: it does not replace graph databases but federates them, allowing enterprises to keep data in Delta/UC while querying through OWL-governed knowledge graphs, optionally persisted to Postgres (Lakebase) or Neo4j — the Neo4j connector shipped in v0.7.0 removed the last major objection for prospects with existing Neo4j deployments. Longer term, fully bridging unstructured sources (documents, PDFs, emails, logs) into the same ontology-governed pipeline remains a backlog aspiration, building on the document-to-markdown groundwork already shipped in v0.5.0 — see **Known limitations** below.

---

## Current State — v0.7.x (August 2026)

### Triple-store / graph backends


| Backend                        | Status | Use case                                                     |
| ------------------------------- | ------ | -------------------------------------------------------------- |
| **Delta Lake (SQL Warehouse)** | GA     | Default; governed, UC-lineage, liquid clustering              |
| **Lakebase (Postgres)**        | GA     | Databricks-native, app-managed or Lakeflow-synced             |
| **Neo4j (Community/Enterprise/AuraDB)** | GA     | Typed property graph; native Cypher traversal; Bolt connectivity |


### Core capabilities

- **Ontology Design** — visual OntoViz canvas, LLM wizard, industry-standard import (FIBO, CDISC, IOF, HL7 FHIR), OWL/RDFS/SKOS/SHACL import/export (replace & append with conflict detection), Business Views, pitfalls detection, neighbourhood highlight, entity search, inheritance toggle
- **Data Mapping** — R2RML generation, LLM auto-map, attribute-level SQL mapping, per-attribute include/exclude, smart Auto-Exclude, always-quoted column names
- **Reasoning** — OWL 2 RL, SWRL, SHACL data quality
- **Knowledge Graph** — Sigma.js exploration, community detection, cohort discovery, bridge navigation, centrality metrics (PageRank, betweenness, degree, closeness, clustering), Data Model Health card, AI interpretation agent
- **Graph engines** — Delta Lake, Lakebase (Postgres), and Neo4j (Community/Enterprise/AuraDB) as interchangeable, per-domain graph backends
- **Graph Chat** — streaming (SSE) natural-language chat over the knowledge graph
- **Collaborative Comments & AI Agents** — domain-scoped discussion threads, task management, AI routing agent dispatching specialized agents (ontology assistant, OWL generator, business rules, icon assigner, auto mapper), outcomes posted back in Discussion
- **Governance & workflow** — version lifecycle (`DRAFT → IN-REVIEW → PUBLISHED`), Validation & Review workspace with per-domain sign-off quorum, build-run tracing, domain-wide audit trail
- **External access** — REST API, auto-generated GraphQL, MCP Server (PUBLISHED-only data plane)
- **Registry** — dual-mode (Volume / Lakebase), scheduler, version management
- **Settings & admin** — App Logs viewer, Registry Access Check, Sidebar user/role panel, Lakebase connection provisioner, Neo4j connection/health/Objects admin
- **Quality engineering** — coverage gates, MCP/contract/property tests, LLM-agent eval harness, ruff + mypy, live & deployed-app e2e
- **Security** — CSRF protection, secure cookies, RBAC via Databricks App permissions

### Known limitations (targeted in next releases)

- A few v0.6.0 workflow items not yet delivered (ontology version diff/iteration, mapping multi-select & orphan validation, scheduled reasoning, temporal & recursive Datalog) — **targeted for v0.8.0**
- Deleting a data source still referenced by a mapping is not blocked or flagged, and metadata refresh applies column changes without a preview — **targeted for v0.8.0**
- MCP Server exposes typed lookups and BFS-style traversal but no semantic/GraphRAG-style retrieval; embedded dashboards, actions, and datasets are not published on the external API; the external API has no concept of draft domains or agentic operations — **targeted for v0.8.0**
- No native, end-to-end unstructured data ingestion pipeline (document → entity extraction → deduplication → knowledge graph); v0.5.0 shipped document-to-markdown conversion feeding the ontology/business-rules agents, but full extraction-to-graph is **unscheduled** pending user feedback in Discussions
- No i18n / localization layer — all UI strings are hardcoded English; **unscheduled**, pending a decision on target languages and scaffolding approach
- No SPARQL federation across multiple domain graphs
- No cross-workspace domain federation

---

## Roadmap

### v0.4.0 — Lakebase as Primary Triple Store (May 2026) — ✅ Delivered

**Theme:** replace the embedded graph engine with Lakebase (Databricks-managed Postgres Autoscaling) as a first-class, production-grade triple store.

#### Key capabilities (delivered)

- **Lakebase GraphDB engine** — Postgres-backed triple store with `app_managed` (direct streaming) and `managed_synced` (Lakeflow UC synced-table pipeline) load modes
- **Managed Sync pipeline** — UC synced-table registration, Lakeflow polling, union-view creation, ghost-state recovery
- **Optimized index layout** — purpose-built indexes covering triple access patterns
- **Transactional reasoning** — OWL 2 RL / SWRL inferred triples land in the build transaction
- **Lakeflow managed-sync** — bulk R2RML movement delegated to a Lakeflow snapshot pipeline
- **Registry OBX export/import**, **Ontology Pitfalls detector**, **HL7 FHIR import**

---

### v0.5.0 — UX, Workflow & Governance (June 2026) — ✅ Delivered

**Theme:** improve day-to-day usability across Graph Chat, Mapping, and Ontology, and add a governed version lifecycle and review workflow.


| Capability                                              | Status      | Notes                                                                                                                                                 |
| ------------------------------------------------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Graph Chat performance**                              | ✅ Delivered | Streaming (SSE) agent loop — live tool-call / token rendering                                                                                         |
| **Mapping — exclude unmapped**                          | ✅ Delivered | Smart **Auto-Exclude** (unmapped + orphans + pure parents) and **Include excluded**                                                                   |
| **Digital Twin publication workflow**                   | ✅ Delivered | `DRAFT → IN-REVIEW → PUBLISHED` lifecycle + Validation & Review workspace, sign-off quorum                                                            |
| **Ontology precision scoring**                          | ✅ Delivered | Precision score + actionable pitfall hints, surfaced in the Domain Cockpit                                                                            |
| **Unstructured data ingestion for Ontology generation** | ✅ Delivered | PDF/Office/image → markdown via `ai_parse_document`, feeding OWL & business-rules agents                                                              |
| **Auto quality rules**                                  | ✅ Delivered | Business-rules generator agent proposes SWRL / decision-table / SPARQL CONSTRUCT / aggregate rules from the ontology + documents, for review & accept |


Also delivered (beyond the original plan):
- Build-run tracing + **Build Analytics** panel and domain-wide **Audit trail**
- Graph/registry **Lakebase separation** (`BranchLakebaseAuth`, in-app *Create Graph DB* provisioner, Permissions tab)
- Business Views overhaul (**New Assistant**, collapse/expand, right-click hide)
- **CNS test foundations** — coverage gates, MCP/contract/property tests, agent eval harness, ruff/mypy, live & deployed-app e2e
- Deploy simplification — single-knob multi-instance, `--dry-run`, hardened `deploy.sh`, owner-run self-healing migrations

---

### v0.6.0 — Collaborative Workflows & Graph Analytics (June 2026) — ✅ Delivered

**Theme:** collaborative domain authoring with AI-driven task routing, graph centrality analytics, and deeper mapping and ontology designer capabilities.


| Capability                                    | Status            | Notes                                                                                                                |
| --------------------------------------------- | ----------------- | --------------------------------------------------------------------------------------------------------------------- |
| **Collaborative Comments & Tasks**            | ✅ Delivered       | Domain-scoped discussion threads on every surface; comments convertible to tasks assigned to teammates or AI agents  |
| **AI Agent as Task Assignee**                 | ✅ Delivered       | Router agent dispatches ontology_assistant, owl_generator, business_rules, icon_assigner, auto_mapper                |
| **Graph Analytics**                           | ✅ Delivered       | PageRank, betweenness, degree, closeness, clustering; Data Model Health card; AI interpretation agent                |
| **Mapping — per-attribute include/exclude**   | ✅ Delivered       | Checkbox column in Status tab; exclusion persisted across unmap/re-map cycles                                        |
| **Append-mode OWL/RDFS import**               | ✅ Delivered       | Two-phase conflict analysis + per-entity resolution; SKOS, alignment, SHACL extended support                         |
| **Ontology Designer improvements**            | ✅ Delivered       | Neighbourhood highlight, inheritance toggle, entity search popup, Business View from right-click                     |
| **App Logs viewer**                           | ✅ Delivered       | Live tail of `ontobricks.log` with level filtering and text search under Settings → Admin                            |
| **Registry Access Check**                     | ✅ Delivered       | Parallel UC + Lakebase probes with exact GRANT SQL to fix failing checks                                             |
| **Sidebar user/role panel**                   | ✅ Delivered       | Connected user email and role badges pinned to the sidebar footer                                                    |
| **Ontology iteration UX**                     | ↪ Moved to v0.8.0 | Compare, diff, promote, and rollback generated ontology versions                                                     |
| **Mapping — multi-select**                    | ↪ Moved to v0.8.0 | Multi-select of entities and relationships in the Mapping canvas                                                     |
| **Mapping — orphan detection**                | ↪ Moved to v0.8.0 | Validate that all mapped entities are connected (no isolated nodes)                                                  |
| **Scheduler — inference & materialization**   | ↪ Moved to v0.8.0 | Trigger OWL 2 RL inference / SWRL materialization as scheduled tasks                                                 |
| **Advanced reasoning — temporal & Datalog**   | ↪ Moved to v0.8.0 | Allen's 13 interval relations + stratified recursive Datalog fixpoint rules                                          |

---

### v0.7.0 — Neo4j Connector (August 2026) — ✅ Delivered

**Theme:** add Neo4j (Community, Enterprise, AuraDB) as a graph engine alongside Delta Lake and Lakebase, enabling customers with existing Neo4j infrastructure to use OntoBricks as their semantic design and mapping front-end.

#### Why this matters

Neo4j is the dominant graph database with 40%+ market share. Customers in finance, healthcare, and telco often have existing Neo4j deployments. A native connector means:

- **No data duplication** — triples are materialized directly into Neo4j as nodes and relationships; no intermediate Delta table needed
- **Native graph queries** — Cypher traversal, shortest path, and graph algorithms run on Neo4j; OntoBricks handles ontology design and mapping
- **Hybrid Lakehouse + graph** — raw data stays in Delta/UC; the knowledge graph lives in Neo4j; OntoBricks bridges both worlds
- **Removed the last objection** for prospects evaluating OntoBricks against a pure graph-DB-plus-ETL approach

#### OWL → Property Graph mapping (delivered typed model)


| OWL concept                | Neo4j representation                                                     |
| --------------------------- | -------------------------------------------------------------------------- |
| Class                       | Node label                                                                |
| Object property             | Relationship type                                                        |
| Datatype property           | Node property                                                            |
| Sub-class                   | Additional label on child node                                           |
| Inferred triple (SWRL/OWL) | Node/relationship with `:Inferred` marker                                |
| Named graph                 | Neo4j database (Enterprise/Aura) or marker label with graceful selector degradation (Community) |


#### Key capabilities (delivered)

- Neo4j 5.x server support (`Neo4jConnection`, `Neo4jStore`, `Neo4jReadOps`/`Neo4jWriteOps`) — typed property-graph writes (nodes + relationships, not flat triples), no APOC required
- Per-domain **Neo4j database selector**, with graceful degradation on Community (single-database) servers
- Reverse-mapped triple reconstruction so Knowledge Graph, GraphQL, and reasoning behave identically across Delta/Lakebase/Neo4j
- Settings → Neo4j: connection config, Bolt/TLS support, Health check tab, and an **Objects** admin view (list/drop every OntoBricks graph in the connected database)
- AuraDB support with automatic connection detection; documented flavor-compatibility matrix (`documentation/neo4j-requirements.md`)
- Optional install — zero impact on Volume-only / Neo4j-less deployments

Also delivered (beyond the original plan):
- ANSI-safe `TRY_CAST` policy across all remaining Spark SQL emitters (SPARQL translation, mapping coverage, Lakeflow companion DDL), with a regression guard preventing regressions to bare `CAST`
- Scheduler CRUD test coverage (43 new tests) for the generic task-type/target-key registration path
- "Keep New Version" ungated on ontology/mapping readiness — branching a new domain version is never blocked by an incomplete ontology or mapping
- Registry stacked-modal UX fix (centering + blur) for Export/Import dialogs
- Top-level navbar icon realignment (Registry, Domain, Knowledge Graph) for visual consistency, with a contract test locking the icon map
- Help Center sync pass — Neo4j coverage, drift fixes against `menu_config.json`, missing guides catalogued

---

### v0.8.0 — Data Integrity, External Surface & Workflow Completeness (October 2026)

**Theme:** three pillars shipped together. First, **data & mapping integrity hardening** — closing gaps in the data-source/mapping lifecycle surfaced by real usage. Second, **external surface expansion** — a GraphRAG-inspired MCP retrieval layer, published dashboard/action/dataset endpoints, and a first design pass on exposing draft domains and agentic modeling externally. Third, **closure of all workflow items deferred from v0.6.0** (ontology version diff, mapping multi-select, orphan detection, scheduled reasoning, temporal & recursive Datalog).

#### Pillar 1 — Data Source & Mapping Integrity


| Capability                              | Description                                                                                                                                                                                                                        | Priority |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| **Data source deletion guard**          | Before removing a metadata table from a domain, cross-check it against every entity/relationship mapping in `assignment`; block or require explicit confirmation (listing the affected entities/relationships) instead of today's silent removal that only surfaces as a broken R2RML/Spark SQL build later | P1       |
| **Metadata refresh — diff preview**     | Surface the column-level diff already computed on "Update Metadata" (columns added/removed/unchanged) as a review-and-confirm step before the merge is applied, instead of applying silently                                        | P1       |
| **Mapping impact warning on source edit** | When a mapped data source's schema changes (columns dropped/renamed), warn in the Mapping designer which entities/attributes are affected, reusing the diff from the item above                                                    | P2       |

#### Pillar 2 — External Surface Expansion


| Capability                                              | Description                                                                                                                                                                                                                     | Priority |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| **Dashboards, actions & datasets on the external API**  | Publish the existing `DashboardService` (embed URLs, parameters, linked datasets) and class-level dataset/action metadata as new routes on `/api/v1`, so external and MCP consumers can retrieve them without a session          | P1       |
| **MCP GraphRAG-style retrieval**                        | Extend the MCP Server beyond typed lookups and BFS traversal with a semantic retrieval layer (embeddings / vector search over graph entities), inspired by the Lakebase Cookbook GraphRAG example — ships through the AI-feature lifecycle gate (SPEC.md + eval dataset) | P2       |
| **External API — draft domains & agentic modeling**     | Design pass on extending the external API beyond published, read-only domains to cover DRAFT-stage domains and agentic operations (auto-map, ontology assistant); requires an auth, rate-limit, and write-safety design before implementation | P3       |

#### Pillar 3 — Automation & Exploration Performance


| Capability                                | Description                                                                                                                                                                                       | Priority |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| **Auto Mapping — live agent trace**       | Stream the auto-mapping agent's existing step-by-step progress over SSE (reusing the Graph Chat streaming pattern), with MLflow tracing, so users see per-entity/relationship reasoning as it happens | P2       |
| **Auto Mapping — parallel execution**     | Investigate safe concurrency for independent entity/relationship mapping sub-tasks (today one serial agentic loop); needs a concurrency-safety design given shared assignment state and LLM rate limits | P3       |
| **Graph Explorer — parallel querying**    | Profile the current synchronous store round-trip to find the actual hot paths (e.g. batched neighbor expansion across multiple nodes) and parallelize those specifically, rather than a blanket rewrite | P3       |

#### Pillar 4 — Ontology & Knowledge Graph Modeling


| Capability                                | Description                                                                                                                                                                                    | Priority |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| **Enumeration attribute type**            | Add a first-class "Enumeration" data type to the ontology attribute editor (today enum-like behavior only exists as a SHACL `sh:in` validation constraint), wired through OWL generation and SHACL shape emission | P2       |
| **SPARQL editor — Knowledge Graph menu**  | A SPARQL editor and backend already exist (`query-execute.js`, `DomainQueryService`); confirm scope with the requester — surface the existing editor under the Knowledge Graph menu (navigation fix), or upgrade it (syntax highlighting/autocomplete) | P2       |

#### Pillar 5 — Platform UX & Localization


| Capability                                    | Description                                                                                                                                                                        | Priority |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| **Unsaved-changes guard beyond the Designer** | Generalize the Ontology Designer's dirty-flag + beacon-save-on-unload pattern to Mapping, Metadata/Data Sources, and Business Rules, which today only guard in-app navigation via confirm dialogs | P2       |
| **Language package foundations**             | No i18n infrastructure exists today; scope as either a translation-key scaffolding pass or a first concrete target language, pending a product decision on which languages/markets to target | P3       |

#### Pillar 6 — Deferred Workflow Items (carried from v0.6.0)


| Capability                                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Priority |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| **Ontology iteration UX**                           | Manage and iterate over generated ontology versions — side-by-side compare, structural diff (added/removed classes, properties, relationships, mappings), promote, and rollback — wired into the `DRAFT → IN-REVIEW → PUBLISHED` lifecycle, with the diff optionally exported to Delta as an audit record on save                                                                                                                                                                                                                                                                                            | P1       |
| **Mapping — multi-select**                          | Multi-select of entities and relationships in the Mapping canvas (shift/ctrl + marquee) so bulk actions (map, exclude, clear) apply to a selection                                                                                                                                                                                                                                                                                                                                                                                                                                                             | P2       |
| **Mapping — orphan detection**                      | Validation pass that flags mapped entities with no relationships (isolated nodes), surfaced as advisory warnings in the Mapping designer and the Cockpit readiness checks                                                                                                                                                                                                                                                                                                                                                                                                                                      | P2       |
| **Scheduler — inference & materialization**         | Extend the scheduler so OWL 2 RL inference and SWRL materialization can run as scheduled tasks alongside the existing build job, with results recorded in the build-run trace                                                                                                                                                                                                                                                                                                                                                                                                                                  | P2       |
| **Advanced reasoning — temporal & recursive rules** | Extend the multi-phase reasoning engine with two new symbolic families: **(1) Temporal reasoning** — Allen's 13 interval relations (before, meets, overlaps, during, …) inferred from entity start/end datatype properties; **(2) recursive Datalog** — stratified, semi-naïve fixpoint rules reusing the SWRL atom syntax for true recursion (e.g. conditional reachability/ancestry) beyond the fixed transitive closure. Shipped as a phased roadmap (temporal first, Datalog second)                                                                                                                     | P2       |

---

### v0.9.0 — Enterprise Hardening (Q4 2026)

**Theme:** prepare OntoBricks for large enterprise deployments with strict governance, performance, and multi-tenancy requirements.


| Feature                        | Description                                                                                |
| ------------------------------- | ---------------------------------------------------------------------------------------------- |
| **Fine-grained RBAC**          | Per-domain, per-version read/write/admin roles via Unity Catalog grants                    |
| **Multi-workspace federation** | Cross-workspace domain registry sync — read a domain built in workspace A from workspace B |
| **Audit log**                  | Every build, reasoning run, and mutation emits a structured event to a Delta audit table   |
| **Large-graph pagination**     | Server-side cursor pagination for 10k+ node knowledge graphs                               |
| **API key authentication**     | Scoped API keys for external REST and GraphQL consumers                                    |
| **One-command deployment**     | Single DAB deploy installs OntoBricks + MCP server + registry together                     |
| **Triple store migration UX**  | Guided migration assistant when switching engine (e.g. Delta → Neo4j or Lakebase)         |

---

### v1.0.0 — General Availability (Q1 2027)

**Theme:** stable API contract, enterprise SLA documentation, and ecosystem integrations.


| Item                          | Description                                                         |
| ------------------------------ | ----------------------------------------------------------------------- |
| **Stable REST API v1**        | SemVer enforced; deprecation policy documented; no breaking changes |
| **Amazon Neptune connector**  | RDF/SPARQL 1.1 over HTTPS                                           |
| **Azure Cosmos DB connector** | Gremlin API; property graph mapping                                 |
| **OntoBricks Hub**            | Public registry of community ontologies and mapping templates       |
| **Databricks Marketplace**    | One-click install from the Databricks Marketplace                   |
| **SSO / SCIM provisioning**   | Enterprise identity integration                                     |

---

## Feature Matrix


| Feature                                          | v0.4 | v0.5 | v0.6 | v0.7 | v0.8 | v0.9 | v1.0 |
| --------------------------------------------------- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Delta Lake triple store                          | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Lakebase named-graph triple store**            | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **UX & workflow improvements**                   | —    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Version lifecycle & review**                   | —    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Auto quality rules**                           | —    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Collaborative comments & AI agents**           | —    | —    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Graph analytics (centrality, health card)**    | —    | —    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Mapping per-attribute include/exclude**        | —    | —    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Append-mode OWL/RDFS/SKOS import**             | —    | —    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Neo4j connector**                              | —    | —    | —    | ✅    | ✅    | ✅    | ✅    |
| **Data source / mapping integrity guards**       | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **External API — dashboards, actions, datasets** | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **MCP GraphRAG-style retrieval**                 | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Enumeration attribute type**                   | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Generalized unsaved-changes guard**            | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Ontology version diff/iteration**              | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Mapping multi-select & orphan check**          | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Scheduled inference / materialization**        | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Temporal & recursive Datalog reasoning**       | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| Fine-grained RBAC                                | —    | —    | —    | —    | —    | ✅    | ✅    |
| Multi-workspace federation                       | —    | —    | —    | —    | —    | ✅    | ✅    |
| API key authentication                           | —    | —    | —    | —    | —    | ✅    | ✅    |
| Amazon Neptune                                   | —    | —    | —    | —    | —    | —    | ✅    |
| Databricks Marketplace                           | —    | —    | —    | —    | —    | —    | ✅    |


---

## Graph Engine Comparison (v0.4+)


| Capability                  | Delta Lake                | Lakebase (v0.4)                 | Neo4j (v0.7)                  |
| ----------------------------- | -------------------------- | ---------------------------------- | -------------------------------- |
| **Storage**                 | Delta table in UC         | Postgres (Lakebase Autoscaling) | Neo4j database or AuraDB      |
| **Query language**          | Spark SQL                 | Postgres SQL + SPARQL subset    | Cypher                        |
| **SPARQL support**          | Via Spark SQL translation | Native                          | Via OntoBricks adapter        |
| **Named graphs**            | Per-domain Delta table    | ✅                               | ✅                             |
| **Transactional reasoning** | Append only                | ✅                               | ✅                             |
| **Multi-hop traversal**     | Recursive CTE (Spark)     | Optimized indexes + CTE         | Native Cypher (best-in-class) |
| **Governance / lineage**    | Full UC lineage            | UC synced table                 | External                      |
| **Deployment**              | Built-in                   | Optional extra                  | Optional extra                |
| **Best for**                | Production, governed data | Databricks-native + SPARQL      | Customers with existing Neo4j |


---

## Open Questions

1. **Data source deletion guard — block vs. warn** — should removing a mapped data source be a hard block, or a confirmation dialog that lists the affected entities/relationships and lets the user proceed? Leaning toward confirm-with-detail, consistent with how other destructive actions (Unmap all, Clear metadata) already behave in the app.
2. **MCP GraphRAG scope** — structured retrieval over the existing ontology graph (extending `describe_entity`/BFS traversal with ranking) vs. classic GraphRAG (unstructured text chunks + embeddings + community summaries). These are materially different builds; needs a SPEC.md before implementation per the AI-feature lifecycle gate.
3. **External API — agentic write operations** — if draft domains and agentic modeling are exposed externally, what's the auth model (scoped API keys land in v0.9.0 — should this pillar wait for that, or ship with a narrower, session-bound external scope first)?
4. **Language package approach** — scaffold a general translation-key pipeline now (higher upfront cost, no immediate payoff) or wait for a concrete target-language requirement and localize incrementally? No committed direction yet.
5. **Lakebase SPARQL subset scope** — BGP + FILTER covers 80% of use cases; OPTIONAL and UNION add another 15%. Aggregates and property paths are deferred to a later patch.
6. **Auto quality rules confidence** — the v0.5.0 business-rules generator is advisory (suggest + review/accept). How aggressively should auto-suggested rules be applied? Auto-apply with confidence thresholds is deferred pending feedback.

---

## How to Contribute

The graph engine abstraction is designed for external contributions. Adding a new store requires implementing the `GraphStore` interface, registering the engine in `GraphDBFactory`, adding an optional dependency group, providing a Settings UI card, and writing unit tests with a mock driver.

See `docs/graphdb-integration.md` for the full engine abstraction contract.

For the **MCP GraphRAG-style retrieval** and **external API redesign** items, this is exactly the kind of feature we want to shape **with** our users. If you have concrete use cases, retrieval patterns, or auth requirements, please share them in the project **Discussions** — your input will directly steer the design and prioritization of this work. The same goes for **unstructured data ingestion** (extraction-to-graph) and **language package** scope, both currently unscheduled pending feedback.
