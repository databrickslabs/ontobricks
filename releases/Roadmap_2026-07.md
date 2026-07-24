# OntoBricks — Product Roadmap

> **Version:** 0.6.x → beyond  
> **Last updated:** 2026-07-01  
> **Status:** Living document — updated after each release

> **Disclaimer:** This roadmap represents the current product direction and planned investments as of the date above. It is provided for informational purposes only and is subject to change at any time without notice. The features, timelines, and priorities described here are aspirational and do not constitute a commitment, promise, or legal obligation to deliver any specific functionality by any specific date. Actual releases may differ materially from what is described here.

---

## Executive Summary

OntoBricks is the only Databricks-native knowledge graph builder that combines ontology design, LLM-powered automation, formal reasoning, and interactive graph exploration in a single deployable App. Versions 0.4.0 (Lakebase as primary triple store), 0.5.0 (UX, workflow & governance), and 0.6.0 (collaborative comments & AI agents, graph analytics, mapping depth) have shipped; **v0.6.0 is the current stable line**.

The next phases of the roadmap focus on four strategic axes:

1. **Graph engine expansion** — add Neo4j (Community, Enterprise, AuraDB) as a graph engine alongside Delta Lake and Lakebase, opening OntoBricks to hybrid Lakehouse + graph deployments (**v0.7.0**).
2. **Unstructured data ingestion** — turn raw documents, PDFs, emails, and transcripts into trustworthy, ontology-governed graph entities through a native extraction and mapping pipeline built on Databricks AI Functions, Vector Search, and Unity Catalog (**v0.8.0**).
3. **Workflow completeness** — close the remaining v0.6.0-deferred UX and automation items (ontology version diff, mapping multi-select & orphan validation, scheduled reasoning, temporal & recursive Datalog) folded into the **v0.8.0** release alongside unstructured ingestion.
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

No existing tool combines ontology design, W3C standards, LLM automation, graph visualization, formal reasoning, **unstructured data ingestion**, and native Databricks deployment in a single open-source application.

### OntoBricks strategic position

OntoBricks can be positioned as the **semantic layer for the Databricks Lakehouse**: it does not replace graph databases but federates them, allowing enterprises to keep data in Delta/UC while querying through OWL-governed knowledge graphs, optionally persisted to Postgres (Lakebase) or Neo4j. With v0.8.0, it will also bridge unstructured sources (documents, PDFs, emails, logs) into the same ontology-governed pipeline — giving organizations a single entry point for both structured and unstructured data into the knowledge graph.

---

## Current State — v0.6.x (July 2026)

### Triple-store backends


| Backend                        | Status | Use case                                          |
| ------------------------------ | ------ | ------------------------------------------------- |
| **Delta Lake (SQL Warehouse)** | GA     | Default; governed, UC-lineage, liquid clustering  |
| **Lakebase (Postgres)**        | GA     | Databricks-native, app-managed or Lakeflow-synced |


### Core capabilities

- **Ontology Design** — visual OntoViz canvas, LLM wizard, industry-standard import (FIBO, CDISC, IOF, HL7 FHIR), OWL/RDFS/SKOS/SHACL import/export (replace & append with conflict detection), Business Views, pitfalls detection, neighbourhood highlight, entity search, inheritance toggle
- **Data Mapping** — R2RML generation, LLM auto-map, attribute-level SQL mapping, per-attribute include/exclude, smart Auto-Exclude, always-quoted column names
- **Reasoning** — OWL 2 RL, SWRL, SHACL data quality
- **Knowledge Graph** — Sigma.js exploration, community detection, cohort discovery, bridge navigation, centrality metrics (PageRank, betweenness, degree, closeness, clustering), Data Model Health card, AI interpretation agent
- **Graph Chat** — streaming (SSE) natural-language chat over the knowledge graph
- **Collaborative Comments & AI Agents** — domain-scoped discussion threads, task management, AI routing agent dispatching specialized agents (ontology assistant, OWL generator, business rules, icon assigner, auto mapper), outcomes posted back in Discussion
- **Governance & workflow** — version lifecycle (`DRAFT → IN-REVIEW → PUBLISHED`), Validation & Review workspace with per-domain sign-off quorum, build-run tracing, domain-wide audit trail
- **External access** — REST API, auto-generated GraphQL, MCP Server (PUBLISHED-only data plane)
- **Registry** — dual-mode (Volume / Lakebase), scheduler, version management
- **Settings & admin** — App Logs viewer, Registry Access Check, Sidebar user/role panel, Lakebase connection provisioner
- **Quality engineering** — coverage gates, MCP/contract/property tests, LLM-agent eval harness, ruff + mypy, live & deployed-app e2e
- **Security** — CSRF protection, secure cookies, RBAC via Databricks App permissions

### Known limitations (targeted in next releases)

- A few v0.6.0 workflow items not yet delivered (ontology version diff/iteration, mapping multi-select & orphan validation, scheduled reasoning, temporal & recursive Datalog) — **targeted for v0.8.0**
- Single graph-DB family (Delta Lake / Lakebase Postgres) — no Neo4j / property-graph export yet — **targeted for v0.7.0**
- No native unstructured data ingestion pipeline (document → entity extraction → knowledge graph) — **targeted for v0.8.0**
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
| --------------------------------------------- | ----------------- | -------------------------------------------------------------------------------------------------------------------- |
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

### v0.7.0 — Neo4j Connector (August 2026)

**Theme:** add Neo4j (Community, Enterprise, AuraDB) as a graph engine alongside Delta Lake and Lakebase, enabling customers with existing Neo4j infrastructure to use OntoBricks as their semantic design and mapping front-end.

#### Why this matters

Neo4j is the dominant graph database with 40%+ market share. Customers in finance, healthcare, and telco often have existing Neo4j deployments. A native connector means:

- **No data duplication** — triples are materialized directly into Neo4j as nodes and relationships; no intermediate Delta table needed
- **Native graph queries** — Cypher traversal, shortest path, and graph algorithms run on Neo4j; OntoBricks handles ontology design and mapping
- **Hybrid Lakehouse + graph** — raw data stays in Delta/UC; the knowledge graph lives in Neo4j; OntoBricks bridges both worlds
- **Removes the last objection** for prospects evaluating OntoBricks against a pure graph-DB-plus-ETL approach

#### OWL → Property Graph mapping


| OWL concept                | Neo4j representation                                    |
| -------------------------- | ------------------------------------------------------- |
| Class                      | Node label                                              |
| Object property            | Relationship type                                       |
| Datatype property          | Node property                                           |
| Sub-class                  | Additional label on child node                          |
| Inferred triple (SWRL/OWL) | Node/relationship with `:Inferred` marker               |
| Named graph                | Neo4j database (Enterprise) or label prefix (Community) |


#### Key capabilities

- Batch node and relationship upsert from the OntoBricks build pipeline
- Typed node label promotion from `rdf:type` triples
- SWRL violation detection via Cypher
- Knowledge Graph visualization sourced from Neo4j via Bolt
- Health-check and connection status in the Settings UI
- AuraDB support with automatic connection string detection
- Optional install — zero impact on Volume-only deployments

---

### v0.8.0 — Unstructured Data & Workflow Completeness (October 2026)

**Theme:** two complementary pillars shipped together. First, a **native unstructured-data ingestion pipeline** that turns raw documents, PDFs, emails, transcripts, and logs into governed, ontology-mapped graph entities — built on Databricks AI Functions, Vector Search, and Unity Catalog, and fully consistent with the existing mapping and reasoning machinery. Second, **closure of all workflow items deferred from v0.6.0** (ontology version diff, mapping multi-select, orphan detection, scheduled reasoning, temporal & recursive Datalog), keeping the platform feature-complete against its original scope.

#### Pillar 1 — Unstructured Data Ingestion

Building a trustworthy unstructured ingestion pipeline is a genuinely hard problem: entity/relation extraction quality, chunking and grounding, deduplication and entity resolution against existing instances, provenance and confidence tracking, cost at scale, and keeping the whole thing governed under Unity Catalog. v0.5.0 shipped a first step — document-to-markdown conversion via `ai_parse_document` feeding the OWL and business-rules agents — and v0.8.0 completes the picture end-to-end.

The design leans on existing Databricks platform capabilities rather than reinventing them, so unstructured ingestion stays consistent with the structured path and avoids a parallel, ungoverned pipeline.


| Capability                                     | Description                                                                                                                                                                                                        | Priority |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| **Document ingestion connector**               | Auto Loader / Lakeflow Declarative Pipeline integration to land raw files (PDF, Office, text, email, image) into a UC Volume, convert to markdown with `ai_parse_document`, and stage for extraction              | P1       |
| **AI entity & relation extraction**            | `ai_extract` / `ai_gen` SQL functions to pull structured entity attributes and relationship candidates from text chunks; configurable extraction prompt anchored to the domain ontology's class/property vocabulary | P1       |
| **Entity resolution & deduplication**          | Vector Search-based semantic matching against existing graph instances; configurable confidence threshold; flagging new vs merged candidates for user review                                                        | P1       |
| **Unstructured → Mapping canvas integration**  | Extracted entities and relationships surfaced as candidate mappings in the existing Mapping Designer; accepts, rejects, and manual edits follow the same UX as structured mappings                                 | P1       |
| **Provenance & confidence tracking**           | Every triple derived from unstructured content carries a `prov:wasAttributedTo` (source document URI), `prov:generatedAtTime`, and a confidence score stored in the Lakebase triple metadata                       | P2       |
| **Unstructured ingestion scheduler**           | Schedule document ingestion + extraction jobs alongside existing build tasks; results recorded in build-run trace                                                                                                   | P2       |
| **Unity Catalog lineage for unstructured data** | Source document registered as a UC data product; lineage from document Volume → extracted entities → graph triples visible in the UC Lineage UI                                                                    | P2       |

#### Pillar 2 — Deferred Workflow Items (carried from v0.6.0)


| Capability                                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Priority |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| **Ontology iteration UX**                           | Manage and iterate over generated ontology versions — side-by-side compare, structural diff (added/removed classes, properties, relationships), promote, and rollback — wired into the `DRAFT → IN-REVIEW → PUBLISHED` lifecycle                                                                                                                                                                                                                                                                                                                                                                               | P1       |
| **Mapping — multi-select**                          | Multi-select of entities and relationships in the Mapping canvas (shift/ctrl + marquee) so bulk actions (map, exclude, clear) apply to a selection                                                                                                                                                                                                                                                                                                                                                                                                                                                             | P2       |
| **Mapping — orphan detection**                      | Validation pass that flags mapped entities with no relationships (isolated nodes), surfaced as advisory warnings in the Mapping designer and the Cockpit readiness checks                                                                                                                                                                                                                                                                                                                                                                                                                                      | P2       |
| **Scheduler — inference & materialization**         | Extend the scheduler so OWL 2 RL inference and SWRL materialization can run as scheduled tasks alongside the existing build job, with results recorded in the build-run trace                                                                                                                                                                                                                                                                                                                                                                                                                                  | P2       |
| **Advanced reasoning — temporal & recursive rules** | Extend the multi-phase reasoning engine with two new symbolic families: **(1) Temporal reasoning** — Allen's 13 interval relations (before, meets, overlaps, during, …) inferred from entity start/end datatype properties; **(2) recursive Datalog** — stratified, semi-naïve fixpoint rules reusing the SWRL atom syntax for true recursion (e.g. conditional reachability/ancestry) beyond the fixed transitive closure. Shipped as a phased roadmap (temporal first, Datalog second)                                                                                                                     | P2       |

---

### v0.9.0 — Enterprise Hardening (Q4 2026)

**Theme:** prepare OntoBricks for large enterprise deployments with strict governance, performance, and multi-tenancy requirements.


| Feature                        | Description                                                                                |
| ------------------------------ | ------------------------------------------------------------------------------------------ |
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
| ----------------------------- | ------------------------------------------------------------------- |
| **Stable REST API v1**        | SemVer enforced; deprecation policy documented; no breaking changes |
| **Amazon Neptune connector**  | RDF/SPARQL 1.1 over HTTPS                                           |
| **Azure Cosmos DB connector** | Gremlin API; property graph mapping                                 |
| **OntoBricks Hub**            | Public registry of community ontologies and mapping templates       |
| **Databricks Marketplace**    | One-click install from the Databricks Marketplace                   |
| **SSO / SCIM provisioning**   | Enterprise identity integration                                     |

---

## Feature Matrix


| Feature                                          | v0.4 | v0.5 | v0.6 | v0.7 | v0.8 | v0.9 | v1.0 |
| ------------------------------------------------ | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
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
| **Ontology version diff/iteration**              | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Mapping multi-select & orphan check**          | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Scheduled inference / materialization**        | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Temporal & recursive Datalog reasoning**       | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Unstructured data ingestion pipeline**         | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| **Provenance & confidence for unstructured**     | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| Fine-grained RBAC                                | —    | —    | —    | —    | —    | ✅    | ✅    |
| Multi-workspace federation                       | —    | —    | —    | —    | —    | ✅    | ✅    |
| API key authentication                           | —    | —    | —    | —    | —    | ✅    | ✅    |
| Amazon Neptune                                   | —    | —    | —    | —    | —    | —    | ✅    |
| Databricks Marketplace                           | —    | —    | —    | —    | —    | —    | ✅    |


---

## Graph Engine Comparison (v0.4+)


| Capability                  | Delta Lake                | Lakebase (v0.4)                 | Neo4j (v0.7)                  |
| --------------------------- | ------------------------- | ------------------------------- | ----------------------------- |
| **Storage**                 | Delta table in UC         | Postgres (Lakebase Autoscaling) | Neo4j database or AuraDB      |
| **Query language**          | Spark SQL                 | Postgres SQL + SPARQL subset    | Cypher                        |
| **SPARQL support**          | Via Spark SQL translation | Native                          | Via OntoBricks adapter        |
| **Named graphs**            | Per-domain Delta table    | ✅                               | ✅                             |
| **Transactional reasoning** | Append only               | ✅                               | ✅                             |
| **Multi-hop traversal**     | Recursive CTE (Spark)     | Optimized indexes + CTE         | Native Cypher (best-in-class) |
| **Governance / lineage**    | Full UC lineage           | UC synced table                 | External                      |
| **Deployment**              | Built-in                  | Optional extra                  | Optional extra                |
| **Best for**                | Production, governed data | Databricks-native + SPARQL      | Customers with existing Neo4j |


---

## Open Questions

1. **Unstructured extraction quality** — `ai_extract` quality varies by document type and model. Should OntoBricks enforce a confidence floor before adding triples, or always ingest and flag low-confidence triples for review? User feedback in Discussions will steer this.
2. **Entity resolution strategy** — semantic Vector Search matching is fast but approximate; exact string deduplication is cheaper. The right mix is use-case dependent. We plan to ship both and make the strategy configurable per domain.
3. **Lakebase SPARQL subset scope** — BGP + FILTER covers 80% of use cases; OPTIONAL and UNION add another 15%. Aggregates and property paths are deferred to a later patch.
4. **Neo4j Community vs Enterprise** — named graphs as separate databases require Neo4j Enterprise. Community edition support will use label prefixing as a documented workaround.
5. **Auto quality rules confidence** — the v0.5.0 business-rules generator is advisory (suggest + review/accept). How aggressively should auto-suggested rules be applied? Auto-apply with confidence thresholds is deferred pending feedback.

---

## How to Contribute

The graph engine abstraction is designed for external contributions. Adding a new store requires implementing the `GraphStore` interface, registering the engine in `GraphDBFactory`, adding an optional dependency group, providing a Settings UI card, and writing unit tests with a mock driver.

See `docs/graphdb-integration.md` for the full engine abstraction contract.

For **unstructured data ingestion**, this is exactly the kind of feature we want to shape **with** our users. If you have concrete use cases, source types, or requirements, please share them in the project **Discussions** — your input will directly steer the design and prioritization of this work.
