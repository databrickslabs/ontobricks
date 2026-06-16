# OntoBricks — Product Roadmap

> **Version:** 0.5.x → beyond  
> **Last updated:** 2026-06-09  
> **Status:** Living document — updated after each release

> **Disclaimer:** This roadmap represents the current product direction and planned investments as of the date above. It is provided for informational purposes only and is subject to change at any time without notice. The features, timelines, and priorities described here are aspirational and do not constitute a commitment, promise, or legal obligation to deliver any specific functionality by any specific date. Actual releases may differ materially from what is described here.

---

## Executive Summary

OntoBricks is the only Databricks-native knowledge graph builder that combines ontology design, LLM-powered automation, formal reasoning, and interactive graph exploration in a single deployable App. Versions 0.4.0 (Lakebase as primary triple store) and 0.5.0 (UX, workflow & governance) have shipped; **v0.5.x is the current stable line**.

The next phases of the roadmap focus on three strategic axes:

1. **Workflow & quality completeness** — close the remaining UX and automation items scoped for the 0.5 line (ontology version diff/iteration, mapping multi-select & orphan validation, scheduled reasoning) in a fast-follow **v0.6.0** release.
2. **Continued UX & workflow improvements** — keep improving day-to-day usability across Graph Chat, Mapping, Ontology, and Scheduler based on direct user feedback.
3. **Neo4j connector** — extend the pluggable graph engine to the industry's leading native graph database, opening OntoBricks to hybrid Lakehouse + graph deployments (v0.7.0).

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

No existing tool combines ontology design, W3C standards, LLM automation, graph visualization, formal reasoning, and native Databricks deployment in a single open-source application.

### OntoBricks strategic position

OntoBricks can be positioned as the **semantic layer for the Databricks Lakehouse**: it does not replace graph databases but federates them, allowing enterprises to keep data in Delta/UC while querying through OWL-governed knowledge graphs, optionally persisted to Postgres (Lakebase) or Neo4j.

---

## Current State — v0.5.x (June 2026)

### Triple-store backends


| Backend                        | Status | Use case                                          |
| ------------------------------ | ------ | ------------------------------------------------- |
| **Delta Lake (SQL Warehouse)** | GA     | Default; governed, UC-lineage, liquid clustering  |
| **Lakebase (Postgres)**        | GA     | Databricks-native, app-managed or Lakeflow-synced |


### Core capabilities

- **Ontology Design** — visual OntoViz canvas, LLM wizard, industry-standard import (FIBO, CDISC, IOF, HL7 FHIR), OWL/RDFS import/export, Business Views (curated subgraphs, New Assistant, collapse/hide), pitfalls detection
- **Data Mapping** — R2RML generation, LLM auto-map, attribute-level SQL mapping, smart Auto-Exclude
- **Reasoning** — OWL 2 RL, SWRL, SHACL data quality
- **Knowledge Graph** — Sigma.js exploration, community detection, cohort discovery, bridge navigation
- **Graph Chat** — streaming (SSE) natural-language chat over the knowledge graph
- **Governance & workflow** — version lifecycle (`DRAFT → IN-REVIEW → PUBLISHED`), Validation & Review workspace with per-domain sign-off quorum, build-run tracing, domain-wide audit trail
- **External access** — REST API, auto-generated GraphQL, MCP Server (PUBLISHED-only data plane)
- **Registry** — dual-mode (Volume / Lakebase), scheduler, version management
- **Quality engineering** — coverage gates, MCP/contract/property tests, LLM-agent eval harness, ruff + mypy, live & deployed-app e2e
- **Security** — CSRF protection, secure cookies, RBAC via Databricks App permissions

### Known limitations (targeted in next releases)

- A few 0.5-line workflow items not yet delivered (version diff, mapping multi-select & orphan validation, scheduled reasoning) — **targeted for v0.6.0**
- Single graph-DB family (Delta Lake / Lakebase Postgres) — no Neo4j / property-graph export yet
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

#### Stabilization (delivered)


| Item                                                                  | Type     | Priority | Status |
| --------------------------------------------------------------------- | -------- | -------- | ------ |
| Fix pre-existing test failures                                        | Bug      | P0       | ✅      |
| Security dependency updates                                           | Security | P0       | ✅      |
| Support non-HTTP(S) URIs in Lakebase reasoning materialization        | Bug      | P1       | ✅      |
| Cohort discovery: persist rule library to registry (not session-only) | Feature  | P1       | ✅      |
| E2E tests for Build → Knowledge Graph happy path                      | Test     | P2       | ✅      |


---

### v0.5.0 — UX, Workflow & Governance (June 2026) — ✅ Delivered

**Theme:** improve day-to-day usability across Graph Chat, Mapping, and Ontology, and add a governed version lifecycle and review workflow.


| Capability                                              | Status            | Notes                                                                                                                                                 |
| ------------------------------------------------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Graph Chat performance**                              | ✅ Delivered       | Streaming (SSE) agent loop — live tool-call / token rendering                                                                                         |
| **Mapping — exclude unmapped**                          | ✅ Delivered       | Smart **Auto-Exclude** (unmapped + orphans + pure parents) and **Include excluded**                                                                   |
| **Digital Twin publication workflow**                   | ✅ Delivered       | `DRAFT → IN-REVIEW → PUBLISHED` lifecycle + Validation & Review workspace, sign-off quorum                                                            |
| **Ontology precision scoring**                          | ✅ Delivered       | Precision score + actionable pitfall hints, surfaced in the Domain Cockpit                                                                            |
| **Unstructured data ingestion for Ontology generation** | ✅ Delivered       | PDF/Office/image → markdown via `ai_parse_document`, feeding OWL & business-rules agents                                                              |
| **Auto quality rules**                                  | ✅ Delivered       | Business-rules generator agent proposes SWRL / decision-table / SPARQL CONSTRUCT / aggregate rules from the ontology + documents, for review & accept |
| **Ontology iteration UX**                               | ↪ Moved to v0.6.0 | Compare, diff, promote, and rollback generated ontology versions                                                                                      |
| **Mapping — multi-select**                              | ↪ Moved to v0.6.0 | Multi-select of entities and relationships in the Mapping canvas                                                                                      |
| **Mapping — orphan detection**                          | ↪ Moved to v0.6.0 | Validate that all mapped entities are connected (no isolated nodes)                                                                                   |
| **Scheduler — inference & materialization**             | ↪ Moved to v0.6.0 | Trigger OWL 2 RL inference / SWRL materialization as scheduled tasks                                                                                  |


#### Also delivered (beyond the original plan)

- Build-run tracing + **Build Analytics** panel and domain-wide **Audit trail**
- Graph/registry **Lakebase separation** (`BranchLakebaseAuth`, in-app *Create Graph DB* provisioner, Permissions tab)
- Business Views overhaul (**New Assistant**, collapse/expand, right-click hide)
- **CNS test foundations** — coverage gates, MCP/contract/property tests, agent eval harness, ruff/mypy, live & deployed-app e2e
- Deploy simplification — single-knob multi-instance, `--dry-run`, hardened `deploy.sh`, owner-run self-healing migrations

---

### v0.6.0 — Workflow & Quality Completeness (End of June 2026)

**Theme:** a fast-follow release that closes the remaining 0.5-line UX and automation items deferred from v0.5.0, building directly on the new version-lifecycle and review foundations.


| Capability                                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Priority |
| --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| **Ontology iteration UX**                           | Manage and iterate over generated ontology versions — side-by-side compare, structural diff (added/removed classes, properties, relationships), promote, and rollback — wired into the `DRAFT → IN-REVIEW → PUBLISHED` lifecycle                                                                                                                                                                                                                                                                                                                                                                                 | P1       |
| **Mapping — multi-select**                          | Multi-select of entities and relationships in the Mapping canvas (shift/ctrl + marquee) so bulk actions (map, exclude, clear) apply to a selection                                                                                                                                                                                                                                                                                                                                                                                                                                                               | P2       |
| **Mapping — orphan detection**                      | Validation pass that flags mapped entities with no relationships (isolated nodes), surfaced as advisory warnings in the Mapping designer and the Cockpit readiness checks                                                                                                                                                                                                                                                                                                                                                                                                                                        | P2       |
| **Scheduler — inference & materialization**         | Extend the scheduler so OWL 2 RL inference and SWRL materialization can run as scheduled tasks alongside the existing build job, with results recorded in the build-run trace                                                                                                                                                                                                                                                                                                                                                                                                                                    | P2       |
| **Graph analytics metrics**                         | Compute graph-specific centrality and structure metrics over the knowledge graph — betweenness, PageRank, degree/closeness centrality, clustering coefficient, connected components — surfaced in the Knowledge Graph explorer (node sizing/ranking) and the Domain Cockpit                                                                                                                                                                                                                                                                                                                                      | P1       |
| **Collaborative comments & tasks**                  | Contextual commenting anywhere on a `DRAFT` domain — on the ontology canvas, mapping designer, and while exploring the knowledge graph. Users can attach threaded comments to any object (class, property, mapping, node/edge) and read the full comment trail to understand what's happening and why. The app also proposes turning a comment into a **personalized task assigned to a teammate**, tracked through the version-lifecycle and surfaced in the Validation & Review workspace                                                                                                                      | P1       |
| **Advanced reasoning — temporal & recursive rules** | Extend the multi-phase reasoning engine with two new symbolic families, both compiling to SQL and integrated into the existing run pipeline (default off, per-rule selectable, materializable): **(1) Temporal reasoning** — Allen's 13 interval relations (before, meets, overlaps, during, …) inferred from entity start/end datatype properties; **(2) recursive Datalog** — stratified, semi-naïve fixpoint rules reusing the SWRL atom syntax for true recursion (e.g. conditional reachability/ancestry) beyond the fixed transitive closure. Shipped as a phased roadmap (temporal first, Datalog second) | P2       |


#### Why a fast-follow 0.6.0

These items share the same surfaces just reworked in 0.5.0 (the ontology designer, the mapping canvas, the version lifecycle, and the scheduler/build-run trace). Shipping them as a cohesive fast-follow — rather than folding them into the larger 0.7.0 Neo4j effort — keeps the 0.5 line feature-complete against its original scope and delivers the most-requested workflow gaps to existing users quickly. Collaborative comments & tasks build directly on the `DRAFT → IN-REVIEW → PUBLISHED` lifecycle and the Validation & Review workspace, turning the review loop from a single sign-off step into an ongoing, contextual conversation across the team.

#### Deployment & data model (Collaborative comments & tasks)

The feature introduces new persistence in the **Lakebase registry schema** (`ontobricks_registry`). It extends the existing append-only `domain_review_events` table (which already records `commented` events at the `(domain_id, version)` grain) with two new tables:

- `**domain_comments`** — threaded, contextual comments anchored to a specific object on a `DRAFT` version. Grain is `(domain_id, version)` plus an `anchor_type` (`ontology_class` | `ontology_property` | `mapping` | `graph_node` | `graph_edge` | `domain`) and `anchor_ref` (the target URI / node id), with `parent_id` for reply threads, `author`, `body`, and `resolved` flag.
- `**domain_tasks**` — personalized tasks created from a comment or standalone, with `assignee` (principal), `created_by`, `title`, `description`, `status` (`open` | `in_progress` | `done` | `cancelled`), optional `due_date`, and a `comment_id` back-reference. Surfaced in the Validation & Review "My Tasks" worklist.

**Deployment is automatic and idempotent.** These tables (and their indexes) are added to `src/back/objects/registry/store/lakebase/schema.sql` using `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`, exactly like the existing registry tables. They are created on the next `LakebaseRegistryStore.initialize()` and re-applied safely on every deploy. Existing deployments pick them up by re-running the registry init (**Settings → Registry → Initialize**) or `make bootstrap-lakebase`, which also re-grants `USAGE/DML` on the new objects to the app and MCP service principals — **no destructive migration and no manual DDL required**.

#### A note on unstructured data ingestion

Unstructured data (documents, PDFs, emails, images, transcripts, logs) was originally scoped to be a first-class part of the **data ingestion** process — sitting alongside the structured R2RML mapping flow so that text and tables could feed the same ontology-governed knowledge graph. As we dug in, it became clear this is a **genuinely hard problem** that deserves more investigation before we commit to a design: entity/relation extraction quality, chunking and grounding, deduplication and entity resolution against existing instances, provenance and confidence, cost at scale, and keeping the whole thing governed under Unity Catalog. v0.5.0 already shipped a first step here — document-to-markdown conversion via `ai_parse_document` feeding the OWL and business-rules agents — but turning raw unstructured content into trustworthy, mappable graph entities is a larger effort. We would rather take the time to get the architecture right than ship something half-baked, so the full capability is **deliberately deferred** while we research it.

In the meantime, the good news is that the **Databricks platform already offers most of the building blocks** to bridge unstructured data into an ontology you have already designed in OntoBricks. Today you can compose, among others:

- **Lakeflow Declarative Pipelines** (batch/streaming) and **Auto Loader** to land and incrementally process raw files
- **AI Functions** in SQL — `ai_parse_document`, `ai_extract`, `ai_classify`, `ai_query`, `ai_gen` — to extract entities, attributes, and relationships directly from text
- **Foundation Model API / Model Serving** for custom extraction, summarization, and structured-output prompting
- **Vector Search** for semantic retrieval, entity linking, and de-duplication against existing graph instances
- **Unity Catalog** for governance, lineage, and permissions across the whole flow

Our current thinking is to **lean on these existing capabilities** rather than reinvent them: use them to extract structured records from unstructured sources, then map those records onto an **ontology that is already designed** in OntoBricks — reusing the same mapping, reasoning, and quality machinery that structured sources already go through. That keeps unstructured ingestion consistent with the rest of the platform and avoids a parallel, ungoverned pipeline.

> **Call to action:** this is exactly the kind of feature we want to shape **with** our users. If you have concrete use cases, source types, or requirements for unstructured ingestion, please share them in the project **Discussions** — your input will directly steer the design and prioritization of this work.

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

### v0.8.0 — Enterprise Hardening (September 2026)

**Theme:** prepare OntoBricks for large enterprise deployments with strict governance, performance, and multi-tenancy requirements.


| Feature                        | Description                                                                                |
| ------------------------------ | ------------------------------------------------------------------------------------------ |
| **Fine-grained RBAC**          | Per-domain, per-version read/write/admin roles via Unity Catalog grants                    |
| **Multi-workspace federation** | Cross-workspace domain registry sync — read a domain built in workspace A from workspace B |
| **Audit log**                  | Every build, reasoning run, and mutation emits a structured event to a Delta audit table   |
| **Large-graph pagination**     | Server-side cursor pagination for 10k+ node knowledge graphs                               |
| **Scheduled reasoning**        | Trigger OWL 2 RL / SWRL inference on a cron schedule                                       |
| **API key authentication**     | Scoped API keys for external REST and GraphQL consumers                                    |
| **One-command deployment**     | Single DAB deploy installs OntoBricks + MCP server + registry together                     |


---

### v1.0.0 — General Availability (November 2026)

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


| Feature                                        | v0.3 | v0.4 | v0.5 | v0.6 | v0.7 | v0.8 | v1.0 |
| ---------------------------------------------- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Delta Lake triple store                        | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Lakebase named-graph triple store**          | —    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Lakebase optimized indexes**                 | —    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Lakebase SPARQL SELECT**                     | —    | ✅    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **UX & workflow improvements**                 | —    | —    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Version lifecycle & review**                 | —    | —    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Auto quality rules**                         | —    | —    | ✅    | ✅    | ✅    | ✅    | ✅    |
| **Ontology version diff/iteration**            | —    | —    | —    | ✅    | ✅    | ✅    | ✅    |
| **Mapping multi-select & orphan check**        | —    | —    | —    | ✅    | ✅    | ✅    | ✅    |
| **Scheduled inference / materialization**      | —    | —    | —    | ✅    | ✅    | ✅    | ✅    |
| **Graph analytics (betweenness, PageRank, …)** | —    | —    | —    | ✅    | ✅    | ✅    | ✅    |
| **Collaborative comments & tasks**             | —    | —    | —    | ✅    | ✅    | ✅    | ✅    |
| **Temporal & recursive Datalog reasoning**     | —    | —    | —    | ✅    | ✅    | ✅    | ✅    |
| **Neo4j connector**                            | —    | —    | —    | —    | ✅    | ✅    | ✅    |
| Fine-grained RBAC                              | —    | —    | —    | —    | —    | ✅    | ✅    |
| Multi-workspace federation                     | —    | —    | —    | —    | —    | ✅    | ✅    |
| Amazon Neptune                                 | —    | —    | —    | —    | —    | —    | ✅    |
| Databricks Marketplace                         | —    | —    | —    | —    | —    | —    | ✅    |


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

1. **Lakebase SPARQL subset scope** — BGP + FILTER covers 80% of use cases; OPTIONAL and UNION add another 15%. Aggregates and property paths are deferred to a later patch.
2. **Neo4j Community vs Enterprise** — named graphs as separate databases require Neo4j Enterprise. Community edition support will use label prefixing as a documented workaround.
3. **Triple store migration UX** — when an admin switches engine (e.g., Delta → Neo4j), OntoBricks will require a rebuild rather than offering an inline migration wizard. A migration assistant is considered for v0.8.
4. **Auto quality rules confidence** — the v0.5.0 business-rules generator is advisory (suggest + review/accept). How aggressively should auto-suggested rules be applied? Auto-apply with confidence thresholds is deferred pending feedback.

---

## How to Contribute

The graph engine abstraction is designed for external contributions. Adding a new store requires implementing the `GraphStore` interface, registering the engine in `GraphDBFactory`, adding an optional dependency group, providing a Settings UI card, and writing unit tests with a mock driver.

See `docs/graphdb-integration.md` for the full engine abstraction contract.