# OntoBricks — Product Roadmap

> **Version:** 0.9.0 shipped → 0.10.0 and beyond<br>
> **Last updated:** 2026-10-21<br>
> **Status:** Living vision — direction of travel, not a delivery contract<br>
> **Scheduling policy:** themes and sequencing are directional. This document
> contains no delivery-date commitments.

> **Disclaimer:** This roadmap represents the current product direction and
> planned investments as of the date above. It is provided for informational
> purposes only and is subject to change at any time without notice. The
> features, ordering, and priorities described here are aspirational and do
> not constitute a commitment, promise, or legal obligation to deliver any
> specific functionality. Actual releases may differ materially from what is
> described here.

---

## Executive Summary

OntoBricks is a Databricks-native semantic engineering platform. Teams design
OWL ontologies, map Unity Catalog data, build interchangeable Lakehouse /
Lakebase / Neo4j knowledge graphs, apply rules and reasoning, and publish
governed graph capabilities through REST, GraphQL, Graph Chat, and MCP.

**v0.9.0 is the current product.** It delivered Lakehouse//RT queries, staged
Ontology Generate, Knowledge Store, GitHub domain mirror, Metric Views, SPARQL
playground, entity SWRL rules, Job class actions, Ontology Studio, OBO / No-LLM
gates, and security hardening. See `releases/ReleaseNotes_V0.9.0.md` (and
`releases/ReleaseNotes_V0.8.0.md` for the previous line).

The next product line (**v0.10.0**) is about finishing the authoring journey
and connecting OntoBricks to the rest of the Databricks semantic surface.
Toward GA we then invest in identity, a stable API contract, Marketplace
distribution, and additional graph stores.

1. **Finish the authoring journey** — Auto-Map becomes the single selective,
   entity-first mapping workflow.
2. **Platform exchange** — bidirectional Genie Pages / Databricks Domains,
   then Ontos once the external API is validated.
3. **Admin LLM allow-list** — administrators choose which Gateway models
   OntoBricks may use.
4. **Enterprise distribution** — Marketplace listing, stable REST v1,
   SSO/SCIM, community Hub, Neptune and Cosmos connectors.
5. **Deeper platform** — governance, scale and federation, per-domain storage
   isolation, industry catalogs, and graph provenance / explainability.

---

## Market Context

### Why the semantic layer matters

- **AI grounding** — agents need governed entities, relationships, actions,
  and live attributes rather than unstructured prompt stuffing.
- **Data-product semantics** — Unity Catalog governs physical assets;
  ontologies add business meaning, constraints, inheritance, and cross-domain
  relationships.
- **Open standards** — OWL, RDFS, SKOS, SHACL, SWRL, R2RML, SPARQL, REST,
  GraphQL, and MCP reduce proprietary lock-in.
- **Graph choice without redesign** — the same ontology and mapping model can
  target Lakehouse, Lakebase, or Neo4j, or publish as ontology-only.
- **Databricks-native operations** — source data remains governed in Unity
  Catalog while OntoBricks provides design, build, reasoning, exploration, and
  AI publication in one Databricks App.

### Strategic position

OntoBricks is the semantic authoring and publication layer for the Databricks
Lakehouse. It does not require every domain to materialize a graph, and it
does not force every graph into one engine.

The next differentiator is workflow quality: selective entity-first mapping,
platform-native Genie/Domain exchange, and enterprise-grade API and governance
controls. Durable parsed documents and three-stage Generate already shipped in
v0.9.0.

---

## How to read this document

- **Available now** — shipped in v0.9.0 (including v0.8.x).
- **Next** — the intended v0.10.0 product line: authoring, platform exchange,
  and LLM administration.
- **Toward GA** — identity, stable API, Marketplace, Hub, and additional
  graph stores. Sequenced after the authoring and platform-exchange work.
- **Exploring** — capabilities under evaluation. They inform the vision; they
  are not assigned to a release.

---

## Available now — v0.9.0

### Semantic authoring

- Visual ontology design (Studio) with entity, relationship, attribute,
  inheritance, constraint, action, virtual-attribute, and inference-rule
  authoring.
- OWL/RDFS/SKOS/SHACL import/export plus R2RML mapping generation; OWL
  fidelity across save/import (labels, language tags, leftover RDF).
- Metadata-refresh diff, data-source deletion impact, schema-drift
  diagnostics, and reliable Studio persistence.
- LLM-assisted three-stage ontology Generate and Auto-Mapping with live agent
  activity and durable audit reports.
- Knowledge Store (Lakebase parsed corpus) shared by Generate and Mapping.
- Unity Catalog Metric Views as data assets (dimension/measure roles,
  `MEASURE()` SQL).

### Graph and reasoning

- Per-domain **Lakehouse**, **Lakebase**, **Neo4j**, or **No Backend**
  selection.
- Lakehouse materialized-table or live-view modes.
- Split **Build** vs **Query** SQL warehouses, optional Lakehouse//RT reads
  over the Statement Execution API, and a global CloudFetch toggle for
  Databricks Apps egress.
- OWL 2 RL, SWRL (including per-entity rules), Data Quality (graph-wide and
  per-entity), cohorts, generated-inference count and safe purge.
- Graph Analytics, Graph Explorer (indexed search, paged BFS, Find Path,
  quality rings), Graph Chat, SPARQL playground, and bounded graph-read
  execution.
- Class actions: Unity Catalog functions and Databricks Jobs (`entity_id`,
  async `run_now`, Background Tasks).

### Publication and integration

- PUBLISHED-only external REST and GraphQL data plane.
- MCP with per-domain tool/context policy, ontology-only `describe_ontology`,
  cross-domain bridge guidance, actions, datasets, on-demand virtual
  attributes, entity SHACL checks, and entity business rules.
- GitHub domain mirror (push on save, Load from GitHub, optional GitHub
  delete).
- Swagger/ReDoc with typed domain payloads and explicit graph-backend
  readiness.

### Governance and operations

- DRAFT → IN-REVIEW → PUBLISHED lifecycle, including ontology-only review.
- Direct-user and Databricks-group permission resolution; admin-only domain
  creation; per-backend OBO.
- Builder graph refresh on frozen versions.
- Dual Volume/Lakebase registry, compact domain audit trail, task/run
  history, scheduler, and DAB deployment.
- Configurable instance-wide title, color, logo, and default entity icon.
- CSRF cookie+header, public `/health` liveness, dialect-aware SQL escaping.

---

## Gaps we are closing

These are the honest limits of v0.9.0 — they are also the spine of the
forward vision.

- Mapping still has separate Manual and Auto-Map paths instead of one
  selective, entity-first workflow.
- MCP visibility rules are session-aware, but the selected domain and cached
  domain context are process-shared in the current standalone server design.
- Fine-grained API credentials, durable Delta audit events, and
  per-domain/per-version Unity Catalog RBAC are not yet available.
- Large-graph APIs do not consistently use server-side cursor pagination
  (Explorer BFS is paged; Overview/status use companions rather than scanning
  the pass-through graph VIEW).
- Cross-workspace federation and guided graph-engine migration are not
  available.
- Databricks Ontos, Genie Pages, and Databricks Domains are not yet
  bidirectional ontology sources and targets.
- Administrators cannot yet restrict which Gateway models are available to
  OntoBricks features.
- Internationalization has not been scaffolded.
- Amazon Neptune and Azure Cosmos DB connectors are not implemented.
- Enumeration is not a first-class attribute type.

---

## Next — v0.10.0

### Finish the mapping journey

**Auto-Map becomes the single mapping entry point.** Potential entity and
relationship mappings appear in separate selectable tabs (all checked by
default). OntoBricks runs only the checked items, **entities first**, persists
keys, then generates relationship mappings against those keys. The Knowledge
Store is reused when document evidence informs mapping. The dedicated Manual
Mapping page, route, and navigation go away.

This is the highest-priority product investment on the 0.10.0 line: one
authoring path from Generate to a governed mapping, instead of two.

### Connect to the Databricks semantic surface

**Genie Pages and Databricks Domains.** Bidirectional exchange: read Genie
Pages and Databricks Domains as Generate sources (combined with uploaded
documents), then publish a validated ontology back as a Domain and/or Genie
Page — with preview, confirmation, permission-aware create/update, and source
traceability.

**Ontos.** Import or align Ontos assets; publish OntoBricks ontology and
mapping assets; resolve granularity, authentication, ownership, and
conflicts. This work waits on a validated external API so the three platform
surfaces share one asset and conflict model.

### Control which models OntoBricks may use

Administrators choose which configured LLM endpoints and models are available
to OntoBricks features. The design covers capability discovery, feature
compatibility, fallback, secret handling, and whether selection is global or
feature-specific.

---

## Toward GA

These investments follow the authoring and platform-exchange work. They
define how OntoBricks is consumed as an enterprise product, not how a domain
is authored.

### Identity and a stable contract

- Stable REST API v1 with SemVer and a documented deprecation policy.
- SSO/SCIM provisioning for enterprise identity lifecycle.

Draft and agentic write operations should not ship before authentication,
authorization, idempotency, and deprecation rules are defined.

### Distribution

- Databricks Marketplace listing, review, and one-click installation.
- OntoBricks Hub for community ontologies and mapping templates.

Hub is separable from the GA contract and should not delay a stable API or
identity.

### Additional graph stores

- Amazon Neptune connector using RDF/SPARQL over HTTPS.
- Azure Cosmos DB connector using the Gremlin API.

Both implement the existing graph-store abstraction and preserve the same
build / read / reasoning capabilities, declaring unsupported features
explicitly. Live service compatibility tests remain mandatory.

---

## Exploring

Capabilities we are evaluating. They shape the longer vision; they are not
scheduled.

### Industry catalogs

Evaluate candidate ontology catalogs (same pattern as FIBO, CDISC, IOF,
FHIR). Checklist per standard: licence and canonical source, OWL/RDF
artefact, volumetry, Lakehouse mapping sketch, overlap, go/no-go. Strongest
product-fit names already in view: QUDT, BFO/CCO, CIM, Brick, RealEstateCore,
Allotrope, AAS, GS1, OMOP, DCAT-AP, PROV-O.

### External surface

Dashboards, actions, and datasets on the external API; MCP GraphRAG-style
retrieval; a design pass for DRAFT domains and agentic external operations.
Parts of this surface already advanced in v0.8.0 / v0.9.0 (typed Swagger,
MCP policy, class actions, bridges, virtual attributes). Remaining API work
is re-scoped before it joins a release.

### Authoring and reasoning depth

- Ontology iteration UX (version diff, promote, rollback).
- Mapping multi-select and mapping orphan detection.
- Scheduled inference / materialization.
- Temporal reasoning and stratified recursive Datalog.
- First-class Enumeration attribute type (the SPARQL playground already
  shipped in v0.9.0).
- Unsaved-change protection beyond Studio; i18n foundations once target
  languages and translation ownership are known.

### Governance and access

- Scoped API-key authentication for external REST/GraphQL.
- Durable structured audit events in Delta.
- Fine-grained per-domain and per-version RBAC backed by Unity Catalog
  grants.

### Scale, deploy, and federation

- Server-side cursor pagination for large graphs.
- One-command deployment of UI, MCP, registry, migrations, jobs, and
  permissions (packaging analysis closed in v0.9.0; Marketplace should reuse
  the same bootstrap model).
- Guided graph-engine migration between Lakehouse, Lakebase, and Neo4j.
- Cross-workspace domain registry and access federation — this requires
  durable domain identity, authorization, and audit first.

### Per-domain storage isolation

Dedicated Lakebase schema and Unity Catalog / Lakehouse schema per domain,
with create, rename-safe bind, retire/drop, grants, and migration lifecycle.

### Rules, provenance, and import bridges

- Evaluate whether on-demand Unity Catalog function values (virtual
  attributes) can participate safely in business-rule conditions
  (determinism, batching, latency, retries, auditability). Virtual
  attributes remain outside Data Quality materialization.
- Graph provenance and explainability: triple-level PROV-O, instance-value
  conflicts at sync, auditable decision events, MCP “why” tools
  (`explain_inference`, subgraph export, retrieve-by-entity), and an
  `ontobricks doctor` operator healthcheck.
- arrows.app JSON → OWL Turtle on the Ontology Import tab, then the existing
  OWL replace/append pipeline.
- Collibra metadata import: authentication, asset hierarchy,
  glossary/ontology mapping, incremental refresh, lineage, and whether the
  integration belongs in source metadata ingestion or ontology import.

---

## How the pieces depend on each other

1. **Knowledge Store → Auto-Map document evidence.** Auto-Map should not
   preserve a second on-demand extraction path.
2. **Validated Generate entities → entity-first Auto-Map.** Entity review,
   synonyms, and selective mapping should converge on one stable entity
   identity contract.
3. **Packaging (closed in v0.9.0) → one-command deploy → Marketplace.**
   Distribution reuses a single bootstrap model rather than a
   Marketplace-only installer.
4. **Stable identity / RBAC → federation.** Cross-workspace access requires
   durable domain identity, authorization, and audit semantics.
5. **Stable graph-store contract → Neptune / Cosmos.** Connectors preserve
   the same capabilities and declare gaps explicitly.
6. **External API write design → API keys and stable v1.** Draft and agentic
   operations wait on authentication, authorization, idempotency, and
   deprecation rules.
7. **Genie / Domain / Ontos boundaries.** Overlapping platform ontology
   surfaces need one asset and conflict model before bidirectional write.

---

## Feature Matrix

| Capability | v0.8.0 | v0.9.0 | Direction |
| --- | --- | --- | --- |
| Ontology-only publication | Delivered | Harden | Stable contract toward GA |
| Lakehouse / Lakebase / Neo4j | Delivered | RT queries, companions | Migration and scale (exploring) |
| Per-domain MCP policy | Delivered | Entity DQ + SWRL tools | Retrieval and auth evolution |
| Virtual attributes | Delivered | Studio Attrs. tab | Evaluate rule integration |
| Knowledge Store / parse once | — | Delivered | Foundation for Auto-Map |
| Human-reviewed Generate | — | Delivered | Stable workflow |
| Selective Auto-Map | Not available | Not available | **Next — 0.10.0 focus** |
| SPARQL playground | — | Delivered | Enumeration / editor extras |
| Job class actions | — | Delivered | Operate |
| GitHub mirror | — | Delivered | Operate |
| Metric Views | — | Delivered | Operate |
| Genie / Databricks Domain exchange | Not available | Not available | Next — 0.10.0 |
| Ontos integration | Not available | Not available | After Genie/Domain, exploring API |
| Admin LLM allow-list | Partial (No LLM + Gateway picker) | Partial | Next — 0.10.0 |
| Fine-grained API / RBAC / audit | Partial | Partial | Exploring, then toward GA |
| Marketplace one-click | Not available | Packaging closed | Toward GA |
| Stable REST API v1 policy | Partial | Prepare | Toward GA |

---

## Graph Engine Comparison

| Capability | Lakehouse | Lakebase | Neo4j | No Backend |
| --- | --- | --- | --- | --- |
| Storage | Delta table or live UC view | Postgres graph tables | Native property graph | None |
| Source governance | Full Unity Catalog | UC source + managed Postgres | UC source + external graph | Ontology in registry |
| Graph query | SQL/SPARQL translation, GraphQL | SQL/SPARQL subset, GraphQL | Cypher adapter, GraphQL | Not available |
| Reasoning materialization | Generated Delta companion | Generated Postgres companion | Backend-dependent; purge unsupported | Not available |
| Graph Analytics | UC snapshot/job | UC snapshot/job | UC snapshot/job | Not available |
| Best fit | Governed/live Lakehouse data | Databricks-native graph persistence | Existing graph-database estates | Semantic contracts and MCP ontology |

---

## Open product questions

These are the decisions that still shape the vision. They are not a backlog.

1. **Auto-Map candidate source** — mapping selection from the validated
   Generate entity set, the full ontology, or both with explicit provenance.
2. **Genie / Domain / Ontos boundaries** — one asset and conflict model
   across platform ontology surfaces.
3. **Domain storage naming and migration** — fixed generated schema names
   versus operator selection, plus safe adoption for existing multi-domain
   installations.
4. **GraphRAG definition** — ranked retrieval over structured graph entities
   versus classic chunk embeddings / community summaries.
5. **External write authentication** — whether DRAFT/agentic APIs wait for
   scoped API keys or use a narrower Databricks-authenticated contract first.
6. **Virtual attributes in rules** — live evaluation versus explicit
   materialization, with deterministic audit and performance limits.
7. **LLM availability policy** — global allow-list, feature-specific
   compatibility matrix, and fallback behavior.
8. **Industry-catalog go/no-go** — which evaluation targets become
   first-class Import catalogs.
9. **Internationalization** — target languages, source-key extraction,
   translation review, and release validation.

---

## Share feedback

The most useful input is a concrete use case, not a feature request in the
abstract.

- **Auto-Map** — examples where entity-first keys materially improve
  relationship mapping.
- **Genie Pages, Databricks Domains, or Ontos** — desired direction of
  exchange and conflict behavior.
- **Industry catalogs** — licence, canonical OWL, and a Lakehouse mapping
  sketch.
- **GraphRAG / explainability** — distinguish structured graph retrieval
  from unstructured chunk retrieval.
- **New graph stores** — required query, reasoning, and governance
  capabilities versus Lakehouse / Lakebase / Neo4j.
- **Enterprise governance** — API credential, RBAC, audit retention, and
  cross-workspace requirements.

Describe the domain, expected outcome, security boundary, scale, and how you
would know it worked. That is what turns a vision item into a release.
