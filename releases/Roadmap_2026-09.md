# OntoBricks — Product Roadmap

> **Version:** 0.8.0 → beyond<br>
> **Last updated:** 2026-09-01<br>
> **Status:** Living document — Asana-first prioritization<br>
> **Scheduling policy:** priorities and dependencies are directional; this roadmap contains no delivery-date commitments.

> **Disclaimer:** This roadmap represents the current product direction and planned investments as of the date above. It is provided for informational purposes only and is subject to change at any time without notice. The features, ordering, and priorities described here are aspirational and do not constitute a commitment, promise, or legal obligation to deliver any specific functionality. Actual releases may differ materially from what is described here.

---

## Executive Summary

OntoBricks is a Databricks-native semantic engineering platform: teams design OWL ontologies, map Unity Catalog data, build interchangeable Lakehouse/Lakebase/Neo4j knowledge graphs, apply rules and reasoning, and publish governed graph capabilities through REST, GraphQL, Graph Chat, and MCP.

Version 0.8.0 closes the most immediate product-integrity gaps and broadens the AI-facing surface. It adds ontology-only domains, per-domain MCP policy, virtual attributes, metadata/mapping drift protection, Lakehouse view-only materialization, safer generated-inference management, configurable UI branding, and a unified application design system.

The forward roadmap is now driven primarily by the active **OntoBricks-Product** Asana board:

1. **Finish and package v0.8.x** — complete the full validation campaign and turn current deployment analysis into a distributable, repeatable path.
2. **Rebuild the authoring journey in v0.9.0** — parse documents once, make ontology generation human-in-the-loop, and make Auto-Map the single selective mapping workflow.
3. **Extend platform integration and governance** — Genie Pages/Databricks Domains, Ontos, API identity, audit, RBAC, scale, migration, and federation.
4. **Prepare the v1.0 contract** — stable API semantics, enterprise identity, ecosystem connectors, Marketplace distribution, and community reuse.

No Asana target date is treated as a roadmap commitment. Items are ordered by explicit Asana priority and status first, then dependencies and product value. Unprioritized records remain visible as **Needs triage**.

---

## Market Context

### Why the semantic layer matters

- **AI grounding** — agents need governed entities, relationships, actions, and live attributes rather than unstructured prompt stuffing.
- **Data-product semantics** — Unity Catalog governs physical assets; ontologies add business meaning, constraints, inheritance, and cross-domain relationships.
- **Open standards** — OWL, RDFS, SKOS, SHACL, SWRL, R2RML, SPARQL, REST, GraphQL, and MCP reduce proprietary lock-in.
- **Graph choice without redesign** — the same ontology and mapping model can target Lakehouse, Lakebase, or Neo4j, or publish as ontology-only.
- **Databricks-native operations** — source data remains governed in Unity Catalog while OntoBricks provides design, build, reasoning, exploration, and AI publication in one Databricks App.

### Strategic position

OntoBricks is the semantic authoring and publication layer for the Databricks Lakehouse. It does not require every domain to materialize a graph, and it does not force every graph into one engine. A team can publish an ontology-only contract, use a live Lakehouse view, build a Lakebase graph, or target an existing Neo4j deployment while retaining one governed domain lifecycle.

The next differentiator is workflow quality: durable shared document context, human validation before ontology completion, selective entity-first mapping, platform-native Genie/Domain exchange, and enterprise-grade API/governance controls.

---

## Prioritization and Status Legend

### Priority

- **High** — explicit High priority in Asana; should be evaluated before unprioritized work in the same release.
- **Medium / Low** — explicit Asana priority when present.
- **Needs triage** — the active Asana task has no priority value. It remains visible but its relative order is provisional.

### Status

- **In progress** — active implementation or validation work.
- **Analyzing** — scope, packaging, or technical direction is being assessed.
- **Not started** — scoped enough to remain on the release board but implementation has not started.
- **Needs status** — the active Asana task has no status value.

### Ordering rule

Within a release:

1. Explicit priority.
2. In progress, then Analyzing, then Not started, then unset status.
3. Dependency order.
4. Product value and risk reduction.

Asana owns planned scope and priority. The repository and v0.8.0 changelogs own delivered status.

---

## Current State — v0.8.0

### Semantic authoring

- Visual ontology design with entity, relationship, attribute, inheritance, constraint, action, virtual-attribute, and business-rule authoring.
- OWL/RDFS/SKOS/SHACL import/export plus R2RML mapping generation.
- Metadata-refresh diff, data-source deletion impact, schema-drift diagnostics, and reliable Designer persistence.
- LLM-assisted ontology generation and Auto-Mapping with live agent activity and durable audit reports.

### Graph and reasoning

- Per-domain **Lakehouse**, **Lakebase**, **Neo4j**, or **No Backend** selection.
- Lakehouse materialized-table or live-view modes.
- OWL 2 RL, SWRL, Data Quality, cohorts, generated-inference count and safe purge.
- Graph Analytics, Graph Explorer, Graph Chat, and bounded graph-read execution.

### Publication and integration

- PUBLISHED-only external REST and GraphQL data plane.
- MCP with per-domain tool/context policy, ontology-only `describe_ontology`, cross-domain bridge guidance, actions, datasets, and on-demand virtual attributes.
- Swagger/ReDoc with typed domain payloads and explicit graph-backend readiness.
- Unity Catalog HTTP connection guidance for attaching the MCP application to Genie.

### Governance and operations

- DRAFT → IN-REVIEW → PUBLISHED lifecycle, including ontology-only review.
- Direct-user and Databricks-group permission resolution.
- Builder graph refresh on frozen versions.
- Dual Volume/Lakebase registry, domain audit trail, task/run history, scheduler, and DAB deployment.
- Configurable instance-wide title, color, logo, and default entity icon.

---

## Known Limitations After v0.8.0

- Uploaded binary documents can still be parsed repeatedly by different agent runs; there is no durable shared parsed corpus.
- Ontology Generate still completes too much work before the user validates the entity set.
- Mapping retains separate Manual and Auto-Map paths instead of one selective workflow.
- MCP visibility rules are session-aware, but the selected domain and cached domain context are process-shared in the current standalone server design.
- Fine-grained API credentials, durable Delta audit events, and per-domain/per-version Unity Catalog RBAC are not yet available.
- Large-graph APIs and Explorer paths do not consistently use server-side cursor pagination.
- Cross-workspace federation and guided graph-engine migration are not available.
- Databricks Ontos, Genie Pages, and Databricks Domains are not yet bidirectional ontology sources/targets.
- Internationalization has not been scaffolded.
- Amazon Neptune and Azure Cosmos DB connectors are not implemented.

---

## Roadmap

### v0.8.x — Release Readiness and Follow-up

#### 1. Full tests campaign

**Priority:** High<br>
**Status:** In progress

Complete the release validation matrix across the non-scenario suite, opt-in live scenarios, navigation/responsive contracts, external API/OpenAPI checks, and deployment preflight. Record environmental skips separately from product failures and keep release evidence reproducible.

**Exit criteria**

- Required suite and contract checks are green.
- Live scenarios that have credentials execute successfully; environmental skips are documented.
- Release notes and upgrade guidance match the validated product.
- No known release blocker remains unclassified.

#### 2. Packaging & Deployment

**Priority:** High<br>
**Status:** Analyzing

Turn the current DAB, bootstrap, migrations, UI app, MCP app, registry, graph resources, and permissions flow into a simpler distribution path.

**Scope under analysis**

- Marketplace integration and deployment prerequisites.
- Deployment simplification and reduced operator input.
- Reuse of current preflight, dry-run, bootstrap, and instance-isolation foundations.
- A clear boundary between v0.8.x packaging groundwork, the v0.9 one-command goal, and the v1.0 Marketplace listing.

---

### v0.9.0 — Product and Workflow Priorities

#### High-priority authoring flow

##### 1. Parse documents once after upload

**Priority:** High<br>
**Status:** Not started

Create a durable parsed-document corpus shared by Generate and Mapping. Parse a successful upload once, persist extracted markdown/text and a manifest next to the original, and invalidate only when content changes.

**Core requirements**

- Original document remains unchanged.
- Parsed sidecar records source hash, parser/schema version, status, timestamp, and error.
- Identical re-upload is a no-op; changed content reparses.
- Generate and Mapping consume ready artifacts and never trigger first-time parsing.
- Failed/pending parsing is visible and retryable.

**Dependency:** prerequisite for the three-stage Generate flow.

##### 2. Three-stage human-in-the-loop ontology Generate

**Priority:** High<br>
**Status:** Not started

Replace one-shot generation with:

1. Detect candidate entities from metadata and the parsed document corpus.
2. Pause for user include/exclude review and per-entity synonyms.
3. Complete the ontology from validated entities only: relationships, then attributes, then axioms.

Excluded entities must not reappear during completion. The validated entity list becomes the durable contract between review and generation.

##### 3. Auto-Map selection and entity-first execution

**Priority:** High<br>
**Status:** Not started

Make Auto-Map the single mapping entry point:

- Remove the Manual Mapping page, route, navigation, and unused client code.
- Present potential entity and relationship mappings in separate selectable tabs.
- Select all candidates by default while allowing explicit exclusion.
- Run selected entities first, persist their keys, then generate relationship mappings against those keys.
- Reuse the parsed corpus when document evidence informs mapping.

#### Platform authoring integrations

##### 4. Genie Pages and Databricks Domains integration

**Priority:** Needs triage<br>
**Status:** Not started

Use Genie Pages and Databricks Domains as additional ontology-generation sources, and publish validated OntoBricks ontology content back through previewed, confirmed, permission-aware create/update operations.

The source model must combine uploaded documents, Genie Pages, and Databricks Domains while preserving source traceability and failure isolation.

##### 5. Ontos integration

**Priority:** Needs triage<br>
**Status:** Needs status

Define and implement the supported relationship with Databricks Ontos:

- Import or align Ontos assets into an OntoBricks domain.
- Publish OntoBricks ontology/mapping assets for platform reuse.
- Resolve asset granularity, authentication, ownership, and conflicts.
- Decide whether the product supports one-way import, publish-only, or bidirectional synchronization.

This remains discovery-heavy until the external API and conflict model are validated.

#### Deferred workflow capabilities

##### 6. Deferred workflow items from v0.6.0

**Priority:** Needs triage<br>
**Status:** Needs status

The Asana rollup retains five capabilities:

- **Ontology iteration UX** — compare structural/version changes, promote, and rollback.
- **Mapping multi-select** — select entities and relationships for bulk actions.
- **Mapping orphan detection** — flag isolated mapped entities.
- **Scheduled inference/materialization** — run OWL/SWRL materialization through the scheduler.
- **Temporal and recursive reasoning** — Allen interval relations and stratified recursive Datalog.

The rollup still carries legacy subtask labels (`P1` for ontology iteration, `P2` for the remaining items), but the parent has no active Asana priority and therefore needs release-level triage.

#### Governance and enterprise access

##### 7. Governance & Access Control

**Priority:** Needs triage<br>
**Status:** Needs status

- Scoped API-key authentication for external REST/GraphQL consumers.
- Durable structured audit events in Delta.
- Fine-grained per-domain and per-version RBAC backed by Unity Catalog grants.

Security design, revocation, rotation, and authorization semantics must be reviewed as product contracts rather than treated as implementation-only work.

#### Scale, deployment, and federation

##### 8. Scale, Deploy & Federation

**Priority:** Needs triage<br>
**Status:** Needs status

- Server-side cursor pagination for large graphs.
- One-command deployment of the UI, MCP server, registry, migrations, jobs, and permissions.
- Guided graph-engine migration between Lakehouse, Lakebase, and Neo4j.
- Cross-workspace domain registry and access federation.

Dependencies: packaging groundwork precedes one-command deployment; stable domain identity and RBAC precede federation.

#### Unscoped v0.9 requests

##### 9. Clone a domain under a new name

**Priority:** Needs triage<br>
**Status:** Needs status

Define clone semantics for versions, source bindings, credentials/connections, graph artifacts, audit history, review status, and MCP exposure before implementation.

##### 10. Administrator-controlled LLM availability

**Priority:** Needs triage<br>
**Status:** Needs status

Allow administrators to choose which configured LLM endpoints/models are available to OntoBricks features. Scope must cover capability discovery, feature compatibility, fallback behavior, secret handling, and whether selection is global or feature-specific.

---

### v1.0.0 — GA and Ecosystem Priorities

All active v1.0 rollups currently need Asana priority and status triage. Their order below reflects GA dependencies, not a date commitment.

#### 1. GA Contract & Identity

**Priority:** Needs triage<br>
**Status:** Needs status

- Stable REST API v1 with SemVer and a documented deprecation policy.
- SSO/SCIM provisioning for enterprise identity lifecycle.

This is the core GA gate: external consumers need a stable contract, and operators need managed identity onboarding/offboarding.

#### 2. Ecosystem Connectors

**Priority:** Needs triage<br>
**Status:** Needs status

- Amazon Neptune connector using RDF/SPARQL over HTTPS.
- Azure Cosmos DB connector using the Gremlin API.

Both should implement the existing graph-store abstraction and preserve OntoBricks ontology, mapping, reasoning, and read semantics. Live service compatibility tests remain mandatory.

#### 3. Distribution & Community

**Priority:** Needs triage<br>
**Status:** Needs status

- Databricks Marketplace one-click installation.
- OntoBricks Hub for community ontologies and mapping templates.

Marketplace distribution depends on the v0.8.x packaging analysis and v0.9 deployment simplification. OntoBricks Hub is separable from the GA gate and should not delay stable API/identity delivery unless explicitly reprioritized.

---

### Requests — To Evaluate

These are active in Asana but are not assigned to a committed release section. Dates and old roadmap placement do not promote them automatically.

#### External Surface Expansion

**Priority:** Needs triage<br>
**Status:** Analyzing

The rollup contains:

- Dashboards, actions, and datasets on the external API.
- MCP GraphRAG-style retrieval.
- A design pass for DRAFT domains and agentic external operations.

Parts of the external surface advanced materially in v0.8.0—domain backend/readiness, typed Swagger responses, MCP policy, class actions/context, bridges, and virtual attributes—but the three listed outcomes remain incomplete as a rollup. Re-scope the remaining API work before assigning it to a release.

#### Ontology & Knowledge Graph Modeling

**Priority:** Needs triage<br>
**Status:** Needs status

- First-class Enumeration attribute type.
- Surface the existing SPARQL editor under Knowledge Graph, with a product decision between navigation-only exposure and a fuller editor upgrade.

The subtasks retain legacy `P2` labels, but the parent requires active triage.

#### Domain Storage Isolation

**Priority:** Needs triage<br>
**Status:** Needs status

- Dedicated Lakebase schema per domain.
- Dedicated Unity Catalog/Lakehouse schema per domain.
- Create, rename-safe bind, retire/drop, grants, and migration lifecycle.

Before promotion, decide the naming convention, compatibility path for existing deployments, and whether version artifacts share or isolate the domain schema.

#### Platform UX & Localization

**Priority:** Needs triage<br>
**Status:** Needs status

- Generalize unsaved-change protection beyond Designer surfaces.
- Establish i18n foundations once target languages and translation ownership are known.

v0.8.0 solved several Designer and Settings-specific persistence/leave guards, so the first subtask must be re-audited and narrowed to remaining surfaces.

#### Virtual attributes in Business Rules conditions

**Priority:** Needs triage<br>
**Status:** Needs status

Evaluate whether on-demand Unity Catalog function values can participate safely in rule conditions. The design must address determinism, batching, latency, retries, auditability, and the current contract that virtual attributes are not materialized or queryable in Data Quality.

#### Arrows.app schema import

**Priority:** Needs triage<br>
**Status:** Needs status

Evaluate an Ontology Import tab for Arrows.app schemas, including supported node/relationship metadata, datatype conversion, identifiers, constraints, and round-trip expectations.

#### Collibra metadata import

**Priority:** Needs triage<br>
**Status:** Needs status

Evaluate Collibra as a metadata source: authentication, asset hierarchy, glossary/ontology mapping, incremental refresh, lineage links, and whether the integration belongs in source metadata ingestion or ontology import.

---

## Cross-Release Dependencies

1. **Parsed document corpus → three-stage Generate**<br>
   Detection and completion must consume durable parsed artifacts before the generation flow is split.

2. **Parsed corpus → Auto-Map document evidence**<br>
   Auto-Map should not preserve a second on-demand extraction path.

3. **Validated entities → entity-first Auto-Map**<br>
   Entity review/synonyms and selective mapping should converge on one stable entity identity contract.

4. **Packaging analysis → one-command deployment → Marketplace**<br>
   Distribution should reuse a single deploy/bootstrap model rather than fork a Marketplace-only installer.

5. **Stable identity/RBAC → federation**<br>
   Cross-workspace access requires durable domain identity, authorization, and audit semantics.

6. **Stable graph-store contract → Neptune/Cosmos connectors**<br>
   Connector work must preserve the same build/read/reasoning capabilities and declare unsupported features explicitly.

7. **External API write design → API keys and stable v1**<br>
   Draft/agentic operations should not ship before authentication, authorization, idempotency, and deprecation rules are defined.

---

## Feature Matrix

| Capability | v0.8.0 | v0.9.0 direction | v1.0.0 direction |
| --- | --- | --- | --- |
| Ontology-only publication | Delivered | Harden | Stable contract |
| Lakehouse / Lakebase / Neo4j | Delivered | Migration and scale | Add Neptune/Cosmos |
| Per-domain MCP policy | Delivered | Retrieval/auth evolution | Stable external contract |
| Virtual attributes | Delivered | Evaluate rule integration | Governed ecosystem use |
| Metadata/mapping integrity | Delivered | Selective entity-first mapping | Stable workflow |
| Parsed document corpus | Not available | Priority | Foundation |
| Human-reviewed Generate | Not available | High priority | Stable workflow |
| Selective Auto-Map | Not available | High priority | Stable workflow |
| Genie/Databricks Domain exchange | Not available | Needs triage | Platform integration |
| Ontos integration | Not available | Needs triage | Platform integration |
| Fine-grained API/RBAC/audit | Partial | Needs triage | GA requirement |
| Cross-workspace federation | Not available | Needs triage | Enterprise scale |
| Stable REST API v1 policy | Partial | Prepare | GA requirement |
| Marketplace one-click install | Not available | Packaging dependency | Target |

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

## Open Decisions

1. **Ontology Generate review contract** — how candidate evidence, synonyms, exclusions, and later edits persist across versions.
2. **Parsed corpus storage** — volume sidecars as the default, Lakebase index, or a hybrid chunk index for retrieval/citations.
3. **Auto-Map candidate source** — whether mapping selection starts from the validated Generate entity set, the full ontology, or both with explicit provenance.
4. **Genie/Domain/Ontos boundaries** — overlapping platform ontology surfaces need one asset and conflict model.
5. **Domain storage naming and migration** — fixed generated schema names versus operator selection, plus safe adoption for existing multi-domain installations.
6. **GraphRAG definition** — ranked retrieval over structured graph entities versus classic chunk embeddings/community summaries.
7. **External write authentication** — whether DRAFT/agentic APIs wait for scoped API keys or use a narrower Databricks-authenticated contract first.
8. **Virtual attributes in rules** — live evaluation versus explicit materialization, with deterministic audit and performance limits.
9. **LLM availability policy** — global allow-list, feature-specific compatibility matrix, and fallback behavior.
10. **Internationalization ownership** — target languages, source-key extraction, translation review, and release validation.

---

## How to Contribute

The most useful contributions map to active, prioritized Asana work or help triage an unprioritized request with a concrete user case.

- For **Generate and parsed documents**, provide representative document sizes, formats, expected citations, and entity-review workflows.
- For **Auto-Map**, provide examples where entity-first keys materially improve relationship mapping.
- For **Genie Pages, Databricks Domains, or Ontos**, document the desired direction of exchange and conflict behavior.
- For **GraphRAG**, distinguish structured graph retrieval needs from unstructured chunk retrieval.
- For **new graph connectors**, implement the graph-store abstraction and declare capability differences explicitly.
- For **enterprise governance**, provide API credential, RBAC, audit retention, and cross-workspace requirements.

Feature discussions should identify the domain, expected outcome, security boundary, scale, and acceptance signal. That evidence is what allows an Asana item marked **Needs triage** to become a credible release priority.
