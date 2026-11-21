# OntoBricks — Product Roadmap

> **Version:** 0.9.3 shipping → 0.10.0 closing → 0.11.0 and beyond<br>
> **Last updated:** 2026-11-16<br>
> **Status:** Living vision — direction of travel, not a delivery contract<br>
> **Scheduling policy:** themes and sequencing are directional. This document
> contains no delivery-date commitments.<br>
> **Source of truth for execution:** Asana project
> [OntoBricks-Product](https://app.asana.com/1/8808412813448/project/1217637563677287)
> (sections Version 0.10.0 / 0.11.0 / 0.12.0). This file is the customer-facing
> synthesis of that board plus the published GitHub milestones.

> **Disclaimer:** This roadmap represents the current product direction and
> planned investments as of the date above. It is provided for informational
> purposes only and is subject to change at any time without notice. The
> features, ordering, and priorities described here are aspirational and do
> not constitute a commitment, promise, or legal obligation to deliver any
> specific functionality. Actual releases may differ materially from what is
> described here.

> **Supersedes** `releases/Roadmap_2026-10.md` (October 2026 vision snapshot).
> Kept as the November 2026 living document.

---

## Executive Summary

OntoBricks is a Databricks-native semantic engineering platform. Teams design
OWL ontologies, map Unity Catalog data, build interchangeable Lakehouse /
Lakebase / Neo4j knowledge graphs, apply rules and reasoning, and publish
governed graph capabilities through REST, GraphQL, Graph Chat, and MCP.

**v0.9.3 is the current patch line** on the 0.9.x product: hierarchy instance
IRI scope, consistent registry import for cross-env promotion, configurable
Graph Chat token budgets, and docs that match shipped transfer paths (OBX +
GitHub). See `releases/ReleaseNotes_V0.9.3.md`.

**v0.10.0** (Asana section largely complete) is the next major product line:
Intelligence / Simulation sandboxes, Mapping Studio consolidation, Knowledge
Pages + Genie/Domains as Generate sources, Business Rules overhaul
(Simple / Complex), Ontology Sanity Checks, GraphDB backend cloisonnement,
pattern & community detection jobs, and Dependabot hardening.

**v0.11.0** is the forward November focus on the Asana board: Relationship
Prediction, Incremental Triple Materialization, admin LLM allow-list,
Marketplace packaging, domain storage isolation design, industry-catalog
pilots (QUDT / PROV-O), external API surface expansion, Arrows.app import,
enumeration attributes, and UX/governance follow-ups.

Toward GA (**v0.12.0+**) we keep investing in Ontos exchange, Neptune /
Cosmos connectors, fine-grained governance, scale / federation, and Hub /
Marketplace distribution — sequenced after the 0.11.0 spine.

1. **Close 0.10.0** — ship the Intelligence / Simulation / Mapping / Genie
   line that is already marked done on the product board.
2. **Authoring & analytics depth (0.11.0)** — predictive relationships,
   incremental builds, sanity autofix, Arrows / enumerations, LLM control.
3. **Enterprise distribution** — Marketplace, stable API surface, storage
   isolation, SSO/SCIM-ready identity story.
4. **Platform exchange** — Ontos (after API validation), Collibra (explore),
   additional graph stores.

---

## Market Context

### Why the semantic layer matters

- **AI grounding** — agents need governed entities, relationships, actions,
  and live attributes rather than unstructured prompt stuffing.
- **Data-product semantics** — Unity Catalog governs physical assets;
  ontologies add business meaning, constraints, inheritance, and cross-domain
  relationships.
- **Open standards** — OWL, RDFS, SKOS, SHACL, SWRL / IF-THEN, R2RML, SPARQL,
  REST, GraphQL, and MCP reduce proprietary lock-in.
- **Graph choice without redesign** — the same ontology and mapping model can
  target Lakehouse, Lakebase, or Neo4j, or publish as ontology-only.
- **Databricks-native operations** — source data remains governed in Unity
  Catalog while OntoBricks provides design, build, reasoning, exploration, and
  AI publication in one Databricks App.

### Strategic position

OntoBricks is the semantic authoring and publication layer for the Databricks
Lakehouse. It does not require every domain to materialize a graph, and it
does not force every graph into one engine.

November’s differentiator is **decision-grade graph work**: what-if
simulation and Intelligence analytics on 0.10.0, then predictive links and
incremental materialization on 0.11.0 — while keeping promotion paths honest
(OBX + GitHub; no undocumented CLI).

---

## How to read this document

- **Available now** — shipped in the 0.9.x line (through v0.9.3).
- **Closing — v0.10.0** — major themes marked complete on the Asana
  Version 0.10.0 section; release packaging and operator docs still finalize.
- **Next — v0.11.0** — incomplete Asana Version 0.11.0 items; directional.
- **Toward GA — v0.12.0+** — identity, connectors, federation, Hub.
- **Exploring** — evaluation items; not assigned to a release.

---

## Available now — v0.9.x (through 0.9.3)

### Semantic authoring

- Visual ontology design (Studio) with entity, relationship, attribute,
  inheritance, constraint, action, virtual-attribute, and inference-rule
  authoring; multi-select Business Views; several UC datasets per class
  including metric views (0.9.2).
- OWL/RDFS/SKOS/SHACL import/export plus R2RML mapping generation.
- LLM-assisted three-stage ontology Generate and Auto-Mapping with live agent
  activity and durable audit reports.
- Knowledge Store (Lakebase parsed corpus) shared by Generate and Mapping.
- Opt-in **hierarchy instance IRI scope** so parent/subclass mappings can
  share one node (0.9.3).

### Graph and reasoning

- Per-domain **Lakehouse**, **Lakebase**, **Neo4j**, or **No Backend**.
- Lakehouse materialized-table or live-view modes; Build vs Query warehouses;
  optional Lakehouse//RT reads.
- OWL 2 RL, SWRL (including builtins and per-entity rules), Data Quality,
  cohorts, Graph Analytics, Explorer, Graph Chat (configurable `max_tokens`),
  SPARQL playground with typed value `FILTER` → SQL (0.9.2).

### Publication, promotion, and integration

- PUBLISHED-only external REST and GraphQL data plane.
- MCP with end-user identity past the Apps proxy, session-safe `domain_name`,
  per-domain tool/context policy, bridges, actions, virtual attributes.
- **OBX** export/import and **GitHub domain mirror** for cross-env promotion
  (Add missing versions / Rename / Skip — 0.9.3). No shipped registry-transfer
  CLI; docs match reality (#227).
- DRAFT → IN-REVIEW → PUBLISHED lifecycle; Databricks-group permissions; dual
  Volume/Lakebase registry; DAB deployment.

---

## Closing — v0.10.0

Themes already closed on the Asana **Version 0.10.0** section (operator
release still in flight). Treat as “product complete on the board,” not as a
published tag until release notes for 0.10.0 ship.

| Theme | What it delivers |
|-------|------------------|
| **Intelligence menu** | Simulation renamed/restructured into Configuration / Discovery / Simulation; Analytics under Intelligence |
| **What-if Simulation** | Copy-on-write sandboxes, hypothetical facts (+ NL agent), inference against sandbox, public Simulation API |
| **Pattern & community detection** | Databricks jobs (`--task patterns` / `communities`), UC snapshots, Intelligence UI |
| **Mapping Studio consolidation** | Mapping under Graph/Build (Informations / Visual / List), Auto-Map & Diagnostics as modals |
| **Knowledge Pages + Genie/Domains** | Browse/select Genie Pages and Databricks Domains as Generate context; Documents rename |
| **Business Rules overhaul** | Simple / Complex (SHACL-AF / IF-THEN), SPARQL-CONSTRUCT→SQL, rules import/export, rule assistant |
| **Ontology Sanity Checks** | Advisory schema lint + propose-then-apply autofix for orphans |
| **GraphDB cloisonnement** | Capability interface + factory (Delta / Lakebase / Neo4j), lock-in guardrail |
| **Studio groups & multi-dataset** | Business Views ownership; several UC datasets / metric views per class (#221) |
| **Generate at scale** | Paged Stage-3 completion for large schemas (#173) |
| **Security** | Dependabot floors / OSV audit continuation |

GitHub milestone **v0.10.0** still tracks a few open items (for example Graph
Chat conversation sharing [#219](https://github.com/databrickslabs/ontobricks/issues/219),
waiting-for-info ontology reuse [#126](https://github.com/databrickslabs/ontobricks/issues/126)
/ [#42](https://github.com/databrickslabs/ontobricks/issues/42)).

---

## Next — v0.11.0

Directional focus from the Asana **Version 0.11.0** section (incomplete as of
2026-11-16). Ordering below is thematic, not a delivery schedule.

### Intelligence and build economics

- **Relationship Prediction** — type-aware structural link prediction
  (Common Neighbours / Adamic-Adar family) as an Intelligence Discovery
  capability; Databricks job + UC snapshot; no automatic write-back of edges.
- **Incremental Triple Materialization** — builds whose cost tracks what
  changed (full → incremental → microbatch). Design under review; Phase 1
  targets slot pipelines / MVs with fallback; realtime / Kafka deferred.

### Authoring quality

- **Ontology Sanity Checks autofix engine** — interactive preview, selective
  apply, audit / undo on top of 0.10.0 advisory lints.
- **Arrows.app import tab** — convert arrows JSON sketches to OWL Turtle,
  preview, then Replace/Append through the existing OWL pipeline (CAFDS POC).
- **First-class Enumeration attribute type** — allowed-value lists in Studio,
  mapping validation, SHACL/DQ generation.
- **Virtual attributes in business rules / conditions** — evaluate whether
  UC-function attributes can participate safely in IF-THEN / conditions.

### Platform control and distribution

- **Admin LLM allow-list** — administrators choose which Gateway / serving
  models OntoBricks features may use.
- **Marketplace integration / deployment** — packaging path toward one-click
  install (listing/review is partly non-code).
- **Domain storage isolation (design spike)** — dedicated Lakebase / Lakehouse
  schemas per domain; migration story for multi-domain estates.
- **External API & MCP expansion** — dashboards, actions, and datasets on
  `/api/v1`; richer MCP inspection; documented auth.
- **Unsaved-changes guard** — dirty-state protection across Domain, Ontology,
  Mapping, Graph, and Simulation workspaces.

### Industry catalogs (pilot)

- First import batch: **QUDT** and **PROV-O**, following the existing FIBO /
  CDISC / IOF / FHIR catalog pattern.

---

## Toward GA — v0.12.0 and later

From the Asana **Version 0.12.0** section and longer-horizon pillars:

- **Ontos integration** — bidirectional exchange once the external API,
  auth, and conflict model are validated (discovery-heavy; low schedule
  confidence until scoped).
- **Ecosystem connectors** — Amazon Neptune (RDF/SPARQL) and Azure Cosmos DB
  (Gremlin) on the existing graph-store factory.
- **Governance & access** — scoped API keys, durable Delta audit, per-domain /
  per-version RBAC via Unity Catalog grants.
- **Scale, deploy & federation** — large-graph cursor pagination, one-command
  deploy, guided engine migration, cross-workspace federation.
- **Collibra metadata import** — explore / request only until API and asset
  model are scoped.
- **Hub + Marketplace GA** — community ontologies / templates and Marketplace
  one-click on a single bootstrap model.

---

## Exploring

Capabilities under evaluation. They inform the vision; they are not committed
to a release train.

- Full industry-catalog evaluation matrix (32 candidates; High fit includes
  BFO/CCO, CIM, Brick, RealEstateCore, Allotrope, AAS, GS1, OMOP, DCAT-AP).
- Graph provenance / explainability inspired by open semantic stacks
  (triple-level PROV-O, instance-value conflicts, MCP “why” tools,
  `ontobricks doctor`) without taking on foreign SDK dependencies.
- Selective entity-first Auto-Map as the single mapping entry point (October
  vision; still the highest-leverage authoring gap if not absorbed earlier).
- i18n foundations once target languages and ownership are decided.
- Scripted / CI registry promotion CLI (thin wrapper over OBX) — explicitly
  **not** scheduled; OBX + GitHub remain the supported paths (#227 follow-up).

---

## How the pieces depend on each other

1. **0.10.0 Intelligence / Simulation → 0.11.0 Relationship Prediction** —
   Discovery jobs and UC snapshot patterns reuse the same job + Intelligence
   shell.
2. **Sanity Checks (0.10.0) → Autofix engine (0.11.0)** — advisory lint must
   stay stable before one-click repair.
3. **Genie / Domain browse (0.10.0) → Ontos (0.12.0)** — one asset and conflict
   model across platform ontology surfaces.
4. **Storage isolation design (0.11.0) → federation / Marketplace GA** —
   durable per-domain schemas before cross-workspace access.
5. **Stable graph-store contract → Neptune / Cosmos** — connectors preserve
   capabilities and declare gaps explicitly.
6. **External write design → API keys and stable v1** — draft/agentic
   operations wait on auth, idempotency, and deprecation rules.
7. **Incremental materialization → realtime** — Phase 3 Kafka/realtime stays
   behind a working incremental / microbatch path.

---

## Feature Matrix

| Capability | v0.9.x | v0.10.0 (closing) | Direction |
| --- | --- | --- | --- |
| Ontology Studio / multi-dataset | Delivered | Groups / Business Views harden | Operate |
| Hierarchy instance IRI scope | **0.9.3** | — | Operate |
| Registry OBX + GitHub promotion | Delivered | Import semantics harden (0.9.3) | Operate; CLI exploring only |
| Genie / Databricks Domain as source | Not available | **Delivered (board)** | Ontos after API |
| What-if Simulation / Intelligence | Partial Analytics | **Delivered (board)** | Relationship Prediction next |
| Business Rules Simple / Complex | SWRL-era | **Overhaul (board)** | Virtual attrs in rules exploring |
| Selective Auto-Map only | Not available | Mapping Studio consolidated | Still a gap / exploring |
| Admin LLM allow-list | Partial (No LLM + picker) | — | **0.11.0** |
| Incremental materialization | Full builds | — | **0.11.0** |
| Marketplace one-click | Packaging foundations | — | **0.11.0 → GA** |
| Neptune / Cosmos | Not available | — | Toward GA |
| Fine-grained API / RBAC / audit | Partial | — | Toward GA |

---

## Graph Engine Comparison

| Capability | Lakehouse | Lakebase | Neo4j | No Backend |
| --- | --- | --- | --- | --- |
| Storage | Delta table or live UC view | Postgres graph tables | Native property graph | None |
| Source governance | Full Unity Catalog | UC source + managed Postgres | UC source + external graph | Ontology in registry |
| Graph query | SQL/SPARQL translation, GraphQL | SQL/SPARQL subset, GraphQL | Cypher adapter, GraphQL | Not available |
| Reasoning materialization | Generated Delta companion | Generated Postgres companion | Backend-dependent; purge unsupported | Not available |
| Simulation sandboxes | Copy-on-write (0.10.0) | Copy-on-write (0.10.0) | Copy-on-write (0.10.0) | Not available |
| Best fit | Governed/live Lakehouse data | Databricks-native graph persistence | Existing graph-database estates | Semantic contracts and MCP ontology |

---

## Open product questions

These are the decisions that still shape the vision. They are not a backlog.

1. **Auto-Map candidate source** — mapping selection from the validated
   Generate entity set, the full ontology, or both with explicit provenance.
2. **Genie / Domain / Ontos boundaries** — one asset and conflict model
   across platform ontology surfaces.
3. **Incremental vs full rebuild defaults** — when the product chooses
   incremental automatically, and how operators force a full rebuild.
4. **Domain storage naming and migration** — fixed generated schema names
   versus operator selection for existing multi-domain installations.
5. **Predicted relationships write-back** — snapshot-only (0.11.0 design)
   versus optional graph mutation with audit.
6. **LLM availability policy** — global allow-list, feature-specific
   compatibility matrix, and fallback behavior.
7. **External write authentication** — scoped API keys versus a narrower
   Databricks-authenticated contract first.
8. **Registry promotion CLI** — whether a thin OBX CLI is worth a release
   versus GitHub Actions over the mirror.
9. **Industry-catalog go/no-go** — which evaluation targets become
   first-class Import catalogs after QUDT / PROV-O.

---

## Share feedback

The most useful input is a concrete use case, not a feature request in the
abstract.

- **Simulation / Relationship Prediction** — which decisions you would run
  before writing triples, and what “good enough” precision looks like.
- **Incremental builds** — domain size, change rate, and acceptable staleness.
- **Genie Pages, Databricks Domains, or Ontos** — desired direction of
  exchange and conflict behavior.
- **Industry catalogs** — licence, canonical OWL, and a Lakehouse mapping
  sketch.
- **Enterprise governance** — API credential, RBAC, audit retention, and
  cross-workspace requirements.
- **Cross-env promotion** — whether OBX + GitHub cover your pipeline, or you
  need headless automation (size, auth, trigger).

Describe the domain, expected outcome, security boundary, scale, and how you
would know it worked. That is what turns a vision item into a release.
