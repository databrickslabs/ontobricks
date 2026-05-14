# OntoBricks — Product Roadmap

> **Version:** 0.3.x → beyond  
> **Last updated:** 2026-05-13  
> **Status:** Living document — updated after each release

> **Disclaimer:** This roadmap represents the current product direction and planned investments as of the date above. It is provided for informational purposes only and is subject to change at any time without notice. The features, timelines, and priorities described here are aspirational and do not constitute a commitment, promise, or legal obligation to deliver any specific functionality by any specific date. Actual releases may differ materially from what is described here.

---

## Executive Summary

OntoBricks is the only Databricks-native knowledge graph builder that combines ontology design, LLM-powered automation, formal reasoning, and interactive graph exploration in a single deployable App. Version 0.3.x is the current stable release, built on Delta Lake as the primary triple-store backend.

The next phases of the roadmap focus on three strategic axes:

1. **Lakebase as the primary triple store** — replacing the embedded graph engine with a fully-indexed, SPARQL-capable, transactional Postgres backend (v0.4.0, shipping Q2 2026).
2. **SPARQL federation** — federate queries across multiple domain graphs in a single Lakebase instance (v0.5.0).
3. **Neo4j connector** — extend the pluggable graph engine to the industry's leading native graph database, opening OntoBricks to hybrid Lakehouse + graph deployments (v0.6.0).

---

## Market Context

### Knowledge graph adoption trends

The knowledge graph market is growing rapidly, driven by:

- **AI grounding**: LLMs need structured, governed knowledge bases to avoid hallucinations. Knowledge graphs provide exactly that.
- **Data product thinking**: organizations are shifting from raw tables to versioned, semantic data products — ontologies are the schema layer.
- **Regulatory pressure**: FIBO (finance), CDISC (pharma), GDPR/data lineage requirements all push toward formal semantics.
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

## Current State — v0.3.x (May 2026)

### Triple-store backends

| Backend | Status | Use case |
|---|---|---|
| **Delta Lake (SQL Warehouse)** | GA | Default; governed, UC-lineage, liquid clustering |

### Core capabilities

- **Ontology Design** — visual OntoViz canvas, LLM wizard, industry-standard import (FIBO, CDISC, IOF), OWL/RDFS import/export
- **Data Mapping** — R2RML generation, LLM auto-map, attribute-level SQL mapping
- **Reasoning** — OWL 2 RL, SWRL, SHACL data quality
- **Knowledge Graph** — Sigma.js exploration, community detection, cohort discovery, bridge navigation
- **External access** — REST API, auto-generated GraphQL, MCP Server
- **Registry** — dual-mode (Volume / Lakebase), scheduler, version management
- **Security** — CSRF protection, secure cookies, RBAC via Databricks App permissions

### Known limitations (targeted in next releases)

- Single triple-store backend (Delta Lake only) — no Postgres / graph DB option
- No SPARQL federation across multiple domain graphs
- No native Neo4j / property graph export
- No cross-workspace domain federation

---

## Roadmap

### v0.4.0 — Lakebase as Primary Triple Store (June 2026)

**Theme:** replace the embedded graph engine with Lakebase (Databricks-managed Postgres Autoscaling) as a first-class, production-grade triple store.

#### Why this matters

Lakebase Postgres is Databricks' managed, autoscaling Postgres service. Using it as the triple store means the knowledge graph lives inside the Databricks platform with no separate infrastructure, full UC lineage via Lakeflow sync, and transactional consistency for reasoning.

#### Key capabilities

- **SPARQL-over-Postgres** — translate SPARQL SELECT/CONSTRUCT to native Postgres SQL without going through Spark, enabling sub-second graph queries
- **Named graphs (quads)** — per-domain named-graph isolation enables federated queries and clear separation between base data and inferred triples
- **Optimized multi-hop traversal** — purpose-built index layout covering all triple access patterns, matching the lookup performance of dedicated triple stores
- **Transactional reasoning** — OWL 2 RL and SWRL inferred triples land in the same database transaction as the build, guaranteeing consistency
- **Near-real-time refresh** — incremental triple UPSERT + Lakeflow CDC propagation, without requiring a full rebuild
- **Lakeflow managed-sync** — delegate bulk R2RML data movement to a Lakeflow snapshot pipeline; the app only orchestrates, not ingests

#### Also in v0.4.0 — Stabilization

| Item | Type | Priority |
|---|---|---|
| Fix pre-existing test failures | Bug | P0 |
| Security dependency updates | Security | P0 |
| Support non-HTTP(S) URIs in Lakebase reasoning materialization | Bug | P1 |
| Cohort discovery: persist rule library to registry (not session-only) | Feature | P1 |
| E2E tests for Build → Knowledge Graph happy path | Test | P2 |

---

### v0.5.0 — Lakebase SPARQL Federation (July 2026)

**Theme:** federate SPARQL queries across multiple domain graphs in a single Lakebase Postgres instance.

| Capability | Description |
|---|---|
| Cross-domain SPARQL queries | Query multiple domain graphs in a single SPARQL statement |
| OntoBricks Query Playground | Interactive SPARQL editor with live result table |
| Query explain view | Inspect the translated SQL and execution plan for any SPARQL query |

---

### v0.6.0 — Neo4j Connector (August 2026)

**Theme:** add Neo4j (Community, Enterprise, AuraDB) as a graph engine alongside Delta Lake and Lakebase, enabling customers with existing Neo4j infrastructure to use OntoBricks as their semantic design and mapping front-end.

#### Why this matters

Neo4j is the dominant graph database with 40%+ market share. Customers in finance, healthcare, and telco often have existing Neo4j deployments. A native connector means:

- **No data duplication** — triples are materialized directly into Neo4j as nodes and relationships; no intermediate Delta table needed
- **Native graph queries** — Cypher traversal, shortest path, and graph algorithms run on Neo4j; OntoBricks handles ontology design and mapping
- **Hybrid Lakehouse + graph** — raw data stays in Delta/UC; the knowledge graph lives in Neo4j; OntoBricks bridges both worlds
- **Removes the last objection** for prospects evaluating OntoBricks against a pure graph-DB-plus-ETL approach

#### OWL → Property Graph mapping

| OWL concept | Neo4j representation |
|---|---|
| Class | Node label |
| Object property | Relationship type |
| Datatype property | Node property |
| Sub-class | Additional label on child node |
| Inferred triple (SWRL/OWL) | Node/relationship with `:Inferred` marker |
| Named graph | Neo4j database (Enterprise) or label prefix (Community) |

#### Key capabilities

- Batch node and relationship upsert from the OntoBricks build pipeline
- Typed node label promotion from `rdf:type` triples
- SWRL violation detection via Cypher
- Knowledge Graph visualization sourced from Neo4j via Bolt
- Health-check and connection status in the Settings UI
- AuraDB support with automatic connection string detection
- Optional install — zero impact on Volume-only deployments

---

### v0.7.0 — Enterprise Hardening (September 2026)

**Theme:** prepare OntoBricks for large enterprise deployments with strict governance, performance, and multi-tenancy requirements.

| Feature | Description |
|---|---|
| **Fine-grained RBAC** | Per-domain, per-version read/write/admin roles via Unity Catalog grants |
| **Multi-workspace federation** | Cross-workspace domain registry sync — read a domain built in workspace A from workspace B |
| **Audit log** | Every build, reasoning run, and mutation emits a structured event to a Delta audit table |
| **Large-graph pagination** | Server-side cursor pagination for 10k+ node knowledge graphs |
| **Scheduled reasoning** | Trigger OWL 2 RL / SWRL inference on a cron schedule |
| **API key authentication** | Scoped API keys for external REST and GraphQL consumers |
| **One-command deployment** | Single DAB deploy installs OntoBricks + MCP server + registry together |

---

### v1.0.0 — General Availability (November 2026)

**Theme:** stable API contract, enterprise SLA documentation, and ecosystem integrations.

| Item | Description |
|---|---|
| **Stable REST API v1** | SemVer enforced; deprecation policy documented; no breaking changes |
| **Amazon Neptune connector** | RDF/SPARQL 1.1 over HTTPS |
| **Azure Cosmos DB connector** | Gremlin API; property graph mapping |
| **OntoBricks Hub** | Public registry of community ontologies and mapping templates |
| **Databricks Marketplace** | One-click install from the Databricks Marketplace |
| **SSO / SCIM provisioning** | Enterprise identity integration |

---

## Feature Matrix

| Feature | v0.3 | v0.4 | v0.5 | v0.6 | v0.7 | v1.0 |
|---|---|---|---|---|---|---|
| Delta Lake triple store | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Lakebase named-graph triple store** | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Lakebase optimized indexes** | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Lakebase SPARQL SELECT** | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| **SPARQL federation (cross-domain)** | — | — | ✅ | ✅ | ✅ | ✅ |
| **Neo4j connector** | — | — | — | ✅ | ✅ | ✅ |
| Fine-grained RBAC | — | — | — | — | ✅ | ✅ |
| Multi-workspace federation | — | — | — | — | ✅ | ✅ |
| Amazon Neptune | — | — | — | — | — | ✅ |
| Databricks Marketplace | — | — | — | — | — | ✅ |

---

## Graph Engine Comparison (v0.4+)

| Capability | Delta Lake | Lakebase (v0.4) | Neo4j (v0.6) |
|---|---|---|---|
| **Storage** | Delta table in UC | Postgres (Lakebase Autoscaling) | Neo4j database or AuraDB |
| **Query language** | Spark SQL | Postgres SQL + SPARQL subset | Cypher |
| **SPARQL support** | Via Spark SQL translation | Native | Via OntoBricks adapter |
| **Named graphs** | Per-domain Delta table | ✅ | ✅ |
| **Transactional reasoning** | Append only | ✅ | ✅ |
| **Multi-hop traversal** | Recursive CTE (Spark) | Optimized indexes + CTE | Native Cypher (best-in-class) |
| **Governance / lineage** | Full UC lineage | UC synced table | External |
| **Deployment** | Built-in | Optional extra | Optional extra |
| **Best for** | Production, governed data | Databricks-native + SPARQL | Customers with existing Neo4j |

---

## Open Questions

1. **Lakebase SPARQL subset scope** — BGP + FILTER covers 80% of use cases; OPTIONAL and UNION add another 15%. Aggregates and property paths are deferred to a later patch.

2. **Neo4j Community vs Enterprise** — named graphs as separate databases require Neo4j Enterprise. Community edition support will use label prefixing as a documented workaround.

3. **Triple store migration UX** — when an admin switches engine (e.g., Delta → Neo4j), OntoBricks will require a rebuild rather than offering an inline migration wizard. A migration assistant is considered for v0.7.

---

## How to Contribute

The graph engine abstraction is designed for external contributions. Adding a new store requires implementing the `GraphStore` interface, registering the engine in `GraphDBFactory`, adding an optional dependency group, providing a Settings UI card, and writing unit tests with a mock driver.

See `docs/graphdb-integration.md` for the full engine abstraction contract.
