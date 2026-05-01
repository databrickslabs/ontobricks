# Data Access Wrappers — Engine Map

This document inventories every way the OntoBricks platform reads (or asks the
backend to read) data, and pins each user-facing feature to the underlying
query engine(s) it ends up using.

The goal is to answer, for any feature in the **UI**, the **MCP server**, or
the **Graph Chat** assistant: *"when I click this, which engine actually
runs?"*

---

## 1. The five access wrappers

OntoBricks exposes data through five layered wrappers. Three are user-facing
languages, two are engine-facing dialects the platform translates *down to*.

| Wrapper | Layer | Implementation | Where the bytes finally land |
|---|---|---|---|
| **REST** | user-facing | `api/external_app.py`, `api/routers/...`, `api/routers/internal/dtwin.py` | Calls into Python services (`DigitalTwin`, `DomainQueryService`, registry, …) |
| **GraphQL** | user-facing | `back/core/graphql/GraphQLSchemaBuilder.py`, `ResolverFactory.py`, `back/fastapi/graphql_routes.py` | Resolvers internally call SPARQL / triples-find → Spark SQL |
| **SPARQL** | user-facing (read-only in chat) | `back/core/w3c/sparql/SparqlTranslator.py`, `SparqlQueryRunner.py`, `DomainQueryService.py` | Translated to **Spark SQL** via R2RML mappings |
| **Spark SQL** | engine-facing | `back/core/triplestore/delta/DeltaTripleStore.py` | Native execution on Databricks SQL Warehouse against Delta views in Unity Catalog |
| **Cypher** | engine-facing | `back/core/graphdb/GraphDBBackend.py` + concrete engines under `back/core/graphdb/<engine>/` (`supports_cypher = True`, `query_dialect = "cypher"`) | Native execution on the embedded GraphDB engine (default: LadybugDB, file-based `.lbug`) |

### Supporting W3C standards (not query languages)

These layers **shape** queries but don't run them — they are mappings,
constraints, and rule definitions:

| Standard | Role | Where |
|---|---|---|
| **R2RML** | Maps relational tables → RDF; consumed by `SparqlTranslator` to lift SPARQL to Spark SQL | `back/core/w3c/r2rml/` |
| **SHACL** | Constraint validation over RDF (data quality) | `back/core/w3c/shacl/SHACLService.py` |
| **OWL / RDFS** | Schema definition + entailment (`OWLRLReasoner` materializes derived triples) | `back/core/w3c/owl/`, `back/core/reasoning/OWLRLReasoner.py` |
| **SWRL** | Rule language; compiled to either SQL (`SWRLSQLTranslator`) or Cypher (`SWRLCypherTranslator`) depending on backend | `back/core/reasoning/SWRL*.py` |

---

## 2. Storage backends — which engine speaks which language

Which engine actually runs your query depends on the domain's configured
triple-store backend (`TripleStoreFactory.create(...)`):

| Domain backend | Storage | Native engine | SPARQL path | Cypher path |
|---|---|---|---|---|
| `view` (`DeltaTripleStore`) | Delta view in Unity Catalog | **Spark SQL** (Databricks SQL Warehouse) | SPARQL → Spark SQL via R2RML | n/a |
| `graph` (`GraphDBBackend`) | Engine-specific embedded file (synced to UC Volume) | **Cypher** | SPARQL → SQL or Cypher (subclass-dependent) | Native |

The selection is made in `back/core/triplestore/TripleStoreFactory.py`. By
default a domain uses the **graph** backend; switching to **view** is purely
a configuration change — the user-facing wrappers (REST / GraphQL / SPARQL)
don't change.

> **GraphDB engines.** `GraphDBBackend` is the abstract base class; concrete
> engines live under `back/core/graphdb/<engine>/`. The current
> default/embedded engine is **LadybugDB** (file-based `.lbug`); additional
> engines (e.g. KuzuDB, Neo4j) can be plugged in via `GraphDBFactory`
> without changing any of the user-facing wrappers above.

---

## 3. Decision tree — which wrapper fires for what?

```text
            "I want to ..."
                  │
        ┌─────────┴──────────────────────────────────────┐
        │                                                │
"... explore / browse / mutate"                "... read graph data"
   the registry, ontology,                              │
   mapping, permissions                                 │
        │                                  ┌────────────┼────────────────┐
        │                                  │            │                │
       REST                              SPARQL      GraphQL          Cypher
   (/api/v1/* or /dtwin/*)            (free-form    (typed,       (graph traversal,
                                       triples,     nested,        SWRL rules)
                                       aggregates,  schema-driven)       │
                                       analytics)                        │
                                                                  Internal only
                                                                 (GraphDB engine
                                                                  + reasoning)
```

---

## 4. UI features — engine map

Pages live under `src/front/templates/` and `src/front/static/<page>/js/...`.
Each row maps a UI feature to the wire protocol it speaks (column **Wrapper**)
and the engine that ultimately runs (column **Engine**).

### 4.1 Registry & Domain

| UI Feature | Page / JS file | Endpoint(s) | Wrapper | Engine |
|---|---|---|---|---|
| Browse / open / create / save domain | `templates/registry.html`, `templates/domain.html` | `GET/POST /api/v1/domain/...`, `/registry/...`, `/domain/...` | REST | Python services (`DomainService`, `RegistryService`); files on UC Volumes |
| Schedule (background actions) | `registry-schedule.html` | `/registry/schedule/*` | REST | Python `BuildScheduler` + Databricks Jobs SDK |
| Cockpit / readiness | `domain-validation` | `GET /api/v1/domain/design-status` | REST | Python aggregator over OWL / R2RML / metadata |
| Data Sources (UC tables preview) | `domain-metadata` | Internal REST → `databricks-sql-connector` | REST → Spark SQL | **Spark SQL** on UC tables (sample queries) |
| Versions | `domain-versions` | `/api/v1/domain/versions` | REST | UC Volume listing |

### 4.2 Ontology Designer

| UI Feature | JS file | Endpoint(s) | Wrapper | Engine |
|---|---|---|---|---|
| Visual ontology editor (`Model`, `Entities`, `Relationships`, `Groups`, `Business Views`) | `ontology-design.js`, `ontology-shared-panels.js`, `ontology-groups.js` | Internal REST `/ontology/...` | REST | Python ontology object model |
| OWL viewer / generator | `ontology-owl.js`, agent `OWLGenerator` | `/ontology/owl/...`, `/agents/owl-generator/run` | REST | `OntologyParser`, `OntologyGenerator` (rdflib) |
| Import (OWL, FIBO, CDISC, IOF) | `ontology-import.js` | `/ontology/import/*` | REST | rdflib parsers |
| Generate (Wizard) | `ontology-wizard.js` | LLM endpoint via `agent_owl_generator` | REST → LLM | Databricks Foundation Model API + tool-calling |
| **Data Quality** rules editor | `ontology-dataquality.js` | `/ontology/dataquality/...` | REST | SHACL (`SHACLService`) on the in-memory ontology |
| **Business Rules (SWRL)** editor | `ontology-business-rules.js` | `/ontology/swrl/...` | REST | `SWRLParser`, validated against ontology |
| Expressions & Axioms | `ontology-axioms.js` | `/ontology/axioms/...` | REST | OWL axiom storage |

### 4.3 Mapping (R2RML)

| UI Feature | JS file | Endpoint(s) | Wrapper | Engine |
|---|---|---|---|---|
| Mapping designer / manual mapping | `mapping-design.js`, `mapping-manual.js` | `/mapping/...` | REST | `R2RMLGenerator` |
| Auto-Map (LLM) | `mapping-autoassign.js`, agent `AutoAssignment` | `/agents/auto-assignment/run` | REST → LLM | Databricks FM API; agent samples UC tables via Spark SQL through `databricks-sql-connector` |
| Diagnostics | `mapping-diagnostics.js` | `/mapping/diagnostics/*` | REST | Python validator |
| **R2RML** view | `mapping-r2rml.js` | `/mapping/r2rml/raw` | REST | rdflib serializer |
| **Spark SQL** preview | `mapping-sparksql.js` | `POST /dtwin/translate` | REST → SPARQL **translation only** | `SparqlTranslator` (no execution; shows the generated SQL) |

### 4.4 Digital Twin (the core read surface)

This is where users actually query the knowledge graph. **Knowledge Graph**,
**GraphQL**, and **Graph Chat** all sit under the *Digital Twin* menu; they
hit different wrappers but eventually share the same Delta / GraphDB
storage.

| UI Feature | JS file | Endpoint(s) | Wrapper | Engine |
|---|---|---|---|---|
| **Insight / Overview** | `query-sync.js` (stats panel), `query.js` | `GET /dtwin/sync/stats`, `GET /dtwin/sync/status` | REST | Spark SQL aggregates on the triple view (Delta) **or** GraphDB stats (graph) |
| **Knowledge Graph** (Sigma.js viz) | `query-sigmagraph.js`, `query-d3graph.js` | `GET /dtwin/groups`, `POST /dtwin/sync/filter`, `GET /dtwin/sync/stats?refresh=true`, `POST /dtwin/clusters/detect`, `GET /dtwin/reasoning/inferred` | REST | Each `/sync/filter` call is a **SPARQL** under the hood, translated to **Spark SQL** (Delta backend) or **Cypher** (GraphDB backend) |
| **Knowledge Graph → SPARQL panel** | `query-execute.js` | `POST /dtwin/execute` | **SPARQL** | `SparqlQueryRunner` → **Spark SQL** on the SQL Warehouse (Delta) — never runs raw Cypher even on the GraphDB backend |
| **GraphQL** | `query-graphql.js` | `GET /graphql/{domain}/schema`, `POST /graphql/{domain}`, `GET /graphql/settings/depth` | **GraphQL** | Schema generated from OWL; resolvers call `DomainQueryService` → SPARQL → **Spark SQL** |
| **Graph Chat** | `query-chat.js`, agent `agent_dtwin_chat` | `POST /dtwin/assistant/chat`, `GET/DELETE /dtwin/assistant/history` | REST → LLM tool-calling | LLM calls REST + GraphQL + SPARQL tools (see §6) |
| **Build** (materialize triple store) | `query-sync.js` | `POST /dtwin/sync/start`, `POST /dtwin/sync/load`, `POST /dtwin/sync/reload-from-registry`, `POST /dtwin/sync/drop-snapshot` | REST | `IncrementalBuildService` runs **Spark SQL** INSERTs against Delta, or GraphDB bulk-load into the embedded graph |
| **Data Quality** (SHACL execution) | `query-dataquality.js` | `POST /dtwin/dataquality/start`, `POST /dtwin/dataquality/execute` | REST | `SHACLService` translates shapes to **Spark SQL** SELECTs that find violations |
| **Inference (Reasoning)** | `query-reasoning.js` | `POST /dtwin/reasoning/start`, `POST /dtwin/reasoning/materialize`, `GET /dtwin/reasoning/inferred` | REST | `ReasoningService`: OWL-RL closure (rdflib) **+** SWRL rules compiled to **Spark SQL** (Delta) **or** **Cypher** (GraphDB), **or** `SPARQLRuleEngine` |

### 4.5 Settings / Permissions / Help

| UI Feature | Endpoint(s) | Wrapper | Engine |
|---|---|---|---|
| Settings (warehouse, profile) | `/settings/*` | REST | Python config service |
| Permissions | `/permissions/*` | REST | `PermissionService` (in-memory + UC Volume manifest) |
| Help / docs modal | static templates | n/a | n/a |

---

## 5. MCP server (`src/mcp-server`) — engine map

The MCP server is a **thin proxy** over the OntoBricks public REST + GraphQL
surface (`/api/v1/...` and `/graphql/...`). It runs as its own Databricks
App, authenticates with an M2M OAuth token, and uses `httpx.AsyncClient`. It
**never speaks SPARQL or Cypher directly.**

| MCP Tool | Endpoint hit | Wrapper | Engine |
|---|---|---|---|
| `list_domains` | `GET /api/v1/domains` | REST | UC Volume listing |
| `list_domain_versions` | `GET /api/v1/domain/versions` | REST | UC Volume listing |
| `get_design_status` | `GET /api/v1/domain/design-status` | REST | Python aggregator |
| `select_domain` | `GET /api/v1/digitaltwin/status` | REST | DeltaTripleStore status (Spark SQL) or GraphDB status |
| `list_entity_types` | `GET /api/v1/digitaltwin/stats` | REST | **Spark SQL** GROUP BY on the triple view (Delta) or GraphDB MATCH counts |
| `describe_entity` | `GET /api/v1/digitaltwin/triples/find` | REST | SPARQL-style BFS internally → **Spark SQL** (Delta) or **Cypher** (GraphDB) |
| `get_status` | `GET /api/v1/digitaltwin/status` | REST | Same as `select_domain` |
| `get_graphql_schema` | `GET /graphql/{domain}/schema` | **GraphQL** | `GraphQLSchemaBuilder` (no DB hit; SDL only) |
| `query_graphql` | `POST /graphql/{domain}` | **GraphQL** | Resolvers → SPARQL → **Spark SQL** |
| `ontobricks://*` resources | various | REST / GraphQL | Same as the equivalent tools |

> **Note.** MCP only sees **published** domain versions because `/api/v1/...`
> requires a registry version. To query an unsaved working session, use the
> Graph Chat (next section), which talks to the session-aware `/dtwin/...`
> internal routes instead.

---

## 6. Graph Chat (`src/agents/agent_dtwin_chat`) — engine map

The Graph Chat agent runs **inside** the OntoBricks process, talks to
loopback (`http://localhost:8000`), forwards user session cookies + the
Databricks Apps `X-Forwarded-*` headers, and targets the **internal**
session-aware `/dtwin/...` routes. This means it works on unsaved domains
and (uniquely) speaks SPARQL.

| Chat Tool | Endpoint hit | Wrapper | Engine |
|---|---|---|---|
| `list_entity_types` | `GET /dtwin/sync/stats` | REST | **Spark SQL** stats (Delta) or GraphDB stats |
| `get_status` | `GET /dtwin/sync/status` | REST | Triple store status |
| `describe_entity` | `GET /dtwin/triples/find` | REST | Bounded BFS → **Spark SQL** (Delta) or **Cypher** (GraphDB) |
| `get_graphql_schema` | `GET /dtwin/graphql/schema` | **GraphQL** | Schema built from in-session ontology (no DB) |
| `query_graphql` | `POST /dtwin/graphql/execute` | **GraphQL** | Resolvers → SPARQL → **Spark SQL** |
| `run_sparql` | `POST /dtwin/execute` | **SPARQL** (read-only — DROP/DELETE/INSERT/CREATE/CLEAR/LOAD/COPY/MOVE/ADD blocked by regex) | `SparqlQueryRunner` → **Spark SQL** on the SQL Warehouse |

The system prompt steers the LLM's protocol choice:

- *"Tell me about X"* → `describe_entity` (REST/triples)
- *"How many / group by / aggregate"* → `run_sparql` (SPARQL → Spark SQL)
- *"Customer + orders + products in one payload"* → `get_graphql_schema` then `query_graphql`

---

## 7. Other LLM agents — engine map

These agents do not query the triple store at runtime; they operate on the
**design-time** ontology / metadata.

| Agent | Purpose | Tools call | Wrapper | Engine |
|---|---|---|---|---|
| `agent_owl_generator` | Build an OWL ontology from metadata + documents | `metadata.list_tables`, `metadata.preview_table`, `documents.read`, `ontology.write_owl` | REST + Spark SQL (samples) | `databricks-sql-connector` against UC tables, plus rdflib write |
| `agent_auto_assignment` | Map ontology entities to Spark SQL queries | `tables.list`, `tables.sample`, `mapping.write` | REST + Spark SQL (samples) | Same as above; output stored as R2RML |
| `agent_auto_icon_assign` | Pick emojis for entities | Inspects ontology + metadata | REST | None — generation only |
| `agent_ontology_assistant` | Conversational ontology editing | Dozens of tools mutating the in-session ontology | REST | Python ontology object model |
| `agent_dtwin_chat` | Conversational graph querying | See §6 | REST + **GraphQL** + **SPARQL** | **Spark SQL** + **Cypher** (engine-side) |

---

## 8. Reasoning engine — engine map

The reasoning pipeline (`back/core/reasoning/`) chooses a translation target
based on the active triple-store backend's `query_dialect`.

| Component | Input | Compiled to | Engine |
|---|---|---|---|
| `OWLRLReasoner` | OWL 2 RL fragment + facts | RDF graph operations (rdflib) | Python in-memory; results materialized to triple store via Spark SQL or GraphDB bulk insert |
| `SWRLEngine` + `SWRLSQLTranslator` | SWRL rules | **Spark SQL** | Databricks SQL Warehouse on the Delta triple view |
| `SWRLEngine` + `SWRLCypherTranslator` / `SWRLFlatCypherTranslator` | SWRL rules | **Cypher** | GraphDB engine (graph backend only) |
| `SPARQLRuleEngine` | SPARQL `CONSTRUCT` rules | **SPARQL** → Spark SQL | Same as SPARQL execution path |
| `DecisionTableEngine` | Decision tables | **Spark SQL** | Databricks SQL Warehouse |
| `AggregateRuleEngine` | Aggregate rules | **Spark SQL** | Databricks SQL Warehouse |

---

## 9. Three end-to-end query flows

### 9.1 User clicks **Graph Chat** → "How many Customers per Country?"

```text
Browser  ──POST /dtwin/assistant/chat──▶  api/routers/internal/dtwin.py
                                                  │
                                                  ▼
                                       agent_dtwin_chat.engine.run_agent
                                                  │
                                                  │ tool_call: run_sparql
                                                  ▼
                          POST /dtwin/execute  (loopback HTTP, session cookies)
                                                  │
                                                  ▼
                                  SparqlTranslator (SPARQL → Spark SQL via R2RML)
                                                  │
                                                  ▼
                                  Databricks SQL Warehouse on Delta view
                                                  │
                                                  ▼
                                       rows → markdown table → reply
```

Wrappers in play: REST → SPARQL → **Spark SQL**.

### 9.2 External LLM (Cursor) calls MCP `query_graphql`

```text
Cursor (stdio)  ──FastMCP──▶  src/mcp-server/server/app.py
                                       │
                                       ▼
                       POST /graphql/{domain}  (httpx, M2M OAuth bearer)
                                       │
                                       ▼
                    back/fastapi/graphql_routes.py → ResolverFactory
                                       │
                                       ▼
                              DomainQueryService (SPARQL)
                                       │
                                       ▼
                       Databricks SQL Warehouse on Delta view
```

Wrappers in play: REST (transport) → **GraphQL** (query) → SPARQL → **Spark SQL**.

### 9.3 User opens **Knowledge Graph** tab and applies a filter

```text
Browser  ──POST /dtwin/sync/filter──▶  api/routers/internal/dtwin.py
                                                │
                                                ▼
                                        DigitalTwin.filter()
                                                │
                                                │   backend = "view"          backend = "graph"
                                                ▼                                    ▼
                                  SparqlTranslator             GraphDBBackend.cypher(...)
                                  (SPARQL → Spark SQL)         (e.g. LadybugDB engine)
                                                │                            │
                                                ▼                            ▼
                              Databricks SQL Warehouse           Embedded GraphDB file
                                                                  (default: .lbug)
                                                │                            │
                                                └──────► triples ◀───────────┘
                                                            │
                                                            ▼
                                              Sigma.js renders the graph
```

Wrappers in play: REST → (SPARQL → **Spark SQL**) **or** **Cypher**, depending on backend.

---

## 10. TL;DR matrix — what each surface exposes

| Surface | REST | GraphQL | SPARQL | Spark SQL (engine-side) | Cypher (engine-side) |
|---|:---:|:---:|:---:|:---:|:---:|
| UI — Registry / Domain / Ontology / Mapping | ✓ | | | sample-only | |
| UI — Knowledge Graph (Sigma) | ✓ | | (via filter) | ✓ (Delta backend) | ✓ (graph backend) |
| UI — SPARQL panel | | | ✓ | ✓ | |
| UI — GraphQL tab | | ✓ | (under) | ✓ | |
| UI — Graph Chat | ✓ | ✓ | ✓ | ✓ | (via reasoning only) |
| UI — Inference | ✓ | | (`SPARQLRuleEngine`) | ✓ | ✓ |
| UI — Data Quality (SHACL) | ✓ | | | ✓ | |
| MCP server | ✓ | ✓ | | ✓ | ✓ (graph backend only) |
| Graph Chat agent | ✓ | ✓ | ✓ | ✓ | ✓ (graph backend only) |
| Other agents (OWL gen, auto-map, …) | ✓ | | | ✓ (sampling UC) | |

> **User-visible** query languages today: **REST, GraphQL, SPARQL.**
> **Engine-side** dialects the platform compiles down to: **Spark SQL** (Delta backend) and **Cypher** (GraphDB backend — default engine: LadybugDB).

---

## 11. Source-code anchors (quick links)

- REST routes — `src/api/routers/internal/dtwin.py`, `src/api/routers/v1.py`, `src/api/external_app.py`
- GraphQL — `src/back/fastapi/graphql_routes.py`, `src/back/core/graphql/{GraphQLSchemaBuilder,ResolverFactory,SchemaMetadata}.py`
- SPARQL — `src/back/core/w3c/sparql/{SparqlTranslator,SparqlQueryRunner,DomainQueryService}.py`
- Triple-store factory — `src/back/core/triplestore/TripleStoreFactory.py`
- Delta backend — `src/back/core/triplestore/delta/DeltaTripleStore.py`
- GraphDB backend — `src/back/core/graphdb/GraphDBBackend.py`, `src/back/core/graphdb/GraphDBFactory.py`
- GraphDB engine (default: LadybugDB) — `src/back/core/graphdb/ladybugdb/{LadybugBase,LadybugFlatStore,LadybugGraphStore}.py`
- Reasoning — `src/back/core/reasoning/{OWLRLReasoner,SWRLSQLTranslator,SWRLCypherTranslator,SWRLFlatCypherTranslator,SPARQLRuleEngine,DecisionTableEngine,AggregateRuleEngine}.py`
- MCP server — `src/mcp-server/server/app.py`, `src/mcp-server/mcp_server.py`
- Graph Chat — `src/agents/agent_dtwin_chat/{engine,tools}.py`
