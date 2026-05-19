# Integration Modes — Flexible vs Deterministic

OntoBricks exposes its knowledge graph through two complementary access patterns:

| Mode | Driven by | Read path | When to use |
|---|---|---|---|
| **Flexible** | LLM agent (via MCP, chat, playground) | LLM chooses tool calls at runtime | Exploratory analysis, conversational Q&A, ad-hoc traversal where the question shape is open-ended |
| **Deterministic** | External orchestrator, application code, or pre-authored rule | Fixed traversal / rule / query; no LLM in the execution path | Repeatable workloads, programmatic integration, scenarios where the same input must always produce the same output |

Both modes operate against the **same triple store** and the **same domain version**. The choice is about *who decides what to ask and how*, not about which data is exposed.

This page is a primer. For the concrete tool surfaces see [`mcp.md`](mcp.md) (Flexible), [`api.md`](api.md) (Deterministic REST + GraphQL), [`cohort_discovery.md`](cohort_discovery.md) (Deterministic rule engine), and [`data-access.md`](data-access.md) (full engine map).

---

## 1. Flexible mode

Flexible mode is the path most users meet first: an LLM client (Cursor, Claude Desktop, the Databricks Playground) connects to the MCP server, lists available domains, picks one, and starts asking questions. The LLM decides which tool to call (`list_entity_types`, `describe_entity`, `query_graphql`, …), interprets the result, and assembles an answer in natural language.

**Characteristics:**

- The question shape is **not known in advance**. The LLM may take different paths for the same intent across runs.
- Execution path includes an LLM at one or more steps. The same prompt may produce different tool sequences.
- Strong for **discovery**: "what entities exist?", "show me everything you know about customer X", "summarize the relationships around this node".
- Stateless from OntoBricks' perspective — each tool call is independent.

**Surfaces:**

- [`docs/mcp.md`](mcp.md) — MCP tools (`list_domains`, `select_domain`, `list_entity_types`, `describe_entity`, `get_graphql_schema`, `query_graphql`, …).
- Chat-based ontology assistants embedded in the OntoBricks UI (`agent_ontology_assistant`, `agent_owl_generator`).

---

## 2. Deterministic mode

Deterministic mode is the path for code that needs to behave the same way every time. The caller — an orchestrator, a backend service, a scheduled job, or a saved rule — specifies the traversal or query up front, and OntoBricks executes it without inserting an LLM into the read path.

**Characteristics:**

- The question shape is **fixed at design time** (a rule, a SPARQL query, a parameterized traversal).
- Same input → same output. Idempotent, replayable.
- Strong for **regulated workloads, BI integration, scheduled materialization, agent tools that need predictable contracts.**
- Auditable: the execution plan is inspectable, not generated on the fly.

**Surfaces available today:**

| Surface | What it does | Reference |
|---|---|---|
| **Cohort Discovery** | Pure-Python 6-stage rule engine that produces deterministic, content-hashed cohort URIs from declarative `CohortRule` JSON. The canonical example of a deterministic consumer in production. | [`docs/cohort_discovery.md`](cohort_discovery.md), `src/back/core/graph_analysis/CohortBuilder.py` |
| **REST triple traversal** | `GET /api/v1/digitaltwin/triples/find` — parameterized BFS traversal (`entity_type`, `search`, `depth`). Callable from any HTTP client without invoking an LLM. | [`docs/api.md`](api.md), `src/api/routers/digitaltwin.py` |
| **GraphQL** | Auto-generated typed schema per domain; structured queries with nested traversal. Per-domain GraphiQL playground for development; programmatic clients in production. | [`docs/api.md`](api.md) |
| **SHACL validation** | Shape-based data quality validation; in-memory PySHACL or SQL-compiled execution against the triple store. | `src/back/core/w3c/shacl/SHACLService.py` |
| **OWL 2 RL & SWRL reasoning** | Deductive closure and Horn-clause rules — same input, same inferred triples. | `src/back/core/reasoning/ReasoningService.py` |
| **R2RML materialization** | Declarative mapping from UC tables to triples; the build is reproducible from the mapping artifact. | [`docs/architecture.md`](architecture.md) |

---

## 3. Hybrid — LLM proposes, deterministic engine executes

The two modes are not mutually exclusive. Several features already combine them in a controlled way: an LLM helps **author** a declarative artifact, then a deterministic engine **executes** it.

**Canonical example — Cohort Discovery Stage 2:**

The cohort designer ships a Stage 2 NL agent (`src/agents/agent_cohort/`) that takes a free-text prompt ("group persons who share a project and have status Exempt") and proposes a validated `CohortRule` JSON. The agent has six **read-only** tools (`list_classes`, `list_properties_of`, `count_class_members`, `sample_values_of`, `propose_rule`, `dry_run`) and **never writes**. The proposed rule lands in the **Build rule** tab; the human reviews it; saving goes through the Builder-protected endpoint; execution runs the deterministic 6-stage engine.

This pattern — *LLM in the authoring loop, no LLM in the execution loop* — lets you use natural language to lower the authoring barrier without giving up reproducibility or auditability at run time.

Other places this pattern applies in OntoBricks today:

- Ontology generation (`agent_owl_generator`) proposes OWL/Turtle; the human accepts; downstream reasoning and materialization are deterministic.
- Auto-mapping (`agent_auto_assignment`) proposes attribute-level R2RML mappings; the human reviews; R2RML execution is deterministic.

---

## 4. Choosing a mode

Use the table below as a starting point.

| Need | Mode | Surface to use |
|---|---|---|
| "Let me chat with my knowledge graph" | Flexible | MCP server, in-app chat assistants |
| "I want to expose the graph to Claude / Cursor / the Playground" | Flexible | MCP server |
| "I want a typed schema for my application to query" | Deterministic | GraphQL |
| "I need a stateless HTTP endpoint to look up an entity and its neighbours" | Deterministic | `GET /api/v1/digitaltwin/triples/find` |
| "I want business users to author rules for grouping entities and re-run them on every refresh" | Deterministic | Cohort Discovery |
| "I want data quality gates that block bad triples before materialization" | Deterministic | SHACL + Reasoning |
| "I want NL authoring, but predictable execution" | Hybrid | Cohort Stage 2 agent, OWL generator, Auto-map |

A workload can use both modes side by side: a single domain can be queried via MCP from a Playground session *and* via GraphQL from a backend application *and* have a saved Cohort rule that materializes on schedule — all three reading the same triple store.

---

## 5. What strengthens each mode

The OntoBricks roadmap (see GitHub Discussion [#25](https://github.com/databrickslabs/ontobricks/discussions/25)) introduces capabilities relevant to both modes:

- **Lakebase as primary triple store (v0.4):** SPARQL-over-Postgres, named graphs, transactional reasoning — improves deterministic-mode latency and consistency.
- **SPARQL federation (v0.5):** Query Playground and Explain view — improves deterministic-mode developer ergonomics.
- **Fine-grained RBAC, audit log, scheduled reasoning (v0.7):** strengthens operational and governance guarantees for both modes.

Discussion #25 is the canonical place to track and contribute to these capabilities.

---

## See also

- [`docs/architecture.md`](architecture.md) — overall system architecture.
- [`docs/data-access.md`](data-access.md) — engine map showing exactly which wrapper every UI / MCP / chat feature uses.
- [`docs/cohort_discovery.md`](cohort_discovery.md) — the canonical deterministic-mode walkthrough.
- [`docs/mcp.md`](mcp.md) — flexible-mode tool reference.
- [`docs/api.md`](api.md) — REST + GraphQL reference for deterministic-mode callers.
