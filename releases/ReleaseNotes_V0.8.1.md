# OntoBricks — Release Notes V0.8.1

**Release window:** September 2026<br>
**Type:** Patch release (v0.8.0 → v0.8.1)<br>
**Test status:** all changes shipped with the non-scenario suite green: 5939 passed, 304 skipped, 6 deselected, 32 warnings.

---

## Highlights

- **Fail-closed Spark SPARQL ([#167](https://github.com/databrickslabs/ontobricks/issues/167))** — Explorer warehouse SPARQL is validated before translation. Unsupported algebra is rejected with a capability error instead of a silent or misleading SQL mapping.
- **Faster `describe_entity` / `/triples/find` ([#182](https://github.com/databrickslabs/ontobricks/pull/182))** — BFS traversal, distinct payload fetch, ordering, and pagination run in one backend query. Type-wide scans no longer materialize hundreds of thousands of triples in Python. Responses keep exact `total` / `entity_count` and add `has_more`.
- **MCP session isolation ([#183](https://github.com/databrickslabs/ontobricks/pull/183))** — concurrent MCP clients on one server process keep their own selected domain and label/action caches. Resource reads use the same session scope as tool calls.
- **Admin-only domain creation** — only Databricks App administrators (`CAN_MANAGE`) can create a new domain. Editors and Builders still save and work on assigned domains.
- **SWRL inference filters** — rules that use string built-ins such as `swrlb:contains` now apply SQL predicates during Run Inference. Active-contract “owns meter” style rules return inferred triples instead of empty results.
- **Data Quality condition builder** — selecting a property after the target entity is chosen no longer clears the property list or drops the selected value.
- **License alignment ([#169](https://github.com/databrickslabs/ontobricks/issues/169))** — product, API, and OntoViz surfaces state the Databricks License as the first-party license.

No registry schema migration. No breaking MCP tool names.

---

## SPARQL (Spark / Lakehouse Explorer)

Spark SPARQL is **fail-closed**. The translator calls `SparqlCapabilityValidator` before generating warehouse SQL. Local RDFLib execution is **not** a fallback for warehouse data.

**Supported subset (warehouse):**

- Basic `SELECT` graph patterns
- `LIMIT`
- `OPTIONAL` / LeftJoin
- Literal `BIND`
- String `FILTER` (`CONTAINS`, `STR` equality, `STRSTARTS`, `STRENDS`)
- `FILTER(?predicate | ?p | ?pred IN (…))` with URI lists
- The specialized relationship-`UNION` shape: projected `?subject ?predicate ?object`, branch variables `?subject` / `?object`, and `BIND(… AS ?predicate)`

**Rejected (examples):** `GROUP BY`, `ORDER BY`, `OFFSET`, numeric or arbitrary `FILTER`, property paths, subqueries, `MINUS`, `VALUES`, `SERVICE`, `GRAPH`, non-specialized `UNION`.

Non-specialized `UNION` fails at capability validation with a `UNION` error rather than a later unrelated mapping failure.

---

## Knowledge Graph (`describe_entity` / `/triples/find`)

`DigitalTwin.find_triples_bfs` now delegates to a folded backend page (`find_triples_bfs_page`) instead of walking the graph, fetching every triple, then slicing in Python.

- SQL backends use a sargable bidirectional-edge walk and return one deterministic page (`DISTINCT`, `ORDER BY`, `LIMIT`/`OFFSET` plus one extra row for `has_more`).
- Neo4j implements the same page contract (walk from structured seeds, de-dup, slice) so metadata stays aligned: `seed_count`, `entity_count`, `total`, `has_more`.
- Public and internal `/triples/find` responses keep existing fields and add **`has_more`**. Missing `has_more` defaults to `false` for older payloads.
- Agent and MCP formatters show exact totals (`{count} of {total}`) and only hint that more rows exist when `has_more` is true.
- MCP `describe_entity` guidance: prefer entity-scoped deep scans via `search`; keep `depth=1` for type-wide scans.

Provenance: originally contributed by Laurent Prat ([#182](https://github.com/databrickslabs/ontobricks/pull/182)). Live warehouse measurement on a type-wide depth-2 Counterparty scan went from ~263s of Python materialization to ~34s for the first 100-triple page.

---

## MCP Server

### Per-connection selected domain

The MCP process still binds **one** `MCPServerSession` (shared registry, policy, HTTP pool). `selected_domain_name`, `ontology_labels`, and `class_actions` are now keyed by MCP session ID (StreamableHTTP `mcp-session-id`).

- Every MCP **request** (tools and resources such as `ontobricks://status`, `ontobricks://stats`, `ontobricks://graphql-schema`) resolves that session’s domain.
- The in-process store keeps at most **512** recently used sessions; an evicted client must call `select_domain` again.
- Off-request / missing session ID uses a shared compatibility bucket and logs a **warning** once per process.
- State is process-local. A multi-worker deployment would need a shared store keyed by the same session ID.

Provenance: originally contributed by Laurent Prat ([#183](https://github.com/databrickslabs/ontobricks/pull/183)).

---

## Ontology, Rules, and Inference

### Data Quality conditions

Delegated condition-row handlers stay bound to the **latest** render options. An empty first render no longer poisons later property selections.

### SWRL Run Inference

`build_inference_sql` no longer treats `swrlb:*` atoms as graph properties. Built-ins are excluded from triple joins and applied as SQL filters after variables are bound (for example `object LIKE CONCAT('%', 'active', '%')` for `swrlb:contains`).

Re-run affected active rules after upgrade; previously empty inference results for status-contains filters should now populate.

---

## Registry, Permissions, and UI

- **New Domain** (Home, Registry, empty state) is gated to administrators.
- The API distinguishes a **new domain** from an **existing-domain save** and rejects non-admin creation.
- Help Center and the development role matrix document Admin-only creation and the assigned-domain workflow for Editors and Builders.

---

## License and metadata

GitHub issue [#169](https://github.com/databrickslabs/ontobricks/issues/169) closed conflicting MIT / Apache 2.0 / Databricks License claims on first-party surfaces.

- README, product, architecture, development, and deployment docs identify OntoBricks (and OntoViz) as source-available under the **Databricks License**.
- OpenAPI schemas (main and external) publish matching license metadata.
- OntoViz header and About license link point at the canonical Databricks License.
- Third-party and vendored notices are unchanged.

---

## Documentation and operator notes

- Architecture and user-guide Explorer sections describe the Spark SPARQL support boundary.
- API, MCP, and Graph Explorer query-card docs describe folded BFS paging, exact totals, additive `has_more`, and `depth=1` for broad scans.
- MCP docs replace the old “one selected domain per process” limitation with concurrent-session behaviour and the 512-session LRU cap.
- **Contributor Guide** (`docs/contributing.md`) is the canonical setup / tests / PR workflow. Repo-root `CONTRIBUTING.md` is a short pointer. README, Help Center index, development, and code-organization link there.
- Agent/git conventions: there is no `develop` branch. Active work uses a **version-named** branch matching `pyproject.toml` (next line is `0.9.0`). Feature branches merge into that version branch; it ships to `master`.

---

## Upgrade notes

- **SPARQL:** queries that previously appeared to “work” with unsupported Spark constructs now fail closed. Rewrite them to the supported subset or run them outside warehouse SPARQL.
- **`/triples/find` and `describe_entity`:** clients can keep using `total` / `entity_count`. Treat `has_more` as additive. Broad type-wide scans should start at `depth=1`; use `search` for entity-scoped deeper walks.
- **MCP:** two clients on one `mcp-ontobricks` process can now select different domains. After LRU eviction (512 sessions) the client must `select_domain` again. Multi-worker MCP still needs an external session store.
- **Domain creation:** grant `CAN_MANAGE` on the Databricks App to users who must create domains. Existing Editor/Builder assignments are enough to save work on domains they already have.
- **Inference:** re-run SWRL rules that use `swrlb:contains` (and other registered string/comparison built-ins). No ontology rewrite is required if the rule text is already valid SWRL.
- **License:** downstream packaging and legal review should treat the Databricks License as authoritative for OntoBricks first-party code.
- **Lakebase / MCP policy:** the v0.8.0 `domains.mcp_policy` migration remains a prerequisite if you are still on 0.7.x. This patch does not add columns.

---

## Bug Fixes (selected)

- Fixed SWRL inference joining `swrlb:contains` as `predicate = '…#contains'` (no triples matched).
- Fixed Data Quality condition property dropdown emptying after entity selection.
- Fixed non-admin users being able to persist a new domain through save/create paths.
- Fixed Spark SPARQL accepting or mis-mapping unsupported UNION / FILTER / GROUP BY shapes.
- Fixed first-party license strings disagreeing across README, OpenAPI, and OntoViz.
- Fixed concurrent MCP clients clobbering each other’s selected domain (markets vs trading entity counts / ontology).
- Fixed MCP resource reads (`ontobricks://status`, stats, GraphQL schema) still sharing the default domain bucket after tool-call isolation.
- Fixed `describe_entity` / `/triples/find` type-wide scans timing out or returning 502s from unbounded Python triple materialization.
- Fixed missing `has_more` on older find payloads defaulting unsafely in internal/public route mapping.
