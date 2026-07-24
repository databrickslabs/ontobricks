# OntoBricks Features

## Ontology Design
- **Ontology Designer (menu)**: Primary visual ontology workspace under **Ontology → Designer** — OntoViz canvas, toolbar, and floating AI Assistant (the sidebar label is *Designer*, not “Model”).
- **Visual Ontology Editor (OntoViz)**: Drag-and-drop canvas to create entities, relationships, and inheritance hierarchies with icons and attributes.
- **Class Hierarchies**: Define rdfs:subClassOf relationships with automatic property inheritance from parent to child entities.
- **SWRL Rules**: Create inference rules using a **graphical D3-based editor** — fullscreen modal with IF/THEN atom builders, ontology-aware context menu, live SWRL preview, and raw-edit mode for advanced users.
- **OWL Constraints**: Define cardinality, value restrictions, and property characteristics (functional, transitive, symmetric).
- **SHACL Data Quality Shapes**: Define data quality rules using W3C SHACL — six categories (completeness, cardinality, uniqueness, consistency, conformance, structural), Turtle round-trip (generate/import), PySHACL in-memory validation, and SQL compilation for triple store execution.
- **OWL Axioms**: Express class relationships, property chains, and complex expressions (equivalent, disjoint, union, intersection).
- **Ontology Pitfalls Detector**: Detect 19 structural, logical, and semantic pitfalls (P1.1–P4.7) in four categories via the **Ontology → Pitfalls** sidebar panel. Fast/graph-only checks run immediately; ML-heavy checks (semantic similarity, NLP naming) require the optional `[pitfalls]` extra (`uv sync --extra pitfalls`). Each check shows a description tooltip and ⚡/💻 speed indicator. Results group by category with an accordion display.
- **OWL Generation**: Automatic generation of W3C-compliant OWL/Turtle from visual design.
- **LLM-Powered Auto-Map Icons**: Automatically assign emoji icons to entities based on their names using the project's configured LLM serving endpoint (Ontology Designer toolbar).
- **Ontology Designer (D3 Graph)**: Force-directed graph view of the full ontology under **Ontology → Designer** — click an entity to highlight its 1-hop neighbourhood (connected links + neighbours dimmed), click a relationship to highlight its two endpoints, toggle inheritance links on/off (state persisted), search any entity by name to pan/zoom to it with neighbourhood highlight, and right-click to create a Business View from a 1-hop neighbourhood (auto-named `Auto_<entity>`, numeric suffix on collision).
- **Dashboard Mapping**: Assign Databricks dashboards to entity types with parameter mapping for embedded display in the Graph Viewer.

## Data Mapping
- **Visual Mapping Designer**: Map ontology classes and relationships to Databricks tables with an interactive designer interface.
- **Direct Edit Mode**: Clicking an already-assigned entity or relationship immediately loads the editable column-mapping grid (no extra Edit button click needed).
- **AI-Powered Wizard**: Generate SQL queries using an LLM endpoint with table context from project metadata.
- **Attribute-Level Mapping**: Map individual ontology attributes to SQL columns with multi-pass matching (exact, normalized, substring, positional).
- **Per-Attribute Include / Exclude**: Each attribute in the **Status tab** of the bottom panel has a checkbox. Unchecking an attribute excludes it from the mapping — excluded attributes are shown with strikethrough styling and a dash icon, are skipped in gap reporting, are not generated in the R2RML export, and are hidden from the auto-mapping agent. Exclusions survive Unmap/re-map cycles and are never overwritten by Auto-Map.
- **Auto-Exclude Unmapped Attributes**: The Status tab exposes an **Auto-Exclude unmapped** button that bulk-excludes all attributes that have no column assignment yet. Useful for quickly scoping a mapping to only the attributes you care about.
- **Partial Mapping Detection (includes-aware)**: Entities with incomplete attribute mappings are highlighted orange on the Designer canvas. The check now considers only *included* attributes — an entity is green when every included attribute has a column assignment, regardless of how many attributes are excluded.
- **Auto-Map**: Batch-map all unmapped entities and relationships asynchronously with progress tracking.
- **Auto-Map KPI — Excluded Counts**: Entity, Relationship, and Attribute KPI tiles on the Auto-Map page show the number of excluded items alongside mapped/total counts (e.g. `13 / 13 · 1 excl.`).
- **Metadata Quality Warning**: The Auto-Map page detects tables and columns with missing `COMMENT` descriptions and displays a collapsible warning. Poor metadata quality reduces LLM mapping accuracy; a direct link opens the Domain → Metadata editor.
- **Re-Assign Missing Attributes**: Targeted re-mapping for entities that have some included attributes unmapped (excluded attributes are ignored by this check).
- **Column Names with Spaces / Special Characters**: The auto-mapping agent is instructed to backtick-quote unsafe column names and alias them to safe `snake_case` names. The R2RML generator double-quotes column names (per R2RML spec §7.4) in `rr:column` values and `rr:template` column references whenever a name is not a plain SQL identifier.
- **Preview Limit**: Control the number of preview rows displayed in the Mapping grid; SQL is stored without LIMIT clause.
- **Unified Panel UI**: Designer and Manual views share the same panel design (tabs, forms, tables) for a consistent experience.
- **SQL Query Testing**: Test and validate SQL queries directly in the mapping interface before saving.
- **Relationship Direction**: Control forward, reverse, or bidirectional relationships with visual indicators.
- **R2RML Generation**: Automatic generation of W3C-compliant R2RML mappings from visual configuration. Excluded attributes are never emitted as `rr:predicateObjectMap` triples.

## Knowledge Graph (Sync & Explore)
- **Two Layers**: Every build materializes a Delta view (Unity Catalog, governance) and a Graph DB engine (Lakebase Postgres today; pluggable behind `GraphDBFactory`).
- **Readiness Status**: Validates ontology, entity mappings, relationship mappings, and attribute mapping completeness before enabling sync and explore actions.
- **Triple Store Sync**: Synchronize generated triples to a Unity Catalog table — SQL is generated automatically from R2RML mappings (no manual query writing required).
- **Last Updated Timestamp**: Triple store status displays the last modification date and time retrieved from Unity Catalog Delta table metadata (`DESCRIBE DETAIL`).
- **Auto-Load Triple Store**: Triples and Graph Viewer views automatically load data from the triple store on navigation (no manual button click required).
- **Async Quality Checks**: Validate data against ontology constraints (cardinality, value, property characteristics, global rules) asynchronously with progress tracking.
- **SHACL Validation**: Run SHACL shapes against the triple store — shapes are compiled to SQL for execution with violation reporting, or validated in-memory via PySHACL for small datasets.
- **Triples Grid**: Interactive data grid with sorting, filtering, and grouping capabilities to browse triple store contents.
- **Graph Viewer**: Interactive sigma.js WebGL-powered graph to explore entities and relationships visually with search, filtering, depth control, and entity detail panels.
- **Data Cluster Detection**: Detect communities in the graph viewer using Louvain, Label Propagation, or Greedy Modularity algorithms — client-side (Graphology) for the visible subgraph and server-side (NetworkX) for the full graph; color-by-cluster mode, adjustable resolution slider, cluster collapse/expand into super-nodes, and cluster member details on click.
- **Graph Analytics**: Dedicated **Analytics** sidebar section that computes five NetworkX centrality metrics over the full knowledge graph or a selected entity-type subset: PageRank (global influence), Betweenness Centrality (bridging), Degree Centrality (raw connectivity), Closeness Centrality (reachability), and Clustering Coefficient (local density). Results render as interactive horizontal bar charts (one per metric) — clicking a bar navigates directly to the entity in the Graph Viewer. A PageRank Detail Table shows all five scores side-by-side with relative mini-bars. Each chart card has an explanatory popup (formula, worked example, importance). Entity type filter (single-select dropdown) restricts chart display to a specific class while computing metrics on the full connected subgraph for accuracy.
- **AI Graph Interpretation**: After running an Analytics computation, click **Interpret** to invoke the `agent_graph_interpreter` AI agent. The agent uses the configured LLM serving endpoint, may call `get_entity_details` to look up top-ranked entities, and produces three structured insight sections (Key Findings, Notable Entities, Recommendations). Results are rendered as styled Bootstrap cards with markdown support. Clickable entity names in "Notable Entities" jump to the Graph Viewer.
- **Audit Trail Integration**: From the AI Insights card, click **Add to audit trail** to post the AI-generated interpretation as a comment on the current domain version (markdown-formatted). The discussion panel opens automatically to show the new entry.
- **Dashboard Embedding**: View assigned Databricks dashboards with entity-specific parameters directly in the Graph Viewer.
- **Violation Details**: View quality check violations in a detailed modal with entity information.

## Project Management
- **Unity Catalog Storage**: Save and load projects from Databricks Unity Catalog Volumes.
- **Version Control**: Create, list, and load multiple versions of a project with automatic versioning. Which version is **Active** (exposed via API / MCP) is managed from **Registry → Browse**; the Domain → Versions page shows that status as a read-only badge.
- **Single-editor concurrency**: A DRAFT domain version can only be **edited by one user at a time**. The first opener edits; anyone else opening the same version is automatically **read-only** with a banner naming the current editor. The lock uses a **renew-only lease** — the editor's browser keeps it alive while the domain is open, so an active session never expires, but a lock left behind (crashed/abandoned tab) auto-releases once its lease lapses (configurable in **Settings → Global → Edit Lock Lease**, or via `ONTOBRICKS_EDIT_LOCK_TTL_S`; default 10 min; `0` disables). It is also released immediately when the editor clicks **Close** (which prompts *Save before closing?* and returns to Home) or an **app-admin takes over**. The **Save** and **Close** buttons sit together in the domain sub-navigation.
- **Domain Cockpit (Validation)**: Readiness tiles including **Active Version** — the version currently exposed via API/MCP (from the registry), with a *(not loaded)* hint when it differs from the version open in the session. Distinct from “latest on disk” vs read-only UI gating (still driven by whether the loaded version is the latest).
- **Audit Trail**: The Domain → Audit trail page is one unified, newest-first activity feed interleaving three streams: **ontology/mapping changes** (who changed what — class/property/mapping add/update/remove, SHACL/SWRL/group edits, imports, resets — and when, with AI-assistant edits tagged), **status & comments** (review/validation decisions), and **build runs**. Change events are buffered in the working session as edits happen and flushed to the registry (`domain_change_events`) on **Save to registry**; the real edit time is preserved. Filter buttons and a version dropdown scope the timeline.
- **New-domain loading**: After **New Domain** from the navbar, a full-page spinner runs until Domain Information has finished its initial fetches (LLM endpoints, version status, domain info).
- **Domain Information — Knowledge Graph fields**: Triple-store FQN and Graph DB table name (e.g. Lakebase `g_<domain>_v<version>`) refresh when the domain name is **committed** (blur / `change`) or the version changes, so previews match naming rules before save.
- **Duplicate domain names**: Save to registry is blocked when the sanitized name already exists (`/domain/check-name` + guard on **Save to UC**); inline validation clears when the name is cleared or the check errors.
- **Navbar domain identity**: Top bar name/version invalidate cached `/navbar/state` (and related caches) after domain mutations so reloads and navigations do not show stale labels for up to 15 seconds.
- **Import/Export**: Import OWL and RDFS ontologies, import industry-standard ontologies (FIBO, CDISC, IOF, HL7 FHIR R4/R4B/R5), import/export R2RML mappings, and export OWL files.
- **Registry OBX Export/Import**: Export one or more registry domains to a portable `.obx` file directly from **Registry → Browse** with per-domain version-mode selection (Latest / Active / All / Choose). Import with per-domain conflict resolution (Skip / Overwrite / Rename). An integer `format_version` field ensures backward compatibility as the format evolves.
- **Project Save/Load**: Save and load projects as JSON for backup or sharing.

## Databricks Integration
- **Native Unity Catalog Support**: Browse catalogs, schemas, tables, and volumes directly from the UI.
- **SQL Warehouse Connectivity**: Connect to Databricks SQL Warehouses for query execution.
- **Dashboard Integration**: Fetch and embed Databricks dashboards with dynamic parameter mapping.
- **Secure Credentials**: Databricks credentials are never saved to project files.

## GraphQL API
- **Auto-Generated Schema**: Strawberry GraphQL schema is derived from the ontology at runtime — each class becomes a type, each data property a field, each object property a typed relationship.
- **Nested Entity Traversal**: Query entities with nested relationships (e.g., `customers { hasInteraction { label date } }`) instead of flat triple lists.
- **GraphiQL Playground**: Interactive in-browser IDE available per project at `/graphql/{project_name}`, with auto-complete, documentation explorer, and query history.
- **Schema Introspection (SDL)**: Machine-readable SDL endpoint (`/graphql/{project_name}/schema`) for external tool integration.
- **Batch Resolution**: Resolvers batch-load triples from the triple store to prevent N+1 query issues.
- **Per-Project Schemas**: Each project gets its own schema, cached and automatically invalidated on ontology changes.

## MCP Server (AI Integration)
- **Model Context Protocol**: Expose the graph viewer to LLM agents via MCP (Streamable HTTP + stdio transports).
- **Project Selection**: Two-step workflow — `list_domains` to browse available graph viewers, `select_domain` to activate one.
- **Entity Discovery**: `list_entity_types` and `describe_entity` provide human-readable text descriptions with BFS traversal.
- **GraphQL via MCP**: `get_graphql_schema` and `query_graphql` tools let LLM agents introspect and query the typed GraphQL API.
- **Databricks Playground**: Deployed as `mcp-ontobricks`, auto-discoverable by LLM agents in the Databricks Playground.
- **Multi-Client**: Works with Cursor, Claude Desktop, or any MCP-compatible client.

## Navigation & UX
- **Deep-Linkable Sections**: Sidebar section changes push `?section=<id>` to browser history — sections are bookmarkable and navigable with Back/Forward.
- **Registry Browse for “Active” version**: Expand a domain in **Registry → Browse** to set which version is **Active** (API/MCP); the Domain app no longer exposes a per-version toggle on **Domain → Versions** (badge only).
- **Breadcrumb Navigation**: Auto-generated breadcrumb bar below the navbar shows Registry > Domain > Ontology > Section context.
- **Keyboard Shortcuts**: `Cmd/Ctrl+S` to save domain, `Cmd/Ctrl+K` to focus sidebar search, `?` for a shortcut overlay.
- **Toast Notifications**: All user feedback uses non-blocking toast notifications (no `alert()` dialogs).

## Performance
- **SQL Connection Pooling**: `SQLWarehouse` maintains a `queue.Queue`-based pool of database connections, eliminating per-query TLS handshake overhead.
- **Dedicated Thread Pool**: Blocking Databricks I/O runs in a dedicated `ThreadPoolExecutor` (configurable via `ONTOBRICKS_THREAD_POOL_SIZE`, default 20).
- **Consistent Asset Versioning**: All static assets use deterministic `?v={{ asset_version }}` cache busting.

## Security
- **CSRF Protection**: Double-submit cookie pattern for all state-changing requests; `X-CSRF-Token` header auto-attached by the frontend fetch wrapper.
- **Secure Cookies**: Session cookies use `secure=True` and `samesite=lax` in Databricks Apps deployments (HTTPS-only).

## Observability
- **Structured JSON Logging**: Set `LOG_FORMAT=json` for machine-readable log lines with `ts`, `level`, `logger`, `module`, `func`, `line`, `msg` fields.
- **Request Timing**: Middleware logs method, path, status code, and duration (ms) for every non-static request.

## Deployment
- **Databricks Apps Ready**: Deploy as a native Databricks App with service principal authentication.
- **MCP Server App**: Separate `mcp-ontobricks` Databricks App for Playground integration.
- **Local Development**: Run locally with hot-reload for development and testing.
