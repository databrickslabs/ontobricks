# OntoBricks Features

## Ontology Design
- **Visual Ontology Editor (OntoViz)**: Drag-and-drop canvas to create entities, relationships, and inheritance hierarchies with icons and attributes.
- **Class Hierarchies**: Define rdfs:subClassOf relationships with automatic property inheritance from parent to child entities.
- **SWRL Rules**: Create inference rules using a **graphical D3-based editor** — fullscreen modal with IF/THEN atom builders, ontology-aware context menu, live SWRL preview, and raw-edit mode for advanced users.
- **OWL Constraints**: Define cardinality, value restrictions, and property characteristics (functional, transitive, symmetric).
- **SHACL Data Quality Shapes**: Define data quality rules using W3C SHACL — six categories (completeness, cardinality, uniqueness, consistency, conformance, structural), Turtle round-trip (generate/import), PySHACL in-memory validation, and SQL compilation for triple store execution.
- **OWL Axioms**: Express class relationships, property chains, and complex expressions (equivalent, disjoint, union, intersection).
- **OWL Generation**: Automatic generation of W3C-compliant OWL/Turtle from visual design.
- **LLM-Powered Auto-Map Icons**: Automatically assign emoji icons to entities based on their names using the project's configured LLM serving endpoint (Ontology Model toolbar).
- **Dashboard Mapping**: Assign Databricks dashboards to entity types with parameter mapping for embedded display in the Knowledge Graph.

## Data Mapping
- **Visual Mapping Designer**: Map ontology classes and relationships to Databricks tables with an interactive designer interface.
- **Direct Edit Mode**: Clicking an already-assigned entity or relationship immediately loads the editable column-mapping grid (no extra Edit button click needed).
- **AI-Powered Wizard**: Generate SQL queries using an LLM endpoint with table context from project metadata.
- **Attribute-Level Mapping**: Map individual ontology attributes to SQL columns with multi-pass matching (exact, normalized, substring, positional).
- **Partial Mapping Detection**: Entities with incomplete attribute mappings are highlighted with an orange indicator on the Designer view.
- **Auto-Map**: Batch-map all unmapped entities and relationships asynchronously with progress tracking.
- **Re-Assign Missing Attributes**: Targeted re-mapping for entities that have some attributes unmapped.
- **Preview Limit**: Control the number of preview rows displayed in the Mapping grid; SQL is stored without LIMIT clause.
- **Unified Panel UI**: Designer and Manual views share the same panel design (tabs, forms, tables) for a consistent experience.
- **SQL Query Testing**: Test and validate SQL queries directly in the mapping interface before saving.
- **Relationship Direction**: Control forward, reverse, or bidirectional relationships with visual indicators.
- **R2RML Generation**: Automatic generation of W3C-compliant R2RML mappings from visual configuration.

## Digital Twin (Sync & Explore)
- **Two Backends**: Choose between **Delta Lake** (SQL Warehouse) and **LadybugDB** (embedded Cypher-based graph database) as the triple store backend per project.
- **Readiness Status**: Validates ontology, entity mappings, relationship mappings, and attribute mapping completeness before enabling sync and explore actions.
- **Triple Store Sync**: Synchronize generated triples to a Unity Catalog table — SQL is generated automatically from R2RML mappings (no manual query writing required).
- **Last Updated Timestamp**: Triple store status displays the last modification date and time retrieved from Unity Catalog Delta table metadata (`DESCRIBE DETAIL`).
- **Auto-Load Triple Store**: Triples and Knowledge Graph views automatically load data from the triple store on navigation (no manual button click required).
- **Async Quality Checks**: Validate data against ontology constraints (cardinality, value, property characteristics, global rules) asynchronously with progress tracking.
- **SHACL Validation**: Run SHACL shapes against the triple store — shapes are compiled to SQL for execution with violation reporting, or validated in-memory via PySHACL for small datasets.
- **Triples Grid**: Interactive data grid with sorting, filtering, and grouping capabilities to browse triple store contents.
- **Knowledge Graph**: Interactive sigma.js WebGL-powered graph to explore entities and relationships visually with search, filtering, depth control, and entity detail panels.
- **Data Cluster Detection**: Detect communities in the knowledge graph using Louvain, Label Propagation, or Greedy Modularity algorithms — client-side (Graphology) for the visible subgraph and server-side (NetworkX) for the full graph; color-by-cluster mode, adjustable resolution slider, cluster collapse/expand into super-nodes, and cluster member details on click.
- **Dashboard Embedding**: View assigned Databricks dashboards with entity-specific parameters directly in the Knowledge Graph.
- **Violation Details**: View quality check violations in a detailed modal with entity information.

## Project Management
- **Unity Catalog Storage**: Save and load projects from Databricks Unity Catalog Volumes.
- **Version Control**: Create, list, and load multiple versions of a project with automatic versioning.
- **Import/Export**: Import OWL and RDFS ontologies, import industry-standard ontologies (FIBO, CDISC, IOF), import/export R2RML mappings, and export OWL files.
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
- **Model Context Protocol**: Expose the knowledge graph to LLM agents via MCP (Streamable HTTP + stdio transports).
- **Project Selection**: Two-step workflow — `list_projects` to browse available knowledge graphs, `select_project` to activate one.
- **Entity Discovery**: `list_entity_types` and `describe_entity` provide human-readable text descriptions with BFS traversal.
- **GraphQL via MCP**: `get_graphql_schema` and `query_graphql` tools let LLM agents introspect and query the typed GraphQL API.
- **Databricks Playground**: Deployed as `mcp-ontobricks`, auto-discoverable by LLM agents in the Databricks Playground.
- **Multi-Client**: Works with Cursor, Claude Desktop, or any MCP-compatible client.

## Navigation & UX
- **Deep-Linkable Sections**: Sidebar section changes push `?section=<id>` to browser history — sections are bookmarkable and navigable with Back/Forward.
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
