<p align="center">
  <img src="src/front/static/global/img/ontobricks-icon.svg" alt="OntoBricks Logo" width="120" height="120">
</p>

<h1 align="center">OntoBricks</h1>

<p align="center">
  <strong>Digital Twin Builder for Databricks</strong>
</p>

<p align="center">
  OntoBricks is a web-based application that turns <strong>Databricks tables into a knowledge graph</strong>.<br>
  Design ontologies using OWL or import industry standards (FIBO, CDISC, IOF&nbsp;…), map them to tables (with R2RML behind the scenes), materialize triples into a Delta or LadybugDB triple store, query them through a typed GraphQL API, and explore your knowledge graph visually.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License">
  <img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/fastapi-0.109+-green.svg" alt="FastAPI">
</p>

## How It Works

OntoBricks builds a **materialized knowledge graph** (triple store) from your Databricks tables using semantic web standards, graph databases, and formal reasoning:

```
                          OntoBricks                      MCP Server
                              │                         (mcp-ontobricks)
    ┌─────────────────────────┼───────────────────┐          │
    │                         │                   │     LLM Agents /
    ▼                         ▼                   │     Playground
┌─────────┐            ┌──────────────┐           │          │
│   OWL   │───────────▶│    R2RML     │───▶ Spark SQL        │
│Ontology │            │   Mapping    │    Generator         │
└─────────┘            └──────────────┘           │          │
    │                    Design &                  │     REST API
    │                   Designer                   │     (httpx)
    │                         ▼                    │          │
    │                 ┌──────────────┐             │          │
    │                 │ Triple Store │◀────────────┘◀─────────┘
    │                 │Delta/LadyBug│     Materialize & Query
    │                 └──────────────┘
    │                    │    │    │
    │               GraphQL  │  Knowledge
    │               (typed)  │   Graph
    │                    │   │      │
    │                    ▼   ▼      ▼
    │            ┌──────────────────────┐
    ├───────────▶│   Reasoning Engine   │
    │            │ OWL 2 RL │ SWRL │   │
    │            │ Graph    │Checks│   │
    │            └──────────────────────┘
    ▼                    ▼         ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Databricks Platform                          │
│              (Unity Catalog + SQL Warehouse)                    │
└─────────────────────────────────────────────────────────────────┘
```

1. **Design** an ontology (OWL) — visually or via LLM-powered generation
2. **Map** ontology entities to Databricks tables using R2RML mappings
3. **Materialize** triples into a triple store — **Delta** (SQL Warehouse) or **LadybugDB** (embedded graph database)
4. **Reason** over the knowledge graph — OWL 2 RL deductive closure, SWRL rules, transitive/symmetric expansion, constraint validation, and SHACL data quality shapes
5. **Query** the knowledge graph via GraphQL with a typed schema auto-generated from the ontology
6. **Explore** the knowledge graph with interactive visualization, GraphiQL playground, and quality checks

## Screenshots

### Ontology Model
Design your ontology visually — create entities, relationships, and inheritance hierarchies on an interactive canvas with emoji icons, attributes, constraints, and dashboard integration.

<p align="center">
  <img src="docs/screenshots/Ontology%20design.png" alt="Ontology Design" width="800">
</p>

### Semantic Mapping
Map ontology entities to Databricks tables — click any entity on the graph, write or generate SQL, then assign columns to attributes with a live data preview.

<p align="center">
  <img src="docs/screenshots/Ontology%20Mapping.png" alt="Ontology Mapping" width="800">
</p>

### Knowledge Graph
Explore your knowledge graph — search, filter, and navigate entities and relationships with full attribute details, up to N levels deep.

<p align="center">
  <img src="docs/screenshots/Graph%20result.png" alt="Knowledge Graph" width="800">
</p>

## Features

### Ontology Design
- **🎨 Visual Ontology Designer**: Drag-and-drop interface to create entities, relationships, and attributes using OntoViz
- **📐 Entity-Relationship Diagram**: Interactive canvas with auto-layout, zoom, pan, and centering
- **🔗 Inheritance Support**: Visual class hierarchies with property inheritance (rdfs:subClassOf)
- **📋 Constraints & Rules**: Define cardinality, value constraints, property characteristics, and SWRL rules
- **🧙 LLM-Powered Wizard**: Generate a complete ontology from table metadata using the configured LLM endpoint
- **📏 SHACL Data Quality**: Define data quality shapes (completeness, cardinality, uniqueness, consistency, conformance, structural) using W3C SHACL — generate Turtle, import from files, validate with PySHACL, and execute shapes as SQL against the triple store

### Data Mapping
- **🏢 Databricks Integration**: Native connectivity to Unity Catalog (catalogs, schemas, tables, volumes)
- **📊 Entity Mapping**: Map ontology classes to Databricks tables with column-level precision
- **🔗 Attribute Mapping**: Map individual entity attributes to SQL columns with visual feedback
- **🔄 Relationship Mapping**: Define relationships with direction control (forward, reverse, bidirectional)
- **🤖 AI-Powered Auto-Map**: LLM-driven automatic SQL generation and column mapping for entities and relationships
- **🔶 Partial Mapping Detection**: Visual indicators (orange) for entities with incomplete attribute mappings
- **🔁 Re-Assign Missing Attributes**: Targeted re-mapping for entities with incomplete attribute mappings
- **📝 R2RML Generation**: Automatic W3C-compliant R2RML mapping generation
- **🔀 Unified Panel UI**: Consistent editing experience across Designer and Manual views

### Digital Twin (Sync & Explore)
- **🔄 Triple Store Sync**: Synchronize mapped data to the configured triple store backend with readiness validation
- **⚡ Incremental Sync**: Detects source data changes via Delta table version tracking and applies only the diff (additions + removals) using a server-side snapshot table — skips the build entirely when nothing changed
- **💾 Two Backends**: **Delta** (Databricks SQL Warehouse) or **LadybugDB** (embedded Cypher-based graph database with automatic UC Volume sync)
- **📈 Knowledge Graph**: Interactive sigma.js WebGL-powered graph to explore entities and relationships visually with search, filtering, and entity detail panels
- **🔬 Data Cluster Detection**: Detect communities in the knowledge graph using Louvain, Label Propagation, or Greedy Modularity algorithms — client-side (Graphology) for the visible subgraph, server-side (NetworkX) for the full graph; color-by-cluster visualization, adjustable resolution, cluster collapse/expand into super-nodes with member details on click
- **🗺️ Ontology Model Viewer**: Read-only D3.js ontology model accessible from Knowledge Graph and GraphQL sections — frozen force-directed graph with pan/zoom in a fullscreen modal
- **📊 Dashboard Integration**: Embed Databricks dashboards with parameter mapping to entities
- **✅ Async Quality Checks**: Validate data against ontology constraints with background processing and progress tracking
- **📏 SHACL Data Quality**: Run SHACL-based validation against the triple store — shapes defined in the ontology are compiled to SQL for execution, with violation reporting and PySHACL in-memory validation support
- **📖 API Documentation**: Built-in REST API reference and schema introspection (SDL) for external integrations
- **🕒 Last Updated Timestamp**: Triple store status shows the last modification date from Unity Catalog metadata

### Reasoning & Inference
- **🧠 OWL 2 RL Reasoner**: Forward-chaining deductive closure on the ontology using the `owlrl` library — infers implicit class hierarchies, property entailments, and type assertions
- **📏 SWRL Rule Engine**: User-defined Horn-clause rules (Antecedent → Consequent) with a **graphical D3-based editor** (fullscreen modal with IF/THEN atom builders, context menu, and live SWRL preview) — compiled to SQL or Cypher depending on the active backend for violation detection and triple materialization
- **🔗 Graph Reasoning**: Automatic transitive closure and symmetric expansion based on OWL property characteristics (`TransitiveProperty`, `SymmetricProperty`)
- **✔️ Constraint Validation**: Cardinality checks, functional/inverse-functional property enforcement, value constraints, orphan detection, and label requirements — executed natively in Cypher (LadybugDB) or SQL (Delta)
- **📊 Materialization**: Inferred triples from any reasoning phase can be written back to the triple store, enriching the knowledge graph with derived facts

### GraphQL API
- **🔮 Auto-Generated Schema**: The GraphQL schema is derived from the ontology — each class becomes a type, each data property a field, each object property a typed relationship
- **🔗 Nested Traversal**: Query entities with nested relationships (e.g., `customers { hasInteraction { label date } }`) instead of flat triple lists
- **🎮 GraphiQL Playground**: Interactive in-browser IDE per domain with introspection, auto-complete, and documentation
- **📐 Schema Introspection**: SDL endpoint lets tools and LLM agents auto-discover the schema
- **⚡ Batch Resolution**: Resolvers batch-load triples from the triple store for efficient query execution
- **📦 Per-Domain Schemas**: Each domain gets its own GraphQL schema, cached and invalidated on ontology change

### MCP Server (AI Integration)
- **🤖 Model Context Protocol**: Expose the knowledge graph to LLM agents via [MCP](https://modelcontextprotocol.io/)
- **📂 `list_projects` / `select_project`**: Two-step workflow — browse available knowledge graphs, then select one to query
- **📋 `list_entity_types`**: Discover all entity types, counts, and predicates in human-readable text
- **🔍 `describe_entity`**: Search by name or type and get a full-text description with attributes, relationships, and hop-by-hop traversal
- **📊 `get_status`**: Retrieve triple store status, readiness, and last-modified timestamp
- **🔮 `get_graphql_schema` / `query_graphql`**: Introspect the auto-generated GraphQL schema and execute GraphQL queries programmatically
- **🎯 Databricks Playground**: Deployed as `mcp-ontobricks`, auto-discoverable in the Databricks Playground
- **🔌 Multi-Client**: Works with Cursor, Claude Desktop, or any MCP-compatible client via stdio or HTTP transport

### Domain Management
- **💾 Unity Catalog Storage**: Save/load domains to UC Volumes with version control
- **📥 Import/Export**: Import OWL, RDFS ontologies and R2RML mappings; export OWL and R2RML
- **🏦 Industry-Standard Ontologies**: One-click import of [FIBO](https://spec.edmcouncil.org/fibo/) (Financial), [CDISC](https://www.cdisc.org/) (Clinical), and [IOF](https://www.industrialontologies.org/) (Manufacturing) — see [Ontology import](docs/user-guide.md#ontology-import-merged) in the user guide
- **☁️ Databricks Apps Ready**: Designed for deployment as a Databricks App

### Registry
- **📂 Multi-Domain Registry**: Central registry backed by a UC Volume that indexes all domains, their versions, and status
- **🔄 Scheduled Refresh**: Background scheduler keeps the registry cache up-to-date on a configurable interval
- **🔗 Entity URI Resolution**: `/resolve` endpoint resolves entity URIs to the correct domain and redirects into the knowledge graph

### Navigation & UI
- **📋 Centralized Menu Configuration**: Top navbar and sidebar menus are driven from a single JSON config (`menu_config.json`)
- **🔔 Unified Status Indicators**: Ontology, Mapping, and Digital Twin navbar indicators refresh simultaneously via a centralized function
- **🏷️ Smart Defaults**: Ontology name defaults to the domain name; version displayed in the top navbar

## Quick Start

### Prerequisites

- Python 3.10 or higher
- Databricks workspace access
- Personal Access Token
- SQL Warehouse ID

### Installation

```bash
# Clone and setup
git clone <repository-url>
cd OntoBricks
./setup.sh

# Configure credentials
cp .env.example .env
# Edit .env with your Databricks credentials

# Run
./start.sh
```

Open **http://localhost:8000**

### Deploy to Databricks Apps

```bash
# Install Databricks CLI
pip install databricks-cli
databricks configure --token

# Deploy
make deploy
# Or: ./deploy.sh
```

After deployment, bind the **sql-warehouse** and **volume** resources in the Databricks Apps UI (**Compute > Apps > ontobricks > Resources**). If the registry volume is empty, open the app and click **Settings > Registry > Initialize**.

See [Deployment Guide](docs/deployment.md) for detailed instructions including resource configuration, permissions, and the full deployment checklist.

### Configuration

Create a `.env` file:

```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_SQL_WAREHOUSE_ID=your-warehouse-id
SECRET_KEY=your-secret-key
# Optional: persist agent traces to Databricks (instead of local mlflow.db)
MLFLOW_TRACKING_URI=databricks
```

## Automated Pipeline: From Tables to Triple Store in 4 Clicks

OntoBricks can automatically build a complete knowledge graph from your Databricks tables using LLM-powered automation:

| Step | Action | What Happens |
|------|--------|--------------|
| **1** | **Import Metadata** (Domain > Metadata) | Fetches table and column metadata from Unity Catalog |
| **2** | **Generate Ontology** (Ontology > Wizard) | LLM designs entities, relationships, and attributes from your metadata |
| **3** | **Auto-Map** (Mapping > Auto-Map) | LLM generates SQL mappings for every entity and relationship |
| **4** | **Synchronize** (Digital Twin > Status) | Executes mappings and populates the triple store table |

After a one-time configuration (Databricks connection, LLM endpoint, triple store table), the entire process from raw tables to a queryable knowledge graph is fully automated.

See the **[Automated triple-store pipeline](docs/user-guide.md#automated-triple-store-pipeline-merged)** section in the user guide for detailed steps, tips, and the REST API equivalent.

## Workflow

### 1. 🎨 Design Ontology

Use the visual **Design** interface to create your ontology:

- **Drag-and-drop entities** on the canvas
- **Add attributes** to entities (data properties)
- **Create relationships** between entities by connecting them
- **Define inheritance** between entities (class hierarchies with property inheritance)
- **Set relationship direction**: Forward, Reverse, or Bidirectional
- **Add icons and descriptions** to entities for better visualization
- **Auto-layout** and **center** the diagram for clarity

Or use the traditional form-based interface:

- Define **Classes**: Person, Employee, Department
- Add **Data Properties**: name, email, budget
- Add **Object Properties**: worksIn, manages
- Define **Inheritance**: Employee → Person (inherits all Person attributes)
- Export as **OWL/Turtle** format

### Or: Import an Industry-Standard Ontology

Instead of designing from scratch, you can import a pre-built ontology from the **Import** sidebar:

| Standard | Domain | Source |
|----------|--------|--------|
| **FIBO** | Financial industry | [EDM Council](https://spec.edmcouncil.org/fibo/) |
| **CDISC** | Clinical data | [PhUSE / CDISC](https://github.com/phuse-org/rdf.cdisc.org) |
| **IOF** | Digital manufacturing | [Industrial Ontologies Foundry](https://github.com/iofoundry/ontology) |

Select the domains you need, click **Import**, and OntoBricks fetches, merges, and parses the modules automatically. You can also import any **OWL** or **RDFS** file from your local machine or Unity Catalog.

See **[Ontology import](docs/user-guide.md#ontology-import-merged)** in the user guide for details on each standard and available domains.

### 2. 🔗 Assign Data Sources

Connect ontology to Databricks tables via the **Mapping** page:

- **Visual Designer**: Click entities/relationships on the interactive designer to configure their SQL queries
- **Direct Edit Mode**: Clicking an assigned item immediately loads the editable column-mapping grid
- **AI Wizard**: Generate SQL queries automatically using the LLM-powered Wizard tab
- **Auto-Map**: Batch-map all unmapped entities and relationships in one async operation
- **Re-Assign Attributes**: Fix entities with missing attribute mappings via targeted re-mapping
- **Preview Limit**: Control how many rows are previewed in the Mapping grid (SQL is stored without LIMIT)
- **R2RML Generation**: Auto-generated W3C-compliant mapping (view in Domain → Export)

### Domain Contents

A saved OntoBricks domain contains:
- **Ontology details**: Entities, relationships, and inheritance hierarchy
- **Design layout**: OntoViz canvas positions and visual configuration
- **Configuration**: SQL Warehouse settings

Domains **never** store:
- Authentication tokens or passwords
- Query results

### 3. 🔍 Digital Twin

Synchronize, validate, and explore your knowledge graph:

- **Build** your Digital Twin — creates a Triple-Store VIEW in Unity Catalog and populates the LadybugDB graph
- **Incremental by default** — subsequent builds detect source Delta table changes and apply only the diff (see [Incremental Sync](#incremental-sync-delta-load))
- **Readiness Status** validates ontology, entity mappings, relationship mappings, and attribute completeness before sync
- **Quality Checks** run asynchronously with progress tracking; validate cardinality, value constraints, property characteristics, and global rules
- **Interactive Knowledge Graph**: Explore entities and relationships as a sigma.js WebGL graph — search, filter, click entities to see all attributes and values
- **View Ontology**: Open a frozen, read-only D3.js ontology model from the Knowledge Graph or GraphQL section to see the full ontology structure while exploring data
- **GraphQL Playground**: Embedded GraphiQL IDE to query the knowledge graph with the auto-generated typed schema — auto-complete, docs explorer, and live results
- **API Documentation**: Built-in REST API reference with schema introspection for programmatic access

## Architecture

OntoBricks follows a clean **Routes → Services → Core** architecture with a consistent sidebar-based UI:

![Architecture](docs/images/architecture-overview.svg)

```
src/
├── agents/                      # LLM agents (unchanged)
│   ├── tracing.py               # MLflow tracing setup & decorators
│   ├── llm_utils.py             # Shared LLM call with retry
│   ├── tools/                   # Shared agent tools (context, metadata, SQL, …)
│   ├── agent_owl_generator/     # OWL ontology generation agent
│   ├── agent_auto_assignment/   # Entity/relationship → SQL mapping agent
│   ├── agent_auto_icon_assign/  # Emoji icon mapping agent
│   └── agent_ontology_assistant/# Conversational assistant + ResponsesAgent wrapper
│
├── api/                         # External REST API (/api/v1/…) + internal JSON API
│   └── routers/                 # v1, digitaltwin, internal/*, …
│
├── back/                        # Core domain, objects, GraphQL (Strawberry)
│   ├── core/                    # Databricks, W3C, triplestore, reasoning, graph_analysis, registry helpers, …
│   ├── objects/                 # Session, project, registry, digital twin models
│   └── fastapi/                 # GraphQL wiring with core services
│
├── front/                       # HTML UI: routes, templates, static assets
│   ├── routes/                  # home, ontology, mapping, digitaltwin, project, …
│   ├── templates/               # Jinja2 pages and partials
│   ├── static/                  # CSS, JS; OntoViz under static/global/ontoviz/
│   ├── config/                  # menu_config.json (navbar / sidebar)
│   └── fastapi/dependencies.py  # Shared FastAPI deps (templates, sessions)
│
├── shared/                      # App factory, health, configuration
│   ├── fastapi/main.py          # create_app(), middleware, static mount, router includes
│   ├── fastapi/health.py
│   └── config/                  # settings, constants
│
└── mcp-server/                  # MCP server (separate Databricks App)
    ├── server/
    │   ├── app.py               # MCP tools, text formatting, combined app
    │   └── main.py              # Entry point
    ├── app.yaml                 # Databricks App config
    ├── deploy-mcp-server.sh     # Deployment script
    └── pyproject.toml           # Dependencies
```

See [Architecture Documentation](docs/architecture.md) for detailed diagrams and explanations.

## OntoViz Library

OntoBricks includes **OntoViz**, a custom JavaScript library for visual entity-relationship diagram editing. OntoViz is **reusable** and can be integrated into other projects.

### Features
- **Entity management**: Create, edit, delete entities with attributes
- **Relationship management**: Connect entities with named relationships and direction control
- **Inheritance links**: Define class hierarchies (rdfs:subClassOf) with property inheritance
- **Drag-and-drop**: Move entities freely on the canvas
- **Auto-layout**: Grid and force-directed layout algorithms
- **Zoom & Pan**: Navigate large diagrams easily
- **Direction control**: Forward, reverse, or bidirectional relationships
- **Icons & descriptions**: Visual customization for entities
- **Theming**: Greyscale theme for professional appearance
- **Export/Import**: JSON serialization for persistence

### Standalone Usage

OntoViz can be used independently. See `src/front/static/global/ontoviz/index.html` for a demo.

```javascript
const canvas = new OntoViz(container, {
    showToolbar: true,
    showMinimap: true,
    snapToGrid: true,
    gridSize: 20,
    onEntityCreate: (entity) => console.log('Created:', entity),
    onRelationshipCreate: (rel) => console.log('Connected:', rel),
    onInheritanceCreate: (inh) => console.log('Inheritance:', inh)
});

// Add entities
canvas.addEntity({ name: 'Person', x: 100, y: 100, properties: [{ name: 'name' }] });
canvas.addEntity({ name: 'Employee', x: 100, y: 250 });

// Add inheritance (Employee extends Person)
canvas.addInheritance({ sourceEntityId: 'person-id', targetEntityId: 'employee-id' });
```

## Graph Database & Reasoning

OntoBricks goes beyond static triple storage — it brings **graph database capabilities** and **formal reasoning** directly to the Databricks Lakehouse, eliminating the need for a separate graph infrastructure.

### Dual Triple Store Architecture

OntoBricks offers two interchangeable backends, each providing different graph capabilities:

| Capability | Delta (SQL Warehouse) | LadybugDB (Embedded Graph) |
|------------|----------------------|---------------------------|
| **Storage** | Delta table `(subject, predicate, object)` with Liquid Clustering | Embedded Cypher-native graph database (`real_ladybug`) |
| **Schema** | Flat triple table | Ontology-derived typed node/relationship tables |
| **Traversal** | SQL recursive CTEs | Native Cypher variable-length paths |
| **Transitive closure** | `WITH RECURSIVE` CTE | Cypher `[:REL*2..N acyclic]` paths |
| **Symmetric expansion** | SQL `NOT EXISTS` anti-join | Cypher `NOT EXISTS` subqueries |
| **Shortest path** | Not available (too expensive in SQL) | Cypher `SHORTEST` algorithm |
| **BFS exploration** | Recursive CTE over flat triples | Iterative Cypher expansion |
| **Governance** | Full Unity Catalog lineage and permissions | Local disk + UC Volume sync for persistence |

**LadybugDB Graph Model**: When an ontology is loaded, OntoBricks generates a **typed graph schema** from the OWL classes and properties — each class becomes a Cypher `NODE TABLE` with columns for its datatype properties, and each object property becomes a `REL TABLE`. This brings **property-graph semantics** to knowledge graph data that originates from relational Databricks tables.

### Incremental Sync (Delta Load)

By default, OntoBricks uses **incremental sync** to keep the LadybugDB graph up to date without rebuilding it from scratch on every run. This is critical for large knowledge graphs where a full rebuild is expensive in SQL Warehouse compute, wall-clock time, and graph-DB write I/O.

The incremental pipeline runs in two phases:

```
Sync triggered (manual / scheduled / API)
        │
        ▼
 ┌─────────────────────┐
 │  Phase 1: Gate      │  DESCRIBE HISTORY on each source Delta table
 │  Version Check      │  Compare versions with stored state
 └────────┬────────────┘
          │
     all match? ──yes──▶ SKIP (nothing changed)
          │ no
          ▼
 ┌─────────────────────┐
 │  Phase 2: Diff      │  SQL EXCEPT against a snapshot table
 │  Server-Side        │  → to_add, to_remove
 └────────┬────────────┘
          │
          ▼
 ┌─────────────────────┐
 │  Phase 3: Apply     │  INSERT new triples, DELETE removed triples
 │  Delta to Graph     │  Refresh snapshot for next run
 └─────────────────────┘
```

#### Phase 1 — Version Gate

Before any SQL is executed, OntoBricks runs `DESCRIBE HISTORY <table> LIMIT 1` on each source Delta table referenced in the R2RML entity and relationship mappings. The returned `version` number (monotonically increasing on every Delta commit) is compared to the versions stored at the previous build. If every source table version matches, the build is **skipped entirely** — no VIEW refresh, no diff, no graph writes.

#### Phase 2 — Server-Side Diff via Delta Snapshot Table

When source changes are detected, OntoBricks refreshes the Unity Catalog VIEW and computes a **triple-level diff** entirely server-side on the SQL Warehouse:

```sql
-- Triples to ADD (present in VIEW, absent from snapshot)
SELECT subject, predicate, object FROM <view>
EXCEPT
SELECT subject, predicate, object FROM <snapshot>

-- Triples to REMOVE (present in snapshot, absent from VIEW)
SELECT subject, predicate, object FROM <snapshot>
EXCEPT
SELECT subject, predicate, object FROM <view>
```

The **snapshot table** (`_ob_snapshot_<project>`) is a managed Delta table in the same catalog/schema as the VIEW. It stores the full set of triples from the previous successful build and is refreshed (`CREATE OR REPLACE TABLE ... AS SELECT`) after every build.

#### Phase 3 — Apply Delta to Graph

Only the diff is applied to LadybugDB:

- **Removals** are processed first — relationships are deleted, then attribute values nulled, then orphaned nodes detached and removed
- **Additions** are inserted using the same batch pipeline as a full build

#### Fallback to Full Rebuild

Incremental sync gracefully falls back to a full rebuild when:

| Condition | Reason |
|-----------|--------|
| Ontology or mappings changed | Schema changes invalidate the existing graph structure |
| No snapshot table exists | First build for this domain — must create baseline |
| Diff exceeds 80% of total triples | Incremental overhead is worse than a full reload |
| User selects "Full rebuild" in UI | Explicit opt-in to drop and recreate |
| Any error during incremental path | Safety net — never leaves the graph in a partial state |

#### Build Modes in the UI

The Digital Twin **Status** page offers two build modes:

| Mode | Behaviour |
|------|-----------|
| **Incremental** (default) | Version gate + diff — only changed triples are written |
| **Full rebuild** | Drops the graph and re-inserts all triples from the VIEW |

The **Triple-Store Digital Twin** card shows the snapshot table status below the VIEW name. A **Drop** button lets you delete the snapshot to force the next build to be a full rebuild.

#### API

The REST API accepts a `build_mode` parameter:

```json
POST /api/v1/digitaltwin/build?project_name=my_project
Content-Type: application/json

{ "build_mode": "incremental" }
```

Possible values: `"incremental"` (default), `"full"`.

The build result includes incremental metadata:

```json
{
  "build_mode": "incremental",
  "diff": { "added": 120, "removed": 15 },
  "triple_count": 50000,
  "duration_seconds": 4.2
}
```

When skipped:

```json
{
  "build_mode": "skipped",
  "skipped_reason": "No source table changes detected",
  "duration_seconds": 0.8
}
```

### Reasoning Engine

OntoBricks implements a **multi-phase reasoning pipeline** (`src/back/core/reasoning/`) that combines formal ontology reasoning with graph-structural inference and rule evaluation:

```
                    Reasoning Pipeline
                          │
          ┌───────────────┼───────────────┐───────────────┐
          ▼               ▼               ▼               ▼
    ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────┐
    │  T-Box   │   │   SWRL   │   │  Graph   │   │  Constraint  │
    │ OWL 2 RL │   │  Rules   │   │Reasoning │   │   Checking   │
    └──────────┘   └──────────┘   └──────────┘   └──────────────┘
    Forward-chain   Violation &    Transitive     Cardinality,
    deductive       materializ.    closure,       functional,
    closure on      via SQL or     symmetric      value checks,
    ontology        Cypher         expansion      global rules
```

#### Phase 1: T-Box Reasoning (OWL 2 RL)

OntoBricks uses the **OWL 2 RL** (Rule Language) profile — a decidable subset of OWL 2 designed for rule-based forward chaining. The [`owlrl`](https://owl-rl.readthedocs.io/) library runs a **deductive closure** over the domain's OWL ontology:

- Parses the generated OWL Turtle into an RDFLib graph
- Applies `DeductiveClosure(OWLRL_Semantics).expand(graph)` — forward-chaining all OWL 2 RL entailment rules
- Extracts **newly inferred triples** (filtering axiomatic noise, blank nodes, and tautologies)
- Supports both T-Box-only closure (ontology schema) and optional A-Box closure (ontology + instance data) for small datasets

**Why OWL 2 RL?** Unlike OWL 2 DL (which requires a tableau reasoner), OWL 2 RL is implementable via forward-chaining rules — making it efficient for materialization workflows where inferred triples are written back to the triple store.

#### Phase 2: SWRL Rule Evaluation

OntoBricks includes a **SWRL (Semantic Web Rule Language)** engine that supports user-defined rules with a visual editor:

- **Rule format**: Horn-clause style — `Antecedent → Consequent` with class atoms (`Person(?x)`) and property atoms (`worksIn(?x, ?y)`)
- **Dual execution**: Rules are compiled to **SQL** (for Delta backend) or **Cypher** (for LadybugDB) depending on the active triple store
- **Violation detection**: Finds instances where the antecedent holds but the consequent does not (`NOT EXISTS` patterns)
- **Materialization**: Optionally inserts inferred consequent triples back into the store
- **Graph-aware translation**: When running on LadybugDB with a typed graph schema, the Cypher translator uses the ontology-derived node/relationship tables for efficient pattern matching

#### Phase 3: Graph Reasoning

Leverages **OWL property characteristics** defined in the ontology to perform structural inference:

- **Transitive properties**: Computes the transitive closure along properties marked as `owl:TransitiveProperty` (e.g., `partOf`, `subRegionOf`) — discovering indirect relationships not explicitly asserted
- **Symmetric properties**: Expands properties marked as `owl:SymmetricProperty` (e.g., `adjacentTo`, `siblingOf`) — adding missing inverse edges
- Inferred triples can be **materialized** back into the triple store

#### Phase 4: Constraint Checking

Validates instance data against formal ontology constraints:

| Constraint Type | What It Checks |
|----------------|---------------|
| **Cardinality** | `minCardinality`, `maxCardinality`, `exactCardinality` on properties |
| **Functional** | `owl:FunctionalProperty` — at most one value per subject |
| **Inverse Functional** | `owl:InverseFunctionalProperty` — at most one subject per value |
| **Value constraints** | `notNull`, `startsWith`, `endsWith`, `contains`, `equals`, `matches` (regex) |
| **Global rules** | `noOrphans` (every subject has `rdf:type`), `requireLabels` (every typed entity has `rdfs:label`) |

Constraint checking runs natively in **Cypher** on LadybugDB or as **quality SQL** on the Delta backend.

### From Relational to Graph — Without Leaving Databricks

The key insight behind OntoBricks is that you **don't need a separate graph database** to get graph capabilities. By combining:

1. **OWL ontologies** for formal schema definition
2. **R2RML mappings** for relational-to-graph transformation
3. **Delta Lake** for governed triple storage (or LadybugDB for embedded graph power)
4. **OWL 2 RL reasoning** for ontology-level inference
5. **SWRL rules** for domain-specific logic
6. **Graph algorithms** (transitive closure, BFS, shortest path) via SQL CTEs or Cypher

...OntoBricks delivers a **complete knowledge graph platform** that runs entirely on Databricks infrastructure.

## Semantic Web Standards

OntoBricks leverages these W3C and semantic web standards:

| Standard | Purpose in OntoBricks |
|----------|----------------------|
| **RDF** | Data model — all knowledge graph data is expressed as Subject-Predicate-Object triples |
| **OWL 2** | Ontology language — classes, properties, restrictions, axioms; serialized as Turtle |
| **OWL 2 RL** | Reasoning profile — forward-chaining deductive closure via the `owlrl` library |
| **RDFS** | Schema vocabulary — `subClassOf`, `domain`, `range`, `label` for class hierarchies |
| **R2RML** | W3C mapping language — transforms relational tables into RDF triples |
| **SPARQL** | Query language — used internally, translated to Spark SQL for execution |
| **SWRL** | Rule language — Horn-clause rules for violation detection and triple materialization |
| **SHACL** | Shapes Constraint Language — data quality shapes for validating RDF graphs |
| **GraphQL** | Client-facing query language with typed schema auto-generated from the ontology |
| **Turtle** | Serialization format for OWL and R2RML files |
| **Cypher** | Graph query language — used by LadybugDB backend for native graph operations |

## Documentation

Documentation is grouped by topic in [`docs/`](docs/README.md):

- **[Get started](docs/get-started.md)** — install, configure, environment variables
- **[User guide](docs/user-guide.md)** — features, automated pipeline, ontology import
- **[Deployment](docs/deployment.md)** — Apps, resources, MCP server
- **[Architecture](docs/architecture.md)** — design, OntoViz, agentic stack, incremental sync spec
- **[API](docs/api.md)** — external REST/GraphQL and internal REST reference
- **[MCP](docs/mcp.md)** — Playground and client configuration
- **[Development](docs/development.md)** — dependencies, testing, SDK notes
- **[Product](docs/product.md)** — value proposition and innovation framing
- **[Examples](docs/examples.md)** — walkthrough tutorials

## Development

```bash
# Run tests
make test

# Format code
make format

# Run in development mode
make dev

# Show all commands
make help
```

## Technology Stack

| Layer | Technologies |
|-------|-------------|
| **Backend** | Python 3.10+, FastAPI 0.109+, Uvicorn, RDFLib 7.0, Strawberry GraphQL |
| **Reasoning** | owlrl 7.0+ (OWL 2 RL forward chaining), PySHACL 0.26+ (SHACL validation), custom SWRL engine (SQL + Cypher translators) |
| **Graph Analysis** | NetworkX 3.0+ (community detection: Louvain, Label Propagation, Greedy Modularity), Graphology communities-louvain (client-side) |
| **Graph** | LadybugDB / real_ladybug (embedded Cypher graph DB), Cypher query language, typed node/rel tables |
| **Frontend** | Bootstrap 5.3, Sigma.js 3.x, Graphology, D3.js 7.x, OntoViz, Vanilla JS |
| **Data** | Databricks SQL Connector, Unity Catalog, Delta Lake |
| **Agents & Observability** | MLflow 2.19+ (tracing, ResponsesAgent, Databricks Agent Framework) |
| **MCP** | FastMCP 2.3+, httpx (Streamable HTTP + stdio transports) |
| **Testing** | pytest, httpx |

## License

MIT License - see [LICENSE](LICENSE)

## Resources

### Standards & Specifications
- **[OWL 2 Overview](https://www.w3.org/TR/owl2-overview/)** - Web Ontology Language
- **[OWL 2 RL Profile](https://www.w3.org/TR/owl2-profiles/#OWL_2_RL)** - Rule-based reasoning profile used by OntoBricks
- **[SWRL](https://www.w3.org/submissions/SWRL/)** - Semantic Web Rule Language
- **[SHACL](https://www.w3.org/TR/shacl/)** - Shapes Constraint Language for RDF validation
- **[R2RML Specification](https://www.w3.org/TR/r2rml/)** - W3C RDB to RDF Mapping Language
- **[SPARQL 1.1](https://www.w3.org/TR/sparql11-query/)** - Query Language for RDF
- **[RDF 1.1 Primer](https://www.w3.org/TR/rdf11-primer/)** - Resource Description Framework

### Libraries & Frameworks
- **[RDFLib](https://rdflib.readthedocs.io/)** - Python library for RDF
- **[owlrl](https://owl-rl.readthedocs.io/)** - OWL 2 RL reasoner for RDFLib (forward-chaining deductive closure)
- **[PySHACL](https://github.com/RDFLib/pySHACL)** - SHACL validator for RDFLib graphs (data quality shapes)
- **[LadybugDB (real_ladybug)](https://pypi.org/project/real-ladybug/)** - Embedded graph database with Cypher support
- **[MLflow](https://mlflow.org/)** - ML lifecycle platform (agent tracing & evaluation)
- **[Model Context Protocol](https://modelcontextprotocol.io/)** - Open standard for LLM tool integration

### Databricks
- **[Databricks Docs](https://docs.databricks.com/)** - Databricks platform documentation
- **[Databricks Agent Framework](https://docs.databricks.com/en/generative-ai/agent-framework/index.html)** - Build, evaluate, and deploy agents

---

**OntoBricks** — Bridging relational data, graph databases, and formal reasoning on Databricks.
