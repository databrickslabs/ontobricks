# OntoBricks MCP Server

OntoBricks exposes its Digital Twin knowledge-graph capabilities via the
[Model Context Protocol (MCP)](https://modelcontextprotocol.io/), allowing
LLM-based tools to browse projects, discover entity types, look up specific
entities with full-text descriptions, and check triple-store health — all
through a standardised interface.

The MCP server lives in the **`src/mcp-server/`** directory as a self-contained
Python project deployed separately from the main OntoBricks web application.

---

## Workflow

The MCP server follows a **two-step workflow**:

1. **Choose a project** — call `list_projects` to see available knowledge graphs with descriptions, then `select_project` to activate one. Only projects with the **API / MCP** flag enabled in OntoBricks are listed.
2. **Query the knowledge graph** — use `list_entity_types`, `describe_entity`, or `get_status` on the selected project.

**Advanced — GraphQL querying:**

After selecting a project, the LLM can also leverage GraphQL for structured data retrieval:
1. Call `get_graphql_schema` to discover the typed schema (types, fields, relationships).
2. Call `query_graphql` with a GraphQL query to retrieve data with nested traversal, specific field selection, and filtering.

The LLM is instructed (via the MCP `instructions` field) to always select a
project before querying entities. If the user's question clearly refers to a
domain covered by one of the projects, the LLM selects it automatically.

---

## Available Tools

| Tool | Description |
|------|-------------|
| `list_projects` | Lists all projects (knowledge graphs) in the registry with their names and descriptions |
| `select_project` | Activates a project by name — all subsequent queries operate on this project's triple store |
| `list_entity_types` | Returns a human-readable overview of the selected project's knowledge graph: total triples, distinct entities, every entity type with instance count, and predicate usage breakdown |
| `describe_entity` | Searches for an entity by name/type and returns a **full-text description** — identity, attributes, relationships, and related entities discovered hop-by-hop (BFS traversal) |
| `get_status` | Compact diagnostic: project name, backend type, table name, data availability, triple count |
| `get_graphql_schema` | Returns the auto-generated GraphQL schema (SDL) for the selected project — shows types, fields, and relationships |
| `query_graphql` | Executes a GraphQL query against the selected project's knowledge graph with structured, nested results |

### Tool Details

#### `list_projects`

No arguments. Always call this first.

> **Note**: Only projects with the **API / MCP** flag enabled (in Project
> Information → Global tab) are listed. Projects without this flag are hidden
> from both the REST API and MCP tools.

Returns formatted text:

```
Available Projects (3)
========================================
  • customer360
    Customer 360 knowledge graph with interactions, contracts, and claims
  • supply_chain
    Supply chain ontology covering suppliers, products, and logistics
  • hr_analytics
    HR data model with employees, departments, and org structure

No project selected yet — call select_project(<name>) next.
```

#### `select_project`

| Parameter | Type | Description |
|-----------|------|-------------|
| `project_name` | string | Exact project name as shown by `list_projects` |

Returns a confirmation with project status:

```
Project 'customer360' selected.
Backend: delta
Table:   catalog.schema.triplestore
Data:    Yes (12,030 triples)

You can now use list_entity_types and describe_entity.
```

#### `list_entity_types`

No arguments. Requires a project to be selected first.

Returns formatted text:

```
Knowledge Graph — customer360
========================================
Total triples:       12,030
Distinct entities:   1,301
Distinct predicates: 41
Labels:              1,301
Type assertions:     1,301
Relationships:       900

Entity Types
----------------------------------------
  • Customer  (100 instances)
    URI: https://ontobricks.com/ontology#Customer
  • Call  (300 instances)
    URI: https://ontobricks.com/ontology#Call
  ...

Predicates (attributes & relationships)
----------------------------------------
  • hasinteraction  (100 usages)
  • lastname  (100 usages)
  ...
```

#### `describe_entity`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `search` | string | — | Text to search in entity names/labels/URIs (e.g. `"Jacob Martinez"`) |
| `entity_type` | string | — | Filter by type local name (e.g. `"Customer"`) |
| `depth` | int | 2 | BFS traversal depth (1–10) |

Requires a project to be selected first.
At least one of `search` or `entity_type` is required.

Returns formatted text:

```
Found 1 matching entity (33 triples across 3 entities, depth=2)

── Matching Entities ──
■ Jacob Martinez  (Customer)
  URI: https://ontobricks.com/ontology/Customer/CUST00094
  Attributes:
    • firstname: Jacob
    • lastname: Martinez
    • email: customer00094@email.fr
    • phone: 33624261017
    • city: Aix-en-Provence
    • country: France
    • dateofbirth: 1988-12-07
    • segment: professional
    • loyaltypoints: 823
  Relationships:
    → hasinteraction: INT000019

── Related Entities (neighbors) ──
■ INT000019  (Interaction)
  URI: https://ontobricks.com/ontology/Interaction/INT000019
  Attributes:
    • label: Service_Activation via in_person
```

Key features of the text output:
- **URI alias merging** — if an entity has multiple URI patterns (e.g. `…/Customer/CUST00094` and `…/CUST00094`), triples are merged into a single block
- **Predicate prettifying** — URIs like `ontologylastname` become `lastname`, camelCase is split
- **Hop-by-hop structure** — matching entities first, then related entities (neighbors)

#### `get_status`

No arguments. Requires a project to be selected first.

Returns compact text:

```
Project: customer360
Backend: delta
Table:   catalog.schema.triplestore
Status:  OK
Data:    Yes (12,030 triples)
```

#### `get_graphql_schema`

No arguments. Requires a project to be selected first.

Returns the auto-generated GraphQL schema in SDL format. The schema is derived from the project's ontology — each class becomes a GraphQL type, each data property becomes a field, and each object property becomes a typed relationship.

Use this to discover available types and fields before calling `query_graphql`.

```
GraphQL Schema — customer360
==================================================

type Customer {
  id: String!
  label: String
  firstname: String
  lastname: String
  email: String
  city: String
  hasInteraction: [Interaction]
}

type Interaction {
  id: String!
  label: String
  date: String
  channel: String
}

type Query {
  allCustomer(limit: Int = 50, offset: Int = 0, search: String): [Customer!]!
  customer(id: String!): Customer
  allInteraction(limit: Int = 50, offset: Int = 0, search: String): [Interaction!]!
  interaction(id: String!): Interaction
}

Use query_graphql to execute queries against this schema.
```

#### `query_graphql`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | string | — | A valid GraphQL query string |
| `variables` | string | — | Optional JSON string of query variables |

Requires a project to be selected first.

Executes a GraphQL query against the knowledge graph and returns structured, formatted results. Ideal for:
- Fetching specific fields without over-fetching
- Nested relationship traversal in a single request
- Filtering with `search`, pagination with `limit`/`offset`

**Example call:**

```
query_graphql(
  query: "{ allCustomer(limit: 3, search: \"Martinez\") { id label email hasInteraction { label date } } }"
)
```

**Returns:**

```
GraphQL Result — customer360
==================================================

allCustomer (1 results)
----------------------------------------
  id: Customer/CUST00094
  label: Jacob Martinez
  email: customer00094@email.fr
  hasInteraction:
      label: Service_Activation via in_person
      date: 2024-01-15
      ---
```

**When to use `query_graphql` vs `describe_entity`:**

| Use case | Recommended tool |
|----------|-----------------|
| Look up an entity by name with full traversal | `describe_entity` |
| Fetch specific fields across many entities | `query_graphql` |
| Get all attributes and relationships for one entity | `describe_entity` |
| Nested relationship queries (2+ levels) | `query_graphql` |
| Explore an unfamiliar project | `get_graphql_schema` → `query_graphql` |

## Available Resources

| URI | Description |
|-----|-------------|
| `ontobricks://projects` | List of projects in the registry (JSON) |
| `ontobricks://status` | Current triple store status for the selected project (JSON) |
| `ontobricks://stats` | Triple store content statistics for the selected project (JSON) |
| `ontobricks://graphql-schema` | GraphQL schema (SDL) for the selected project (JSON) |

---

## Databricks Playground (Custom MCP Server)

The MCP server is deployed as **`mcp-ontobricks`**, a separate Databricks
App whose name starts with `mcp-` so it is automatically discoverable in
the Databricks Playground.

### How it works

```
Databricks Playground / Agent
    │
    │  Streamable HTTP (Databricks OAuth)
    ▼
mcp-ontobricks  (Databricks App)
    │
    │  httpx  →  ONTOBRICKS_URL
    ▼
OntoBricks  (Databricks App)
    ├── /api/v1/digitaltwin/*    (REST — entity search, stats, status)
    └── /graphql/{project}       (GraphQL — typed queries, nested traversal)
    │
    ▼
Triple Store (Delta Lake via SQL Warehouse)
```

`mcp-ontobricks` is a lightweight FastAPI + FastMCP application that
forwards every tool call to the main OntoBricks REST API via `httpx`.
Authentication between the two apps uses Databricks OAuth (service principal).

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ONTOBRICKS_URL` | Yes | `http://localhost:8000` | URL of the main OntoBricks app |
| `REGISTRY_CATALOG` | Yes (deployed) | — | Unity Catalog catalog containing the project registry |
| `REGISTRY_SCHEMA` | Yes (deployed) | — | Schema within the catalog |
| `REGISTRY_VOLUME` | No | `OntoBricksRegistry` | Volume name for project storage |

The registry variables are passed as query parameters to every
`/api/v1/digitaltwin/*` call, letting the MCP server operate without a
browser session.  Set them in `src/mcp-server/app.yaml` to match the
registry you configured in the OntoBricks Settings UI.

### Project structure

```
src/mcp-server/
├── app.yaml                 # Databricks App config (command + env vars)
│                            #   ONTOBRICKS_URL, REGISTRY_CATALOG/SCHEMA/VOLUME
├── deploy-mcp-server.sh     # One-command deployment script
├── requirements.txt         # "uv" — dependency manager
├── pyproject.toml           # Python dependencies
└── server/
    ├── __init__.py
    ├── app.py               # MCP tools, project selection, text formatting,
    │                        #   URI helpers, combined FastAPI+MCP app factory
    └── main.py              # Entry point: uv run mcp-ontobricks
```

### Deployment

```bash
# One-command deploy (creates the app if it doesn't exist)
cd mcp-server
./deploy-mcp-server.sh

# Or manual steps:
databricks sync mcp-server "/Users/$USER/mcp-ontobricks" --watch=false
databricks apps deploy mcp-ontobricks \
  --source-code-path "/Workspace/Users/$USER/mcp-ontobricks"
```

### Using in Playground

1. Go to your Databricks workspace
2. Navigate to **Playground**
3. **mcp-ontobricks** appears in the MCP Servers list (apps starting with `mcp-` are shown automatically)
4. Select it — you now have access to `list_entity_types`, `describe_entity`, and `get_status`
5. Ask questions like *"What entity types are in the knowledge graph?"* or *"Tell me about Jacob Martinez"*

---

## Standalone / Local Usage

### stdio (for Cursor, Claude Desktop, etc.)

Run the standalone entry point from the project root:

```bash
python mcp_server.py              # stdio transport
python mcp_server.py --http       # streamable-http on port 9100
```

Or from the `mcp-server` directory:

```bash
cd mcp-server
uv run python -c "from server.app import create_mcp_server; create_mcp_server('standalone').run(transport='stdio')"
```

By default the server connects to `http://localhost:8000`. Override with:

```bash
ONTOBRICKS_URL=http://your-host:8000 python mcp_server.py
```

If the main app's registry is configured only in the browser session
(not via env vars), pass the registry explicitly:

```bash
REGISTRY_CATALOG=my_catalog REGISTRY_SCHEMA=my_schema python mcp_server.py
```

## Client Configuration

### Cursor

Add to your `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "ontobricks": {
      "command": "python",
      "args": ["mcp_server.py"],
      "cwd": "/path/to/OntoBricks"
    }
  }
}
```

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "ontobricks": {
      "command": "python",
      "args": ["mcp_server.py"],
      "cwd": "/path/to/OntoBricks",
      "env": {
        "ONTOBRICKS_URL": "http://localhost:8000",
        "REGISTRY_CATALOG": "my_catalog",
        "REGISTRY_SCHEMA": "my_schema"
      }
    }
  }
}
```

### Remote HTTP Client

For MCP clients that support Streamable HTTP transport, point to the
deployed Databricks App:

```json
{
  "mcpServers": {
    "ontobricks": {
      "type": "streamable-http",
      "url": "https://<mcp-ontobricks-app-url>/mcp"
    }
  }
}
```

## Testing with MCP Inspector

```bash
npx -y @modelcontextprotocol/inspector
```

Then connect to `https://<mcp-ontobricks-app-url>/mcp` (HTTP) or launch
the stdio server and point the inspector at it.

---

## Text Formatting Pipeline

The MCP server transforms raw JSON API responses into LLM-friendly text:

### REST API responses (`describe_entity`, `list_entity_types`)

1. **URI local-name extraction** — `https://…/Customer/CUST00094` → `CUST00094`
2. **Predicate prettifying** — strips `ontology` prefix, splits camelCase, replaces underscores
3. **Triple classification** — each triple is classified as type assertion, label, attribute (literal object), or relationship (URI object)
4. **URI alias merging** — triples from different URI patterns for the same entity ID are merged into a single block
5. **Entity block formatting** — each entity shows name, type, URI, attributes, and relationships
6. **Seed vs. neighbor grouping** — matching entities are shown first, then related entities discovered by BFS

### GraphQL responses (`query_graphql`)

1. **Top-level field grouping** — each root field in the response is rendered with a header and result count
2. **Recursive entity formatting** — nested objects are indented, with key-value pairs rendered inline
3. **List handling** — relationship lists are rendered with `---` separators between items
4. **Error reporting** — GraphQL errors are formatted as bullet-pointed warning lists

---

## Dependencies (src/mcp-server/pyproject.toml)

- `fastmcp >= 2.3.1` — MCP server SDK
- `httpx >= 0.25.0` — Async HTTP client for calling the OntoBricks REST API
- `fastapi >= 0.115.0` — Web framework (health endpoint + combined app)
- `uvicorn >= 0.34.0` — ASGI server
- `pydantic >= 2` — Data validation
- `databricks-sdk >= 0.20.0` — OAuth authentication in Databricks mode
