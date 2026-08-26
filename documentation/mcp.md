# OntoBricks MCP Server

OntoBricks exposes its Knowledge Graph knowledge-graph capabilities via the
[Model Context Protocol (MCP)](https://modelcontextprotocol.io/), allowing
LLM-based tools to browse domains, discover entity types, look up specific
entities with full-text descriptions, and check triple-store health — all
through a standardised interface.

The MCP server lives in the **`src/mcp-server/`** directory as a self-contained
Python package deployed separately from the main OntoBricks web application.

---

## Workflow

The MCP server follows a **two-step workflow**:

1. **Choose a domain** — call `list_domains` to see available graph viewers with descriptions, then `select_domain` to activate one. Only domains with the **API / MCP** flag enabled in OntoBricks are listed.
2. **Query the graph viewer** — use `list_entity_types`, `describe_entity`, or `get_status` on the selected domain.

**Which version?** For each domain folder, the registry stores exactly one **Active** (MCP/API-enabled) version at a time. Operators set that version in the main OntoBricks app under **Registry → Browse** (expand the domain, then **Set as Active** on a row). **Domain → Versions** shows the outcome as a read-only badge but does not change it.

**Advanced — GraphQL querying:**

After selecting a domain, the LLM can also leverage GraphQL for structured data retrieval:
1. Call `get_graphql_schema` to discover the typed schema (types, fields, relationships).
2. Call `query_graphql` with a GraphQL query to retrieve data with nested traversal, specific field selection, and filtering.

The LLM is instructed (via the MCP `instructions` field) to always select a
domain before querying entities. If the user's question clearly refers to a
topic covered by one of the listed domains, the LLM selects it automatically.

---

## Available Tools

| Tool | Description |
|------|-------------|
| `list_domains` | Lists all domains (graph viewers) in the registry with their names and descriptions |
| `select_domain` | Activates a domain by name — all subsequent queries operate on this domain's triple store |
| `list_domain_versions` | Lists registry versions for a named domain (latest first) |
| `get_design_status` | Design pipeline readiness (ontology, metadata, assignment, build_ready) for a domain |
| `describe_ontology` | Returns the selected domain's ontology **structure** — a class inventory (with dataset/bridge/action/virtual-attribute tags) plus the raw OWL/Turtle that carries the full attribute and relationship (domain/range) detail. Reads the ontology schema only, so it works without a built graph and is the sole domain tool an ontology-only domain exposes |
| `list_entity_types` | Returns a human-readable overview of the selected domain's graph viewer: total triples, distinct entities, every entity type with instance count, and predicate usage breakdown |
| `describe_entity` | Searches for an entity by name/type and returns a **full-text description** — identity, attributes, relationships, and related entities discovered hop-by-hop (BFS traversal) |
| `get_entity_context` | Returns a node's external context: linked Unity Catalog dataset (optionally with rows), cross-domain bridges, the Unity Catalog function **actions** configured on its class, and its **virtual attributes** (declared always; values via `compute_virtual_attributes` or the inline `compute_virtual_attributes=True` flag) |
| `compute_virtual_attributes` | Runs the Unity Catalog functions that compute an entity's **virtual attributes** and returns their live values. Call this when the user asks about a virtual attribute — those values are not stored in the graph. Only functions declared on the entity's class can be invoked |
| `invoke_entity_action` | Runs one of the class's Unity Catalog function actions on an entity. The function is called with exactly one argument: the entity's ID. Only functions declared on the entity's ontology class can be invoked |
| `get_status` | Compact diagnostic: domain name, view table, graph name, data availability, triple count |
| `get_graphql_schema` | Returns the auto-generated GraphQL schema (SDL) for the selected domain — shows types, fields, and relationships |
| `query_graphql` | Executes a GraphQL query against the selected domain's graph viewer with structured, nested results |

The first four tools are **registry-level**: they run before a domain is
resolved, so they are always exposed. The other nine are **domain-scoped**
and can be switched off per domain — see [Per-domain MCP policy](#per-domain-mcp-policy).
A domain published with an ontology but no Knowledge Graph build exposes only
`describe_ontology` among the domain-scoped tools — see
[Ontology-only domains](#ontology-only-domains).

### Tool Details

#### `list_domains`

No arguments. Always call this first.

> **Note**: Only domains with the **API / MCP** flag enabled (in Domain
> Information → Global tab) are listed. Domains without this flag are hidden
> from both the REST API and MCP tools.

Returns formatted text:

```
Available Domains (3)
========================================
  • customer360
    Customer 360 graph viewer with interactions, contracts, and claims
  • supply_chain
    Supply chain ontology covering suppliers, products, and logistics
  • hr_analytics
    HR data model with employees, departments, and org structure

No domain selected yet — call select_domain(<name>) next.
```

#### `select_domain`

| Parameter | Type | Description |
|-----------|------|-------------|
| `domain_name` | string | Exact domain name as shown by `list_domains` |

Returns a confirmation with domain status:

```
Domain 'customer360' selected.
View:  catalog.schema.triplestore
Graph: customer360_graph
Data:  Yes (12,030 triples)

You can now use list_entity_types and describe_entity.
```

When the domain's MCP policy switches tools off, selecting it also reports
what will not be there — the client's tool list is updated at the same time:

```
Domain 'customer360' selected.
View:  catalog.schema.triplestore
Graph: customer360_graph
Data:  Yes (12,030 triples)

Not available for this domain: invoke_entity_action, query_graphql

You can now use list_entity_types and describe_entity.
```

#### `list_entity_types`

No arguments. Requires a domain to be selected first.

Returns formatted text:

```
Graph Viewer — customer360
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

Requires a domain to be selected first.
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

  [Context — class: Customer]
  Dataset: main.crm.customers  (key: customer_id = 'CUST00094')
    → call get_entity_context(fetch_dataset_rows=True) to retrieve rows
  Bridges:
    → finance / Contract  "Owns contracts"
      Target domain: Finance ontology with contracts and payments
    → to query the target domain, call select_domain(<target_domain>) then re-run describe_entity or GraphQL there. get_entity_context(follow_bridges=True) only peeks — it does NOT switch the session.
```

Key features of the text output:
- **URI alias merging** — if an entity has multiple URI patterns (e.g. `…/Customer/CUST00094` and `…/CUST00094`), triples are merged into a single block
- **Predicate prettifying** — URIs like `ontologylastname` become `lastname`, camelCase is split
- **Hop-by-hop structure** — matching entities first, then related entities (neighbors)
- **Bridges expose target domain descriptions** so the agent can decide to hop with `select_domain(<target>)` — bridges to non-MCP-visible domains are hidden

#### `get_status`

No arguments. Requires a domain to be selected first.

Returns compact text:

```
Domain: customer360
View:    catalog.schema.triplestore
Graph:   customer360_graph
Status:  OK
Data:    Yes (12,030 triples)
```

#### `get_graphql_schema`

No arguments. Requires a domain to be selected first.

Returns the auto-generated GraphQL schema in SDL format. The schema is derived from the domain's ontology — each class becomes a GraphQL type, each data property becomes a field, and each object property becomes a typed relationship.

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

Requires a domain to be selected first.

Executes a GraphQL query against the graph viewer and returns structured, formatted results. Ideal for:
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
| Explore an unfamiliar domain | `get_graphql_schema` → `query_graphql` |

### Hopping across domains

Ontology classes can declare **bridges** to related classes in other
domains (e.g. `Customer` in `customer360` bridges to `Contract` in
`finance`). MCP exposes bridges in two places:

1. `describe_entity` — the `[Context]` block lists each bridge with the
   target domain's **name** and **description** (pulled from the
   registry), plus the target class.
2. `get_entity_context` — the `Cross-domain Bridges:` section same shape,
   with `follow_bridges=True` optionally peeking at matching entities on
   the target graph.

**Only bridges whose target is API/MCP-enabled are shown.** Bridges to
private / non-published domains are hidden so the LLM never proposes a
hop it cannot perform.

To actually query the target domain, the agent must hop:

```
list_domains
   ↓
select_domain("customer360")
   ↓
describe_entity(search="Jacob Martinez")
   ↓  (sees a bridge → finance / Contract with description)
select_domain("finance")     ← ACTUAL hop; previous domain is replaced
   ↓
describe_entity(search="CUST00094", entity_type="Contract")
```

`get_entity_context(follow_bridges=True)` is a **peek only** — it reads
the target graph in a single request and returns the matching triples,
but `_selected_domain` is unchanged. Any subsequent `describe_entity`,
GraphQL, or `list_entity_types` still runs on the origin domain. Prefer
`select_domain(<target>)` whenever the user's question requires more
than a lookup.

## Per-domain MCP policy

Each domain decides what it publishes over MCP, from **Domain → Information
→ MCP**. A domain that has never been configured behaves exactly as before
0.8: every tool exposed, every attachment surfaced normally.

### Exposed tools

The nine domain-scoped tools have one checkbox each. Unchecking one removes
it from `tools/list` for any session that selects the domain, and refuses the
call if a client tries it anyway.

The four registry-level tools (`list_domains`, `select_domain`,
`list_domain_versions`, `get_design_status`) are shown read-only. They run
before a domain is resolved, so no per-domain policy can govern them —
hiding `select_domain` would make the domain unusable *and* unrecoverable.

### Ontology context

`Datasets`, `Bridges`, `Actions` and `Virtual attributes` each take one of
three states:

| State | Effect |
|---|---|
| **Preferred** | The follow-up hint becomes a directive instruction ("ALWAYS follow these bridges…") instead of a neutral mention. Nothing is reordered and no payload changes |
| **Normal** | Today's behaviour |
| **Disabled** | The element is withheld from every MCP and external REST response |

Disabling is enforced server-side, not by the MCP process: the element never
leaves `/api/v1/digitaltwin/nodes/context` or `/api/v1/domain/classes`. The
authoring UI is unaffected and always shows the ontology designer everything.

> **Overlap to know about.** The `invoke_entity_action` **tool** and the
> `Actions` **context element** are separate switches over the same feature.
> Disabling the element also refuses invocation, even when the tool is still
> checked — otherwise a client that already knew a function name could run it
> after the names were hidden. `Virtual attributes` works the same way: with
> the element disabled, `compute_virtual_attributes(entity_uri)` is refused
> (and so is `get_entity_context(compute_virtual_attributes=True)`) rather than
> silently returning nothing.

### Switching domains switches the tool set

The tool list is recomputed inside `select_domain`, using FastMCP's
session-scoped component visibility, and the client receives a
`ToolListChangedNotification` — no MCP server restart. Selecting a second
domain resets the previous domain's rules first, so a tool hidden by domain A
comes back in domain B.

> **Concurrency limitation.** Visibility rules are per-session, but the
> *selected domain* is not: `create_mcp_server()` runs once per process and
> holds the selection in a module-level closure shared by every connection. If
> two clients select different domains at the same time, the last
> `select_domain` wins for both — and the call-time policy check follows that
> same selection. This predates the policy and affects query results
> identically, so it is not new, but it does mean the policy is only reliable
> for one concurrent client per process. Single-client use (Playground,
> Cursor, Claude Desktop) is unaffected.

Clients that ignore the notification (or replay a cached list) can still emit
a call for a hidden tool. Every domain-scoped tool therefore re-checks the
policy on entry and returns a refusal naming the domain.

### Ontology-only domains

A domain can be published with an ontology but **no Knowledge Graph build**
(no mapping, no graph). The lifecycle allows it: `DRAFT → IN-REVIEW` now
requires *either* a build *or* a valid ontology, so an ontology-only version
goes through the same `DRAFT → IN-REVIEW → PUBLISHED` workflow.

`GET /api/v1/domains` flags such a domain with `has_graph: false` (the
numeric-latest PUBLISHED version has never been built). When a client selects
it, the MCP server hides **every** graph tool and leaves `describe_ontology`
as the only domain-scoped tool — on top of, and independent from, the
per-domain policy. `describe_ontology` reads the ontology schema (not the
graph), so it is always usable; the call-time guard refuses the graph tools
for that domain even if a stale client calls them. `list_domains` marks these
domains `(ontology-only)`.

This restriction is computed from `has_graph`, not stored in `mcp_policy`: a
domain automatically regains its graph tools once its published version is
built.

### Storage

The policy lives in the registry, in the `domains.mcp_policy` JSONB column,
and is published on `GET /api/v1/domains`:

```json
{
  "name": "customer360",
  "description": "Customer 360 ontology",
  "mcp_policy": {
    "disabled_tools": ["query_graphql"],
    "context": {"bridges": "preferred", "actions": "disabled",
                "virtual_attributes": "preferred"}
  }
}
```

Only non-default entries are stored, so an unconfigured domain is `{}`. The
column sits on `domains`, not `domain_versions`: the policy is a property of
the domain and applies to all of its versions.

Upgrading an existing registry needs
`scripts/migrations/upgrade_0.7_to_0.8.sql` (or `make bootstrap-lakebase`);
the app also self-heals the column lazily.

---

## Available Resources

| URI | Description |
|-----|-------------|
| `ontobricks://domains` | List of domains in the registry (JSON) |
| `ontobricks://status` | Current triple store status for the selected domain (JSON) |
| `ontobricks://stats` | Triple store content statistics for the selected domain (JSON) |
| `ontobricks://graphql-schema` | GraphQL schema (SDL) for the selected domain (JSON) |

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
    └── /graphql/{domain}        (GraphQL — typed queries, nested traversal; path segment is the registry domain name)
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
| `REGISTRY_CATALOG` | Yes (deployed) | — | Unity Catalog catalog containing the domain registry |
| `REGISTRY_SCHEMA` | Yes (deployed) | — | Schema within the catalog |
| `REGISTRY_VOLUME` | No | `OntoBricksRegistry` | Volume name for domain registry storage |

The registry variables are passed as query parameters to every
`/api/v1/digitaltwin/*` call, letting the MCP server operate without a
browser session.  Set them in `src/mcp-server/app.yaml` to match the
registry you configured in the OntoBricks Settings UI.

### MCP server layout

```
src/mcp-server/
├── app.yaml                 # Databricks App config (command + env vars)
│                            #   ONTOBRICKS_URL, REGISTRY_CATALOG/SCHEMA/VOLUME
├── deploy-mcp-server.sh     # One-command deployment script
├── requirements.txt         # "uv" — dependency manager
├── pyproject.toml           # Python dependencies
└── server/
    ├── __init__.py
    ├── app.py               # MCP tools, domain selection, text formatting,
    │                        #   URI helpers, combined FastAPI+MCP app factory
    └── main.py              # Entry point: uv run mcp-ontobricks
```

### Deployment

The MCP server ships in the **same Databricks Asset Bundle** as the main app
(`databricks.yml` → `mcp_ontobricks_app`). Prefer the DAB path:

```bash
# Deploy both app definitions (from repo root)
make deploy
# or: scripts/deploy.sh -t <DAB_TARGET>

# Start the MCP app if it is not already running
databricks bundle run mcp_ontobricks_app -t <DAB_TARGET>
```

Legacy standalone script (still under `src/mcp-server/` for one-off workspace
deploys outside the bundle):

```bash
cd src/mcp-server
./deploy-mcp-server.sh
```

See `documentation/deployment.md` §7 for Playground wiring and `app.yaml` env.

To register the deployed MCP app as a **Unity Catalog HTTP connection** and
attach it to **Genie One** (plus Playground / Genie Code), follow
[`documentation/uc-mcp-connection-genie-one.md`](uc-mcp-connection-genie-one.md).

### Using in Playground

1. Go to your Databricks workspace
2. Navigate to **Playground**
3. **mcp-ontobricks** appears in the MCP Servers list (apps starting with `mcp-` are shown automatically)
4. Select it — you now have access to `list_entity_types`, `describe_entity`, and `get_status`
5. Ask questions like *"What entity types are in the graph viewer?"* or *"Tell me about Jacob Martinez"*

---

## Standalone / Local Usage

### stdio (for Cursor, Claude Desktop, etc.)

Run the standalone entry point from the repository root:

```bash
python src/mcp-server/mcp_server.py              # stdio transport
python src/mcp-server/mcp_server.py --http       # streamable-http on port 9100
```

Or from the `mcp-server` directory:

```bash
cd mcp-server
uv run python -c "from server.app import create_mcp_server; create_mcp_server('standalone').run(transport='stdio')"
```

By default the server connects to `http://localhost:8000`. Override with:

```bash
ONTOBRICKS_URL=http://your-host:8000 python src/mcp-server/mcp_server.py
```

If the main app's registry is configured only in the browser session
(not via env vars), pass the registry explicitly:

```bash
REGISTRY_CATALOG=my_catalog REGISTRY_SCHEMA=my_schema python src/mcp-server/mcp_server.py
```

## Client Configuration

### Cursor

Add to your `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "ontobricks": {
      "command": "python",
      "args": ["src/mcp-server/mcp_server.py"],
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
      "args": ["src/mcp-server/mcp_server.py"],
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
