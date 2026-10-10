# OntoBricks MCP Server

OntoBricks exposes its Knowledge Graph knowledge-graph capabilities via the
[Model Context Protocol (MCP)](https://modelcontextprotocol.io/), allowing
LLM-based tools to browse domains, discover entity types, look up specific
entities with full-text descriptions, and check triple-store health — all
through a standardised interface.

The MCP server lives in the **`src/mcp-server/`** directory as a self-contained
Python package deployed separately from the main OntoBricks web application.

<!-- toc -->
**Contents**

- [Workflow](#workflow)
- [Available Tools](#available-tools)
- [Per-domain MCP policy](#per-domain-mcp-policy)
- [Available Resources](#available-resources)
- [Databricks Playground (Custom MCP Server)](#databricks-playground-custom-mcp-server)
- [Standalone / Local Usage](#standalone--local-usage)
- [Client Configuration](#client-configuration)
- [Testing with MCP Inspector](#testing-with-mcp-inspector)
- [Text Formatting Pipeline](#text-formatting-pipeline)
- [Dependencies (src/mcp-server/pyproject.toml)](#dependencies-srcmcp-serverpyprojecttoml)
- [Register the MCP in Unity Catalog and Genie One](#register-the-mcp-in-unity-catalog-and-genie-one)
<!-- /toc -->

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
| `run_entity_business_rule` | Runs one of the SWRL **business rules** attached to the entity's class, scoped to that entity, and materialises the inferred triples into the graph (no confirmation step). Requires the Builder role on the domain; only rules declared on the class can be run. On Neo4j-backed domains it currently infers nothing |
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
| `depth` | int | 1 | BFS traversal depth (1–10). Use `depth=1` first for broad type-wide scans. |

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

(Showing 100 of 420 triples — increase limit or use pagination for more)
```

Key features of the text output:
- **URI alias merging** — if an entity has multiple URI patterns (e.g. `…/Customer/CUST00094` and `…/CUST00094`), triples are merged into a single block
- **Predicate prettifying** — URIs like `ontologylastname` become `lastname`, camelCase is split
- **Hop-by-hop structure** — matching entities first, then related entities (neighbors)
- **Bridges expose target domain descriptions** so the agent can decide to hop with `select_domain(<target>)` — bridges to non-MCP-visible domains are hidden
- **Pagination hint fidelity** — truncation text appears only when backend `has_more=true`; `total` remains exact

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
> silently returning nothing. `Business rules` follows the same rule:
> disabled, `run_entity_business_rule` is refused.

### Switching domains switches the tool set

The tool list is recomputed inside `select_domain`, using FastMCP's
session-scoped component visibility, and the client receives a
`ToolListChangedNotification` — no MCP server restart. Selecting a second
domain resets the previous domain's rules first, so a tool hidden by domain A
comes back in domain B.

> **Concurrent sessions.** The selected domain and its label/action caches are
> isolated by MCP session ID for every request, including tool calls and
> resource reads. Two clients connected to one server process can therefore
> select and query different domains without changing each other's state.
> The in-process session store retains at most 512 recently used sessions; an
> evicted client must call `select_domain` again. This state is process-local,
> so a future multi-worker deployment will require a shared session store.

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
    └── main.py              # Entry point: uv run --frozen mcp-ontobricks
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

Legacy wrapper (still under `src/mcp-server/` for compatibility; it delegates
to the repository DAB deployment):

```bash
cd src/mcp-server
./deploy-mcp-server.sh
```

See `docs/deployment.md` §7 for Playground wiring and `app.yaml` env.

To register the deployed MCP app as a **Unity Catalog HTTP connection** and
attach it to **Genie One** (plus Playground / Genie Code), follow
[Register the MCP in Unity Catalog and Genie One](#register-the-mcp-in-unity-catalog-and-genie-one)
at the end of this guide.

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
uv run --frozen python -c "from server.app import create_mcp_server; create_mcp_server('standalone').run(transport='stdio')"
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

---

## Register the MCP in Unity Catalog and Genie One

This procedure registers the deployed OntoBricks MCP Databricks App as a
**Unity Catalog HTTP connection** (with an optional **MCP Service**), then
attaches that connection to a **Genie One** chat so Genie can call OntoBricks
tools (`list_domains`, `select_domain`, `describe_entity`, GraphQL, …).

Genie One will not list a custom MCP until the Unity Catalog connection exists.
Create the connection first, then add it to the conversation.

Official Databricks references:

- [Connect to external HTTP services](https://docs.databricks.com/aws/en/query-federation/http)
- [Register an external MCP server](https://docs.databricks.com/aws/en/ai-gateway/register-mcp-service)
- [Connect to external tools and sources (Genie One)](https://docs.databricks.com/aws/en/genie-one/external-sources)

---

### What you end up with

```
Genie One chat  (or AI Playground / Genie Code)
        │
        │  Unity Catalog HTTP proxy  (managed credentials)
        ▼
UC connection  ontobricks_mcp_<instance>
        │
        │  Streamable HTTP  POST …/mcp
        ▼
mcp-ontobricks-<instance>   Databricks App
        │
        │  ONTOBRICKS_URL
        ▼
ontobricks-<instance>       main FastAPI app + graph viewer
```

Databricks recommends **OAuth M2M** for custom MCP connections used from
Genie One. User-to-machine (U2M) OAuth works but needs an extra OAuth app and
redirect URI. This procedure uses M2M.

---

### Prerequisites

| Item | Why |
|------|-----|
| Databricks CLI ≥ 0.229, profile pointing at the **same workspace** as the MCP app | All API calls below |
| `CREATE CONNECTION` on the metastore (workspace admins have it on auto-enabled UC workspaces) | Create the HTTP connection |
| `USE CATALOG` / `USE SCHEMA` / `CREATE SERVICE` on a catalog.schema you own | Optional MCP Service |
| MCP app running, name starting with `mcp-` | Streamable HTTP at `/mcp` |
| **CAN_USE** on that MCP app (you, and the service principal created below) | Token is accepted by Apps |
| Model Serving region + **Third Party Connectors for Agents** preview | Genie One custom MCP picker |
| Domains with **API / MCP** enabled in OntoBricks | Otherwise `list_domains` is empty |

Confirm the MCP app:

```bash
PROFILE=DEFAULT          # your CLI profile
MCP_APP=mcp-ontobricks-08x

databricks apps get "$MCP_APP" -p "$PROFILE" --output json \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['url'], d['app_status']['state'])"
```

The MCP endpoint is always:

```text
https://<mcp-app-url>/mcp
```

Example: `https://mcp-ontobricks-08x-2508734981122804.aws.databricksapps.com/mcp`

---

### Variables

Fill these once; every command below uses them.

```bash
export PROFILE=DEFAULT
export WORKSPACE_HOST=https://fe-vm-bcayla-demos.cloud.databricks.com   # no trailing slash
export MCP_APP=mcp-ontobricks-08x
export MCP_APP_URL=https://mcp-ontobricks-08x-2508734981122804.aws.databricksapps.com
export CONN_NAME=ontobricks_mcp_08x
export SP_DISPLAY_NAME=ontobricks-mcp-08x-uc-connection
export SP_APP_ID=   # filled in step 1

# Optional MCP Service (AI Gateway / Playground three-level name)
export UC_CATALOG=benoit_cayla
export UC_SCHEMA=ontobricks_demo_08x_sc
export MCP_SERVICE_ID=ontobricks_mcp
```

---

### 1. Create a service principal and OAuth secret

Genie One injects this principal’s token into requests to the MCP app.
Do **not** reuse the MCP app’s own service principal.

```bash
databricks service-principals create \
  --display-name "$SP_DISPLAY_NAME" --active \
  -p "$PROFILE" --output json

# id = numeric SCIM id (for secrets). applicationId = OAuth client_id.
eval "$(
  databricks service-principals list -p "$PROFILE" --output json \
    | SP_DISPLAY_NAME="$SP_DISPLAY_NAME" python3 -c "
import json,sys,os
name=os.environ['SP_DISPLAY_NAME']
for s in json.load(sys.stdin):
    if s.get('displayName')==name:
        print('SP_ID='+s['id'])
        print('SP_APP_ID='+s['applicationId'])
        break
"
)"
export SP_ID SP_APP_ID
echo "SP_ID=$SP_ID SP_APP_ID=$SP_APP_ID"
```

Create an OAuth secret (730 days). The secret is shown **once**.

```bash
databricks service-principal-secrets-proxy create "$SP_ID" \
  --lifetime 63072000s -p "$PROFILE" --output json \
  > /tmp/ontobricks_sp_secret.json
# Keep that file out of git. Extract:
python3 -c "import json; print(json.load(open('/tmp/ontobricks_sp_secret.json'))['secret'])"
# Store it as CLIENT_SECRET in your shell only.
```

---

### 2. Grant the principal CAN_USE on the MCP app

```bash
databricks apps update-permissions "$MCP_APP" -p "$PROFILE" --json "$(
python3 -c "
import json,os
print(json.dumps({'access_control_list':[{
  'service_principal_name': os.environ['SP_APP_ID'],
  'permission_level': 'CAN_USE'}]}))
")"
```

`users` already having CAN_USE is not enough if the principal is not in that
group. Grant it explicitly.

---

### 3. Create the Unity Catalog HTTP connection

`is_mcp_connection=true` marks the connection for MCP / Genie One / Playground.

OAuth token endpoint is the **workspace** OIDC token URL, not the Apps URL.

```bash
python3 <<'PY'
import json, os
secret = json.load(open("/tmp/ontobricks_sp_secret.json"))["secret"]
body = {
  "name": os.environ["CONN_NAME"],
  "connection_type": "HTTP",
  "comment": "OntoBricks MCP Databricks App (streamable HTTP /mcp) for Genie One.",
  "read_only": True,
  "options": {
    "host": os.environ["MCP_APP_URL"],
    "port": "443",
    "base_path": "/mcp",
    "client_id": os.environ["SP_APP_ID"],
    "client_secret": secret,
    "oauth_scope": "all-apis",
    "token_endpoint": os.environ["WORKSPACE_HOST"].rstrip("/") + "/oidc/v1/token",
    "is_mcp_connection": "true",
  },
}
open("/tmp/ontobricks_uc_conn.json", "w").write(json.dumps(body))
PY

databricks connections create -p "$PROFILE" --json @/tmp/ontobricks_uc_conn.json --output json
rm -f /tmp/ontobricks_uc_conn.json /tmp/ontobricks_sp_secret.json
```

Success looks like:

- `connection_type`: `HTTP`
- `credential_type`: `OAUTH_M2M`
- `provisioning_info.state`: `ACTIVE`
- `options.access_token_expiration` present (token exchange worked)
- `url`: `https://<mcp-app-url>:443/mcp`

#### UI alternative (same result)

1. Catalog → **+** → **Create a connection**.
2. Type **HTTP**. Auth **OAuth Machine to Machine**.
3. Host = MCP app URL (no `/mcp`). Port `443`. Base path `/mcp`.
4. Client ID = service principal `applicationId`. Client secret = OAuth secret.
5. Token endpoint = `https://<workspace-host>/oidc/v1/token`.
6. Scope `all-apis`. Enable the MCP-connection flag if the form shows it.
7. Save. Confirm the connection is **Active**.

---

### 4. (Optional) Register an MCP Service

Needed for **AI Gateway** governance and Playground **External MCP servers**.
Genie One’s picker uses the **connection** name; the MCP Service is still useful
for grants and tool selection.

```bash
databricks api post \
  "/api/2.1/unity-catalog/mcp-services?parent=schemas/${UC_CATALOG}.${UC_SCHEMA}&mcp_service_id=${MCP_SERVICE_ID}" \
  --json "{
    \"comment\": \"OntoBricks MCP companion server\",
    \"config\": {
      \"source_connection\": { \"name\": \"connections/${CONN_NAME}\" },
      \"include_tool_selectors\": []
    }
  }" \
  -p "$PROFILE"
```

An empty `include_tool_selectors` exposes every tool. The three-level name is:

```text
<catalog>.<schema>.<mcp_service_id>
```

Example: `benoit_cayla.ontobricks_demo_08x_sc.ontobricks_mcp`

---

### 5. Grants

| Who | Privilege | On |
|-----|-----------|----|
| You (already owner) | — | connection + MCP Service |
| Colleagues who will use Genie One | `USE CONNECTION` | the HTTP connection |
| Colleagues who will invoke via AI Gateway | `EXECUTE` | the MCP Service |
| Do **not** grant `USE CONNECTION` to end users if you only want them going through the MCP Service | — | they could bypass tool policies |

SQL (warehouse with UC):

```sql
GRANT USE CONNECTION ON CONNECTION ontobricks_mcp_08x TO `user@example.com`;
GRANT EXECUTE ON <catalog>.<schema>.ontobricks_mcp TO `user@example.com`;
```

---

### 6. Add the MCP server in Genie One

This is the step that actually makes OntoBricks tools available in a chat.

#### 6.1 Enable the preview (workspace admin, once)

1. Click your user menu → **Previews**.
2. Turn on **Third Party Connectors for Agents** (required for Genie One
   external / custom MCP connections).
3. If Genie One itself is missing, also enable **Genie One** / **Chat in Genie
   One** for the workspace.

Custom MCP connections only work in regions that support **Model Serving**.

#### 6.2 Attach the connection to a conversation

1. Open **Genie One** (workspace home / Genie One entry point — not a classic
   Genie Agent / former Genie Space).
2. On the Genie One home page, click the **+** (plus) at the **bottom left of
   the search bar**.
3. Pick a built-in source if you need one, or click **More connections**.
4. Choose **custom MCP** / Unity Catalog connection.
5. Select the connection you created (`ontobricks_mcp_08x` in the example).
   - If it is missing: the connection is not `HTTP` + `is_mcp_connection`, or
     you lack `USE CONNECTION`, or it is still provisioning.
6. Click **Sign in** only if the connection uses per-user OAuth (U2M).
   **M2M connections skip this** — Databricks already holds the client secret.
7. Confirm the connection appears on the conversation (chip / attached source).

Databricks rule: **the Unity Catalog connection must exist before you can add
it to a Genie One chat.** You cannot create it from the plus menu alone.

#### 6.3 Check that Genie will actually call the tools

Genie One does not always pick a custom MCP on the first message. Prompt it
explicitly until you see tool traces:

```text
Use the OntoBricks MCP server. Call list_domains and list every domain.
```

Then:

```text
Select the domain <name> and describe entity types in the graph viewer.
```

```text
In domain <name>, tell me about <entity>.
```

If search “doesn’t start”, Databricks’s own guidance is to name the tool or
source in the prompt (`use OntoBricks`, `use list_domains`).

#### 6.4 What “good” looks like

- The conversation lists the OntoBricks connection as an attached source.
- A question about domains triggers `list_domains` (only API/MCP-enabled
  domains appear).
- A follow-up that names a person or ID triggers `select_domain` then
  `describe_entity` or GraphQL.
- Errors that mention 401 / Apps OAuth almost always mean the M2M principal
  lacks **CAN_USE** on `mcp-ontobricks-*`.
- Empty domain lists mean the registry flag **API / MCP** is off, or
  `ONTOBRICKS_URL` on the MCP app is wrong.

---

### 7. Optional: same connection in Playground and Genie Code

**AI Playground**

1. Playground → model with **Tools**.
2. **Tools → + Add tool → MCP Servers → External MCP servers**.
3. Select the MCP Service (`catalog.schema.ontobricks_mcp`) or the HTTP
   connection, depending on the picker.

**Genie Code** (coding agent, not Genie One chat)

1. Open the Genie Code pane → **Settings**.
2. **MCP Servers → Add Server**.
3. Either:
   - **External MCP server** → the Unity Catalog connection (login first if U2M), or
   - **Custom MCP server** → the Databricks App `mcp-ontobricks-*` directly
     (same workspace, endpoint `/mcp`, stateless HTTP). Same-workspace Apps
     do not need the UC connection; Genie One custom MCP **does**.

---

### 8. Worked example (08x sandbox)

These objects were created on workspace `fe-vm-bcayla-demos` for the running
app `mcp-ontobricks-08x`:

| Object | Name |
|--------|------|
| MCP app URL | `https://mcp-ontobricks-08x-2508734981122804.aws.databricksapps.com` |
| HTTP connection | `ontobricks_mcp_08x` (OAuth M2M, `/mcp`, Active) |
| Service principal | `ontobricks-mcp-08x-uc-connection` |
| MCP Service | `benoit_cayla.ontobricks_demo_08x_sc.ontobricks_mcp` |

Reuse them in Genie One: **+** on the search bar → **More connections** →
`ontobricks_mcp_08x`.

---

### 9. Troubleshooting

| Symptom | Check |
|---------|--------|
| Connection create fails on token endpoint | `WORKSPACE_HOST` must be the workspace URL (`https://….cloud.databricks.com/oidc/v1/token`), not the Apps hostname |
| `ACTIVE` but Genie One 401 | `databricks apps get-permissions $MCP_APP` — principal `applicationId` must have CAN_USE |
| Connection missing in Genie One | `is_mcp_connection` true; you have `USE CONNECTION`; preview **Third Party Connectors for Agents** on; Model Serving region |
| Tools never run | Prompt with the server/tool name; confirm domains have API/MCP enabled |
| U2M “must log in” | Expected for per-user OAuth. For Genie One, prefer M2M (this procedure) |
| Schema-level `parent` ignored by `connections create` | Current CLI treats HTTP connections as metastore-level; name is `connections/<CONN_NAME>`. MCP Services still live under a schema |

---

### 10. Security notes

- The OAuth client secret lives only in Unity Catalog. Do not put it in
  `app.yaml`, `.env`, or this repo.
- M2M means **every Genie One user shares the service principal identity**
  toward the MCP app. OntoBricks domain ACL still applies inside the app using
  that identity. For per-user graph ACL, switch the connection to OAuth U2M
  Per User (extra OAuth app + redirect
  `https://<databricks-region>.cloud.databricks.com/api/2.0/http/oauth/redirect`).
- Do not grant `USE CONNECTION` broadly if you rely on MCP Service tool
  selectors and policies.
