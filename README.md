<p align="center">
  <img src="src/front/static/global/img/ontobricks-icon.svg" alt="OntoBricks Logo" width="120" height="120">
</p>

<h1 align="center">OntoBricks 0.9.0</h1>

<p align="center">
  <strong>Turn your Databricks tables into a living knowledge graph — in four clicks.</strong>
</p>

<p align="center">
  <a href="https://ontobricks.org/">Website</a>
  ·
  <a href="https://ontobricks.org/">Demos &amp; Screenshots</a>
  ·
  <a href="https://github.com/databrickslabs/ontobricks">GitHub</a>
  ·
  <a href="docs/README.md">Documentation</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/runs%20on-Databricks%20Apps-FF3621.svg" alt="Databricks Apps">
  <img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python">
  <img src="https://img.shields.io/badge/standards-OWL%20·%20R2RML%20·%20SPARQL-6E4AFF.svg" alt="Standards">
  <img src="https://img.shields.io/badge/AI-MCP%20ready-10B981.svg" alt="MCP ready">
</p>

---

Every enterprise has the data. Almost none have the **meaning**. Your Lakehouse
stores billions of rows, but nothing captures what they *represent* — that a
*Customer* holds *Contracts*, which contain *Invoices*, governed by rules only
your senior engineers remember.

**OntoBricks closes that gap.** It is a Databricks-native app that turns Unity
Catalog tables into an explorable, queryable **knowledge graph** — design the
meaning, let AI map it to your data, and navigate the result visually or expose
it to your favorite LLM. No separate graph platform, no RDF expertise, no
months-long integration project.

## Why OntoBricks

- **From schema to graph in minutes, not months.** LLM-powered automation drives
  the whole pipeline — import metadata, generate the ontology, map the data,
  materialize the graph.
- **Lives inside Databricks.** Deploys as a Databricks App and reuses what you
  already have: Unity Catalog, SQL Warehouse, Model Serving, Lakebase. Your
  triples stay in your Lakehouse.
- **Standards under the hood, simplicity on top.** OWL, R2RML and SPARQL power
  the engine; everything executes as Spark SQL. Your users never touch RDF.
- **Built for the AI era.** Publish any domain to LLM agents over the Model
  Context Protocol (MCP) and let them reason over governed, explainable
  business semantics.
- **Governed by design.** Versioned domains, DRAFT → IN-REVIEW → PUBLISHED
  lifecycle, single-editor locking, and an append-only review audit trail.

## From tables to a knowledge graph in four clicks

| Step | Action | What happens | Powered by |
|------|--------|--------------|------------|
| **1** | **Import Metadata** — Domain &gt; Metadata | Fetches table &amp; column metadata from Unity Catalog | Unity Catalog |
| **2** | **Generate Ontology** — Ontology &gt; Wizard | The LLM designs entities, relationships &amp; attributes from your metadata | Model Serving |
| **3** | **Auto-Map** — Mapping &gt; Auto-Map | The LLM generates SQL mappings for every entity and relationship | Model Serving |
| **4** | **Synchronize** — Knowledge Graph &gt; Status | Executes the mappings and populates the triple store | SQL Warehouse |

Prefer full control? Every step is also available as a guided **manual
workflow** — design, map, build, query, and reason at column-level precision.
See the [User Guide](docs/user-guide.md).

## What's inside

### Design ontologies visually or with AI
Drag-and-drop entities, relationships and inheritance on the **OntoViz** canvas,
or import industry standards in one click (FIBO, CDISC, IOF, HL7 FHIR
R4/R4B/R5) and your own OWL/RDFS. A floating **AI Assistant** edits your
ontology through natural language. Catch design issues early with the built-in
**Ontology Pitfalls Detector** (19 structural, logical and semantic checks).

### Map to your data — automatically
Let the LLM generate SQL and column mappings for every entity and relationship,
then refine with live data preview. Mappings compile to W3C-compliant **R2RML**
you never have to write. Schema-drift warnings flag upstream renames and drops
before they break your graph.

### Build, reason and explore
Materialize triples incrementally, then **reason** over the graph with OWL 2 RL
inference, SWRL rules and SHACL validation. Explore interactively: two-phase
search, N-hop neighbour expansion, cross-domain bridge navigation, community
detection (Louvain, Label Propagation, Greedy Modularity), and explainable
**cohort discovery**. Query everything through an auto-generated **GraphQL**
API. Details in the [User Guide](docs/user-guide.md) and
[Cohort Discovery](docs/cohort_discovery.md).

### Publish to AI agents (MCP)
Expose your knowledge graph to Cursor, Claude Desktop or the Databricks
Playground over the [Model Context Protocol](https://modelcontextprotocol.io/).
Each domain decides exactly **what it publishes** — which tools, which datasets,
which class actions and virtual attributes — right down to ontology-only domains
that expose a single `describe_ontology` tool. See
[MCP Integration](docs/mcp.md).

### Govern the whole lifecycle
Every domain version carries a **DRAFT / IN-REVIEW / PUBLISHED** status enforced
server-side. A **single editor** holds a DRAFT at a time (auto-releasing leases,
admin take-over), a business-friendly **review workflow** collects sign-offs
with a configurable quorum, and every decision is persisted append-only. Move
domains between environments with the **OBX** export/import (UI or
[CLI](docs/import-export.md)).

### Pluggable graph engine
Pick a backend **per domain**: **Lakebase (Postgres)** by default,
**Lakehouse** (governed Delta triple tables, zero extra infra), **Neo4j**, or
**No Backend** for ontology-only domains that publish their ontology with no
graph built (Mapping and Knowledge Graph are disabled and the MCP surface
serves only ontology information).
Connection config stays workspace-global. Full reference:
[Lakebase Graph DB](docs/lakebase-graphdb.md) ·
[Graph DB Integration](docs/graphdb-integration.md).

## Runs entirely on your Databricks

OntoBricks is not a separate platform. It deploys as a **Databricks App** and
uses the services you already run — Unity Catalog for storage and metadata, a
SQL Warehouse for execution, Model Serving for LLM automation, and Lakebase for
the registry and graph store. Your data and your triples never leave your
workspace. Architecture deep-dive: [architecture.md](docs/architecture.md).

## Get started

**Try it locally**

```bash
git clone <repository-url> && cd OntoBricks
uv sync --frozen --extra lakebase  # install the committed public-PyPI lock
cp .env.example .env        # set your Databricks host, token, warehouse ID
scripts/start.sh            # open http://localhost:8000
```

Always use `--frozen` for routine `uv sync` / `uv run` commands. The lock
records package indexes as `https://pypi.org/simple` and immutable artifact
downloads on `https://files.pythonhosted.org`; do not commit internal
`pypi-proxy.dev.databricks.com` URLs.

**Deploy to Databricks Apps**

```bash
databricks auth login --host https://<workspace>
# edit scripts/deploy.config.sh (profile, warehouse, registry, Lakebase), then:
make deploy
```

Prerequisites (Databricks workspace with Apps enabled, a SQL Warehouse, a
**Lakebase Autoscaling** database, and a Unity Catalog Volume) and the full,
idempotent deploy checklist — including one-click graph-DB provisioning and
permission bootstrap — are in the
**[Get Started](docs/get-started.md)** and
**[Deployment Guide](docs/deployment.md)**.

The bundle syncs only runtime source, the MCP and graph-job files, dependency
locks, and the in-app Help Center set. `databricks.yml` `sync.exclude` is
authoritative; `.databricksignore` is a reviewer/test mirror and is not read
by the Databricks CLI.

## Documentation

- **[Documentation hub](docs/README.md)** — start here
- **[Value proposition](docs/product.md)** — the business case &amp; go-to-market
- **[User Guide](docs/user-guide.md)** · **[Features](docs/features.md)** · **[Architecture](docs/architecture.md)** · **[API](docs/api.md)**
- **[MCP](docs/mcp.md)** · **[Graph DB](docs/lakebase-graphdb.md)** · **[Import / Export](docs/import-export.md)**
- **[Developing OntoBricks](docs/development.md)** — build, test, contribute

Product overview, screenshots and videos:
[ontobricks.org](https://ontobricks.org/).

## Project Support

Please note that all projects in the /databrickslabs github account are provided
for your exploration only, and are not formally supported by Databricks with
Service Level Agreements (SLAs). They are provided AS-IS and we do not make any
guarantees of any kind. Please do not submit a support ticket relating to any
issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub
Issues on the Repo. They will be reviewed as time permits, but there are no
formal SLAs for support.
