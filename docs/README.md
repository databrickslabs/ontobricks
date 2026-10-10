# OntoBricks Documentation

OntoBricks is a **Graph Viewer Builder** for the Databricks platform. Design
ontologies visually, map them to Unity Catalog tables, materialize a triple
store, and explore the result as an interactive graph — all from one Databricks
App.

Markdown in this folder is the **single source of truth**. The in-app Help
Center and the Sphinx site include the same files, grouped the same way.

**New here?** [Get Started](getting-started.md) → [Examples](examples.md) →
[User Guide](user-guide.md).

---

## Start here

| Guide | What you'll find |
|-------|------------------|
| [Get Started](getting-started.md) | Install, first run, Databricks setup, environment variables |
| [Examples](examples.md) | Family-tree and customer-journey walkthroughs |

## Using OntoBricks

| Guide | What you'll find |
|-------|------------------|
| [User Guide](user-guide.md) | Domain cockpit, Studio, mapping, triple-store pipeline, quality, reasoning, industry import. Ends with the [feature inventory](user-guide.md#feature-inventory) |
| [Advanced features](advanced_features.md) | [Ontology specifics](advanced_features.md#ontology-specifics) (`name` vs `label`, URI minting, `ontobricks:` vocabulary), [registry import and export](advanced_features.md#registry-import-and-export) (OBX, `registry_transfer.sh`), [cohort discovery](advanced_features.md#cohort-discovery) |
| [MCP](mcp.md) | MCP server, Playground, clients, [Unity Catalog + Genie One registration](mcp.md#register-the-mcp-in-unity-catalog-and-genie-one) |

## Platform

Each domain picks a graph engine (Lakebase, Lakehouse / Delta, Neo4j, or none).

| Guide | What you'll find |
|-------|------------------|
| [Graph backends](backend.md) | [Lakebase graph store](backend.md#lakebase-graph-store) (Postgres schema, write modes, bootstrap), [Neo4j backend](backend.md#neo4j-backend) (Aura / Community / Enterprise), [engine integration](backend.md#engine-integration) (backend contract, UC layout, [query optimizations](backend.md#graph-query-optimizations)) |
| [Architecture](architecture.md) | System design, standards, agents, OntoViz, [Lakehouse UC objects](architecture.md#lakehouse-unity-catalog-objects), [data access engine map](architecture.md#data-access-engine-map) |
| [API](api.md) | External REST & GraphQL, internal REST |

## Deploy & develop

| Guide | What you'll find |
|-------|------------------|
| [Deployment](deployment.md) | Local dev, Databricks Apps, grants, MCP deploy, plus the [deployment checklist](deployment.md#deployment-checklist), [Asset Bundle reference](deployment.md#asset-bundle-reference) and [sizing questionnaire](deployment.md#production-sizing-questionnaire) |
| [Development](development.md) | Dependencies, tests, permissions, [code map](development.md#code-map) |

## About

| Guide | What you'll find |
|-------|------------------|
| [Product](product.md) | Value proposition and slide-ready GTM material |

---

## Outside the Help Center

The Databricks App bundle excludes these (`databricks.yml` / `.databricksignore`):

- [sphinx/](sphinx/) — Sphinx build
- [diagrams/](diagrams/) — diagram sources
- [superpowers/](superpowers/) — agent planning scratch
- [pr47-neo4j-demo/](pr47-neo4j-demo/) — historical Neo4j demo notes

Reviewer process: [`.github/PR_REVIEW_CHECKLIST.md`](../.github/PR_REVIEW_CHECKLIST.md).
Project overview: root [README](../README.md).

## Assets

| Path | Purpose |
|------|---------|
| [images/](images/) | Architecture and standards diagrams (SVG) |
| [screenshots/](screenshots/) | UI screenshots |

## Sphinx site

- **Build:** `scripts/build_docs.sh` (Sphinx + myst-parser; see `pyproject.toml` extras).
- **Output:** `docs/sphinx/_build/html/index.html` — topic guides are MyST
  `{include}`s of the Markdown above.
- **Open:** root [`documentation.html`](../documentation.html) redirects to the build.

## Quick links

- [Main README](../README.md)
- [Release notes V0.9.0](../releases/ReleaseNotes_V0.9.0.md)
- [Swagger UI](http://localhost:8000/docs) (local run)
