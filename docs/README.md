# OntoBricks Documentation

OntoBricks is a **Graph Viewer Builder** for the Databricks platform. Design
ontologies visually, map them to Unity Catalog tables, materialize a triple
store, and explore the result as an interactive graph — all from one Databricks
App.

Markdown in this folder is the **single source of truth**. The in-app Help
Center and the Sphinx site include the same files, grouped the same way.

**New here?** [Get Started](getting-started.md) → [Examples](examples.md) →
[User Guide](user-guide.md).

## What's new in 0.9.1

Patch on 0.9.0. Full details:
[Release notes V0.9.1](../releases/ReleaseNotes_V0.9.1.md).

| Feature | Where it is documented |
|---------|------------------------|
| Viewer Explorer search and Graph Chat (read-only POSTs) | [Development](development.md#enforcement-rules) |
| SWRL `swrlb:` builtins on inference and materialization | [User Guide](user-guide.md#feature-inventory) |
| Auto-Map Metric View SQL (`MEASURE` + `GROUP BY`) | [User Guide](user-guide.md#auto-map-sidebar), [Architecture](architecture.md) |
| Bulk / entity Data Quality snapshot and live activity log | [User Guide](user-guide.md#data-quality-sidebar), [API](api.md) |
| Lakebase quality SQL on the Build warehouse | [Graph backends](backend.md) |

## What's new in 0.9.0

Operator guides for this version. Full details:
[Release notes V0.9.0](../releases/ReleaseNotes_V0.9.0.md).

| Feature | Where it is documented |
|---------|------------------------|
| Lakehouse//RT query warehouse, SEA, CloudFetch | [Get Started](getting-started.md#sql-warehouses-and-explorer-timing), [User Guide](user-guide.md#lakehouse-sql-warehouses-and-cloudfetch), [Deployment](deployment.md) |
| Three-stage Ontology Generate (detect → review → complete) | [User Guide](user-guide.md#option-c-ai-powered-wizard) |
| Knowledge Store (parsed text in Lakebase, no Volume) | [User Guide](user-guide.md#knowledge-store), [Get Started](getting-started.md#upload-documents-to-the-knowledge-store-optional) |
| Ontology Studio (renamed Designer), entity panel tabs | [User Guide](user-guide.md#ontology-studio-map-full-graph) |
| Entity Data Quality Checks, SWRL on an entity, Jobs as class actions | [User Guide](user-guide.md#feature-inventory), [MCP](mcp.md) |
| SPARQL playground | [User Guide](user-guide.md#sparql-playground) |
| GitHub domain mirror | [User Guide](user-guide.md#github-mirror), [Advanced features](advanced_features.md#load-from-github-repository) |
| Load opens Domain → General | [User Guide](user-guide.md#domain-saveload) |
| No LLM, AI Gateway picker, OBO / MCP Team gate | [User Guide](user-guide.md#no-llm-mode), [Deployment](deployment.md) |
| App `/health` vs readiness, CSRF | [API](api.md#csrf-protection), [Deployment](deployment.md) |

The in-app Help Center serves these same files. [ontobricks.org](https://ontobricks.org/)
mirrors the last published `master` snapshot and stays on the previous
release until this documentation is published.

---

## Start here

| Guide | What you'll find |
|-------|------------------|
| [Get Started](getting-started.md) | Install, first run, Databricks setup, environment variables |
| [Examples](examples.md) | Family-tree and customer-journey walkthroughs |

## Using OntoBricks

| Guide | What you'll find |
|-------|------------------|
| [User Guide](user-guide.md) | Domain cockpit, Studio, Knowledge Store, staged Generate, mapping, Explorer quality/rules/jobs, SPARQL, GitHub mirror. Ends with the [feature inventory](user-guide.md#feature-inventory) |
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

- `sphinx/` — Sphinx build (local)
- `diagrams/` — diagram sources (local)
- `superpowers/` — agent planning scratch (gitignored, not published)
- `pr47-neo4j-demo/` — historical Neo4j demo notes (local)

Reviewer process: [`.github/PR_REVIEW_CHECKLIST.md`](../.github/PR_REVIEW_CHECKLIST.md).
Project overview: root [README](../README.md).

## Assets

| Path | Purpose |
|------|---------|
| [images/](images/) | Architecture and standards diagrams (SVG) |
| [screenshots/](screenshots/) | UI screenshots |

## Sphinx site

- **Build:** `ci/build_docs.sh` from the repository root (Sphinx + myst-parser;
  see `pyproject.toml` extras).
- **Output:** `docs/sphinx/_build/html/index.html` — topic guides are MyST
  `{include}`s of the Markdown above.

## Quick links

- [Main README](../README.md)
- [Release notes V0.9.1](../releases/ReleaseNotes_V0.9.1.md)
- [Release notes V0.9.0](../releases/ReleaseNotes_V0.9.0.md)
- [Swagger UI](http://localhost:8000/docs) (local run)
