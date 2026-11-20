# OntoBricks Documentation

OntoBricks is a **Graph Viewer Builder** for the Databricks platform. Design
ontologies visually, map them to [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) tables, materialize a triple
store, and explore the result as an interactive graph — all from one Databricks
App.

Markdown in this folder is the **single source of truth**. The in-app Help
Center and the Sphinx site include the same files, grouped the same way.

**New here?** [Get Started](getting-started.md) → [Examples](examples.md) →
[User Guide](user-guide.md).

## What's new in 0.10.0

Operator guides for this version.

| Feature | Where it is documented |
|---------|------------------------|
| What-if simulation sandboxes: scenarios, hypothetical facts, Compare, run history | [Intelligence](intelligence.md#simulation--scenarios), [Intelligence](intelligence.md#simulation--compare), [Architecture](architecture.md#simulation--what-if-sandboxes) |
| Simulation REST API and simulation graph source on queries | [API](api.md#simulation-api) |
| Graph Analytics (centrality job, Dashboard, Data Model Health, Interpret, Runs) | [Intelligence](intelligence.md#discovery--analytics) |
| Patterns and communities at scale (Coverage KPI, hub pruning, Home tiles) | [Intelligence](intelligence.md#discovery--patterns), [Intelligence](intelligence.md#discovery--communities), [Intelligence](intelligence.md#home) |
| Graph Chat and entity search in the viewer | [User Guide](user-guide.md#explorer-sidebar) |
| Live activity log on Data Quality checks | [User Guide](user-guide.md#data-quality-sidebar) |
| Auto-Map emits [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/) metric-view SQL | [User Guide](user-guide.md#auto-map-modal) |
| W3C JSON-LD for OWL import/export, SPARQL CONSTRUCT/DESCRIBE, MCP | [Architecture](architecture.md#2b-json-ld-json-for-linked-data), [API](api.md), [MCP](mcp.md) |
| Typed SPARQL value FILTERs compiled to SQL | [User Guide](user-guide.md#sparql-playground) |
| Graph DB cloisonnement (capability interfaces per engine family) | [Architecture](architecture.md#backend-capability-and-object-lifecycle), [Graph backends](backend.md#engine-integration) |
| MCP end-user identity forwarding, graph tools without sticky session | [MCP](mcp.md) |

The in-app Help Center serves these same files. [ontobricks.org](https://ontobricks.org/)
mirrors the last published `master` snapshot and stays on the previous
release until this documentation is published.

---

## Start here

| Guide | What you'll find |
|-------|------------------|
| [Get Started](getting-started.md) | Install, first run, Databricks setup, environment variables, [First Steps including Intelligence](getting-started.md#7-discover-and-simulate-intelligence) |
| [Examples](examples.md) | Family-tree and customer-journey walkthroughs |

## Using OntoBricks

| Guide | What you'll find |
|-------|------------------|
| [User Guide](user-guide.md) | Context General (cockpit), Configuration, Studio, Documents, staged Generate, mapping, Explorer quality/rules/jobs, SPARQL, GitHub mirror. Ends with the [feature inventory](user-guide.md#feature-inventory). Intelligence is summarised as [Step 4](user-guide.md#step-4-intelligence-cohorts-discovery--simulation) |
| [Intelligence](intelligence.md) | Home, Cohorts, Analytics, Communities, Patterns, Scenarios, Compare / Evaluate, run history, energy walkthrough |
| [Advanced features](advanced_features.md) | [Ontology specifics](advanced_features.md#ontology-specifics) (`name` vs `label`, URI minting, `ontobricks:` vocabulary), [registry import and export](advanced_features.md#registry-import-and-export) (OBX, GitHub mirror), [cohort discovery](advanced_features.md#cohort-discovery) |
| [MCP](mcp.md) | MCP server, Playground, clients, [Unity Catalog + Genie One registration](mcp.md#register-the-mcp-in-unity-catalog-and-genie-one) |

## Platform

Each domain picks a graph engine ([Lakebase](https://docs.databricks.com/aws/en/oltp/), Lakehouse / Delta, Neo4j, or none).

| Guide | What you'll find |
|-------|------------------|
| [Graph backends](backend.md) | [Lakebase graph store](backend.md#lakebase-graph-store) (Postgres schema, write modes, bootstrap), [Neo4j backend](backend.md#neo4j-backend) (Aura / Community / Enterprise), [engine integration](backend.md#engine-integration) (backend contract, UC layout, [query optimizations](backend.md#graph-query-optimizations)) |
| [Architecture](architecture.md) | System design, standards ([SHACL data quality](architecture.md#shacl-data-quality), RDF, OWL, R2RML, SPARQL, JSON-LD), agents, OntoViz, [Lakehouse UC objects](architecture.md#lakehouse-unity-catalog-objects), [data access engine map](architecture.md#data-access-engine-map) |
| [API](api.md) | External REST & GraphQL, internal REST |

## Deploy & develop

| Guide | What you'll find |
|-------|------------------|
| [Deployment](deployment.md) | Local dev, [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/), grants, MCP deploy, plus the [deployment checklist](deployment.md#deployment-checklist), [Asset Bundle reference](deployment.md#asset-bundle-reference) and [sizing questionnaire](deployment.md#production-sizing-questionnaire) |
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
- [Release notes V0.9.0](../releases/ReleaseNotes_V0.9.0.md)
- [Swagger UI](http://localhost:8000/docs) (local run)
