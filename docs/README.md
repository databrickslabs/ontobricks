# OntoBricks Documentation

OntoBricks is a **Graph Viewer Builder** for the Databricks platform. It lets you design ontologies visually, map them to Unity Catalog tables, materialize a triple store, and explore the result as an interactive graph viewer — all from a single Databricks App.

**New here?** Start with the [Get Started](get-started.md) guide, then browse the [Examples](examples.md) for end-to-end walkthroughs.

---

## Topic Index

| Topic | File | What you'll find |
|-------|------|------------------|
| **Get Started** | [get-started.md](get-started.md) | Install, first run, Databricks setup, environment variables |
| **User Guide** | [user-guide.md](user-guide.md) | Day-to-day usage — domain cockpit & versions (MCP-active vs loaded), ontology **Designer**, data mapping, triple-store pipeline, quality checks, reasoning, import (OWL, FIBO, CDISC, IOF, FHIR), Pitfalls Detector, Registry OBX export/import |
| **Examples** | [examples.md](examples.md) | Family-tree and customer-journey walkthroughs you can follow along |
| **Deployment** | [deployment.md](deployment.md) | Local dev, Databricks Apps, `app.yaml` resources, Unity Catalog grants for the app SP, in-app permissions, MCP deploy |
| **Architecture** | [architecture.md](architecture.md) | System design, semantic web standards, agents, OntoViz, triple-store backends, reasoning engine |
| **API** | [api.md](api.md) | External (stateless) REST & GraphQL, plus internal REST reference |
| **Data Access** | [data-access.md](data-access.md) | Engine map — which wrapper (REST / GraphQL / SPARQL / Spark SQL / Cypher) every UI / MCP / Chat feature actually uses |
| **MCP** | [mcp.md](mcp.md) | MCP server, Databricks Playground integration, client configuration |
| **Development** | [development.md](development.md) | Dependencies, test suite, permission / SDK notes |
| **Code Map** | [code_organization.md](code_organization.md) | UI routes & templates, API surfaces, agents, MCP wiring |
| **Lakebase GraphDB** | [lakebase-graphdb.md](lakebase-graphdb.md) | Lakebase setup, write modes, Postgres schema layout, permissions bootstrap, troubleshooting |
| **GraphDB Integration** | [graphdb-integration.md](graphdb-integration.md) | Deep-dive: UC schema layout, resolver design, synced-table pipeline steps |
| **Cohort Discovery** | [cohort_discovery.md](cohort_discovery.md) | Cohort rule builder, path traversal, predicate namespace handling |
| **Import / Export** | [import-export.md](import-export.md) | Registry OBX UI (browser) + CLI `registry_transfer.sh` (CI/CD) |
| **Product** | [product.md](product.md) | Value proposition, slide-ready material, competitive landscape |

---

## Assets

| Path | Purpose |
|------|---------|
| [images/](images/) | Architecture and standards diagrams (SVG) |
| [screenshots/](screenshots/) | UI screenshots |
| [../data/customer/README.md](../data/customer/README.md) | Sample dataset README |

## Generated API Docs (Sphinx)

- **Build:** `scripts/build_docs.sh` from the repo root (requires **Sphinx** and **myst-parser** — see `pyproject.toml` dev dependencies).
- **Output:** `docs/sphinx/_build/html/index.html` — the topic guides above are pulled into the same site via MyST `{include}`, keeping Markdown as the single source of truth.
- **Quick open:** root [`documentation.html`](../documentation.html) redirects to the Sphinx build.

## Quick Links

- [Main README](../README.md) — project overview
- [Swagger UI](http://localhost:8000/docs) — interactive API docs (when running locally)
