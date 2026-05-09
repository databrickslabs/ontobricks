# OntoBricks Documentation

OntoBricks is a **Knowledge Graph Builder** for the Databricks platform. It lets you design ontologies visually, map them to Unity Catalog tables, materialize a triple store, and explore the result as an interactive knowledge graph — all from a single Databricks App.

**New here?** Start with the [Get Started](get-started.md) guide, then browse the [Examples](examples.md) for end-to-end walkthroughs.

---

## Topic Index

| Topic | File | What you'll find |
|-------|------|------------------|
| **Get Started** | [get-started.md](get-started.md) | Install, first run, Databricks setup, environment variables |
| **User Guide** | [user-guide.md](user-guide.md) | Day-to-day usage — domain cockpit & versions (MCP-active vs loaded), ontology **Designer**, data mapping, triple-store pipeline, quality checks, reasoning, import (OWL, FIBO, CDISC, IOF) |
| **Examples** | [examples.md](examples.md) | Family-tree and customer-journey walkthroughs you can follow along |
| **Deployment** | [deployment.md](deployment.md) | Local dev, Databricks Apps, `app.yaml` resources, Unity Catalog grants for the app SP, in-app permissions, MCP deploy |
| **Architecture** | [architecture.md](architecture.md) | System design, semantic web standards, agents, OntoViz, triple-store backends, reasoning engine |
| **API** | [api.md](api.md) | External (stateless) REST & GraphQL, plus internal REST reference |
| **Data Access** | [data-access.md](data-access.md) | Engine map — which wrapper (REST / GraphQL / SPARQL / Spark SQL / Cypher) every UI / MCP / Chat feature actually uses |
| **MCP** | [mcp.md](mcp.md) | MCP server, Databricks Playground integration, client configuration |
| **Development** | [development.md](development.md) | Dependencies, test suite, permission / SDK notes |
| **Code Map** | [code_organization.md](code_organization.md) | UI routes & templates, API surfaces, agents, MCP wiring |
| **Product** | [product.md](product.md) | Value proposition, slide-ready material, competitive landscape |

---

## Assets

| Path | Purpose |
|------|---------|
| [images/](images/) | Architecture and standards diagrams (SVG) |
| [screenshots/](screenshots/) | UI screenshots |
| [../data/customer/README.md](../data/customer/README.md) | Sample dataset README |

## Generated API Docs (Sphinx)

The Sphinx HTML site is **not built or published automatically** — there is no
CI job and the build artifacts under `docs/sphinx/_build/` are gitignored.
Source files (`docs/sphinx/conf.py`, `docs/sphinx/api/*.rst`, etc.) are kept
in sync with the code, so anyone can build the site locally on demand.

- **Build locally:** `scripts/build_docs.sh` from the repo root (requires **Sphinx** and **myst-parser** — see `pyproject.toml` dev dependencies).
- **Open:** `scripts/build_docs.sh open` — builds and opens the result in your browser.
- **Clean:** `scripts/build_docs.sh clean` — removes `docs/sphinx/_build/`.
- **Output:** `docs/sphinx/_build/html/index.html` — the topic guides above are pulled into the same site via MyST `{include}`, keeping Markdown as the single source of truth.

## Quick Links

- [Main README](../README.md) — project overview
- [Swagger UI](http://localhost:8000/docs) — interactive API docs (when running locally)
