# OntoBricks — Databricks Automation Bundle

The bundle configuration lives at the project root (`databricks.yml`) as required by the Databricks CLI. This directory contains supplementary documentation.

For the full deployment guide, see **[docs/deployment.md](../docs/deployment.md)**.

See also: [Databricks Asset Bundles docs](https://docs.databricks.com/dev-tools/bundles/)

## What Gets Deployed

| App | Bundle Key | Name | Description |
|-----|------------|------|-------------|
| **OntoBricks** | `ontobricks_app` | `ontobricks` | Main FastAPI application — ontology editor, mapping, Knowledge Graph builder |
| **MCP Server** | `mcp_ontobricks_app` | `mcp-ontobricks` | Model Context Protocol companion — exposes knowledge-graph tools to the Databricks Playground |

## Quick Start

```bash
# From the project root:

# 1. Validate
databricks bundle validate

# 2. Deploy both apps (configured Lakebase target)
make deploy

# Or deploy/run explicitly:
databricks bundle deploy -t <DAB_TARGET>
databricks bundle run ontobricks_dev_app -t <DAB_TARGET>
databricks bundle run mcp_ontobricks_app -t <DAB_TARGET>

# DAB_TARGET normally comes from scripts/deploy.config.sh.
```

## Convenience Script (`scripts/deploy.sh`)

```bash
scripts/deploy.sh                  # validate + deploy + run both apps
scripts/deploy.sh -t dev           # explicit Volume-only target
scripts/deploy.sh -t dev-lakebase  # legacy unsuffixed Lakebase target
scripts/deploy.sh --no-run         # deploy without starting either app
scripts/deploy.sh --bind           # bind existing apps during deployment
scripts/deploy.sh --dry-run        # preflight and validate without changes
```

## Targets

| Target | Mode | Description |
|--------|------|-------------|
| `dev` | development | Volume-only registry backend. |
| `dev-lakebase` | development | Legacy unsuffixed Lakebase target. |
| `dev-lakebase-<INSTANCE_ID>` | development | Generated per-instance Lakebase target used by `make deploy`. |

## Variables

Override defaults with `--var` flags or in a target-specific `variables:` block:

| Variable | Description |
|----------|-------------|
| `app_name` / `mcp_app_name` | Main and MCP Databricks App names |
| `warehouse_id` | SQL Warehouse bound to both apps |
| `registry_catalog` / `registry_schema` / `registry_volume` | Unity Catalog registry Volume |
| `lakebase_project` / `lakebase_branch` / `lakebase_database_resource_segment` | Lakebase Autoscaling binding |
| `lakebase_registry_schema` | Postgres schema used by the registry |

```bash
databricks bundle deploy --var warehouse_id=abc123def456
```

## Binding Existing Apps

If the apps already exist from a previous manual deployment:

```bash
databricks bundle deployment bind ontobricks_app ontobricks
databricks bundle deployment bind mcp_ontobricks_app mcp-ontobricks
databricks bundle deploy
```

## Post-Deploy Steps (First Time Only)

1. **Bind resources** — In the Databricks Apps UI, bind `sql-warehouse` and `volume` for both apps
2. **Initialize registry** — Open the app > Settings > Registry > Initialize
3. **Set MCP URL** — Update `ONTOBRICKS_URL` in `src/mcp-server/app.yaml` with the main app URL

Resource bindings persist across redeployments.

## File Sync

DAB sync = git-tracked files − `.gitignore` + `sync.include` − `sync.exclude`
in `databricks.yml`. `.databricksignore` is a human/test mirror only.

**Shipped:** `src/` (including MCP + `src/jobs/`), `run.py`, `app.yaml`,
lockfiles, `LICENSE.txt` / `NOTICE.txt`, and the Help Center set
(`docs/*.md` catalogued in `help.py`, plus `docs/images/` and
`docs/screenshots/`).

**Not shipped:** tests, CI, scripts, changelogs, `.github/`, `.planning/`,
third-party `licenses/`, Sphinx/demo docs, DAB YAML under `resources/`.

Do not exclude `README.md` or `/README.md`: CLI 0.298 applies both recursively
and drops Help Center Overview (`docs/README.md`). Both README files ship.

Do not exclude `src/mcp-server/`; both apps share one files tree. Keep both
`app.yaml.template` files too: this CLI matches an anchored root-template
exclude recursively, and the deploy guard requires the MCP template.

## Key Files

| File | Purpose |
|------|---------|
| `databricks.yml` | Bundle definition — apps, permissions, targets, **sync.exclude** |
| `app.yaml` | Main app runtime config (command, env vars, resources) |
| `src/mcp-server/app.yaml` | MCP server runtime config |
| `.databricksignore` | Mirror of `sync.exclude` (CLI does not read it) |
| `scripts/deploy.sh` | Convenience wrapper around DAB commands |
