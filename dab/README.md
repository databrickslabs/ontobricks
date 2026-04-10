# OntoBricks — Databricks Asset Bundle

The bundle configuration lives at the project root (`databricks.yml`) as required by the Databricks CLI. This directory contains supplementary documentation.

For the full deployment guide, see **[docs/deployment.md](../docs/deployment.md)**.

See also: [Databricks Asset Bundles docs](https://docs.databricks.com/dev-tools/bundles/)

## What Gets Deployed

| App | Bundle Key | Name | Description |
|-----|------------|------|-------------|
| **OntoBricks** | `ontobricks_app` | `ontobricks` | Main FastAPI application — ontology editor, mapping, Digital Twin builder |
| **MCP Server** | `mcp_ontobricks_app` | `mcp-ontobricks` | Model Context Protocol companion — exposes knowledge-graph tools to the Databricks Playground |

## Quick Start

```bash
# From the project root:

# 1. Validate
databricks bundle validate

# 2. Deploy both apps (dev target)
databricks bundle deploy

# 3. Start
databricks bundle run ontobricks_app
databricks bundle run mcp_ontobricks_app

# Or use the convenience script:
./deploy.sh --all
```

## Convenience Script (`deploy.sh`)

```bash
./deploy.sh                  # validate + deploy + run main app (dev)
./deploy.sh --all            # validate + deploy + run both apps
./deploy.sh --mcp-only       # validate + deploy + run MCP only
./deploy.sh -t prod          # deploy to production target
./deploy.sh --no-run         # deploy without starting apps
./deploy.sh --bind           # also bind existing apps post-deploy
```

## Targets

| Target | Mode | Description |
|--------|------|-------------|
| `dev` | development | Default. Uses the authenticated user's workspace path. |
| `prod` | production | Explicit root path, restricted permissions. |

## Variables

Override defaults with `--var` flags or in a target-specific `variables:` block:

| Variable | Default | Description |
|----------|---------|-------------|
| `warehouse_id` | `66e8366e84d57752` | SQL Warehouse ID |
| `registry_catalog` | `benoit_cayla` | Catalog for the project registry |
| `registry_schema` | `ontobricks` | Schema for the project registry |
| `registry_volume` | `OntoBricksRegistry` | Volume name for the project registry |

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

The `.databricksignore` at the project root excludes non-runtime files (tests, docs, data, IDE config, the MCP server source) from the main app sync. The MCP server has its own `source_code_path` pointing directly to `src/mcp-server/`.

## Key Files

| File | Purpose |
|------|---------|
| `databricks.yml` | Bundle definition — apps, permissions, targets |
| `app.yaml` | Main app runtime config (command, env vars, resources) |
| `src/mcp-server/app.yaml` | MCP server runtime config |
| `.databricksignore` | Excludes non-runtime files from the bundle sync |
| `deploy.sh` | Convenience wrapper around DAB commands |
