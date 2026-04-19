# OntoBricks Deployment Guide

## Overview

This guide covers deploying OntoBricks both locally for development and to Databricks Apps for production use, including the optional MCP server for Databricks Playground integration and instructions for deploying to a new workspace.

Deployment uses **Databricks Asset Bundles (DAB)** — a declarative, repeatable approach that deploys both apps in a single command from `databricks.yml` at the project root.

**Architecture at a glance:**

```
┌──────────────────────┐      ┌──────────────────────┐
│   ontobricks         │      │   mcp-ontobricks     │
│   (Databricks App)   │◄─────│   (Databricks App)   │
│                      │ REST │                      │
│   Web UI + REST API  │ API  │   MCP Server for     │
│   Knowledge Graph    │      │   Databricks         │
│                      │      │   Playground          │
└──────────┬───────────┘      └──────────────────────┘
           │
           ├──────────────────┐
           ▼                  ▼
   ┌───────────────────────────────────┐
   │  SQL Warehouse (Delta backend)    │
   │  + LadybugDB (embedded graph)     │
   └───────────────────────────────────┘
           │
           ▼
   ┌───────────────┐
   │  MLflow        │
   │  Tracking      │
   │  (Experiments) │
   └───────────────┘
```

**Key files:**

| File | Purpose |
|------|---------|
| `databricks.yml` | DAB bundle definition — apps, permissions, targets |
| `app.yaml` | Main app runtime config — command, env vars, resource declarations |
| `src/mcp-server/app.yaml` | MCP server runtime config |
| `.databricksignore` | Excludes non-runtime files from the bundle sync |
| `scripts/deploy.sh` | Convenience wrapper around DAB commands |

---

## 1. Local Development Setup

### Prerequisites

- Python 3.10 or higher
- `uv` package manager (recommended) or `pip`
- Git
- Access to a Databricks workspace
- Databricks CLI installed and authenticated

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd OntoBricks

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate
uv sync
```

### Environment Variables

Create a `.env` file from the example:

```bash
cp .env.example .env
```

Edit `.env` with your values:

```bash
# Required
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_SQL_WAREHOUSE_ID=your-warehouse-id

# Optional
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default

# MLflow — persist agent traces to your workspace (recommended)
MLFLOW_TRACKING_URI=databricks
```

### Run Locally

```bash
scripts/start.sh
# Or directly: uv run python run.py
# Open http://localhost:8000
```

### Running Tests

```bash
uv run pytest                                      # all tests
uv run pytest --cov=back --cov=front --cov=shared --cov=api --cov=agents --cov-report=html   # with coverage
uv run pytest tests/e2e/ -v                        # end-to-end tests
```

---

## 2. Databricks Apps Deployment (DAB)

Deployment uses **Databricks Asset Bundles** to deploy both the main app and the MCP server declaratively from a single `databricks.yml` at the project root.

### Prerequisites

| Requirement | Details |
|---|---|
| Databricks CLI | `>= 0.250.0` — check with `databricks -v` |
| Authenticated CLI | `databricks auth login --host https://<workspace>` |
| SQL Warehouse | A running SQL Warehouse in the workspace |
| Apps feature | Databricks Apps must be enabled on the workspace |
| Unity Catalog | A catalog, schema, and volume for the project registry |

### Step 1 — Authenticate

```bash
databricks auth login --host https://<workspace>.cloud.databricks.com

# Verify
databricks current-user me
```

### Step 2 — Customize `app.yaml` (workspace-specific values)

Edit the workspace-specific values in `app.yaml` (main app) and `src/mcp-server/app.yaml` (MCP server):

| Variable | File | Description | How to find it |
|----------|------|-------------|----------------|
| `DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT` | `app.yaml` | Fallback SQL Warehouse ID | **SQL Warehouses** > select warehouse > **Connection details** |
| `DATABRICKS_TRIPLESTORE_TABLE` | `app.yaml` | Default triple store table | Choose or create a `catalog.schema.table` for triple storage |
| `ONTOBRICKS_APP_NAME` | `app.yaml` | Deployed app name (for permission checks) | Must match the name in `databricks.yml` (default: `ontobricks`) |
| `ONTOBRICKS_URL` | `src/mcp-server/app.yaml` | Main app URL | Set after first deploy — `databricks apps get ontobricks` |

The `REGISTRY_CATALOG`, `REGISTRY_SCHEMA`, and `REGISTRY_VOLUME` static variables are **only needed for local development**. In a deployed app the `volume` resource binding injects the registry path automatically.

### Step 3 — Customize `databricks.yml` (bundle variables)

Update the default variable values to match your workspace:

```yaml
variables:
  warehouse_id:
    default: "<your-warehouse-id>"
  registry_catalog:
    default: "<your-catalog>"
  registry_schema:
    default: "<your-schema>"
  registry_volume:
    default: "OntoBricksRegistry"
```

Update the `permissions` section to grant `CAN_MANAGE` to the deploying user:

```yaml
permissions:
  - level: CAN_MANAGE
    user_name: <your-email>
  - level: CAN_USE
    group_name: users
```

### Step 4 — Validate the bundle

```bash
databricks bundle validate
```

This checks the bundle configuration without deploying. Fix any errors before proceeding.

### Step 5 — Deploy

```bash
# Deploy both apps (dev target)
databricks bundle deploy

# Or use the convenience script
scripts/deploy.sh
```

If the apps already exist in the workspace (e.g., from a previous manual deploy), bind them first:

```bash
databricks bundle deployment bind ontobricks_app ontobricks
databricks bundle deployment bind mcp_ontobricks_app mcp-ontobricks
databricks bundle deploy
```

### Step 6 — Start the apps

```bash
# Start the main app
databricks bundle run ontobricks_app

# Start the MCP server
databricks bundle run mcp_ontobricks_app
```

Or use the convenience script:

```bash
scripts/deploy.sh              # deploy + start main app
scripts/deploy.sh --all        # deploy + start both apps
scripts/deploy.sh --mcp-only   # deploy + start MCP only
```

### Step 7 — Bind resources (first deploy only)

After the first deployment, bind the app resources in the Databricks workspace UI:

1. Go to **Compute > Apps** and find `ontobricks`
2. Click **Resources**
3. Bind `sql-warehouse` to a running SQL Warehouse
4. Bind `volume` to the Unity Catalog Volume for the project registry (e.g., `your_catalog.your_schema.OntoBricksRegistry`)
5. Repeat for `mcp-ontobricks` (same warehouse and volume)
6. Verify both apps show status **Running**

> **Note:** Resource bindings persist across redeployments — you only need to do this once per workspace. Once the `sql-warehouse` and `volume` resources are bound, the corresponding controls in the Settings page are **locked**. To change them, update the resource bindings in the Apps UI and restart the app.

### Step 8 — Initialize the registry (first deploy only)

If the volume is empty (first deployment):

1. Open the app URL
2. Go to **Settings > Registry**
3. Click **Initialize** to bootstrap the registry

### Step 9 — Verify

```bash
# Check app status
databricks apps get ontobricks
databricks apps get mcp-ontobricks

# Or via bundle
databricks bundle summary
```

### `scripts/deploy.sh` Reference

The convenience script wraps all DAB commands:

```bash
scripts/deploy.sh                  # validate + deploy + run main app (dev)
scripts/deploy.sh --all            # validate + deploy + run both apps
scripts/deploy.sh --mcp-only       # validate + deploy + run MCP only
scripts/deploy.sh -t prod          # deploy to production target
scripts/deploy.sh --no-run         # deploy without starting apps
scripts/deploy.sh --bind           # also bind existing apps post-deploy
```

### `app.yaml` Configuration — Full Reference

The `app.yaml` file controls the Databricks App runtime. Here is every variable explained:

```yaml
# Command to start the app — uv resolves dependencies from pyproject.toml
command:
  - "uv"
  - "run"
  - "python"
  - "run.py"

env:
  # ── SQL Warehouse ──────────────────────────────────────────────
  # Injected from the sql-warehouse resource binding (configured in the Apps UI)
  - name: DATABRICKS_SQL_WAREHOUSE_ID
    valueFrom: sql-warehouse

  # Static fallback warehouse ID for MCP / session-less API calls
  # (when no resource binding is available)
  - name: DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT
    value: "<your-warehouse-id>"

  # ── Unity Catalog defaults ─────────────────────────────────────
  - name: DATABRICKS_CATALOG
    value: "main"
  - name: DATABRICKS_SCHEMA
    value: "default"

  # ── Triple store fallback ──────────────────────────────────────
  # Fully-qualified Delta triple store table used when no project session
  # is active (e.g. MCP API calls). Format: catalog.schema.table
  - name: DATABRICKS_TRIPLESTORE_TABLE
    value: "<catalog>.<schema>.<table>"

  # ── Project Registry ───────────────────────────────────────────
  # Injected from the volume resource binding.
  # The Databricks Apps runtime sets this to /Volumes/<catalog>/<schema>/<volume>.
  # When present, it overrides the three static REGISTRY_* variables below.
  - name: REGISTRY_VOLUME_PATH
    valueFrom: volume

  # Static fallbacks — used for local development and MCP when no
  # volume resource is bound.
  - name: REGISTRY_CATALOG
    value: "<your-catalog>"
  - name: REGISTRY_SCHEMA
    value: "<your-schema>"
  - name: REGISTRY_VOLUME
    value: "OntoBricksRegistry"

  # ── Permission Management ──────────────────────────────────────
  # App name used to check CAN_MANAGE permissions via the Databricks API
  # Must match the deployed app name exactly
  - name: ONTOBRICKS_APP_NAME
    value: "ontobricks"

  # ── MLflow ─────────────────────────────────────────────────────
  # Persist agent traces to the workspace tracking server
  - name: MLFLOW_TRACKING_URI
    value: "databricks"

# ── Resources ──────────────────────────────────────────────────
# Configure these resources in the Databricks Apps UI after deployment.
# Once bound, the corresponding Settings UI controls are locked (read-only).
resources:
  - name: sql-warehouse
    description: "SQL Warehouse for executing queries and metadata operations"
    sql_warehouse:
      permission: CAN_USE
  - name: volume
    description: "Unity Catalog Volume for the OntoBricks project registry"
    volume:
      permission: CAN_READ_WRITE
```

### Resource-Locked Settings

When OntoBricks detects that it is running as a Databricks App with resource bindings, the Settings page automatically locks the affected controls:

| Resource | What is locked | How to change |
|----------|---------------|---------------|
| `sql-warehouse` | SQL Warehouse dropdown + refresh button | Rebind the `sql-warehouse` resource in **Compute > Apps > Resources** |
| `volume` | Registry Change button | Rebind the `volume` resource in **Compute > Apps > Resources** |

The **Initialize** button remains available when the volume resource is bound but the registry has not been initialized yet (no `.registry` marker). This allows first-time setup without unlocking the UI.

In **local development mode** (no Databricks App resources), all Settings controls remain fully editable.

---

## 3. Permission Management

OntoBricks includes a built-in permission system that controls who can access the app and what they can do. Permissions are managed in **Settings > Permissions** and are only active when running as a Databricks App (local development has no restrictions).

### How It Works

| Role | Access |
|------|--------|
| **Admin** | Full access + can manage the permission list. Determined by **CAN_MANAGE** on the Databricks App. |
| **Editor** | Full access to all features (read + write). |
| **Viewer** | Read-only access (cannot create, edit, or delete). |
| **No role** | Blocked entirely (redirect to Access Denied page). |

When no permissions are configured yet, only users with **CAN_MANAGE** on the Databricks App have access. Everyone else is blocked until an admin adds them via the Permissions tab.

### How Admin Detection Works

At runtime, the app checks whether the logged-in user has `CAN_MANAGE` on the Databricks App by calling the Permissions API. The check uses the **user's own OAuth token** (forwarded by the Databricks Apps proxy via `x-forwarded-access-token`). This means:

- **No special SP setup is needed** — admin detection works out of the box on a fresh deployment.
- The app's service principal does not need `CAN_MANAGE` on itself.
- If the user's forwarded token is not available (e.g., local dev), the app falls back to the SDK (service principal) and then a REST call with the SP token.

### Managing Permissions

1. Ensure you have **CAN_MANAGE** on the `ontobricks` app in the Databricks UI (Compute > Apps > ontobricks > Permissions)
2. Open the app and go to **Settings > Permissions**
3. Click **Add** to grant access to workspace users or groups
4. Assign each principal a **Viewer** or **Editor** role
5. Users not in the list are blocked from accessing the app

### Diagnostics

If a user reports "Access Denied", hit the diagnostic endpoint (accessible even when blocked):

```
https://<app-url>/settings/permissions/diag
```

This returns:
- The user's email and forwarded token status
- SDK (SP token) check result
- User-token check result
- Which principals have `CAN_MANAGE`
- The cached admin decision and its age

---

## 4. Deploying to a New Workspace

When moving OntoBricks to a different Databricks workspace, follow these steps.

### 4.1 — Authenticate to the new workspace

```bash
# Option A: Set as default
databricks auth login --host https://<new-workspace>.cloud.databricks.com

# Option B: Use a named profile
databricks auth login --host https://<new-workspace>.cloud.databricks.com --profile new-ws
```

Verify:

```bash
databricks current-user me
# Or with a profile: databricks current-user me --profile new-ws
```

### 4.2 — Prepare Unity Catalog resources

The new workspace needs a catalog/schema where OntoBricks can store projects and triple stores:

```bash
# Create a schema for OntoBricks (adjust catalog name)
databricks sql query "CREATE SCHEMA IF NOT EXISTS main.ontobricks"

# Create the registry volume
databricks sql query "CREATE VOLUME IF NOT EXISTS main.ontobricks.OntoBricksRegistry"
```

### 4.3 — Update configuration files

**`databricks.yml`** — update the variable defaults:

```yaml
variables:
  warehouse_id:
    default: "<new-warehouse-id>"
  registry_catalog:
    default: "<new-catalog>"
  registry_schema:
    default: "<new-schema>"
```

Update the `permissions` section with the deploying user's email.

**`app.yaml`** — update workspace-specific values:

```yaml
- name: DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT
  value: "<new-warehouse-id>"
- name: DATABRICKS_TRIPLESTORE_TABLE
  value: "<catalog>.<schema>.<triplestore_table>"
```

**`src/mcp-server/app.yaml`** — update the main app URL (after first deploy):

```yaml
- name: ONTOBRICKS_URL
  value: "https://<new-ontobricks-app-url>"
```

### 4.4 — Deploy

```bash
# Validate first
databricks bundle validate

# Deploy and start
scripts/deploy.sh --all
```

### 4.5 — Bind resources

1. Go to **Compute > Apps > ontobricks > Resources**
2. Bind `sql-warehouse` to a running SQL Warehouse
3. Bind `volume` to the registry UC Volume
4. Repeat for `mcp-ontobricks`

### 4.6 — Initialize and verify

1. Open the app URL
2. Go to **Settings > Registry > Initialize** (if the volume is empty)
3. Verify both apps are **Running**: `databricks apps get ontobricks`

### 4.7 — Update MCP server URL

After the main app is deployed and running:

```bash
# Get the main app URL
databricks apps get ontobricks -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])"
```

Update `ONTOBRICKS_URL` in `src/mcp-server/app.yaml` with this URL, then redeploy:

```bash
scripts/deploy.sh --mcp-only
```

### New Workspace Checklist

```
[ ] 1.  databricks auth login --host https://<new-workspace>
[ ] 2.  Verify: databricks current-user me
[ ] 3.  Create Unity Catalog resources (schema, volume)
[ ] 4.  Update databricks.yml variables (warehouse_id, registry_*)
[ ] 5.  Update databricks.yml permissions (your email)
[ ] 6.  Update app.yaml (DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT, DATABRICKS_TRIPLESTORE_TABLE)
[ ] 7.  databricks bundle validate
[ ] 8.  scripts/deploy.sh --all
[ ] 9.  Bind sql-warehouse and volume resources in the Apps UI (both apps)
[ ] 10. Initialize registry (Settings > Registry > Initialize)
[ ] 11. Verify both apps are RUNNING
[ ] 12. Update ONTOBRICKS_URL in src/mcp-server/app.yaml with the main app URL
[ ] 13. scripts/deploy.sh --mcp-only (redeploy MCP with correct URL)
[ ] 14. Verify MCP appears in Databricks Playground
```

---

## 5. Triple Store Backend Configuration

OntoBricks supports two triple store backends. Choose one in your project settings.

### Delta (`view`) — No Extra Setup Required

Delta uses a Databricks SQL Warehouse to store triples in a Delta table. On Databricks Apps, the app's service principal authenticates via OAuth automatically — the only requirement is the SQL Warehouse resource declared in `app.yaml` (already configured).

### LadybugDB (`graph`) — No Extra Setup Required

LadybugDB is an embedded graph database that stores data locally at `/tmp`. When the project has an ontology loaded, LadybugDB uses a true graph model (OWL classes become node tables, object properties become relationship tables). Graph data is automatically archived to the registry UC Volume when saving a project and restored on load.

---

## 6. MCP Server Deployment (Databricks Playground)

The MCP server (`mcp-ontobricks`) is a **separate** Databricks App that exposes OntoBricks knowledge-graph tools to the Databricks Playground. It must have a name starting with `mcp-` to be discoverable.

### Prerequisites

| Requirement | Details |
|---|---|
| Main app deployed | `ontobricks` must be deployed and running first |
| Databricks CLI | Authenticated to the same workspace |
| Playground access | Databricks Playground must be enabled |

### Deploy with DAB

The MCP server is deployed alongside the main app by the same `databricks.yml` bundle:

```bash
# Deploy both apps
scripts/deploy.sh --all

# Or deploy MCP only (main app already running)
scripts/deploy.sh --mcp-only
```

### MCP `app.yaml` Configuration

```yaml
command:
  - "uv"
  - "run"
  - "mcp-ontobricks"

env:
  - name: ONTOBRICKS_URL
    value: "https://<your-ontobricks-app-url>"

  - name: DATABRICKS_SQL_WAREHOUSE_ID
    valueFrom: sql-warehouse

  - name: REGISTRY_VOLUME_PATH
    valueFrom: volume

resources:
  - name: sql-warehouse
    sql_warehouse:
      permission: CAN_USE
  - name: volume
    volume:
      permission: CAN_READ_WRITE
```

> **Important**: Update `ONTOBRICKS_URL` to match your main app's URL before deploying. Find it with:
> ```bash
> databricks apps get ontobricks -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])"
> ```

### Post-Deployment Resource Binding

After the first deployment, bind the MCP server's resources:

1. Go to **Compute > Apps > mcp-ontobricks > Resources**
2. Bind `sql-warehouse` to the same SQL Warehouse used by the main app
3. Bind `volume` to the same registry UC Volume

### Grant the MCP App Access to the Main App

The MCP server calls the main app's REST API using its service principal's OAuth token. The `users` group should already have `CAN_USE` on the main app (set in `databricks.yml`). If it doesn't:

```bash
databricks apps update-permissions ontobricks --json '{
  "access_control_list": [
    { "group_name": "users", "permission_level": "CAN_USE" }
  ]
}'
```

### Using in Databricks Playground

1. Go to your Databricks workspace
2. Navigate to **Playground**
3. `mcp-ontobricks` appears in the **MCP Servers** list (apps starting with `mcp-` are shown automatically)
4. Select it to use OntoBricks knowledge-graph tools in conversations

### Available MCP Tools

| Tool | Description |
|---|---|
| `list_projects` | List all projects (knowledge graphs) in the registry with names and descriptions |
| `select_project` | Activate a project by name — subsequent queries operate on its triple store |
| `list_entity_types` | Human-readable overview of the selected project's knowledge graph (entity types, counts, predicates) |
| `describe_entity` | Search by name/type and get a full-text description with attributes, relationships, and BFS traversal |
| `get_status` | Compact diagnostic: project, backend, table, data availability, triple count |

### Standalone / Local MCP Usage

For LLM clients like Cursor or Claude Desktop (stdio transport):

```bash
cd mcp-server
uv run python -c \
  "from server.app import create_mcp_server; create_mcp_server('standalone').run(transport='stdio')"
```

Or via the convenience wrapper at the project root:

```bash
python src/mcp-server/mcp_server.py              # stdio (default)
python src/mcp-server/mcp_server.py --http       # streamable-http on port 9100
```

Override the target URL:

```bash
ONTOBRICKS_URL=http://your-host:8000 python src/mcp-server/mcp_server.py
```

### Client Configuration Examples

**Cursor** (`.cursor/mcp.json`):

```json
{
  "mcpServers": {
    "ontobricks": {
      "command": "uv",
      "args": ["run", "python", "-c",
        "from server.app import create_mcp_server; create_mcp_server('standalone').run(transport='stdio')"
      ],
      "cwd": "/path/to/OntoBricks/mcp-server"
    }
  }
}
```

**Claude Desktop** (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "ontobricks": {
      "command": "uv",
      "args": ["run", "python", "-c",
        "from server.app import create_mcp_server; create_mcp_server('standalone').run(transport='stdio')"
      ],
      "cwd": "/path/to/OntoBricks/mcp-server",
      "env": { "ONTOBRICKS_URL": "http://localhost:8000" }
    }
  }
}
```

---

## 7. MLflow Agent Observability

OntoBricks agents are instrumented with MLflow tracing. When deployed to Databricks, traces are persisted to the workspace tracking server.

### How It Works

- `MLFLOW_TRACKING_URI=databricks` is set in `app.yaml`
- Application startup in `src/shared/fastapi/main.py` calls `setup_tracing()`, which creates the `/Shared/ontobricks-agents` experiment
- Every agent call (OWL Generator, Auto-Mapping, Auto Icon Assign, Ontology Assistant) produces a span tree:

```
AGENT (run_agent)
├── LLM (_call_llm)        — endpoint, tokens, latency
├── TOOL (tool:get_metadata) — arguments, result
├── LLM (_call_llm)        — next iteration
├── TOOL (tool:execute_sql)  — SQL query, result
└── ...
```

### Viewing Traces

1. In your Databricks workspace, go to **Machine Learning > Experiments**
2. Open **`/Shared/ontobricks-agents`**
3. Click any run, then the **Traces** tab

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MLFLOW_TRACKING_URI` | *(none)* | Set to `databricks` for persistent traces |
| `ONTOBRICKS_MLFLOW_EXPERIMENT` | `ontobricks-agents` | Experiment name (auto-prefixed with `/Shared/` on Databricks) |

Tracing degrades gracefully: if MLflow is not configured, agents run normally without traces.

---

## 8. DAB Reference

### Bundle Structure

```
OntoBricks/
├── databricks.yml          # Bundle definition (apps, permissions, targets)
├── .databricksignore       # Excludes non-runtime files from sync
├── app.yaml                # Main app runtime config
├── src/mcp-server/
│   └── app.yaml            # MCP server runtime config
└── docs/dab-reference.md
    └── README.md           # DAB-specific documentation
```

### Targets

| Target | Mode | Description |
|--------|------|-------------|
| `dev` | development | Default. Uses the authenticated user's workspace path. |
| `prod` | production | Explicit root path, restricted permissions. |

```bash
databricks bundle deploy -t prod
databricks bundle run ontobricks_app -t prod
```

### Variables

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

### Makefile Targets

```bash
make bundle-validate        # Validate the bundle config
make bundle-deploy          # Deploy both apps (dev)
make bundle-deploy-prod     # Deploy both apps (prod)
make bundle-run             # Start the main app
make bundle-run-mcp         # Start the MCP server
make bundle-summary         # Show bundle summary
```

### File Sync

The `.databricksignore` at the project root excludes non-runtime files (tests, docs, data, IDE config, the MCP server source) from the main app sync. The MCP server has its own `source_code_path` pointing directly to `src/mcp-server/`.

### Binding Existing Apps

If the apps already exist in the workspace from a previous manual deployment:

```bash
# Bind the bundle definitions to existing workspace apps
databricks bundle deployment bind ontobricks_app ontobricks
databricks bundle deployment bind mcp_ontobricks_app mcp-ontobricks

# Then deploy normally
databricks bundle deploy
```

---

## 9. Full Deployment Checklist

Use this checklist when deploying OntoBricks from scratch on any workspace:

```
[ ] 1.  Databricks CLI installed (>= 0.250.0) and authenticated
          databricks auth login --host https://<workspace>
          databricks current-user me
[ ] 2.  SQL Warehouse created and running
[ ] 3.  Unity Catalog resources available:
        [ ] A catalog you can use (e.g., main or your personal catalog)
        [ ] A schema within that catalog (e.g., ontobricks)
        [ ] A Volume for the project registry (e.g., OntoBricksRegistry)
[ ] 4.  Update databricks.yml:
        [ ] Variable defaults (warehouse_id, registry_catalog, registry_schema, registry_volume)
        [ ] Permissions (your email with CAN_MANAGE)
[ ] 5.  Update app.yaml:
        [ ] DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT
        [ ] DATABRICKS_TRIPLESTORE_TABLE
        [ ] (local dev only) REGISTRY_CATALOG / REGISTRY_SCHEMA / REGISTRY_VOLUME
[ ] 6.  Validate: databricks bundle validate
[ ] 7.  Deploy main app:
          scripts/deploy.sh
[ ] 8.  Bind sql-warehouse resource in the Apps UI
[ ] 9.  Bind volume resource to the registry UC Volume
[ ] 10. Verify main app is RUNNING:
          databricks apps get ontobricks
[ ] 11. Initialize registry if the volume is empty:
          Open app → Settings → Registry → Initialize
[ ] 12. (If using MCP) Update ONTOBRICKS_URL in src/mcp-server/app.yaml:
          databricks apps get ontobricks -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])"
[ ] 13. (If using MCP) Deploy MCP server:
          scripts/deploy.sh --mcp-only
[ ] 14. (If using MCP) Bind MCP resources (same warehouse + volume)
[ ] 15. (If using MCP) Verify in Databricks Playground
```

---

## 10. Troubleshooting

### App Won't Start

```bash
# Check app status
databricks apps get ontobricks

# Check recent deployment status
databricks apps list-deployments ontobricks
```

### "Access Denied" Despite Having CAN_MANAGE

1. Hit the diagnostic endpoint: `https://<app-url>/settings/permissions/diag`
2. Check `user_token_present` — should be `true` in Databricks App mode
3. Check `user_token_can_manage` — should list your email
4. If `email_is_manager` is `false`, verify your email matches the CAN_MANAGE grant exactly
5. Check `admin_cache` — if stale, restart the app to clear it

### "Databricks credentials not configured"

The agents need OAuth credentials to call the Foundation Model API. In a Databricks App, these are resolved automatically via the service principal. If you see this error:

1. Verify the app is running as a Databricks App (not locally)
2. Check that the LLM endpoint is configured in the project settings
3. Review app logs for OAuth token resolution errors

### Connection Errors

- Verify the SQL Warehouse is running and the resource binding is correct
- Check the SP has correct permissions on catalogs/schemas
- Review app logs in the Databricks Apps UI

### `localhost` Redirects When Deployed

All internal navigation links must include a trailing slash (e.g., `/dtwin/` not `/dtwin`). FastAPI's `redirect_slashes` generates `localhost:8000` redirects behind the Databricks proxy.

### Module Import Errors After Redeployment

Stale `__pycache__` or old directories in the workspace can cause import conflicts:

```bash
# Remove stale workspace files if needed
databricks workspace delete /Users/<you>/ontobricks/<stale-dir> --recursive
```

### MCP Server Returns 401 Unauthorized

The MCP app's SP needs `CAN_USE` permission on the main app. The `users` group should already have this via `databricks.yml`.

### Agent Traces Not Appearing

1. Verify `MLFLOW_TRACKING_URI=databricks` is set in `app.yaml`
2. Check the app logs for `MLflow tracing enabled — experiment='/Shared/ontobricks-agents'`
3. Run an agent call (e.g., generate an ontology) to create the first trace
4. The experiment is created on first use, not at startup

---

## 11. Production Considerations

### Security

- Never commit `.env` files or secrets to Git
- Use Databricks Secrets for sensitive data (passwords, tokens)
- Use service principals for production (automatically handled by Databricks Apps)
### Performance

- Use an appropriately sized SQL Warehouse (enable auto-stop to save costs)

### Monitoring

- **App logs**: Available in the Databricks Apps console
- **Log level**: Configure via `LOG_LEVEL` environment variable (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- **Structured JSON logs**: Set `LOG_FORMAT=json` to emit one JSON object per log line — ideal for log aggregation and search
- **Request timing**: Every non-static request is logged with method, path, status code, and duration in milliseconds
- **Thread pool**: Tune concurrent blocking work via `ONTOBRICKS_THREAD_POOL_SIZE` (default `20`)
- **Health checks**: `GET /health` and `GET /health/detailed`
- **Agent traces**: View under **Machine Learning > Experiments > `/Shared/ontobricks-agents`** — each agent call shows a span tree with inputs, outputs, latency, and token usage

### Updating

```bash
# Pull latest code and redeploy
git pull origin main
scripts/deploy.sh --all
```
