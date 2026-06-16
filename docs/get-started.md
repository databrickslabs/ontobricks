# Getting Started with OntoBricks

By the end of this guide you will have OntoBricks running locally and connected to your Databricks workspace, ready to design your first ontology and explore a knowledge graph.

## Prerequisites

Before you begin, ensure you have:

- **Python 3.10+** installed on your system
- **Databricks workspace** access (Databricks Apps must be enabled)
- **Personal Access Token** from Databricks (local dev) or service-principal auth (Databricks Apps)
- **SQL Warehouse** in the workspace (you will need its ID for local dev)
- **Databricks Lakebase Autoscaling** project + branch + Postgres database — used for
  the domain registry (domains, versions, permissions, schedules, global config)
  and for the Graph DB triple store. Required since **v0.4.0**.
  Provisioned Lakebase instances are **not** supported.
- **Unity Catalog Volume** — reserved for binary artefacts (`documents/`
  uploads — domain-scoped attachments imported by the ontology designer).
  The same catalog/schema that hosts the volume is also used by
  OntoBricks for the Delta triplestore VIEWs (`triplestore_<domain>_v<n>`).
- **`psql`** (libpq client) on `PATH` for the Lakebase permission
  bootstrap scripts. On macOS: `brew install libpq && brew link --force libpq`.

> **Lakebase note.** OntoBricks targets **Lakebase Autoscaling** exclusively.
> Create a project + branch from the Databricks workspace UI (Compute → Postgres
> → New project) or via `databricks api post /api/2.0/postgres/projects …`,
> then create at least one Postgres database in it. Note the `db-…`
> resource id from `databricks postgres list-databases "projects/<id>/branches/<branch>" -o json` — you'll need it for the deployment bundle.

## Installation

### Option 1: Using the Setup Script (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd OntoBricks

# Make the setup script executable and run it
chmod +x scripts/setup.sh
scripts/setup.sh
```

The setup script will:
1. Check your Python version
2. Install uv package manager (if not present)
3. Create a virtual environment
4. Install all dependencies
5. Create a `.env` template file

### Option 2: Manual Installation

```bash
# Clone the repository
cd OntoBricks

# Create virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # On macOS/Linux
# .venv\Scripts\activate   # On Windows

# Install dependencies (Lakebase Postgres driver is mandatory since v0.4.0)
uv sync --extra lakebase
# Or with pip:
pip install -e ".[lakebase]"
```

> **Why `--extra lakebase`?** Since v0.4.0 the domain registry lives in
> Lakebase Postgres, so `psycopg[binary]` and `psycopg-pool` are required
> at runtime. They are declared as optional in `pyproject.toml`
> (`[project.optional-dependencies] lakebase`) so volume-only forks can
> opt out, but every standard deployment must install them.

## Configuration

### Step 1: Get Your Databricks Credentials

#### Databricks Host
Your workspace URL, for example:
- Azure: `https://adb-1234567890123456.7.azuredatabricks.net`
- AWS: `https://dbc-a1b2c3d4-e5f6.cloud.databricks.com`
- GCP: `https://12345678901234.8.gcp.databricks.com`

#### Personal Access Token
1. In Databricks, click your username (top right)
2. Select **User Settings**
3. Go to **Access tokens** tab
4. Click **Generate new token**
5. Give it a name (e.g., "OntoBricks")
6. Set expiration (e.g., 90 days)
7. Click **Generate** and **copy the token immediately**

#### SQL Warehouse ID
1. In Databricks, go to **SQL Warehouses**
2. Click on your warehouse
3. Find the ID in the URL or **Connection Details** tab:
   ```
   /sql/1.0/warehouses/abc123def456
                        ↑ This is your ID
   ```

### Step 2: Create the .env File

Create a `.env` file in the OntoBricks directory:

```bash
# Databricks Configuration (Required)
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef
DATABRICKS_SQL_WAREHOUSE_ID=abc123def456

# Registry Volume — required if you want the binary-artefact volume
# to be available in local dev. The volume must already exist in UC.
REGISTRY_CATALOG=<your-catalog>
REGISTRY_SCHEMA=<your-schema>
REGISTRY_VOLUME=OntoBricksRegistry

# Lakebase (Required since v0.4.0 for the domain registry)
# When deployed as a Databricks App with a `database` resource bound,
# Databricks auto-injects PG* — leave these unset in that case.
# For local dev use the semantic coordinates below:
LAKEBASE_PROJECT=ontobricks-app          # Autoscaling project name
LAKEBASE_BRANCH=develop                  # Branch to connect to
LAKEBASE_DATABASE=ontobricks_registry    # Postgres database (datname)
LAKEBASE_SCHEMA=ontobricks_registry      # Postgres schema for the registry
PGUSER=you@example.com                   # Your Databricks email (local dev)
# Postgres password is minted at runtime via `LakebaseAuth.password()`,
# do NOT set PGPASSWORD here.

# Optional Configuration
SECRET_KEY=your-secret-key-here
DATABRICKS_APP_PORT=8000
```

> **Lakebase auth in local dev.** The Postgres password is a short-lived
> JWT minted by `LakebaseAuth` via `POST /api/2.0/postgres/credentials`
> using your `DATABRICKS_TOKEN`. Set `LAKEBASE_PROJECT` + `LAKEBASE_BRANCH`
> (and optionally `LAKEBASE_DATABASE`) in `.env` — `LakebaseAuth` resolves
> the endpoint hostname automatically via the Postgres API.
> In a deployed App, `PGHOST` / `PGDATABASE` / `PGUSER` are auto-injected
> by the platform; `LAKEBASE_*` vars then serve as informational labels only.

## Running the Application

### Start the Application

```bash
# Using the start script (recommended)
scripts/start.sh

# Run in background
scripts/start.sh --background

# Or using make
make run

# Or manually
source .venv/bin/activate
python run.py
```

### Stop the Application

```bash
# Using the stop script
scripts/stop.sh

# Or press Ctrl+C if running in foreground
```

### Access the Application

Open your browser to: **http://localhost:8000**

## Permission Management (Databricks App Only)

When running as a Databricks App, OntoBricks enforces role-based access control:

- **Admin**: Users with **CAN_MANAGE** on the Databricks App. They can manage the permission list in **Settings → Permissions**.
- **Editor**: Full read/write access to all features.
- **Viewer**: Read-only access.
- **No role**: Blocked from accessing the app entirely.

> **First-time setup**: When no permissions are configured yet, only users with **CAN_MANAGE** on the Databricks App have access. Everyone else is blocked. Add users via **Settings -> Permissions** to grant them access.

To manage permissions, you must:
1. Have **CAN_MANAGE** set on the app in the Databricks UI (Compute → Apps → ontobricks → Permissions)
2. The app's service principal must have **CAN_MANAGE** on itself — `make deploy` runs `scripts/bootstrap-app-permissions.sh` automatically; otherwise run `make bootstrap-perms`. See the [Deployment Guide](deployment.md#4-permission-management).
3. The app's service principal must have **USAGE + DML** on the Lakebase
   registry / graph / sync schemas — `scripts/deploy.sh` runs
   `scripts/bootstrap-lakebase-perms.sh` automatically on the
   `dev-lakebase` target; otherwise run `make bootstrap-lakebase`. See
   the [Deployment Guide §2 Step 5b](deployment.md#step-5b--lakebase-schema-grants-target-dev-lakebase-only).
4. The app's service principal must have **Unity Catalog** privileges
   on the registry catalog/schema/volume **and** on every source table
   referenced by an R2RML mapping. See the [Deployment Guide §3](deployment.md#3-unity-catalog-permissions-for-the-service-principal)
   for the exact grants.

In local development mode, there are no restrictions — all users have full admin access.

## First Steps in OntoBricks

### 1. Verify Configuration

1. Click **Settings** in the navigation bar
2. Your credentials should be loaded from `.env`
3. Select a **SQL Warehouse** from the dropdown
4. Click **Test Connection** to verify
5. You should see "Connection successful"
6. Open **Settings → Registry**. On a fresh Lakebase database the
   registry tables don't exist yet — click **Initialize**. This runs
   the schema migration in `LAKEBASE_SCHEMA` (default
   `ontobricks_registry`) and creates the registry Volume if missing.
   Then run `make bootstrap-lakebase` once to grant the app SP
   USAGE/DML on the freshly-created schema.

### 2. Design an Ontology (Visual Designer)

The fastest way to create an ontology is using the visual **Design** interface:

1. Go to the **Ontology** page (click on "Ontology" in the navbar)
2. Click **Design** in the sidebar

#### Create Entities
1. Click **+ Add Entity** button
2. A new entity appears on the canvas
3. Click on the entity name to edit it (e.g., "Person")
4. Click the **+** button on the entity to add attributes
5. Click the **🎨** button to add an icon
6. Click the **📝** button to add a description

#### Create Relationships
1. Click and drag from one entity's connector (○) to another
2. Click on the relationship label to rename it (e.g., "worksIn")
3. Click the direction button (→/←/↔) to set the relationship direction
4. Click **+** on the relationship to add attributes

#### Organize the Diagram
- **Drag** entities to reposition them
- Click **Auto Layout** to organize automatically
- Click **Center** to fit everything in view
- Use the **scroll wheel** to zoom

All changes are **automatically saved**.

### 3. Design an Ontology (Form-Based)

Alternatively, use the traditional form interface:

1. Click **Information** in the sidebar
2. Set an **Ontology Name** (e.g., "MyOrganization")
3. The **Base URI** is auto-generated from the configured domain

#### Add Entities
1. Click **Entities** in the sidebar
2. Click "Add Class"
3. Enter a name (e.g., "Person", "Department")
4. Add attributes in the Attributes section
5. Choose an icon
6. Click "Add"

#### Add Relationships
1. Click **Relationships** in the sidebar
2. Click "Add Property"
3. Select source entity (Domain)
4. Select target entity (Range)
5. Set the direction (Forward/Reverse/Bidirectional)
6. Add relationship attributes if needed
7. Click "Add"

### 4. Preview and Save

1. Click **OWL Content** to see your generated ontology in Turtle format
2. Click **Validate** to check your ontology
3. Click **Save** to store in Unity Catalog

### 5. Assign Data Sources

1. Go to the **Mapping** page (click on "Mapping" in the navbar)
2. Your ontology must be loaded (indicated by green checkmark)

#### Visual Mapping (Designer View)
1. Click **Designer** in the sidebar to use the visual mapping interface
2. Click on entities to open the mapping dialog:
   - Enter a SQL query that returns entity data (e.g., `SELECT * FROM main.default.person`)
   - Click **Test Query** to validate and preview results
   - Select the **ID Column** for generating unique URIs
   - Select the **Label Column** for display names
   - Map each entity attribute to a query column
   - Click "Save Mapping"
3. Click on relationships to map them:
   - Write a SQL query that returns source and target IDs
   - Click **Test Query** to validate
   - Select source and target ID columns from results
   - Click "Save Mapping"

#### Manual Mapping
1. Click **Manual** in the sidebar for advanced mapping options

#### R2RML Output
1. Navigate to **Domain** → **Export**
2. View the auto-generated R2RML mapping
3. Copy or download as needed

4. Click **Validate** (in navbar) to verify all mappings are complete

### 6. Explore Your Data (Digital Twin)

1. Go to the **Digital Twin** page
2. Click **Synchronize** to generate triples from your mappings and write them to the configured Unity Catalog table
3. Once synced, browse the **Triples** tab to see the generated data in a sortable grid
4. Explore the **Knowledge Graph** tab for an interactive graph:
   - Click on entities to see details in the right panel
   - Use **Find** to search for specific entities
   - Use **Filters** to narrow down by entity type, field, or depth
   - View all mapped attributes, values, and relationships
5. Run **Quality** checks to validate your data against ontology constraints
6. Run **Data Quality** (SHACL) checks from the **Data Quality** sidebar section — validates cardinality, datatypes, patterns, and custom SPARQL rules against the triple store
7. Run **Reasoning** from the **Reasoning** sidebar section — executes OWL 2 RL inference and SWRL business rules to discover inferred triples
8. Access the **GraphQL** playground to query your knowledge graph with auto-generated typed queries

## Common Commands

```bash
# Start application
scripts/start.sh

# Start in background
scripts/start.sh --background

# Stop application
scripts/stop.sh

# Run tests
make test

# Format code
make format

# Clean up
make clean

# Show all make commands
make help
```

## Understanding the Navigation

The OntoBricks interface has a navigation bar with status indicators:

| Element | Description |
|---------|-------------|
| **SQL Warehouse** | Dropdown to select/switch SQL warehouses |
| **Ontology** | Shows ✓ (green) when ontology is loaded, ✗ (red) otherwise |
| **Mapping** | Shows ✓ (green) when R2RML mapping exists, ✗ (red) otherwise |
| **Digital Twin** | Access the sync, knowledge graph, and quality checks interface |
| **Settings** | Manage Databricks connection and settings |

## Ontology Sidebar Navigation

| Menu Item | Description |
|-----------|-------------|
| **Wizard** | AI-powered ontology generation with quick templates |
| **Design** | Visual drag-and-drop ontology designer |
| **Information** | Basic ontology settings (name, URI) |
| **Entities** | Manage classes with form interface |
| **Relationships** | Manage object properties |
| **Designer** | Interactive force-directed ontology graph (OntoViz canvas) |
| **SWRL Rules** | Define SWRL inference rules |
| **Data Quality** | SHACL shape-based data quality rules |
| **Constraints** | Property cardinality and value constraints |
| **Expr. & Axioms** | OWL class expressions and axioms |
| **Import** | Import OWL, RDFS, FIBO, CDISC, IOF standards |
| **OWL Content** | View generated Turtle/OWL |

> **Tip**: The Wizard provides domain-specific quick templates (CRM, E-Commerce, IoT, Healthcare, Energy). These are defined in `src/shared/config/constants.py` and can be customised or extended.

## Troubleshooting

### "Connection failed" Error

- Verify your `DATABRICKS_HOST` includes `https://`
- Check your token hasn't expired
- Ensure the SQL Warehouse is running (check Databricks UI)
- Verify network connectivity

### No Tables Showing

- Check catalog and schema names are correct (case-sensitive)
- Verify you have permissions to access the schema
- Try listing tables directly in Databricks SQL

### Port Already in Use

Change the port in `.env`:
```bash
DATABRICKS_APP_PORT=8080
```

Then restart the application.

### Virtual Environment Issues

If you have issues with the virtual environment:
```bash
# Remove existing environment
rm -rf .venv

# Run setup again
scripts/setup.sh
```

### Sync or Knowledge Graph Fails

- Ensure both Ontology and Mapping have green checkmarks in the navbar
- Check that the R2RML mapping is generated (visible in Domain → Export)
- Verify the SQL Warehouse is running and accessible
- Check browser console for any errors

### Design Changes Not Appearing

- Wait for the "Saving..." indicator to complete
- Switch to another tab and back to refresh
- Check browser console for any errors

## Next Steps

- Read the [User Guide](user-guide.md) for detailed instructions
- See the [Architecture](architecture.md) for technical details
- Check [Deployment](deployment.md) for Databricks Apps deployment
- Explore the [MCP Server](mcp.md) for Databricks Playground integration
- Review [Data Quality](user-guide.md#data-quality) for SHACL shape validation
- Explore [Reasoning](user-guide.md#reasoning) for OWL 2 RL and SWRL inference
- See the [External API](api.md) for REST and GraphQL programmatic access

---

Need help? Check the troubleshooting section or review the logs in `.ontobricks.log` when running in background mode.


---

## Environment variables (full reference)

OntoBricks uses environment variables for configuration, making it easy to deploy in different environments without code changes.

### Configuration Variables

#### Required for Databricks Connectivity

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABRICKS_HOST` | Your Databricks workspace URL | `https://your-workspace.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal access token or service principal token | `dapi1234567890abcdef...` |
| `DATABRICKS_SQL_WAREHOUSE_ID` | SQL Warehouse identifier | `abc123def456...` |

#### Optional Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SECRET_KEY` | Secret key for session encryption | Random (dev only) |
| `DATABRICKS_APP_PORT` | Port the application listens on | `8000` |
| `REGISTRY_VOLUME_PATH` | Full volume path injected by the Databricks App `volume` resource (`/Volumes/<catalog>/<schema>/<volume>`). When set, overrides the three `REGISTRY_*` variables below. | *(from volume resource)* |
| `REGISTRY_CATALOG` | Unity Catalog catalog for the domain registry (local dev fallback) | *(from session or Settings)* |
| `REGISTRY_SCHEMA` | Schema for the domain registry (local dev fallback) | *(from session or Settings)* |
| `REGISTRY_VOLUME` | Volume name for domain storage (local dev fallback) | `OntoBricksRegistry` |
| `DATABRICKS_TRIPLESTORE_TABLE` | Default triple store table (catalog.schema.table) | *(none)* |
| `DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT` | Fallback SQL Warehouse ID for MCP/API calls | *(none)* |

#### Lakebase (Required since v0.4.0)

| Variable | Description | Default / Source |
|----------|-------------|------------------|
| `LAKEBASE_SCHEMA` | Postgres schema used by the registry inside `PGDATABASE`. Mirror the bundle's `lakebase_registry_schema`. | `ontobricks_registry` |
| `LAKEBASE_PROJECT` | Lakebase Autoscaling **project id** (`projects/<this>/...`). Used by `LakebaseAuth` for local dev host resolution. In deployed Apps, informational only (injected via `app.yaml`). | *(set in `.env` for local dev)* |
| `LAKEBASE_BRANCH` | Branch to connect to (e.g. `develop`, `production`). Used together with `LAKEBASE_PROJECT` to resolve the endpoint hostname locally. | *(set in `.env` for local dev)* |
| `LAKEBASE_DATABASE` | Postgres database name (`datname`). Resolved from the branch when unset. | *(set in `.env` for local dev)* |
| `PGHOST` | Lakebase Autoscaling endpoint (`ep-<id>.database.<region>.cloud.databricks.com`). **Auto-injected** by the `database` Apps resource binding — do not set in `.env`. | *(auto-injected by the Apps platform)* |
| `PGPORT` | Postgres port. | `5432` |
| `PGDATABASE` | Postgres database name. **Auto-injected** by the Apps platform. | *(auto-injected)* |
| `PGUSER` | Postgres role — for local dev, your Databricks email; in Apps, the SP client id. | *(auto-injected in Apps; set in `.env` locally)* |

The Postgres password is **never** set via env var — `LakebaseAuth`
mints a short-lived JWT via `POST /api/2.0/postgres/credentials` using
the workspace token (locally) or the SP token (in Apps).

#### MLflow / Agent Tracing

| Variable | Description | Default |
|----------|-------------|---------|
| `MLFLOW_TRACKING_URI` | Set to `databricks` to persist agent traces to the workspace tracking server. When unset, traces go to a local `mlflow.db` file. | *(none)* |
| `ONTOBRICKS_MLFLOW_EXPERIMENT` | Experiment name for agent traces. On Databricks, relative names are automatically prefixed with `/Shared/`. | `ontobricks-agents` |

#### MCP Server

| Variable | Description | Default |
|----------|-------------|---------|
| `ONTOBRICKS_URL` | URL of the main OntoBricks app (used by the MCP server) | `http://localhost:8000` |

#### Databricks Runtime Detection

| Variable | Description | Set By |
|----------|-------------|--------|
| `DATABRICKS_RUNTIME_VERSION` | Databricks runtime version | Databricks (automatic) |

When this variable is present, OntoBricks automatically switches to production mode and uses service principal authentication.

### Setting Environment Variables

#### Local Development (.env file)

Create a `.env` file in the project root:

```bash
## Databricks Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef...
DATABRICKS_SQL_WAREHOUSE_ID=abc123def456...

## Optional Configuration
SECRET_KEY=your-secret-key-here
DATABRICKS_APP_PORT=8000

## Performance / Observability (optional)
LOG_FORMAT=json                        # Structured JSON logging (default: text)
LOG_LEVEL=INFO                         # DEBUG, INFO, WARNING, ERROR, CRITICAL
ONTOBRICKS_THREAD_POOL_SIZE=20         # Max threads for blocking I/O
```

#### Shell Export

```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi1234567890abcdef...
export DATABRICKS_SQL_WAREHOUSE_ID=abc123def456...
```

#### Databricks Apps Deployment

When deploying to Databricks Apps, set environment variables in the app configuration:

1. Navigate to your app in Databricks UI
2. Go to Configuration → Environment Variables
3. Add the required variables
4. Restart the app

**Note**: In Databricks Apps, `DATABRICKS_HOST` and authentication are typically handled automatically by the service principal.

### Automatic Configuration Detection

OntoBricks automatically detects and uses environment variables in the following priority:

1. **Environment Variables**: Direct OS environment variables
2. **App Settings**: Application configuration (loaded from environment via Pydantic)
3. **Manual Entry**: User input through the settings page

#### Settings Page Behavior

When you open the Settings page:

- **Environment Variables Set**: Fields are pre-populated and marked as read-only
- **No Environment Variables**: Fields are empty and editable
- **Partial Configuration**: Mix of pre-populated and editable fields

#### Resource-Locked Controls (Databricks Apps)

When the app is deployed with Databricks App resource bindings (`sql-warehouse` and/or `volume`), the corresponding Settings controls are **locked**:

- **SQL Warehouse**: The warehouse dropdown and refresh button are disabled. A lock icon indicates the value is configured via the Databricks App resource.
- **Registry**: The Change button is disabled. If the volume is bound but the registry is not yet initialized, the **Initialize** button remains available so an admin can bootstrap the registry.

To change these values, update the resource bindings in **Compute > Apps > Resources** and restart the app.

#### Visual Indicators

The configuration page shows:

```
ℹ️ Using credentials from environment
```

And individual fields show:

```
✓ Loaded from environment
```

For fields that are automatically configured.

### Security Best Practices

#### Development

1. **Use .env file**: Keep credentials in `.env` (never commit to git)
2. **Add to .gitignore**: Ensure `.env` is in `.gitignore`
3. **Use example file**: Provide `.env.example` for team members

#### Production

1. **Use Service Principal**: Avoid personal access tokens in production
2. **Environment-Only**: Set all credentials as environment variables
3. **Secret Management**: Use Databricks Secrets or similar
4. **Rotate Regularly**: Change tokens and secrets periodically

### Troubleshooting

#### Variables Not Loading

**Problem**: Configuration page is empty despite setting environment variables

**Solutions**:
1. Restart the application after setting variables
2. Check variable names match exactly (case-sensitive)
3. Verify variables are exported in the shell
4. Check `.env` file location (must be in project root)

#### Read-Only Fields

**Problem**: Can't edit configuration fields

**Reason**: Fields are loaded from environment variables

**Solutions**:
1. Unset environment variables to allow manual entry:
   ```bash
   unset DATABRICKS_HOST
   unset DATABRICKS_TOKEN
   ```
2. Or override by restarting app without loading `.env`

#### Token Not Working

**Problem**: Connection fails despite token being set

**Solutions**:
1. Verify token hasn't expired
2. Check token has correct permissions
3. Ensure no extra spaces or quotes in `.env` file
4. Test token with curl:
   ```bash
   curl -H "Authorization: Bearer $DATABRICKS_TOKEN" \
        $DATABRICKS_HOST/api/2.0/clusters/list
   ```

### Example Configurations

#### Local Development

```bash
## .env
DATABRICKS_HOST=https://my-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef
DATABRICKS_SQL_WAREHOUSE_ID=abc123
SECRET_KEY=dev-secret-key
DATABRICKS_APP_PORT=8000

## MLflow (optional — persist traces to Databricks)
MLFLOW_TRACKING_URI=databricks
```

#### Databricks Apps

```bash
## Injected from app.yaml resource bindings
DATABRICKS_SQL_WAREHOUSE_ID=<from sql-warehouse resource>
REGISTRY_VOLUME_PATH=<from volume resource, e.g. /Volumes/catalog/schema/volume>

## Set via app.yaml env section
MLFLOW_TRACKING_URI=databricks
DATABRICKS_APP_PORT=8000

## Static fallbacks (used for local dev / MCP when no resource is bound)
REGISTRY_CATALOG=main
REGISTRY_SCHEMA=default
REGISTRY_VOLUME=OntoBricksRegistry

## MCP server (when deployed as separate app)
ONTOBRICKS_URL=https://your-ontobricks-app-url.databricks-apps.com

## Automatically set by Databricks
DATABRICKS_HOST=<automatically-configured>
## Authentication via service principal (automatic)
```

When both the `sql-warehouse` and `volume` resources are bound, the Settings UI locks the warehouse and registry controls. See the [Deployment Guide](deployment.md) for details.

#### CI/CD Pipeline

```bash
## Set as secrets in CI/CD system
DATABRICKS_HOST=${{ secrets.DATABRICKS_HOST }}
DATABRICKS_TOKEN=${{ secrets.DATABRICKS_TOKEN }}
DATABRICKS_SQL_WAREHOUSE_ID=${{ secrets.WAREHOUSE_ID }}
```

### Global Constants (Non-Environment)

Some application-wide constants are not controlled by environment variables. These live in `src/shared/config/constants.py` and include:

| Constant | Default | Description |
|----------|---------|-------------|
| `APP_NAME` | `"OntoBricks"` | Application display name |
| `APP_VERSION` | `"0.9.0"` | Current version string |
| `ONTOBRICKS_NS` | `http://ontobricks.com/schema#` | RDF namespace for OntoBricks extensions |
| `DEFAULT_BASE_URI` | `http://ontobricks.com/ontology#` | Default ontology base URI |
| `LLM_DEFAULT_MAX_TOKENS` | `4096` | Default max tokens for LLM generation |
| `LLM_DEFAULT_TEMPERATURE` | `0.1` | Default temperature for LLM generation |
| `WIZARD_TEMPLATES` | *(dict)* | Ontology Wizard quick-template definitions (CRM, IoT, Energy, etc.) |
| `MAX_NOTIFICATIONS` | `10` | Max notification messages shown in the notification center |
| `AUTO_ASSIGN_CHUNK_SIZE` | `5` | Max entities + relationships per auto-map agent run |
| `AUTO_ASSIGN_CHUNK_COOLDOWN` | `15` | Seconds to wait between auto-map chunks (avoids LLM rate limits) |

These values are edited directly in the Python file. They do not depend on `.env` or OS environment variables.

### Loading Priority

When the application starts, configuration is loaded in this order (later values override earlier ones):

1. Default values in code (including `src/shared/config/constants.py`)
2. Environment variables from OS
3. `.env` file (if present)
4. Manual user input (stored in session)

### Validation

The application validates environment variables on startup:

- ✅ **Valid**: All required variables present and well-formed
- ⚠️ **Partial**: Some variables missing (manual input required)
- ❌ **Invalid**: Variables present but malformed (check format)

Check the terminal output on startup for validation messages.
