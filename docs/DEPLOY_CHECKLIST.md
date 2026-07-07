# OntoBricks deployment checklist

Use this list before your first deploy (or when moving to a new workspace /
instance). Automate most checks with:

```bash
make deploy-check
# or, equivalently:
scripts/_internal/check-deploy-prerequisites.sh
scripts/deploy.sh --dry-run
```

---

## 1. Local workstation

| Requirement | Why | How to verify |
|-------------|-----|---------------|
| **Python ≥ 3.10** | App + tooling | `python3 --version` |
| **uv** | Dependency install (`make install`, `make setup`) | `uv --version` (setup.sh installs it if missing) |
| **Databricks CLI ≥ 0.250.0** | Bundle deploy, bootstrap scripts | `databricks version` |
| **python3** | JSON parsing, app.yaml render | `python3 --version` |
| **psql** (libpq client) | `bootstrap-lakebase-perms.sh`, SQL upgrade scripts | `psql --version` — macOS: `brew install libpq && brew link --force libpq` |
| **curl** | `setup-lakebase.sh` provisioning API calls | `curl --version` |
| **Git clone + venv** | Project source | `make setup` or `make install` |

Local-only check:

```bash
scripts/_internal/check-deploy-prerequisites.sh --local
```

---

## 2. Databricks CLI authentication

| Requirement | Why | How to verify |
|-------------|-----|---------------|
| **Authenticated profile** | Every deploy / bootstrap step | `databricks current-user me` |
| **Correct workspace profile** | Avoid granting on the wrong project | Set `DEFAULT_DATABRICKS_PROFILE` inferences in `scripts/deploy.config.sh` |

```bash
databricks auth login --host https://<workspace> --profile <profile>
```

---

## 3. Workspace resources (must exist before deploy)

Configure in `scripts/deploy.config.sh` (single source of truth).

| Resource | Config variable(s) | Permission you need |
|----------|------------------|---------------------|
| **SQL warehouse** | `DEFAULT_WAREHOUSE_ID` | CAN USE |
| **Unity Catalog catalog** | `DEFAULT_REGISTRY_CATALOG` | USE CATALOG, CREATE SCHEMA (first time) |
| **UC schema + Volume** | `DEFAULT_REGISTRY_SCHEMA`, `DEFAULT_REGISTRY_VOLUME` | ALL PRIVILEGES on schema (or CREATE TABLE/VOLUME) |
| **Lakebase project** | `DEFAULT_LAKEBASE_PROJECT` | CAN USE on project; create via `scripts/bootstrap/setup-lakebase.sh` if missing |
| **Lakebase branch** | `DEFAULT_LAKEBASE_BRANCH` | Usually `production` |
| **Postgres database (datname)** | `DEFAULT_LAKEBASE_DATABASE` | Created by `setup-lakebase.sh` or UI — use `status.postgres_database` from `list-databases`, **not** the hyphenated `db-…` id |
| **Postgres registry schema** | `DEFAULT_LAKEBASE_SCHEMA` | Created by **Settings → Registry → Initialize** in the app (after first deploy) |
| **Databricks Apps (sandbox)** | `DEFAULT_APP_NAME`, `DEFAULT_MCP_APP_NAME` | Created by `make deploy` on first run |

Verify resources:

```bash
databricks warehouses get <WAREHOUSE_ID>
databricks volumes read <catalog>.<schema>.<volume>
databricks postgres list-databases "projects/<project>/branches/<branch>" -o json
```

---

## 4. Lakebase bootstrap (`bootstrap-lakebase-perms.sh`)

Run automatically on every `dev-lakebase` deploy (`make deploy`), or manually:

```bash
make bootstrap-lakebase
```

| Requirement | Why |
|-------------|-----|
| **psql on PATH** | Connects with a minted Lakebase JWT |
| **CLI authenticated as schema owner** (or GRANT OPTION) | Applies GRANTs + idempotent DDL migrations |
| **Active Postgres endpoint** | Project/branch must expose a host via `/api/2.0/postgres/.../endpoints` |
| **Registry schema exists** | After **Settings → Registry → Initialize** — CAN_USE is granted even before init, but schema GRANTs need the schema |
| **Apps deployed** | Service principal ids are resolved from existing apps (warn-only on first deploy) |

Preflight (read-only):

```bash
scripts/_internal/check-deploy-prerequisites.sh --lakebase
```

---

## 5. Registry DB upgrades (0.4 → 0.5 → 0.6)

For **in-place upgrades** of an existing Lakebase registry, schema DDL must be applied **as the schema owner**. OntoBricks applies the same objects in three ways (pick one):

1. **`make bootstrap-lakebase`** / `scripts/bootstrap/lakebase-perms.sh` (recommended — idempotent Step 2b)
2. **`scripts/migrations/upgrade_0.4_to_0.5.sql`** — adds `domain_versions.status` + backfill from `mcp_enabled`
3. **`scripts/migrations/upgrade_0.5_to_0.6.sql`** — collaborative tables, graph analytics, edit locks, change events

Preflight reports **pending** or **stale** migration objects before deploy:

```bash
python3 scripts/_lakebase_preflight.py \
  --project "$LAKEBASE_PROJECT" \
  --branch "$LAKEBASE_BRANCH" \
  --database "$LAKEBASE_DATABASE" \
  --schema "$LAKEBASE_SCHEMA"
```

Manual upgrade example:

```bash
psql "host=<endpoint> dbname=<datname> sslmode=require user=<you>" \
  -v reg_schema=<schema> \
  -f scripts/migrations/upgrade_0.5_to_0.6.sql
```

---

## 6. App permission bootstrap (`bootstrap-app-permissions.sh`)

Run automatically after deploy, or manually:

```bash
make bootstrap-perms
```

| Requirement | Why |
|-------------|-----|
| **Apps exist** | Resolves each app's service principal |
| **CAN_MANAGE on both apps** | Your user must grant CAN_MANAGE to each app's own SP |
| **UC schema ALL_PRIVILEGES** | SPs need CREATE OR REPLACE on registry objects |

---

## 7. Deploy workflow (happy path)

```text
[ ] Edit scripts/deploy.config.sh (DEFAULT_APP_NAME, Lakebase coords, warehouse, UC)
[ ] scripts/_internal/check-deploy-prerequisites.sh          # or make deploy-check
[ ] make deploy-dry-run                            # read-only full orchestrator check
[ ] make deploy                                    # deploy + bootstrap
[ ] Databricks Apps UI: bind sql-warehouse, volume, postgres (first time only)
[ ] App UI: Settings → Registry → Initialize
[ ] make bootstrap-lakebase                          # if schema was created after deploy
[ ] App UI: Settings → Graph DB → Create graph DB  # grants graph schema separately
```

---

## 8. Optional / mode-specific

| Item | When needed |
|------|-------------|
| **`uv sync --extra pitfalls`** | ML-based pitfalls detection panel |
| **`managed_synced` graph mode** | UC catalog ALL_PRIVILEGES (`-c` / `UC_CATALOG` on bootstrap script) |
| **Lakebase via UI “New project”** | **Avoid** for Synced Tables — use `scripts/bootstrap/setup-lakebase.sh` instead |
| **Volume-only backend** | Deploy with `make deploy-volume` (`-t dev`) — skips Lakebase checks |

---

## Quick reference — scripts

| Script | Purpose |
|--------|---------|
| `scripts/_internal/check-deploy-prerequisites.sh` | Read-only preflight (this checklist, automated) |
| `scripts/deploy.sh --dry-run` | Full DAB validate + resource checks, no mutations |
| `scripts/bootstrap/setup-lakebase.sh` | Create Synced-Tables-compatible Lakebase project |
| `scripts/bootstrap/lakebase-perms.sh` | CAN_USE + schema GRANTs + registry migrations |
| `scripts/bootstrap/app-permissions.sh` | App SP self-perms + MCP CAN_USE + UC schema grants |
| `scripts/migrations/upgrade_0.4_to_0.5.sql` | Explicit 0.4→0.5 lifecycle migration |
| `scripts/migrations/upgrade_0.5_to_0.6.sql` | Explicit 0.5→0.6 collaborative / analytics migration |
