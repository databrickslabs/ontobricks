# Updating OntoBricks to 0.8.0

This guide covers a fresh OntoBricks 0.8.0 installation and an in-place
upgrade from 0.7.x. For older installations, first upgrade sequentially to
0.7.x with the migration scripts under `scripts/migrations/`.

## Supported paths

- **Fresh installation:** deploy 0.8.0 to new Databricks Apps and initialize a
  new registry.
- **In-place upgrade:** retain the existing UI and MCP app names, resource
  bindings, registry data, and app URLs while replacing 0.7.x code with 0.8.0.

The only 0.8 registry DDL change is the additive
`domains.mcp_policy jsonb NOT NULL DEFAULT '{}'::jsonb` column. The empty
object preserves 0.7 behavior: all MCP tools remain available and all context
attachments use their normal behavior.

## Before you start

1. Schedule a maintenance window and stop registry writes.
2. Back up the Lakebase database with your normal managed-backup process or
   `pg_dump`. For example, when an authenticated PostgreSQL URL is available:

   ```bash
   pg_dump "$PGURL" --format=custom --file=ontobricks-before-0.8.dump
   ```

3. Back up `scripts/deploy.config.sh` **before** checking out or pulling
   0.8.0. That file is the local source of truth for `make deploy` (instance
   id, warehouse, Unity Catalog volume, Lakebase project/database/schema).
   The 0.8 tree ships different defaults (`DEFAULT_INSTANCE_ID=08x` and
   matching Lakebase/UC names), so a pull can overwrite a working 0.7.x
   config. Also snapshot the live apps:

   ```bash
   cp scripts/deploy.config.sh scripts/deploy.config.sh.pre-0.8
   databricks apps get <UI_APP_NAME> -o json > ui-app.pre-0.8.json
   databricks apps get <MCP_APP_NAME> -o json > mcp-app.pre-0.8.json
   ```

   Keep the backup outside the repo if you prefer
   (`~/ontobricks-deploy.config.sh.pre-0.8`). Restore it after the 0.8
   checkout whenever you will run `make deploy` against the **same** apps.

4. Confirm the Databricks CLI is authenticated to the correct workspace and
   that `psql` is available:

   ```bash
   databricks current-user me
   psql --version
   ```

5. Obtain the 0.8.0 source and install its committed dependency set without
   changing `uv.lock`:

   ```bash
   uv sync --frozen --extra lakebase
   ```

The principal applying the manual SQL migration must own the `domains` table
or be a member of its owner role. PostgreSQL schema `CREATE` permission alone
does not authorize `ALTER TABLE`.

## Fresh 0.8.0 installation

1. Provision a SQL Warehouse, a Unity Catalog schema and Volume, and a
   Lakebase project/database. Use the repository setup helper for Lakebase:

   ```bash
   scripts/bootstrap/setup-lakebase.sh \
     --name <LAKEBASE_PROJECT> \
     --branch <LAKEBASE_BRANCH> \
     --database <LAKEBASE_DATABASE>
   ```

2. Edit `scripts/deploy.config.sh` (do not reuse a 0.7 backup here). Set
   `DEFAULT_INSTANCE_ID`, `DEFAULT_WAREHOUSE_ID`, the `DEFAULT_REGISTRY_*`
   volume, and the `DEFAULT_LAKEBASE_*` project/branch/database/schema for
   this new instance. App names and the DAB target are derived from
   `DEFAULT_INSTANCE_ID`.
3. Run the read-only checks, then deploy both the UI and MCP apps:

   ```bash
   make deploy-check
   make deploy
   ```

4. Confirm the `sql-warehouse`, `volume`, and `postgres` resources are bound
   to both apps under **Compute > Apps > Resources**.
5. Open **Settings > Registry > Initialize**, then reapply the idempotent
   Lakebase grants now that the registry schema exists:

   ```bash
   make bootstrap-lakebase
   ```

See `docs/deployment.md` for complete first-install permissions and resource
setup.

## Upgrade an existing 0.7.x deployment

### Keep `scripts/deploy.config.sh`

After the 0.8.0 checkout, restore the pre-upgrade backup if git replaced the
file:

```bash
cp scripts/deploy.config.sh.pre-0.8 scripts/deploy.config.sh
```

Then confirm these values still match the **live** 0.7.x instance — do not
bump them to the 0.8 sample defaults:

| Knob | Keep from 0.7.x |
|------|-----------------|
| `DEFAULT_INSTANCE_ID` | Suffix of the live UI app (`060` for `ontobricks-060`) |
| `DEFAULT_WAREHOUSE_ID` | Same SQL Warehouse |
| `DEFAULT_REGISTRY_*` | Same UC catalog / schema / volume |
| `DEFAULT_LAKEBASE_*` | Same project, branch, datname, and registry schema |
| `DEFAULT_DAB_TARGET` | Only set this if the live app was deployed under unsuffixed `dev-lakebase`; otherwise leave the derived `dev-lakebase-<id>` |

Changing `DEFAULT_INSTANCE_ID` creates **new** app names
(`ontobricks-<id>` / `mcp-ontobricks-<id>`) and a new DAB target. That is a
parallel install, not an in-place upgrade.

### Recommended automatic migration

Use the in-place updater from the 0.8.0 checkout. It does **not** rewrite
`deploy.config.sh`: it reads warehouse, volume, Lakebase, and `app.yaml`
env from the live apps, then passes those values into `scripts/deploy.sh`.

```bash
scripts/update-deployed-app.sh <UI_APP_NAME> <MCP_APP_NAME>
```

Still keep the restored `deploy.config.sh` on disk. Later `make deploy`
runs use that file, not the updater's one-shot overrides.

The updater redeploys both apps, starts them, and runs the Lakebase
permission/bootstrap step. The bootstrap checks the registry first: it adds
`domains.mcp_policy` only when missing and skips owner-only DDL when the
schema is already current. The application also performs the same
idempotent check as its table-owning service principal when the registry is
initialized or first read.

Do not change the app names or DAB target during an in-place update. App names
are immutable, and changing a name under the same Terraform state replaces the
app instead of upgrading it.

To upgrade with `make deploy` instead of the updater, the restored
`deploy.config.sh` must already match the live instance (table above). For a
pre-`INSTANCE_ID` 0.6/0.7 app still tracked as unsuffixed `dev-lakebase`:

```bash
DEFAULT_INSTANCE_ID=<live-suffix> DAB_TARGET=dev-lakebase make deploy
```

### Manual DBA migration

Use this alternative when database changes must be applied separately by a
DBA. Run the migration as the `domains` table owner before or after the code
deployment:

```bash
psql "$PGURL" \
  -v reg_schema=<REGISTRY_SCHEMA> \
  -f scripts/migrations/upgrade_0.7_to_0.8.sql
```

The script enables `ON_ERROR_STOP`, runs inside a transaction, is safe to
repeat, and verifies that the column exists before committing. After it
succeeds, run the normal updater; its migration check will report that the
registry is current and skip DDL:

```bash
scripts/update-deployed-app.sh <UI_APP_NAME> <MCP_APP_NAME>
```

## Post-upgrade verification

1. Verify the registry column and default:

   ```sql
   SELECT column_name, data_type, is_nullable, column_default
   FROM information_schema.columns
   WHERE table_schema = '<REGISTRY_SCHEMA>'
     AND table_name = 'domains'
     AND column_name = 'mcp_policy';
   ```

   Expect one `jsonb`, non-nullable column with an empty-object default.

2. Confirm both apps remain on the expected names and reach `RUNNING`:

   ```bash
   databricks apps get <UI_APP_NAME> -o json
   databricks apps get <MCP_APP_NAME> -o json
   ```

   Recheck after at least one minute because dependency failures may appear
   after the initial successful start response.

3. Confirm both apps retain their SQL Warehouse, Volume, and Postgres resource
   bindings.
4. Request `https://<UI_APP_URL>/healthz`.
5. Open an existing domain and confirm its MCP policy loads. Then invoke a
   read-only MCP tool through the MCP app to verify UI-to-MCP connectivity.

## Rollback

If application verification fails:

1. Check out the previous 0.7.x source.
2. Restore `scripts/deploy.config.sh` from `scripts/deploy.config.sh.pre-0.8`
   (or the copy kept outside the repo).
3. Redeploy with the same app names, DAB target, and resource bindings
   (`scripts/update-deployed-app.sh` or `make deploy` against that restored
   config).

The additive `mcp_policy` column can remain in place because 0.7.x ignores it.
Do not drop the column: doing so would discard any MCP policy saved after the
0.8 upgrade.

Restore the database backup only if registry data was changed or damaged for a
reason unrelated to the additive migration.

## Troubleshooting

- **`must be owner of table domains`:** run the manual migration as the table
  owner or let the 0.8 application service principal perform its lazy,
  ownership-aware upgrade.
- **Schema not found:** open **Settings > Registry > Initialize**, then rerun
  `make bootstrap-lakebase` or the in-place updater.
- **`psql` unavailable:** install the libpq client before the upgrade. On
  macOS: `brew install libpq && brew link --force libpq`.
- **New apps appeared (`ontobricks-08x`):** `deploy.config.sh` was not restored
  after the 0.8 checkout. Copy `scripts/deploy.config.sh.pre-0.8` back, or set
  `DEFAULT_INSTANCE_ID` to the live suffix, and redeploy. Leave the accidental
  08x apps stopped if you do not need them.
- **Resource mismatch:** restore the bindings recorded in the pre-upgrade app
  JSON and restart both apps. Also compare `DEFAULT_WAREHOUSE_ID` /
  `DEFAULT_REGISTRY_*` / `DEFAULT_LAKEBASE_*` against that JSON.
- **MCP cannot reach the UI:** redeploy once the UI URL is available. The
  deployment script resolves `ONTOBRICKS_URL` and regenerates the MCP
  `app.yaml`.
