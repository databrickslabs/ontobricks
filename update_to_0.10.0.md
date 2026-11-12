# Updating OntoBricks to 0.10.0

This guide covers a fresh 0.10.0 install and an in-place upgrade from 0.9.x.
Older lines must reach 0.9 first (`releases/ReleaseNotes_V0.9.0.md` and, for
pre-0.8 registries, `scripts/migrations/upgrade_0.7_to_0.8.sql`).

There is **no 0.9 → 0.10 warehouse registry DDL**. Schema migrations and
service-principal grants are applied automatically by the deployment path
(`make deploy` / `scripts/update-deployed-app.sh` → Lakebase bootstrap). Any
pending pre-0.9 objects are still created idempotently when missing.

## Supported paths

- **Fresh installation:** deploy 0.10.0 to new Databricks Apps and initialize a
  new registry.
- **In-place upgrade:** keep the existing UI and MCP app names, resource
  bindings, registry data, and app URLs while replacing 0.9.x code with 0.10.0.

## Before you start

1. Schedule a maintenance window and stop registry writes.
2. Back up Lakebase (`pg_dump`) and `scripts/deploy.config.sh` **before**
   checking out 0.10.0. Sample defaults move from `09x` to `10x`; a pull can
   overwrite a working 0.9 config and create **new** apps.

   ```bash
   pg_dump "$PGURL" --format=custom --file=ontobricks-before-0.10.dump
   cp scripts/deploy.config.sh scripts/deploy.config.sh.pre-0.10
   databricks apps get <UI_APP_NAME> -o json > ui-app.pre-0.10.json
   databricks apps get <MCP_APP_NAME> -o json > mcp-app.pre-0.10.json
   ```

3. Confirm the CLI workspace and `psql`:

   ```bash
   databricks current-user me
   psql --version
   ```

4. Install the committed 0.10.0 dependency set without rewriting `uv.lock`:

   ```bash
   uv sync --frozen --extra lakebase
   ```

## Fresh 0.10.0 installation

1. Provision warehouse and Lakebase (`scripts/bootstrap/setup-lakebase.sh`).
2. Edit `scripts/deploy.config.sh` for this **new** instance (do not reuse a 0.9 backup).
3. `make deploy-check` then `make deploy`.
4. Confirm SQL warehouse and postgres bindings on both apps.
5. **Settings → Registry → Initialize**, then `make bootstrap-lakebase`.
6. Grant **end users** (not only the app SP) Unity Catalog `SELECT` / `USE` /
   `EXECUTE` on data they will query. UC **reads** run on-behalf-of the
   signed-in user. Builds still use the app service principal.

See `docs/deployment.md` for first-install permissions, including the OBO
section.

## Upgrade an existing 0.9.x deployment

### Keep `scripts/deploy.config.sh`

After checkout, restore the backup if git replaced the file:

```bash
cp scripts/deploy.config.sh.pre-0.10 scripts/deploy.config.sh
```

Keep live 0.9 knobs: `DEFAULT_INSTANCE_ID`, warehouse, UC catalog/schema,
Lakebase project/branch/database/schema, and DAB target. Changing
`DEFAULT_INSTANCE_ID` to `10x` creates `ontobricks-10x` /
`mcp-ontobricks-10x` — a parallel install, not an in-place upgrade.

### Recommended automatic migration

Redeploy with the existing apps. The deployment scripts apply code and any
pending registry SQL automatically:

```bash
scripts/update-deployed-app.sh <UI_APP_NAME> <MCP_APP_NAME>
```

Or, with a restored `deploy.config.sh` that still matches the live instance:

```bash
make deploy
```

`scripts/update-deployed-app.sh` does not rewrite `deploy.config.sh`. It reads
warehouse, Lakebase, and `app.yaml` from the live apps, redeploys both, and
runs Lakebase bootstrap. Bootstrap re-grants the app service principals and
applies pending idempotent registry DDL when needed; there is nothing
0.10-specific beyond that.

Do not change app names or DAB target. App names are immutable; a rename
under the same Terraform state replaces the app.

### After the code is live

- Confirm each user or group that queries Lakehouse data holds the UC grants
  (fail-closed: no silent fallback to the app SP).
- External / non-UI clients: HTTP `/ontology/swrl/*` moved to
  `/ontology/if-then/*`; inference field and phase `swrl` → `if_then`. Legacy
  stored rules still load.
- Health probes that parsed `GET /health` `checks` must use admin-only
  `GET /settings/diagnostics/readiness`. HTTP 200 liveness is unchanged.
- Non-browser clients posting to internal web routes need `/api/v1/` or
  `/graphql/` (CSRF).

## Post-upgrade verification

1. Both apps stay on the expected names and reach `RUNNING`:

   ```bash
   databricks apps get <UI_APP_NAME> -o json
   databricks apps get <MCP_APP_NAME> -o json
   ```

   Recheck after a minute; dependency failures can appear after the first
   start response.

2. Resource bindings (SQL warehouse, postgres) unchanged.
3. `https://<UI_APP_URL>/healthz`.
4. Open an existing domain. Confirm MCP policy still loads, then a read-only
   MCP tool. UC reads should fail closed for users without table grants.

## Rollback

1. Check out the previous 0.9.x source.
2. Restore `scripts/deploy.config.sh.pre-0.10`.
3. Redeploy with the same app names (`scripts/update-deployed-app.sh` or
   `make deploy`).

Restore the database dump only if registry data was damaged for a reason
unrelated to this upgrade.

## Troubleshooting

- **New apps (`ontobricks-10x`):** restore `deploy.config.sh.pre-0.10` (or set
  `DEFAULT_INSTANCE_ID` to the live suffix) and redeploy. Leave accidental 10x
  apps stopped if unused.
- **Empty Graph Explorer / mapping browse for some users:** missing per-user
  UC grants (OBO). The app SP grant is not enough for reads.
- **Broken SWRL API clients:** retarget to `/ontology/if-then/*` and the
  `if_then` inference field/phase.
- **`must be owner of table …`:** run bootstrap as the schema owner, or let
  the app SP perform ownership-aware DDL.
- **MCP cannot reach the UI:** redeploy once the UI URL is available so
  `ONTOBRICKS_URL` is rewritten into the MCP `app.yaml`.
