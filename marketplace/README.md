# Marketplace packaging

Databricks Marketplace installs an app from a **subfolder of a tagged Git
commit** and runs it as-is: no render step, no build step. OntoBricks keeps its
normal repo layout and builds that subfolder at release time.

| File | Purpose |
|---|---|
| `app.yaml` | Fixed Apps config shipped to Marketplace installs (no `${…}` placeholders) |
| `manifest.yaml` | Marketplace manifest: name, description, resource specs |
| `LISTING.md` | Listing description (paste into the provider console) |

Plan, open questions and status: `.planning/marketplace-listing/PLAN.md`.

## Test a v1 in a workspace

This installs the assembled folder exactly the way Marketplace would:
- the fixed `app.yaml`
- resources bound by the names in `manifest.yaml`, and the manifest's `user_api_scopes`
- **none** of the `make deploy` bootstrap steps, so no generated `app.yaml`, no `app-permissions.sh` and no `lakebase-perms.sh`

The manifest can't declare a Lakebase database (Q1), so a real installer binds
one right after install. With `--lakebase-branch`, the helper does that in the
same step.

```bash
make marketplace-test-install ARGS="\
  --profile <cli-profile> \
  --warehouse-id <sql-warehouse-id> \
  --lakebase-branch projects/<lakebase-project>/branches/production"
```

Options:
- `--app-name` (default `ontobricks-mp-test`)
- `--lakebase-database` (datname or `db-…` id, default `databricks_postgres`)
- `--dry-run` prints every CLI call and changes nothing
- `--grant-self` also grants the app's service principal `CAN_MANAGE` on itself, which is what `make deploy` does
- Omit `--lakebase-branch` to see how the app behaves with no Lakebase bound

The source is uploaded to
`/Workspace/Users/<you>/ontobricks-marketplace-test/<app-name>` (that folder is
replaced on every run). Re-running against an app this helper created replaces
its resources and redeploys. Any other existing app is refused, so a typo in
`--app-name` can't redeploy a real instance. `--grant-self` is never undone, so
once used, that app can no longer answer Q4; test Q4 on a fresh `--app-name`.

The resources must already exist: the warehouse and a Lakebase Autoscaling
project. Use a fresh project (or schema) per test app: the app's service
principal has to create and own the `ontobricks_registry` schema, and it can't
take over one another principal created.

Then check:
1. The app starts, and `/health` does not report `status: error`.
2. **Settings → Registry → Initialize** succeeds. It runs as the app's service principal, which creates and owns the registry schema.
3. A second, **non-admin** user with `CAN_USE` can open the app.
4. `POST /settings/save` with `{"token": "x"}` returns 400. The app never accepts a PAT in Apps mode.

### 0.10.x port (2026-11-12)

- **No Volume.** 0.10.x keeps no domain artefacts on a UC Volume (`8fad03ff`), so the manifest binds only the SQL warehouse.
- **No Postgres in the manifest.** The SDK's `AppManifestAppResourceSpec` has no postgres/database spec, so Lakebase becomes post-install step 1 in `LISTING.md` (Q1).
- **`sql` user scope.** It ships via the manifest's `user_api_scopes`, matching `databricks.yml`.
- **Docs payload.** The Help Center set is `docs/*.md` + `docs/images/` + `docs/screenshots/`, mirroring `databricks.yml` `sync` and `.databricksignore`. The bundle is 15 MB.
- **New gap (Q7): no install-time source for `REGISTRY_CATALOG` / `REGISTRY_SCHEMA`.**
  - Apps can't bind a UC schema, and Settings has no input for them.
  - `RegistryCfg.is_configured` needs them before Initialize.
- **Startup fix.** 0.10.0 imports `httpx` at runtime but only listed it in the dev group. This branch moves it to the runtime dependencies (`pyproject.toml` + two lines in `uv.lock`; no download URLs changed).
  - Re-run `uv lock --no-config --default-index https://pypi.org/simple` somewhere with network before merging, to regenerate the lock properly.

### Results: E2 dogfood, 2026-11-12 (`ontobricks-mp-test-010`, 0.10.0 + this branch)

Installed fresh, the Marketplace way: warehouse + `sql` scope, Lakebase bound as the post-install step, no bootstrap scripts. Lakebase project: `ontobricks-mp-test-010`.

- **Starts.** It's the real app, not `run.py`'s failure page (the `httpx` fix works). The warehouse connects over OAuth.
- **Q4 reproduces.** Everyone is locked out, including the installer, even with the `sql` scope. The service principal gets `apps.ruleSets/get` denied, and the forwarded user token gets a 403 on `/api/2.0/permissions/apps/<app>`. Granting the service principal CAN_MANAGE fixes it (post-install step 2).
- **Q7 confirmed.** Readiness says "Registry catalog/schema not fully resolved — set REGISTRY_CATALOG / REGISTRY_SCHEMA". Initialize returns 400 "Registry catalog, schema, and volume must be configured first".
- **Q7 is the only thing blocking Initialize.** With the two vars supplied as a *test-only* deployment `env_vars` override, Initialize succeeds and readiness shows 0 errors.
  - It created the `ontobricks_registry` schema as the service principal, plus a UC Volume `OntoBricksRegistry` in that catalog/schema. So a real installer would also have to let the service principal create objects in their schema.
  - Deployment `env_vars` *replace* `app.yaml`'s env (the warehouse binding was dropped), so that's not usable as a workaround.
- **The location doesn't survive a redeploy without the vars.** After a plain redeploy, readiness is unresolved again. The catalog/schema written to the Lakebase `registries` row is never read back.
  - The reason: `LakebaseAuth.instance_name` finds the project by walking *every* Lakebase project → branch → endpoint to match `PGHOST`. Dogfood has ~2,400 projects, and the walk aborts on the first one it can't read (`projects/chatter-uw2-e-1lmpe1 … not found`).
  - `make deploy` avoids this by setting `LAKEBASE_PROJECT`; a Marketplace install can't. Fix upstream: skip unreadable projects, and ideally resolve the endpoint without a full walk.
- **Credential checks (CSRF token required on POST in 0.10.0):**
  - `/settings/save` with a host returns 400.
  - With a token it returns 200, but the token isn't stored (`/settings/current` shows `token: null`).
  - `/api/v1` with a bogus `X-Databricks-Token` runs as the service principal (200).
- **Still open:** check 3 (non-admin user).

### Results: E2 dogfood, 2026-11-03 (`ontobricks-mp-test`, 0.7 base)

- **The app starts from the fixed `app.yaml` alone.** The warehouse (OAuth), Volume read/write, UC DDL and the Lakebase connection all work. The registry config is resolved purely from the bound Volume.
- **Q4 answered: the install does NOT grant the service principal access to its own app.** Without that grant, every user, including the CAN_MANAGE installer, resolves to role `none` and is sent to access-denied.
  - The service principal gets `apps.ruleSets/get` denied, and the installer's forwarded token gets a 403 from the app permissions API.
  - Granting the service principal CAN_MANAGE on the app fixes it, so this is now post-install step 1 in `LISTING.md`.
  - Q4 is still open with the Marketplace team: can the platform do this grant automatically?
- **Q3 answered: no `user_api_scopes` needed.** Effective scopes default to `iam.access-control:read` and `iam.current-user:read` even with none declared.
- **Initialize after the grant:**
  - It succeeded, creating the `ontobricks_registry` schema as the service principal and granting it USAGE and DML.
  - It couldn't grant CAN_USE on the Lakebase project. That wasn't needed: the bound `postgres` resource was enough to connect.
  - It skipped the MCP app, which isn't part of v1.
- **Check 4:** `POST /settings/save` returns 400 for both `token` and `host`. `/api/v1` ignores a bogus `X-Databricks-Token` and runs as the service principal.
- **Health after Initialize:** 0 errors. Two warnings: no graph schema yet (build a knowledge graph), and Lakebase Accelerated Sync isn't available in dogfood.
- **Still open:** check 3 needs a second, non-admin user on the test app.
- **Error hints:** the ones in `/health` and on the access-denied page still name bootstrap scripts that aren't shipped in the bundle.

## Cutting a Marketplace release
1. Cut the normal GitHub release `vX.Y.Z` on `main`.
2. `make marketplace-bundle` builds `dist/marketplace/ontobricks/`.
3. Commit that folder to the `marketplace` branch and tag it `marketplace-vX.Y.Z`. The CI workflow for this is pending open question Q2.
4. Update the listing to the new tag in the provider workspace (SOP step 6).

## Changing app config
Edit `app.yaml.template` **and** `marketplace/app.yaml`.
`tests/units/scripts/test_marketplace_package.py` fails until they agree, or
until a deploy-only var is added to `DEPLOY_ONLY_ENV` with a reason.

`test_manifest_has_no_resource_type_the_sdk_cannot_express` keeps the manifest
inside what the SDK's `AppManifest` model can express. If the Marketplace team
adds a Lakebase spec (Q1), add it there and move Lakebase back to install time.
