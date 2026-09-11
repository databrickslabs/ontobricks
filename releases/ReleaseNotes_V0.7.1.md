# OntoBricks — Release Notes V0.7.1

**Release window:** August, 2026
**Type:** Patch release (v0.7.0 → v0.7.1)
**Test status:** all changes shipped with the suite green (4798 passed, 276 skipped, 5 deselected, 1 xfailed).

---

## Summary

v0.7.1 is a focused **deploy / first-time onboarding** patch on top of v0.7.0.
It clears five GitHub issues that blocked or confused new Databricks Apps
installs: broken bootstrap script paths after the `scripts/bootstrap/` reorg,
incorrect DAB app-permission nesting, stale Lakebase segment docs, a forced
Neo4j secret binding that most workspaces do not have, and Registry → Initialize
looking up the wrong MCP app name on instance-suffixed deploys.

No product-feature changes. No schema migrations. No breaking API changes.

---

## Highlights

- **Bootstrap scripts find the repo root again ([#133](https://github.com/databrickslabs/ontobricks/issues/133))** —
  `setup-lakebase.sh`, `app-permissions.sh`, and `lakebase-perms.sh` `cd` to the
  repository root after the `scripts/bootstrap/` move so `deploy.config.sh` and
  sibling helpers resolve correctly.
- **DAB app permissions nesting ([#134](https://github.com/databrickslabs/ontobricks/issues/134))** —
  `CAN_MANAGE` / `CAN_USE` for both apps live under `resources.apps.*` in
  `databricks.yml` (bundle top-level permissions are invalid / ignored).
- **Lakebase deploy-config docs ([#135](https://github.com/databrickslabs/ontobricks/issues/135))** —
  documentation matches auto-resolved `LAKEBASE_DATABASE_RESOURCE_SEGMENT` and
  the current `DEFAULT_LAKEBASE_DATABASE` / `_SCHEMA` knobs.
- **Neo4j secret no longer required at deploy ([#136](https://github.com/databrickslabs/ontobricks/issues/136))** —
  DAB no longer binds `neo4j-password` / `ontobricks-secrets`; Neo4j stays
  optional via Settings → Neo4j (Secrets API).
- **MCP SP + UC grants on first Initialize ([#137](https://github.com/databrickslabs/ontobricks/issues/137))** —
  registry/graph grants derive `mcp-{APP_NAME}` (e.g. `mcp-ontobricks-07x`);
  `MCP_APP_NAME` is injected into `app.yaml`; UC catalog `ALL_PRIVILEGES` runs
  in `lakebase-perms.sh` **before** the schema-existence guard so first deploy
  still applies them.

---

## Bug Fixes

### Bootstrap path after `scripts/bootstrap/` reorg ([#133](https://github.com/databrickslabs/ontobricks/issues/133))

After bootstrap scripts moved under `scripts/bootstrap/`, relative `cd` /
`source` paths no longer reached the repo root. First-time Lakebase setup and
permission grants failed or ran against the wrong working directory.

- `scripts/bootstrap/setup-lakebase.sh`
- `scripts/bootstrap/app-permissions.sh`
- `scripts/bootstrap/lakebase-perms.sh`

### DAB app-level permissions nesting ([#134](https://github.com/databrickslabs/ontobricks/issues/134))

App ACL entries must be nested under each app resource. Top-level bundle
`permissions` do not apply the intended `CAN_USE` / `CAN_MANAGE` grants.

- `databricks.yml` — nest permissions under `resources.apps.ontobricks_dev_app`
  and `resources.apps.mcp_ontobricks_app`
- `documentation/deployment.md` — document the correct nesting

### Stale Lakebase segment / deploy.config variable names ([#135](https://github.com/databrickslabs/ontobricks/issues/135))

Docs still described a manually set `DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT`
after deploy started auto-resolving the `db-…` segment from the Postgres API.

- `documentation/deployment.md`
- `documentation/lakebase-graphdb.md`
- `scripts/bootstrap/setup-lakebase.sh` help text

### Neo4j secret binding aborted first deploy ([#136](https://github.com/databrickslabs/ontobricks/issues/136))

`./scripts/deploy.sh` failed when the workspace lacked
`ontobricks-secrets` / `neo4j-password`, even though Neo4j is optional and
configured in-app via the Secrets API.

- `databricks.yml` — remove `neo4j_secret_scope` and the `neo4j-password` overlay
- `scripts/_internal/_ensure-instance-target.sh` — stop emitting the secret for
  generated `dev-lakebase-<id>` targets
- `scripts/deploy.config.sh` / `scripts/deploy.sh` — drop the secret-scope
  preflight / `--var`
- `app.yaml.template` — clarify the unbound resource is legacy-only

### MCP companion SP skipped on instance-suffixed apps ([#137](https://github.com/databrickslabs/ontobricks/issues/137))

With `INSTANCE_ID=07x`, deploy creates `mcp-ontobricks-07x`, but Registry →
Initialize looked up bare `mcp-ontobricks` and skipped MCP grants. Separately,
UC catalog grants in `lakebase-perms.sh` ran only after the schema guard, so a
first deploy (schema not yet created) never granted catalog `ALL_PRIVILEGES`;
in-app re-grants as the app SP then warned about missing `MANAGE`.

- `src/back/core/databricks/lakebase/grants.py` — `resolve_mcp_app_name`
  (`mcp-{APP_NAME}`); clearer CAN_USE / UC warning text
- `src/back/objects/domain/SettingsService.py` — registry + graph provision use
  the derived MCP name
- `app.yaml.template` — inject `MCP_APP_NAME` at deploy time
- `scripts/bootstrap/lakebase-perms.sh` — UC catalog grants before schema guard
- `src/front/templates/settings.html` — blank MCP field derives `mcp-{main app}`

---

## Documentation

- Deployment guide: bootstrap paths, app ACL nesting, Lakebase variable names,
  Neo4j-optional deploy, MCP naming (`mcp-${APP_NAME}`), first-deploy UC grant
  timing, and why elevating the app SP to `CAN_MANAGE` is the wrong fix for
  Initialize warnings.
- Lakebase Graph DB guide aligned with auto-resolved database resource segment.
- Neo4j secret-configuration note: DAB does not bind Neo4j secrets.

---

## Upgrade notes

- **From 0.7.0:** pull `0.7.1` and re-run `./scripts/deploy.sh` (or `make deploy`).
  No registry or graph rebuild required for this patch alone.
- **Existing instance-suffixed apps (`ontobricks-07x` / `mcp-ontobricks-07x`):**
  after deploy, open **Settings → Registry → Repair permissions** (or Initialize
  again) so MCP grants target `mcp-ontobricks-07x`. If UC catalog privileges are
  still missing, re-run as a workspace admin:

  ```bash
  source scripts/deploy.config.sh
  scripts/bootstrap/lakebase-perms.sh \
    -i "$LAKEBASE_PROJECT" -b "$LAKEBASE_BRANCH" \
    -d "$LAKEBASE_DATABASE" -s "$LAKEBASE_SCHEMA" \
    -c "$REGISTRY_CATALOG" \
    -a "$APP_NAME" -a "$MCP_APP_NAME"
  ```

- **Do not** elevate the app service principal to Lakebase `CAN_MANAGE` or
  catalog `MANAGE` just to silence Initialize warnings — grant `ALL PRIVILEGES`
  **to** the SP from an admin principal via the bootstrap script (or SQL) instead.
- **Neo4j:** if you previously relied on a DAB-bound `neo4j-password` resource,
  configure the connection under **Settings → Neo4j** with a Databricks secret
  scope/key (unchanged product path).
- **New deploys:** use `./scripts/deploy.sh` end-to-end; bootstrap scripts now
  resolve correctly from `scripts/bootstrap/`.

---

## Issues closed

| Issue | Title |
|-------|--------|
| [#133](https://github.com/databrickslabs/ontobricks/issues/133) | Lakebase setup / bootstrap path after scripts reorg |
| [#134](https://github.com/databrickslabs/ontobricks/issues/134) | DAB app permissions nesting |
| [#135](https://github.com/databrickslabs/ontobricks/issues/135) | Stale Lakebase deploy.config variable docs |
| [#136](https://github.com/databrickslabs/ontobricks/issues/136) | Deploy requires missing Neo4j secret |
| [#137](https://github.com/databrickslabs/ontobricks/issues/137) | MCP SP unresolved / UC grants after registry init |
