# Configuring the Neo4j password as a Databricks secret

**Settings → Back end → Neo4j** always asks for the Bolt **username** directly, and always resolves the **password** live from a Databricks secret scope/key — there is no plain-text password field in the UI, and no clear-text password is ever persisted to `global_config`.

The app resolves the secret at connect time via the Databricks Secrets REST API (`/api/2.0/secrets/get`), using its own identity — the service principal's OAuth token in the deployed app, or your PAT/CLI profile in local dev. This is the same identity every other Databricks REST call in this codebase already uses (`DatabricksAuth`).

## One-time setup

### 1. Store the password in a workspace secret

Pick (or create) a workspace secret scope, then put the Neo4j password into a key inside it:

```bash
databricks secrets create-scope ontobricks-secrets               # one-off
databricks secrets put-secret  ontobricks-secrets neo4j-password
# Paste the password at the prompt, Ctrl-D to commit.
```

The scope name and key name are free — you'll pick both from dropdowns in Settings.

### 2. Grant the app's identity READ access to the scope

Unlike a Databricks Apps **secret resource** (declarative binding in `app.yaml`, edited in the Apps UI), this is a live API call the app makes itself, so the caller needs an explicit ACL:

```bash
databricks secrets put-acl --scope ontobricks-secrets \
  --principal <app-service-principal-id-or-name> \
  --permission READ
```

Find the app's service principal under **Apps → ontobricks → Authorization** in the Databricks UI, or via `databricks apps get <app-name>`.

For local development, grant `READ` to your own user/CLI-profile identity instead (or reuse the same scope — most workspaces already grant broad read access to the deploying user).

### 3. Configure it in Settings

Open the OntoBricks app: **Settings → Back end → Neo4j**.

1. Fill in the Bolt **URI** and **Username**.
2. Pick the **Secret scope** from the dropdown (only scopes your identity can read appear here — click the refresh icon after granting access in step 2 if it's not showing up yet).
3. Pick the **Secret name** (the key) from the dropdown once a scope is selected.
4. **Save**. Nothing password-shaped is written to `global_config` — only the scope/key names.
5. Click **Test connection** to confirm the app can resolve the password and open a Bolt session.

## Local development

Local dev goes through the exact same path as the deployed app — no plain-text fallback. Your local Databricks auth (`DATABRICKS_TOKEN`, or a `databricks auth login` CLI profile) is used to call the Secrets API, so make sure that identity has `READ` on the chosen scope (step 2 above).

## Legacy: `NEO4J_PASSWORD` env var / Apps secret resource

Deployments that already bind the `neo4j-password` Apps secret resource (`app.yaml` `valueFrom`) keep working unmodified — the backend still supports `auth_method: "basic"` and prioritizes the `NEO4J_PASSWORD` env var when it's set. This path is **not exposed in the Settings UI anymore**; it only remains for existing configs that haven't been migrated. To migrate, open Settings → Neo4j and Save once with a scope/key picked — this overwrites `auth_method` to `"databricks_secret"`.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Secret scope dropdown is empty | Identity has no `READ` ACL on any scope | Grant it: `databricks secrets put-acl --scope <scope> --principal <principal> --permission READ`, then click the refresh icon |
| `InfrastructureError: Databricks secrets/get denied (403)` on Test connection | Scope is visible in `list-scopes` but `READ` wasn't actually granted (or was granted to the wrong principal) | Re-run `put-acl` with the exact app service-principal ID from **Apps → Authorization** |
| `Secret '<scope>/<key>' not found (404)` | Key was renamed/deleted after being selected | Re-open Settings → Neo4j, pick the current key from the refreshed dropdown |
| Connection succeeds locally but fails in the deployed app | Your local identity has `READ` on the scope, the app's service principal does not | Grant `READ` to the app's service principal too (step 2) |

## Why this design

- **Zero clear-text credential in `global_config`** — the save endpoint always strips `password` when `auth_method == "databricks_secret"`.
- **No redeploy to change the password** — rotating the secret value in the workspace takes effect within `_SECRET_VALUE_CACHE_TTL_SECONDS` (5 minutes) of the next connection, no Apps resource rebind and no bundle redeploy.
- **One flow everywhere** — local dev and the deployed app resolve the password the same way, through the same `DatabricksAuth` identity resolution every other Databricks call in this codebase uses.
- **Least surprise on permissions** — a 403 from the Secrets API maps to a plain-English "grant READ to this principal" message instead of a raw HTTP error.
