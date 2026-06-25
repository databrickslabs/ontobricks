# Configuring the Neo4j password as a Databricks Apps secret

The Neo4j Bolt password is sourced at runtime from the `NEO4J_PASSWORD` environment variable, populated by a Databricks Apps **secret resource** declared in `app.yaml`. The deployed app refuses to instantiate `Neo4jStore` if the variable is missing — no clear-text password ever lives in `global_config`.

## One-time setup

### 1. Store the password in a workspace secret

Pick (or create) a workspace secret scope, then put the Neo4j password into a key inside it:

```bash
databricks secrets create-scope ontobricks               # one-off
databricks secrets put-secret  ontobricks neo4j-password
# Paste the password at the prompt, Ctrl-D to commit.
```

The scope name and key name are free — only the binding in step 2 matters.

### 2. Bind the secret to the app's `neo4j-password` resource

The bundle's `app.yaml.template` already declares the resource:

```yaml
resources:
  - name: neo4j-password
    secret:
      permission: READ
```

After `make deploy`, open the deployed app in the Databricks UI:

1. **Apps → ontobricks → Resources**
2. Locate the row `neo4j-password` (status will be **Unbound**).
3. Click **Edit** → pick **Secret** → fill `Scope = ontobricks`, `Key = neo4j-password` (or whatever you used in step 1).
4. Save. The status flips to **Bound**.

The app does not need a redeploy after binding — the platform re-injects `NEO4J_PASSWORD` on the next request.

### 3. Verify

Open the OntoBricks app: **Settings → Triple store → Neo4j**. The **Password** field shows a green badge **From Apps secret** and the input is disabled. Save the engine config — any persisted clear-text `password` in `global_config` is stripped server-side at save time.

## Local development

When `DATABRICKS_APP_PORT` is unset (running on your laptop), the password falls back to `engine_config.password` from the Settings UI. This path is **disabled in the deployed app** — the runtime check uses the platform-injected `DATABRICKS_APP_PORT` variable to detect prod.

To run locally against an Aura instance, either:

- Set `NEO4J_PASSWORD` in your `.env` (so the secret path is exercised in dev too), or
- Leave it unset and enter the password once in Settings → Neo4j (persisted to your local `global_config`).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| App logs `InfrastructureError: NEO4J_PASSWORD env var is required` at first build | Resource bound but not propagated, **or** wrong scope/key | Re-open Resources, re-bind, ensure scope+key exist via `databricks secrets list-secrets <scope>` |
| Settings badge stays **Local-dev fallback** in deployed app | Resource is unbound | Bind it via Apps → Resources (step 2) |
| Connection succeeds locally but fails in deployed app | Local `.env` has the right password, prod secret does not | Verify the value: `databricks secrets get-secret <scope> <key>` |

## Why this design

- **Zero clear-text credential in `global_config`** — the save endpoint strips `password` whenever `NEO4J_PASSWORD` is set.
- **Declarative** — the binding is part of the app's resources, reviewable in the Apps UI, with audit trails on the secret scope.
- **No runtime call to the Secrets API** — the platform injects the value at startup; the app code reads a plain env var.
- **No per-user secrets** — the binding is at the app level (service principal), consistent with the Lakebase OAuth pattern (`LakebaseAuth`).
