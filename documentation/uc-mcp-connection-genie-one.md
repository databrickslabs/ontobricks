# Register OntoBricks MCP in Unity Catalog and add it to Genie One

This procedure registers the deployed OntoBricks MCP Databricks App as a
**Unity Catalog HTTP connection** (with an optional **MCP Service**), then
attaches that connection to a **Genie One** chat so Genie can call OntoBricks
tools (`list_domains`, `select_domain`, `describe_entity`, GraphQL, …).

Genie One will not list a custom MCP until the Unity Catalog connection exists.
Create the connection first, then add it to the conversation.

Official Databricks references:

- [Connect to external HTTP services](https://docs.databricks.com/aws/en/query-federation/http)
- [Register an external MCP server](https://docs.databricks.com/aws/en/ai-gateway/register-mcp-service)
- [Connect to external tools and sources (Genie One)](https://docs.databricks.com/aws/en/genie-one/external-sources)

---

## What you end up with

```
Genie One chat  (or AI Playground / Genie Code)
        │
        │  Unity Catalog HTTP proxy  (managed credentials)
        ▼
UC connection  ontobricks_mcp_<instance>
        │
        │  Streamable HTTP  POST …/mcp
        ▼
mcp-ontobricks-<instance>   Databricks App
        │
        │  ONTOBRICKS_URL
        ▼
ontobricks-<instance>       main FastAPI app + graph viewer
```

Databricks recommends **OAuth M2M** for custom MCP connections used from
Genie One. User-to-machine (U2M) OAuth works but needs an extra OAuth app and
redirect URI. This procedure uses M2M.

---

## Prerequisites

| Item | Why |
|------|-----|
| Databricks CLI ≥ 0.229, profile pointing at the **same workspace** as the MCP app | All API calls below |
| `CREATE CONNECTION` on the metastore (workspace admins have it on auto-enabled UC workspaces) | Create the HTTP connection |
| `USE CATALOG` / `USE SCHEMA` / `CREATE SERVICE` on a catalog.schema you own | Optional MCP Service |
| MCP app running, name starting with `mcp-` | Streamable HTTP at `/mcp` |
| **CAN_USE** on that MCP app (you, and the service principal created below) | Token is accepted by Apps |
| Model Serving region + **Third Party Connectors for Agents** preview | Genie One custom MCP picker |
| Domains with **API / MCP** enabled in OntoBricks | Otherwise `list_domains` is empty |

Confirm the MCP app:

```bash
PROFILE=DEFAULT          # your CLI profile
MCP_APP=mcp-ontobricks-08x

databricks apps get "$MCP_APP" -p "$PROFILE" --output json \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['url'], d['app_status']['state'])"
```

The MCP endpoint is always:

```text
https://<mcp-app-url>/mcp
```

Example: `https://mcp-ontobricks-08x-2508734981122804.aws.databricksapps.com/mcp`

---

## Variables

Fill these once; every command below uses them.

```bash
export PROFILE=DEFAULT
export WORKSPACE_HOST=https://fe-vm-bcayla-demos.cloud.databricks.com   # no trailing slash
export MCP_APP=mcp-ontobricks-08x
export MCP_APP_URL=https://mcp-ontobricks-08x-2508734981122804.aws.databricksapps.com
export CONN_NAME=ontobricks_mcp_08x
export SP_DISPLAY_NAME=ontobricks-mcp-08x-uc-connection
export SP_APP_ID=   # filled in step 1

# Optional MCP Service (AI Gateway / Playground three-level name)
export UC_CATALOG=benoit_cayla
export UC_SCHEMA=ontobricks_demo_08x_sc
export MCP_SERVICE_ID=ontobricks_mcp
```

---

## 1. Create a service principal and OAuth secret

Genie One injects this principal’s token into requests to the MCP app.
Do **not** reuse the MCP app’s own service principal.

```bash
databricks service-principals create \
  --display-name "$SP_DISPLAY_NAME" --active \
  -p "$PROFILE" --output json

# id = numeric SCIM id (for secrets). applicationId = OAuth client_id.
eval "$(
  databricks service-principals list -p "$PROFILE" --output json \
    | SP_DISPLAY_NAME="$SP_DISPLAY_NAME" python3 -c "
import json,sys,os
name=os.environ['SP_DISPLAY_NAME']
for s in json.load(sys.stdin):
    if s.get('displayName')==name:
        print('SP_ID='+s['id'])
        print('SP_APP_ID='+s['applicationId'])
        break
"
)"
export SP_ID SP_APP_ID
echo "SP_ID=$SP_ID SP_APP_ID=$SP_APP_ID"
```

Create an OAuth secret (730 days). The secret is shown **once**.

```bash
databricks service-principal-secrets-proxy create "$SP_ID" \
  --lifetime 63072000s -p "$PROFILE" --output json \
  > /tmp/ontobricks_sp_secret.json
# Keep that file out of git. Extract:
python3 -c "import json; print(json.load(open('/tmp/ontobricks_sp_secret.json'))['secret'])"
# Store it as CLIENT_SECRET in your shell only.
```

---

## 2. Grant the principal CAN_USE on the MCP app

```bash
databricks apps update-permissions "$MCP_APP" -p "$PROFILE" --json "$(
python3 -c "
import json,os
print(json.dumps({'access_control_list':[{
  'service_principal_name': os.environ['SP_APP_ID'],
  'permission_level': 'CAN_USE'}]}))
")"
```

`users` already having CAN_USE is not enough if the principal is not in that
group. Grant it explicitly.

---

## 3. Create the Unity Catalog HTTP connection

`is_mcp_connection=true` marks the connection for MCP / Genie One / Playground.

OAuth token endpoint is the **workspace** OIDC token URL, not the Apps URL.

```bash
python3 <<'PY'
import json, os
secret = json.load(open("/tmp/ontobricks_sp_secret.json"))["secret"]
body = {
  "name": os.environ["CONN_NAME"],
  "connection_type": "HTTP",
  "comment": "OntoBricks MCP Databricks App (streamable HTTP /mcp) for Genie One.",
  "read_only": True,
  "options": {
    "host": os.environ["MCP_APP_URL"],
    "port": "443",
    "base_path": "/mcp",
    "client_id": os.environ["SP_APP_ID"],
    "client_secret": secret,
    "oauth_scope": "all-apis",
    "token_endpoint": os.environ["WORKSPACE_HOST"].rstrip("/") + "/oidc/v1/token",
    "is_mcp_connection": "true",
  },
}
open("/tmp/ontobricks_uc_conn.json", "w").write(json.dumps(body))
PY

databricks connections create -p "$PROFILE" --json @/tmp/ontobricks_uc_conn.json --output json
rm -f /tmp/ontobricks_uc_conn.json /tmp/ontobricks_sp_secret.json
```

Success looks like:

- `connection_type`: `HTTP`
- `credential_type`: `OAUTH_M2M`
- `provisioning_info.state`: `ACTIVE`
- `options.access_token_expiration` present (token exchange worked)
- `url`: `https://<mcp-app-url>:443/mcp`

### UI alternative (same result)

1. Catalog → **+** → **Create a connection**.
2. Type **HTTP**. Auth **OAuth Machine to Machine**.
3. Host = MCP app URL (no `/mcp`). Port `443`. Base path `/mcp`.
4. Client ID = service principal `applicationId`. Client secret = OAuth secret.
5. Token endpoint = `https://<workspace-host>/oidc/v1/token`.
6. Scope `all-apis`. Enable the MCP-connection flag if the form shows it.
7. Save. Confirm the connection is **Active**.

---

## 4. (Optional) Register an MCP Service

Needed for **AI Gateway** governance and Playground **External MCP servers**.
Genie One’s picker uses the **connection** name; the MCP Service is still useful
for grants and tool selection.

```bash
databricks api post \
  "/api/2.1/unity-catalog/mcp-services?parent=schemas/${UC_CATALOG}.${UC_SCHEMA}&mcp_service_id=${MCP_SERVICE_ID}" \
  --json "{
    \"comment\": \"OntoBricks MCP companion server\",
    \"config\": {
      \"source_connection\": { \"name\": \"connections/${CONN_NAME}\" },
      \"include_tool_selectors\": []
    }
  }" \
  -p "$PROFILE"
```

An empty `include_tool_selectors` exposes every tool. The three-level name is:

```text
<catalog>.<schema>.<mcp_service_id>
```

Example: `benoit_cayla.ontobricks_demo_08x_sc.ontobricks_mcp`

---

## 5. Grants

| Who | Privilege | On |
|-----|-----------|----|
| You (already owner) | — | connection + MCP Service |
| Colleagues who will use Genie One | `USE CONNECTION` | the HTTP connection |
| Colleagues who will invoke via AI Gateway | `EXECUTE` | the MCP Service |
| Do **not** grant `USE CONNECTION` to end users if you only want them going through the MCP Service | — | they could bypass tool policies |

SQL (warehouse with UC):

```sql
GRANT USE CONNECTION ON CONNECTION ontobricks_mcp_08x TO `user@example.com`;
GRANT EXECUTE ON <catalog>.<schema>.ontobricks_mcp TO `user@example.com`;
```

---

## 6. Add the MCP server in Genie One

This is the step that actually makes OntoBricks tools available in a chat.

### 6.1 Enable the preview (workspace admin, once)

1. Click your user menu → **Previews**.
2. Turn on **Third Party Connectors for Agents** (required for Genie One
   external / custom MCP connections).
3. If Genie One itself is missing, also enable **Genie One** / **Chat in Genie
   One** for the workspace.

Custom MCP connections only work in regions that support **Model Serving**.

### 6.2 Attach the connection to a conversation

1. Open **Genie One** (workspace home / Genie One entry point — not a classic
   Genie Agent / former Genie Space).
2. On the Genie One home page, click the **+** (plus) at the **bottom left of
   the search bar**.
3. Pick a built-in source if you need one, or click **More connections**.
4. Choose **custom MCP** / Unity Catalog connection.
5. Select the connection you created (`ontobricks_mcp_08x` in the example).
   - If it is missing: the connection is not `HTTP` + `is_mcp_connection`, or
     you lack `USE CONNECTION`, or it is still provisioning.
6. Click **Sign in** only if the connection uses per-user OAuth (U2M).
   **M2M connections skip this** — Databricks already holds the client secret.
7. Confirm the connection appears on the conversation (chip / attached source).

Databricks rule: **the Unity Catalog connection must exist before you can add
it to a Genie One chat.** You cannot create it from the plus menu alone.

### 6.3 Check that Genie will actually call the tools

Genie One does not always pick a custom MCP on the first message. Prompt it
explicitly until you see tool traces:

```text
Use the OntoBricks MCP server. Call list_domains and list every domain.
```

Then:

```text
Select the domain <name> and describe entity types in the graph viewer.
```

```text
In domain <name>, tell me about <entity>.
```

If search “doesn’t start”, Databricks’s own guidance is to name the tool or
source in the prompt (`use OntoBricks`, `use list_domains`).

### 6.4 What “good” looks like

- The conversation lists the OntoBricks connection as an attached source.
- A question about domains triggers `list_domains` (only API/MCP-enabled
  domains appear).
- A follow-up that names a person or ID triggers `select_domain` then
  `describe_entity` or GraphQL.
- Errors that mention 401 / Apps OAuth almost always mean the M2M principal
  lacks **CAN_USE** on `mcp-ontobricks-*`.
- Empty domain lists mean the registry flag **API / MCP** is off, or
  `ONTOBRICKS_URL` on the MCP app is wrong.

---

## 7. Optional: same connection in Playground and Genie Code

**AI Playground**

1. Playground → model with **Tools**.
2. **Tools → + Add tool → MCP Servers → External MCP servers**.
3. Select the MCP Service (`catalog.schema.ontobricks_mcp`) or the HTTP
   connection, depending on the picker.

**Genie Code** (coding agent, not Genie One chat)

1. Open the Genie Code pane → **Settings**.
2. **MCP Servers → Add Server**.
3. Either:
   - **External MCP server** → the Unity Catalog connection (login first if U2M), or
   - **Custom MCP server** → the Databricks App `mcp-ontobricks-*` directly
     (same workspace, endpoint `/mcp`, stateless HTTP). Same-workspace Apps
     do not need the UC connection; Genie One custom MCP **does**.

---

## 8. Worked example (08x sandbox)

These objects were created on workspace `fe-vm-bcayla-demos` for the running
app `mcp-ontobricks-08x`:

| Object | Name |
|--------|------|
| MCP app URL | `https://mcp-ontobricks-08x-2508734981122804.aws.databricksapps.com` |
| HTTP connection | `ontobricks_mcp_08x` (OAuth M2M, `/mcp`, Active) |
| Service principal | `ontobricks-mcp-08x-uc-connection` |
| MCP Service | `benoit_cayla.ontobricks_demo_08x_sc.ontobricks_mcp` |

Reuse them in Genie One: **+** on the search bar → **More connections** →
`ontobricks_mcp_08x`.

---

## 9. Troubleshooting

| Symptom | Check |
|---------|--------|
| Connection create fails on token endpoint | `WORKSPACE_HOST` must be the workspace URL (`https://….cloud.databricks.com/oidc/v1/token`), not the Apps hostname |
| `ACTIVE` but Genie One 401 | `databricks apps get-permissions $MCP_APP` — principal `applicationId` must have CAN_USE |
| Connection missing in Genie One | `is_mcp_connection` true; you have `USE CONNECTION`; preview **Third Party Connectors for Agents** on; Model Serving region |
| Tools never run | Prompt with the server/tool name; confirm domains have API/MCP enabled |
| U2M “must log in” | Expected for per-user OAuth. For Genie One, prefer M2M (this procedure) |
| Schema-level `parent` ignored by `connections create` | Current CLI treats HTTP connections as metastore-level; name is `connections/<CONN_NAME>`. MCP Services still live under a schema |

---

## 10. Security notes

- The OAuth client secret lives only in Unity Catalog. Do not put it in
  `app.yaml`, `.env`, or this repo.
- M2M means **every Genie One user shares the service principal identity**
  toward the MCP app. OntoBricks domain ACL still applies inside the app using
  that identity. For per-user graph ACL, switch the connection to OAuth U2M
  Per User (extra OAuth app + redirect
  `https://<databricks-region>.cloud.databricks.com/api/2.0/http/oauth/redirect`).
- Do not grant `USE CONNECTION` broadly if you rely on MCP Service tool
  selectors and policies.
