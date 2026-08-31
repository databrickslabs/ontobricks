# `ai_parse_document` prerequisite for PDF-grounded ontology generation

OntoBricks' **Generate Ontology** wizard (and the auto-mapping LLM agent) can ingest binary documents — **PDF**, Office, and image files — uploaded to the domain's UC Volume `documents/` directory. The agent calls a tool named `read_document` that under the hood invokes Databricks' built-in [`ai_parse_document`](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document) SQL function via the app's bound SQL warehouse.

If the SQL warehouse cannot run `ai_parse_document`, the tool returns:

```json
{
  "filename": "your-file.pdf",
  "error": "Binary document could not be parsed. A SQL warehouse with ai_parse_document access is required …"
}
```

The agent **gracefully falls back** to filename-only inference — it can still generate a plausible ontology from the filename + any plain-text / `.md` / `.csv` siblings in the volume, but it will not see the actual PDF content. This is the regime that produced the v0.7 demo's 38-class ontology when the PDF parse failed: the LLM read `AV-TR-2026-001_MMSF-PFAS-Risk-Assessment.pdf` as a filename, recognized "MMSF" (Mixed Media Sand Filter) and "PFAS Risk Assessment", and produced a sensible class hierarchy from domain priors alone.

## Symptom checklist

You're hitting this if:
- The Generate task completes with `Generated N classes, 0 properties` and the task `agent_steps` log shows `read_document` returning the error above.
- Your domain `documents/` folder contains a PDF that the wizard never appears to have "read" in its summary.
- `databricks apps logs <app>` shows `tool_read_document: '...' is a binary document but could not be parsed`.

## Fix

Two paths.

### Option A — grant the app's service principal access to `ai_parse_document`

```bash
# Grant USE CATALOG on system + ALL on system.ai to the app SP
databricks sql query --warehouse-id <wh-id> "
  GRANT USE CATALOG ON CATALOG system TO \`<app-service-principal-uuid>\`;
  GRANT USE SCHEMA, EXECUTE ON SCHEMA system.ai TO \`<app-service-principal-uuid>\`;
"
```

The app SP UUID is the `application_id` field on the App resource (visible via `databricks apps get <app-name>` → `service_principal.application_id`).

### Option B — bind a SQL warehouse that has `ai_parse_document` enabled at workspace level

Some workspaces enable `ai_parse_document` per warehouse via the **Settings → Compute → SQL Warehouses → \<wh\> → AI functions** toggle. Pick a warehouse where that toggle is on and re-bind the `sql-warehouse` Apps resource:

1. Databricks UI → **Apps → ontobricks → Resources → sql-warehouse → Edit**
2. Pick the warehouse with `ai_parse_document` enabled
3. Save (auto-injects `DATABRICKS_SQL_WAREHOUSE_ID` env var into the app)

No redeploy needed — the new binding propagates on the next app request.

## Verification

After fixing, re-run **Generate Ontology** and check the task result:

```bash
DATABRICKS_CONFIG_PROFILE=<your-profile> databricks apps logs <app-name> \
  | grep "tool_read_document\|ai_parse_document"
```

You should see `parsed '...pdf' → <N> chars via ai_parse_document` instead of the binary-document error. The Generated ontology will now have **properties / relationships** (not just classes) — and the GraphQL Playground will load a real schema instead of the v0.7 "GraphQL not ready — no properties" friendly state.

## Why this matters for the v0.7 demo

The v0.6 demo (2026-06-12) had a workspace warehouse with `ai_parse_document` enabled — Benoit's `d2096aa075ad44a3`. That run produced 32 classes + 13 properties from the PFAS PDF, and the GraphQL Playground showed a populated schema.

The v0.7 re-deploy on FEVM-Mjolnir used `8ea372c75c4a5251` (the only available warehouse), which does **not** have `ai_parse_document` enabled, so the agent fell back to filename-only inference: 38 classes, 0 properties. The OntoBricks core code is identical between v0.6 and v0.7; the difference is purely environmental, and the new error-message + this doc make the prerequisite explicit.

**Related:** [[secret-configuration.md]] for the Neo4j password setup (sibling Apps-secret-resource prerequisite).
