# Materialized Inference Purge Design

## Goal

Allow builders to remove all application-generated triples from the active
Knowledge Graph without modifying mapped source triples.

The action is available from **Knowledge Graph → Build**, **Inference**, and
**Cohorts** under the label **Purge Inferences**.

## Scope

The purge removes the complete generated-triple companion for the active
domain version:

- triples materialized by reasoning phases;
- cohort entity, metadata, and membership triples.

The UI calls this combined count **materialized inferences**. It always matches
the number of triples the purge operation will remove; reasoning and cohort
triples cannot be counted separately in the current companion schema.

It does not remove:

- mapped source triples;
- saved reasoning or cohort rules;
- standalone Delta tables selected from the Inference materialization panel;
- cohort Unity Catalog output tables.

## Storage Semantics

Lakehouse and Lakebase keep mapped source triples and application-generated
triples in separate physical relations exposed through a union view:

- Lakehouse: `_data` + `_inferred` → `_graph`;
- Lakebase: `_sync` + `__app` → logical graph view.

Purging truncates only the generated relation. The source relation and union
view remain in place.

Neo4j currently stores source and generated triples together without durable
provenance. The endpoint must reject the operation with a safe infrastructure
error rather than risk deleting source data.

## Backend Design

Add a generated-triple purge capability to the graph-store abstraction. The
Lakehouse implementation resolves and truncates the `_inferred` table. The
Lakebase implementation truncates the `__app` companion in both supported
layout modes. Backends that cannot guarantee source preservation report the
capability as unsupported.

Expose a builder-protected DELETE endpoint under the existing
`/dtwin/reasoning` router. The endpoint:

1. resolves the active domain, settings, graph name, and graph store;
2. checks graph-refresh permission through the existing domain Builder gate;
3. counts generated triples before deletion when the backend supports it;
4. purges the generated companion;
5. returns the number of purged triples and the active graph name.

Expected response:

```json
{
  "success": true,
  "graph_name": "sales_V3",
  "purged_count": 124
}
```

Upgrade the existing `GET /dtwin/reasoning/inferred` compatibility endpoint to
return a lightweight live status payload:

```json
{
  "success": true,
  "graph_name": "sales_V3",
  "materialized_inference_count": 124,
  "purge_supported": true,
  "reasoning": {
    "inferred_count": 124,
    "inferred_triples": []
  }
}
```

The nested `reasoning` fields remain for backward compatibility. The endpoint
does not return individual triples because materialized provenance is not
persisted. Neo4j returns a null count with `purge_supported: false`.

Errors use the existing `OntoBricksError` hierarchy and global FastAPI error
handler. An unavailable graph backend raises `InfrastructureError`; an
unsupported backend also fails safely without changing graph data.

## Frontend Design

Create one shared button partial and include it in the Build, Inference, and
Cohorts page headers. The button uses the existing secondary destructive style:

- label: `Purge Inferences`;
- icon: Bootstrap `trash`;
- class: small outlined danger button;
- graph mutation gating: the existing `window.OB.canRefreshGraph()` check.

A shared JavaScript action first calls `GET /dtwin/reasoning/inferred`, then
uses `showConfirmDialog` to display:

- the active graph name;
- the exact number of materialized inferences that will be deleted;
- that the count combines reasoning and cohort triples;
- that all materialized inference and cohort triples will be deleted;
- that mapped source data and external UC/Delta outputs are preserved.

The confirmation button is styled as danger and labelled `Purge`.

After confirmation, the client calls the purge endpoint, disables the action
while the request runs, and reports the result through `showNotification`.
All shared button instances are kept in the same busy state.
On success, stale in-page inference results are cleared and the Knowledge
Graph readiness/status indicator is refreshed through the existing page
refresh hooks where available.

The **Domain → Cockpit** Knowledge Graph card displays the same live count as
`Materialized inferences`. It loads through the lightweight reasoning status
endpoint during initial Cockpit loading and manual refresh. It shows `N/A`
when the backend reports that source-safe purge/count semantics are
unsupported.

## Security and Concurrency

The endpoint requires the domain Builder role, matching existing graph
materialization operations. Frozen-version refresh rules remain governed by
the existing permission dependency.

Companion truncation is idempotent. Repeated requests return zero after the
first successful purge. Buttons remain disabled during a request to prevent
accidental duplicate calls.

## Testing

Tests are added before production changes.

Backend tests cover:

- Lakehouse purges only the inferred companion;
- Lakebase purges only the application companion;
- source relations are never truncated;
- an empty companion returns zero;
- unsupported backends fail without attempting deletion;
- the route requires Builder permission and returns the purge count.

Frontend contract tests cover:

- the shared button appears in all three KG sections;
- all instances invoke the same shared action;
- the action loads the current count before confirmation;
- the confirmation displays the combined count and scope;
- the action uses the standard confirmation dialog;
- graph-refresh permission is checked;
- the request targets the protected purge endpoint;
- success and error notifications are emitted;
- the Cockpit exposes and populates the materialized-inference metric.

The repository unit suite is run with:

```bash
uv run --frozen pytest -q -m "not scenario"
```
