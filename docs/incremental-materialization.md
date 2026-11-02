# Incremental triple materialization

Every knowledge-graph build used to be a full rebuild: the R2RML gateway VIEW was
recomputed, `…_data` was rewritten by CTAS, and the graph store (Lakebase, Neo4j)
was dropped and streamed again. A domain now has a **materialization mode** that
decides how much of that work is repeated. The mode is a per-domain setting
(`materialization_mode`, shown in the domain information panel and editable in
Settings) and defaults to `full`, so existing domains behave as before.

<!-- toc -->
**Contents**

- [Modes](#modes)
- [Readiness and the fallback ladder](#readiness-and-the-fallback-ladder)
- [What a build does per backend](#what-a-build-does-per-backend)
- [Scheduled builds](#scheduled-builds)
- [Lakehouse//RT](#lakehousert)
- [Build-run trace](#build-run-trace)
- [Status](#status)
- [Reconciliation](#reconciliation)
- [Preview enrolment](#preview-enrolment)
- [Verification](#verification)
- [Design traceability](#design-traceability)
- [Known limitations](#known-limitations)
- [Realtime](#realtime)
- [DAB deployment](#dab-deployment)
<!-- /toc -->

## Modes

| Mode | Slot pipeline | `_data` relation | Lakebase sync schedule | Use when |
|---|---|---|---|---|
| `full` | none | Delta table rewritten by CTAS (or a pass-through view on the Lakehouse engine) | snapshot | Default; small graphs, sources without row tracking, any backend |
| `incremental` | Lakeflow SDP pipeline, **triggered** once per build | materialized view, `REFRESH POLICY INCREMENTAL` (Enzyme) | triggered | Sources tracked, freshness = one build |
| `microbatch` | the same pipeline run **continuously** (a triggered bootstrap first) | the same materialized view, refreshing itself between builds: `TRIGGER ON UPDATE` (at most ten sources) or every 15 minutes | continuous | Sources tracked, minutes freshness between builds |
| `realtime` | the `microbatch` pipeline, plus a Real-Time Mode stream job from a Kafka-compatible topic into Lakebase `_live` | the `microbatch` view | continuous (and `_live` read live by the union view) | Lakebase domains with a change-event topic, seconds freshness; needs `ONTOBRICKS_REALTIME_ENABLED` and the domain's `realtime_source` ([Realtime](#realtime)) |

The Lakebase schedule is the mode's schedule capped by
`graph_engine_config.lakebase.sync_table_mode` (snapshot < triggered < continuous):
the default `snapshot` keeps every domain on snapshots until an operator raises the
cap. The schedule follows the resolver's decision, not a runtime fallback: when the
slot pipeline, the row-tracking probe, the MV or the MERGE fails during a build and
`_data` is rebuilt by CTAS (row tracking and `object_hash` on), the synced table
keeps its triggered/continuous registration instead of being deleted and recreated
as a snapshot (and back on the next good build); `materialization_mode_effective`
still reports `full`. When the row-tracking probe itself fails there is no resolver
decision: the synced table keeps the schedule the last good build registered (its
`sync_mode_effective`), so neither a genuinely untracked domain (snapshot) nor a
tracked one (triggered) is re-registered; without such a run the gate (the sources
counted as tracked) decides. The synced
table is always registered on `_data` (never on `_slots`); a pass-through `_data`
view falls back to the gateway VIEW as a snapshot.

### What the slot pipeline does

Row-local entity mappings (one source table, no join, no `sql_query`) are compiled by
`SlotProjectionCompiler` into *slot events*: one row per
`(map_id, source_key, predicate)` read from the source table's change feed
(`STREAM … WITH (readChangeFeed = true)`), with `op = DELETE` for deleted rows,
pre-images and `NULL` objects. An `AUTO CDC … KEYS (map_id, source_key, predicate)
… SEQUENCE BY STRUCT(_commit_version, seq_rank) … STORED AS SCD TYPE 1` flow folds
them into `triplestore_<domain>_V<n>_slots`; within one commit pre-images and deletes
rank first (`seq_rank` 0), so a key a commit deletes and inserts again
(`REPLACE WHERE`, a partial `INSERT OVERWRITE`) ends upserted. Rows with a `NULL`
key are dropped (no subject URI, no slot key). `tests/property/test_slot_fold.py`
checks, for any change history, that the fold equals the final rows' slots and its
distinct non-blank triples equal the full build's.
Join, aggregate and `sql_query` mappings stay batch: their SQL becomes the batch
branch of the `_data` MV. Relationship mappings always stay batch: under that slot
key a subject with several objects of one predicate would keep only one of them.
For the same reason a row-local entity mapping needs a **unique id column** (the
columns of its subject URI template): source rows that share a key collapse onto
one slot. The pipeline is serverless, on the `PREVIEW` channel by
default (`settings.slot_pipeline_channel`, environment variable
`ONTOBRICKS_SLOT_PIPELINE_CHANNEL`: `PREVIEW` or `CURRENT`), never in development mode, and is
always reused across builds; it carries a fingerprint of its SQL
(`ontobricks.slot_sql_sha256`) that decides between an incremental update
(unchanged mapping) and a full refresh (changed mapping).

### The `_data` materialized view

In `incremental` and `microbatch` the `_data` relation is
`CREATE OR REPLACE MATERIALIZED VIEW … REFRESH POLICY INCREMENTAL` over
`SELECT subject, predicate, object, sha2(object, 256) AS object_hash FROM
(_slots WHERE object IS NOT NULL AND TRIM(object) != '' UNION ALL <batch branches>)
GROUP BY …` — `GROUP BY` instead of `DISTINCT`, no `stack()`, blank objects (unset
attributes) dropped from the slots branch, row tracking on,
`pipelines.channel = preview`. In `microbatch` the view also refreshes itself
between builds (the continuous pipeline only keeps `_slots` current): with
`TRIGGER ON UPDATE` when it reads at most ten relations (`_slots` plus the tables of
table-based batch mappings — the limit of the
[CREATE MATERIALIZED VIEW reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view)),
otherwise, or when a `sql_query` / join hides the sources, with
`SCHEDULE CRON '0 0/15 * * * ?' AT TIME ZONE 'UTC'` (every 15 minutes; `SCHEDULE
EVERY` only takes hours, days or weeks). `incremental` and the MERGE table get no
clause. The MV definition, clause included, is hashed; a build whose definition
did not change refreshes the MV instead of replacing it, and switching between
`incremental` and `microbatch` replaces it. The refresh technique Enzyme chose is read from the
pipeline event log and recorded in the build-run trace (`refresh_technique`).

## Readiness and the fallback ladder

`MaterializationModeResolver.resolve` turns the requested mode into the one that
runs. Each step applies to the mode the previous step left and the recorded
reason is the first downgrade's:

1. `realtime` → `microbatch` — `realtime_not_available` unless a realtime topic is
   available: the domain's `realtime_source` is complete, `settings.realtime_enabled`
   is on (`ONTOBRICKS_REALTIME_ENABLED`, default off) and the graph backend is
   `lakebase` (the stream serves through `_live`) — `RealtimeSource.topic_available`.
2. `incremental` / `microbatch` / `realtime` → `full` — `serverless_unavailable` when
   `settings.serverless_pipelines_enabled` is off (environment variable
   `ONTOBRICKS_SERVERLESS_PIPELINES_ENABLED`, default on), or `backend_not_eligible` when
   the domain's graph backend is not one of `databricks`, `lakebase`, `neo4j`.
3. `microbatch` / `realtime` → `incremental` — `row_tracking_missing` (the stream
   bootstraps from `_slots`, which the change feed fills).
4. `incremental` → `full` — `row_tracking_missing`.

Row tracking is probed with `SHOW TBLPROPERTIES <table> ('delta.enableRowTracking')`
on every source table of the compiled slot pipeline (`SourceReadiness.check`);
a domain whose mappings are all batch has no source tables and counts as tracked.
A table owner makes a source ready with `ALTER TABLE <table> SET TBLPROPERTIES
(delta.enableRowTracking = true, delta.enableDeletionVectors = true)`. A failed probe or a failed slot pipeline / MV build
is `incremental_failed:<ErrorClass>` and the build continues on the full path.

**Readiness preflight.** `GET /dtwin/incremental/readiness` (builder role,
`TwinIncrementalReadiness.report`) answers before a build what the requested mode
would run as: `mode_requested`, `decision` (`effective`, `reason` — the same
resolver a build uses — and `merge_fallback`: an `incremental` request without
serverless runs as `incremental` by MERGE, as the build does),
`serverless_available`, `sources` (each slot-pipeline source table with its
`row_tracking` flag), `compile_error` (mappings that do not compile, with the
build's `incremental_failed:ValidationError` decision) and `previews` (the slot
pipeline's Lakeflow channel). It runs the probe on the build warehouse as the
caller; a probe that fails is an error, not "untracked". `POST
/dtwin/incremental/enable-row-tracking` (builder role) runs the `ALTER TABLE`
above on the untracked sources, or on a `tables` subset of the domain's own
slot-pipeline sources (any other name is refused before anything runs). It runs
as the caller, so Unity Catalog decides who may alter which table, and reports
`{table, success[, error]}` per table. The Domain information panel shows the
answer under the materialization mode (Readiness line, "Enable row tracking"
button when a source is untracked; the action needs the builder role).

**Full refresh of the slot pipeline** (`TwinSlotRefreshPlan`). Before every
slot pipeline update — a main build's and an `incremental` tick's — each source's
Delta version and table id are read, and compared with the ones the last
*completed* update recorded (the newest run with `source_versions`, whatever the
store base). A source is rewritten when its table id changed, its newest version is
below the recorded one (`DROP` + `CREATE` restarted the history), or its history
since (`DESCRIBE HISTORY … LIMIT <versions since + 1>`, at most 10000 versions;
more, or a history that does not reach back to the version after the recorded one,
count as a rewrite) holds `CREATE OR REPLACE TABLE [AS SELECT]`, `REPLACE TABLE
[AS SELECT]`, `CLONE`, `TRUNCATE`, `RESTORE`, an `INSERT OVERWRITE` (a `WRITE` in
`Overwrite` mode) or a `Complete`-mode `STREAMING UPDATE`. A rewrite breaks the
change feed the pipeline reads, so the update runs as a **full refresh in the same
mode**. A reconciliation drift (see [Reconciliation](#reconciliation)) does too,
and also refreshes `_data` with `REFRESH MATERIALIZED VIEW … FULL`. The reason is
recorded as `slot_full_refresh_reason` (`source_overwritten` / `reconcile_drift`);
`mode_fallback_reason` keeps the first downgrade. The refresh is **sticky**:
`slot_full_refresh_pending` is recorded true before the update and false only once
it and the `_data` refresh completed, and a run whose update did not complete
carries the previous `source_versions` forward, so the next build or tick retries.
The versions recorded are the ones read *before* the update: a commit landing
during it is checked next time. A source history that cannot be read is an
`incremental_failed:InfrastructureError` for a build; a tick leaves `_data` as it
is for that tick (nothing refreshed, nothing dropped) instead of a CTAS every later
tick would repeat.

Two further fallbacks apply after the mode is decided:

- **MERGE fallback** (`strategy = merge`). When an `incremental` request fell to
  `full` for `serverless_unavailable`, or because the slot pipeline failed with an
  `InfrastructureError` mentioning serverless or pipelines (every slot-pipeline
  error does), `_data` is kept as a Delta table maintained by
  `MERGE INTO _data USING (SELECT … FROM <gateway VIEW> GROUP BY …) … WHEN NOT
  MATCHED THEN INSERT … WHEN NOT MATCHED BY SOURCE THEN DELETE` (row tracking,
  deletion vectors and the change data feed on). The run reports
  `materialization_mode_effective = incremental`, the reason stays. A fingerprint
  (source table versions + R2RML + base URI + generated SQL) is recorded; when it
  equals the previous base run's and `_data` is unchanged since, the build is
  recorded as a success with the message "no source changes (skipped)". The
  skip never fires when batch mappings exist (their tables are not in the
  fingerprint). A MERGE that fails degrades to the CTAS.
- **Net-change apply** (`apply_mode`). For Neo4j and app-managed Lakebase, an
  incremental `_data` (MV or MERGE table) is consumed through its change feed:
  `table_changes('<data>', since + 1, latest)` reduced to net inserts/deletes
  since the `_data` version the store was last brought to, deletes first. The
  store falls back to the full stream (`apply_mode = full_stream`) with
  `apply_fallback_reason` = `no_previous_version` (no matching base run, the
  base came from a different `_data` relation kind / table id, or the store no
  longer holds the graph), `changeset_too_large` (more than 10 % of the current
  `_data` rows), `cdf_unavailable` (any change-feed read error, before or during
  the stream, or a base version newer than `_data`) or `inferred_not_separable` (the store
  cannot purge its inferred triples on their own — Neo4j today). After a net
  apply the store's inferred triples are purged and the graph cache rebuilt, so
  reasoning starts from a clean slate exactly as after a full build.

## What a build does per backend

**Lakehouse / Delta engine.** `_data` is the graph. `full`: CTAS. `incremental` /
`microbatch`: slot pipeline + `_data` MV (or the MERGE table); `_inferred` is
reset in every mode, except by a MERGE build skipped for unchanged sources, which
completes before the reset (the inferences of the unchanged graph stay); no graph
stream (`apply_mode = none`). Reads may go to a
Lakehouse//RT warehouse (below).

**Lakebase, managed_synced.** `_data` is built as above and the Lakeflow synced
table is registered on it with the mode's schedule (capped by `sync_table_mode`);
a drifted registration (other source or schedule) is deleted and recreated, the
Postgres union view is dropped only right before that delete and recreated after.
A continuous table is waited for until it streams (`ONLINE_CONTINUOUS_UPDATE`).
`apply_mode = lakeflow_sync`.

**Lakebase, app_managed.** `_data` is built as above; the app writes the
`<graph>_sync` table itself: a net-change apply when a matching base run exists
(`delete_from_sync_iter` then `bulk_load_into_sync`), otherwise drop + full stream.

**Neo4j.** `_data` is built as above; the graph is always streamed in full in this
release (`inferred_not_separable`).

## Scheduled builds

A scheduled tick (`registry/schedule/build.py`) resolves the domain's mode with the
same resolver and the same row-tracking probe as the main build
(`TwinBuildPostMaterialize.decide_mode_for`), and counts a decision the main build
promotes to the MERGE fallback (`TwinMergeFallback.promotes`) as `incremental`, so a
tick and a main build register the Lakebase synced table on the same source and
schedule and never alternate between triggered/continuous and snapshot.

The tick decides (and probes) before it writes anything, so a failed probe drops
nothing. When `_data` already is the incremental main build's materialized view the
tick refreshes it in place instead of replacing it: `incremental` runs one triggered
update of the slot pipeline, then `REFRESH MATERIALIZED VIEW`; `microbatch` only
refreshes (its continuous pipeline keeps `_slots` current). It does so only while
the view still has the current definition: the tick recompiles the mappings and
compares the MV definition fingerprint and the slot pipeline's SQL fingerprint, so
mappings edited since the last main build are picked up by the CTAS (the next main
build recreates the view) instead of being silently ignored. A `full` or
MERGE-promoted domain, a `_data` that is not our MV, or a missing slot pipeline get
the CTAS from the gateway VIEW, as before.

## Lakehouse//RT

In the incremental and microbatch modes `_data` becomes a materialized view that
Lakehouse//RT can read directly (RT reads managed tables, materialized views and
streaming tables, not plain views). `_graph` remains a union view for now.

Graph reads go to the query warehouse (`graph_engine_config.lakehouse.warehouse_id`
plus "Use Lakehouse//RT for queries"). A heavy traversal can trip an RT guardrail
(2-minute runtime, 300 GB shuffle, statement timeout — see the Lakehouse//RT
documentation for the current values) and `DeltaFlatStore.execute_query` retries
that SELECT/WITH once on the build warehouse, as the same caller (same OBO token),
under the graph statement timeout. Writes, other errors and a build warehouse
equal to the query warehouse are not retried. `last_read_route` (`"query"` or
`"build-fallback"`) tells which path answered.

## Build-run trace

Every build run persists `stats.materialization` (registry `build_runs.stats`):

| Key | Meaning |
|---|---|
| `materialization_mode_requested` / `materialization_mode_effective` | the domain's setting and the mode that ran |
| `mode_fallback_reason` | first downgrade reason (`""` when none) |
| `refresh_technique` | Enzyme technique of the `_data` MV refresh (`""` when not an MV) |
| `slot_pipeline_id`, `slots_table` | the Lakeflow pipeline and the `_slots` table (`""` when none) |
| `triples_source`, `inferred_reset` | the relation the graph was counted/streamed from; whether `_inferred` was reset |
| `sync_source`, `sync_mode_effective` | what the Lakebase synced table was registered on, and its schedule |
| `strategy` | `mv`, `merge`, `ctas` or `""` |
| `apply_mode`, `apply_fallback_reason` | `net_changes`, `full_stream`, `lakeflow_sync`, `none`; why a net apply was not done |
| `data_table_version`, `data_table_id`, `fingerprint` | the `_data` version/table id the store matches; the MERGE fingerprint |
| `source_versions`, `source_table_ids` | each slot-pipeline source's Delta version and table id read before the last completed update (only on runs that reached a slot update) |
| `slot_full_refresh_pending`, `slot_full_refresh_reason` | a full refresh owed until an update completes it; `source_overwritten` / `reconcile_drift` (only on runs that reached a slot update) |
| `reconcile_drift` | a scheduled tick's reconciliation found drift (tick runs only) |
| `live_drift` | a realtime tick found `_live` and the slots apart (tick runs only; restart the stream) |
| `realtime_run_id`, `realtime_state` | the stream job run a `realtime` build started (or a later build stopped), its state (`PENDING`, `RUNNING`, `CANCELLING`, `failed:<Error>`); `""` otherwise |

The Knowledge Graph › Runs cockpit shows the effective mode (with the requested
one and the fallback reason when they differ), the strategy / refresh technique
and the apply mode per run, and a "Materialization" block in the run details.
The newest successful run with the same graph engine, sync mode and graph name
whose apply reached the store is the *base* the next build continues from. The
search (newest 50 runs) ends without a base — and the next build streams in
full — at a run that touched the store without recording these facts (a
scheduled tick, a pre-0.9.1 build), at a failed run whose apply had started, or
at a run of the same graph under another sync mode; failed runs that never
reached the store and Lakebase skip runs are passed over.

## Status

`GET /dtwin/incremental/status` (builder role, `TwinIncrementalStatus.report`)
says where an incremental domain stands now, without writing anything:

| Field | Source |
|---|---|
| `mode_requested` | The domain's materialization mode |
| `slot_pipeline` | `{name, id, state}` of this version's slot pipeline (`SlotPipelineRunner.get_state`; `absent` without one) |
| `data`, `slots` | `{table, kind, version, committed_at}`: kind `mv` / `table` / `streaming_table` / `view` / `absent` from `information_schema.tables`, last commit from `DESCRIBE HISTORY … LIMIT 1` |
| `freshness_lag_s` | Now minus the last `_data` commit (`null` when unknown) |
| `lakebase_sync` | `{synced_table, state, message}` of a `managed_synced` store (`null` otherwise) |
| `last_build` | `{status, build_kind, finished_at, materialization}` of the newest build run |

Each section is read on its own: a failing read puts `error` (and `unknown`) in
its section and leaves the others. A `full` domain is answered without any
warehouse, pipelines or registry call (every section `null`), so the build
panels never wake a warehouse for it. The scans run on the build warehouse as the
caller. The digital twin build panels (Lakebase/Neo4j and Lakehouse) show it as
a compact "Incremental status" card, hidden for a `full` domain.

## Reconciliation

`POST /dtwin/incremental/reconcile` (builder role) compares the domain's `_slots`
streaming table with its `_data` relation the way the MV reads the slots:
distinct-triple row counts and `sum(xxhash64(subject, predicate, object))` of
both, blank objects and NULL-subject rows left out (a NULL source key gives the
gateway VIEW a NULL-subject triple the slot view drops: garbage, not drift; the MV
and VIEW keep it), `drift` when they differ, and the slot counts per
`map_id`. The scans run on the build warehouse as the caller. `_data` is the
union of the slots and the batch branches, so the response carries
`batch_branches_present` (derived from the domain's *current* mappings) and
`slots_run_ahead` (a `microbatch` domain, whose continuous pipeline keeps
moving `_slots` between builds); `drift` is meaningful
(`drift_meaningful: true`) only when both are false. A domain without a slots table answers 404; an unreadable
catalog or a missing warehouse client answers 502, not 404.

Two more pairs are compared with `_data`, each reported in `pairs` (`true` when
the sides differ):

- `view_data`: the **batch recompute** — the gateway VIEW (every triple
  recomputed from the sources), counted and hashed exactly like `_data`. It
  scans the whole VIEW, a full read of the sources, so on demand it only runs
  with `?recompute=true`.
- `sync_data`: the row count of the Lakebase synced table (`managed_synced`
  stores only; counts, no hash). A store that cannot be counted is left out.

A domain without `_slots` (every mapping batch) compares these two alone
(`slots_present: false`); with nothing to compare it answers 404.
`pairs_meaningful` says which pairs can tell drift: `slots_data` as above, the
other two whenever the slots do not run ahead. Both compare as of the last
refresh of `_data`, so a source changed since shows as drift until the next
build or tick. `reconcile_drift` is true when a meaningful pair differs.

**After a scheduled tick.** A tick of a Lakeflow-synced `incremental` domain
whose `_data` is still the materialized view it refreshed in place (a CTAS table
equals the VIEW by construction) reconciles at the end, right after the
synced-table refresh, with the batch recompute; the synced table is counted only
when its refresh came `ONLINE`. A `microbatch` domain is not reconciled (its
slots run ahead: no pair can tell). Drift is logged at warning and recorded
as `stats.materialization.reconcile_drift = true` on the tick's build run (a
scheduled run still ends the base-run search: the tick rewrote the store). The
next build or `incremental` tick that updates the slots owes a full refresh
(`TwinSlotRefreshPlan.owed`): its slot pipeline update runs with
`full_refresh=True` and `_data` refreshes `FULL`; a batch-only domain refreshes
`_data` `FULL`. The drift stays owed until such a run records
`slot_full_refresh_pending = false`; runs that never reach a slot update (MERGE
or full builds) pass it on. A reconciliation that fails is logged and never fails
the tick.

## Preview enrolment

Three Databricks previews are assumed on the workspace; without them the modes
degrade as described above:

- **Enzyme incremental materialized views** on the `PREVIEW` pipeline channel,
  and the **materialized-view change data feed** (`table_changes` on an MV; the
  Databricks preview notes ask for a one-time `REPAIR TABLE … SYNC METADATA`,
  which the app does not run — live-unverified) used by the net-change apply.
- **Materialized view → Lakebase triggered/continuous synced tables**.
- **Lakehouse//RT** read warehouses (guardrail values quoted above are the
  documented ones at design time; verify on the workspace).

On the customer's source tables the app only requires row tracking
(`delta.enableRowTracking`, the basis of automatic change data feed on DBR 19);
the `ALTER TABLE` above also turns on deletion vectors, which row tracking needs
for in-place updates.

## Verification

Whether the `_data` definition a build creates plans an incremental refresh is
checked on a real warehouse by an opt-in test, `tests/external/test_mv_incremental_plan.py`
(`external` marker). It compiles a row-local and a `sql_query` fixture mapping,
builds the `_data` view definition the main build would create
(`TwinIncrementalBuild.data_definition_for` + `MaterializedViewBuilder.data_mv_sql`),
runs `EXPLAIN CREATE MATERIALIZED VIEW …` (`MaterializedViewBuilder.explain_sql`,
which explains a `CREATE OR REPLACE` as its `CREATE`) and asserts
`MaterializedViewBuilder.plan_is_incremental`: an incremental technique
(`ROW_BASED`, `GROUP_AGGREGATE`, `APPEND_ONLY`, …) and no `FULL_RECOMPUTE`. It
creates the schema, a source table and a Delta table with the slot columns
(standing in for the `_slots` streaming table) if absent, and materializes nothing:

```bash
ONTOBRICKS_EXTERNAL_WAREHOUSE=1 ONTOBRICKS_EXTERNAL_SCHEMA=<catalog.schema> \
DATABRICKS_CONFIG_PROFILE=<profile> ONTOBRICKS_EXTERNAL_WAREHOUSE_ID=<warehouse id> \
uv run --frozen pytest -q -m external tests/external/test_mv_incremental_plan.py
```

Instead of a CLI profile it also takes host and token —
`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_SQL_WAREHOUSE_ID` — which is how
the nightly `external-smoke` job (`.github/workflows/nightly.yml`) runs it on the
integration workspace, with `ONTOBRICKS_EXTERNAL_WAREHOUSE=1` and
`ONTOBRICKS_EXTERNAL_SCHEMA` from the repository variable
`ONTOBRICKS_INT_EXTERNAL_SCHEMA` (default `main.ontobricks_it`; the test creates the
schema if absent). That step is **blocking**, unlike the job's other external
probes: an incremental plan that regresses fails the job (with a step summary and
an error annotation) and the nightly opens its regression issue.

Without those variables the test is skipped, so the unit run stays offline.

## Design traceability

Requirements of the design document (`[Design] Incremental Triple Materialization | OntoBricks`)
against this release. ✅ implemented · ◐ implemented with a documented deviation · ⛔ deferred.

| Phase | Requirement | Status | Where |
|---|---|---|---|
| 1 P0 | Compute the source once (`count`, stream, Lakeflow source read `_data`) | ✅ | `TwinBuildPostMaterialize.triples_source_relation`, `SyncPlan` |
| 1 P0 | `_inferred` reset on both build paths | ◐ | reset on every build that changes base triples; the MERGE *skip* (no source change) keeps it |
| 1 P0 | `SlotProjectionCompiler` reuses the translator's predicate unpivot; deletes / NULL objects / pre-images → tombstones | ✅ | SCD1 physical deletes stand in for a `deleted` column |
| 1 P0 | Slot pipeline per (domain, version) via DAB, AUTO CDC into `_slots` | ✅ | rendered per published version into `resources/_generated_triple_slots.yml` at deploy time ([DAB deployment](#dab-deployment)); a pipeline the runner created first is bound to its resource before the deploy; a compile error keeps the rendered resource; development targets set `presets: {name_prefix: "", pipelines_development: false}` and the runner also finds a `[dev <user>] ` name |
| 1 P0 | `_data` / `_graph` MVs with `REFRESH POLICY INCREMENTAL`; CI check with `EXPLAIN` | ◐ | `_data` MV ✅; `_graph` stays a VIEW (Lakehouse//RT reads `_data`); `EXPLAIN` check is the nightly external test `tests/external/test_mv_incremental_plan.py` |
| 1 P0 | Readiness preflight, admin "enable row tracking", missing tracking degrades to `full` | ✅ | `GET /dtwin/incremental/readiness`, `POST /dtwin/incremental/enable-row-tracking`, `row_tracking_missing` |
| 1 P0 | Lakebase `managed_synced` triggered from `_slots` / `_data` | ◐ | from `_data` only (`_slots` would expose duplicate and blank rows to asserted-only readers); capped by `sync_table_mode` |
| 1 P0 | Build-run trace: mode, refresh technique, fallback reason | ✅ | `stats.materialization`; cockpit column, details and status card |
| 1 P1 | Net changes for app-managed Lakebase and Neo4j (~10 % threshold) | ◐ | Lakebase ✅ (`TwinNetChangeApply`); Neo4j streams in full — its model has no place to mark a triple as inferred |
| 1 P1 | MERGE fallback with row tracking and a no-op skip | ✅ | `DeltaMergeMaterializer`, `TwinMergeFallback` |
| 2 P0 | `microbatch` = continuous pipeline; mode switch without backfill | ✅ | triggered bootstrap then continuous; fingerprint decides a full refresh |
| 2 P0 | Source overwrites detected from `DESCRIBE HISTORY` → fall back | ✅ | `SourceReadiness.source_overwritten_since`; `source_versions` / table ids in the trace; full refresh of the slot pipeline |
| 2 P0 | Lakehouse//RT read warehouse; DDL on DBSQL; Statement Execution API; guardrail routing | ✅ | `DeltaFlatStore` reads-only fallback as the caller |
| 2 P0 | CONTINUOUS synced tables; union view `DISTINCT`, no tombstones | ✅ | `ContinuousSyncWait`; Postgres union view |
| 2 P0 | Observability: pipeline state, last commit, freshness lag, fallback events | ✅ | `GET /dtwin/incremental/status` + status card; fallback events in the trace |
| 2 P0 | Reconciliation: count + hash of `_slots`, `_data`, Lakebase, batch recompute; re-backfill on drift | ✅ | `POST /dtwin/incremental/reconcile` (+ `recompute`), tick reconciliation, `reconcile_drift` → full refresh |
| 3 P2 | Prerequisites: topic config, readiness, resolver | ✅ | `RealtimeSource`, `realtime_topic_available`, readiness `realtime` facts, domain panel fields |
| 3 P2 | Processing: RTM + `transformWithState` + `SlotDiffProcessor` | ◐ | `jobs/triple_stream` (`main.py`, `processor.py`); the Spark / connector / RTM wiring is live-unverified |
| 3 P2 | Serving: Lakebase `_live`, union view, tombstone purge | ✅ | `_companion_ddl`, `LakebaseLiveTable`, `TwinLiveServing`: seeded from `_slots` at a pinned version the stream starts from (topic replayed from just before it), in the union view only while served, retired on stop / a non-realtime build / a dead stream (tick), tombstones purged by the tick; deletes and updates reach readers once `_sync` syncs them |
| 3 P2 | Operations: job deployment, start/stop, status, `_live` reconciliation | ◐ | `resources/triple_rtm.job.yml`, `TwinRealtimeStream`, status card; one stream per domain and per deployment at a time; the slot pipeline reads the CDF, not the topic ([why](#realtime)); cluster sizing and run-as grants are live-unverified |
| Verif. | Unit · property (slot fold, slot diff) · external (EXPLAIN plan) | ✅ | `tests/units/…`, `tests/property/test_slot_fold.py`, `tests/property/test_slot_diff_property.py`, `tests/external/…` |

## Known limitations

**Upgrading.** After upgrading to this release, run one build of every
`incremental` / `microbatch` domain. The slot pipeline SQL changed (deletes now rank
with pre-images, `seq_rank` 0), so its fingerprint changed: that first build runs one
full refresh of the slot pipeline, and until it has run the scheduled ticks rebuild
`_data` by CTAS (the slot pipeline no longer runs the current SQL).

Behaviour in this release:

- `augment_mappings_from_config` assigns a `sql_query` to every configured
  entity, so most real mappings classify as batch; the slot pipeline engages only
  for table-based mappings and `incremental` is then the `_data` MV over the batch
  branch. Follow-up: treat single-table `SELECT … FROM t` queries as row-local.
- `microbatch` freshness between builds is the `_data` view's own refresh: on every
  source update (at most once a minute) for at most ten sources, otherwise every 15
  minutes — and every domain with a `sql_query` or join mapping (most real ones,
  see above) gets the 15-minute schedule.
- Neo4j always streams in full (`inferred_not_separable`). A net apply keeps the
  store and must purge the inferred triples first, but the Neo4j property graph
  has no place to mark a triple as inferred: a literal triple is a node property
  (`SET n += props`, one value per key), `rdf:type` is a node label, and only
  object triples are relationships that could carry a flag. Tagging would need
  per-node bookkeeping of inferred property keys and labels, an `inferred` flag
  through every triple writer, and still could not tell a value that is both
  mapped and inferred; until then the fallback stays.
- The MERGE fallback engages for any slot-pipeline failure in `incremental`
  (every runner message mentions the pipeline); `microbatch` falls back to CTAS.
  The MERGE skip only fires for all-row-local domains. A MERGE build sets the table
  properties only when `SHOW TBLPROPERTIES` lacks one, and adds `object_hash` to an
  existing MERGE table that lacks it (a domain that became Lakeflow-synced); the
  same MERGE fills `object_hash` on the rows merged before
  (`WHEN MATCHED AND t.object_hash IS NULL THEN UPDATE SET object_hash = sha2(s.object, 256)`),
  so the synced table's key is never `NULL`.
- With no new `_data` version the net apply still purges inferences, optimizes
  and rebuilds the graph cache; the Lakebase graph cache is rebuilt once inside
  the purge.
- Base runs recorded before 0.9.1 carry no `_data` version; Lakebase skip runs
  are passed over in the base search, so after 50 consecutive skips one full
  stream runs. The empty-graph completion keeps its own "Full rebuild" message.
- Slot pipelines of earlier versions keep running after a version bump; only the
  current version's pipeline is stopped.
- The first build after upgrading re-registers every managed-synced table on
  `_data` instead of the VIEW (one full resync). `SyncedTableManager.trigger_refresh`
  still starts updates with `full_refresh=True`. The drift check covers every name
  `ensure` reuses, ghost-state fallbacks (`_b` / `_c` / `_d`) included, but a
  fallback registration left from an earlier ghost period is not deleted once the
  primary name works again; an unreadable registration spec is kept and logged at
  warning; a delete→create race is handled by polling; a registration costs up to
  four synced-table GETs per build.
- The Lakehouse//RT fallback runs as the caller, who therefore needs CAN_USE on
  the build warehouse.
- The standalone Delta build (`/dtwin/databricks-build/start`,
  `DeltaTripleStoreBuildPipeline`) does not know the materialization modes: it
  rebuilds `_data` by CTAS, which drops an incremental domain's materialized view
  (the next main build recreates it), and it never stops a running `microbatch`
  slot pipeline.

Live-unverified (workspace spike checklist):

- `EXPLAIN CREATE MATERIALIZED VIEW` / event-log `planning_information` format;
  custom TBLPROPERTIES on an MV; `DESCRIBE HISTORY`, `DESCRIBE DETAIL` and
  `table_changes()` on a standalone MV; the change data feed on the MERGE table.
- A Lakeflow streaming table or the `_data` MV as a triggered/continuous synced
  table source.
- `ImportFormat.AUTO` for the pipeline notebook; the stop→start race of a
  continuous pipeline.
- A build's `REFRESH MATERIALIZED VIEW` while a `TRIGGER ON UPDATE` or scheduled
  refresh of the same view is running.
- A foreign or shared (Delta Sharing) source table counted toward the ten upstream
  sources of `TRIGGER ON UPDATE`.
- A CTAS `_data` that keeps a triggered/continuous registration — the scheduled
  tick's CTAS on a MERGE-promoted domain, or a main build whose probe or MERGE
  failed — carries row tracking only (no explicit `delta.enableChangeDataFeed`)
  until the next MERGE or MV build; the Lakebase sync reading it in between relies
  on row-tracking-derived change data.

## Realtime

`realtime` keeps the `microbatch` machinery (continuous slot pipeline over the
change data feed, self-refreshing `_data` view, continuous synced table) and adds a
stream that brings a source change into the graph in seconds: a classic-compute job
reads a Kafka-compatible topic, diffs each changed source row's slots and upserts
them into a Lakebase `_live` table the graph's union view reads.

**Prerequisites.**

- Switching `ONTOBRICKS_REALTIME_ENABLED` off deployment-wide does not stop running
  streams or retire a served `_live` (the realtime tick no longer runs): call
  `POST /dtwin/incremental/realtime/stop` for every realtime domain first.
- The replay window the stream starts from is the seed version's commit time minus a
  fixed 10-minute margin (`TwinLiveServing`); a source fed from the topic with more
  lag, or a slot update longer than ten minutes, needs a wider margin (follow-up:
  make it a setting).
- The deployment: `ONTOBRICKS_REALTIME_ENABLED=true`, the stream job deployed
  (`resources/triple_rtm.job.yml`, name `<app>-triple-rtm` or
  `ONTOBRICKS_REALTIME_JOB_NAME`, the app principal with `CAN_MANAGE_RUN` on it), a
  Postgres role and a secret holding its password for the sink
  (`ONTOBRICKS_REALTIME_LAKEBASE_USER`, `ONTOBRICKS_REALTIME_LAKEBASE_SECRET` =
  `scope/key`), and a UC volume path (`/Volumes/…`) for the checkpoints and each
  run's mappings (`ONTOBRICKS_REALTIME_CHECKPOINT_ROOT`).
- The domain: a **Lakebase** graph backend, a **published** version, row-tracked
  sources (as `microbatch`), and its `realtime_source` (Domain › Information ›
  Materialization mode › Real-time source):
  `{kind: kafka | eventhubs | kinesis, bootstrap_servers, topic, secret_scope,
  secret_key, key_format: json, state_ttl_hours}` — `RealtimeSource` stores the
  secret's scope and key *names* only and refuses any other field (a password, a
  JAAS config). The secret holds the SASL JAAS config (`kafka`), the namespace
  connection string (`eventhubs`, through its Kafka endpoint) or
  `ACCESS_KEY:SECRET` (`kinesis`, whose `bootstrap_servers` is the region).
  **Kinesis is experimental** (its sequence numbers are opaque, see below; the panel
  and the readiness line say so). `state_ttl_hours` (default 72) is how long the
  stream keeps a row's state. The readiness preflight reports `realtime`
  (`enabled`, `source_configured`, `kind`, `topic`, `backend_eligible`,
  `experimental`) and the domain panel shows it.

**The topic.** One JSON change envelope per source-row change, Debezium style:
`{"op": "c" | "u" | "d" | "r", "after": {...}, "before": {...}, "source": {"table":
"catalog.schema.table"}, "ts_ms": …}`, optionally under `"payload"`. The topic must be
keyed by the source row key, so every change of a row stays in one partition, in
order: the Kafka offset is the event `seq` (Kinesis: the arrival millisecond × 10⁶
plus the sequence number's last six digits — ordered within a shard and a
millisecond). Values are decoded with the Unity Catalog source table's types into
the text `CAST(… AS STRING)` gives them (`SparkColumnText`: epoch dates and
timestamps, base64 or text decimals, Java double formatting, UTC), so the stream
projects exactly what the slot SQL projects. An update that changes a row's key also
tombstones the old key (from `before`), as the AUTO CDC fold does. A bare
`source.table` (Debezium often sends only the table name) matches the mapped table of
that leaf name when exactly one has it. An envelope that does not parse, has no `seq`,
names an unmapped table or holds a value that does not decode is skipped and counted
per reason (logged), never failing the stream.

**Realtime processing core.** The Real-Time-Mode job groups the change events by
`(map_id, source_key)` and runs `jobs.triple_stream.processor.SlotDiffStatefulProcessor`
with `transformWithState`. For each source-row event it projects the after-image
through `SlotRowProjector`, a pure-Python mirror of the slot-event SQL built from the
same mapping dict (`SlotRowProjector.for_mappings` returns one per row-local
mapping). It then diffs the result against the row's `MapState` with
`SlotDiffProcessor`: changed or new slots are emitted as `SlotChange` upserts (with
`object_hash` = `sha2(object, 256)`), and vanished or blank slots (`None` or
whitespace-only, as `TRIM(object) != ''`) as tombstones. A `seq` that is not newer
than the state's is ignored, and a delete keeps `(None, None, seq)` watermarks for
the row's predicates instead of emptying the state, so a late or replayed event
cannot resurrect a deleted row. The initial state is loaded from the `…_slots`
snapshot at the version `_live` was seeded at (`--slots-version`), and state
eviction is the `state_ttl_ms` passed to the processor
(`--state-ttl-hours`; with a TTL the job runs `timeMode="ProcessingTime"`, which
`transformWithState` requires, without one `"None"`). The TTL must exceed the
maximum event lateness — an evicted watermark no longer guards its slot; with no
TTL the state keeps one watermark per deleted predicate (unbounded). The equivalence
"AUTO CDC fold == SlotDiffProcessor fold == full-build projection" is checked by
`tests/property/test_slot_diff_property.py`.

**The job** (`jobs/triple_stream/main.py`, `TripleStreamJob`): arguments
(`--domain`, `--version`, the source, `--slots-table`, `--mappings-json-path`, the
Lakebase host / database / role / secret, `--live-table`, `--checkpoint`,
`--state-ttl-hours`, `--slots-version`, `--start-timestamp-ms`), `spark.readStream`
of the topic, the envelope parse and
explode (`TripleStreamEnvelope`, `mapInPandas`), the stateful diff
(`transformWithState`, output mode **update** — the only one Real-Time Mode runs),
and a `foreach` writer (`LiveSink`, `writeStream.outputMode("update")`) that upserts
each change as it arrives:

```sql
INSERT INTO <live> AS live (…, updated_at) VALUES (…, now())
ON CONFLICT (map_id, source_key, predicate) DO UPDATE SET … WHERE EXCLUDED.seq > live.seq
```

A `foreach` writer, not the native `postgresql` streaming sink, because that sink
has no `seq` guard: a replayed or reordered event would overwrite a newer one. The
trigger is Real-Time Mode (`spark.databricks.streaming.realTimeMode.enabled` and
`trigger(realTime=…)`, `--real-time-mode` by default); a runtime that refuses to
start the query in it falls back to a `processingTime` micro-batch trigger (logged).

The job cluster is classic (Standard runtime, no Photon, no autoscaling, on-demand
workers) and runs in UTC. It installs no app wheel: the driver and the executors
import `back.core.streaming` and `jobs.triple_stream` from the synced workspace files
(`PYTHONPATH` = `${workspace.file_path}/src`), whose import closure needs no app
layer, web stack, SPARQL parser or pydantic (`tests/units/jobs/test_triple_stream_closure.py`);
the job installs `psycopg` and `databricks-sql-connector` and never pins or replaces
the runtime's `pyarrow`. **Sizing:** Real-Time Mode schedules every stage at once, so
the workers' task slots (`var.realtime_num_workers` × cores per worker) must be at
least the topic's partitions plus `spark.sql.shuffle.partitions`
(`var.realtime_shuffle_partitions`; the stream has one shuffle, the
`transformWithState` grouping). The defaults — 2 × 4-core workers, 4 shuffle
partitions — fit a topic of up to 4 partitions. Arrow batches to the Python
processor hold 100 records (1 is the lowest latency, at a throughput cost).
**Runtime:** `scripts/deploy.sh` passes the workspace's newest DBR 19 (LTS first;
`scripts/_internal/_select-spark-version.py`) because Kinesis polling in Real-Time
Mode needs DBR 19 and the Python `transformWithState` low-throughput latency fix
needs 18.2 or later; when the runtimes cannot be listed the bundle default (the
newest LTS key known to the repo) stays. A failed run retries three times a minute
apart.

**`_live`** (`_companion_ddl.live_table_ddl`, `LakebaseLiveTable`):
`g_<dom>_v<n>_live(map_id, source_key, predicate, subject, object, object_hash,
deleted, seq, updated_at)`, primary key `(map_id, source_key, predicate)`, partial
indexes `(subject)`, `(predicate, subject)`, `(object_hash)` `WHERE NOT deleted`, and
early autovacuum (`autovacuum_vacuum_scale_factor = 0.02`,
`autovacuum_analyze_scale_factor = 0.01`). When it exists the union view is a
distinct `UNION` of the synced table, the companion (app writes and inferences) and
the live rows (`WHERE NOT deleted`, blank objects dropped) — but only while `_live`
is **served**, which its table comment (`ontobricks:live:serving`) records: a stopped
or dead stream's `_live` is emptied and leaves the view, so it never freezes stale
values in the graph. The union is distinct on the triple, not on the slot, so a
deleted slot stays visible through the synced table until that table syncs the
delete, and an **updated** slot shows both its old value (synced table) and its new
one (`_live`) until then. `LakebaseLiveTable.ensure` grants the stream's Postgres
role `USAGE` on the schema and `SELECT, INSERT, UPDATE` on `_live`.
`LakebaseLiveTable.purge_tombstones` (`DELETE … WHERE deleted AND updated_at < now()
- interval '<n> hours'`) keeps tombstones as long as an event can be late.

**Orchestration** (`TwinRealtimeStream`, `TwinLiveServing`, `TwinRealtimeJob`). An
effective `realtime` build starts the stream once the slot pipeline and the `_data`
view are up. `start` checks the prerequisites, cancels a run of the same domain at
another version (one stream per domain), and reuses a run of this version — found
among the job's active runs by its `domain` / `version` parameters, so `stop` and
`status` need no stored state; the cancelled version's `_live` is retired. Otherwise
it creates `_live` (with the grants), seeds it from the `…_slots` snapshot **at a
pinned Delta version** (`DESCRIBE HISTORY … LIMIT 1`, then `SELECT … VERSION AS OF
<v>`, read on the build warehouse in batches, at the snapshot `seq` -1, so every
stream event wins), marks it served and refreshes the union view, uploads the
mappings JSON to the checkpoint volume (`<checkpoint root>/_mappings/<run name>.json`),
and `run_now`s the job with `job_parameters` (`state_ttl_hours`, `slots_version` =
`<v>` and `start_timestamp_ms` included). A failed upload or `run_now` after the seed
retires `_live` again before the error is raised. The build records
`realtime_run_id` / `realtime_state` and `slots_seed_version` (`<v>`); a stream that
cannot start is recorded (`failed:<Error>`), never fails the build.

**No change between the seed and the stream's start is lost.** The job reads its
`transformWithState` initial state from the same `…_slots` version `<v>` and, on a
query's first start, replays the topic from `start_timestamp_ms` — `<v>`'s commit
time less ten minutes, the slot pipeline's lag between a source change and its
`_slots` commit (Kafka and the Event Hubs Kafka endpoint: `startingTimestamp` for
every partition, the newest offset for a partition with nothing newer; Kinesis:
`initialPosition` `at_timestamp`). The overlap replays in order, so each slot
converges on its newest event (a slot may show an older value for the moment the
replay passes it); the sink's `seq` guard absorbs duplicates. A restarted run
resumes from its checkpoint instead. `stop` cancels the run and retires `_live` (emptied, no longer
served); a later build that is not `realtime`, on a Lakebase store whose `_live` is
served (on a deployment with realtime enabled; otherwise no Postgres query is
made), does the same. A scheduled tick of a `realtime` domain retires a served
`_live` whose stream no longer runs (crashed past its retries: readers fall back to
the microbatch graph until the next build restarts it) and otherwise purges
tombstones older than the state TTL (at least 24 hours). `POST
/dtwin/incremental/realtime/start` / `stop` (builders) and the status endpoint's
`realtime` section (`run_id`, `state`) and card line do the same by hand.

**Identities.** The job runs as the deployer (its owner), who needs READ on the
domain's secret scope (source credentials, the Lakebase password), SELECT on the
domain's `…_slots` table, and READ VOLUME + WRITE VOLUME on the checkpoint volume.
The app principal needs CAN_MANAGE_RUN on the job (granted by
`scripts/bootstrap/app-permissions.sh`) and WRITE VOLUME on the checkpoint volume
(the mappings upload). A target may set the job's `run_as` instead, if its deployer
holds `servicePrincipal.user` on that principal.

**Reconciliation.** For a `realtime` domain whose `_live` is served the
reconciliation also counts `_live` (not deleted, non-blank) against the `_slots` slot
rows (`live_slots`, counts only); a retired `_live` is not counted, so a stopped
stream reports no drift.
A difference is `live_drift` — the stream diverged; restart it — and never a slot
full refresh. A scheduled tick of a realtime domain reconciles that pair (no batch
recompute: its slots run ahead) and records `live_drift`.

**Two consumers of the same changes, on purpose.** The slot pipeline reads the
sources' change data feed and the stream reads the topic. `_slots` stays the
complete, replayable snapshot every mode shares — `_data`, the reconciliation and the
stream's own seed are built from it — while a topic is neither complete history nor
guaranteed to carry every write to a source table (a backfill or a repair `UPDATE`
reaches the change data feed only). The stream is the low-latency overlay on top,
reconciled against `_slots` by the tick. One stream runs per deployment at a time
(`max_concurrent_runs: 1`; raise it to stream several domains).

**Live-unverified:** Real-Time Mode in update output mode with
`transformWithState` (row API, `MapState` TTL) and the per-row `foreach` Postgres
writer; the task-slot sizing rule on the default cluster; the Kafka / Event Hubs /
Kinesis connectors; executors importing the synced source through `PYTHONPATH`, and
the `databricks-sql-connector` install leaving the runtime's `pandas` / `pyarrow`
alone; the DBR 19 key `deploy.sh` resolves; the run-as identity's grants (secret
scope, `_slots`, checkpoint volume) and the `_live` grants to the stream role; the
job's `[dev <user>] ` name lookup and whether `presets.name_prefix: ""` really
suppresses that prefix for pipelines (the runner finds the pipeline under either
name, so the twin problem is covered either way); `VERSION AS OF` on the `_slots` streaming table
(the seed and the initial state) and the timestamp start of the Kafka, Event Hubs
(Kafka endpoint) and Kinesis sources; whether `presets.name_prefix: ""` turns the
development prefix off on the CLI in use; Debezium encodings beyond the defaults
(`decimal.handling.mode=precise` / `string`, `time.precision.mode=adaptive`).

## DAB deployment

`scripts/deploy.sh` renders `resources/_generated_triple_slots.yml` (gitignored,
picked up by `include: resources/*.yml`) with
`scripts/_internal/_render-slot-pipelines.py` right before `bundle validate`: one
Lakeflow pipeline resource per **published** domain version whose requested mode is
`incremental`, `microbatch` or `realtime` and whose mappings compile into at least
one row-local slot flow. The resource is what `SlotPipelineRunner.ensure` would
create (name `ontobricks-slots-<domain>-v<n>`, catalog / schema of `_slots`,
serverless, channel, not in development mode, continuous for `microbatch` /
`realtime`), so the runner finds the deployed pipeline by name and only creates one
when it is absent. Three rules keep DAB and the runner from fighting: the library is
the SQL file the runner uploads at build time
(`/Workspace/Users/<app principal>/.ontobricks/pipelines/<name>.sql`); the
`ontobricks.slot_sql_sha256` fingerprint is the one the deployed pipeline already
carries (`""` for a new pipeline), so DAB never stamps a fingerprint for SQL the
pipeline has not run (that would skip the full refresh a mapping change needs); and
the app principal (`APP_SP_CLIENT_ID`, looked up from the app) gets `CAN_MANAGE`.

When the registry or the workspace cannot be reached, or the app principal is
unknown, nothing is rendered and the previous file is **kept**: removing it would
make the deploy delete every pipeline it deployed before, with its `_slots`
streaming table. For the same reason a version whose mappings no longer compile
keeps its previously rendered resource (logged). A version that stops being
published, or a domain switched to `full`, drops out of the file and its pipeline is
deleted by the next deploy. DAB declares the *requested* mode; when a build resolves
another (e.g. untracked sources), the runner adjusts the pipeline and the next
deploy re-applies the declared one.

**Adopting a pipeline the app created.** A slot pipeline the runner created before
the bundle declared it (same name, no bundle deployment metadata) would get a twin
on the same `_slots` table from `bundle deploy`. The renderer lists those in
`resources/_generated_triple_slots.bind` (`<resource key> <pipeline id>`, gitignored),
and `deploy.sh` runs `databricks bundle deployment bind <key> <id> --auto-approve`
for each before the deploy (Lakebase targets only); a CLI without `bundle deployment
bind` skips the step with a notice. A pipeline another bundle manages is never listed.

**Development mode.** A target in `mode: development` would deploy every resource as
`[dev <user>] <name>` and every pipeline with `development: true`, so the adopted
pipeline would be renamed under the runner. The repo's development targets (`dev`,
`dev-lakebase`, the generated `dev-lakebase-<id>`) therefore set — and any other
development target should set —

```yaml
presets:
  name_prefix: ""
  pipelines_development: false
```

As a safety net the runner also finds a pipeline deployed as `[dev <user>] <name>`
(an exact name wins; two distinct candidates are refused with a `ConflictError`),
keeps that deployed name when it updates the pipeline, and reads the fingerprint
from it; the renderer prints a notice when a deployed pipeline carries a prefix.

**Identities.** Bundle-deployed pipelines run as the deployer, and their library is
the SQL file under `/Workspace/Users/<app principal>/.ontobricks/pipelines/`: the
deployer needs CAN_READ on that folder (the app principal owns it), and the app
principal keeps CAN_MANAGE on the pipeline to update and run it.

The realtime stream job (`resources/triple_rtm.job.yml`) is always deployed and
inert — no schedule, no trigger; the app starts it per realtime domain version. DAB
cannot include a resource conditionally, so `var.realtime_enabled` (default
`false`) only tags the deployed job; the app refuses to start a stream while
`ONTOBRICKS_REALTIME_ENABLED` is off. Its cluster is sized by
`var.realtime_num_workers`, `var.realtime_shuffle_partitions`,
`var.realtime_spark_version` (resolved by `deploy.sh`, see [Realtime](#realtime)) and
`var.realtime_node_type_id`. The job itself is cloud-neutral: the node type has no
default and the AWS targets (`dev`, `dev-lakebase` and the generated
`dev-lakebase-<id>`) set `i3.xlarge` with on-demand `aws_attributes`; an Azure or GCP
target sets its own node type (e.g. `Standard_D4ds_v5`, `n2-highmem-4`) and
`azure_attributes` / `gcp_attributes`. `scripts/bootstrap/app-permissions.sh` grants
the app principals CAN_MANAGE_RUN on it, as on the graph analytics job.
