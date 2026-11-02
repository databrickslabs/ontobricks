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
- [Reconciliation](#reconciliation)
- [Preview enrolment](#preview-enrolment)
- [Known limitations](#known-limitations)
- [Realtime (deferred)](#realtime-deferred)
<!-- /toc -->

## Modes

| Mode | Slot pipeline | `_data` relation | Lakebase sync schedule | Use when |
|---|---|---|---|---|
| `full` | none | Delta table rewritten by CTAS (or a pass-through view on the Lakehouse engine) | snapshot | Default; small graphs, sources without row tracking, any backend |
| `incremental` | Lakeflow SDP pipeline, **triggered** once per build | materialized view, `REFRESH POLICY INCREMENTAL` (Enzyme) | triggered | Sources tracked, freshness = one build |
| `microbatch` | the same pipeline run **continuously** (a triggered bootstrap first) | the same materialized view, refreshing itself between builds: `TRIGGER ON UPDATE` (at most ten sources) or every 15 minutes | continuous | Sources tracked, minutes freshness between builds |
| `realtime` | reserved | — | — | Not built in this release; resolves to `microbatch` (`realtime_not_available`) |

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
… STORED AS SCD TYPE 1` flow folds them into `triplestore_<domain>_V<n>_slots`.
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

1. `realtime` → `microbatch` — `realtime_not_available` (always, in this release).
2. `incremental` / `microbatch` → `full` — `serverless_unavailable` when
   `settings.serverless_pipelines_enabled` is off (environment variable
   `ONTOBRICKS_SERVERLESS_PIPELINES_ENABLED`, default on), or `backend_not_eligible` when
   the domain's graph backend is not one of `databricks`, `lakebase`, `neo4j`.
3. `microbatch` → `incremental` — `row_tracking_missing`.
4. `incremental` → `full` — `row_tracking_missing`.

Row tracking is probed with `SHOW TBLPROPERTIES <table> ('delta.enableRowTracking')`
on every source table of the compiled slot pipeline (`SourceReadiness.check`);
a domain whose mappings are all batch has no source tables and counts as tracked.
A table owner makes a source ready with `ALTER TABLE <table> SET TBLPROPERTIES
(delta.enableRowTracking = true, delta.enableDeletionVectors = true)`. A failed probe or a failed slot pipeline / MV build
is `incremental_failed:<ErrorClass>` and the build continues on the full path.

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

## Reconciliation

`POST /dtwin/incremental/reconcile` (builder role) compares the domain's `_slots`
streaming table with its `_data` relation the way the MV reads the slots:
distinct-triple row counts and `sum(xxhash64(subject, predicate, object))` of
both, blank objects left out, `drift` when they differ, and the slot counts per
`map_id`. The scans run on the build warehouse as the caller. `_data` is the
union of the slots and the batch branches, so the response carries
`batch_branches_present` (derived from the domain's *current* mappings) and
`slots_run_ahead` (a `microbatch` domain, whose continuous pipeline keeps
moving `_slots` between builds); `drift` is meaningful
(`drift_meaningful: true`) only when both are false. A domain without a slots table answers 404; an unreadable
catalog or a missing warehouse client answers 502, not 404.

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

## Known limitations

Behaviour in this release:

- `augment_mappings_from_config` assigns a `sql_query` to every configured
  entity, so most real mappings classify as batch; the slot pipeline engages only
  for table-based mappings and `incremental` is then the `_data` MV over the batch
  branch. Follow-up: treat single-table `SELECT … FROM t` queries as row-local.
- `microbatch` freshness between builds is the `_data` view's own refresh: on every
  source update (at most once a minute) for at most ten sources, otherwise every 15
  minutes — and every domain with a `sql_query` or join mapping (most real ones,
  see above) gets the 15-minute schedule.
- Neo4j always streams in full (`inferred_not_separable`).
- The MERGE fallback engages for any slot-pipeline failure in `incremental`
  (every runner message mentions the pipeline); `microbatch` falls back to CTAS.
  The MERGE skip only fires for all-row-local domains. A MERGE build sets the table
  properties only when `SHOW TBLPROPERTIES` lacks one, and adds `object_hash` to an
  existing MERGE table that lacks it (a domain that became Lakeflow-synced).
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

## Realtime (deferred)

`realtime` is reserved: the enum value exists, the resolver maps it to
`microbatch`, and the slot model is the one a Real-Time Mode stream would feed.
The designed path — Kafka/Event Hubs source, a `SlotDiffProcessor` keyed by
source row, direct upserts into a Lakebase `_live` table, a parallel pipeline
consumer for the lakehouse copy and a nightly reconciliation — is additive to
what ships here and is not built in this release.
