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

| Mode | Slot pipeline | `_data` relation | `_graph` relation | Lakebase sync schedule | Use when |
|---|---|---|---|---|---|
| `full` | none | Delta table rewritten by CTAS (or a pass-through view on the Lakehouse engine) | union VIEW | snapshot | Default; small graphs, sources without row tracking, any backend |
| `incremental` | Lakeflow SDP pipeline, **triggered** once per build | materialized view, `REFRESH POLICY INCREMENTAL` (Enzyme) | materialized view over `_data ∪ _inferred` on the Lakehouse engine, union VIEW on Lakebase / Neo4j ([below](#the-_graph-relation)) | triggered | Sources tracked, freshness = one build |
| `microbatch` | the same pipeline run **continuously** (a triggered bootstrap first) | the same materialized view, refreshing itself between builds: `TRIGGER ON UPDATE` (at most ten sources) or every 15 minutes | the same, `TRIGGER ON UPDATE` (it refreshes when `_data` does); Lakehouse//RT reads of a domain without batch mappings skip it and read `_slots ∪ _inferred` directly ([fast path](#lakehousert)) | continuous | Sources tracked, minutes freshness between builds (pipeline cadence on the Lakehouse//RT fast path) |
| `realtime` | the `microbatch` pipeline fed by the domain's Kafka-compatible topic instead of the change feed, plus a Real-Time Mode stream job from that topic into Lakebase `_live` | the `microbatch` view | union VIEW (realtime is Lakebase-only) | continuous (and `_live` read live by the union view) | Lakebase domains with a change-event topic, seconds freshness; needs `ONTOBRICKS_REALTIME_ENABLED` and the domain's `realtime_source` ([Realtime](#realtime)) |

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

### The `_graph` relation

`_graph` is the union of `_data` and `_inferred` that graph queries read
(`DeltaFlatStore` reads `_graph` whenever it exists, `_data` only for asserted-only
reads; a `microbatch` domain on the [Lakehouse//RT fast path](#lakehousert) reads the
slots instead). On the **Lakehouse engine** an `incremental` / `microbatch` build makes it
a materialized view too, so Lakehouse//RT can read it (RT reads no plain views):
`CREATE OR REPLACE MATERIALIZED VIEW <graph> … REFRESH POLICY INCREMENTAL` over
`SELECT subject, predicate, object FROM _data UNION ALL SELECT … FROM _inferred`,
row tracking on `_inferred`, its own definition fingerprint
(`ontobricks.mv_sql_sha256`, which also tells it from the VIEW) and, when `_data`
refreshes itself (`microbatch`), `TRIGGER ON UPDATE` (the CREATE MATERIALIZED VIEW
reference accepts materialized views as upstreams). A build whose definition did
not change keeps it (`GraphRelation.ensure_materialized`, dropping a union VIEW
first, `TwinGraphRelation.after_data`); the build's inferred reset, which follows the
`_data` refresh, refreshes it — once per build. A scheduled tick that refreshes
`_data` in place refreshes it next. Every write to `_inferred` — the store's app
writes (reasoning, cohorts), the inferred purge, `ReasoningService.materialize_to_delta`
into the companion — runs a synchronous `REFRESH MATERIALIZED VIEW` through one
method, `GraphRelation.refresh_after_write`, before it returns, so the graph caches the
writers rebuild next (adjacency, entity search, properties) read the new union
(best-effort: a failed refresh is logged and the next write, build or trigger catches
up). A union VIEW needs none of this and is left alone. `full`, the MERGE
fallback, the standalone Delta build and the other engines (whose readers use their
own store) keep the union VIEW; a switch drops the other kind first, as for
`_data`. The build-run trace records which one it is (`graph_relation`: `mv` /
`view`).

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
   bootstraps from `_slots`, which the change feed fills; a topic-fed `realtime`
   pipeline fills it from the topic, so its sources count as tracked).
4. `incremental` → `full` — `row_tracking_missing`.

Row tracking is probed with `SHOW TBLPROPERTIES <table> ('delta.enableRowTracking')`
on every source table of the compiled slot pipeline (`SourceReadiness.check`);
a domain whose mappings are all batch has no source tables and counts as tracked,
and so does a `realtime` slot pipeline fed by its topic (`spec.needs_row_tracking`
false, `TwinSlotFeed.feed_tables`): it reads no change feed, so neither the
build, the tick nor the preflight probes its sources.
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
build's `incremental_failed:ValidationError` decision), `previews` (the slot
pipeline's Lakeflow channel) and `read_relation` (`slots_live` / `graph`, as in
[Status](#status)). `mappings` gives each mapping's freshness class, one entry
per class of an entity mapping and per relationship mapping, in the slot
compiler's order: `{class_uri, predicate, map_id, kind, reason}`, where `kind` is
`slot` for a row-local mapping (it feeds the slot pipeline, so `microbatch`
streams it; `map_id` is its flow's id) and `batch` for one that `_data`'s batch
branch recomputes incrementally (`map_id` `null`; `reason` is the compiler's own,
`SlotProjectionCompiler.batch_reason`: `reference predicate (join)`, `sql_query
source`, `relationship (several objects per subject share one slot key)`, `no
table`, ...). `slot_mappings` / `batch_mappings` count them; mappings that do not
compile give `[]` and `null` counts. The counts are per class, so a TriplesMap
with two classes counts twice here and once in the trace's `batch_mappings`.
The mappings are derived as a build derives them
(`TwinIncrementalBuild.mappings_for`): the R2RML is session-only, so a domain
opened from the registry has none until it is regenerated from the saved
mappings — the derivation regenerates it first (`ensure_generated_content`, as
every build and query path does), so the preflight, the reconciliation and a
`realtime` start never report a mapped domain as having no mappings.
`lakehouse_rt` (Lakehouse engine only, `null` on the others) is
`{query_warehouse_configured, use_sea, fast_path}`: whether
`graph_engine_config.lakehouse.warehouse_id` names a query warehouse, whether "Use
Lakehouse//RT for queries" is on, and `read_relation` again. Lakehouse//RT is
offered only for tables without row filters or column masks; `_slots`, `_data`,
`_graph` and `_inferred` are created by the app without either, so the preflight
checks nothing for them. A topic-fed pipeline reports
`row_tracking_required: false` and lists its sources unprobed (`row_tracking:
null`), which the panel does not flag. It runs the probe on the build warehouse as the
caller; a probe that fails is an error, not "untracked". `POST
/dtwin/incremental/enable-row-tracking` (builder role) runs the `ALTER TABLE`
above on the untracked sources, or on a `tables` subset of the domain's own
slot-pipeline sources (any other name is refused before anything runs). It runs
as the caller, so Unity Catalog decides who may alter which table, and reports
`{table, success[, error]}` per table. The Domain information panel shows the
answer under the materialization mode (Readiness line, "Enable row tracking"
button when a source is untracked; the action needs the builder role), a
"Mappings: n row-local (microbatch-eligible), m batch-incremental" line whose
tooltip names each batch mapping (class or predicate local name) and its reason,
and on the Lakehouse engine a "Lakehouse//RT: query warehouse set / not set · RT
reads on / off" line (with "fast path (live slots)" when reads take the slots).

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
mode**. A topic-fed `realtime` pipeline reads no change feed (and a full refresh
would replay its topic), so its sources' history is not read. A reconciliation drift (see [Reconciliation](#reconciliation)) does too,
and also refreshes `_data` with `REFRESH MATERIALIZED VIEW … FULL`. The reason is
recorded as `slot_full_refresh_reason` (`source_overwritten` / `reconcile_drift` /
`slot_update_failed` — a slot update that failed would resume from the same
checkpoint and fail again, so the next one starts over);
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
  the stream, or a base version newer than `_data`), `inferred_unreadable` (the
  Neo4j inference list below could not be read) or `inferred_not_separable` (a
  store with neither way to purge its inferences). The store's inferred triples
  are purged, so reasoning starts from a clean slate exactly as after a full
  build: Lakebase purges its `__app` companion after the writes (and rebuilds the
  graph cache). **Neo4j** cannot mark a triple as inferred (a literal is a node
  property, `rdf:type` a label), so its app writers — reasoning and cohorts —
  list every batch in the Delta `_inferred` table *before* writing it to Neo4j
  (`InferredTripleLedger`, kept for a domain that is not `full`). Before the net
  changes the build (`TwinListedInferencePurge`) deletes from Neo4j every listed
  triple that `_data` does not assert, `SELECT subject, predicate, object FROM
  <inferred> VERSION AS OF <v> EXCEPT SELECT subject, predicate, object FROM <data>
  VERSION AS OF <latest>`, streamed in batches of 500 into `delete_triples` — a
  triple both inferred and mapped stays. Both relations are read at pinned versions:
  `_inferred` at its version read up front, `_data` at the version the net changes go
  up to. Neo4j keeps one value per property key, label set and name, so deleting a
  listed literal also unsets an asserted value of the same key: the `_data` triples
  on the subject and predicate of a deleted one are written again (`LEFT SEMI JOIN`),
  then only the rows of `_inferred` version `<v>` leave the list (`DELETE … WHERE
  EXISTS (… VERSION AS OF <v> …)`, not a `TRUNCATE`: a reasoning run listing during
  the purge keeps its rows) and the net changes are applied. A listed triple whose
  Neo4j write failed is harmless (its delete removes nothing). Such a store records
  `inferred_listed` in the trace and only continues from a base run that did: the
  inferences of an earlier build were never listed, so the first build after the
  upgrade streams in full (`no_previous_version`).

## What a build does per backend

**Lakehouse / Delta engine.** `_data` is the graph. `full`: CTAS. `incremental` /
`microbatch`: slot pipeline + `_data` MV (or the MERGE table) and the `_graph` MV
over it ([The `_graph` relation](#the-_graph-relation)); `_inferred` is
reset in every mode, except by a MERGE build skipped for unchanged sources, which
completes before the reset (the inferences of the unchanged graph stay); no graph
stream (`apply_mode = none`). Reads may go to a
Lakehouse//RT warehouse (below).

**Lakebase, managed_synced.** `_data` is built as above and the Lakeflow synced
table is registered with the mode's schedule (capped by `sync_table_mode`) on
**`_slots`** — key `(map_id, source_key, predicate)` — when the schedule is triggered
or continuous and the slots carry the whole graph (a slot pipeline ran and the build
counted no join / aggregate / `sql_query` / relationship mapping, `batch_mappings = 0`),
otherwise on **`_data`** (`subject, predicate, object_hash`). A streaming table has a
change feed every workspace can sync from; the `_data` materialized view only has one
where the MV change-feed preview is enabled — measured live, a triggered or continuous
registration on the MV creates and snapshots, then its sync pipeline fails
(`MATERIALIZED_VIEW_CDF_REQUIRES_EXTERNAL_METADATA`). When that happens the build
re-registers the same view as a **snapshot** and syncs again
(`TwinLakebaseSync.trigger_and_wait`; trace `sync_mode_effective = snapshot`,
`sync_fallback_reason = mv_change_feed_unavailable`); a MERGE-table `_data` keeps its
schedule (row tracking gives it a feed). Postgres gets two views over the `_sync`
table: the union view readers query (`DISTINCT` and non-blank over a slots source) and
the **asserted view** `<graph>_asserted` — the synced triples alone, distinct and
non-blank — which `include_inferred=False` reads, the asserted entity-search rebuild,
BFS expansion and the reconciler use (`LakebaseFlatStore.synced_table_name`), so a
slots source reads like `_data` to them. A drifted registration (other source or
schedule — including a fallback build, whose slots are not live, going back to `_data`)
is deleted and recreated: a full resync; both views are dropped right before that
delete and recreated after. A continuous table is waited for until it streams
(`SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE`). `apply_mode = lakeflow_sync`.

**Lakebase, app_managed.** `_data` is built as above; the app writes the
`<graph>_sync` table itself: a net-change apply when a matching base run exists
(`delete_from_sync_iter` then `bulk_load_into_sync`), otherwise drop + full stream.

**Neo4j.** `_data` is built as above; an incremental `_data` reaches the graph as
net changes after the listed inferences are purged (above), otherwise by drop +
full stream.

## Scheduled builds

A scheduled tick (`registry/schedule/build.py`) resolves the domain's mode with the
same resolver and the same row-tracking probe as the main build
(`TwinBuildPostMaterialize.decide_mode_for`), and counts a decision the main build
promotes to the MERGE fallback (`TwinMergeFallback.promotes`) as `incremental`, so a
tick and a main build register the Lakebase synced table on the same source and
schedule and never alternate between triggered/continuous and snapshot. The source
follows the same rule too: a tick that kept the `_data` view (the slot pipeline is
live) syncs `_slots` when the compiled spec has no batch mapping, `_data` otherwise
(`_tick_sync_plan`); a tick that rebuilt `_data` syncs it. A failed sync of the kept
view falls back to a snapshot of it as the main build does.

The tick decides (and probes) before it writes anything, so a failed probe drops
nothing. When `_data` already is the incremental main build's materialized view the
tick refreshes it in place instead of replacing it: `incremental` runs one triggered
update of the slot pipeline, then `REFRESH MATERIALIZED VIEW`; `microbatch` only
refreshes (its continuous pipeline keeps `_slots` current). A `_graph` materialized
view is refreshed right after it. On the **Lakehouse engine**, whose graph is `_data`,
the tick does the same, empties `_inferred` first (as the main build's reset; the
inferences were derived from the previous data) and streams nothing into the store; any
other `_data` there is rebuilt from the gateway VIEW the way the main build builds it
(a table, or the domain's pass-through view). It refreshes in place only while
the view still has the current definition: the tick recompiles the mappings and
compares the MV definition fingerprint and the slot pipeline's SQL fingerprint, so
mappings edited since the last main build are picked up by the CTAS (the next main
build recreates the view) instead of being silently ignored. A `full` or
MERGE-promoted domain, a `_data` that is not our MV, or a missing slot pipeline get
the CTAS from the gateway VIEW, as before.

## Lakehouse//RT

In the incremental and microbatch modes `_data` and, on the Lakehouse engine,
`_graph` become materialized views that Lakehouse//RT can read directly (RT reads
managed tables, materialized views and streaming tables, not plain views): graph
queries read `_graph`, so RT serves the union of mapped and inferred triples. A
source change reaches it with `_data`'s refresh (the build, the tick, or
`TRIGGER ON UPDATE` in `microbatch`) and `_graph`'s after it: two chained `TRIGGER
ON UPDATE` refreshes, each at most once a minute, so in `microbatch` `_graph` can
trail `_data` by up to a minute more. An inferred triple is in `_graph` once its
write returns (the write waits for the refresh).

**Fast path (`microbatch`).** Measured live, that chain shows a change minutes after
the continuous slot pipeline committed it to `_slots` (217–383 s, [Known
limitations](#known-limitations) › Live verification). A `microbatch` domain read
through Lakehouse//RT therefore skips both views and reads the slots, so a change is
visible at the pipeline's commit cadence (seconds). The swap happens in the store:
`DeltaFlatStore._sql_relation` — every read template's `FROM`, and
`sql_table_reference` for callers that build their own subquery (the `dt_triples` type
filter, the bridge seed of the node context, the find seed) — turns this domain's
`_graph` FQN into

```sql
(SELECT DISTINCT subject, predicate, object FROM <_slots> WHERE object IS NOT NULL AND TRIM(object) != ''
 UNION ALL SELECT subject, predicate, object FROM <_inferred>)
```

(one line) and its `_data` FQN into the first branch alone
(`(SELECT DISTINCT subject, predicate, object FROM <_slots> WHERE …)`); every other name
(the companions, `_inferred`, logical graph names, other domains) is left alone, and
`physical_table_id` always answers the table name. The result is read-only SQL: a
writer (the reasoning `INSERT INTO`) passes a logical name, which is never swapped. That is
exactly `_graph`'s multiset: `_data`'s `GROUP BY` over the non-blank slots, `UNION ALL`
the inferred. The relation carries no alias — the templates alias it themselves
(`FROM {relation} t`) and Spark names an unaliased subquery; a unit test parses every
read template with both relations (sqlglot, `databricks` dialect).
`effective_graph_query_table` stays a pure name lookup (`_graph` / `_data`), so catalog
probes, status, display names and messages keep the table name; full-graph dumps and
counts (`query_triples`, `count_triples`) read through the store too, without the
statement timeout but with the guardrail fallback below.
`GraphReadRelation.decide` takes the fast path (`read_relation = slots_live`) only when:

- the domain is on the Lakehouse engine, requested `microbatch` (or `realtime`, which
  runs as `microbatch` there) and "Use Lakehouse//RT for queries" is on
  (`graph_engine_config.lakehouse.use_sea`) — otherwise no lookup at all; and
- `_data`'s stored definition fingerprint (`ontobricks.mv_sql_sha256`, one `SHOW
  TBLPROPERTIES`) equals the one of the `microbatch` view over the slots alone
  (`MaterializedViewBuilder.data_mv_sha` of the slots, no batch branch, `TRIGGER ON
  UPDATE`). That one value says `_data` is a materialized view, that it reads exactly
  the non-blank slots (a join, aggregate, `sql_query` or relationship mapping adds a
  batch branch and changes it), and that it is the self-refreshing variant (the
  `incremental` view has no refresh clause and another fingerprint). A cancelled or
  failed build that already replaced `_data`, or a scheduled tick or a fallback that
  rebuilt it by CTAS (no property), therefore turns the fast path off.

The lookup runs on the reading store's own client (the query warehouse, as the
reader), wherever the store runs that read: in the `run_blocking` worker thread for the
store methods the routes offload, and inline in `dt_triples` (all of its store reads)
and in the bridge seed, as those reads already ran. It never opens a build-warehouse
client. An absent `_data` reads as `graph` quietly; any other failing lookup is logged
at warning and reads as `graph`. The decision is cached per `_data` table (one per
domain version) for 30 s (`GraphReadRelation.TTL_S`); a lookup that was in flight
across a cache drop can re-cache the old answer for at most that long, in its process
only. The process that changes `_data` drops its cache at once:
`TwinIncrementalBuild._leave` (leaving the incremental modes or falling back),
`_apply_views` once a new definition is in place, and `materialize.apply_data_relation`
when a full build, a fallback or a scheduled tick replaces the view by a table or a
pass-through view. Other workers see the change within 30 s. Reads answered from the
graph-index companions (`_entity_search`, `_adj_*`, `_props`, when the search cache
serves them) keep the freshness of their last rebuild, and a rebuild reads `_graph` /
`_data` themselves, never the slots (it follows a refreshed `_data`). Data-quality and
SHACL reads keep the views too. The remaining difference from `_graph` is the grants:
the reader needs `SELECT` on `_slots` and `_inferred` themselves, not only on `_graph`
(the build grants nothing on these relations; grant them on the schema). Verified live on
a Lakehouse//RT warehouse ([Live verification](#known-limitations)): the fingerprint probe
runs there (a refusal would keep the views, with a warning every 30 s), every read shape
the store emits runs over the relation, and a source commit is queryable through RT
14 s median / 23 s p95 after it. What RT does **not** run is a recursive CTE
(`[UNSUPPORTED_FEATURE] Recursive CTEs is not supported in Lakehouse//RT`): the BFS and
transitive-closure templates therefore take the build-warehouse fallback below on every
call (`last_read_route = "build-fallback"`), at the views' freshness, so a domain whose
reads are mostly traversals gains little from the toggle. Each reference to the relation
re-reads the slots with a `DISTINCT`, so measure a domain's heavy reads (unfiltered
counts, dumps) on its RT warehouse before turning the toggle on.

Graph reads go to the query warehouse (`graph_engine_config.lakehouse.warehouse_id`
plus "Use Lakehouse//RT for queries"). A heavy traversal can trip an RT guardrail
(2-minute runtime, 300 GB shuffle, statement timeout — see the Lakehouse//RT
documentation for the current values) and `DeltaFlatStore.execute_query` retries
that SELECT/WITH once on the build warehouse, as the same caller (same OBO token),
under the graph statement timeout (a dump or count without it) — the same SQL, so a
fast-path read reads the slots there too. Writes, other errors and a build warehouse
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
| `sync_source`, `sync_source_kind`, `sync_mode_effective` | what the Lakebase synced table was registered on (`slots` or `triples`), and its schedule as synced |
| `sync_fallback_reason` | `mv_change_feed_unavailable` when a triggered / continuous sync of the `_data` materialized view failed and the table was re-registered as a snapshot of it ([per backend](#what-a-build-does-per-backend)); `""` otherwise |
| `strategy` | `mv`, `merge`, `ctas` or `""` |
| `apply_mode`, `apply_fallback_reason` | `net_changes`, `full_stream`, `lakeflow_sync`, `none`; why a net apply was not done |
| `data_table_version`, `data_table_id`, `fingerprint` | the `_data` version/table id the store matches; the MERGE fingerprint |
| `source_versions`, `source_table_ids` | each slot-pipeline source's Delta version and table id read before the last completed update (only on runs that reached a slot update) |
| `slot_full_refresh_pending`, `slot_full_refresh_reason` | a full refresh owed until an update completes it; `source_overwritten` / `reconcile_drift` / `slot_update_failed` (only on runs that reached a slot update) |
| `reconcile_drift` | a scheduled tick's reconciliation found drift (tick runs only) |
| `live_drift` | a realtime tick found `_live` and the slots apart (tick runs only; restart the stream) |
| `realtime_run_id`, `realtime_state` | the stream job run a `realtime` build started (or a later build stopped), its state (`PENDING`, `RUNNING`, `CANCELLING`, `failed:<Error>`); `""` otherwise |
| `graph_relation` | `mv` when the build made `_graph` a materialized view, `view` for the union VIEW |
| `inferred_listed` | the store lists its inferences in `_inferred` (Neo4j); a later net apply needs a base run with it |
| `batch_mappings` | how many mappings the slot pipeline left to `_data`'s batch branch (`null` when no incremental build ran); the [Lakehouse//RT fast path](#lakehousert) needs `0` |
| `slots_changed` | `{rows_output, rows_upserted, rows_deleted, full_refresh}` the build's slot pipeline update wrote: `num_output_rows`, `num_upserted_rows` and `num_deleted_rows` summed over the update's `flow_progress` events (`SlotPipelineRunner.update_metrics`; a metric no event carries is `null`; counts may arrive as digit strings). Verified live: the AUTO CDC slot flows carry `num_upserted_rows` / `num_deleted_rows` on one `RUNNING` event (`QUEUED` / `STARTING` / `COMPLETED` carry none, so nothing is counted twice) and no `num_output_rows`, so `rows_output` is `null` for them; a changed source row re-upserts every slot of that row (a one-row update reported 3 upserts for its 3 slots). The update read is the first one the build ran: the full refresh of a bootstrap, else the triggered update, or the continuous update a `microbatch` build adopts once it runs (its metrics so far). `full_refresh` is `true` for a full refresh, which reports the whole table (a bootstrap: 9 upserts for 9 slot rows), not churn. `null` when no update ran (no row-local mapping, a pipeline left streaming, the full path), no event carried a metric, or the update's events could not be read to their end |
| `slot_update_id`, `warehouse_id` | the slot pipeline update `slots_changed` describes (`""` when none ran), recorded as soon as it starts so a failed update keeps it, and the build's SQL warehouse |

The events are read from the pipelines REST endpoint
(`GET /api/2.0/pipelines/<id>/events`, newest first, up to 20 pages of 250) because
the SDK's `PipelineEvent` drops `details`. Reading stops at the first event of
another update after the update's own (events without an update id, such as user
actions, are skipped); 20 pages without reaching that end record `null` (with a
warning when the update's events had begun) rather than a partial sum. A read that
fails is a warning and never fails the build. The KPI "build DBUs per build" is not computed by the app: the
trace carries the pipeline id, update id and warehouse id (`slot_pipeline_id`,
`slot_update_id`, `warehouse_id`, with the run's `started_at` / `finished_at`),
which join to `system.billing.usage` (`usage_metadata.dlt_pipeline_id` /
`dlt_update_id`, `usage_metadata.warehouse_id` over the run's window).

The Knowledge Graph › Runs cockpit shows the effective mode (with the requested
one and the fallback reason when they differ), the strategy / refresh technique
and the apply mode per run, and a "Materialization" block in the run details
(with "Slots Changed" next to the refresh technique).
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
| `read_relation` | What Lakehouse graph reads take now: `slots_live` ([Lakehouse//RT fast path](#lakehousert)) or `graph` (also for a `full` domain and every other engine, answered without a lookup) |
| `slot_pipeline` | `{name, id, state}` of this version's slot pipeline (`SlotPipelineRunner.get_state`; `absent` without one) |
| `data`, `slots` | `{table, kind, version, committed_at}`: kind `mv` / `table` / `streaming_table` / `view` / `absent` from `information_schema.tables`, last commit from `DESCRIBE HISTORY … LIMIT 1`. A materialized view's history cannot be read (`EXPECT_TABLE_NOT_VIEW`), so an MV `_data` answers `version` / `committed_at` `null` with an `error` saying so; `_slots` (a streaming table) and a MERGE-fallback `_data` table carry their commit |
| `freshness_lag_s` | Now minus the last `_data` commit (`null` when unknown — always for an MV `_data`, see above) |
| `lakebase_sync` | `{synced_table, state, message}` of a `managed_synced` store (`null` otherwise) |
| `last_build` | `{status, build_kind, finished_at, materialization}` of the newest build run |

Each section is read on its own: a failing read puts `error` (and `unknown`) in
its section and leaves the others. A `full` domain is answered without any
warehouse, pipelines or registry call (every section `null`), so the build
panels never wake a warehouse for it. The scans run on the build warehouse as the
caller. The digital twin build panels (Lakebase/Neo4j and Lakehouse) show it as
a compact "Incremental status" card, hidden for a `full` domain; its "Last build"
line gives the strategy, refresh technique and apply mode, then "slots changed:
n rows, u upserted, d deleted" (with "(full refresh)" for a full refresh) when the
build recorded `slots_changed`.

**Overview.** `GET /dtwin/incremental/overview` (`TwinIncrementalStatus.overview`)
answers the KPI "number of domains per mode, and fallback events by reason" for the
whole registry, without any warehouse or pipelines call. It is an app-level ops KPI
over every domain, so it needs the app **admin** role (a builder of the loaded
domain is refused). It returns `{domains_per_mode: {<effective mode>: n},
fallbacks_by_reason: {<mode_fallback_reason>: n}, domains: [{name, version,
mode_requested, mode_effective, strategy, last_build_at, fallback_reason}],
domains_skipped: n}`. Every domain the registry lists (`list_domain_details_cached`)
counts with its newest *main* build run among its newest 50 runs of any version
(`TwinBuildRunHistory.newest_main_run`: scheduled ticks and runs without
materialization facts are skipped); the newest main run decides whatever its status
and version, so a failed run or a rebuild of an older version can set a domain's
mode. `strategy` (`mv` / `merge` / `ctas`, `""` when not recorded) tells an
`incremental` maintained by MERGE from one maintained by the materialized view. A
domain without a main run — never built, only ticks lately, or its runs unreadable
(the registry store answers `[]` on an error) — is left out and counted in
`domains_skipped`, the signal that the counts are partial. The cost is one
`load_build_runs(limit=50)` per domain (one registry query each): an admin KPI, not
a hot path. A fallback counts once per domain whose newest main run fell back, not
once per historical event. There is no UI for it yet.

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
- `sync_data`: the row count of the Lakebase asserted view over the synced table
  (`managed_synced` stores only; distinct non-blank triples whether the table was
  registered on `_data` or on `_slots`; counts, no hash). A store that cannot be
  counted is left out.

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
  and the **materialized-view change data feed** (`table_changes` on an MV, a Beta)
  used by the net-change apply and by an MV-sourced triggered / continuous synced
  table. Its documented enabler is the table property
  `'pipelines.externalMetadata.enabled' = 'true'` ("external data access to
  streaming tables and materialized views"), which the `_data` and `_graph` views
  carry from this release; the build also runs `REPAIR TABLE … SYNC METADATA`
  after creating a view (best-effort). Measured live on the integration workspace:
  with the property, `REPAIR TABLE … SYNC METADATA` is accepted (it was refused
  without it) but the external metadata is never materialised — `DESCRIBE HISTORY`
  and `table_changes` keep failing with `DELTA_LOG_FILE_NOT_FOUND` at the view's
  `_external_metadata/_delta_log` after ten minutes of polling, refreshes and
  repairs — so the feed stays unreadable there and the callers fall back as
  described. An existing view gets the property on its next `CREATE OR REPLACE`
  (a definition change), not on a refresh.
- **Materialized view → Lakebase triggered/continuous synced tables** — the same
  external-metadata preview: the synced-table pipeline reads the MV's change feed
  (measured live, [Live verification](#known-limitations)). Not needed when the slots
  carry the whole graph (the sync reads the `_slots` streaming table); otherwise the
  build falls back to a snapshot of the view and records `mv_change_feed_unavailable`.
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

The same variables drive `tests/external/test_incremental_readiness_live.py`, which runs the
readiness and status reports the two endpoints serve (`TwinIncrementalReadiness.report`,
`TwinIncrementalStatus.report`) on the same fixture tables with the real row-tracking probe,
`information_schema` lookups, Pipelines-API lookup and `GraphReadRelation` decision — only the
R2RML extraction is the test's fixture. The nightly workflow runs both in its blocking step.

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

**Freshness and drift.** `scripts/measure_freshness.py` (`FreshnessProbe`) measures
the design's KPIs on a domain that already runs in its mode. It updates one row of a
source table `--changes` times (default 5, `--spacing` 10 s apart), each time to a
new marker value of a column the domain maps as a plain literal — a text column takes
`ob-freshness-<run>-<i>`, a numeric one a negative number near the bottom of its
type's range, where no real value sits (`-2 000 000 000 - k` for an integer, `-32 000
- k` for a smallint, `-9 900 000 - k` for a double, `-(10^(p-s) - 1) + k` for a
`decimal(p, s)` with at least six integer digits), written through `CAST` and expected
as `CAST(… AS STRING)` prints it; any other type is refused before anything is written
— and polls every path — each on its own thread (one Postgres
connection per Lakebase path), timed from the write — until the marker is the object
of a triple there: `data` / `graph` (`_data` / `_graph` on the warehouse;
`--read-warehouse-id` is the Lakehouse//RT query warehouse) and `sync` / `live` (the
Lakebase synced table and `_live`, with `--pg-dsn`); `--subject` / `--predicate` (the
row's subject URI, the predicate the column maps to) hold the match to that triple.
`--paths` defaults to `data,graph`, plus `live` in `realtime` and `sync` when
`--sync-table` is set; a Lakebase path without its DSN and table is a usage error, and a
run without `data` or `graph` warns (the reconciliation may then run before `_data`
holds the last marker). It reports p50 / p95 / p99 per path (a
change not seen within `--timeout`, default 120 s, counts as never seen); once the
last change is seen it reconciles `_data` with the batch recompute (the gateway VIEW),
the synced table and `_live` (`SlotReconciler.compare`; `slots_data` is reported but
not judged, since batch mappings feed `_data` too), then restores the row and checks
the SLOs of `--mode`. Only a fixture table is updated: its name must match
`--allow-pattern` (default `*ob_it_*,*fixture*`), and a key that names no row is a
usage error (exit status 2, nothing written).

| Mode | SLO |
|---|---|
| `microbatch` | `graph` ≤ 30 s p95 (Lakehouse//RT), `sync` ≤ 60 s p95 (Lakebase) — the `graph` path polls `_graph` itself, whose chained `TRIGGER ON UPDATE` refreshes (each at most once a minute) can miss it by design; graph reads on the [fast path](#lakehousert) read `_slots` at the pipeline's cadence instead |
| `realtime` | `live` ≤ 1 s p99, `sync` ≤ 60 s p95 |
| every mode | no drift in `view_data`, `sync_data`, `live_slots` |

A path that is not configured is not judged; an `incremental` domain only moves on a
build, so only its drift is. With `--source-dsn` the row is updated on the Postgres
database a `realtime` domain's Debezium connector captures instead of the warehouse.
Every option also reads `ONTOBRICKS_FRESHNESS_<OPTION>`, with the credentials of the
EXPLAIN test:

```bash
DATABRICKS_CONFIG_PROFILE=<profile> ONTOBRICKS_EXTERNAL_WAREHOUSE_ID=<warehouse id> \
PYTHONPATH=src uv run --frozen python scripts/measure_freshness.py \
  --stem <catalog.schema.triplestore_<dom>_V<n>> --mode microbatch \
  --source-table <catalog.schema.table> --key-column id --key-value 42 --value-column name \
  [--read-warehouse-id <RT warehouse>] [--pg-dsn <dsn> --sync-table <schema.table>]
```

`tests/external/test_freshness_slo.py` (`external`) runs it from the environment and
asserts the SLOs; it is skipped without `ONTOBRICKS_EXTERNAL_WAREHOUSE=1`, the
credentials and the domain (`ONTOBRICKS_FRESHNESS_STEM`, `_SOURCE_TABLE`,
`_KEY_COLUMN`, `_KEY_VALUE`, `_VALUE_COLUMN`). The nightly `external-smoke` job runs it
as a **non-blocking** step (`continue-on-error`, a warning annotation and a step
summary with the report) when the repository variables name an integration domain:
`ONTOBRICKS_INT_FRESHNESS_STEM`, `_MODE` (default `microbatch`), `_SOURCE_TABLE`,
`_KEY_COLUMN`, `_KEY_VALUE`, `_VALUE_COLUMN`, `_ALLOW_PATTERN`, `_SYNC_TABLE` and
`_LIVE_TABLE`, plus `ONTOBRICKS_INT_RT_WAREHOUSE_ID` (the Lakehouse//RT query
warehouse) and the repository **secret** `ONTOBRICKS_INT_LAKEBASE_DSN` (the Lakebase
libpq DSN for `sync` / `live`). An empty variable reads as unset.

## Design traceability

Requirements of the design document (`[Design] Incremental Triple Materialization | OntoBricks`)
against this release. Two facts per row, kept apart: **Built** — ✅ as designed · ◐ with a
documented deviation · ⛔ deferred; **Live** — whether the row was exercised on the integration
workspace `fevm-ontobricks-int` or the classic sandbox `ontobricks-rtm` (Real-Time Mode, the
deployed-app journey) (✅ verified · ○ unit/property-tested only · — nothing to run).
The live evidence is under [Known limitations](#known-limitations) › Live verification.

| Phase | Requirement | Built | Live | Where |
|---|---|---|---|---|
| 1 P0 | Compute the source once (`count`, stream, Lakeflow source read `_data`) | ✅ | ○ | `TwinBuildPostMaterialize.triples_source_relation`, `SyncPlan` |
| 1 P0 | `_inferred` reset on both build paths | ✅ | ○ | reset on every build that changes base triples, and by a Lakehouse scheduled tick (which empties it before refreshing `_data` / `_graph`, as every other tick discards its store's inferences); a MERGE *skip* (no source change) has nothing to reset and keeps it |
| 1 P0 | `SlotProjectionCompiler` reuses the translator's predicate unpivot; deletes / NULL objects / pre-images → tombstones | ✅ | ✅ | SCD1 physical deletes stand in for a `deleted` column; both pipeline variants (change feed, Debezium topic) validate in Lakeflow, the change-feed one bootstraps and folds mutations to the translator's output exactly |
| 1 P0 | Slot pipeline per (domain, version) via DAB, AUTO CDC into `_slots` | ✅ | ✅ | rendered per published version into `resources/_generated_triple_slots.yml` at deploy time ([DAB deployment](#dab-deployment)); a pipeline the runner created first is bound to its resource before the deploy; a compile error keeps the rendered resource; development targets set `presets: {name_prefix: "", pipelines_development: false}` and the runner also finds a `[dev <user>] ` name. Live: the runner's create / validate / start / adopt / stop cycle (the DAB render itself is a unit test) |
| 1 P0 | `_data` / `_graph` MVs with `REFRESH POLICY INCREMENTAL`; CI check with `EXPLAIN` | ✅ | ✅ | `MaterializedViewBuilder`, `TwinGraphRelation` / `GraphRelation` ([The `_graph` relation](#the-_graph-relation)); `_graph` is an MV on the Lakehouse engine — the design wants the MV so Lakehouse//RT can read it, and only that engine's readers read `_graph`; Lakebase / Neo4j (every `realtime` domain among them) keep the VIEW their sync and stream read. Live: `EXPLAIN` incremental-eligible (`tests/external/test_mv_incremental_plan.py`, nightly), post-mutation refresh `GROUP_AGGREGATE` |
| 1 P0 | Readiness preflight, admin "enable row tracking", missing tracking degrades to `full` | ✅ | ✅ | `GET /dtwin/incremental/readiness`, `POST /dtwin/incremental/enable-row-tracking`, `row_tracking_missing`. Live: the probe (`SourceReadiness.check`) reads `row_tracking = true` on the fixture tables and `false` on one replaced by `CREATE OR REPLACE TABLE AS SELECT` (its properties are gone), which is what degrades the mode; the enable statement was run on every fixture |
| 1 P0 | Lakebase `managed_synced` triggered from `_slots` / `_data` | ✅ | ✅ | both sources: `_slots` (slot key) when the slots carry the whole graph, else `_data`; asserted-only readers go through a `DISTINCT` non-blank Postgres view; capped by `sync_table_mode`. Live: TRIGGERED from `_slots` online in 57 s; TRIGGERED from the `_data` MV registers and snapshots but its updates fail on `MATERIALIZED_VIEW_CDF_REQUIRES_EXTERNAL_METADATA` (private preview) → the build falls back to a snapshot registration and records `mv_change_feed_unavailable` |
| 1 P0 | Build-run trace: mode, refresh technique, fallback reason, slot counts changed | ✅ | ✅ | `stats.materialization`; cockpit column, details and status card; `slots_changed` from the update's `flow_progress` metrics (`SlotUpdateMetrics`), with `slot_update_id` / `warehouse_id` for the DBU join on `system.billing.usage`. Live: a bootstrap reported 9 upserted slots against 9 slot rows, a one-row source update 3 (every slot of the row) — one RUNNING event carries the totals, no double count |
| 1 exp. | Per-mapping freshness class: the UI shows which mappings are row-local (`microbatch`-eligible) and which are batch-incremental | ✅ | ✅ | readiness `mappings` (`kind` = `slot` / `batch` with the compiler's reason) + counts; "Mappings: n row-local, m batch-incremental" line on the readiness card. Live at the service layer: the fixture mappings read `slot` (Customer) / `batch` (Account, `sql_query source`), counts (1, 1); on the deployed sandbox app (scenario 06) the Domain › Backend card reads "incremental runs as incremental · Mappings: 2 row-local (microbatch-eligible), 0 batch-incremental" |
| 1 P1 | Net changes for app-managed Lakebase and Neo4j (~10 % threshold) | ✅ | ○ | `TwinNetChangeApply`; Neo4j purges the inferences its writers list in `_inferred` (`InferredTripleLedger`, `_inferred EXCEPT _data`); the MV change feed needs external metadata (private preview) — until then the apply falls back to a full stream as designed; the MERGE-table feed is live-verified (`table_changes` on the MERGE table lists exactly the merged change) |
| 1 P1 | MERGE fallback with row tracking and a no-op skip | ✅ | ✅ | `DeltaMergeMaterializer`, `TwinMergeFallback`. Live: the first build creates `_data` with row tracking / deletion vectors / change feed and merges 3 rows; an unchanged source merges 0 rows; one changed row → 1 delete + 1 insert; one deleted row → 1 delete; `table_changes` on the MERGE table lists exactly that change |
| 2 P0 | `microbatch` = continuous pipeline; mode switch without backfill | ✅ | ✅ | triggered bootstrap then continuous; fingerprint decides a full refresh. Live: the continuous pipeline lands a source change in `_slots` after 8.1 s median / 11.9 s max (n = 8) |
| 2 P0 | Source overwrites detected from `DESCRIBE HISTORY` → fall back | ✅ | ✅ | `SourceReadiness.source_overwritten_since`; `source_versions` / table ids in the trace; full refresh of the slot pipeline. Live: a plain `UPDATE` is not a rewrite; `INSERT OVERWRITE` and `CREATE OR REPLACE TABLE AS SELECT` are detected |
| 2 P0 | Lakehouse//RT read warehouse; DDL on DBSQL; Statement Execution API; guardrail routing | ✅ | ✅ | query warehouse + "Use Lakehouse//RT"; `DeltaFlatStore` reads-only fallback as the caller; a `microbatch` domain reads `_slots` ∪ `_inferred` inline ([fast path](#lakehousert), `GraphReadRelation`). Live: the app's `use_sea` client on the RT warehouse `realtime` runs the fingerprint probe and every store read shape over the relation; recursive CTEs are refused there and take the build-warehouse fallback; point reads p50 216 ms through the SEA from a laptop |
| 2 P1 | Lakehouse//RT offered only where available, and only for tables without row filters / column masks | ✅ | ✅ | readiness `lakehouse_rt` facts (query warehouse configured, RT reads on, fast path) + card line; `_slots` / `_data` / `_graph` / `_inferred` are app-created without filters or masks, so nothing to check. Live: the deployed sandbox app (no RT warehouse) answers `{query_warehouse_configured: false, use_sea: false, fast_path: "graph"}` and the card line reads "Lakehouse//RT: query warehouse not set · RT reads off" |
| 2 P0 | CONTINUOUS synced tables; union view `DISTINCT`, no tombstones | ✅ | ✅ | `ContinuousSyncWait`; Postgres union view. Live: CONTINUOUS from `_slots` online in 56 s, a source change visible in Postgres after 45.8 s (target < 60 s) |
| 2 P0 | Observability: pipeline state, last commit, freshness lag, fallback events | ✅ | ✅ | `GET /dtwin/incremental/status` + status card (incl. `read_relation`); fallback events in the trace. Live at the service layer (`tests/external/test_incremental_readiness_live.py`, nightly): the `_slots` / `_data` catalog lookups, the slot-pipeline lookup, `freshness_lag_s` and `lakebase_sync` for a fixture domain; on the deployed sandbox app (scenario 06) the Knowledge Graph › Build card reads "Slot pipeline: IDLE · _data: mv · lag unknown · _slots: streaming_table v5 · Last build: success (session) · mv / NO_OP / none · slots changed: 6 upserted, 0 deleted (full refresh)", and after the fallback build it listed `_data: table` with its commit and `freshness_lag_s: 9` |
| 2 P0 | Reconciliation: count + hash of `_slots`, `_data`, Lakebase, batch recompute; re-backfill on drift | ✅ | ✅ | `POST /dtwin/incremental/reconcile` (+ `recompute`), tick reconciliation, `reconcile_drift` → full refresh. Live: `drift: false`, identical counts and `xxhash64` sums after bootstrap and after mutations |
| 3 P2 | Prerequisites: topic config, readiness, resolver | ✅ | ○ | `RealtimeSource`, `realtime_topic_available`, readiness `realtime` facts, domain panel fields |
| 3 P2 | Processing: RTM + `transformWithState` + `SlotDiffProcessor` | ✅ | ✅ | `jobs/triple_stream` (`main.py`, `processor.py`); the diff logic is property-tested without Spark. Live in Real-Time Mode on a classic DBR 19 cluster (`ontobricks-rtm`): Kafka → envelope explode → processor under `transformWithState` → `LiveSink` → `_live`, **p99 244 ms** end to end over 2 982 events (the serverless `availableNow` run before it verified state restore and tombstones). The explode must be Spark-native — RTM refuses `mapInPandas` — and the sink / processor must accept the tuples RTM hands over (both fixed) |
| 3 P2 | Serving: Lakebase `_live`, union view, tombstone purge | ✅ | ✅ | `_companion_ddl`, `LakebaseLiveTable`, `TwinLiveServing`: seeded from `_slots` at a pinned version the stream starts from (topic replayed from just before it), in the union view only while served, retired on stop / a non-realtime build / a dead stream (tick), tombstones purged by the tick; deletes and updates reach readers once `_sync` syncs them. Live: DDL, `seq` guard, tombstone, purge, detach on `ontobricks-int`; sink upsert p50 105 ms / p99 218 ms |
| 3 P2 | Operations: job deployment, start/stop, status, `_live` reconciliation; the slot pipeline consumes the same topic | ✅ | ◐ | `resources/triple_rtm.job.yml`, `TwinRealtimeStream`, status card; the slot pipeline reads the topic (`TwinSlotFeed.topic`; a Kinesis stream keeps the change feed); one stream per domain and per deployment at a time. Live: the job's stream wiring (`RECORDS_VIEW` → `explode_sql` → processor → `LiveSink`, `trigger(realTime=…)`) ran in Real-Time Mode on `ontobricks-rtm`, and the sizing rule the job encodes (source + shuffle partitions ≤ task slots) is the one the platform enforced (`CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT`); the topic-fed pipeline SQL validates in Lakeflow. Not exercised: the DAB deployment of the job and `TwinRealtimeStream`'s start / stop against a deployed app (needs `make deploy`) |
| Verif. | Unit · property (slot fold, slot diff) · external (EXPLAIN plan) | ✅ | ✅ | `tests/units/…`, `tests/property/test_slot_fold.py`, `tests/property/test_slot_diff_property.py`, `tests/external/…` |
| Verif. | KPIs: `microbatch` ≤ 30 s p95 via Lakehouse//RT, `realtime` p99 < 1 s via `_live`, drift 0 | ✅ | ◐ | `scripts/measure_freshness.py` + nightly step for the deployed app. Live (fixtures, serverless): drift 0 ✅; **`microbatch` through Lakehouse//RT 14.0 s p50 / 22.9 s p95 (n = 10) — the ≤ 30 s p95 KPI is met ✅**; `microbatch` source → `_slots` 8.1 s p50 / 11.9 s max (n = 8), source → `_graph` through the views 217–383 s; `incremental` source → `_data` 82 s; **`realtime` through Real-Time Mode p50 185 ms / p95 234 ms / p99 244 ms (n = 198) — the p99 < 1 s KPI is met ✅** (on the classic workspace `ontobricks-rtm`; serverless refuses the trigger) |
| Verif. | KPIs: domains per mode, fallback events by reason, build DBUs | ✅ | ○ | `GET /dtwin/incremental/overview` (admin): `domains_per_mode`, `fallbacks_by_reason`, per-domain rows; DBUs are not computed by the app — the trace's pipeline id, `slot_update_id` and `warehouse_id` join to `system.billing.usage` |

## Known limitations

**Upgrading.** After upgrading to this release, run one build of every
`incremental` / `microbatch` domain. The slot pipeline SQL changed (deletes now rank
with pre-images, `seq_rank` 0), so its fingerprint changed: that first build runs one
full refresh of the slot pipeline, and until it has run the scheduled ticks rebuild
`_data` by CTAS (the slot pipeline no longer runs the current SQL).

Also after upgrading:

- **Lakebase `managed_synced` domains.** Asserted-only reads (`include_inferred=false`,
  the asserted entity-search rebuild, BFS expansion, the reconciler's Lakebase leg) now
  query the Postgres view `<graph>_asserted`, which the build creates next to the union
  view; run one build of every such domain, as those reads fail on a domain that has
  not been rebuilt. The same build re-registers a triggered / continuous synced table on
  `_slots` when the slots carry the whole graph (a full resync, once).
- **Lakehouse domains with a schedule.** Earlier scheduled ticks on the Lakehouse
  engine appended every asserted triple to `_inferred` (and created a stray
  `<Name>_V<n>` table in the warehouse's default schema); a tick no longer does. The
  next tick or main build empties `_inferred`; drop the stray table by hand.
- **Neo4j domains** that are not `full`, MERGE-promoted ones included, stream in full
  once (`no_previous_version`): their earlier inferences were never listed, so a net
  apply could not purge them.

Behaviour in this release:

- A Lakehouse tick that cannot read a source's history empties `_inferred` but refreshes
  neither `_data` nor `_graph`, so a `_graph` materialized view serves the pre-tick union
  (old `_data` plus old inferences) until its next refresh — the next tick or any inferred
  write. No wrong data is written; a three-state refresh result would close the gap.
- `augment_mappings_from_config` assigns a `sql_query` to every configured
  entity, so most real mappings classify as batch; the slot pipeline engages only
  for table-based mappings and `incremental` is then the `_data` MV over the batch
  branch. Follow-up: treat single-table `SELECT … FROM t` queries as row-local.
- A domain deleted from the registry keeps its graph artefacts, its slot pipeline
  among them. A domain re-created under the same name and version adopts that
  pipeline (same SQL fingerprint), and its first update resumes the old checkpoint —
  if the sources' change feed is no longer readable from there (seen live: row
  tracking toggled off and on between the two incarnations,
  `DELTA_MISSING_ROW_TRACKING_FOR_CDC`), that build falls back to a full one and
  records `slot_update_failed`, so the next build's update starts over. To start
  clean on the first build, drop the former pipeline (`ontobricks-slots-<folder>-v<n>`)
  before rebuilding. Follow-up: delete a domain's pipeline and tables with the domain.
- `microbatch` freshness between builds is the `_data` view's own refresh: on every
  source update (at most once a minute) for at most ten sources, otherwise every 15
  minutes — and every domain with a `sql_query` or join mapping (most real ones,
  see above) gets the 15-minute schedule.
- Neo4j's inference list (`_inferred`) is emptied by a net apply only; a domain
  whose builds keep streaming in full (e.g. `changeset_too_large`) accumulates
  listed triples until one does (harmless: deleting an absent triple removes
  nothing). An inferred literal that overwrote a mapped value of the same key
  leaves Neo4j showing the inferred value until the purge, as before. Residuals of
  the purge, from the property-graph model (`Neo4jWriteOps.delete_triples`):
  - predicates map to property keys and relationship types by **local name**, so
    deleting a listed triple also removes a mapped one of another predicate URI with
    the same local name; only same-predicate triples are written again;
  - a node left with only its marker label, no relationship and no name is swept
    (deleted) — a node whose only class came from an inference;
  - a **multi-valued** literal (several objects of one predicate) is one property:
    deleting one listed value unsets the key, and the rewrite restores one asserted
    value (Neo4j keeps one per key in any case);
  - a reasoning run that lists a triple identical to one being purged, while the
    purge runs, loses that listing (the list is cleared by triple values).
- A write to `_inferred` on the Lakehouse engine (reasoning, cohorts, the purge)
  waits for the synchronous `_graph` refresh: the writer takes as long as the refresh,
  and readers see the old union until it completes. Requests that write inside the
  request — `POST /cohorts/materialize`, `DELETE /reasoning/inferred` — answer only
  after that refresh, which on a large graph may exceed a proxy timeout; run
  reasoning as a background task there.
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

Live verification (2026-11-02, workspace `fevm-ontobricks-int`, serverless SQL warehouse,
scratch schema `ontobricks_int_catalog.itm_spike`, everything dropped afterwards):

- `EXPLAIN CREATE MATERIALIZED VIEW` of the generated `_data` definition answers
  "The Materialized View can be incrementally refreshed — No issues detected" for
  both the `incremental` and the `microbatch` (scheduled) DDL; `tests/external/test_mv_incremental_plan.py`
  passes against the workspace.
- The generated `_data` MV (`REFRESH POLICY INCREMENTAL`, custom `ontobricks.mv_sql_sha256`
  property, `SCHEDULE CRON … AT TIME ZONE 'UTC'`), `REFRESH MATERIALIZED VIEW`, the `_graph` MV
  over `_data ∪ _inferred`, the MERGE fallback statements (`ensure_table_sql`, `merge_sql`:
  2 inserts, then 1 delete + 1 insert after a source update), `table_changes()` on the MERGE
  table, `DESCRIBE HISTORY` on a source table and `SHOW TBLPROPERTIES` all run as emitted.
  The custom TBLPROPERTY is persisted on the MV; filtered `SHOW TBLPROPERTIES … ('key')`
  returns a `value`-only row (the sha reader was fixed to read it). The event log reports
  the first refresh as `COMPLETE_RECOMPUTE` (`event_log(TABLE(…))` works on the MV).
- Both slot-pipeline variants validate in a real Lakeflow pipeline (`start_update(validate_only)`
  → `COMPLETED`): the change-feed variant (streaming table + `stack()` view over
  `readChangeFeed` + `AUTO CDC … STORED AS SCD TYPE 1`) and the Debezium-topic variant
  (`read_kafka` with backticked options and `secret()`; the secret key must exist — a
  missing key fails analysis with `FAILED_FUNCTION_CALL … None.get`).
- Not available on that workspace: change-feed reads on a materialized view —
  `table_changes()`, `DESCRIBE HISTORY`, `DESCRIBE DETAIL` and `VERSION AS OF` on an MV fail
  with `MATERIALIZED_VIEW_CDF_REQUIRES_EXTERNAL_METADATA` / `EXPECT_TABLE_NOT_VIEW`, and
  `REPAIR TABLE … SYNC METADATA` with `REPAIR_TABLE_REQUIRES_EXTERNAL_METADATA` (external
  metadata for pipeline tables is a private preview). The build now runs the REPAIR after
  creating a view (best-effort); until the workspace is enrolled the MV-fed net-change apply
  falls back as designed (`cdf_unavailable` → full stream) while the MERGE-table feed works.
  `ALTER MATERIALIZED VIEW … SET TBLPROPERTIES` is not supported (properties come from the
  definition).

Live cross-mode equivalence and freshness (2026-11-02, same workspace; a real Lakeflow run of the
change-feed slot pipeline on two fixture tables, 30 slots / 28 distinct triples; all objects dropped
afterwards):

- **Equivalence**: after the pipeline bootstrap, and again after a round of `UPDATE`, `DELETE`,
  `INSERT` on the sources plus an incremental pipeline update and `REFRESH MATERIALIZED VIEW`,
  the `_data` view equals the full-build translator output exactly — `EXCEPT` is empty both ways,
  the reconciler reports `drift: false` with identical row counts and `xxhash64` sums (28 → 29
  triples; the deleted subject gone, the inserted one present). The design's "AUTO CDC fold =
  translator output" holds on the workspace.
- **Refresh technique**: the post-mutation refresh planned as `GROUP_AGGREGATE` (incremental), not
  a recompute.
- **Triggered (`incremental`) freshness**: slot pipeline bootstrap 65 s; incremental pipeline
  update 59 s; source commit → change visible in `_data` **82 s** (update + refresh, serverless).
- **Continuous (`microbatch`) freshness through the view**: with the pipeline continuous and the
  `_data` view on `TRIGGER ON UPDATE`, three source updates became visible after **383 s, 217 s and
  361 s** (the slot was in `_slots` long before — the view's trigger runs at a cadence of minutes;
  two further samples were lost when the warehouse session dropped). This is the materialized-view
  path; the design's ≤ 30 s target is the Lakehouse//RT path over `_slots` / `_graph`, which this
  workspace cannot offer (gated beta). Treat "seconds" as the pipeline's and "minutes" as the view's.
- **Continuous (`microbatch`) freshness at `_slots`** (same day, a second continuous run of the
  change-feed pipeline on one fixture table, eight single-row updates spaced apart, the slots table
  polled every second from the build warehouse): source commit → new label in `_slots` after
  **8.1 s median, 11.9 s max, 3.1 s min** (n = 8). This is what the [fast path](#lakehousert)
  reads; the next bullet measures the same path through a Lakehouse//RT warehouse.
- **Continuous (`microbatch`) freshness through Lakehouse//RT** (same day, after the user created
  the RT warehouse `realtime` — serverless, Large; the API lists it with `warehouse_type: null` —
  a third continuous run on one fixture table plus a one-row `_inferred` table, ten single-row
  updates, the inline relation polled every second through the app's own
  `DatabricksClient(use_sea=True)` on the RT warehouse): source commit → visible through the
  relation after **14.0 s median, 22.9 s p95 / max, 9.5 s min** (n = 10; samples 22.9, 9.8, 15.8,
  14.6, 16.2, 13.5, 15.2, 10.4, 9.5, 10.7). **The design's `microbatch` KPI (≤ 30 s p95 via
  Lakehouse//RT) is met.** On the same warehouse: the fingerprint probe (`SHOW TBLPROPERTIES`)
  matches `GraphReadRelation.microbatch_data_sha`; unaliased `FROM (…)`, `FROM (…) t`, a self
  join, the asserted-only form, the `subject IN (SELECT …)` seed, and direct reads of the
  streaming table and the MV all run; the inferred triple is visible through the relation and
  its count is `_data` + `_inferred`; point reads of the relation from a laptop through the
  Statement Execution API: p50 216 ms, p95 404 ms, max 541 ms (n = 20, HTTP round trip
  included). Refused: recursive CTEs (`UNSUPPORTED_FEATURE … SQLSTATE: 0A000`) — the message
  carries the `lakehouse//rt` marker, so the store's BFS / transitive closure fall back to the
  build warehouse (pinned with the verbatim message in
  `tests/units/graphdb/delta/test_read_warehouse_routing.py`).
- **Found and fixed**: switching the pipeline to continuous starts an update by itself, so the
  explicit `start_update` of the microbatch bootstrap was refused (`An active update … already
  exists`) — the runner now adopts that update.

Live realtime serving leg (2026-11-02, Lakebase instance `ontobricks-int`, scratch schema dropped
afterwards): the generated `_live` DDL ran as emitted (primary key, the three partial
`WHERE NOT deleted` indexes, `autovacuum_vacuum_scale_factor=0.02` /
`autovacuum_analyze_scale_factor=0.01`, the serving comment); `LiveSink` upserts behaved as
designed — a newer `seq` replaced the row, a stale `seq` was rejected, a tombstone marked the
row deleted — `purge_live_tombstones_sql` removed it and detaching cleared the serving comment.
Single-row upsert latency from a laptop over TLS: p50 105 ms, p99 218 ms (the sink's share of the
sub-second budget; a cluster in-region sits well below that).

Live Lakebase synced tables (2026-11-02, instance `ontobricks-int`, through the app's own
`SyncedTableManager`, fixture slot pipeline + `_data` view in a scratch schema, all dropped afterwards):

- **TRIGGERED from the `_data` materialized view**: the registration is accepted and the initial snapshot
  lands (9 rows in Postgres), but every sync pipeline update ends `SYNCED_TABLE_ONLINE_PIPELINE_FAILED` —
  `[SYNCED_TABLE_USER_ERROR.SOURCE_READ_ERROR] … [MATERIALIZED_VIEW_CDF_REQUIRES_EXTERNAL_METADATA] Change
  Data Feed reads from Materialized Views require external metadata`. The synced-table pipeline reads the
  MV's change feed, so "MV → Lakebase TRIGGERED / CONTINUOUS" needs the same private preview as the MV
  change feed itself; without it the build falls back to a snapshot registration ([per
  backend](#what-a-build-does-per-backend)).
- **TRIGGERED from the `_slots` streaming table** (key `map_id, source_key, predicate`): works —
  `SYNCED_TABLE_PROVISIONING_PIPELINE_RESOURCES` → `…_PROVISIONING_INITIAL_SNAPSHOT` →
  `SYNCED_TABLE_ONLINE_TRIGGERED_UPDATE` → `SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE` in 57 s, 9 rows.
- **CONTINUOUS from `_slots`**: works — `SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE` after 56 s; with the slot
  pipeline continuous, a source `UPDATE` was visible in the Postgres synced table after **45.8 s** (the
  design's Lakebase target is < 60 s). This is why a domain whose slots carry the whole graph syncs
  Lakebase from `_slots` in the triggered and continuous schedules.

Live MERGE fallback and source checks (2026-11-02, warehouse only, fixture tables dropped
afterwards): `DeltaMergeMaterializer.apply` created `_data` with row tracking, deletion vectors
and the change feed and merged 3 rows; a build with no source change merged **0 rows**
(`numOutputRows 0`); one changed source row → 1 delete + 1 insert (a triple's identity changes
with its object); one deleted row → 1 delete, 2 rows left; `table_changes` on the MERGE table at
that version listed exactly the one delete (the feed the net-change apply reads when `_data` is a
table). `SourceReadiness.source_overwritten_since`: a plain `UPDATE` is not a rewrite,
`INSERT OVERWRITE` (`WRITE`, mode Overwrite) and `CREATE OR REPLACE TABLE AS SELECT` are. The
row-tracking probe read `false` on the table that `CREATE OR REPLACE` had replaced (its properties
were gone) — the case the readiness preflight reports and degrades to `full`.

Live realtime processing leg on Spark (2026-11-02, a serverless **notebook** task — the same
harness as a serverless *script* task aborts the Python client with SIGABRT before reporting
anything; classic job clusters are refused on the workspace, "Only serverless compute is
supported"):

- **What the platform says.** Serverless compute refuses any continuous trigger —
  `[INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED] Trigger type ProcessingTime is not supported for
  this cluster type. Use … AvailableNow, Once` — which is the precise reason Real-Time Mode (and
  every always-on stream) needs classic DBR 19 compute. The DBFS root is disabled there
  (checkpoints go to a UC volume, as the RTM job does with `ONTOBRICKS_REALTIME_CHECKPOINT_ROOT`),
  and the runtime's bundled SDK predates the Lakebase API (the submitter minted the credential).
- **What ran.** With `trigger(availableNow=True)` over a Delta table of Debezium-style events
  (standing in for the topic), `SlotDiffStatefulProcessor` under `transformWithState` →
  `LiveSink` into a Lakebase `_live` table, two waves on one checkpoint: wave 1, 40 upserts → 80
  state rows and **80 live rows** (two slots per customer), 0 tombstones; wave 2, state restored
  from the checkpoint, 10 renames + 5 deletes → **70 live rows, 10 tombstones**, 10 renamed
  labels, 0 stale labels, 0 deleted keys still live. State init / restore, the slot diff, delete
  → tombstone, `seq` handling and the sink's guarded upserts behave on real Spark as the property
  tests say.
- **Real-Time Mode, measured** (2026-11-03, on `ontobricks-rtm`, an FEVM `aws_sandbox_classic`
  workspace in us-east-1: a classic DBR 19 cluster, `m5d.xlarge` × 2 workers,
  `spark.databricks.streaming.realTimeMode.enabled`, a single-node KRaft Kafka 4.3 broker on the
  driver standing in for the customer's topic, a Lakebase instance in that workspace for `_live`).
  Debezium-style envelopes keyed by the source id → Kafka source → a Spark-native envelope explode
  → `SlotDiffStatefulProcessor` under `transformWithState` → `LiveSink` (`foreach`) → `_live`,
  `trigger(realTime="1 minute")`, 20 events/s for 150 s (2 982 events, 28 deletes, 200 keys):
  three real-time batches ran back to back (781 / 1 218 / 983 rows), 400 live rows (two slots per
  key) and 4 tombstones landed, and the end-to-end latency from the event's `ts_ms` to the Postgres
  `updated_at` was **p50 185 ms, p95 234 ms, p99 244 ms, max 246 ms** (n = 198). **The design's
  `realtime` KPI (p99 < 1 s) is met.** Found on the way, and fixed or recorded:
  - Real-Time Mode refuses the `mapInPandas` envelope explode `jobs/triple_stream/main.py` had
    (`STREAMING_REAL_TIME_MODE.OPERATOR_OR_SINK_NOT_IN_ALLOWLIST: MapInPandasExec`; with the
    allowlist check bypassed it fails inside the operator, `ClassCastException … RealTimeIterator`),
    and it refuses a `UNION` of several reads of the same stream
    (`IDENTICAL_SOURCES_IN_UNION_NOT_SUPPORTED` — the job then fell back to a 1-second micro-batch
    trigger, which measured p50 1.17 s / p99 1.85 s). The explode is therefore one Spark SQL pass
    (`TripleStreamEnvelope.explode_sql`: `from_json` typed by the described columns, one
    `named_struct` per mapping and case in one `array`, `filter`ed and `explode`d). **Run with
    that SQL, exactly as `main.py` wires it: p50 179 ms, p95 243 ms, p99 248 ms** (n = 198), three
    real-time batches, 400 live rows, 4 tombstones ([Realtime](#realtime)).
  - Under the real-time trigger `foreach` hands the writer, and `transformWithState` the processor,
    plain tuples rather than `Row` objects: `LiveSink.values` and the processor's row decoding
    accept both (the sink's `dict(tuple)` was the first live failure).
  - Real-Time Mode schedules every stage at once and needs *source partitions + shuffle partitions*
    free task slots (`CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT: 204 required` with the default 200
    shuffle partitions on a 4-slot cluster): `spark.sql.shuffle.partitions` must be sized to the
    cluster, and a stream a failed notebook left running keeps its slots until the cluster restarts.
  - Serverless compute refuses the real-time trigger (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`),
    which is why the earlier serverless attempts could only run `availableNow`.
- **The deployed-app journey** (2026-11-03, the UI and MCP apps deployed to `ontobricks-rtm` by
  `scripts/deploy.sh`; `tests/e2e/scenarios/test_scenario_06_incremental_live.py`, opt-in, no LLM):
  a Lakehouse domain in `incremental` mode over two fixture tables, a one-class-per-table
  ontology and two row-local mappings, all through the app's own routes. Readiness answered
  `decision: {effective: full, reason: row_tracking_missing}` for the untracked source, two `slot`
  mappings, `read_relation: graph`, `lakehouse_rt: {query_warehouse_configured: false, use_sea:
  false, fast_path: graph}`; `POST /dtwin/incremental/enable-row-tracking` on that table flipped
  the probe and the decision to `incremental`. The Domain › Backend card read "incremental runs as
  incremental · Mappings: 2 row-local (microbatch-eligible), 0 batch-incremental · Lakehouse//RT:
  query warehouse not set · RT reads off". The build created the slot pipeline
  (`ontobricks-slots-zzincrementallive-v1`, PREVIEW channel), bootstrapped `_slots` (6 upserted
  slots, `slots_changed.full_refresh: true`), created the `_data` MV, and the trace carried
  `materialization_mode_effective: incremental`, `slot_update_id`, `source_versions` /
  `source_table_ids` per source, `inferred_reset: true`; `GET /dtwin/incremental/status` listed the
  pipeline `IDLE`, `_slots` as a `streaming_table` with its last commit and `_data` as `mv` without a
  readable history. Re-run after dropping the former pipeline and tables (a clean first build):
  `strategy: mv`, `graph_relation: mv`, 6 upserted slots, `refresh_technique: NO_OP` for the
  freshly created `_data`, 192.5 s end to end, and the overview's `fallbacks_by_reason: {}`; the
  Knowledge Graph › Build status card read "Slot pipeline: IDLE · _data: mv · lag unknown ·
  _slots: streaming_table v5 · Last build: success (session) · mv / NO_OP / none · slots changed:
  6 upserted, 0 deleted (full refresh)" and the whole journey passed in 300 s. The
  journey also found three defects, fixed in this release: the readiness
  preflight (and the reconciliation and `realtime` start) read the session-only R2RML without
  regenerating it, so a domain opened from the registry reported no mappings; the registry
  delete by display name deleted nothing and reported success; and a slot update that fails
  did not owe the next build a full refresh (re-creating the domain adopted the former pipeline,
  whose stream failed on the fixture's toggled row tracking and fell back to a MERGE build,
  `strategy: merge`, effective `incremental`, `freshness_lag_s: 9`, as the ladder promises).

Lakehouse//RT is no longer blocked either: RT warehouses are created in the UI — Compute → SQL
Warehouses → Create → Real-Time; the SDK's warehouse types are still `CLASSIC` / `PRO` only and the
API lists an RT warehouse with `warehouse_type: null` — and the measurements above were taken once
one existed; `scripts/measure_freshness.py --read-warehouse-id <rt>` repeats them against a
deployed domain.

Live-unverified (workspace spike checklist):

- The MV change data feed and time travel once external metadata is enabled
  (`REPAIR TABLE … SYNC METADATA`): the net-change apply from the `_data` MV and the
  Neo4j purge's `VERSION AS OF` reads have only been exercised against the MERGE table.
- `ImportFormat.AUTO` for the pipeline notebook; the stop→start race of a
  continuous pipeline.
- A build's `REFRESH MATERIALIZED VIEW` while a `TRIGGER ON UPDATE` or scheduled
  refresh of the same view is running.
- The Neo4j purge's `DELETE … WHERE EXISTS` over a time-travel copy of `_inferred`
  (a Delta table, so `VERSION AS OF` applies; not yet run end to end).
- A foreign or shared (Delta Sharing) source table counted toward the ten upstream
  sources of `TRIGGER ON UPDATE`.
- A CTAS `_data` that keeps a triggered/continuous registration — the scheduled
  tick's CTAS on a MERGE-promoted domain, or a main build whose probe or MERGE
  failed — carries row tracking only (no explicit `delta.enableChangeDataFeed`)
  until the next MERGE or MV build; the Lakebase sync reading it in between relies
  on row-tracking-derived change data.

## Realtime

`realtime` keeps the `microbatch` machinery (continuous slot pipeline,
self-refreshing `_data` view, continuous synced table) and adds a stream that brings
a source change into the graph in seconds: a classic-compute job reads a
Kafka-compatible topic, diffs each changed source row's slots and upserts them into a
Lakebase `_live` table the graph's union view reads. The slot pipeline consumes the
same topic ([below](#the-slot-pipeline-in-realtime-mode)), so `_slots` / `_data` and
`_live` follow the same changes.

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
- The domain: a **Lakebase** graph backend, a **published** version (row tracking on
  the sources is only needed for a Kinesis stream, whose slot pipeline keeps the
  change feed), and its `realtime_source` (Domain › Information ›
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
explode (`TripleStreamEnvelope.explode_sql` — one Spark SQL pass: `from_json` images typed
by the described columns, one `named_struct` per mapping and case in one `array`, `filter`ed
and `explode`d; Real-Time Mode refuses `mapInPandas` and a `UNION` of several reads of the
same stream, both measured live), the stateful diff
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

### The slot pipeline in realtime mode

An effective `realtime` domain's slot pipeline reads the same Debezium topic as the
stream job instead of the source tables' change data feed, so `_slots` / `_data`
(Delta) and `_live` (Lakebase) follow the same changes, as the design has it.
`TwinIncrementalBuild.compile_for` — the one compile the build, the scheduled tick,
the MERGE fingerprint, the readiness preflight and the DAB renderer share, hence one
SQL fingerprint — calls `SlotProjectionCompiler.compile(..., source="topic",
topic=TopicSource.from_realtime_source(...), column_types={table: {column: type}})`
whenever the domain's best mode is `realtime` (`TwinSlotFeed.topic`);
the column types come from `DESCRIBE TABLE` of each row-local source, read once per
compile on the build's or the tick's warehouse (a source that cannot be described is
an `InfrastructureError`, not other SQL). A **Kinesis** stream, which `read_kafka`
cannot read, keeps the change feed (and needs row tracking).

**Reading the topic.** Each row-local mapping's view reads `STREAM read_kafka(...)`
from the earliest offset. It logs in with `SASL_SSL` / `PLAIN` through
`secret('<scope>', '<key>')`: the JAAS line for Kafka, the connection string for
Event Hubs.

**Parsing the envelope.** It uses the stream job's rules: an optional `payload`
wrapper; case-insensitive columns, where the last of two case-variants wins;
`source.table` matched by full name, or by leaf name when no other mapped table
shares it (a shared leaf logs a warning, because stock Debezium sends bare names);
deletes from `before`, upserts from `after`, and the old key deleted on a
key-changing update.

**Typing values.** Values are typed from the source tables' column types, so
Debezium's epoch-day dates, epoch timestamps and base64 decimals read as `CAST(<column>
AS STRING)` prints them (`DebeziumValueText`, the stream job's `SparkColumnText`
rules). A mapped column without a type logs a warning and keeps its text; JSON
numbers and booleans are read as their text, so an exponent float or a Debezium
encoding beyond the defaults can still print differently from the change-feed
variant.

**Sequencing.** The Kafka offset sequences the AUTO CDC flow. That flow, `_slots`
and the `_data` MV are unchanged, and a topic source needs no row tracking on the
source tables (`spec.needs_row_tracking` is `False`): the build, the tick and the
preflight probe none, and no source history is read for the full-refresh plan.

**What the topic must provide.**

- It must be keyed by the source row's primary key: offsets order changes only
  within a partition, so a key reached by events in two partitions (a key-changing
  update, a mapping keyed by a non-PK column) can be misordered.
- It must carry full pre-images (`REPLICA IDENTITY FULL`, `binlog_row_image=FULL`), so
  deletes and re-keys of mappings keyed by non-PK columns are seen.
- It must retain, or compact, every row's latest change (log compaction or a Debezium
  snapshot). A full refresh — which any change to the compiled SQL triggers, including
  a new broker list, topic or secret name, and a reconciliation drift — replays it from
  the earliest offset; with time-based retention a rebuilt `_slots` misses the rows
  not changed recently. Rotate a credential by writing the new value under the same
  secret scope and key.
- A write that reaches a source table but not the topic (a backfill, a repair
  `UPDATE` on the Delta copy) reaches neither `_slots` nor `_live`; the reconciliation
  against the batch recompute (`view_data`) shows it as drift.

The stream is the low-latency overlay on top, reconciled against `_slots` by the
tick. One stream runs per deployment at a time (`max_concurrent_runs: 1`; raise it to
stream several domains).

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
