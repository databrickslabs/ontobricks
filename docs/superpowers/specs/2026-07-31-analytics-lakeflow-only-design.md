# Analytics: Lakeflow-only, R2RML-sourced

**Date:** 2026-07-31
**Status:** Approved (design)
**Supersedes:** the three-mode analytics selector (`in_memory` / `pushdown` / `job`)

## Problem

Graph analytics behaves differently per backend and per graph size, so the same
domain can produce different KPIs depending on how it was built.

1. **Backend-coupled source.** Metrics read the *engine* graph relation resolved
   by `effective_graph_query_table` — the Delta `…_graph` union on Lakehouse, the
   Postgres union view on Lakebase, Cypher on Neo4j. `resolve_spark_source`
   then refuses the Databricks job outright for Neo4j and for Lakebase
   `app_managed`, because neither is readable from Spark. Those domains can
   never compute PageRank, components, clustering, betweenness or closeness.
2. **Three compute paths.** `GraphMetrics` (NetworkX, in-process, capped by
   `ONTOBRICKS_ANALYTICS_MAX_TRIPLES`), `PushdownMetrics` (engine SQL, fewer
   metrics), and `JobMetrics` (Lakeflow). A size test picks between them, so a
   graph crossing the cap silently loses metrics and the UI has to explain
   which subset the user got.
3. **Scope drift.** The engine graph carries mapped triples *plus* reasoning
   output and cohort writes, and what it carries varies by engine and by
   whether reasoning has been run.

## Decisions

| # | Decision | Rationale |
|---|---|---|
| 1 | Analytics scores the **mapped graph only** | The R2RML VIEW is the one artefact every Build produces on every backend. Excluding inferred/cohort triples makes a KPI reproducible from the mapping alone. |
| 2 | Compute source is UC `…_data`, materialised from the R2RML VIEW on **every** Build, for **every** engine | `_data` already means "materialised mapped triples from the VIEW" on Lakehouse. Extending that guarantee to Lakebase and Neo4j removes the backend coupling without a second copy. |
| 3 | **Lakeflow is the only compute path.** Delete the NetworkX metrics path | One path means one answer. Removes the size cap and the mode-selection UX. |
| 4 | Entity-type filter is a **job parameter** | Keeps the existing "analyse one entity type" workflow, now uniformly and at any size. |
| 5 | **Every metric is computed by the Lakeflow job.** `PushdownMetrics` is deleted from the analytics path | One compute path, no exceptions. The job gains per-entity-type aggregation, `rdf:type` and `rdfs:label` resolution, so nothing is left for the app to compute. |
| 5b | The app still applies `profiles.flat_reasons` / `profiles.has_temporal_predicates` to job output | These are pure Python string heuristics over `(instance_count, distinct_predicates)` and predicate local names — *labelling*, not metric computation. Keeping them in `profiles.py` avoids reimplementing keyword matching in SQL and keeps one definition of "flat". |
| 6 | When the job cannot run, **hard fail** with a specific reason | Silently returning thinner KPIs is what this work exists to remove. |

Not in scope: inferred/cohort triples in KPIs, community detection, cohort
discovery. Both keep their NetworkX paths, so `networkx` stays a dependency.

## Architecture

```
BUILD (any engine)
  1. CREATE OR REPLACE VIEW  <cat>.<sch>.triplestore_<dom>_V<n>        (R2RML; already unconditional)
  2. CREATE OR REPLACE TABLE <cat>.<sch>.triplestore_<dom>_V<n>_data   (NEW: unconditional, all engines)
         AS SELECT subject, predicate, object FROM <view>
  3. engine load (Delta _inferred + _graph union / Lakebase sync / Neo4j insert) — unchanged

RUN ANALYSIS
  source = effective_databricks_table(domain)   ->  …_data       (never the engine store)
  preflight: toggle on AND job name resolvable AND …_data exists non-empty
      -> otherwise HARD FAIL with the specific missing prerequisite
  compute     : Lakeflow job(source_table=…_data, class_filter, exclude_predicates, pivots, max_depth)
                  -> <out>              per-node metrics + type_uri + label
                  -> <out>_summary      node/edge/component counts, pivot + bfs flags
                  -> <out>_type_profiles     per-type counts, degree sum, avg clustering/betweenness
                  -> <out>_type_predicates   (type_uri, predicate) rows
  read-back   : top-N per metric + summary + the two type tables; the app only
                assembles and applies the pure-Python flat/temporal labels
```

`…_data` is the contract between Build and Analytics. Build guarantees it;
Analytics only reads it.

## Components

### Build — unconditional mapped snapshot

`_BuildPipeline` (`src/back/objects/digitaltwin/_build_pipeline.py`) gains one
phase after `_create_view()` succeeds and before `_apply_full_rebuild()`:
materialise `…_data` from the VIEW via the existing
`materialize.materialize_from_view(client, view_fqn, table_fqn)`. The FQN comes
from `_table_naming.data_table_fqn(domain, settings)`.

Today only `DeltaTripleStoreBuildPipeline` (Lakehouse) does this. The step moves
into the shared pipeline so Lakebase and Neo4j builds produce it too. The
Lakehouse pipeline must not materialise twice.

Failure to materialise fails the build — analytics correctness depends on it,
and a half-built domain that silently cannot be analysed is worse than a clear
build error.

### Source resolution — replaces `resolve_spark_source`

New `resolve_analytics_source(domain, settings) -> (table, reason)`:

- returns `(data_table_fqn, "")` when the FQN resolves to `catalog.schema.table`
- returns `("", reason)` otherwise

No store-dialect, `sync_mode`, or Neo4j branch — the source no longer depends on
the engine. `resolve_spark_source` and its Lakebase synced-table lookup
(`_lakebase_uc_source`) are deleted.

Existence is a separate check so the reason can name the remedy: a missing
`…_data` means "Run Knowledge Graph → Build first", not "this engine is
unsupported".

### Preflight — `_analytics_job_status`

Reduces from four prerequisites to three, in this order:

1. `resolve_analytics_job_enabled(domain, settings)` — else `(False, "")` (off is not broken)
2. `resolve_analytics_job_name(settings)` — else name-not-resolvable reason
3. `…_data` resolves **and** `SELECT 1 FROM … LIMIT 1` returns a row — else build-first reason

The `pushdown_ok` parameter disappears: engine capability is no longer a
prerequisite, because analytics never touches the domain's graph engine.

### Compute — `DigitalTwin.compute_graph_metrics`

Collapses to a single path. No `mode` argument, no `allow_pushdown_fallback`, no
size test, no `store` argument. It resolves the source and runs `JobMetrics`.
`MetricsRequest.max_triples` / `max_nodes_betweenness` are dropped from the
analytics call path (they remain for cohorts/community).

### `JobMetrics` — read-back assembler only

No `store`. Its dependencies are the runner, a `query` callable (warehouse SQL
for reading the job's own output tables), and the `…_data` source table.

- `compute()` triggers the job, then assembles `MetricsResult` **entirely** from
  the four output tables. It performs no graph computation.
- `stats` comes from `<out>_summary`, which must now carry both
  `node_count` (all distinct entity URIs in the source) and `graph_node_count`
  (nodes with at least one surviving edge). `avg_degree` and `density` are
  derived arithmetic on those two numbers, unchanged in formula.
- `nodes` / `node_types` / `node_labels` come from the top-N query, which now
  selects `type_uri` and `label` alongside the metrics.
- `entity_type_profiles` are built from `<out>_type_profiles` +
  `<out>_type_predicates`, then labelled with `flat_reasons` /
  `has_temporal_predicates` per decision 5b.
- Passes `request.class_filter` to the new `class_filter` job parameter and
  `request.predicate_filter` to the existing `exclude_predicates` one (today
  hardcoded to `None`).
- `mode` is `MODE_JOB`.
- `avg_clustering` / `avg_betweenness` per type are now real values from the job
  rather than the zeros the pushdown path was forced to report.

### Job — `src/jobs/graph_analytics_job.py`

Exactly **one** new parameter is required: `--class-filter` (comma-separated
class URIs), threaded `Settings → JobMetrics → LakeflowRunner → job_parameters`
exactly as `pivots` and `max_depth` are, and declared in
`resources/graph_analytics.job.yml`.

`predicate_filter` needs **no new parameter**. `GraphBuilder` treats it as an
*exclusion* list (`excluded = _DEFAULT_EXCLUDED_PREDICATES | request.predicate_filter`),
which is precisely the existing `--exclude-predicates` job parameter.
`JobMetrics` currently hardcodes `exclude_predicates=None`; it must instead pass
`request.predicate_filter` through. The job's own default exclusions must stay
aligned with `_DEFAULT_EXCLUDED_PREDICATES` (`rdf:type`, `rdfs:label`,
`rdfs:comment`, `rdfs:seeAlso`).

Filter semantics must match the induced subgraph the NetworkX path produced:

- `class_filter` keeps only nodes carrying `rdf:type` in that set, then keeps
  only edges whose **both** endpoints survive.
- Filtering happens once, when the bidirectional edge list is built. Every
  downstream stage is unchanged, so PageRank, components, clustering and the
  pivot BFS all operate on the filtered graph.

Betweenness/closeness remain Brandes-Pich estimates with the existing
`bfs_complete` depth-cap contract.

**New output stages** (all portable SQL — the test harness runs these statements
against SQLite, so no `collect_set`, window-only, or Spark-specific functions):

1. `write_output` gains `type_uri` and `label` columns, each a `LEFT JOIN` onto
   the source for `rdf:type` / `rdfs:label`, aggregated with `MIN(object)` so a
   node carrying several types resolves deterministically.
2. `type_profiles()` → `<out>_type_profiles`:
   `type_uri, instance_count, connected_count, degree_sum, avg_clustering,
   avg_betweenness`. `instance_count` counts every instance of the type in the
   source (isolated included); `connected_count`, `degree_sum` and the averages
   join the per-node output, so they cover only nodes that survived filtering.
3. `type_predicates()` → `<out>_type_predicates`: distinct `(type_uri,
   predicate)` rows over entity-entity edges, excluding the excluded predicates.
   Emitted as rows rather than an array so the SQL stays portable.

`summary` gains `total_node_count` so the app can report `node_count`
separately from `graph_node_count`.

Both new tables are created even when empty, so the read-back never has to
distinguish "job did not write it" from "no types".

### API and UI

`/dtwin/sync/stats` drops `analytics_max_triples` and
`analytics_pushdown_available`, keeps `analytics_job_available` and
`analytics_job_blocked_reason`.

`_query_analytics.html` loses the size-limit banner, the "engine-side
aggregation" wording, and the in-memory/pushdown branches of the zero-value
notices. Run Analysis is enabled iff `analytics_job_available`; otherwise it is
disabled and shows `analytics_job_blocked_reason`. The entity-type select is
sent as `class_filter`. The unavailable-metric notice keeps only the job
wording, and clustering keeps its genuine-zero explanation.

## Removals

| Path | Action |
|---|---|
| `src/back/core/graph_analysis/GraphMetrics.py` | delete |
| `tests/units/core/test_graph_metrics.py` | delete |
| `MODE_IN_MEMORY`, `MODE_PUSHDOWN` | delete; `MetricsResult.mode` defaults to `MODE_JOB` |
| `src/back/core/graph_analysis/PushdownMetrics.py` | delete, with `supports_pushdown` |
| `tests/units/core/test_pushdown_metrics.py` | delete |
| `resolve_spark_source`, `_lakebase_uc_source` | delete |
| mode selection + `allow_pushdown_fallback` in `dtwin.py` and `DigitalTwin` | delete |
| `Settings.analytics_pushdown_enabled` | delete |
| `Settings.analytics_max_triples` | keep the setting — `POST /clusters/detect` still uses it. Remove only its use in the analytics panel, where there is no longer a size ceiling to enforce. |

`profiles.py` is **kept** (decision 5b) and its module docstring must stop
citing `GraphMetrics` / `PushdownMetrics` as its callers.

`get_graph_structure_stats`, `get_top_nodes_by_degree`, `get_type_edge_stats`
and `get_type_predicate_pairs` lose their only caller across every store
implementation. Deleting them is **out of scope** — they are part of the
`TripleStoreBackend` contract and belong to a separate cleanup pass.
`get_type_distribution` keeps other callers (the entity-type dropdown in
`digitaltwin.py` and `dtwin.py`) and stays in use.

## Testing

`networkx` remains a legitimate **test oracle** even though it is gone from
runtime.

1. **Job filter parity** — extend `tests/units/core/test_graph_analytics_job_sql.py`:
   for each fixture graph, running with a `class_filter` must reproduce
   `nx.degree_centrality` / `nx.pagerank` / `nx.clustering` computed on
   `graph.subgraph(nodes_of_that_type)`. Plus a test that `predicate_filter`
   reaches the job as `exclude_predicates` and drops those edges.
2. **Build guarantees `_data`** — a Lakebase and a Neo4j build both call
   `materialize_from_view` with the `…_data` FQN; a materialise failure fails
   the build.
3. **Source resolution** — `resolve_analytics_source` returns `…_data`
   independent of engine; a non-qualified FQN yields a reason.
4. **Preflight** — each of the three prerequisites, in order, yields its own
   reason; toggle-off yields an empty reason.
5. **Compute** — `compute_graph_metrics` always runs `JobMetrics`; a missing
   job or missing `_data` raises rather than degrading.
6. **UI contract** — `test_analytics_unavailable_metrics.py` and the front tests
   drop pushdown/in-memory assertions and assert the job-only wording.

## Migration

- Domains last built before this change have no `…_data` on Lakebase/Neo4j.
  Run Analysis hard-fails with "Run Knowledge Graph → Build first" until
  rebuilt. No backfill script — a rebuild is the documented remedy.
- The job must be redeployed (`make deploy`) for the new `class_filter`
  parameter; `run_now` rejects undeclared parameters.
- Stored `graph_analytics` rows from earlier runs may carry
  `mode: "in_memory"` / `"pushdown"`. The read path must tolerate an unknown
  mode string rather than assuming `MODE_JOB`.

## Risks

| Risk | Mitigation |
|---|---|
| Every Run Analysis now costs a Lakeflow run, including tiny graphs | Accepted; a serverless run on a small graph is short. Revisit only if latency complaints appear. |
| Build gets slower for Lakebase/Neo4j (extra CTAS) | The CTAS reads the same VIEW the engine load already reads. |
| KPIs change for users who had run reasoning | Intended per decision 1. UI labels the scope as the mapped graph. |
| `_data` and the engine graph drift if reasoning runs after Build | Acceptable: KPIs are defined on the mapped graph, not the live engine graph. |
