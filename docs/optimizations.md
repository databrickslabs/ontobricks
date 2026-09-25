# Graph Query Optimizations

This guide describes the performance techniques currently used by OntoBricks
for interactive graph reads. It covers Explorer Preview, bounded graph
expansion, payload retrieval, backend-specific physical layouts, query
transport, safety limits, and operational refresh behavior.

The implementation supports three execution models:

- **Lakehouse** — SQL over Delta tables and views.
- **Lakebase** — SQL over Postgres tables and a reader-facing union view.
- **Neo4j** — native Bolt/Cypher traversal; the SQL companion tables described
  below do not apply.

## 1. Read path overview

An Explorer search is split into three database/display phases:

1. **Preview** finds up to 500 typed seed entities.
2. **Expansion** performs a bounded breadth-first search (BFS) from the selected
   seeds.
3. **Payload fetch** retrieves the outgoing triples needed to render the
   discovered entities.

Lakehouse and Lakebase avoid repeatedly scanning the full
subject-predicate-object (SPO) relation by materializing graph-index
companions:

| Companion | Shape | Purpose |
|-----------|-------|---------|
| `_adj_out` | `(src, predicate, dst)` | Outgoing typed entity-to-entity hops |
| `_adj_in` | `(dst, predicate, src)` | Incoming typed entity-to-entity hops |
| `_entity_search` | `(uri, type_uri, label, uri_lc, label_lc)` | Preview (Inferred on) |
| `_entity_search_asserted` | same | Preview (Inferred off) |
| `_props` | `(subject, predicate, object)` | Expansion payload for typed subjects |

The companions are snapshots of the reader-facing graph at the moment of the
rebuild — this is a full snapshot replacement, **not** an incremental refresh.
Build and **Refresh cache** both trigger the same `rebuild_adjacency` backend
operation; Refresh cache is a lightweight alternative when the underlying source
data has not changed and only the indexes need to be brought up to date.

For Lakehouse (Delta) graphs, four or five companion tables are rebuilt
concurrently using a `ThreadPoolExecutor(max_workers=min(5, N))` per rebuild
invocation, where N is the number of companions scheduled. The fifth companion,
`entity_search_asserted`, is conditional: it is added only when both the
asserted-SPO table and the companion FQN resolve to non-empty strings. Each
worker submits its DDL statement through the Databricks SQL connector, which
maintains a thread-safe connection pool; each worker borrows a separate pooled
connection so the CTAS statements run on the warehouse in true parallel without
contention on the Python-side connection. (The Statement Execution API is used
only for real-time reads — not for the DDL/write rebuild path.) Per-companion elapsed time and total wall time are
logged at `INFO` level on completion.

Lakebase graphs rebuild adjacency in three sequential steps that remain serial:
DDL (table creation / schema changes) runs outside a transaction, then all
companion data is replaced inside a single Postgres transaction, then ANALYZE
runs in a separate non-transactional step. The transactional data-rebuild phase
keeps the reader-facing union view consistent and avoids partial reads during
the window; DDL and ANALYZE are intentionally excluded from the transaction.

## 2. Materialized Delta read layer

In Lakehouse `table` materialization mode, the mapping result is copied into a
Delta `_data` table:

```sql
CREATE OR REPLACE TABLE <graph>_data
USING DELTA
CLUSTER BY (predicate, subject)
AS SELECT subject, predicate, object FROM <mapping_view>
```

The layout co-locates common predicate/subject filters. `OPTIMIZE` compacts
files and applies Liquid Clustering after builds. App-written inferred triples
remain in `_inferred`; the `_graph` view combines `_data` and `_inferred`.

In `view` materialization mode, `_data` remains a pass-through view. This avoids
copying source data but makes raw SPO queries re-run mapping SQL. The
physical graph-index companions are therefore especially important in this
mode.

Implementation:

- `src/back/core/graphdb/delta/materialize.py`
- `src/back/core/graphdb/delta/DeltaFlatStore.py`

## 3. Entity Preview index

`_entity_search` reduces Preview from repeated SPO joins/scans to one bounded
lookup. It stores one deterministic row per typed entity:

- the subject URI;
- one type (`MIN(rdf:type)`);
- one label (`MIN(rdfs:label)`, or an empty string);
- normalized lowercase URI and label columns.

Preview filters directly on `type_uri`, `uri_lc`, and `label_lc`, then probes
with `LIMIT 501` (no warehouse `ORDER BY`). The extra row indicates that the
displayed 500-result list was capped without running a separate count query.
Rows are sorted by type then label in the app. Explorer Search defaults to
**Starts with**; **Contains** remains available, including programmatic
local-name lookup.

### Lakehouse layout

The Delta table uses `CLUSTER BY (type_uri, label_lc)` plus a best-effort
Bloom filter on `label_lc` and `uri_lc`. Type-restricted prefix searches
benefit most; Bloom helps equality more than leading-wildcard `contains`.

### Lakebase indexes

Lakebase creates:

- a primary key on `uri`;
- a btree on `type_uri`;
- `text_pattern_ops` btrees on `label_lc` and `uri_lc`;
- `pg_trgm` GIN indexes on `label_lc` and `uri_lc` when the extension can be
  created (skipped if the role cannot `CREATE EXTENSION`).

The pattern indexes accelerate equality and prefix (`starts with`) lookups.
GIN accelerates `contains` (`LIKE '%value%'`) when the planner selects it.

### Asserted-only Preview

The optimized union table is used when Explorer **Inferred** is enabled.
When Inferred is off, Preview uses `_entity_search_asserted`, rebuilt from
`_data` (Lakehouse) or `_sync` (Lakebase). A missing companion falls back to
the asserted SPO relation; unrelated SQL errors are not swallowed.

Implementation:

- `src/back/core/graphdb/entity_search.py`
- `GraphDBBackend.find_preview_seeds`

## 4. Query-shape and round-trip reductions

Several smaller optimizations avoid unnecessary warehouse work:

- Entity metadata fetch requests `rdf:type` and `rdfs:label` in one
  predicate-`IN` query, then splits the rows in Python. This replaces two
  scans/round trips through the graph union view.
- Top-node analytics applies `LIMIT` before joining labels and types, so
  metadata joins touch only the returned nodes.
- Delta relation existence uses `SELECT 1 ... LIMIT 1`, not `COUNT(*)`.
  Missing-relation errors map to false; authorization and transport failures
  still propagate.
- Lakebase relation existence queries `information_schema.tables` and
  `information_schema.views` with `UNION ALL ... LIMIT 1`.
- Lakebase URI-alias expansion groups `%/<local-id>` patterns by suffix length
  and emits `RIGHT(subject, length) = ANY(ARRAY[...])`. Query size therefore
  scales with the few distinct suffix lengths rather than hundreds or
  thousands of `OR subject LIKE ...` clauses.

Implementations:

- `GraphDBBackend.get_entity_metadata`
- `GraphDBBackend.get_top_nodes_by_degree`
- `DeltaFlatStore.table_exists`
- `LakebaseFlatStore.table_exists`
- `LakebaseFlatStore.find_subjects_by_patterns`

## 5. Bidirectional adjacency

The adjacency tables contain only typed entity-to-entity relationships.
`rdf:type`, `rdfs:label`, literal-valued properties, and links to untyped
objects are excluded from hop discovery.

Two physical directions avoid an `OR` over subject and object for every BFS
step:

```sql
SELECT dst FROM <graph>_adj_out WHERE src IN (...)
UNION ALL
SELECT src FROM <graph>_adj_in WHERE dst IN (...)
```

Physical layout:

| Backend | `_adj_out` | `_adj_in` |
|---------|------------|-----------|
| Lakehouse | `CLUSTER BY (src, predicate)` | `CLUSTER BY (dst, predicate)` |
| Lakebase | primary key `(src, predicate, dst)` + btree `(src, predicate)` | primary key `(dst, predicate, src)` + btree `(dst, predicate)` |

This makes both outgoing and incoming exploration key-based rather than a scan
of the SPO union.

## 6. Single-statement bounded BFS

Lakehouse and Lakebase perform multi-level expansion and payload retrieval in
one SQL statement. Each BFS level reads the adjacency tables, removes entities
already visited, and applies the configured entity bound.

The dialect-specific visited-set operation is:

- Spark: `LEFT ANTI JOIN`;
- Postgres: `WHERE NOT EXISTS`.

Seed URIs are deduplicated before SQL generation. Candidate levels use
`UNION ALL`; deduplication happens once at the level boundary rather than on
every branch.

The query probes `max_entities + 1` and `max_triples + 1`. This detects
truncation without unbounded counting or transferring an oversized graph.

Compatibility fallbacks retain the same bounds:

1. If adjacency tables are unavailable, Delta generates fixed-depth SPO CTEs;
   Lakebase derives temporary `adj_out`/`adj_in` CTEs from the SPO relation.
2. A backend without single-statement expansion uses the legacy Python loop,
   querying one frontier at a time and fetching subject triples in batches.
   That path also has a 120-second payload-fetch budget.

Implementation: `src/back/core/graphdb/adjacency.py`.

## 7. Property companion for payload fetch

Adjacency makes hop discovery fast, but it intentionally omits labels, types,
literals, and some IRI-valued properties. `_props` closes the remaining
performance gap by materializing every outgoing triple whose subject is a
typed instance.

After BFS, the final query joins discovered entities to `_props`:

```sql
SELECT p.subject, p.predicate, p.object
FROM <graph>_props p
JOIN discovered_entities e ON e.entity = p.subject
LIMIT <max_triples + 1>
```

Physical layout:

- Lakehouse: `CLUSTER BY (subject)` plus best-effort `OPTIMIZE`.
- Lakebase: btree on `subject`, followed by `ANALYZE`.

If `_props` is absent on a graph built before this optimization, the backend
retries the same adjacency expansion with the reader-facing SPO relation for
the final payload. Only a missing-table error triggers this compatibility
fallback. The process remembers that negative result so later expansions skip
the guaranteed-failing companion query. A successful Build or **Refresh cache**
clears the negative after rebuilding `_props`.

Depth-zero expansion bypasses BFS CTEs and filters the payload relation directly
with `WHERE subject IN (...)`. Spark BFS payload joins broadcast the bounded
entity set instead of shuffling it with the graph relation.

Implementation: `src/back/core/graphdb/props.py`.

## 8. Aggregate and analysis pushdown

The graph overview combines total triples, distinct subjects/predicates, type
assertions, and labels in one aggregate query.

Delta overrides exact `COUNT(DISTINCT ...)` with Spark
`approx_count_distinct(...)` for the overview. This avoids a full distinct
shuffle on multi-million-row triple tables; the approximate value is
appropriate for at-a-glance statistics. Lakebase keeps exact Postgres
distinct counts.

Filtered graph analytics pushes class and predicate scope into SQL through
`query_triples_for_analysis`. For class filters, it loads selected instances
plus their direct neighbors and metadata instead of transferring the complete
triple store and filtering only in Python. Non-SQL engines retain the
in-process fallback.

Implementation:

- `GraphDBBackend.get_aggregate_stats`
- `DeltaFlatStore._distinct_count_expr`
- `GraphDBBackend.query_triples_for_analysis`

## 9. Snapshot rebuild and statistics

All four companions share one refresh clock:

- full Knowledge Graph Build;
- manual **Refresh cache**;
- reasoning materialization when triples change;
- cohort writes that change graph edges/properties.

Graph Cache Refresh can also run as a standalone Scheduler task for a selected
domain and version. It invokes the same full companion-index rebuild as the
interactive **Refresh cache** action. The task is available for Lakehouse and
Lakebase graphs; Neo4j does not use these companions.

Lakebase performs `TRUNCATE` and `INSERT` for all companions in one
transaction, then runs `ANALYZE` so the Postgres planner sees current
statistics.

Lakehouse uses sequential `CREATE OR REPLACE TABLE` operations followed by
best-effort `OPTIMIZE`. The replacements are not a cross-table atomic swap;
avoid reading during a Build/Refresh when a same-instant view of all companions
is required.

Freshness differs by mode:

- **Lakehouse table mode:** source-table changes require a full Build because
  `_data` is a snapshot.
- **Lakehouse view mode:** Refresh cache re-runs the live mapping view and
  captures current source rows in the companions.
- **Lakebase:** Refresh cache captures the current reader-facing union view.

Post-write maintenance also preserves read performance:

- Delta applies best-effort `OPTIMIZE` to the inferred and graph-index tables.
- Lakebase runs `VACUUM ANALYZE` on app-managed bulk data and writable
  companions (only the writable companion in managed-synced mode).
- Large Lakebase mutations use `COPY FROM STDIN` staging plus set-based
  `INSERT ... ON CONFLICT DO NOTHING` or `DELETE ... USING`; small mutations
  avoid staging overhead.

## 10. GraphQL and MCP find read the same companions

`query_graphql`'s typed list resolver (`SchemaMetadata._query_subjects` →
`GraphDBBackend.find_subjects_by_type`) and its nested-field triple loader
(`SchemaMetadata._load_triples` → `GraphDBBackend.get_triples_for_subjects`)
read `_entity_search` and `_props` when ready, instead of the reader-facing
SPO relation. `search` on the GraphQL list resolver then matches only
`rdfs:label`/URI (`_entity_search`'s `label_lc`/`uri_lc`) — the same fields
Explorer Preview searches — rather than every literal predicate.

MCP's `describe_entity` and the shared `/triples/find` route
(`DigitalTwin.find_triples_bfs` → `GraphDBBackend.bfs_traversal`) seed from
`_entity_search` and walk `_adj_out`/`_adj_in` instead of a `WITH RECURSIVE`
scan of the SPO relation, when both companions are ready. Hop endpoints then
require both sides of an edge to have an `rdf:type` assertion, matching
Explorer's own adjacency restriction; a companion-missing graph (pre-build,
pre-refresh) or a missing-table error mid-query falls back to the exact
`WITH RECURSIVE` SQL used before this change.

`/triples/find` response pagination is deterministic on `(subject, predicate,
object)` and reports exact `total` plus additive `has_more` for downstream
formatters/clients; truncation hints should follow `has_more` rather than
deriving from `total > page_size`.

`entity_type` keeps two matching modes that already existed before this
change and are preserved exactly: GraphQL/Preview take a full class URI
(`type_uri` equality); MCP's `describe_entity`/`/triples/find` take a bare
local name (`type_uri` suffix match, `#name` / `/name`).

Neo4j is unaffected — its `find_subjects_by_type`, `get_triples_for_subjects`,
and `bfs_traversal` overrides use native Cypher and never reach these
companion branches.

Implementation:

- `back/core/graphdb/entity_search.py`: `entity_search_uri_search_sql`,
  `entity_search_seed_sql`, `_entity_search_text_clause`,
  `is_missing_relation_error`.
- `back/core/graphdb/adjacency.py`: `seeded_bfs_sql`.
- `back/core/graphdb/GraphDBBackend.py`: `find_subjects_by_type`,
  `get_triples_for_subjects`, `bfs_traversal`.

## 11. Query bounds and cancellation

Graph reads are protected independently from long-running build operations:

- default graph statement timeout: 60 seconds;
- configurable range: 5–900 seconds;
- default graph-chat result cap: 10,000 triples;
- configurable result-cap range: 100–100,000 triples;
- Explorer Preview, entity, triple, and depth bounds are applied before result
  serialization.

Lakebase scopes `SET statement_timeout` to the borrowed pooled connection and
resets it in `finally`, preventing the timeout from leaking into later writes.

Lakehouse passes the same graph timeout to the SQL service. Thrift connections
use warehouse `STATEMENT_TIMEOUT`; Statement Execution API requests use a
client deadline and cancel timed-out statements.

Implementation: `src/back/core/query_limits.py`.

## 12. Lakehouse/RT query transport

Build and graph reads can use different warehouses:

- Build and graph-index DDL always use the Build SQL Warehouse over Thrift.
- Interactive reads may use a dedicated Lakehouse/RT query warehouse.

Inside Databricks Apps, RT reads use the Statement Execution API with
`JSON_ARRAY` and `INLINE` results. This avoids Kernel CloudFetch downloads from
external object storage, which are unavailable from the App runtime. Internal
chunk links are followed only on the workspace host; external links and
truncated responses are rejected.

Local development keeps the SQL connector/Kernel path when configured.

Implementation:

- `src/back/core/databricks/StatementExecutionWarehouse.py`
- `src/back/core/databricks/DatabricksClient.py`

The regular SQL Warehouse service also retries transient transport failures
up to three times with bounded backoff and a fresh connection. Retry
classification covers cold-start/auto-scaling 5xx responses and stale pooled
connections; SQL errors and unsupported-protocol configuration errors fail
immediately.

## 13. Neo4j native indexes and traversal

Neo4j does not mirror the SQL graph-index companions. Each graph gets a marker
label and a uniqueness constraint on node `uri`; Neo4j uses the constraint's
backing index for identity lookup and `MERGE`.

Entity types are represented as node labels and graph relationships are real
Neo4j relationships. Search can therefore match a graph marker plus a class
label directly, while expansion uses native variable-length, bidirectional
Cypher paths rather than SQL BFS over SPO rows. Seed lists and search values
are passed as parameters, and result limits are pushed into Cypher.

Implementation:

- `src/back/core/graphdb/neo4j/Neo4jWriteOps.py`
- `src/back/core/graphdb/neo4j/Neo4jReadOps.py`

## 14. Connection and transfer efficiency

Lakebase uses a connection pool rather than opening a Postgres connection for
each graph query. Expansion is returned in one result set instead of one
request per BFS level.

The optimized Preview and expansion paths avoid preliminary table-existence
round trips. They optimistically query companion tables and fall back only
when the database reports a missing relation. This reduces steady-state
latency while preserving compatibility with graphs built by older versions.

Repeated graph-status and artifact-existence probes use the Digital Twin
session cache. Only successful, known results are cached; transient failures
are not stored as false negatives. This cache reduces page-level metadata
queries but does not cache Explorer neighborhoods or query result sets.

## 15. Browser-observed timings

Explorer records the latest completed search timing as:

- Preview request;
- Expansion request;
- Display/render;
- Total active search time.

The total excludes user dwell in the seed-selection dialog. This split helps
distinguish database search cost, traversal/payload cost, and browser rendering
cost before choosing the next optimization.

On 2026-09-16, a BIGCustomers Lakehouse graph built before `_props` measured:

- Preview: about 870 ms for both Starts with and Contains on one selective term;
- Expansion depth 0 / 1 / 2 / 3: 10.85 s / 5.84 s / 2.90 s / 3.71 s;
- depth 3 returned 14,594 triples, while request transfer added only 2–3 ms.

The missing `_props` error and SPO retry occurred on every expansion. These
measurements prioritize companion refresh and expansion SQL over more Preview
work. Re-run the same comparison after `_props` is present before drawing
conclusions about rendering or payload changes.

After **Refresh cache** rebuilt `_props`, the same three-run benchmark measured
1.48 s / 1.83 s / 1.69 s for depths 0 / 1 / 2. That is approximately 86%,
69%, and 42% faster than the stale-companion path, respectively. No missing
`_props` errors appeared. The first request at each shape remained slower,
consistent with warehouse/cache warm-up.

Implementation: `src/front/static/query/js/query-sigmagraph.js`.

## 16. Concrete operating example

After a Build or after reasoning/cohort writes:

1. Open **Knowledge Graph → Build**.
2. Run **Refresh cache** if a full Build is not required.
3. Confirm an old graph no longer logs a missing `_props` companion during
   expansion before tuning Preview.
4. In Explorer Search, select an entity type when possible.
5. Prefer **Exact** or **Starts with** over **Contains** for selective indexed
   Preview searches.
6. Keep depth and entity/triple caps as low as the use case allows.
7. Open the search timing details. Optimize Preview if Preview dominates;
   optimize hop/payload paths only if Expansion dominates.

For a Lakehouse table-mode domain, use a full Build when source tables changed;
Refresh cache alone only reindexes the existing `_data` snapshot.

## 17. Backend summary

| Technique | Lakehouse | Lakebase | Neo4j |
|-----------|-----------|----------|-------|
| Materialized/clustered SPO | Delta table mode | Native Postgres storage | Native graph |
| `_entity_search` | Delta, clustered by type | Btree/pattern indexes | Native query |
| `_adj_out` / `_adj_in` | Delta, clustered by endpoint | Endpoint btrees | Native traversal |
| `_props` | Delta, clustered by subject | Subject btree | Native properties |
| Single-statement bounded BFS | Spark SQL | Postgres SQL | Native Cypher path |
| Statement timeout | Warehouse/SEA | Postgres | Driver/query behavior |
| Dedicated RT read transport | Optional | N/A | N/A |

## 18. Planned, not yet implemented

Application-side Preview sorting, Starts-with default, asserted search,
Lakebase trigram indexes, and Lakehouse search clustering/Bloom are shipped.

Do not implement type columns on adjacency, N-hop materialization, integer ID
interning, CSR arrays, visualization-only payloads, or process-local
neighborhood caches without new timings after `_props` is present. Predicate
layout changes are useful only for a measured predicate-filtered traversal.

See:

- `docs/superpowers/plans/2026-09-15-search-transversal-next.md`
- `docs/superpowers/plans/2026-09-16-expansion-fallback-latency.md`

## 19. Parallel companion rebuild — measured performance

On 2026-09-17, a **Refresh cache** on the BIGCustomers Lakehouse domain
(~1.35 M entity-search rows, ~1.70 M adjacency rows, ~7.64 M props rows) was
timed before and after the parallel implementation shipped in `e5e6ced7`.
The serial baseline was recorded on 2026-09-16; the parallel run on 2026-09-17.
Exact data equality between the two runs was not recorded — the speedup is
**indicative rather than a controlled benchmark** but represents the same named
domain under comparable load.

| Run | Wall time | Method |
|-----|-----------|--------|
| Prior serial baseline | 128.62 s | Sequential rebuild |
| Parallel (N=5 workers) | **45.0 s** | `ThreadPoolExecutor(max_workers=min(5, 5))` |

N=5 because `entity_search_asserted` resolved (asserted-SPO table present).
**Indicative speedup: ~2.86× / ~65 % wall-time reduction.**

Per-companion log lines from the parallel run:

| Companion | Elapsed |
|-----------|---------|
| `adj_in` | 37.3 s |
| `adj_out` | 38.6 s |
| `entity_search_asserted` | 44.4 s |
| `props` | 44.5 s |
| `entity_search` | 45.0 s |

The adjacency tables complete first (37–38 s); the three heavier companions
finish together at the 44–45 s mark. The critical path is `entity_search` at
45 s — exactly what the full wall time measures. No warehouse admission queuing
was observed; all five CTAS statements ran in parallel because `entity_search_asserted`
was present (N=5).

Post-rebuild smoke checks confirmed `adjacency_ready = True`, correct row
counts in all five companions, successful entity search (label lookup), and
successful expansion (depth 1, 6 entities, 30 triples from one seed).

The `DROP_COMMAND_TYPE_MISMATCH` warnings that appear when companions already
exist as tables (rather than legacy views) are benign and handled by the
existing `materialize.drop_relation` guard. The Bloom filter warning on
`entity_search` is a warehouse-tier limitation and does not affect search
correctness.

Implementation: `src/back/core/graphdb/delta/DeltaFlatStore.py`
(`rebuild_adjacency`, `_rebuild_adjacency_table`, `_rebuild_props_table`,
`_rebuild_entity_search_table`).

## Related documentation

- [Graph DB integration](graphdb-integration.md)
- [Architecture](architecture.md)
- [Lakebase graph DB](lakebase-graphdb.md)
- [User guide](user-guide.md)
