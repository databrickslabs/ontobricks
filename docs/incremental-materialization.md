# Incremental triple materialization

<!-- Task 8 adds modes, readiness, fallbacks, previews, realtime reservation here -->

## Lakehouse//RT

In the incremental and microbatch modes `_data` becomes a materialized view that
Lakehouse//RT can read directly (RT reads managed tables, materialized views and
streaming tables, not plain views). `_graph` remains a union view for now.

Graph reads go to the query warehouse (`graph_engine_config.lakehouse.warehouse_id`
plus "Use Lakehouse//RT for queries"). A heavy traversal can trip an RT guardrail
(2-minute runtime, 300 GB shuffle, statement timeout)
<!-- TODO cite: Lakehouse//RT docs for the guardrail values -->
and `DeltaFlatStore.execute_query` retries that SELECT/WITH once on the build
warehouse, as the same caller (same OBO token), under the graph statement timeout.
Writes, other errors and a build warehouse equal to the query warehouse are not
retried. `last_read_route` (`"query"` or `"build-fallback"`) tells which path answered.
