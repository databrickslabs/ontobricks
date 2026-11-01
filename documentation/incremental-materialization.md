# Incremental triple materialization

<!-- Task 8 adds modes, readiness, fallbacks, previews, realtime reservation here -->

## Lakehouse//RT

In the incremental and microbatch modes `_graph` is a materialized view, so
Lakehouse//RT can read it (RT reads managed tables, materialized views and
streaming tables, not plain views).

Graph reads go to the query warehouse: set `graph_engine_config.lakehouse.warehouse_id`
and turn on "Use Lakehouse//RT for queries" in Settings > Back end > Lakehouse.
A heavy traversal can trip an RT guardrail (2-minute runtime, 300 GB shuffle,
statement timeout). `DeltaFlatStore.execute_query` then retries the statement
once on the build warehouse, still bounded by the graph statement timeout, and
logs a warning with the SQL length and its first 120 characters. Any other
error propagates unchanged.

`DeltaFlatStore.last_read_route` reports which path answered the last read:
`"query"` or `"build-fallback"`.
