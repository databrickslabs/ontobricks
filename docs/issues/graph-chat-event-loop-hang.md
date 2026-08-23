# Graph Chat read bounds and overload handling

GitHub issue #114 reported that a broad Graph Chat question could freeze the
application until redeployment. The original failure had two independent
parts:

1. Blocking graph reads ran on the uvicorn event loop.
2. Database reads had no server-side timeout or Graph Chat result ceiling.

The event-loop problem is already fixed on `develop`: internal graph routes
execute blocking work through `run_blocking`. Performance fixes #112 and #115
also reduced the frequency of expensive reads.

The remaining resilience gap is bounded by this change. A pathological read
can no longer hold a Lakebase or SQL warehouse connection indefinitely, and
excessively broad `triples/find` responses are capped before reaching the
agent.

## Runtime bounds

Two settings apply only to graph reads:

- `graph_query_timeout_s` cancels a graph statement server-side. It defaults
  to 60 seconds and is clamped to 5–900 seconds.
- `graph_chat_result_cap` limits triples returned by `triples/find`. It
  defaults to 10,000 and is clamped to 100–100,000.

Resolution order is:

1. An administrator override saved in the registry global configuration.
2. `ONTOBRICKS_GRAPH_QUERY_TIMEOUT_S` or
   `ONTOBRICKS_GRAPH_CHAT_RESULT_CAP`.
3. The built-in default.

Administrators can change both values in **Settings → Graph DB → Graph read
limits**. Entering `0` clears the saved override.

## Backend behavior

- Lakebase applies PostgreSQL `statement_timeout` for each graph read and
  resets it in `finally` before returning the pooled connection.
- The SQL warehouse applies `STATEMENT_TIMEOUT` around bounded graph reads and
  restores the unbounded session value afterward.
- Build pipelines and full-graph exports keep their existing unbounded paths.
- The Graph Chat loopback timeout is longer than the database timeout, allowing
  the server-side cancellation to reach the agent as a normal error.

## Resource pressure

The dedicated blocking pool scales from the Databricks App vCPU count while
retaining the historical minimum of 20 workers. When all workers are occupied,
Graph Chat responses include a resource-pressure advisory so users see why
responses are delayed and administrators can consider a larger App instance.

This advisory is diagnostic only. The statement timeout and result cap remain
the safeguards that bound each individual graph read.
