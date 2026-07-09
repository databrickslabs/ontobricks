# [BUG]: Graph Chat heavy query blocks the event loop and freezes the app until redeploy

> Tracked as GitHub issue #114. Formatted to match `.github/ISSUE_TEMPLATE/bug.yml`.

## Is there an existing issue for this?

- [x] I have searched the existing issues

## Current Behavior

Asking Graph Chat a broad question (e.g. *"What are the top 10 violations?"*) hangs
the request and then freezes the **entire** app: no other request — from any user —
completes until the app is redeployed.

Root cause: the Graph Chat agent runs in a worker thread and makes **synchronous
loopback HTTP calls** back into the same FastAPI process. The internal `/dtwin/...`
routes it hits (`dtwin_triples_find`, `dtwin_graphql_execute`, `dtwin_neighbors`,
`sync/stats`) are declared `async def` but execute **blocking DB work directly on the
asyncio event loop** — notably the recursive BFS CTE in
`TripleStoreBackend.bfs_traversal`. That query joins on
`(t.subject = b.entity OR t.object = b.entity)` with no `LIMIT` and no DB
`statement_timeout`, so a broad seed explodes into a multi-minute / effectively
unbounded query. Because it runs on the event loop, the single uvicorn worker is
frozen for the whole query, stalling every other request. The agent's httpx client
timeout fires client-side (`triples/find error: timed out`) but does **not** cancel
the server-side query, so the loop stays blocked and the Lakebase connection pool
(`_POOL_MAX_SIZE = 4`) gets pinned.

This violates the project's own guidance in `src/.coding_rules.md`
("synchronous I/O in an async handler blocks the event loop … wrap it in
`asyncio.to_thread`"). The helper already exists: `DatabricksHelpers.run_blocking()`.

## Expected Behavior

- Slow/broad questions time out **gracefully** with an error message the agent can
  react to, cancelled server-side by a `statement_timeout`.
- The app stays responsive to all other users/requests while a heavy Graph Chat
  query runs (blocking DB work is offloaded off the event loop).
- Under sustained load, the app advises upgrading the Databricks App instance size
  instead of silently freezing.

## Steps To Reproduce

1. Create / load a domain and build its Knowledge Graph (Lakebase backend).
2. Open the **Graph Chat** tab.
3. Ask a broad question, e.g. *"What are the top 10 violations?"*.
4. Observe the request hang; then observe that all other pages/requests also stall
   until the app is redeployed.

## Cloud

<!-- AWS / Azure / GCP -->

## Browser

<!-- Chrome / Firefox / Edge / Safari / Other -->

## OntoBricks Version

0.6.1

## Relevant log output

```shell
WARNING  agents.agent_graph_chat_tools | tools._error:71 | agent_dbx_chat: triples/find error: timed out
INFO     uvicorn.access | 'POST /dtwin/graphql/execute HTTP/1.1' 200
# ... after the timeout, no further requests are served until redeploy
```

## Additional Context

- Graph backend: Lakebase Postgres (the same class of bug affects the Delta/SQL
  warehouse backend, which likewise has no per-statement cancellation — only a
  30s socket timeout).
- Deployed logs reference `agents.agent_graph_chat_tools` / `agent_dbx_chat`, which
  are renamed to `agent_dtwin_chat` on current `main`; the architectural bug is
  identical on `main`.

## Related issues (query-performance layer)

This issue is the **resilience / graceful-degradation** half of the problem. The
contributor **@ulsmo** confirmed the bug and traced the *triggers* on a
1–5M-triple graph to two underlying performance bottlenecks — this fix bounds
their blast radius but does not replace them:

- **#112** — Indexes dropped/not applied during Lakeflow sync, leaving `_sync`
  tables unindexed and causing exploding query times. A root cause of the slow
  reads that trip this hang.
- **#115** — `Perf(graphdb): Optimize finding subjects with local id's` (PR):
  optimizes `describe_entity` alias expansion on large ID sets — the same
  `expand_uri_aliases` / `get_triples_for_subjects` path the `triples/find` route
  invokes.

These are complementary: even with #112 and #115 fixed, an unbounded read can
still starve the single event loop, so the bounding + offloading here is still
required.

## Proposed Fix

1. Offload the blocking DB work in the internal `/dtwin` graph routes via
   `run_blocking(...)` so it never runs on the event loop.
2. Add a configurable graph-read `statement_timeout` on **both** backends
   (Lakebase `SET statement_timeout`, warehouse `SET STATEMENT_TIMEOUT`) so a runaway
   query is cancelled server-side.
3. Auto-tune the blocking thread pool from the instance size and surface an
   admin-configurable advisory recommending an instance-size upgrade under pressure.

### Caveat / trade-off

On an **unindexed** large graph (see #112), a legitimate `describe` / `triples/find`
can now exceed the default 60s `statement_timeout` and be **cancelled** — turning a
*silent app-wide freeze* into a *clean, per-request "query cancelled" error*. This
is the intended trade-off; the admin timeout knob (up to 900s) lets operators raise
the bound until #112/#115 fix the underlying query speed.
