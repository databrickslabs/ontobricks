# KG Analytics all-node metric series

## Goal

Replace the selected-metric Top-N horizontal bar chart in Knowledge Graph →
Analytics with a ranked point-and-line chart built from every scored node.
Keep the existing Top N control for the detail table only.

## Current constraint

The Analytics job writes one row per node to its per-domain-version Delta
metrics table, but the cached `graph_analytics.result.nodes` payload contains
only the union of the configured Top-N rows for each metric. Expanding that
registry JSON to every node would make Lakebase rows and `/metrics/latest`
responses unbounded. The exhaustive series must therefore be read from the
Delta output table on demand.

## Backend design

Add a read-only endpoint:

`GET /dtwin/metrics/series?metric=<name>`

Accepted metric names are exactly:

- `pagerank`
- `betweenness`
- `degree`
- `closeness`
- `clustering`

The endpoint:

1. Resolves the current domain and its latest stored Analytics result.
2. Returns `has_result: false` when no completed result exists.
3. Rejects metric names outside the allowlist before generating SQL.
4. Derives the existing per-domain-version Analytics output table with
   `DigitalTwin.analytics_output_table`.
5. Executes one exhaustive query ordered by the selected score descending,
   then `node_uri` ascending for deterministic tie-breaking.
6. Keeps every point through 5,000 rows. Above that threshold, applies LTTB
   server-side and returns 2,000 representative source points with their
   original one-based ranks. All rows participate in the sampling decision.
7. Returns parallel arrays rather than one JSON object per point:

```json
{
  "success": true,
  "has_result": true,
  "metric": "pagerank",
  "computed_at": "2026-09-01T10:00:00Z",
  "total": 1350051,
  "sampled": true,
  "ranks": [1],
  "uris": ["..."],
  "labels": ["Customer"],
  "scores": [0.42]
}
```

The query uses the same Databricks credentials, SQL warehouse, output schema,
and table naming rules as `JobMetrics`. The selected column is interpolated
only after allowlist validation. Missing output tables or Databricks failures
surface through the existing `InfrastructureError` path.

This one-query/server-sampling design supersedes the initial 25,000-row client
pagination. Runtime validation on `bigcustomers` showed that approach required
54 full-table ordered queries, approximately 137 MB of browser transfer, and
5 minutes 29 seconds to display PageRank. Server-side LTTB retains the visual
shape derived from all scores while removing repeated warehouse scans and the
unbounded browser payload.

The stored result's `computed_at` is included in the response and used by the
frontend as a cache generation. This prevents a series from an older run being
combined with newly loaded KPIs.

## Frontend design

The existing metric tiles and segmented metric selector remain unchanged.
Selecting a metric:

1. Immediately updates the selected state and chart title.
2. Reuses an in-memory series cached by `(computed_at, metric)` when available.
3. Otherwise shows the existing branded inline loading treatment inside the
   ranking card and requests `/dtwin/metrics/series` once.
4. Uses an `AbortController` when the metric or Analytics generation changes.
   A late response is also generation-checked before it can update the chart.
5. Stores only complete responses in the reusable cache; aborted requests are
   discarded.

The chart uses Chart.js's line controller with visible points, which provides
the requested scatter appearance plus a connecting line:

- X axis: one-based rank, sorted by score descending.
- Y axis: metric score.
- Tooltip: rank, display label, URI, and score.
- Point click: navigate to the selected node in Explorer, preserving the
  current behavior.
- Title: `Nodes by <Metric>`; no Top-N wording.
- The approximation and unavailable-metric notices remain unchanged.

For series of up to 5,000 nodes, every point is returned and rendered. Above
that threshold, the backend returns 2,000 LTTB samples carrying their original
ranks, labels, URIs, and scores. The UI states both the total node count and
that visual sampling is active. Tooltip and click behavior operate on those
retained source points.

The `analyticsTopN` input no longer triggers a chart rebuild. It continues to
control only the detail table below the chart.

## State and invalidation

- Clear all metric-series caches when `_clearAnalyticsResults` runs.
- Clear caches when a newly loaded Analytics result has a different
  `computed_at`.
- Abort or ignore stale requests when the active metric changes.
- Do not persist chart selection or downloaded series across page reloads.

## Accessibility and empty states

- Keep the existing five labeled metric buttons and pressed-state semantics.
- Give both axes visible titles.
- Preserve the explanatory notices for unavailable and all-zero metrics.
- On a series-load failure, keep the KPI/distribution dashboard visible and
  show an Analytics-local error in the ranking card plus the global
  notification.

## Testing

Backend tests cover:

- metric allowlist validation;
- one exhaustive ordered query per selected metric;
- deterministic LTTB output, threshold behavior, original ranks, and total;
- deterministic exhaustive SQL ordering;
- output-table resolution;
- no-result response;
- successful payload serialization;
- infrastructure error propagation.

Frontend contract and browser tests cover:

- line chart with points instead of a bar chart;
- no `.slice(0, _topN())` in the selected-metric chart;
- Top N affects only the detail table;
- per-generation/per-metric caching;
- one-request response assembly;
- stale-response rejection;
- server-sampling threshold and notice;
- tooltip and Explorer click-through;
- logarithmic distribution-strip behavior remains independent;
- desktop and mobile rendering without console errors.

## Out of scope

- Changing the Analytics computation algorithms.
- Changing the five distribution histograms.
- Persisting all-node series in the registry.
- Sending all-node series to the AI interpretation agent.
- Adding another charting library or a WebGL renderer.
