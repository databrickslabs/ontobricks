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

`GET /dtwin/metrics/series?metric=<name>&offset=<n>&limit=<n>`

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
5. Parses `offset` and `limit` as non-negative integers. The default page size
   is 25,000 rows and the server-enforced maximum is also 25,000.
6. Reads one page ordered by the selected score descending, then `node_uri`
   ascending for deterministic tie-breaking. `COUNT(*) OVER()` supplies the
   total without a second query.
7. Returns parallel arrays rather than one JSON object per node:

```json
{
  "success": true,
  "has_result": true,
  "metric": "pagerank",
  "computed_at": "2026-09-01T10:00:00Z",
  "offset": 0,
  "total": 1350051,
  "next_offset": 25000,
  "uris": ["..."],
  "labels": ["Customer"],
  "scores": [0.42]
}
```

The query uses the same Databricks credentials, SQL warehouse, output schema,
and table naming rules as `JobMetrics`. The selected column is interpolated
only after allowlist validation; pagination values are parsed integers.
Missing output tables or Databricks failures surface through the existing
`InfrastructureError` path.

The stored result's `computed_at` is included in the response and used by the
frontend as a cache generation. This prevents a series from an older run being
combined with newly loaded KPIs.

## Frontend design

The existing metric tiles and segmented metric selector remain unchanged.
Selecting a metric:

1. Immediately updates the selected state and chart title.
2. Reuses an in-memory series cached by `(computed_at, metric)` when available.
3. Otherwise shows the existing branded inline loading treatment inside the
   ranking card and requests `/dtwin/metrics/series` pages sequentially until
   `next_offset` is null. The loading caption reports loaded and total nodes.
4. Uses an `AbortController` when the metric or Analytics generation changes.
   A late response is also generation-checked before it can update the chart.
5. Stores only complete series in the reusable cache; aborted partial series
   are discarded.

The chart uses Chart.js's line controller with visible points, which provides
the requested scatter appearance plus a connecting line:

- X axis: one-based rank, sorted by score descending.
- Y axis: metric score.
- Tooltip: rank, display label, URI, and score.
- Point click: navigate to the selected node in Explorer, preserving the
  current behavior.
- Title: `Nodes by <Metric>`; no Top-N wording.
- The approximation and unavailable-metric notices remain unchanged.

For series of up to 5,000 nodes, every point is rendered. Above that threshold,
Chart.js's built-in LTTB decimation renders 2,000 representative samples while
the full source series remains loaded. The UI states both the total node count
and that visual decimation is active. The tooltip and click behavior operate
on retained source points.

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
- pagination parsing, maximum page size, total count, and final-page behavior;
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
- sequential page assembly and progress;
- stale-response rejection;
- LTTB threshold configuration and notice;
- tooltip and Explorer click-through;
- logarithmic distribution-strip behavior remains independent;
- desktop and mobile rendering without console errors.

## Out of scope

- Changing the Analytics computation algorithms.
- Changing the five distribution histograms.
- Persisting all-node series in the registry.
- Sending all-node series to the AI interpretation agent.
- Adding another charting library or a WebGL renderer.
