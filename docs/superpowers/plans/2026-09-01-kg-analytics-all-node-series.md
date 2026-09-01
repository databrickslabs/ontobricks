# KG Analytics All-Node Series Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the selected-metric Top-N bar chart with a ranked point-and-line chart sourced from every scored node.

**Architecture:** Keep the bounded `graph_analytics.result.nodes` registry payload unchanged. Add a validated, paginated read endpoint over the existing per-version Delta metrics table, assemble and cache complete metric series in the browser, and render them through Chart.js with LTTB decimation for large graphs.

**Tech Stack:** FastAPI, Databricks SQL, Python dataclasses/services, vanilla JavaScript, Chart.js 4, pytest, Playwright.

## Global Constraints

- Accepted metrics are exactly `pagerank`, `betweenness`, `degree`, `closeness`, and `clustering`.
- Pages contain at most 25,000 rows; responses use parallel `uris`, `labels`, and `scores` arrays.
- Rows are ordered by score descending and `node_uri` ascending.
- Render every point through 5,000 nodes; above that, retain the full source and use LTTB with 2,000 rendered samples.
- Top N controls only the detail table.
- Do not widen the persisted registry payload or AI interpretation payload.
- Use `uv run --frozen` for every pytest command.

---

### Task 1: Exhaustive metric-series SQL contract

**Files:**
- Modify: `src/back/core/graph_analysis/JobMetrics.py`
- Modify: `src/back/core/graph_analysis/__init__.py`
- Test: `tests/units/core/test_job_metrics.py`

**Interfaces:**
- Produces: `metric_series_query(output_table: str, metric: str, offset: int, limit: int) -> str`
- Consumes: the five-name metric allowlist and an already-sanitized output table name.

- [ ] **Step 1: Write failing SQL tests**

Add tests asserting that `metric_series_query("cat.sch.metrics", "pagerank", 25, 10)`:

```python
sql = metric_series_query("cat.sch.metrics", "pagerank", 25, 10)
assert "pagerank AS score" in sql
assert "COUNT(*) OVER() AS total_count" in sql
assert "ORDER BY pagerank DESC, node_uri ASC" in sql
assert "LIMIT 10 OFFSET 25" in sql
```

Also assert every allowed metric succeeds and `"drop table metrics"` raises `ValidationError`.

- [ ] **Step 2: Verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/core/test_job_metrics.py -k metric_series
```

Expected: import or assertion failure because `metric_series_query` does not exist.

- [ ] **Step 3: Implement the query helper**

Add:

```python
METRIC_SERIES_COLUMNS = frozenset(
    {"pagerank", "betweenness", "degree", "closeness", "clustering"}
)

def metric_series_query(
    output_table: str, metric: str, offset: int, limit: int
) -> str:
    if metric not in METRIC_SERIES_COLUMNS:
        raise ValidationError("Unsupported graph metric")
    page_offset = max(0, int(offset))
    page_limit = min(25_000, max(1, int(limit)))
    return (
        "SELECT node_uri, label, "
        f"{metric} AS score, COUNT(*) OVER() AS total_count\n"
        f"FROM {output_table}\n"
        f"ORDER BY {metric} DESC, node_uri ASC\n"
        f"LIMIT {page_limit} OFFSET {page_offset}"
    )
```

Export the helper and allowlist from `back.core.graph_analysis`.

- [ ] **Step 4: Verify GREEN**

Run the Task 1 test command and expect all selected tests to pass.

- [ ] **Step 5: Commit**

```bash
git add src/back/core/graph_analysis/JobMetrics.py src/back/core/graph_analysis/__init__.py tests/units/core/test_job_metrics.py
git commit -m "feat(analytics): query exhaustive metric series"
```

---

### Task 2: Paginated metric-series API

**Files:**
- Modify: `src/back/objects/digitaltwin/DigitalTwin.py`
- Modify: `src/api/routers/internal/dtwin.py`
- Create: `tests/units/api/test_dtwin_metric_series.py`

**Interfaces:**
- Consumes: `metric_series_query(...)` from Task 1.
- Produces: `DigitalTwin.load_graph_metric_series(...) -> dict`.
- Produces: `GET /dtwin/metrics/series`.

- [ ] **Step 1: Write failing service and route tests**

Cover:

```python
assert payload == {
    "offset": 0,
    "total": 2,
    "next_offset": None,
    "uris": ["urn:a", "urn:b"],
    "labels": ["A", "B"],
    "scores": [0.9, 0.8],
}
```

Assert the route returns `has_result: false` before creating a Databricks client when `_load_stored_metrics` returns `None`, clamps `limit` to 25,000, carries `computed_at`, and rejects an unsupported metric with HTTP 400.

- [ ] **Step 2: Verify RED**

```bash
uv run --frozen pytest -q tests/units/api/test_dtwin_metric_series.py
```

Expected: failures because the service and route are absent.

- [ ] **Step 3: Implement the DigitalTwin reader**

Resolve host/token, SQL warehouse, Analytics output schema, and table name with the same helpers as `build_job_metrics`. Execute `metric_series_query`, then serialize parallel arrays:

```python
total = int(rows[0].get("total_count", 0) or 0) if rows else 0
next_offset = offset + len(rows)
return {
    "offset": offset,
    "total": total,
    "next_offset": next_offset if next_offset < total else None,
    "uris": [str(row.get("node_uri") or "") for row in rows],
    "labels": [str(row.get("label") or "") for row in rows],
    "scores": [float(row.get("score", 0.0) or 0.0) for row in rows],
}
```

- [ ] **Step 4: Implement the FastAPI endpoint**

Use typed query parameters:

```python
@router.get("/metrics/series")
async def get_graph_metric_series(
    metric: str = Query(...),
    offset: int = Query(0, ge=0),
    limit: int = Query(25_000, ge=1, le=25_000),
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
```

Load the stored result first, use its `graph_name` and `computed_at`, call the DigitalTwin reader, and return the page plus `success`, `has_result`, `metric`, and `computed_at`.

- [ ] **Step 5: Verify GREEN**

Run the Task 2 test command and expect all tests to pass.

- [ ] **Step 6: Commit**

```bash
git add src/back/objects/digitaltwin/DigitalTwin.py src/api/routers/internal/dtwin.py tests/units/api/test_dtwin_metric_series.py
git commit -m "feat(api): expose paginated analytics metric series"
```

---

### Task 3: Ranked point-and-line chart

**Files:**
- Modify: `src/front/static/query/js/query-analytics.js`
- Modify: `src/front/templates/partials/dtwin/_query_analytics.html`
- Modify: `src/front/static/query/css/query-analytics.css`
- Modify: `tests/units/front/test_analytics_dashboard.py`
- Modify: `tests/units/front/test_analytics_stale_result_gate.py`

**Interfaces:**
- Consumes: `GET /dtwin/metrics/series`.
- Produces: `_loadMetricSeries(metric, generation) -> Promise<Array<Point>>`.
- Produces: `_renderMetricSeriesChart(meta, points)`.

- [ ] **Step 1: Write failing frontend contracts**

Assert:

```python
ranking = _fn(js, "_renderRankingChart")
assert "type: 'line'" in js
assert "showLine: true" in js
assert "algorithm: 'lttb'" in js
assert "samples: 2000" in js
assert ".slice(0, _topN())" not in ranking
assert "/dtwin/metrics/series" in js
assert "AbortController" in js
assert "Nodes by " in js
```

Pin that `analyticsTopN` calls a table-only handler and that cache clearing occurs in `_clearAnalyticsResults`.

- [ ] **Step 2: Verify RED**

```bash
uv run --frozen pytest -q tests/units/front/test_analytics_dashboard.py tests/units/front/test_analytics_stale_result_gate.py
```

Expected: failures against the current bar-chart renderer.

- [ ] **Step 3: Implement paginated loading and generation cache**

Add:

```javascript
var _metricSeriesCache = {};
var _metricSeriesController = null;
var _analyticsGeneration = '';
var _metricSeriesRequestId = 0;
var _SERIES_PAGE_SIZE = 25000;
var _DECIMATION_THRESHOLD = 5000;
var _DECIMATION_SAMPLES = 2000;
```

Fetch sequential pages, concatenate compact arrays into points
`{x: rank, y: score, uri, label}`, report progress, generation-check each
response, cache only after completion, and abort on metric/generation changes.

- [ ] **Step 4: Replace the bar renderer**

Build a Chart.js line dataset:

```javascript
{
    type: 'line',
    data: { datasets: [{
        label: meta.label,
        data: points,
        parsing: false,
        showLine: true,
        pointRadius: points.length > 5000 ? 0 : 2,
        borderWidth: 1.5
    }]},
    options: {
        plugins: {
            decimation: {
                enabled: points.length > 5000,
                algorithm: 'lttb',
                samples: 2000
            }
        },
        scales: {
            x: {type: 'linear', title: {display: true, text: 'Rank'}},
            y: {title: {display: true, text: meta.label}}
        }
    }
}
```

Preserve approximate/unavailable notices, tooltip URI/label/score, pointer
cursor, and point click through `_navigateToGraph`.

- [ ] **Step 5: Make Top N table-only**

Expose `analyticsRenderDetailTable()` and change the template's Top N
`onchange` to call it. Keep `_renderPagerankTable` behavior unchanged.

- [ ] **Step 6: Add loading, decimation, and error states**

Use the existing brand spinner in the ranking card. Show
`Loading 25,000 / 1,350,051 nodes…` while paging and
`1,350,051 nodes · visually sampled to 2,000 points` when decimated.
On failure, retain the rest of the dashboard and call
`showNotification("Unable to load metric series", "error")`.

- [ ] **Step 7: Verify GREEN**

Run the Task 3 test command and expect all Analytics frontend tests to pass.

- [ ] **Step 8: Commit**

```bash
git add src/front/static/query/js/query-analytics.js src/front/templates/partials/dtwin/_query_analytics.html src/front/static/query/css/query-analytics.css tests/units/front/test_analytics_dashboard.py tests/units/front/test_analytics_stale_result_gate.py
git commit -m "feat(analytics): chart all node scores by rank"
```

---

### Task 4: Documentation, end-to-end verification, and delivery

**Files:**
- Modify: `docs/user-guide.md`
- Modify: `docs/features.md`
- Modify: `changelogs/v0.8.0/benoitcayladbx_2026-09-01.log`

**Interfaces:**
- Consumes: completed backend and frontend behavior from Tasks 1–3.
- Produces: verified, documented, pushed, deployable change.

- [ ] **Step 1: Update documentation**

Document the ranked all-node point-and-line chart, table-only Top N control,
progressive page loading, and visible LTTB notice for series above 5,000 nodes.

- [ ] **Step 2: Run focused tests**

```bash
uv run --frozen pytest -q tests/units/core/test_job_metrics.py tests/units/api/test_dtwin_metric_series.py tests/units/front/test_analytics_dashboard.py tests/units/front/test_analytics_stale_result_gate.py
```

Expected: all pass.

- [ ] **Step 3: Browser-test**

On `bigcustomers`, verify PageRank and Clustering each load all 1,350,051
scores, loading progress advances, title/axes/tooltips are correct, LTTB notice
is visible, metric switching aborts stale work, Top N changes only the table,
point click opens Explorer, mobile has no horizontal overflow, and the console
has no Analytics errors.

- [ ] **Step 4: Run the full suite**

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: zero failures.

- [ ] **Step 5: Append the changelog**

Use the full-suite and browser summaries in the v0.8.0 daily changelog section.

- [ ] **Step 6: Commit and push delivery files**

```bash
git add docs/user-guide.md docs/features.md changelogs/v0.8.0/benoitcayladbx_2026-09-01.log
git commit -m "docs(analytics): document exhaustive metric chart"
git push origin develop
```

- [ ] **Step 7: Resume deployment**

Re-run the clean-tree, `uv.lock`, authentication, and full-test preflight,
execute `make deploy`, start the MCP app if required, then verify `/healthz`
and Databricks app state again after at least 60 seconds.

---

### Task 5: Replace repeated pagination with one-query server sampling

**Context:** Runtime validation on `bigcustomers` measured 54 ordered queries,
approximately 137 MB transferred, and 5 minutes 29 seconds before PageRank
rendered. The user selected one exhaustive query with server-side LTTB before
deployment.

**Files:**
- Modify: `src/back/core/graph_analysis/JobMetrics.py`
- Modify: `src/back/core/graph_analysis/__init__.py`
- Modify: `src/back/objects/digitaltwin/DigitalTwin.py`
- Modify: `src/api/routers/internal/dtwin.py`
- Modify: `src/front/static/query/js/query-analytics.js`
- Modify: `tests/units/core/test_job_metrics.py`
- Modify: `tests/units/api/test_dtwin_metric_series.py`
- Modify: `tests/units/front/test_analytics_dashboard.py`
- Modify: `tests/units/front/test_analytics_stale_result_gate.py`

**Interfaces:**
- `metric_series_query(output_table, metric)` returns one deterministic ordered
  query without `COUNT(*) OVER()`, `LIMIT`, or `OFFSET`.
- `sample_metric_series(rows, threshold=5_000, sample_size=2_000)` returns
  retained rows and their original one-based ranks.
- `GET /dtwin/metrics/series?metric=<name>` returns `total`, `sampled`, `ranks`,
  `uris`, `labels`, and `scores`.

- [ ] **Step 1: Write failing backend tests**

Assert one SQL query, exact allowlist validation, all rows through 5,000, 2,000
deterministic LTTB samples above 5,000, preserved first/last points and ranks,
compact parallel arrays, and no pagination response fields.

- [ ] **Step 2: Verify backend RED**

```bash
uv run --frozen pytest -q tests/units/core/test_job_metrics.py -k metric_series tests/units/api/test_dtwin_metric_series.py
```

- [ ] **Step 3: Implement one-query sampling**

Run one ordered Databricks query, pass every returned row to the pure LTTB
sampler, and serialize only retained source points plus exhaustive `total`.
Validate the metric before creating the Databricks client.

- [ ] **Step 4: Verify backend GREEN**

Run the Step 2 command and expect all selected tests to pass.

- [ ] **Step 5: Write failing frontend tests**

Assert one series fetch without `offset`, `limit`, or `next_offset`; consume
server-provided `ranks`; retain request abort/generation/cache gates; and show
the exhaustive total plus 2,000-point sampling caption.

- [ ] **Step 6: Verify frontend RED**

```bash
uv run --frozen pytest -q tests/units/front/test_analytics_dashboard.py tests/units/front/test_analytics_stale_result_gate.py
```

- [ ] **Step 7: Implement the one-response frontend**

Build points from parallel response arrays with `x = ranks[index]`, cache only
complete responses, and render returned points directly. Keep Top N table-only,
tooltip/click behavior, logarithmic distributions, and stale-request handling.

- [ ] **Step 8: Verify frontend GREEN and full suite**

```bash
uv run --frozen pytest -q tests/units/front/test_analytics_dashboard.py tests/units/front/test_analytics_stale_result_gate.py
uv run --frozen pytest -q -m "not scenario"
```

- [ ] **Step 9: Re-run `bigcustomers` browser validation**

Confirm one HTTP 200 series request, substantially lower transfer and latency,
correct exhaustive total/sampling caption, line rendering, metric-switch abort,
cache reuse, table-only Top N, click-through, and mobile containment.
