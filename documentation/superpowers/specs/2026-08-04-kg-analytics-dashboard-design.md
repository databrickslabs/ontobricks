# KG → Analytics becomes a dashboard with global metric distributions

Date: 2026-08-04
Status: approved, ready for implementation planning

## Problem

Knowledge Graph → Analytics presents five centrality metrics as five sibling
tabs, each holding exactly one chart: a horizontal bar chart of the top N nodes.
Two things are wrong with that.

**It only ever shows the head.** The app reads back a bounded top-N union from
the job's per-node table (`Settings.analytics_top_n`, default 100), so a user
looking at a 12,000-node graph sees the 10 loudest nodes and has no way to ask
whether those 10 are outliers or typical. "Is a PageRank of 0.004 high?" is
unanswerable from the current page.

**Tabs hide the comparison that matters.** The five metrics are most useful
read against each other — a node with high betweenness *and* low clustering is
a bridge between communities, which is a statement about two metrics at once.
Tabs make that a memory exercise.

The per-node scores for the whole graph already exist in the job's output Delta
table. Nothing needs recomputing; the app simply never asks for a summary of
them.

## Decisions

Taken with the user during brainstorming, recorded here because each one closes
off alternatives an implementer would otherwise reopen.

1. **The distribution is global, over every scored node** — not over the top-N
   slice already in the browser. A histogram of the top 100 is a histogram of
   the head, which is the very thing the page already over-reports.
2. **Layout: a distribution strip driving one focused ranking chart.** The five
   metric tabs are removed. Five small-multiple histograms sit across the top,
   always all visible; selecting one drives a single full-width ranking chart
   below. Rejected: five full-width stacked metric cards (five screens of
   scrolling) and a two-up grid (node labels get cramped at half width, which is
   what the ranking chart is for).
3. **Binning: 20 equal-width bins, with a log-scale count axis toggle.** One
   rule for all five metrics, and an X axis that reads directly as metric
   values. Rejected: a linear count axis alone (PageRank, betweenness and
   clustering collapse into a single bar) and log-spaced bins (zero-valued
   nodes have no logarithm, so betweenness and clustering would need a bolted-on
   `= 0` bucket — and on a real KG that bucket *is* the story).
4. **The histogram is a read-back aggregate, not a job change.** No new Delta
   table, no new SQL in `graph_analytics_job.py`, no touching the NetworkX
   oracle tests.
5. **Median and p90 are derived in Python** from the cumulative bin counts
   rather than asked of SQL. Labelled `median ≈` in the UI.
6. **The partial is split into template + CSS + JS** as part of this work
   (see §5). Not optional cleanup: the layout change is a rewrite of the markup
   and most of the chart JS, and doing it in place would grow a file that
   already violates two structural rules.

## Architecture

One new query in the read-back, one new field on the result payload, and a
rebuilt front-end section. No layer moves; no schema migration.

```
Lakeflow job (unchanged)
  └── <out>  one row per scored node          ← already written today
                    ↓
JobMetrics read-back (DatabricksClient.execute_query, SQL warehouse)
  ├── summary_query          (unchanged)
  ├── top_nodes_query        (unchanged — still the bounded top-N union)
  ├── type_profiles_query    (unchanged)
  ├── type_predicates_query  (unchanged)
  ├── distribution_bounds_query  ← NEW: min/max/mean per metric
  └── distributions_query        ← NEW: 20 bin counts per metric
                    ↓
MetricsResult.distributions → to_dict() → graph_analytics.result (jsonb)
                    ↓
GET /dtwin/metrics/latest → Dashboard tab (5 histogram tiles + 1 ranking chart)
```

The distribution is computed in the **read-back** rather than in the job for
three reasons: the job's SQL is verified statement-by-statement against a
NetworkX oracle in `tests/units/core/test_graph_analytics_job_sql.py` and every
addition there costs that verification; the aggregate is a single cheap pass
over a table the warehouse already has; and a histogram is a presentation
concern that should not become a fifth job output table.

## Components

### 1. Distribution read-back SQL — `back/core/graph_analysis/JobMetrics.py`

Two new module-level query builders alongside the four existing ones:

- `distribution_bounds_query(output_table)` — one row carrying `MIN`, `MAX` and
  `AVG` for each of the five metrics.
- `distributions_query(output_table, bins)` — one row per
  `(metric, bin_index)` with a node count.

Two statements rather than one: combining them forces the bounds into either a
repeated `CASE` expression in the `GROUP BY` or a constant-ordinal `GROUP BY`,
and the portable spelling of each differs across the three engines below. Two
plain aggregates are individually obvious and individually testable, and the
extra warehouse round trip is irrelevant next to a job that takes minutes.

`bins` is a parameter of the builder so the SQLite tests can exercise a small bin
count, but it has exactly one production call site and a module-level default of
20. It is deliberately **not** plumbed through to `Settings` or the UI.

**The SQL must be portable across SQLite, Spark and Postgres.**
`tests/units/core/test_job_metrics.py` executes the read-back SQL for real
against an in-memory SQLite database *and* asserts every read-back statement
parses under all three sqlglot dialects. That rules out `width_bucket`,
`percentile_approx`, `histogram_numeric` and lateral `UNNEST`.

It also rules out `least` / `greatest`: the *job* test harness registers those as
custom SQLite functions, but the read-back harness (`_OutputDB`) does not, and
adding them there would weaken the portability guarantee this query needs. The
clamp is therefore written as a `CASE` expression, which every engine has.

Bucket assignment per metric, grouped inside a subquery so the `GROUP BY` targets
a plain column name:

```sql
CASE
  WHEN b.hi <= b.lo    THEN 0
  WHEN t.<m> >= b.hi   THEN <bins - 1>
  ELSE CAST((t.<m> - b.lo) * <bins> / (b.hi - b.lo) AS INTEGER)
END AS bin_index
```

The three branches are the two edge cases plus the general one, and all three are
common rather than exotic:

- **`hi == lo`** — every node scores identically, which happens on a fully
  regular graph and on any metric that came back all-zero. The general
  expression would divide by zero, so this collapses to a single populated bin.
- **A node at exactly `hi`** would compute bin index `bins`, one past the last.
- The general branch relies on every metric being **non-negative**, which makes
  `CAST(... AS INTEGER)` truncation equivalent to `floor`. That is true of all
  five metrics by construction and is why no `floor` function is needed.

Bins with no nodes must be present in the payload as `0`, not absent — the front
end indexes bins positionally.

### 2. `MetricDistribution` — `back/core/graph_analysis/models.py`

A new dataclass beside `NodeMetrics` and `EntityTypeProfile`, following the same
`to_dict()` convention:

| Field | Meaning |
|---|---|
| `bins: List[int]` | node counts, always `len == bin_count`, index 0 = lowest |
| `bin_count: int` | echoed so a reader never infers it from `len(bins)` |
| `lo`, `hi: float` | the range the bins span |
| `mean: float` | from SQL |
| `median`, `p90: float` | bin-interpolated in Python (§3) |

`MetricsResult` gains `distributions: Dict[str, MetricDistribution]`, keyed by
the same metric names as `NODE_METRIC_KEYS`, and carries it through `to_dict()`.

The docstring must state that `bins` covers **every scored node** while `nodes`
is a bounded top-N slice. Those two fields describe different populations, and
`MetricsResult` already carries a warning against deriving counts from
`len(nodes)` — this is the same trap one field over.

### 3. `_read_distributions` — `JobMetrics`

A new private reader called from `compute()` after `_read_summary()`, since it
depends on the unavailable-metric decision the summary makes.

**Metrics in `unavailable_metrics` get no entry at all.** Betweenness and
closeness are written as zeros when the pivot BFS hit its depth cap, and a
histogram of fabricated zeros is exactly the misreading the existing UI goes out
of its way to prevent (`_setPagerankTableNote`, the per-chart zero notices).
Absence is the honest encoding, and the front end already has copy explaining
why the metric is missing.

Metrics in `approximate_metrics` **do** get a distribution, badged `≈` in the UI
like the ranking chart and detail table already are.

Median and p90 are derived from the cumulative bin counts by linear
interpolation within the containing bin. This is an estimate bounded by the bin
width, which is why the UI labels it `median ≈`. A helper is worth extracting so
it can be unit-tested against an exact oracle independently of any SQL.

### 4. Front end — the Dashboard tab

The tab strip goes from seven tabs to three: **Dashboard · Data Model Health ·
AI Insights**. The five metric tab triggers and panes are deleted; the Health
and AI Insights panes keep their current content and behaviour verbatim.

The Dashboard pane scrolls, with four stacked blocks:

**a. KPI row.** The six existing aggregates (Nodes, Edges, Components, Avg
Degree, Density, Computed in), rebuilt on the shared **`.ob-kpi-tile`** component
instead of the hand-rolled `card text-center h-100 border-0 bg-light` markup used
today. The tile component exists precisely for this and the current markup
duplicates it.

**b. Distribution strip.** Five tiles in a responsive grid, one per metric, each
with:

- the metric name in its existing colour
- a compact Chart.js bar chart of `bins`
- a caption: `median ≈ <v>` and `max <v>`
- an `≈ estimate` badge when the metric is in `approximate_metrics`

Selecting a tile makes it the ranking chart's metric. The selected tile carries a
visible selected state; selection is keyboard-reachable, so the tiles are
`<button>`s rather than click-handled `<div>`s. **PageRank is selected on load**,
matching which tab was active before this change. Selection is per-visit state —
not persisted to `localStorage` or the URL.

The `?` help button that opens `_showMetricInfo` lives on the **ranking card
header** (§4c), not on the tile: a tile is itself a `<button>`, and nesting a
button inside one is invalid HTML that browsers resolve unpredictably. Since the
selected tile and the ranking card always name the same metric, one help affordance
covers both. The `_METRIC_INFO` copy behind the modal is good and stays unchanged.

A metric with **no** distribution entry (unavailable, or a legacy stored payload)
renders the tile as an explanatory empty state, never as an empty chart frame.

**c. Ranking card.** One full-width horizontal bar chart of the top N for the
selected metric — today's chart, unchanged in behaviour: click-through to the
Graph Viewer via `_navigateToGraph`, the `Top N` input, the all-zero notice, and
the sampled-estimate notice. Its header carries a segmented control mirroring
the strip selection, so the metric can be changed from either place.

**d. Node detail table.** Today's PageRank-tab table (all five metrics as
columns, `≈` and `—` markers, row click-through), moved to the bottom of the
Dashboard pane unchanged.

**Log-scale toggle.** One control in the section header, applying to all five
tiles at once. It switches the Chart.js `y` scale between `linear` and
`logarithmic`, and the tile caption must state which is active — bar heights stop
being proportional to counts under a log axis, and an unlabelled log chart
misleads. Empty bins simply render no bar under a log scale. It defaults to
**linear** and, like tile selection, is per-visit state rather than a persisted
preference.

### 5. Splitting `partials/dtwin/_query_analytics.html`

The file is ~1450 lines: markup, an inline `<style>` block, and a ~1100-line
inline `<script>`. Both inline blocks are forbidden by
`.cursor/11-frontend-design.mdc`, and every sibling section in this area is
already split. The new layout rewrites most of the markup and chart JS anyway.

| Goes to | Contents |
|---|---|
| `partials/dtwin/_query_analytics.html` | markup only |
| `static/query/css/query-analytics.css` | the current inline `<style>`, plus tile/strip styles |
| `static/query/js/query-analytics.js` | the current inline `<script>` |

The area is `query` — `templates/partials/dtwin/` pairs with
`static/query/`, as `query-sync.js`, `query-cohorts.js` and the rest already do.
Wiring goes in `dtwin.html`'s `extra_css` / `extra_js` blocks with
`?v={{ asset_version }}`, after the existing Chart.js CDN tag.

The metric-explanation modal (`#analyticsMetricModal`) stays in the partial.

Scope discipline: this moves code and changes what the new layout requires.
The AI Insights rendering, the audit-trail POST, `analyticsResume`, the
task-resumption logic and the job-availability banners are **relocated, not
redesigned**. The globals the rest of the page calls
(`window.analyticsLoadTypes`, `analyticsCompute`, `analyticsRenderCharts`,
`analyticsResume`, `_analyticsDrillURI`, `_showMetricInfo`) keep their names and
signatures — `query.js` and `query-sigmagraph.js` call across this boundary.

### 6. Backward compatibility

Results cached before this change have no `distributions` key. The front end
must treat the key as optional and render each tile's empty state with a
"re-run the analysis to see distributions" message. It must not throw, and it
must not block the ranking chart or the detail table, both of which render fine
from a legacy payload.

No migration and no cache invalidation: `graph_analytics.result` is `jsonb`, and
a stored row without distributions is still a completely valid result for
everything else on the page.

## Error handling

A failure to read the distributions must not fail the run. `_read_distributions`
catches and logs, leaving `distributions` empty — the same posture
`_read_summary` takes when the summary table is missing. An analysis that
computed every node metric successfully must not be discarded because a
presentation aggregate failed; the front end already has the empty state for it
from §6.

The all-identical and all-zero cases from §1 are **not** errors and must not be
logged as such. A single populated bin is the correct rendering of a graph where
every node scores the same, and the UI's existing zero-value notices already
explain the all-zero case.

## Testing

**Read-back SQL against SQLite** — extending the existing harness in
`tests/units/core/test_job_metrics.py`, which already runs read-back SQL against
a real SQLite database:

- a known skewed distribution produces the expected bin counts, checked against
  a hand-computed oracle
- every node scoring identically (`hi == lo`) yields one populated bin and no
  division-by-zero
- the node at exactly `hi` lands in the last bin, not one past it
- empty bins are present as `0`
- `len(bins) == bin_count` for all five metrics

**Assembly** — `JobMetrics`:

- a metric in `unavailable_metrics` gets **no** distribution entry
- a metric in `approximate_metrics` **does** get one
- bin-interpolated median and p90 match an exact oracle within one bin width
- a raised exception in the distributions read leaves `distributions` empty and
  still returns a complete `MetricsResult`

**Payload** — `MetricsResult.to_dict()` round-trips `distributions`, and the
existing `graph_analytics` store tests still pass with the widened payload.

**Template** — extending `tests/units/front/`:

- the five metric tab triggers and panes are gone; `Dashboard`,
  `Data Model Health` and `AI Insights` are present
- the partial contains no inline `<style>` or `<script>` block
- `dtwin.html` loads `query-analytics.css` and `query-analytics.js` with
  `asset_version` cache-busting
- the KPI row uses `.ob-kpi-tile`
- the existing `test_analytics_unavailable_metrics.py` expectations still hold
  against the new markup

## Out of scope

- Changing `graph_analytics_job.py`, its output tables, or its NetworkX oracle
  tests
- Making `analytics_top_n` or the bin count user-configurable — 20 bins is fixed
- Drilling from a histogram bin into the nodes it contains: those nodes are not
  in the bounded payload, so it would need a new per-bin query
- Per-entity-type distributions. The entity-type filter already narrows the
  whole run, so the strip reflects it; a type breakdown within one run is a
  different feature
- Distribution history or run-to-run comparison
- Redesigning the AI Insights or Data Model Health panes
- Feeding the distributions to the `agent_graph_interpreter` prompt. Worth doing
  later, but it is a prompt change and would trip the
  `.cursor/12-ai-feature-lifecycle.mdc` eval gate, which this UI work should not
  drag in
