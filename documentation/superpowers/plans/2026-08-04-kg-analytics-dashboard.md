# KG Analytics Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the five per-metric tabs on Knowledge Graph → Analytics with a single scrolling dashboard that shows a global distribution histogram for all five metrics at once, driving one focused top-N ranking chart.

**Architecture:** The Lakeflow job already writes one row per scored node to its output Delta table; the app only ever reads back a bounded top-N union. Two new read-back aggregates summarise the full node population into 20 histogram bins per metric, which ride along in the existing `graph_analytics.result` jsonb column. The front-end section is rebuilt and, in the process, split out of its 1450-line inline-everything partial into the template + CSS + JS layout every sibling section already uses.

**Tech Stack:** Python 3.11, FastAPI, Jinja2, Bootstrap 5.3.2, Bootstrap Icons, Chart.js 4 (CDN), Databricks SQL warehouse via `DatabricksClient.execute_query`, pytest, SQLite + sqlglot for read-back SQL verification, `uv` for dependency and test running.

**Spec:** `documentation/superpowers/specs/2026-08-04-kg-analytics-dashboard-design.md`

## Global Constraints

Every task's requirements implicitly include this section.

- **Run tests with `uv run --frozen pytest -q -m "not scenario"`.** The `--frozen` flag is mandatory — a bare `uv run` poisons `uv.lock` and breaks the next deploy. Never run the `tests/e2e/scenarios/` suites unless explicitly asked.
- **Read-back SQL must be portable across SQLite, Spark and Postgres.** No `width_bucket`, `percentile_approx`, `histogram_numeric`, lateral `UNNEST`, `least` or `greatest`. The existing `test_read_back_sql_parses_in_both_dialects` test asserts every read-back statement parses under all three sqlglot dialects, and every new statement must be added to it.
- **No inline `<style>` or `<script>` blocks in templates.** CSS goes in `src/front/static/<area>/css/`, JS in `src/front/static/<area>/js/`. The area for `templates/partials/dtwin/` is `query` (see `query-sync.js`, `query-cohorts.js`).
- **Always cache-bust assets** with `?v={{ asset_version }}`.
- **Use design tokens, never hard-coded colours.** `--db-*` everywhere under `static/**` except `static/global/ontoviz/**`, which uses `--ovz-*`. Never mix the two in one selector.
- **Tab strips use `class="nav nav-tabs ob-tabs"`.** Do not re-apply `px-3`, `pt-2`, `bg-white`, `font-size`, or `border-top-0 rounded-top-0` on the strip or the card after it — all baked into `ob-tabs`.
- **Never call `alert()`, `confirm()` or `prompt()`.** User-facing messages go through `showNotification(message, type)` from `global/js/utils.js`.
- **Reuse `.ob-kpi-tile`** for KPI tiles (`.ob-kpi-tile-icon`, `.ob-kpi-tile-value`, `.ob-kpi-tile-label`). Do not hand-roll `card text-center bg-light`.
- **Icon-only buttons must carry `title=`.** Every tab button starts with a Bootstrap icon.
- **Metric names are exactly** `degree`, `pagerank`, `betweenness`, `closeness`, `clustering` — matching `NODE_METRIC_KEYS` in `models.py`. Never rename or reorder them.
- **The bin count is fixed at 20** (`DEFAULT_DISTRIBUTION_BINS`). Not exposed to `Settings` or the UI.
- **After all code changes**, add a changelog entry at `changelogs/v0.7.0/benoitcayladbx_2026-08-04.log` (version `0.7.0` from `pyproject.toml`). If the file exists, append a section.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `src/back/core/graph_analysis/models.py` | Add `MetricDistribution`, `MetricsResult.distributions` | 1 |
| `src/back/core/graph_analysis/JobMetrics.py` | Add `distribution_bounds_query`, `distributions_query`, `interpolate_quantile`, `_read_distributions` | 2, 3, 4 |
| `tests/units/core/test_job_metrics.py` | Extend: read-back SQL against SQLite, dialect parse, assembly | 2, 3, 4 |
| `src/front/static/query/css/query-analytics.css` | **New.** All Analytics section styles | 5, 7 |
| `src/front/static/query/js/query-analytics.js` | **New.** All Analytics section JS | 5–10 |
| `src/front/templates/partials/dtwin/_query_analytics.html` | Markup only | 5–9 |
| `src/front/templates/dtwin.html` | Wire the two new assets | 5 |
| `tests/units/front/test_analytics_unavailable_metrics.py` | Repoint JS assertions to the new JS file | 5 |
| `tests/units/dtwin/test_analytics_job_status.py` | Repoint JS assertions to the new JS file | 5 |
| `tests/units/front/test_runs_page.py` | Repoint two helper-survival assertions | 5 |
| `tests/units/front/test_analytics_dashboard.py` | **New.** Dashboard layout assertions | 6–10 |
| `changelogs/v0.7.0/benoitcayladbx_2026-08-04.log` | Changelog | 11 |

---

## Task 1: `MetricDistribution` model

**Files:**
- Modify: `src/back/core/graph_analysis/models.py` (add after `MetricsStats`, ~line 111; extend `MetricsResult` ~line 149-197)
- Test: `tests/units/core/test_job_metrics.py`

**Interfaces:**
- Consumes: nothing (first task)
- Produces:
  - `MetricDistribution(bins: List[int], bin_count: int, lo: float, hi: float, mean: float, median: float, p90: float)` with `.to_dict() -> Dict[str, Any]`
  - `MetricsResult.distributions: Dict[str, MetricDistribution]`, keyed by the `NODE_METRIC_KEYS` names
  - `MetricsResult.to_dict()` emits a `"distributions"` key

- [ ] **Step 1: Write the failing test**

Append to `tests/units/core/test_job_metrics.py`:

```python
# ---------------------------------------------------------------------------
# MetricDistribution model
# ---------------------------------------------------------------------------


def _distribution(**overrides):
    from back.core.graph_analysis.models import MetricDistribution

    base = dict(
        bins=[4, 2, 1, 0], bin_count=4,
        lo=0.0, hi=1.0, mean=0.25, median=0.125, p90=0.55,
    )
    base.update(overrides)
    return MetricDistribution(**base)


def test_distribution_to_dict_round_trips_every_field():
    d = _distribution().to_dict()
    assert d["bins"] == [4, 2, 1, 0]
    assert d["bin_count"] == 4
    assert d["lo"] == 0.0
    assert d["hi"] == 1.0
    assert d["mean"] == 0.25
    assert d["median"] == 0.125
    assert d["p90"] == 0.55


def test_distribution_to_dict_copies_the_bin_list():
    """A shared list would let a caller mutate the stored payload."""
    d = _distribution()
    payload = d.to_dict()
    payload["bins"].append(99)
    assert d.bins == [4, 2, 1, 0]


def test_metrics_result_defaults_to_no_distributions():
    """Absent, not empty-per-metric: the UI distinguishes the two."""
    assert MetricsResult().distributions == {}
    assert MetricsResult().to_dict()["distributions"] == {}


def test_metrics_result_to_dict_serialises_distributions():
    result = MetricsResult()
    result.distributions["pagerank"] = _distribution()
    payload = result.to_dict()
    assert payload["distributions"]["pagerank"]["bins"] == [4, 2, 1, 0]
    assert payload["distributions"]["pagerank"]["bin_count"] == 4
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/units/core/test_job_metrics.py -q -k distribution`
Expected: FAIL with `ImportError: cannot import name 'MetricDistribution'`

- [ ] **Step 3: Write minimal implementation**

In `src/back/core/graph_analysis/models.py`, add after the `MetricsStats` class:

```python
#: Histogram bins per metric in a distribution. Fixed: not a Settings value.
DEFAULT_DISTRIBUTION_BINS = 20


@dataclass
class MetricDistribution:
    """The distribution of one metric across **every** scored node.

    This is the counterpart to :class:`NodeMetrics`, which describes a single
    node, and it deliberately covers a different population than
    ``MetricsResult.nodes``: ``nodes`` is a bounded top-N slice, while ``bins``
    summarises the whole graph. Do not derive one from the other.

    ``bins[i]`` is the number of nodes whose score falls in the *i*-th of
    ``bin_count`` equal-width buckets spanning ``[lo, hi]``. Empty buckets are
    present as ``0`` so the front end can index positionally.

    ``median`` and ``p90`` are interpolated from the bin counts rather than
    computed exactly — the read-back must run on engines without a percentile
    function. They are accurate to within one bin width, which is why the UI
    renders them as "median ~".
    """

    bins: List[int] = field(default_factory=list)
    bin_count: int = 0
    lo: float = 0.0
    hi: float = 0.0
    mean: float = 0.0
    median: float = 0.0
    p90: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            # Copied: a shared list would let a caller mutate the payload that
            # is about to be persisted.
            "bins": list(self.bins),
            "bin_count": self.bin_count,
            "lo": self.lo,
            "hi": self.hi,
            "mean": self.mean,
            "median": self.median,
            "p90": self.p90,
        }
```

Then in `MetricsResult`, add the field after `entity_type_profiles`:

```python
    #: metric name -> distribution over every scored node. Absent for metrics
    #: in ``unavailable_metrics``: those are stored as zeros, and a histogram of
    #: fabricated zeros would read as a real measurement.
    distributions: Dict[str, "MetricDistribution"] = field(default_factory=dict)
```

and in its `to_dict()`, after the `entity_type_profiles` entry:

```python
            "distributions": {
                k: v.to_dict() for k, v in self.distributions.items()
            },
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --frozen pytest tests/units/core/test_job_metrics.py -q -k distribution`
Expected: PASS (4 passed)

- [ ] **Step 5: Run the full unit suite for regressions**

Run: `uv run --frozen pytest -q -m "not scenario"`
Expected: PASS — the new key is additive, so nothing should break.

- [ ] **Step 6: Commit**

```bash
git add src/back/core/graph_analysis/models.py tests/units/core/test_job_metrics.py
git commit -m "Add MetricDistribution to the graph metrics payload

Carries the distribution of a metric across every scored node, which the
top-N node slice cannot express."
```

---

## Task 2: Distribution read-back SQL

**Files:**
- Modify: `src/back/core/graph_analysis/JobMetrics.py` (add builders after `type_predicates_query`, ~line 140)
- Test: `tests/units/core/test_job_metrics.py`

**Interfaces:**
- Consumes: `DEFAULT_DISTRIBUTION_BINS` from Task 1
- Produces:
  - `distribution_bounds_query(output_table: str) -> str` — one row with `lo_<m>`, `hi_<m>`, `mean_<m>` for each of the five metrics
  - `distributions_query(output_table: str, bins: int = DEFAULT_DISTRIBUTION_BINS) -> str` — rows of `(metric, bin_index, node_count)`

- [ ] **Step 1: Write the failing test**

Add to `tests/units/core/test_job_metrics.py`, inside `class TestReadBackSql`:

```python
    def test_bounds_query_returns_min_max_mean_per_metric(self):
        rows = self._db().query(distribution_bounds_query("metrics"))
        assert len(rows) == 1
        row = rows[0]
        # pagerank across _sample_rows is 0.05 .. 0.50
        assert row["lo_pagerank"] == pytest.approx(0.05)
        assert row["hi_pagerank"] == pytest.approx(0.50)
        assert row["mean_pagerank"] == pytest.approx(0.20)
        # every metric must be present, even the all-zero ones
        for m in ("degree", "pagerank", "betweenness", "closeness", "clustering"):
            assert f"lo_{m}" in row
            assert f"hi_{m}" in row
            assert f"mean_{m}" in row

    def test_distributions_query_buckets_every_node_exactly_once(self):
        rows = self._db().query(distributions_query("metrics", 4))
        by_metric = {}
        for r in rows:
            by_metric.setdefault(r["metric"], 0)
            by_metric[r["metric"]] += r["node_count"]
        # 5 nodes, 5 metrics, no node counted twice or dropped
        assert by_metric == {m: 5 for m in
                             ("degree", "pagerank", "betweenness",
                              "closeness", "clustering")}

    def test_distributions_query_assigns_the_expected_bins(self):
        """degree is 0.1..0.9; with 4 bins over that range width is 0.2."""
        rows = [r for r in self._db().query(distributions_query("metrics", 4))
                if r["metric"] == "degree"]
        counts = {r["bin_index"]: r["node_count"] for r in rows}
        # 0.1 -> bin 0 | 0.3 -> bin 1 | 0.5 -> bin 2 | 0.7 -> bin 3
        # 0.9 == hi   -> clamped into bin 3
        assert counts == {0: 1, 1: 1, 2: 1, 3: 2}

    def test_top_value_is_clamped_into_the_last_bin_not_past_it(self):
        rows = [r for r in self._db().query(distributions_query("metrics", 4))
                if r["metric"] == "degree"]
        assert max(r["bin_index"] for r in rows) == 3

    def test_identical_scores_collapse_into_one_bin_without_dividing_by_zero(self):
        """A fully regular graph, or an all-zero metric. hi == lo."""
        db = _OutputDB([
            {"node_uri": f"{NS}{c}", "degree": 0.5} for c in "ABCD"
        ])
        rows = [r for r in db.query(distributions_query("metrics", 4))
                if r["metric"] == "degree"]
        assert rows == [{"metric": "degree", "bin_index": 0, "node_count": 4}]

    def test_all_zero_metric_collapses_into_one_bin(self):
        """betweenness is 0.0 for every node in _sample_rows."""
        rows = [r for r in self._db().query(distributions_query("metrics", 4))
                if r["metric"] == "betweenness"]
        assert rows == [{"metric": "betweenness", "bin_index": 0, "node_count": 5}]
```

Extend the existing dialect-parse test in the same class to cover both new statements:

```python
    @pytest.mark.parametrize("dialect", ["databricks", "spark", "postgres"])
    def test_read_back_sql_parses_in_both_dialects(self, dialect):
        sqlglot = pytest.importorskip("sqlglot")
        for sql in (
            summary_query("cat.sch.metrics"),
            top_nodes_query("cat.sch.metrics", 50),
            type_profiles_query("cat.sch.metrics"),
            type_predicates_query("cat.sch.metrics"),
            distribution_bounds_query("cat.sch.metrics"),
            distributions_query("cat.sch.metrics", 20),
        ):
            try:
                sqlglot.parse_one(sql, dialect=dialect)
            except Exception as exc:  # noqa: BLE001
                pytest.fail(f"{dialect} rejected:\n{sql}\n{exc}")
```

And add both names to the import block at the top of the file:

```python
from back.core.graph_analysis.JobMetrics import (
    APPROXIMATE_METRICS,
    UNAVAILABLE_METRICS,
    JobMetrics,
    distribution_bounds_query,
    distributions_query,
    resolve_analytics_source,
    summary_query,
    top_nodes_query,
    type_predicates_query,
    type_profiles_query,
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/units/core/test_job_metrics.py -q -k "bounds or distributions or clamped or identical or all_zero"`
Expected: FAIL with `ImportError: cannot import name 'distribution_bounds_query'`

- [ ] **Step 3: Write minimal implementation**

In `src/back/core/graph_analysis/JobMetrics.py`, add after `type_predicates_query`:

```python
def distribution_bounds_query(output_table: str) -> str:
    """Per-metric ``MIN`` / ``MAX`` / ``AVG`` over every scored node.

    Split from :func:`distributions_query` rather than folded into it: combining
    them forces the bounds into either a repeated ``CASE`` expression in the
    ``GROUP BY`` or a constant-ordinal ``GROUP BY``, and the portable spelling of
    each differs across SQLite, Spark and Postgres. Two plain aggregates are
    individually obvious, and the extra round trip is nothing next to a job that
    runs for minutes.
    """
    columns = ",\n".join(
        f"       MIN({m}) AS lo_{m},\n"
        f"       MAX({m}) AS hi_{m},\n"
        f"       AVG({m}) AS mean_{m}"
        for m in NODE_METRIC_KEYS
    )
    return f"SELECT\n{columns}\nFROM {output_table}"


def _bin_index_expression(metric: str, bins: int) -> str:
    """Portable bucket assignment for one metric.

    Written as a ``CASE`` rather than with ``least`` / ``greatest``: the *job*
    test harness registers those as custom SQLite functions, but this read-back
    is verified against a plain SQLite connection and under three sqlglot
    dialects, so it must stick to constructs every engine has natively.

    The three branches are the two degenerate cases plus the general one:

    * ``hi <= lo`` — every node scores identically (a fully regular graph, or a
      metric the run left all-zero). The general expression would divide by
      zero, so everything collapses into bin 0.
    * ``value >= hi`` — the top-scoring node would otherwise compute ``bins``,
      one index past the last bucket.
    * otherwise, scale into ``[0, bins)``. Every metric is non-negative by
      construction, which is what makes ``CAST(... AS INTEGER)`` truncation
      equivalent to ``floor`` and saves needing a ``floor`` function.
    """
    return (
        f"CASE\n"
        f"      WHEN b.hi <= b.lo THEN 0\n"
        f"      WHEN t.{metric} >= b.hi THEN {int(bins) - 1}\n"
        f"      ELSE CAST((t.{metric} - b.lo) * {int(bins)}"
        f" / (b.hi - b.lo) AS INTEGER)\n"
        f"    END"
    )


def distributions_query(
    output_table: str, bins: int = DEFAULT_DISTRIBUTION_BINS
) -> str:
    """Node counts per histogram bin, for all five metrics.

    One ``SELECT`` per metric, stacked with ``UNION ALL``. Each groups inside a
    subquery so the ``GROUP BY`` targets a plain column name — grouping directly
    on the ``CASE`` expression or on a constant ordinal is spelled differently
    across the three engines this has to satisfy.

    Bins containing no nodes produce no row; the caller pads them to zero.
    """
    bins = max(1, int(bins))
    parts = [
        f"SELECT '{metric}' AS metric, bin_index, COUNT(*) AS node_count\n"
        f"FROM (\n"
        f"  SELECT {_bin_index_expression(metric, bins)} AS bin_index\n"
        f"  FROM {output_table} t\n"
        f"  CROSS JOIN (\n"
        f"    SELECT MIN({metric}) AS lo, MAX({metric}) AS hi"
        f" FROM {output_table}\n"
        f"  ) b\n"
        f") q\n"
        f"GROUP BY bin_index"
        for metric in NODE_METRIC_KEYS
    ]
    return "\nUNION ALL\n".join(parts)
```

Add the two imports to the existing `models` import block at the top of `JobMetrics.py`:

```python
from back.core.graph_analysis.models import (
    DEFAULT_DISTRIBUTION_BINS,
    MODE_JOB,
    NODE_METRIC_KEYS,
    EntityTypeProfile,
    MetricDistribution,
    MetricsRequest,
    MetricsResult,
    NodeMetrics,
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --frozen pytest tests/units/core/test_job_metrics.py -q`
Expected: PASS — all read-back tests including the three dialects.

- [ ] **Step 5: Commit**

```bash
git add src/back/core/graph_analysis/JobMetrics.py tests/units/core/test_job_metrics.py
git commit -m "Add the distribution read-back queries

Summarises every scored node into fixed-width histogram bins, which the
bounded top-N read cannot do. Portable across SQLite, Spark and Postgres so
the existing read-back verification still covers it."
```

---

## Task 3: Quantile interpolation

**Files:**
- Modify: `src/back/core/graph_analysis/JobMetrics.py`
- Test: `tests/units/core/test_job_metrics.py`

**Interfaces:**
- Consumes: nothing from earlier tasks
- Produces: `interpolate_quantile(bins: List[int], lo: float, hi: float, q: float) -> float`

- [ ] **Step 1: Write the failing test**

Add a new test class to `tests/units/core/test_job_metrics.py`:

```python
# ---------------------------------------------------------------------------
# Quantiles interpolated from bin counts
# ---------------------------------------------------------------------------


class TestInterpolateQuantile:
    """Exact oracle: a uniform distribution has analytically known quantiles.

    Four bins of two nodes each spanning 0..4 is a uniform distribution whose
    median is exactly 2.0 and whose p90 is exactly 3.6, so the interpolation can
    be checked against a real number rather than against itself.
    """

    UNIFORM = [2, 2, 2, 2]

    def test_median_of_a_uniform_distribution_is_exact(self):
        assert interpolate_quantile(self.UNIFORM, 0.0, 4.0, 0.5) == pytest.approx(2.0)

    def test_p90_of_a_uniform_distribution_is_exact(self):
        assert interpolate_quantile(self.UNIFORM, 0.0, 4.0, 0.9) == pytest.approx(3.6)

    def test_all_mass_in_the_first_bin_lands_inside_that_bin(self):
        """The heavy-tail case: the median must not escape bin 0."""
        v = interpolate_quantile([10, 0, 0, 0], 0.0, 4.0, 0.5)
        assert 0.0 <= v <= 1.0

    def test_all_mass_in_the_last_bin_lands_inside_that_bin(self):
        v = interpolate_quantile([0, 0, 0, 10], 0.0, 4.0, 0.5)
        assert 3.0 <= v <= 4.0

    def test_degenerate_range_returns_the_single_value(self):
        assert interpolate_quantile([4], 0.5, 0.5, 0.5) == pytest.approx(0.5)

    def test_empty_distribution_returns_the_low_bound(self):
        assert interpolate_quantile([], 0.0, 1.0, 0.5) == pytest.approx(0.0)

    def test_zero_total_returns_the_low_bound(self):
        assert interpolate_quantile([0, 0], 0.0, 1.0, 0.5) == pytest.approx(0.0)

    def test_result_never_leaves_the_range(self):
        for q in (0.0, 0.25, 0.5, 0.9, 1.0):
            v = interpolate_quantile([1, 5, 1], 2.0, 8.0, q)
            assert 2.0 <= v <= 8.0
```

Add `interpolate_quantile` to the `JobMetrics` import block at the top of the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/units/core/test_job_metrics.py -q -k InterpolateQuantile`
Expected: FAIL with `ImportError: cannot import name 'interpolate_quantile'`

- [ ] **Step 3: Write minimal implementation**

In `src/back/core/graph_analysis/JobMetrics.py`, add after `distributions_query`:

```python
def interpolate_quantile(
    bins: List[int], lo: float, hi: float, q: float
) -> float:
    """Estimate the *q*-th quantile from histogram bin counts.

    The read-back has to run on engines without a percentile function (see
    :func:`distribution_bounds_query`), so the quantile is recovered from the
    bins by assuming each bucket's mass is spread evenly across its width. The
    error is therefore bounded by one bin width, which is why the UI presents
    the result as an approximation rather than as a measured median.

    Returns *lo* for an empty or zero-total distribution, and for the degenerate
    ``hi == lo`` case where every node scored the same.
    """
    total = sum(bins)
    if not bins or total <= 0 or hi <= lo:
        return float(lo)

    width = (hi - lo) / len(bins)
    target = total * q
    cumulative = 0
    for index, count in enumerate(bins):
        if count <= 0:
            continue
        if cumulative + count >= target:
            # Clamped because q == 0 puts the target at or below the first
            # bucket's left edge, and q == 1 at its right edge.
            fraction = min(1.0, max(0.0, (target - cumulative) / count))
            return lo + (index + fraction) * width
        cumulative += count
    return float(hi)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --frozen pytest tests/units/core/test_job_metrics.py -q -k InterpolateQuantile`
Expected: PASS (8 passed)

- [ ] **Step 5: Commit**

```bash
git add src/back/core/graph_analysis/JobMetrics.py tests/units/core/test_job_metrics.py
git commit -m "Interpolate median and p90 from histogram bin counts

The read-back must run on engines without a percentile function, so the
quantiles are recovered from the bins with an error bounded by one bin width."
```

---

## Task 4: Assemble distributions into the result

**Files:**
- Modify: `src/back/core/graph_analysis/JobMetrics.py` (`compute` ~line 171-223; new `_read_distributions`)
- Test: `tests/units/core/test_job_metrics.py`

**Interfaces:**
- Consumes: `MetricDistribution` (Task 1), `distribution_bounds_query` / `distributions_query` (Task 2), `interpolate_quantile` (Task 3)
- Produces: `MetricsResult.distributions` populated by `JobMetrics.compute()`

- [ ] **Step 1: Extend the fake query dispatcher**

The existing `_query_for` helper dispatches on the table-name suffix in the SQL. Both `top_nodes_query` and the two distribution queries read the **main** output table, so the suffix trick cannot tell them apart. Replace `_query_for` in `tests/units/core/test_job_metrics.py` with:

```python
#: Keys that read the main output table and so cannot be told apart by a table
#: suffix. Matched on a token that appears only in that statement.
_QUERY_TOKENS = {
    "distribution_bounds": "lo_pagerank",
    "distributions": "bin_index",
}


def _query_for(rows_by_table: Dict[str, List[Dict[str, Any]]]):
    """A fake warehouse query that dispatches on what is being read."""
    def query(sql: str) -> List[Dict[str, Any]]:
        for key, token in _QUERY_TOKENS.items():
            if key in rows_by_table and token in sql:
                return rows_by_table[key]
        for suffix, rows in rows_by_table.items():
            if suffix in _QUERY_TOKENS:
                continue
            if suffix and f"_{suffix}" in sql:
                return rows
        return rows_by_table.get("", [])
    return query
```

Note the ordering: token matches are checked **before** suffix matches, because `distribution_bounds_query` contains no `_summary` / `_type_profiles` marker but the distributions statements must never fall through to the bare-table branch that serves node rows.

- [ ] **Step 2: Write the failing test**

Add to `tests/units/core/test_job_metrics.py`:

```python
# ---------------------------------------------------------------------------
# Distribution assembly
# ---------------------------------------------------------------------------


def _bounds_row(**overrides):
    """One bounds row with a usable range for every metric."""
    base = {}
    for m in ("degree", "pagerank", "betweenness", "closeness", "clustering"):
        base[f"lo_{m}"] = 0.0
        base[f"hi_{m}"] = 4.0
        base[f"mean_{m}"] = 2.0
    base.update(overrides)
    return base


def _bin_rows(metric: str, counts: List[int]):
    return [
        {"metric": metric, "bin_index": i, "node_count": c}
        for i, c in enumerate(counts) if c
    ]


_ALL_FIVE = ("degree", "pagerank", "betweenness", "closeness", "clustering")

#: Sentinel so a caller can ask for *no* bounds row, which None cannot express
#: here — None has to keep meaning "give me the default".
_ABSENT = object()


def _metrics_with_distributions(*, summary=None, bin_rows=None, bounds=_ABSENT):
    if bounds is _ABSENT:
        bounds_rows = [_bounds_row()]
    elif bounds is None:
        bounds_rows = []
    else:
        bounds_rows = [bounds]
    if bin_rows is None:
        bin_rows = [r for m in _ALL_FIVE for r in _bin_rows(m, [2, 2, 2, 2])]
    query = _query_for({
        "summary": [summary or _default_summary_row()],
        "type_profiles": [],
        "type_predicates": [],
        "distribution_bounds": bounds_rows,
        "distributions": bin_rows,
        "": [],
    })
    return _job_metrics(query=query)


def test_distributions_are_assembled_for_available_metrics():
    result = _metrics_with_distributions().compute(MetricsRequest())
    d = result.distributions["pagerank"]
    assert d.bins == [2, 2, 2, 2]
    assert d.bin_count == 4
    assert d.lo == pytest.approx(0.0)
    assert d.hi == pytest.approx(4.0)
    assert d.mean == pytest.approx(2.0)
    # uniform over 0..4
    assert d.median == pytest.approx(2.0)
    assert d.p90 == pytest.approx(3.6)


def test_empty_bins_are_padded_with_zero_not_dropped():
    """The front end indexes bins positionally, so gaps must be explicit."""
    rows = _bin_rows("pagerank", [5, 0, 0, 3])
    result = _metrics_with_distributions(bin_rows=rows).compute(MetricsRequest())
    assert result.distributions["pagerank"].bins == [5, 0, 0, 3]


def test_unavailable_metrics_get_no_distribution():
    """Stored as zeros; a histogram of them would read as real measurements."""
    summary = _default_summary_row(pivot_count=0, bfs_complete=True)
    result = _metrics_with_distributions(summary=summary).compute(MetricsRequest())
    assert set(result.unavailable_metrics) == set(UNAVAILABLE_METRICS)
    assert "betweenness" not in result.distributions
    assert "closeness" not in result.distributions
    # the exactly-computed metrics are unaffected
    assert "pagerank" in result.distributions
    assert "degree" in result.distributions
    assert "clustering" in result.distributions


def test_truncated_bfs_also_withholds_the_distribution():
    summary = _default_summary_row(pivot_count=8, bfs_complete=False)
    result = _metrics_with_distributions(summary=summary).compute(MetricsRequest())
    assert "betweenness" not in result.distributions
    assert "closeness" not in result.distributions


def test_approximate_metrics_do_get_a_distribution():
    result = _metrics_with_distributions().compute(MetricsRequest())
    assert set(result.approximate_metrics) == set(APPROXIMATE_METRICS)
    assert "betweenness" in result.distributions
    assert "closeness" in result.distributions


def test_a_failing_distribution_read_does_not_fail_the_run():
    """An analysis that scored every node must not be thrown away because a
    presentation aggregate failed."""
    def query(sql: str):
        if "bin_index" in sql or "lo_pagerank" in sql:
            raise RuntimeError("warehouse blew up")
        if "_summary" in sql:
            return [_default_summary_row()]
        return []

    result = _job_metrics(query=query).compute(MetricsRequest())
    assert result.distributions == {}
    assert result.stats.node_count == 5
    assert result.mode == MODE_JOB


def test_missing_bounds_row_yields_no_distributions():
    """No bounds means no bin edges, so there is nothing honest to draw."""
    metrics = _metrics_with_distributions(bounds=None)
    assert metrics.compute(MetricsRequest()).distributions == {}


def test_the_default_fixture_does_produce_distributions():
    """Guards the test above: it must fail for the reason it claims."""
    assert _metrics_with_distributions().compute(MetricsRequest()).distributions
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run --frozen pytest tests/units/core/test_job_metrics.py -q -k "distribution or bfs or bounds"`
Expected: FAIL — `KeyError: 'pagerank'`, because `compute()` never populates `distributions`.

- [ ] **Step 4: Write minimal implementation**

In `src/back/core/graph_analysis/JobMetrics.py`, add the call inside `compute()` between `self._read_type_profiles(result)` (line 210) and `result.stats.elapsed_ms = ...` (line 211) — before the timer, so the "Computed in" KPI accounts for the read:

```python
        self._read_type_profiles(result)
        self._read_distributions(result)
        result.stats.elapsed_ms = int((time.time() - t0) * 1000)
```

The order matters: `_read_distributions` reads `result.unavailable_metrics`, which `_read_summary` sets at line 234 or 270.

Then add the reader method after `_read_type_profiles`:

```python
    def _read_distributions(self, result: MetricsResult) -> None:
        """Summarise every scored node into per-metric histograms.

        Called after :meth:`_read_summary` because it depends on that method's
        verdict on which metrics are trustworthy.

        Never raises. A distribution is a presentation aggregate, and an analysis
        that successfully scored the whole graph must not be discarded because
        the histogram query failed — the UI already renders an explanatory empty
        state for a result without one.
        """
        try:
            bounds_rows = self._query(
                distribution_bounds_query(self._output_table)
            ) or []
            if not bounds_rows:
                return
            bounds = bounds_rows[0]

            counts: Dict[str, Dict[int, int]] = {}
            for row in self._query(
                distributions_query(self._output_table, self._distribution_bins)
            ) or []:
                metric = row.get("metric") or ""
                if metric not in NODE_METRIC_KEYS:
                    continue
                counts.setdefault(metric, {})[
                    int(row.get("bin_index", 0) or 0)
                ] = int(row.get("node_count", 0) or 0)

            for metric in NODE_METRIC_KEYS:
                # A metric this run could not compute is stored as zeros. Its
                # histogram would be a single tall bar at zero, which reads as a
                # measurement rather than as an absence.
                if metric in result.unavailable_metrics:
                    continue

                lo = float(bounds.get(f"lo_{metric}", 0.0) or 0.0)
                hi = float(bounds.get(f"hi_{metric}", 0.0) or 0.0)
                mean = float(bounds.get(f"mean_{metric}", 0.0) or 0.0)

                per_bin = counts.get(metric, {})
                # Padded rather than sparse: the front end indexes positionally,
                # and a bin with no nodes is a real, meaningful zero.
                bins = [
                    per_bin.get(i, 0) for i in range(self._distribution_bins)
                ]

                result.distributions[metric] = MetricDistribution(
                    bins=bins,
                    bin_count=self._distribution_bins,
                    lo=round(lo, 8),
                    hi=round(hi, 8),
                    mean=round(mean, 8),
                    median=round(interpolate_quantile(bins, lo, hi, 0.5), 8),
                    p90=round(interpolate_quantile(bins, lo, hi, 0.9), 8),
                )
        except Exception:  # noqa: BLE001 — a chart is never worth failing a run
            logger.warning(
                "could not read metric distributions for %s; the result is "
                "complete but the Analytics page will show no histograms",
                self._output_table,
                exc_info=True,
            )
            result.distributions.clear()
```

Add `_distribution_bins` to `JobMetrics.__init__`, after `self._max_depth`:

```python
        self._distribution_bins = DEFAULT_DISTRIBUTION_BINS
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run --frozen pytest tests/units/core/test_job_metrics.py -q`
Expected: PASS

- [ ] **Step 6: Run the full unit suite**

Run: `uv run --frozen pytest -q -m "not scenario"`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/back/core/graph_analysis/JobMetrics.py tests/units/core/test_job_metrics.py
git commit -m "Assemble metric distributions into the analytics result

Withholds a distribution for metrics the run could not compute, since those
are stored as zeros and a histogram of them would read as a measurement.
A failed histogram read leaves the rest of the result intact."
```

---

## Task 5: Split the partial into template + CSS + JS

**Pure move. No behaviour change, no markup change.** Doing the layout work first would mean rewriting inside a file that already breaks two structural rules.

**Files:**
- Create: `src/front/static/query/css/query-analytics.css`
- Create: `src/front/static/query/js/query-analytics.js`
- Modify: `src/front/templates/partials/dtwin/_query_analytics.html` (remove the `<style>` block at lines 2-19 and the `<script>` block at lines 336-1452)
- Modify: `src/front/templates/dtwin.html` (`extra_css` / `extra_js` blocks; Chart.js CDN is at line 156)
- Modify: `tests/units/front/test_analytics_unavailable_metrics.py:20`
- Modify: `tests/units/dtwin/test_analytics_job_status.py:154`
- Modify: `tests/units/front/test_runs_page.py:34-36`

**Interfaces:**
- Consumes: nothing
- Produces: `static/query/js/query-analytics.js` holding every function previously inline, with **unchanged names**: `window.analyticsLoadTypes`, `analyticsCompute`, `analyticsRenderCharts`, `analyticsResume`, `analyticsLoadLatest`, `analyticsInterpret`, `analyticsAddToAuditTrail`, `_analyticsDrillURI`, `_showMetricInfo`. `query.js` and `query-sigmagraph.js` call across this boundary.

- [ ] **Step 1: Write the failing test**

Create `tests/units/front/test_analytics_dashboard.py`:

```python
"""The Analytics section follows the project's asset-split convention.

The section used to carry ~1100 lines of inline JS and an inline <style> block,
both forbidden by .cursor/11-frontend-design.mdc. Everything else in this area
(query-sync, query-cohorts, ...) is already split; this pins that.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
PANEL = REPO_ROOT / "src/front/templates/partials/dtwin/_query_analytics.html"
DTWIN = REPO_ROOT / "src/front/templates/dtwin.html"
CSS = REPO_ROOT / "src/front/static/query/css/query-analytics.css"
JS = REPO_ROOT / "src/front/static/query/js/query-analytics.js"


@pytest.fixture(scope="module")
def panel() -> str:
    return PANEL.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js() -> str:
    return JS.read_text(encoding="utf-8")


class TestAssetsAreSplitOut:
    def test_css_and_js_files_exist(self):
        assert CSS.is_file()
        assert JS.is_file()

    def test_partial_has_no_inline_style_block(self, panel):
        assert "<style>" not in panel

    def test_partial_has_no_inline_script_block(self, panel):
        # The metric-explanation modal markup stays; executable JS does not.
        assert "<script>" not in panel

    def test_dtwin_loads_both_assets_cache_busted(self):
        html = DTWIN.read_text(encoding="utf-8")
        for asset in ("query/css/query-analytics.css",
                      "query/js/query-analytics.js"):
            assert asset in html
        block = html[html.index("query-analytics.js") - 400:]
        assert "asset_version" in block[:600]

    def test_cross_file_globals_keep_their_names(self, js):
        """query.js and query-sigmagraph.js call these by name."""
        for name in (
            "window.analyticsLoadTypes",
            "window.analyticsCompute",
            "window.analyticsRenderCharts",
            "window.analyticsResume",
            "window.analyticsLoadLatest",
            "window.analyticsInterpret",
            "window.analyticsAddToAuditTrail",
            "window._analyticsDrillURI",
            "window._showMetricInfo",
        ):
            assert name in js, f"{name} must survive the move"

    def test_the_metric_explanation_modal_stays_in_the_partial(self, panel):
        assert 'id="analyticsMetricModal"' in panel
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/units/front/test_analytics_dashboard.py -q`
Expected: FAIL — `assert CSS.is_file()` fails; the inline-block assertions fail.

- [ ] **Step 3: Move the CSS**

Create `src/front/static/query/css/query-analytics.css` with the contents of the partial's `<style>` block (lines 3-18), commented:

```css
/* ==========================================================================
   Knowledge Graph -> Analytics
   Paired with templates/partials/dtwin/_query_analytics.html and
   static/query/js/query-analytics.js.
   ========================================================================== */

/* Fill the available height and scroll inside the pane rather than the page. */
#analyticsSection {
    overflow: hidden;
}

#analyticsResults {
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
}

#analyticsResults > .ob-tab-content {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    overflow-x: hidden;
}
```

Delete lines 2-19 (the `<style>` block) from the partial.

- [ ] **Step 4: Move the JS verbatim**

Create `src/front/static/query/js/query-analytics.js` containing **exactly** the body of the partial's `<script>` block (lines 337-1451), i.e. the whole `(function () { ... })();` IIFE, with this header prepended:

```javascript
/* Knowledge Graph -> Analytics section.
 *
 * Paired with templates/partials/dtwin/_query_analytics.html.
 * The window.* functions at the bottom are called from query.js and
 * query-sigmagraph.js -- do not rename them without updating both.
 */
```

Do not reformat, rename, or "improve" anything in this step. Then delete lines 336-1452 (the `<script>` block) from the partial.

- [ ] **Step 5: Wire the assets in `dtwin.html`**

In the `extra_css` block, add alongside the other `query/css` links:

```html
<link rel="stylesheet" href="{{ url_for('static', filename='query/css/query-analytics.css') }}?v={{ asset_version }}">
```

In the `extra_js` block, **after** the Chart.js CDN tag at line 156 (the section builds charts on load, so Chart must already be defined):

```html
<script defer src="{{ url_for('static', filename='query/js/query-analytics.js') }}?v={{ asset_version }}"></script>
```

- [ ] **Step 6: Repoint the three test files that read JS out of the partial**

These currently assert on JS strings inside the partial and will all fail after the move.

In `tests/units/front/test_analytics_unavailable_metrics.py`, change line 20 and the `html` fixture:

```python
PANEL = REPO_ROOT / "src/front/templates/partials/dtwin/_query_analytics.html"
SCRIPT = REPO_ROOT / "src/front/static/query/js/query-analytics.js"


@pytest.fixture(scope="module")
def html() -> str:
    """Markup and behaviour now live in two files; assert across both."""
    return PANEL.read_text() + "\n" + SCRIPT.read_text()
```

In `tests/units/dtwin/test_analytics_job_status.py`, change line 154:

```python
PANEL = ROOT / "src/front/static/query/js/query-analytics.js"
```

(Every assertion in that file's `TestTheReasonReachesTheBanner` is about JS — `_jobBlockedReason`, `data.analytics_job_blocked_reason`, the banner copy — all of which moved.)

In `tests/units/front/test_runs_page.py`, change the helper at lines 34-36:

```python
_ANALYTICS_JS = Path("src/front/static/query/js/query-analytics.js")


def _analytics_partial() -> str:
    """Markup plus behaviour: the History tab's traces could be in either."""
    return _ANALYTICS.read_text(encoding="utf-8") + "\n" + _ANALYTICS_JS.read_text(encoding="utf-8")
```

This keeps `test_no_trace_of_the_history_tab_in_the_source` honest (absence must hold in **both** files) and fixes `test_helpers_the_other_tabs_still_use_survive`, which looks for `function _formatComputedAt` and `function _localName`.

- [ ] **Step 7: Run the tests**

Run: `uv run --frozen pytest tests/units/front/test_analytics_dashboard.py tests/units/front/test_analytics_unavailable_metrics.py tests/units/dtwin/test_analytics_job_status.py tests/units/front/test_runs_page.py tests/units/front/test_kg_readiness_indicator.py -q`
Expected: PASS

- [ ] **Step 8: Run the full unit suite**

Run: `uv run --frozen pytest -q -m "not scenario"`
Expected: PASS

- [ ] **Step 9: Verify in the browser**

Load `/dtwin/`, open Knowledge Graph → Analytics. The page must look and behave **identically** to before: charts render, Run Analysis works, tabs switch, the `?` modals open. This step is the whole point of keeping the move separate.

- [ ] **Step 10: Commit**

```bash
git add src/front/static/query/css/query-analytics.css \
        src/front/static/query/js/query-analytics.js \
        src/front/templates/partials/dtwin/_query_analytics.html \
        src/front/templates/dtwin.html \
        tests/units/front/test_analytics_dashboard.py \
        tests/units/front/test_analytics_unavailable_metrics.py \
        tests/units/dtwin/test_analytics_job_status.py \
        tests/units/front/test_runs_page.py
git commit -m "Split the Analytics section into template, CSS and JS

The partial carried an inline <style> block and ~1100 lines of inline JS,
both forbidden by the frontend rules and both about to be rewritten. Pure
move: no markup or behaviour change."
```

---

## Task 6: Collapse seven tabs to three and build the Dashboard shell

**Files:**
- Modify: `src/front/templates/partials/dtwin/_query_analytics.html`
- Modify: `src/front/static/query/js/query-analytics.js`
- Modify: `src/front/static/query/css/query-analytics.css`
- Test: `tests/units/front/test_analytics_dashboard.py`

**Interfaces:**
- Consumes: the split files from Task 5
- Produces: DOM ids `analytics-tab-dashboard` / `atab-dashboard`, `atab-btn-health` / `atab-health`, `atab-btn-insights` / `atab-insights`; a `#analyticsDistStrip` container and `#analyticsRankingCard` container for Tasks 7 and 8; the KPI row rebuilt on `.ob-kpi-tile` keeping the ids `aStatNodes`, `aStatEdges`, `aStatComponents`, `aStatAvgDegree`, `aStatDensity`, `aStatElapsed`, `aStatGraphNodes`

- [ ] **Step 1: Write the failing test**

Add to `tests/units/front/test_analytics_dashboard.py`:

```python
class TestTabStripIsThreeTabs:
    REMOVED = (
        "atab-btn-pagerank", "atab-pagerank",
        "atab-btn-betweenness", "atab-betweenness",
        "atab-btn-degree", "atab-degree",
        "atab-btn-closeness", "atab-closeness",
        "atab-btn-clustering", "atab-clustering",
    )

    @pytest.mark.parametrize("marker", REMOVED)
    def test_per_metric_tabs_are_gone(self, panel, marker):
        assert marker not in panel

    @pytest.mark.parametrize("marker", (
        "atab-btn-dashboard", "atab-dashboard",
        "atab-btn-health", "atab-health",
        "atab-btn-insights", "atab-insights",
    ))
    def test_the_three_surviving_tabs_are_present(self, panel, marker):
        assert marker in panel

    def test_dashboard_is_the_default_tab(self, panel):
        strip = panel[panel.index('id="analyticsTabs"'):]
        strip = strip[: strip.index("</ul>")]
        assert strip.count("nav-link active") == 1
        dashboard_at = strip.index("atab-btn-dashboard")
        active_at = strip.index("nav-link active")
        assert active_at < dashboard_at, "the active class must be on Dashboard"

    def test_the_strip_uses_the_shared_ob_tabs_treatment(self, panel):
        assert 'class="nav nav-tabs ob-tabs' in panel

    def test_the_strip_does_not_re_apply_baked_in_utilities(self, panel):
        strip = panel[panel.index('id="analyticsTabs"'):]
        strip = strip[: strip.index(">")]
        for banned in ("px-3", "pt-2", "pt-3", "bg-white", "font-size"):
            assert banned not in strip


class TestDashboardShell:
    def test_the_dashboard_pane_holds_the_strip_and_ranking_containers(self, panel):
        pane = panel[panel.index('id="atab-dashboard"'):]
        pane = pane[: pane.index('id="atab-health"')]
        assert 'id="analyticsDistStrip"' in pane
        assert 'id="analyticsRankingCard"' in pane
        assert 'id="pagerankDetailTable"' in pane, "detail table moves here"


class TestKpiRowUsesTheSharedTile:
    def test_kpi_tiles_use_the_ob_kpi_tile_component(self, panel):
        row = panel[panel.index('id="analyticsStatsRow"'):]
        row = row[: row.index('id="analyticsDistStrip"')]
        assert "ob-kpi-tile" in row

    def test_the_hand_rolled_tile_markup_is_gone(self, panel):
        row = panel[panel.index('id="analyticsStatsRow"'):]
        row = row[: row.index('id="analyticsDistStrip"')]
        assert "border-0 bg-light" not in row

    @pytest.mark.parametrize("stat_id", (
        "aStatNodes", "aStatEdges", "aStatComponents",
        "aStatAvgDegree", "aStatDensity", "aStatElapsed", "aStatGraphNodes",
    ))
    def test_every_stat_id_survives_the_rebuild(self, panel, stat_id):
        """_renderAnalyticsData writes to these by id."""
        assert stat_id in panel
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/units/front/test_analytics_dashboard.py -q -k "TabStrip or DashboardShell or KpiRow"`
Expected: FAIL — `atab-btn-pagerank` is still present, `atab-btn-dashboard` is missing.

- [ ] **Step 3: Replace the tab strip**

In `_query_analytics.html`, replace the whole `<ul id="analyticsTabs">` (lines 130-173 pre-Task-5 numbering) with:

```html
<ul class="nav nav-tabs ob-tabs mb-0" id="analyticsTabs" role="tablist">
    <li class="nav-item" role="presentation">
        <button class="nav-link active" id="atab-btn-dashboard" data-bs-toggle="tab"
                data-bs-target="#atab-dashboard" type="button" role="tab"
                aria-controls="atab-dashboard" aria-selected="true">
            <i class="bi bi-speedometer2 me-1"></i>Dashboard
        </button>
    </li>
    <li class="nav-item" role="presentation">
        <button class="nav-link" id="atab-btn-health" data-bs-toggle="tab"
                data-bs-target="#atab-health" type="button" role="tab"
                aria-controls="atab-health" aria-selected="false">
            <i class="bi bi-activity me-1 text-info"></i>Data Model Health
        </button>
    </li>
    <li class="nav-item" role="presentation">
        <button class="nav-link" id="atab-btn-insights" data-bs-toggle="tab"
                data-bs-target="#atab-insights" type="button" role="tab"
                aria-controls="atab-insights" aria-selected="false">
            <i class="bi bi-stars me-1 text-warning"></i>AI Insights
        </button>
    </li>
</ul>
```

- [ ] **Step 4: Replace the five metric panes with one Dashboard pane**

Delete the five `<div class="tab-pane" id="atab-pagerank|betweenness|degree|closeness|clustering">` blocks. In their place, as the first pane inside `#analyticsTabContent`:

```html
<!-- TAB: Dashboard -->
<div class="tab-pane fade show active" id="atab-dashboard" role="tabpanel"
     aria-labelledby="atab-btn-dashboard">

    <!-- Global distributions across every scored node -->
    <div class="analytics-dist-strip mt-2" id="analyticsDistStrip"></div>

    <!-- Top-N ranking for the selected metric -->
    <div id="analyticsRankingCard"></div>

    <!-- Node detail — all five metrics per node -->
    <div class="card mt-3">
        <div class="card-header py-2 small fw-semibold d-flex justify-content-between align-items-center">
            <span><i class="bi bi-table me-1"></i>Node detail — all metrics</span>
            <span class="text-muted fw-normal" style="font-size:0.78rem">
                Click a row to open it in the Graph Viewer
            </span>
        </div>
        <div class="card-body p-0">
            <div id="pagerankTableNote" class="small text-muted px-3 pt-2 d-none"></div>
            <div class="table-responsive">
                <table class="table table-sm table-hover align-middle mb-0" id="pagerankDetailTable">
                    <thead class="table-light">
                        <tr>
                            <th class="text-center ps-3" style="width:32px">#</th>
                            <th>Node</th>
                            <th class="text-end text-primary">PageRank</th>
                            <th class="text-end text-success">Degree</th>
                            <th class="text-end text-danger">Betweenness</th>
                            <th class="text-end text-info">Closeness</th>
                            <th class="text-end text-warning pe-3">Clustering</th>
                        </tr>
                    </thead>
                    <tbody id="pagerankDetailBody"></tbody>
                </table>
            </div>
        </div>
    </div>
</div>
```

Move the inline `style="font-size:0.78rem"` above into the CSS file as `.analytics-detail-hint` in Step 6 — inline styles are forbidden.

The `#atab-health` and `#atab-insights` panes keep their existing content **verbatim**.

- [ ] **Step 5: Rebuild the KPI row on `.ob-kpi-tile`**

Replace the `#analyticsStatsRow` block with:

```html
<div class="row g-2 mb-3" id="analyticsStatsRow">
    <div class="col-6 col-md-4 col-lg-2">
        <div class="ob-kpi-tile">
            <div class="ob-kpi-tile-icon"><i class="bi bi-circle-fill"></i></div>
            <div class="ob-kpi-tile-value" id="aStatNodes">—</div>
            <div class="ob-kpi-tile-label">Nodes</div>
            <div class="ob-kpi-tile-label analytics-kpi-sublabel" id="aStatGraphNodes"></div>
        </div>
    </div>
    <div class="col-6 col-md-4 col-lg-2">
        <div class="ob-kpi-tile">
            <div class="ob-kpi-tile-icon"><i class="bi bi-share"></i></div>
            <div class="ob-kpi-tile-value" id="aStatEdges">—</div>
            <div class="ob-kpi-tile-label">Edges</div>
        </div>
    </div>
    <div class="col-6 col-md-4 col-lg-2">
        <div class="ob-kpi-tile tile-success">
            <div class="ob-kpi-tile-icon"><i class="bi bi-diagram-2"></i></div>
            <div class="ob-kpi-tile-value" id="aStatComponents">—</div>
            <div class="ob-kpi-tile-label">Components</div>
        </div>
    </div>
    <div class="col-6 col-md-4 col-lg-2">
        <div class="ob-kpi-tile">
            <div class="ob-kpi-tile-icon"><i class="bi bi-node-plus"></i></div>
            <div class="ob-kpi-tile-value" id="aStatAvgDegree">—</div>
            <div class="ob-kpi-tile-label">Avg Degree</div>
        </div>
    </div>
    <div class="col-6 col-md-4 col-lg-2">
        <div class="ob-kpi-tile tile-muted">
            <div class="ob-kpi-tile-icon"><i class="bi bi-grid-3x3"></i></div>
            <div class="ob-kpi-tile-value" id="aStatDensity">—</div>
            <div class="ob-kpi-tile-label">Density</div>
        </div>
    </div>
    <div class="col-6 col-md-4 col-lg-2">
        <div class="ob-kpi-tile tile-muted">
            <div class="ob-kpi-tile-icon"><i class="bi bi-stopwatch"></i></div>
            <div class="ob-kpi-tile-value" id="aStatElapsed">—</div>
            <div class="ob-kpi-tile-label">Computed in</div>
        </div>
    </div>
</div>
```

Before writing this, **read `src/front/static/global/css/components.css`** and confirm the `.ob-kpi-tile` sub-element class names and available `.tile-*` variants; use what is actually there rather than the names above if they differ.

- [ ] **Step 6: Add the CSS**

Append to `query-analytics.css`:

```css
/* --- Dashboard: KPI sub-label and detail hint ---------------------------- */

.analytics-kpi-sublabel {
    font-size: 0.7rem;
    color: var(--db-text-muted);
}

.analytics-detail-hint {
    font-size: 0.78rem;
}
```

Replace the inline `style="font-size:0.78rem"` from Step 4 with `class="text-muted fw-normal analytics-detail-hint"`.

- [ ] **Step 7: Remove the dead chart plumbing from the JS**

In `query-analytics.js`, the `shown.bs.tab` handler resizes charts by mapping the deleted tab targets. Replace its `keyMap` lookup with a single dashboard branch:

```javascript
    // Chart.js renders at 0px in a hidden pane, so charts are resized when the
    // Dashboard tab becomes visible.
    document.addEventListener('shown.bs.tab', function (e) {
        var target = e.target && e.target.getAttribute('data-bs-target');
        if (target !== '#atab-dashboard') return;
        Object.keys(_charts).forEach(function (key) {
            if (_charts[key]) _charts[key].resize();
        });
    });
```

- [ ] **Step 8: Run the tests**

Run: `uv run --frozen pytest tests/units/front/ tests/units/dtwin/test_analytics_job_status.py -q`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add src/front/templates/partials/dtwin/_query_analytics.html \
        src/front/static/query/js/query-analytics.js \
        src/front/static/query/css/query-analytics.css \
        tests/units/front/test_analytics_dashboard.py
git commit -m "Collapse the five metric tabs into one Dashboard pane

The five metrics are most useful read against each other, which tabs made a
memory exercise. Also rebuilds the KPI row on the shared .ob-kpi-tile
component instead of the hand-rolled copy of it."
```

---

## Task 7: The distribution strip

**Files:**
- Modify: `src/front/static/query/js/query-analytics.js`
- Modify: `src/front/static/query/css/query-analytics.css`
- Test: `tests/units/front/test_analytics_dashboard.py`

**Interfaces:**
- Consumes: `#analyticsDistStrip` (Task 6); `_analyticsData.distributions` (Task 4)
- Produces: `_renderDistributionStrip()`, `_selectMetric(key)`, module state `_selectedMetric` (defaults to `'pagerank'`), `_logScale` (defaults to `false`), `_distCharts`, `_fmtMetric(v)`, canvas ids `distChart_<metric>`, tile ids `distTile_<metric>`

> **Ordering dependency:** `_selectMetric` calls `_renderRankingChart`, which
> **Task 8** delivers. Function declarations hoist, so the file parses, but
> clicking a tile throws until Task 8 lands. Do the browser check for this task
> *after* Task 8, and do not "fix" the missing function by stubbing it here —
> Task 8 replaces the old chart loop with the real one.

> **No `?` help button on the tiles.** A tile is a `<button>`, and nesting a
> button inside one is invalid HTML. The help affordance lives on the ranking
> card header in Task 8, which always names the same metric as the selected tile.

- [ ] **Step 1: Write the failing test**

Add to `tests/units/front/test_analytics_dashboard.py`:

```python
class TestDistributionStrip:
    def test_the_strip_is_rendered_from_the_distributions_payload(self, js):
        assert "function _renderDistributionStrip" in js
        assert "_analyticsData.distributions" in js

    def test_the_metric_table_lists_all_five_in_display_order(self, js):
        """PageRank was absent from the old _METRICS list because its tab held a
        table, not a chart. The strip charts all five."""
        table = js[js.index("_ALL_METRICS = ["):]
        table = table[: table.index("];")]
        keys = re.findall(r"key:\s*'(\w+)'", table)
        assert keys == ["pagerank", "betweenness", "degree",
                        "closeness", "clustering"]

    def test_every_metric_in_the_table_has_a_colour_and_an_icon(self, js):
        table = js[js.index("_ALL_METRICS = ["):]
        table = table[: table.index("];")]
        assert len(re.findall(r"color:", table)) == 5
        assert len(re.findall(r"icon:", table)) == 5

    def test_pagerank_is_selected_on_load(self, js):
        assert "_selectedMetric = 'pagerank'" in js

    def test_a_missing_distribution_renders_an_empty_state_not_a_chart(self, js):
        """A legacy payload has no distributions at all; an unavailable metric
        has none for that key. Neither may reach Chart.js."""
        fn = _fn(js, "_renderDistributionStrip")
        assert "Re-run the analysis" in fn
        assert "Not computed for this run" in fn
        # The guard must return before any chart is constructed.
        guard_at = fn.index("!dist.bins")
        assert guard_at < fn.index("new Chart")
        between = fn[guard_at:fn.index("new Chart")]
        assert "return" in between

    def test_tiles_are_buttons_so_selection_is_keyboard_reachable(self, js):
        fn = _fn(js, "_renderDistributionStrip")
        assert "<button" in fn

    def test_approximate_metrics_are_badged_in_the_strip(self, js):
        fn = _fn(js, "_renderDistributionStrip")
        assert "asymp" in fn or "estimate" in fn.lower()

    def test_the_median_is_labelled_as_approximate(self, js):
        """Interpolated from bins; presenting it as exact would overstate it."""
        fn = _fn(js, "_renderDistributionStrip")
        assert "median" in fn.lower()
        assert "asymp" in fn or "~" in fn

    def test_selecting_a_tile_redraws_the_ranking_chart(self, js):
        fn = _fn(js, "_selectMetric")
        assert "_renderRankingChart" in fn
```

Add this helper and the `re` import near the top of the test file:

```python
def _fn(source: str, name: str) -> str:
    """The body of a named function, up to the next declaration at its level.

    The section's JS is one IIFE of 4-space-indented declarations, so the next
    `\\n    function ` or `\\n    window.` is a reliable terminator.
    """
    header = "function " + name
    start = source.index(header)
    rest = source[start + len(header):]
    ends = [i for i in (rest.find("\n    function "),
                        rest.find("\n    window.")) if i != -1]
    return rest[: min(ends)] if ends else rest
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/units/front/test_analytics_dashboard.py -q -k DistributionStrip`
Expected: FAIL — `ValueError: substring not found` for `function _renderDistributionStrip`.

- [ ] **Step 3: Implement the strip**

In `query-analytics.js`, extend the metric table so all five metrics (not just the four that had charts) carry presentation metadata, replacing the existing `_METRICS`:

```javascript
    // Every metric, in display order, with its presentation metadata. PageRank
    // was previously absent from this list because its tab held a table rather
    // than a chart; the dashboard charts all five.
    var _ALL_METRICS = [
        { key: 'pagerank',    label: 'PageRank',    icon: 'bi-diagram-3',          color: 'rgba(13, 110, 253, 0.75)' },
        { key: 'betweenness', label: 'Betweenness', icon: 'bi-share',              color: 'rgba(220, 53, 69, 0.75)'  },
        { key: 'degree',      label: 'Degree',      icon: 'bi-node-plus',          color: 'rgba(25, 135, 84, 0.75)'  },
        { key: 'closeness',   label: 'Closeness',   icon: 'bi-arrows-fullscreen',  color: 'rgba(13, 202, 240, 0.75)' },
        { key: 'clustering',  label: 'Clustering',  icon: 'bi-hexagon',            color: 'rgba(255, 193, 7, 0.85)'  }
    ];

    var _selectedMetric = 'pagerank';
    var _distCharts = {};
    var _logScale = false;
```

Add the renderer:

```javascript
    // Format a metric value compactly enough for a tile caption.
    function _fmtMetric(v) {
        if (v === 0) return '0';
        if (v == null || isNaN(v)) return '—';
        return Math.abs(v) < 0.001 ? v.toExponential(1) : v.toFixed(4);
    }

    function _renderDistributionStrip() {
        var host = document.getElementById('analyticsDistStrip');
        if (!host) return;

        var dists = (_analyticsData && _analyticsData.distributions) || {};
        var approximate = (_analyticsData && _analyticsData.approximate_metrics) || [];
        var unavailable = (_analyticsData && _analyticsData.unavailable_metrics) || [];

        Object.keys(_distCharts).forEach(function (k) {
            if (_distCharts[k]) _distCharts[k].destroy();
        });
        _distCharts = {};

        host.innerHTML = _ALL_METRICS.map(function (m) {
            var isApprox = approximate.indexOf(m.key) !== -1;
            var badge = isApprox
                ? '<span class="analytics-dist-badge" title="Sampled estimate">&asymp;</span>'
                : '';
            return ''
                + '<button type="button" class="analytics-dist-tile'
                + (m.key === _selectedMetric ? ' selected' : '') + '"'
                + ' id="distTile_' + m.key + '"'
                + ' data-metric="' + m.key + '"'
                + ' aria-pressed="' + (m.key === _selectedMetric) + '"'
                + ' title="Show the top-ranked nodes by ' + m.label + '">'
                + '  <span class="analytics-dist-head">'
                + '    <i class="bi ' + m.icon + '"></i>' + m.label + badge
                + '  </span>'
                + '  <span class="analytics-dist-body">'
                + '    <canvas id="distChart_' + m.key + '"></canvas>'
                + '  </span>'
                + '  <span class="analytics-dist-caption" id="distCaption_' + m.key + '"></span>'
                + '</button>';
        }).join('');

        host.querySelectorAll('.analytics-dist-tile').forEach(function (el) {
            el.addEventListener('click', function () {
                _selectMetric(el.getAttribute('data-metric'));
            });
        });

        _ALL_METRICS.forEach(function (m) {
            var dist = dists[m.key];
            var caption = document.getElementById('distCaption_' + m.key);
            var canvas = document.getElementById('distChart_' + m.key);
            if (!canvas || !caption) return;

            // A metric the run could not compute is stored as zeros, and a
            // legacy cached result predates distributions entirely. Neither may
            // be drawn as a chart.
            if (!dist || !dist.bins || !dist.bins.length) {
                canvas.style.display = 'none';
                caption.innerHTML = unavailable.indexOf(m.key) !== -1
                    ? '<i class="bi bi-dash-circle me-1"></i>Not computed for this run'
                    : '<i class="bi bi-info-circle me-1"></i>Re-run the analysis to see the distribution';
                return;
            }
            canvas.style.display = '';

            caption.innerHTML = 'median &asymp; ' + _fmtMetric(dist.median)
                + ' &middot; max ' + _fmtMetric(dist.hi)
                + (_logScale ? ' &middot; <em>log</em>' : '');

            var width = (dist.hi - dist.lo) / dist.bins.length;
            _distCharts[m.key] = new Chart(canvas, {
                type: 'bar',
                data: {
                    labels: dist.bins.map(function (_, i) {
                        return _fmtMetric(dist.lo + i * width);
                    }),
                    datasets: [{
                        data: dist.bins,
                        backgroundColor: m.color,
                        borderRadius: 1,
                        borderSkipped: false
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                title: function (items) {
                                    var i = items[0].dataIndex;
                                    return _fmtMetric(dist.lo + i * width) + ' – '
                                        + _fmtMetric(dist.lo + (i + 1) * width);
                                },
                                label: function (item) {
                                    return item.parsed.y.toLocaleString() + ' nodes';
                                }
                            }
                        }
                    },
                    scales: {
                        x: { display: false, grid: { display: false } },
                        y: {
                            display: false,
                            type: _logScale ? 'logarithmic' : 'linear',
                            beginAtZero: !_logScale
                        }
                    }
                }
            });
        });
    }

    function _selectMetric(key) {
        if (!key) return;
        _selectedMetric = key;
        document.querySelectorAll('.analytics-dist-tile').forEach(function (el) {
            var on = el.getAttribute('data-metric') === key;
            el.classList.toggle('selected', on);
            el.setAttribute('aria-pressed', String(on));
        });
        _renderRankingChart();
    }
```

- [ ] **Step 4: Add the strip CSS**

Append to `query-analytics.css`:

```css
/* --- Dashboard: global distribution strip -------------------------------- */

.analytics-dist-strip {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 0.5rem;
    margin-bottom: 0.75rem;
}

@media (max-width: 1200px) {
    .analytics-dist-strip { grid-template-columns: repeat(2, 1fr); }
}

@media (max-width: 576px) {
    .analytics-dist-strip { grid-template-columns: 1fr; }
}

.analytics-dist-tile {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    width: 100%;
    padding: 0.4rem 0.5rem;
    text-align: left;
    background: #fff;
    border: 1px solid var(--db-border);
    border-radius: var(--bs-card-border-radius);
    transition: var(--db-transition-fast);
    cursor: pointer;
}

.analytics-dist-tile:hover {
    box-shadow: var(--db-shadow-sm);
}

.analytics-dist-tile.selected {
    border-color: var(--db-primary);
    box-shadow: var(--db-shadow-primary);
}

.analytics-dist-head {
    font-size: 0.72rem;
    font-weight: 600;
    color: var(--db-text);
}

.analytics-dist-head i {
    margin-right: 0.25rem;
}

.analytics-dist-badge {
    margin-left: 0.25rem;
    font-weight: 400;
    color: var(--db-warning);
}

/* Fixed height: Chart.js needs a sized box, and all five tiles must share a
   baseline for their shapes to be comparable. */
.analytics-dist-body {
    position: relative;
    display: block;
    height: 56px;
}

.analytics-dist-caption {
    font-size: 0.65rem;
    color: var(--db-text-muted);
}
```

Confirm `--db-warning` and `--db-shadow-primary` exist in `global/css/main.css` before using them; substitute a token that does exist if not.

- [ ] **Step 5: Call the renderer**

In `_renderAnalyticsData`, replace the `analyticsRenderCharts();` call with:

```javascript
        _renderDistributionStrip();
        analyticsRenderCharts();
```

- [ ] **Step 6: Run the tests**

Run: `uv run --frozen pytest tests/units/front/test_analytics_dashboard.py -q`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/front/static/query/js/query-analytics.js \
        src/front/static/query/css/query-analytics.css \
        tests/units/front/test_analytics_dashboard.py
git commit -m "Add the global distribution strip to the Analytics dashboard

Five small-multiple histograms over every scored node, so a top-ranked score
can finally be read against the population it came from."
```

---

## Task 8: The focused ranking card

**Files:**
- Modify: `src/front/static/query/js/query-analytics.js`
- Modify: `src/front/static/query/css/query-analytics.css`
- Test: `tests/units/front/test_analytics_dashboard.py`

**Interfaces:**
- Consumes: `#analyticsRankingCard` (Task 6), `_selectedMetric` / `_selectMetric` (Task 7)
- Produces: `_renderRankingChart()` replacing the old per-metric loop in `analyticsRenderCharts`; canvas id `analyticsRankingChart`; segmented control ids `rankSeg_<metric>`

- [ ] **Step 1: Write the failing test**

```python
class TestRankingCard:
    def test_one_ranking_chart_is_rendered_for_the_selected_metric(self, js):
        assert "function _renderRankingChart" in js
        assert "analyticsRankingChart" in js

    def test_the_segmented_control_offers_all_five_metrics(self, js):
        fn = _fn(js, "_renderRankingChart")
        assert "rankSeg_" in fn

    def test_clicking_a_segment_selects_that_metric(self, js):
        fn = _fn(js, "_renderRankingChart")
        assert "_selectMetric" in fn

    def test_click_through_to_the_graph_viewer_survives(self, js):
        fn = _fn(js, "_renderRankingChart")
        assert "_navigateToGraph" in fn

    def test_the_top_n_input_still_drives_the_chart(self, js):
        assert "analyticsTopN" in js

    def test_the_estimate_notice_survives(self, js):
        assert "Estimate." in js

    def test_the_all_zero_notice_survives(self, js):
        assert "All values are 0." in js

    def test_the_not_computed_notice_survives(self, js):
        assert "Not computed for this graph." in js

    def test_the_detail_table_is_still_rendered(self, js):
        assert "_renderPagerankTable" in js
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/units/front/test_analytics_dashboard.py -q -k RankingCard`
Expected: FAIL — `function _renderRankingChart` not found.

- [ ] **Step 3: Implement the ranking card**

Replace the body of `window.analyticsRenderCharts` (which looped over `_METRICS` building four charts) with a delegation, keeping the exported name because `_query_analytics`'s `Top N` input calls it via `onchange`:

```javascript
    window.analyticsRenderCharts = function () {
        if (!_analyticsData) return;
        _renderRankingChart();
        var topN = _topN();
        _renderPagerankTable(
            Object.keys(_analyticsData.nodes || {}),
            _analyticsData.nodes || {},
            _analyticsData.node_types || {},
            topN
        );
    };

    function _topN() {
        var el = document.getElementById('analyticsTopN');
        return Math.max(3, parseInt(el && el.value, 10) || 10);
    }
```

Then add the ranking renderer. It reuses the notice logic the per-metric charts had, now for one metric at a time:

```javascript
    function _renderRankingChart() {
        var host = document.getElementById('analyticsRankingCard');
        if (!host || !_analyticsData) return;

        var meta = _ALL_METRICS.filter(function (m) {
            return m.key === _selectedMetric;
        })[0] || _ALL_METRICS[0];

        var unavailable = _analyticsData.unavailable_metrics || [];
        var approximate = _analyticsData.approximate_metrics || [];
        var pivotCount = _analyticsData.pivot_count || 0;

        var segments = _ALL_METRICS.map(function (m) {
            return '<button type="button" class="analytics-rank-seg'
                + (m.key === _selectedMetric ? ' selected' : '') + '"'
                + ' id="rankSeg_' + m.key + '" data-metric="' + m.key + '"'
                + ' aria-pressed="' + (m.key === _selectedMetric) + '">'
                + '<i class="bi ' + m.icon + ' me-1"></i>' + m.label
                + '</button>';
        }).join('');

        host.innerHTML = ''
            + '<div class="card mt-2">'
            + '  <div class="card-header py-2 d-flex justify-content-between align-items-center flex-wrap gap-2">'
            + '    <span class="small fw-semibold">'
            + '      <i class="bi ' + meta.icon + ' me-1"></i>Top nodes by ' + meta.label
            + '      <button class="btn btn-link btn-sm p-0 text-muted ms-1"'
            + '              onclick="_showMetricInfo(\'' + meta.key + '\')"'
            + '              title="What is ' + meta.label + '?">'
            + '        <i class="bi bi-question-circle"></i></button>'
            + '    </span>'
            + '    <span class="analytics-rank-segs">' + segments + '</span>'
            + '  </div>'
            + '  <div class="card-body">'
            + '    <div id="analyticsRankingNotice"></div>'
            + '    <div class="analytics-rank-canvas-wrap">'
            + '      <canvas id="analyticsRankingChart"></canvas>'
            + '    </div>'
            + '  </div>'
            + '</div>';

        host.querySelectorAll('.analytics-rank-seg').forEach(function (el) {
            el.addEventListener('click', function () {
                _selectMetric(el.getAttribute('data-metric'));
            });
        });

        var notice = document.getElementById('analyticsRankingNotice');
        var canvas = document.getElementById('analyticsRankingChart');
        if (!canvas || !notice) return;

        var allNodes = _analyticsData.nodes || {};
        var sorted = Object.keys(allNodes).sort(function (a, b) {
            return (allNodes[b][meta.key] || 0) - (allNodes[a][meta.key] || 0);
        }).slice(0, _topN());
        var values = sorted.map(function (uri) {
            return +(allNodes[uri][meta.key] || 0).toFixed(6);
        });

        if (_charts.ranking) { _charts.ranking.destroy(); _charts.ranking = null; }

        // A flat zero chart would imply a measurement of zero. Explain instead.
        if (!values.length || values.every(function (v) { return v === 0; })) {
            canvas.style.display = 'none';
            notice.innerHTML = '<div class="alert alert-light border small text-muted mb-0">'
                + '<i class="bi bi-info-circle me-1"></i>'
                + _zeroReason(meta.key, unavailable) + '</div>';
            return;
        }
        canvas.style.display = '';

        notice.innerHTML = approximate.indexOf(meta.key) !== -1
            ? '<div class="alert alert-warning border small py-1 px-2 mb-2">'
              + '<i class="bi bi-exclamation-triangle me-1"></i><strong>Estimate.</strong> '
              + meta.label + ' is sampled from ' + pivotCount + ' source node'
              + (pivotCount === 1 ? '' : 's') + ' rather than all of them, because the '
              + 'exact computation is quadratic in the graph size. Use it to rank nodes, '
              + 'not as an absolute value — nodes with similar scores may be ordered '
              + 'wrongly. Analyse a single Entity Type for exact values.</div>'
            : '';

        var labels = sorted.map(_displayName);
        _charts.ranking = new Chart(canvas, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: meta.label,
                    data: values,
                    backgroundColor: meta.color,
                    borderRadius: 4,
                    borderSkipped: false
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                onClick: function (event, elements) {
                    if (!elements || !elements.length) return;
                    var uri = sorted[elements[0].index];
                    if (uri) _navigateToGraph(uri);
                },
                onHover: function (event) {
                    event.native.target.style.cursor =
                        event.chart.getElementsAtEventForMode(
                            event.native, 'nearest', { intersect: true }, true
                        ).length ? 'pointer' : 'default';
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: function (items) {
                                var uri = sorted[items[0].dataIndex];
                                var type = (_analyticsData.node_types || {})[uri];
                                return type ? uri + '  [' + _localName(type) + ']' : uri;
                            },
                            beforeBody: function (items) {
                                var nm = allNodes[sorted[items[0].dataIndex]] || {};
                                return _ALL_METRICS.map(function (m) {
                                    return m.label + ' : ' + (nm[m.key] || 0).toFixed(6);
                                }).concat(['──────────────────────────']);
                            },
                            label: function (item) {
                                return '► ' + item.dataset.label + ' : ' + item.formattedValue;
                            },
                            afterLabel: function () { return '\nClick to open in Graph Viewer'; }
                        }
                    }
                },
                scales: {
                    x: { beginAtZero: true, ticks: { font: { size: 11 } },
                         grid: { color: 'rgba(0,0,0,0.05)' } },
                    y: { ticks: { font: { size: 11 },
                                  callback: function (val, idx) {
                                      var l = labels[idx];
                                      return l.length > 40 ? l.slice(0, 39) + '…' : l;
                                  } },
                         grid: { display: false } }
                }
            }
        });
    }

    // Why a metric charts as all zeros. Kept as one function so the three
    // explanations cannot drift apart.
    function _zeroReason(key, unavailable) {
        var label = key.charAt(0).toUpperCase() + key.slice(1);
        if (unavailable.indexOf(key) !== -1) {
            return '<strong>Not computed for this graph.</strong> ' + label
                + ' is estimated from a sample of source nodes, and this run could '
                + 'not produce a sample it can stand behind — either no pivots were '
                + 'sampled or the breadth-first search hit its depth cap. Raise the '
                + 'analytics job\'s max depth in Settings and re-run.';
        }
        if (key === 'clustering') {
            return '<strong>All values are 0.</strong> Clustering coefficient is 0 '
                + 'when none of a node\'s neighbors are connected to each other — '
                + 'typical for KGs with a bipartite structure (e.g. Customer → Order '
                + '→ Product). Triangles are rare unless entities of the same type '
                + 'link directly.';
        }
        return '<strong>All values are 0.</strong> No ' + key + ' scores could be '
            + 'computed for the current graph / filter.';
    }
```

Delete the now-dead `_METRICS` array and the old per-canvas chart loop.

- [ ] **Step 4: Add the ranking CSS**

```css
/* --- Dashboard: ranking card -------------------------------------------- */

.analytics-rank-segs {
    display: inline-flex;
    flex-wrap: wrap;
    border: 1px solid var(--db-border);
    border-radius: var(--bs-border-radius);
    overflow: hidden;
}

.analytics-rank-seg {
    padding: 0.15rem 0.5rem;
    font-size: 0.72rem;
    color: var(--db-text-secondary);
    background: #fff;
    border: 0;
    border-right: 1px solid var(--db-border);
    transition: var(--db-transition-fast);
}

.analytics-rank-seg:last-child { border-right: 0; }

.analytics-rank-seg:hover { background: var(--db-light-gray); }

.analytics-rank-seg.selected {
    font-weight: 600;
    color: var(--db-text);
    background: var(--db-light-gray);
    box-shadow: inset 0 -2px 0 var(--db-primary);
}

/* Sized so a top-50 ranking scrolls inside the card rather than stretching it. */
.analytics-rank-canvas-wrap {
    position: relative;
    height: 420px;
}
```

- [ ] **Step 5: Run the tests**

Run: `uv run --frozen pytest tests/units/front/ -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/front/static/query/js/query-analytics.js \
        src/front/static/query/css/query-analytics.css \
        tests/units/front/test_analytics_dashboard.py
git commit -m "Add the focused ranking chart to the Analytics dashboard

One full-width top-N chart driven by the selected distribution tile, so long
node labels stay readable instead of being cramped into a grid cell."
```

---

## Task 9: Log-scale toggle

**Files:**
- Modify: `src/front/templates/partials/dtwin/_query_analytics.html` (section header)
- Modify: `src/front/static/query/js/query-analytics.js`
- Test: `tests/units/front/test_analytics_dashboard.py`

**Interfaces:**
- Consumes: `_logScale`, `_renderDistributionStrip` (Task 7)
- Produces: `#analyticsLogScale` checkbox; `window.analyticsToggleLogScale()`

- [ ] **Step 1: Write the failing test**

```python
class TestLogScaleToggle:
    def test_the_toggle_exists_in_the_section_header(self, panel):
        header = panel[: panel.index('id="analyticsSpinner"')]
        assert 'id="analyticsLogScale"' in header

    def test_the_toggle_is_labelled(self, panel):
        assert "Log scale" in panel

    def test_the_toggle_redraws_the_strip(self, js):
        fn = _fn(js, "analyticsToggleLogScale")
        assert "_renderDistributionStrip" in fn

    def test_it_defaults_to_linear(self, js):
        assert "_logScale = false" in js

    def test_the_axis_type_follows_the_flag(self, js):
        assert "'logarithmic'" in js
        assert "_logScale ? 'logarithmic' : 'linear'" in js

    def test_the_caption_states_when_log_is_active(self, js):
        """Bar heights stop being proportional to counts; an unlabelled log
        chart misleads."""
        fn = _fn(js, "_renderDistributionStrip")
        assert "_logScale" in fn
        assert "log" in fn
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/units/front/test_analytics_dashboard.py -q -k LogScale`
Expected: FAIL — `analyticsLogScale` not in the header.

- [ ] **Step 3: Add the control**

In the `.section-header` right-hand `<div>` of `_query_analytics.html`, **before** the `Run Analysis` button (utility controls come before CTAs):

```html
<div class="form-check form-switch form-check-inline mb-0 analytics-logscale-switch">
    <input class="form-check-input" type="checkbox" role="switch"
           id="analyticsLogScale" onchange="analyticsToggleLogScale()">
    <label class="form-check-label small text-muted" for="analyticsLogScale"
           title="Draw the distribution counts on a logarithmic axis — makes the heavy tail visible">
        Log scale
    </label>
</div>
```

- [ ] **Step 4: Add the handler**

```javascript
    // Applies to all five distribution tiles at once. Per-visit state: a
    // persisted axis choice would silently change how a colleague reads a
    // shared screenshot.
    window.analyticsToggleLogScale = function () {
        var el = document.getElementById('analyticsLogScale');
        _logScale = !!(el && el.checked);
        _renderDistributionStrip();
    };
```

- [ ] **Step 5: Add the CSS**

```css
.analytics-logscale-switch .form-check-label {
    cursor: pointer;
}
```

- [ ] **Step 6: Run the tests**

Run: `uv run --frozen pytest tests/units/front/ -q`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/front/templates/partials/dtwin/_query_analytics.html \
        src/front/static/query/js/query-analytics.js \
        src/front/static/query/css/query-analytics.css \
        tests/units/front/test_analytics_dashboard.py
git commit -m "Add a log-scale toggle for the distribution strip

KG centrality is heavily skewed, so a linear count axis collapses PageRank,
betweenness and clustering into a single bar. The caption states which axis
is active, since log bar heights are not proportional to counts."
```

---

## Task 10: Keep distributions out of the AI interpret payload

`analyticsInterpret` posts `Object.assign({}, _analyticsData, {...})` to `/dtwin/metrics/interpret`, and that body becomes `metrics_payload` for `agent_graph_interpreter`. Left alone, the new field silently changes an LLM prompt — which is an AI-feature change requiring the `.cursor/12-ai-feature-lifecycle.mdc` eval gate. Feeding distributions to the agent is explicitly out of scope for this work.

**Files:**
- Modify: `src/front/static/query/js/query-analytics.js`
- Test: `tests/units/front/test_analytics_dashboard.py`

**Interfaces:**
- Consumes: `_analyticsData.distributions` (Task 4)
- Produces: no new exports

- [ ] **Step 1: Write the failing test**

```python
class TestInterpretPayloadExcludesDistributions:
    """Sending them would change an LLM prompt, which needs the eval gate."""

    @staticmethod
    def _interpret(js: str) -> str:
        """The body of window.analyticsInterpret, which is an assigned
        expression rather than a declaration, so _fn does not apply."""
        start = js.index("window.analyticsInterpret")
        rest = js[start + 25:]
        end = rest.find("\n    window.")
        return rest if end == -1 else rest[:end]

    def test_distributions_are_deleted_from_the_interpret_body(self, js):
        assert "delete payload.distributions" in self._interpret(js)

    def test_the_deletion_happens_after_the_payload_is_built(self, js):
        """Deleting before the Object.assign would be a no-op that reads as
        protection."""
        body = self._interpret(js)
        assert body.index("Object.assign") < body.index("delete payload.distributions")

    def test_the_deletion_is_before_the_fetch(self, js):
        body = self._interpret(js)
        assert body.index("delete payload.distributions") < body.index("fetch(")

    def test_the_reason_is_recorded_at_the_deletion(self, js):
        """Without the reason, a later reader restores the field to 'give the
        agent more context' and trips the eval gate unknowingly."""
        body = self._interpret(js)
        near = body[body.index("delete payload.distributions") - 400:
                    body.index("delete payload.distributions")]
        assert "eval" in near.lower()
        assert "metrics_payload" in near or "prompt" in near.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest tests/units/front/test_analytics_dashboard.py -q -k Interpret`
Expected: FAIL — `delete payload.distributions` not found.

- [ ] **Step 3: Implement**

In `window.analyticsInterpret`, replace the payload construction:

```javascript
            var payload = Object.assign({}, _analyticsData, {
                class_filter: _getSelectedTypes()
            });
            // The whole body becomes the agent's metrics_payload, so adding a
            // field here changes an LLM prompt — which requires an eval delta
            // under .cursor/12-ai-feature-lifecycle.mdc. Distributions are a
            // presentation aggregate; they stay out until that gate is run.
            delete payload.distributions;
```

- [ ] **Step 4: Run the tests**

Run: `uv run --frozen pytest tests/units/front/ -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/front/static/query/js/query-analytics.js \
        tests/units/front/test_analytics_dashboard.py
git commit -m "Keep distributions out of the AI interpret payload

The interpret body is the agent's metrics_payload, so shipping a new field
there would change an LLM prompt without the required eval delta."
```

---

## Task 11: Manual verification, docs and changelog

**Files:**
- Modify: `documentation/features.md`, `documentation/user-guide.md` (Analytics section)
- Create: `changelogs/v0.7.0/benoitcayladbx_2026-08-04.log`

- [ ] **Step 1: Full test suite**

Run: `uv run --frozen pytest -q -m "not scenario"`
Expected: PASS. Record the exact pass/fail counts — they go in the changelog.

- [ ] **Step 2: Manual verification against a real graph**

Load `/dtwin/` → Knowledge Graph → Analytics and confirm:

1. A **legacy cached result** (one computed before this change) renders: KPI row, ranking chart and detail table all populate, and each distribution tile shows "Re-run the analysis to see the distribution". Nothing throws in the console.
2. **Run Analysis** completes and all five tiles draw histograms.
3. Clicking each tile switches the ranking chart, and the segmented control agrees with the tile selection.
4. **Log scale** visibly changes the heavy-tailed tiles and the caption says `log`.
5. With betweenness/closeness **unavailable** (set `ONTOBRICKS_ANALYTICS_JOB_PIVOTS=0` and re-run), their tiles show "Not computed for this run", their ranking charts show the depth-cap explanation, and the detail table dashes those columns.
6. Clicking a ranking bar still opens the Graph Viewer filtered on that node.
7. **Interpret** still returns sections, and the request body in the Network tab has **no** `distributions` key.
8. Data Model Health and AI Insights tabs are unchanged.

- [ ] **Step 3: Update the documentation**

Grep for the Analytics tab list and update it to the three-tab structure, describing the distribution strip:

```bash
grep -rn "PageRank" documentation/features.md documentation/user-guide.md
```

Describe what the distribution answers ("is this node's score unusual?"), that the median is an interpolated approximation, and that betweenness/closeness distributions are absent when the run could not compute them.

- [ ] **Step 4: Write the changelog**

Create `changelogs/v0.7.0/benoitcayladbx_2026-08-04.log`:

```
# KG Analytics dashboard with global metric distributions

## Context

Knowledge Graph -> Analytics charted only a bounded top-N slice per metric
across five sibling tabs. A reader could not tell whether a top-ranked score
was an outlier or typical, and could not compare two metrics without switching
tabs. The Lakeflow job already wrote per-node scores for the whole graph; the
app never summarised them.

Spec:  documentation/superpowers/specs/2026-08-04-kg-analytics-dashboard-design.md
Plan:  documentation/superpowers/plans/2026-08-04-kg-analytics-dashboard.md

## Changes

1. src/back/core/graph_analysis/models.py — added MetricDistribution and
   MetricsResult.distributions; DEFAULT_DISTRIBUTION_BINS = 20.
2. src/back/core/graph_analysis/JobMetrics.py — added
   distribution_bounds_query, distributions_query, interpolate_quantile and
   _read_distributions. SQL is portable across SQLite, Spark and Postgres; a
   failed histogram read no longer discards the run.
3. src/front/static/query/css/query-analytics.css — new; all Analytics styles.
4. src/front/static/query/js/query-analytics.js — new; the ~1100 lines of JS
   previously inline in the partial, plus the dashboard renderers.
5. src/front/templates/partials/dtwin/_query_analytics.html — markup only.
   Seven tabs became three (Dashboard, Data Model Health, AI Insights); KPI
   row rebuilt on the shared .ob-kpi-tile component.
6. src/front/templates/dtwin.html — wired the two new assets.
7. Distributions are stripped from the /dtwin/metrics/interpret body so the
   change does not alter an LLM prompt without an eval delta.
8. Repointed the three test files that asserted on JS inside the partial.

## Modified files

(fill from `git diff --name-only <first-commit-of-this-work>..HEAD`)

## Test result

(fill with the literal output line from Step 1, e.g.
 uv run --frozen pytest -q -m "not scenario"  ->  1284 passed, 3 skipped)
```

Both parenthesised notes are values to read off the actual run — do not commit
them as written.

- [ ] **Step 5: Commit**

```bash
git add documentation/ changelogs/
git commit -m "Document the Analytics dashboard and log the change"
```

---

## Notes for the implementer

- **`_charts` is shared state.** The old code keyed it by metric name for four per-tab charts. The dashboard uses `_charts.ranking` for the single ranking chart and a separate `_distCharts` map for the tiles. Always `destroy()` before replacing, or Chart.js leaks canvases and tooltips start reporting stale data.
- **Chart.js needs a sized parent.** Both `.analytics-dist-body` and `.analytics-rank-canvas-wrap` set an explicit height with `maintainAspectRatio: false`. Removing either collapses the canvas to 0px.
- **Never derive a node count from `len(nodes)` / `Object.keys(nodes).length`.** It is a bounded top-N slice. Use `stats.node_count`. The existing code carries this warning in two places; the distribution adds a third population to keep straight.
- **`aria-pressed` on toggle buttons** must be kept in sync in `_selectMetric`, or the strip is unreadable to a screen reader even though it looks fine.
- Before using any CSS token (`--db-warning`, `--db-shadow-primary`, `--bs-card-border-radius`), confirm it exists in `global/css/main.css` or Bootstrap. Do not invent tokens, and do not hard-code a hex fallback.
