"""Tests for the job-backed analytics mode: source resolution and read-back.

The Lakeflow job itself is verified against NetworkX in
``test_graph_analytics_job_sql.py``. What is covered here is the app-side half:
source resolution validation, the read-back SQL (run for real against SQLite),
and assembling the four job output tables into a MetricsResult without touching
the store.
"""

import sqlite3
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.core.graph_analysis.JobMetrics import (
    APPROXIMATE_METRICS,
    METRIC_SERIES_MAX_LIMIT,
    UNAVAILABLE_METRICS,
    JobMetrics,
    analytics_snapshot,
    distribution_bounds_query,
    distributions_query,
    interpolate_quantile,
    metric_series_query,
    resolve_analytics_source,
    summary_query,
    top_nodes_query,
    type_predicates_query,
    type_profiles_query,
)
from back.core.graph_analysis.models import (
    MODE_JOB,
    MetricsRequest,
    MetricsResult,
    NodeMetrics,
)

pytestmark = pytest.mark.unit

NS = "http://ex.org/"


# ---------------------------------------------------------------------------
# resolve_analytics_source
# ---------------------------------------------------------------------------


def test_analytics_source_is_the_data_table_whatever_the_engine(monkeypatch):
    """No engine branch: every backend resolves the same mapped snapshot."""
    from back.core.helpers.SQLHelpers import SQLHelpers

    monkeypatch.setattr(
        SQLHelpers, "effective_databricks_table",
        staticmethod(lambda domain, settings=None: "cat.sch.triplestore_dom_V3_data"),
    )
    table, reason = resolve_analytics_source(object(), object())
    assert table == "cat.sch.triplestore_dom_V3_data"
    assert reason == ""


def test_unqualified_source_is_refused_with_a_remedy(monkeypatch):
    """A half-configured domain must say what to fix, not fail obscurely."""
    from back.core.helpers.SQLHelpers import SQLHelpers

    monkeypatch.setattr(
        SQLHelpers, "effective_databricks_table",
        staticmethod(lambda domain, settings=None: "triplestore_dom_V3_data"),
    )
    table, reason = resolve_analytics_source(object(), object())
    assert table == ""
    assert "catalog.schema.table" in reason


def test_missing_source_points_at_the_build(monkeypatch):
    """The remedy for an unbuilt domain is a Build, not a support ticket."""
    from back.core.helpers.SQLHelpers import SQLHelpers

    monkeypatch.setattr(
        SQLHelpers, "effective_databricks_table",
        staticmethod(lambda domain, settings=None: ""),
    )
    table, reason = resolve_analytics_source(object(), object())
    assert table == ""
    assert "Build" in reason


def test_quoted_identifiers_are_accepted(monkeypatch):
    """Delta naming may hand back backticked parts; the job needs them bare."""
    from back.core.helpers.SQLHelpers import SQLHelpers

    monkeypatch.setattr(
        SQLHelpers, "effective_databricks_table",
        staticmethod(lambda domain, settings=None: " `cat`.`sch`.`tbl_data` "),
    )
    table, reason = resolve_analytics_source(object(), object())
    assert table == "cat.sch.tbl_data"
    assert reason == ""


# ---------------------------------------------------------------------------
# analytics_snapshot
# ---------------------------------------------------------------------------


def _view_only_domain():
    """A Lakehouse domain whose ``…_data`` is a pass-through view."""
    return SimpleNamespace(
        info={
            "name": "Dom",
            "graph_backend": "databricks",
            "lakehouse_materialization": "view",
        },
        current_version=3,
        delta={"catalog": "cat", "schema": "sch"},
    )


def test_a_materialized_domain_is_scanned_in_place():
    """Nothing to prepare, and nothing to clean up, when ..._data is a table."""
    domain = SimpleNamespace(info={"graph_backend": "databricks"})
    with analytics_snapshot(domain, None, "cat.sch.t_data") as table:
        assert table == "cat.sch.t_data"


def test_a_view_only_domain_gets_a_disposable_snapshot(monkeypatch):
    """The job scans its source repeatedly, which a view would re-derive each time."""
    statements: List[str] = []
    client = SimpleNamespace(execute_statement=statements.append)
    monkeypatch.setattr(
        "back.core.graphdb.delta.DeltaBase.create_databricks_client",
        lambda domain, settings=None: client,
    )

    with analytics_snapshot(_view_only_domain(), None, "cat.sch.t_data") as table:
        assert table == "cat.sch.triplestore_dom_V3_analytics"
        assert "CREATE OR REPLACE TABLE" in statements[0]
        assert "FROM cat.sch.t_data" in statements[0]
        assert len(statements) == 1

    assert statements[1] == (
        "DROP TABLE IF EXISTS cat.sch.triplestore_dom_V3_analytics"
    )


def test_the_snapshot_is_dropped_even_when_the_run_fails(monkeypatch):
    """A failed run must not leave storage behind for the next one to pay for."""
    statements: List[str] = []
    client = SimpleNamespace(execute_statement=statements.append)
    monkeypatch.setattr(
        "back.core.graphdb.delta.DeltaBase.create_databricks_client",
        lambda domain, settings=None: client,
    )

    with pytest.raises(RuntimeError, match="job died"):
        with analytics_snapshot(_view_only_domain(), None, "cat.sch.t_data"):
            raise RuntimeError("job died")

    assert any(s.startswith("DROP TABLE IF EXISTS") for s in statements)


def test_a_view_only_domain_without_a_warehouse_says_so(monkeypatch):
    """Silently scanning the view instead would make the run cost unbounded."""
    monkeypatch.setattr(
        "back.core.graphdb.delta.DeltaBase.create_databricks_client",
        lambda domain, settings=None: None,
    )

    with pytest.raises(InfrastructureError, match="temporary Delta snapshot"):
        with analytics_snapshot(_view_only_domain(), None, "cat.sch.t_data"):
            pass


# ---------------------------------------------------------------------------
# Read-back SQL, executed for real against SQLite
# ---------------------------------------------------------------------------


class _OutputDB:
    """SQLite holding fabricated job output tables: main, summary, profiles, predicates."""

    def __init__(
        self,
        rows: List[Dict[str, Any]],
        *,
        pivot_count: int = 8,
        bfs_complete: int = 1,
        total_node_count: int = 0,
    ):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            "CREATE TABLE metrics ("
            "node_uri TEXT, degree REAL, pagerank REAL, "
            "clustering REAL, betweenness REAL, closeness REAL, "
            "component_id TEXT, type_uri TEXT, label TEXT)"
        )
        self.conn.executemany(
            "INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    r["node_uri"],
                    r.get("degree", 0.0),
                    r.get("pagerank", 0.0),
                    r.get("clustering", 0.0),
                    r.get("betweenness", 0.0),
                    r.get("closeness", 0.0),
                    r.get("component_id", "c"),
                    r.get("type_uri", ""),
                    r.get("label", ""),
                )
                for r in rows
            ],
        )
        effective_total = total_node_count or len(rows)
        self.conn.execute(
            "CREATE TABLE metrics_summary ("
            "node_count INTEGER, total_node_count INTEGER, edge_count INTEGER, "
            "component_count INTEGER, components_converged INTEGER, "
            "pivot_count INTEGER, bfs_complete INTEGER)"
        )
        self.conn.execute(
            "INSERT INTO metrics_summary VALUES (?, ?, ?, ?, ?, ?, ?)",
            (len(rows), effective_total, 7, 2, 1, pivot_count, bfs_complete),
        )
        self.conn.execute(
            "CREATE TABLE metrics_type_profiles ("
            "type_uri TEXT, instance_count INTEGER, connected_count INTEGER, "
            "degree_sum INTEGER, avg_clustering REAL, avg_betweenness REAL)"
        )
        self.conn.execute(
            "CREATE TABLE metrics_type_predicates (type_uri TEXT, predicate TEXT)"
        )
        self.conn.commit()

    def query(self, sql: str) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(sql).fetchall()]


def _sample_rows() -> List[Dict[str, Any]]:
    """Five nodes with deliberately different pagerank and clustering orders."""
    return [
        {"node_uri": f"{NS}A", "degree": 0.9, "pagerank": 0.50, "clustering": 0.10},
        {"node_uri": f"{NS}B", "degree": 0.7, "pagerank": 0.20, "clustering": 0.90},
        {"node_uri": f"{NS}C", "degree": 0.5, "pagerank": 0.15, "clustering": 0.80},
        {"node_uri": f"{NS}D", "degree": 0.3, "pagerank": 0.10, "clustering": 0.05},
        {"node_uri": f"{NS}E", "degree": 0.1, "pagerank": 0.05, "clustering": 0.00},
    ]


class TestReadBackSql:
    def _db(self) -> _OutputDB:
        return _OutputDB(_sample_rows())

    def test_summary_query_reads_the_run_row(self):
        rows = self._db().query(summary_query("metrics"))
        assert rows[0]["component_count"] == 2
        assert rows[0]["node_count"] == 5
        assert rows[0]["total_node_count"] == 5

    def test_top_nodes_unions_all_rankings(self):
        # top-2 by degree == A, B; top-2 by pagerank == A, B; top-2 by clustering == B, C.
        rows = self._db().query(top_nodes_query("metrics", 2))
        assert {r["node_uri"] for r in rows} == {f"{NS}A", f"{NS}B", f"{NS}C"}

    def test_top_nodes_is_bounded_not_exhaustive(self):
        rows = self._db().query(top_nodes_query("metrics", 1))
        assert len(rows) < len(_sample_rows())

    def test_top_nodes_ordered_by_pagerank_desc(self):
        rows = self._db().query(top_nodes_query("metrics", 5))
        ranks = [r["pagerank"] for r in rows]
        assert ranks == sorted(ranks, reverse=True)

    def test_metric_series_query_has_the_validated_sql_contract(self):
        sql = metric_series_query("cat.sch.metrics", "pagerank", 25, 10)
        assert "pagerank AS score" in sql
        assert "COUNT(*) OVER() AS total_count" in sql
        assert "ORDER BY pagerank DESC, node_uri ASC" in sql
        assert "LIMIT 10 OFFSET 25" in sql

    @pytest.mark.parametrize(
        "metric", ["pagerank", "betweenness", "degree", "closeness", "clustering"]
    )
    def test_metric_series_query_accepts_all_allowed_metrics(self, metric):
        sql = metric_series_query("cat.sch.metrics", metric, 0, 100)
        assert f"{metric} AS score" in sql

    def test_metric_series_query_rejects_unknown_metric(self):
        with pytest.raises(ValidationError, match="Unsupported graph metric"):
            metric_series_query("cat.sch.metrics", "drop table metrics", 0, 100)

    def test_metric_series_query_clamps_limit_to_shared_max(self):
        sql = metric_series_query(
            "cat.sch.metrics",
            "pagerank",
            0,
            METRIC_SERIES_MAX_LIMIT + 99,
        )
        assert f"LIMIT {METRIC_SERIES_MAX_LIMIT} OFFSET 0" in sql

    def test_type_profiles_query_reads_the_rollup_table(self):
        db = _OutputDB(_sample_rows())
        db.conn.execute(
            "INSERT INTO metrics_type_profiles VALUES (?, ?, ?, ?, ?, ?)",
            ("http://ex/Person", 10, 8, 16, 0.25, 0.05),
        )
        db.conn.commit()
        rows = db.query(type_profiles_query("metrics"))
        assert len(rows) == 1
        assert rows[0]["type_uri"] == "http://ex/Person"
        assert rows[0]["instance_count"] == 10

    def test_type_predicates_query_reads_the_pairs_table(self):
        db = _OutputDB(_sample_rows())
        db.conn.execute(
            "INSERT INTO metrics_type_predicates VALUES (?, ?)",
            ("http://ex/Person", "http://ex/knows"),
        )
        db.conn.commit()
        rows = db.query(type_predicates_query("metrics"))
        assert rows[0]["type_uri"] == "http://ex/Person"
        assert rows[0]["predicate"] == "http://ex/knows"

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


# ---------------------------------------------------------------------------
# JobMetrics assembly — fake-query helpers
# ---------------------------------------------------------------------------


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


def _default_summary_row(**overrides):
    base = {
        "node_count": 5, "total_node_count": 5, "edge_count": 7,
        "component_count": 2, "components_converged": True,
        "pivot_count": 8, "bfs_complete": True,
    }
    base.update(overrides)
    return base


class _FakeRunner:
    def __init__(self, *, success: bool = True) -> None:
        self.success = success
        self.calls: List[Dict[str, Any]] = []
        self.last_kwargs: Dict[str, Any] = {}

    def run_and_wait(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(kwargs)
        self.last_kwargs = kwargs
        if kwargs.get("on_progress"):
            kwargs["on_progress"](45, "Computing graph metrics on Databricks")
        return {
            "success": self.success,
            "life_cycle_state": "TERMINATED",
            "result_state": "SUCCESS" if self.success else "FAILED",
            "message": "" if self.success else "boom",
            "run_page_url": "https://example/run/1",
            "run_id": 1,
        }


def _job_metrics(
    *,
    query=None,
    runner=None,
    top_n: int = 100,
    pivots: int = 64,
    max_depth: int = 32,
    distribution_bins: int = 4,  # matches the 4-bin fake rows the distribution fixtures build
) -> JobMetrics:
    if runner is None:
        runner = _FakeRunner()
    if query is None:
        query = _query_for({
            "summary": [_default_summary_row()],
            "type_profiles": [],
            "type_predicates": [],
            "": [],
        })
    return JobMetrics(
        "main.onto.triples",
        runner=runner,
        query=query,
        output_table="metrics",
        top_n=top_n,
        pivots=pivots,
        max_depth=max_depth,
        distribution_bins=distribution_bins,
    )


def _job_metrics_with_runner():
    runner = _FakeRunner()
    return _job_metrics(runner=runner), runner


# ---------------------------------------------------------------------------
# New assembly tests (from brief)
# ---------------------------------------------------------------------------


def test_stats_come_from_the_job_summary():
    """node_count and graph_node_count are distinct numbers, both from SQL."""
    query = _query_for({
        "summary": [{
            "node_count": 4, "total_node_count": 7, "edge_count": 3,
            "component_count": 2, "components_converged": True,
            "pivot_count": 64, "bfs_complete": True,
        }],
        "type_profiles": [],
        "type_predicates": [],
        "": [],
    })
    metrics = _job_metrics(query=query)
    result = metrics.compute(MetricsRequest())

    assert result.mode == MODE_JOB
    assert result.stats.node_count == 7
    assert result.stats.graph_node_count == 4
    assert result.stats.edge_count == 3
    assert result.stats.connected_components == 2
    # 2 * 3 / 4
    assert result.stats.avg_degree == pytest.approx(1.5)
    # 2 * 3 / (4 * 3)
    assert result.stats.density == pytest.approx(0.5)


def test_type_profiles_are_assembled_and_labelled():
    """The job supplies the numbers; profiles.py supplies the 'flat' verdict."""
    query = _query_for({
        "summary": [{"node_count": 2, "total_node_count": 30, "edge_count": 1,
                     "component_count": 1, "components_converged": True,
                     "pivot_count": 0, "bfs_complete": True}],
        "type_profiles": [{
            "type_uri": "http://ex/Reading", "instance_count": 25,
            "connected_count": 25, "degree_sum": 25,
            "avg_clustering": 0.0, "avg_betweenness": 0.0,
        }],
        "type_predicates": [
            {"type_uri": "http://ex/Reading", "predicate": "http://ex/sensor"},
        ],
        "": [],
    })
    result = _job_metrics(query=query).compute(MetricsRequest())

    profile = result.entity_type_profiles["http://ex/Reading"]
    assert profile.count == 25
    assert profile.distinct_predicates == 1
    # One predicate across 25 instances is the flat-dataset signal.
    assert profile.is_flat is True
    assert "only 1 distinct relationship predicate" in profile.flat_reasons[0]


def test_nodes_carry_type_and_label_from_the_job():
    query = _query_for({
        "summary": [{"node_count": 1, "total_node_count": 1, "edge_count": 0,
                     "component_count": 1, "components_converged": True,
                     "pivot_count": 8, "bfs_complete": True}],
        "type_profiles": [],
        "type_predicates": [],
        "": [{
            "node_uri": "http://ex/a", "degree": 0.5, "pagerank": 0.25,
            "clustering": 0.0, "betweenness": 0.1, "closeness": 0.4,
            "component_id": 1, "type_uri": "http://ex/Person", "label": "Alice",
        }],
    })
    result = _job_metrics(query=query).compute(MetricsRequest())

    assert result.node_types == {"http://ex/a": "http://ex/Person"}
    assert result.node_labels == {"http://ex/a": "Alice"}
    assert result.nodes["http://ex/a"].degree == 0.5
    assert result.nodes["http://ex/a"].betweenness == pytest.approx(0.1)


def test_class_and_predicate_filters_reach_the_runner():
    """Both filters are the job's business now, not the app's."""
    metrics, runner = _job_metrics_with_runner()
    metrics.compute(MetricsRequest(
        class_filter=["http://ex/Person"],
        predicate_filter=["http://ex/noisy"],
    ))
    assert runner.last_kwargs["class_filter"] == ["http://ex/Person"]
    assert runner.last_kwargs["exclude_predicates"] == ["http://ex/noisy"]


# ---------------------------------------------------------------------------
# Kept tests — updated to new constructor
# ---------------------------------------------------------------------------


def test_full_compute_reports_job_mode_with_nothing_unavailable():
    result = _job_metrics().compute(MetricsRequest())
    assert result.mode == MODE_JOB
    assert result.unavailable_metrics == []
    assert set(result.approximate_metrics) == set(APPROXIMATE_METRICS)
    assert result.pivot_count == 8
    assert result.stats.connected_components == 2


def test_estimates_are_withheld_when_no_pivots_were_sampled():
    query = _query_for({
        "summary": [_default_summary_row(pivot_count=0)],
        "type_profiles": [], "type_predicates": [], "": [],
    })
    result = _job_metrics(query=query).compute(MetricsRequest())
    assert set(result.unavailable_metrics) == set(UNAVAILABLE_METRICS)
    assert result.approximate_metrics == []
    assert result.pivot_count == 0


def test_estimates_are_withheld_when_the_bfs_was_truncated():
    # A truncated BFS biases the distance sums, so the numbers must not be
    # published as estimates — a wrong number is worse than a missing one.
    query = _query_for({
        "summary": [_default_summary_row(bfs_complete=False)],
        "type_profiles": [], "type_predicates": [], "": [],
    })
    result = _job_metrics(query=query).compute(MetricsRequest())
    assert set(result.unavailable_metrics) == set(UNAVAILABLE_METRICS)
    assert result.approximate_metrics == []


def test_withheld_estimates_are_not_written_onto_nodes():
    query = _query_for({
        "summary": [_default_summary_row(pivot_count=0)],
        "type_profiles": [], "type_predicates": [],
        "": [{"node_uri": "http://ex/a", "degree": 0.5, "pagerank": 0.25,
               "clustering": 0.0, "betweenness": 0.9, "closeness": 0.9,
               "component_id": 1, "type_uri": "", "label": ""}],
    })
    result = _job_metrics(query=query).compute(MetricsRequest())
    assert all(n.betweenness == 0.0 for n in result.nodes.values())
    assert all(n.closeness == 0.0 for n in result.nodes.values())


def test_estimates_land_on_nodes_when_available():
    query = _query_for({
        "summary": [_default_summary_row()],
        "type_profiles": [], "type_predicates": [],
        "": [{"node_uri": "http://ex/a", "degree": 0.5, "pagerank": 0.25,
               "clustering": 0.0, "betweenness": 0.42, "closeness": 0.77,
               "component_id": 1, "type_uri": "", "label": ""}],
    })
    result = _job_metrics(query=query).compute(MetricsRequest())
    node = result.nodes["http://ex/a"]
    assert node.betweenness == pytest.approx(0.42)
    assert node.closeness == pytest.approx(0.77)


def test_pivot_count_reaches_the_runner():
    runner = _FakeRunner()
    _job_metrics(runner=runner, pivots=32).compute(MetricsRequest())
    assert runner.calls[0]["pivots"] == 32


def test_max_depth_reaches_the_runner():
    # A depth cap that never leaves the app would strand betweenness and
    # closeness as unavailable with no way to raise it.
    runner = _FakeRunner()
    _job_metrics(runner=runner, max_depth=48).compute(MetricsRequest())
    assert runner.calls[0]["max_depth"] == 48


def test_progress_is_reported():
    seen: List[tuple] = []
    _job_metrics().compute(
        MetricsRequest(), on_progress=lambda p, m: seen.append((p, m))
    )
    assert seen
    assert all(0 <= p <= 100 for p, _ in seen)


def test_source_and_output_table_reach_the_runner():
    runner = _FakeRunner()
    _job_metrics(runner=runner).compute(MetricsRequest())
    assert runner.calls[0]["source_table"] == "main.onto.triples"
    assert runner.calls[0]["output_table"] == "metrics"


def test_output_table_is_sanitised_for_unquoted_sql():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin
    from jobs.graph_analytics_job import validate_identifier

    class _Domain:
        uc_domain_folder = "My Domain/v2 (draft)"
        current_version = "1.0.0"

    table = DigitalTwin.analytics_output_table(
        "main.onto", _Domain(), "graph"
    )
    # Must survive the job's own identifier guard, since it is interpolated
    # into generated SQL without quoting.
    assert validate_identifier(table, "output") == table
    assert table.startswith("main.onto.graph_metrics_")


def test_output_table_is_stable_per_version():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    class _Domain:
        uc_domain_folder = "sales"
        current_version = "1.2.0"

    first = DigitalTwin.analytics_output_table("c.s", _Domain(), "g")
    second = DigitalTwin.analytics_output_table("c.s", _Domain(), "g")
    assert first == second == "c.s.graph_metrics_sales_1_2_0"


def test_output_table_falls_back_when_domain_is_empty():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin
    from jobs.graph_analytics_job import validate_identifier

    class _Domain:
        uc_domain_folder = ""
        current_version = ""

    table = DigitalTwin.analytics_output_table("c.s", _Domain(), "mygraph")
    assert validate_identifier(table, "output") == table


def test_failed_run_raises_with_the_run_url():
    jm = _job_metrics(runner=_FakeRunner(success=False))
    with pytest.raises(InfrastructureError) as exc:
        jm.compute(MetricsRequest())
    assert "example/run/1" in str(exc.value.detail)


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


def test_bins_are_padded_to_the_configured_count_not_the_observed_one():
    """The SQL emits no row for an empty bin, so a payload whose top bins are
    all empty must still be bin_count long. Deriving the length from the highest
    bin index seen would make the payload's shape depend on the data."""
    rows = _bin_rows("pagerank", [5, 3])  # only bins 0 and 1 come back
    result = _metrics_with_distributions(bin_rows=rows).compute(MetricsRequest())
    dist = result.distributions["pagerank"]
    assert dist.bins == [5, 3, 0, 0]
    assert dist.bin_count == 4


def test_an_all_identical_metric_still_reports_the_full_bin_count():
    """hi == lo puts every node in bin 0. That is one *populated* bin out of
    bin_count, not a distribution with one bin."""
    rows = _bin_rows("pagerank", [7])
    bounds = _bounds_row(lo_pagerank=0.25, hi_pagerank=0.25, mean_pagerank=0.25)
    result = _metrics_with_distributions(
        bin_rows=rows, bounds=bounds
    ).compute(MetricsRequest())
    dist = result.distributions["pagerank"]
    assert dist.bins == [7, 0, 0, 0]
    assert dist.bin_count == 4
    assert dist.median == pytest.approx(0.25)


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
