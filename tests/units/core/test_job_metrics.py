"""Tests for the job-backed analytics mode: source resolution and read-back.

The Lakeflow job itself is verified against NetworkX in
``test_graph_analytics_job_sql.py``. What is covered here is the app-side half:
source resolution validation, the read-back SQL (run for real against SQLite),
and assembling the four job output tables into a MetricsResult without touching
the store.
"""

import sqlite3
from typing import Any, Dict, List

import pytest

from back.core.errors import InfrastructureError
from back.core.graph_analysis.JobMetrics import (
    APPROXIMATE_METRICS,
    UNAVAILABLE_METRICS,
    JobMetrics,
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

    @pytest.mark.parametrize("dialect", ["databricks", "spark", "postgres"])
    def test_read_back_sql_parses_in_both_dialects(self, dialect):
        sqlglot = pytest.importorskip("sqlglot")
        for sql in (
            summary_query("cat.sch.metrics"),
            top_nodes_query("cat.sch.metrics", 50),
            type_profiles_query("cat.sch.metrics"),
            type_predicates_query("cat.sch.metrics"),
        ):
            try:
                sqlglot.parse_one(sql, dialect=dialect)
            except Exception as exc:  # noqa: BLE001
                pytest.fail(f"{dialect} rejected:\n{sql}\n{exc}")


# ---------------------------------------------------------------------------
# JobMetrics assembly — fake-query helpers
# ---------------------------------------------------------------------------


def _query_for(rows_by_table: Dict[str, List[Dict[str, Any]]]):
    """A fake warehouse query that dispatches on the table suffix being read."""
    def query(sql: str) -> List[Dict[str, Any]]:
        for suffix, rows in rows_by_table.items():
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
