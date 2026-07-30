"""Tests for the job-backed analytics mode: source resolution, read-back, merge.

The Lakeflow job itself is verified against NetworkX in
``test_graph_analytics_job_sql.py``. What is covered here is the app-side half:
deciding whether a graph is even reachable from Spark, the read-back SQL (run
for real against SQLite), and merging the job's per-node scores onto the
pushdown base result without breaking the bounded-payload contract.
"""

import sqlite3
from typing import Any, Dict, List, Optional

import pytest

from back.core.errors import InfrastructureError
from back.core.graph_analysis.JobMetrics import (
    UNAVAILABLE_METRICS,
    JobMetrics,
    metrics_for_nodes_query,
    resolve_spark_source,
    summary_query,
    top_nodes_query,
    type_clustering_query,
)
from back.core.graph_analysis.models import (
    MODE_JOB,
    EntityTypeProfile,
    MetricsRequest,
    MetricsResult,
    MetricsStats,
    NodeMetrics,
)

# Reuses the SQLite-backed store and the hub fixture from the pushdown suite
# rather than re-creating a second SQL harness.
from tests.units.core.test_pushdown_metrics import (
    CUSTOMER,
    NS,
    SqliteStore,
    _hub_triples,
)

pytestmark = pytest.mark.unit

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


# ---------------------------------------------------------------------------
# Source resolution
# ---------------------------------------------------------------------------


class _DeltaLikeStore:
    query_dialect = "sql"

    def __init__(self, relation: str) -> None:
        self._relation = relation

    def sql_table_reference(self, name: str) -> str:
        return self._relation


class _CypherStore:
    query_dialect = "cypher"


class _LakebaseStore:
    """Minimal stand-in exposing only what source resolution looks at."""

    query_dialect = "sql"

    def __init__(self, *, synced: bool, source: Optional[Any] = "") -> None:
        self.sync_mode = "managed_synced" if synced else "app_managed"
        self.is_synced = synced
        self._source = source

    def synced_uc_name(self, name: str) -> str:
        return f"cat.sch.{name}_sync"

    def synced_manager(self) -> Any:
        store = self

        class _Manager:
            def get(self, name: str) -> Any:
                return store._source

        return _Manager()


class _Spec:
    def __init__(self, source_table_full_name: str) -> None:
        self.source_table_full_name = source_table_full_name


class _Synced:
    def __init__(self, source: str) -> None:
        self.spec = _Spec(source)


class TestResolveSparkSource:
    def test_delta_table_resolves_to_itself(self):
        store = _DeltaLikeStore("main.onto.demo_graph")
        table, reason = resolve_spark_source(store, "demo_graph")
        assert table == "main.onto.demo_graph"
        assert reason == ""

    def test_backticks_are_stripped(self):
        store = _DeltaLikeStore("`main`.`onto`.`demo_graph`")
        table, _ = resolve_spark_source(store, "demo_graph")
        assert table == "main.onto.demo_graph"

    def test_unqualified_name_is_refused(self):
        store = _DeltaLikeStore("demo_graph")
        table, reason = resolve_spark_source(store, "demo_graph")
        assert table == ""
        assert "catalog.schema.table" in reason

    def test_no_store_is_refused(self):
        table, reason = resolve_spark_source(None, "g")
        assert table == ""
        assert reason

    def test_cypher_engine_is_refused_with_a_workaround(self):
        table, reason = resolve_spark_source(_CypherStore(), "g")
        assert table == ""
        # The message has to tell the user what to do instead.
        assert "entity-type filter" in reason

    def test_app_managed_lakebase_is_refused(self):
        table, reason = resolve_spark_source(_LakebaseStore(synced=False), "g")
        assert table == ""
        assert "managed_synced" in reason

    def test_managed_synced_lakebase_resolves_to_uc_source(self):
        store = _LakebaseStore(synced=True, source=_Synced("main.onto.demo_graph"))
        table, reason = resolve_spark_source(store, "demo_graph")
        assert table == "main.onto.demo_graph"
        assert reason == ""

    def test_managed_synced_dict_shape_also_works(self):
        store = _LakebaseStore(
            synced=True,
            source={"spec": {"source_table_full_name": "main.onto.g"}},
        )
        table, _ = resolve_spark_source(store, "g")
        assert table == "main.onto.g"

    def test_missing_synced_table_is_refused(self):
        store = _LakebaseStore(synced=True, source=None)
        table, reason = resolve_spark_source(store, "g")
        assert table == ""
        assert "Rebuild the graph" in reason

    def test_synced_manager_failure_is_refused_not_raised(self):
        class _Broken(_LakebaseStore):
            def synced_manager(self):
                raise RuntimeError("no manager wired")

        table, reason = resolve_spark_source(_Broken(synced=True), "g")
        assert table == ""
        assert reason


# ---------------------------------------------------------------------------
# Read-back SQL, executed for real
# ---------------------------------------------------------------------------


class _OutputDB:
    """SQLite holding a fabricated job output table plus the source triples."""

    def __init__(self, rows: List[Dict[str, Any]], triples: List[Dict[str, str]]):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            "CREATE TABLE metrics (node_uri TEXT, degree REAL, pagerank REAL, "
            "clustering REAL, component_id TEXT, degree_raw INTEGER)"
        )
        self.conn.executemany(
            "INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    r["node_uri"],
                    r.get("degree", 0.0),
                    r.get("pagerank", 0.0),
                    r.get("clustering", 0.0),
                    r.get("component_id", "c"),
                    r.get("degree_raw", 0),
                )
                for r in rows
            ],
        )
        self.conn.execute(
            "CREATE TABLE metrics_summary (node_count INTEGER, edge_count INTEGER, "
            "component_count INTEGER, components_converged INTEGER)"
        )
        self.conn.execute(
            "INSERT INTO metrics_summary VALUES (?, ?, ?, ?)",
            (len(rows), 7, 2, 1),
        )
        self.conn.execute(
            "CREATE TABLE triples (subject TEXT, predicate TEXT, object TEXT)"
        )
        self.conn.executemany(
            "INSERT INTO triples VALUES (?, ?, ?)",
            [(t["subject"], t["predicate"], t["object"]) for t in triples],
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
        return _OutputDB(_sample_rows(), _hub_triples())

    def test_summary_query_reads_the_run_row(self):
        rows = self._db().query(summary_query("metrics"))
        assert rows[0]["component_count"] == 2
        assert rows[0]["node_count"] == 5

    def test_top_nodes_unions_both_rankings(self):
        # top-2 by pagerank == A, B; top-2 by clustering == B, C.
        rows = self._db().query(top_nodes_query("metrics", 2))
        assert {r["node_uri"] for r in rows} == {f"{NS}A", f"{NS}B", f"{NS}C"}

    def test_top_nodes_is_bounded_not_exhaustive(self):
        rows = self._db().query(top_nodes_query("metrics", 1))
        assert len(rows) < len(_sample_rows())

    def test_top_nodes_ordered_by_pagerank_desc(self):
        rows = self._db().query(top_nodes_query("metrics", 5))
        ranks = [r["pagerank"] for r in rows]
        assert ranks == sorted(ranks, reverse=True)

    def test_metrics_for_nodes_selects_only_requested(self):
        rows = self._db().query(
            metrics_for_nodes_query("metrics", [f"{NS}D", f"{NS}E"])
        )
        assert {r["node_uri"] for r in rows} == {f"{NS}D", f"{NS}E"}

    def test_metrics_for_nodes_escapes_quotes(self):
        sql = metrics_for_nodes_query("metrics", ["http://ex.org/o'brien"])
        assert "o''brien" in sql
        # And is still executable.
        assert self._db().query(sql) == []

    def test_type_clustering_averages_per_type(self):
        # Give two Customers a known clustering, then check the mean.
        rows = [
            {"node_uri": f"{NS}C0", "clustering": 0.4},
            {"node_uri": f"{NS}C1", "clustering": 0.6},
        ]
        db = _OutputDB(rows, _hub_triples())
        out = {
            r["type_uri"]: r for r in db.query(type_clustering_query("metrics", "triples"))
        }
        assert out[CUSTOMER]["avg_clustering"] == pytest.approx(0.5)
        assert out[CUSTOMER]["instance_count"] == 2

    @pytest.mark.parametrize("dialect", ["databricks", "spark", "postgres"])
    def test_read_back_sql_parses_in_both_dialects(self, dialect):
        sqlglot = pytest.importorskip("sqlglot")
        for sql in (
            summary_query("cat.sch.metrics"),
            top_nodes_query("cat.sch.metrics", 50),
            metrics_for_nodes_query("cat.sch.metrics", ["http://ex.org/a"]),
            type_clustering_query("cat.sch.metrics", "cat.sch.triples"),
        ):
            try:
                sqlglot.parse_one(sql, dialect=dialect)
            except Exception as exc:  # noqa: BLE001
                pytest.fail(f"{dialect} rejected:\n{sql}\n{exc}")


# ---------------------------------------------------------------------------
# Merge behaviour
# ---------------------------------------------------------------------------


class _FakeRunner:
    def __init__(self, *, success: bool = True) -> None:
        self.success = success
        self.calls: List[Dict[str, Any]] = []

    def run_and_wait(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(kwargs)
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


class _QualifiedSqliteStore(SqliteStore):
    """A SQLite store that reports a Unity-Catalog-shaped name.

    Source resolution requires a ``catalog.schema.table`` identifier, while the
    analytics SQL must keep hitting the plain SQLite table. Only
    ``sql_table_reference`` is overridden, so the two stay independent — which
    is exactly the split that exists in production for a synced Lakebase graph.
    """

    def sql_table_reference(self, graph_name: str) -> str:
        return f"main.onto.{graph_name}"


def _job_metrics(
    db: _OutputDB, runner: _FakeRunner, *, top_n: int = 100
) -> JobMetrics:
    return JobMetrics(
        _QualifiedSqliteStore(_hub_triples()),
        "triples",
        runner=runner,
        query=db.query,
        output_table="metrics",
        top_n=top_n,
    )


class TestMergeJobResults:
    def _base(self) -> MetricsResult:
        """A pushdown-shaped base: degree only, no components, one profile."""
        return MetricsResult(
            nodes={f"{NS}D": NodeMetrics(degree=0.3)},
            stats=MetricsStats(
                node_count=5, graph_node_count=5, edge_count=7, connected_components=None
            ),
            entity_type_profiles={
                CUSTOMER: EntityTypeProfile(
                    uri=CUSTOMER,
                    count=2,
                    avg_degree=0.5,
                    avg_clustering=0.0,
                    avg_betweenness=0.0,
                    distinct_predicates=2,
                    has_temporal_predicates=False,
                    is_flat=False,
                )
            },
        )

    def test_component_count_is_filled_in(self):
        db = _OutputDB(_sample_rows(), _hub_triples())
        jm = _job_metrics(db, _FakeRunner())
        base = self._base()
        jm._merge_job_results(base, "triples")
        assert base.stats.connected_components == 2

    def test_pagerank_and_clustering_land_on_nodes(self):
        db = _OutputDB(_sample_rows(), _hub_triples())
        jm = _job_metrics(db, _FakeRunner())
        base = self._base()
        jm._merge_job_results(base, "triples")
        assert base.nodes[f"{NS}A"].pagerank == pytest.approx(0.50)
        assert base.nodes[f"{NS}B"].clustering == pytest.approx(0.90)

    def test_degree_ranked_node_keeps_its_degree_and_gains_pagerank(self):
        # D was selected by the pushdown pass on degree and is not in the
        # job's top-2 by either metric, so it must be back-filled rather than
        # left charted as a zero.
        db = _OutputDB(_sample_rows(), _hub_triples())
        jm = _job_metrics(db, _FakeRunner(), top_n=2)
        base = self._base()
        jm._merge_job_results(base, "triples")
        assert base.nodes[f"{NS}D"].degree == pytest.approx(0.3)
        assert base.nodes[f"{NS}D"].pagerank == pytest.approx(0.10)

    def test_top_pagerank_is_ordered_and_bounded(self):
        db = _OutputDB(_sample_rows(), _hub_triples())
        jm = _job_metrics(db, _FakeRunner(), top_n=3)
        base = self._base()
        jm._merge_job_results(base, "triples")
        assert base.top_pagerank[0] == f"{NS}A"
        assert len(base.top_pagerank) <= 3

    def test_per_type_clustering_is_filled_in(self):
        rows = [
            {"node_uri": f"{NS}C0", "clustering": 0.4},
            {"node_uri": f"{NS}C1", "clustering": 0.6},
        ]
        db = _OutputDB(rows, _hub_triples())
        jm = _job_metrics(db, _FakeRunner())
        base = self._base()
        jm._merge_job_results(base, "triples")
        assert base.entity_type_profiles[CUSTOMER].avg_clustering == pytest.approx(0.5)

    def test_per_type_clustering_failure_does_not_fail_the_run(self):
        db = _OutputDB(_sample_rows(), _hub_triples())
        jm = _job_metrics(db, _FakeRunner())
        base = self._base()
        # Point the rollup at a table that does not exist.
        jm._merge_job_results(base, "no_such_table")
        # The per-node merge still happened.
        assert base.nodes[f"{NS}A"].pagerank == pytest.approx(0.50)


class TestJobMetricsCompute:
    def test_full_compute_reports_job_mode_and_narrow_unavailable_set(self):
        db = _OutputDB(_sample_rows(), _hub_triples())
        runner = _FakeRunner()
        result = _job_metrics(db, runner).compute(MetricsRequest())
        assert result.mode == MODE_JOB
        # Only betweenness/closeness remain — pagerank, clustering and the
        # component count now come from the job.
        assert set(result.unavailable_metrics) == set(UNAVAILABLE_METRICS)
        assert "pagerank" not in result.unavailable_metrics
        assert result.stats.connected_components == 2

    def test_progress_is_reported(self):
        db = _OutputDB(_sample_rows(), _hub_triples())
        seen: List[tuple] = []
        _job_metrics(db, _FakeRunner()).compute(
            MetricsRequest(), on_progress=lambda p, m: seen.append((p, m))
        )
        assert seen
        assert all(0 <= p <= 100 for p, _ in seen)

    def test_resolved_source_and_output_reach_the_runner(self):
        db = _OutputDB(_sample_rows(), _hub_triples())
        runner = _FakeRunner()
        _job_metrics(db, runner).compute(MetricsRequest())
        assert runner.calls[0]["source_table"] == "main.onto.triples"
        assert runner.calls[0]["output_table"] == "metrics"

    def test_unreachable_graph_raises_with_the_reason(self):
        db = _OutputDB(_sample_rows(), _hub_triples())
        jm = JobMetrics(
            _CypherStore(),
            "g",
            runner=_FakeRunner(),
            query=db.query,
            output_table="metrics",
        )
        with pytest.raises(InfrastructureError):
            jm.compute(MetricsRequest())

    def test_output_table_is_sanitised_for_unquoted_sql(self):
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

    def test_output_table_is_stable_per_version(self):
        from back.objects.digitaltwin.DigitalTwin import DigitalTwin

        class _Domain:
            uc_domain_folder = "sales"
            current_version = "1.2.0"

        first = DigitalTwin.analytics_output_table("c.s", _Domain(), "g")
        second = DigitalTwin.analytics_output_table("c.s", _Domain(), "g")
        assert first == second == "c.s.graph_metrics_sales_1_2_0"

    def test_output_table_falls_back_when_domain_is_empty(self):
        from back.objects.digitaltwin.DigitalTwin import DigitalTwin
        from jobs.graph_analytics_job import validate_identifier

        class _Domain:
            uc_domain_folder = ""
            current_version = ""

        table = DigitalTwin.analytics_output_table("c.s", _Domain(), "mygraph")
        assert validate_identifier(table, "output") == table

    def test_failed_run_raises_with_the_run_url(self):
        db = _OutputDB(_sample_rows(), _hub_triples())
        jm = _job_metrics(db, _FakeRunner(success=False))
        with pytest.raises(InfrastructureError) as exc:
            jm.compute(MetricsRequest())
        assert "example/run/1" in str(exc.value.detail)
