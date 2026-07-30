"""Verify the Lakeflow job's graph algorithms against a NetworkX oracle.

The job emits its algorithms as SQL strings and drives them through injected
``execute`` / ``scalar`` callables, so these tests run the *exact same*
statements and the *exact same* orchestration against SQLite. NetworkX supplies
the expected answers.

That is the whole point of the SQL-generation design: without it, distributed
PageRank, connected components and triangle counting could only be checked on a
live Spark cluster.

Dialect caveat: SQLite is not Spark SQL. These tests validate algorithm logic,
not Databricks-specific behaviour. ``TestJobSqlDialects`` covers the parse side
for Spark and Postgres.
"""

import sqlite3
from typing import Dict, List, Optional

import networkx as nx
import pytest

from jobs.graph_analytics_job import (
    DEFAULT_EXCLUDED_PREDICATES,
    GraphAnalyticsSQL,
    parse_args,
    run_analysis,
)

pytestmark = pytest.mark.unit

NS = "http://ex.org/"
REL = NS + "rel"
NAME = NS + "name"
RDF_TYPE = DEFAULT_EXCLUDED_PREDICATES[0]


class SqliteRunner:
    """Executes the job's SQL against SQLite, standing in for the Spark driver."""

    def __init__(self, triples: List[Dict[str, str]]) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        # The only two functions the generated SQL uses that SQLite lacks.
        self.conn.create_function("least", 2, lambda a, b: min(a, b))
        self.conn.create_function("greatest", 2, lambda a, b: max(a, b))
        self.conn.execute(
            "CREATE TABLE triples (subject TEXT, predicate TEXT, object TEXT)"
        )
        self.conn.executemany(
            "INSERT INTO triples VALUES (?, ?, ?)",
            [(t["subject"], t["predicate"], t["object"]) for t in triples],
        )
        self.conn.commit()

    def execute(self, stmt: str) -> None:
        self.conn.executescript(stmt) if ";" in stmt else self.conn.execute(stmt)

    def scalar(self, query: str):
        row = self.conn.execute(query).fetchone()
        return row[0] if row is not None else None

    def rows(self, query: str) -> List[Dict[str, object]]:
        return [dict(r) for r in self.conn.execute(query).fetchall()]


def _triples_from_graph(graph: nx.Graph) -> List[Dict[str, str]]:
    """Render a NetworkX graph as triples, plus noise the job must ignore."""
    triples = [
        {"subject": u, "predicate": REL, "object": v} for u, v in graph.edges()
    ]
    for n in graph.nodes():
        # Excluded predicates and literal objects must not become edges.
        triples.append({"subject": n, "predicate": RDF_TYPE, "object": NS + "Thing"})
        triples.append({"subject": n, "predicate": NAME, "object": "a literal"})
    return triples


def _run(graph: nx.Graph, **kwargs) -> tuple:
    """Run the job pipeline over *graph* and return (runner, builder, stats)."""
    runner = SqliteRunner(_triples_from_graph(graph))
    builder = GraphAnalyticsSQL(
        source_table="triples",
        work_prefix="w",
        output_table="out",
        excluded_predicates=list(DEFAULT_EXCLUDED_PREDICATES),
    )
    stats = run_analysis(
        runner.execute,
        runner.scalar,
        builder,
        cleanup=False,
        **kwargs,
    )
    return runner, builder, stats


def _output(runner: SqliteRunner) -> Dict[str, Dict[str, object]]:
    return {r["node_uri"]: r for r in runner.rows("SELECT * FROM out")}


def _relabel(graph: nx.Graph) -> nx.Graph:
    """Namespace the node ids so they look like URIs."""
    return nx.relabel_nodes(graph, {n: f"{NS}n{n}" for n in graph.nodes()})


# ---------------------------------------------------------------------------
# Fixtures: graphs with genuinely different structure
# ---------------------------------------------------------------------------

def _hub() -> nx.Graph:
    return _relabel(nx.star_graph(6))


def _two_components() -> nx.Graph:
    g = nx.Graph()
    g.add_edges_from([(0, 1), (1, 2), (2, 0)])       # triangle
    g.add_edges_from([(10, 11), (11, 12)])           # path
    return _relabel(g)


def _triangle_rich() -> nx.Graph:
    return _relabel(nx.complete_graph(5))


def _karate() -> nx.Graph:
    return _relabel(nx.karate_club_graph())


class TestEdgeConstruction:
    def test_literals_and_excluded_predicates_are_not_edges(self):
        graph = _hub()
        runner, builder, stats = _run(graph)
        assert stats["edge_count"] == graph.number_of_edges()
        assert stats["node_count"] == graph.number_of_nodes()

    def test_reverse_direction_collapses_to_one_edge(self):
        triples = [
            {"subject": f"{NS}a", "predicate": REL, "object": f"{NS}b"},
            {"subject": f"{NS}b", "predicate": REL, "object": f"{NS}a"},
        ]
        runner = SqliteRunner(triples)
        builder = GraphAnalyticsSQL(
            source_table="triples", work_prefix="w", output_table="out"
        )
        stats = run_analysis(runner.execute, runner.scalar, builder, cleanup=False)
        assert stats["edge_count"] == 1

    def test_self_loops_dropped(self):
        triples = [
            {"subject": f"{NS}a", "predicate": REL, "object": f"{NS}a"},
            {"subject": f"{NS}a", "predicate": REL, "object": f"{NS}b"},
        ]
        runner = SqliteRunner(triples)
        builder = GraphAnalyticsSQL(
            source_table="triples", work_prefix="w", output_table="out"
        )
        stats = run_analysis(runner.execute, runner.scalar, builder, cleanup=False)
        assert stats["edge_count"] == 1

    def test_empty_graph_writes_summary_and_stops(self):
        runner = SqliteRunner([])
        builder = GraphAnalyticsSQL(
            source_table="triples", work_prefix="w", output_table="out"
        )
        stats = run_analysis(runner.execute, runner.scalar, builder, cleanup=False)
        assert stats["node_count"] == 0
        assert runner.rows("SELECT * FROM out_summary")[0]["node_count"] == 0


class TestDegreeParity:
    @pytest.mark.parametrize(
        "graph_fn", [_hub, _two_components, _triangle_rich, _karate]
    )
    def test_degree_matches_networkx(self, graph_fn):
        graph = graph_fn()
        runner, _, _ = _run(graph)
        out = _output(runner)
        expected = nx.degree_centrality(graph)
        assert set(out) == set(graph.nodes())
        for uri, row in out.items():
            assert row["degree_raw"] == graph.degree(uri)
            assert row["degree"] == pytest.approx(expected[uri], abs=1e-9)


class TestPageRankParity:
    @pytest.mark.parametrize(
        "graph_fn", [_hub, _two_components, _triangle_rich, _karate]
    )
    def test_pagerank_matches_networkx(self, graph_fn):
        graph = graph_fn()
        # 200 iterations, not the job default of 20. Power iteration converges
        # at roughly damping^k, and bipartite-ish graphs (a star, a path) sit
        # at the slow end of that, so matching to 1e-6 needs ~115 rounds. See
        # test_default_iterations_rank_correctly for what the default buys.
        runner, _, _ = _run(graph, pagerank_iterations=200)
        out = _output(runner)
        # weight=None matters: nx.pagerank is weighted by default, and
        # nx.karate_club_graph() ships edge weights. A triple graph has no
        # weights, so the unweighted form is the correct oracle. Converge it
        # tightly too — the default tol=1e-6 leaves ~1e-5 of error, which is
        # coarser than what we want to assert here.
        expected = nx.pagerank(
            graph, alpha=0.85, weight=None, tol=1e-14, max_iter=2000
        )
        for uri, row in out.items():
            assert row["pagerank"] == pytest.approx(expected[uri], abs=1e-6)

    @pytest.mark.parametrize(
        "graph_fn", [_hub, _two_components, _triangle_rich, _karate]
    )
    def test_default_iterations_rank_correctly(self, graph_fn):
        """The default iteration count is chosen for ranking, not precision.

        At 20 iterations absolute scores are only good to ~damping^20 (about
        4e-2), but the app only ever charts a top-N ordering, so what has to
        hold is that the ordering agrees with a fully converged PageRank.
        """
        graph = graph_fn()
        runner, _, _ = _run(graph)          # job default iteration count
        out = _output(runner)
        expected = nx.pagerank(
            graph, alpha=0.85, weight=None, tol=1e-14, max_iter=2000
        )
        got_order = sorted(out, key=lambda u: (-out[u]["pagerank"], u))
        want_order = sorted(expected, key=lambda u: (-expected[u], u))
        assert got_order == want_order

    def test_rank_mass_is_conserved(self):
        runner, _, _ = _run(_karate(), pagerank_iterations=30)
        total = sum(r["pagerank"] for r in _output(runner).values())
        assert total == pytest.approx(1.0, abs=1e-9)

    def test_hub_outranks_leaves(self):
        graph = _hub()
        runner, _, _ = _run(graph, pagerank_iterations=30)
        out = _output(runner)
        ranked = sorted(out.items(), key=lambda kv: -kv[1]["pagerank"])
        assert graph.degree(ranked[0][0]) == max(d for _, d in graph.degree())


class TestComponentParity:
    @pytest.mark.parametrize(
        "graph_fn", [_hub, _two_components, _triangle_rich, _karate]
    )
    def test_component_count_matches_networkx(self, graph_fn):
        graph = graph_fn()
        _, _, stats = _run(graph)
        assert stats["component_count"] == nx.number_connected_components(graph)
        assert stats["components_converged"] is True

    def test_component_labels_partition_the_same_way(self):
        graph = _two_components()
        runner, _, _ = _run(graph)
        out = _output(runner)
        by_label: Dict[object, set] = {}
        for uri, row in out.items():
            by_label.setdefault(row["component_id"], set()).add(uri)
        expected = {frozenset(c) for c in nx.connected_components(graph)}
        assert {frozenset(v) for v in by_label.values()} == expected

    def test_non_convergence_is_reported_not_hidden(self):
        # One iteration cannot label a long path, and the run must say so
        # rather than publishing a wrong component count as final.
        graph = _relabel(nx.path_graph(12))
        _, _, stats = _run(graph, component_iterations=1)
        assert stats["components_converged"] is False


class TestClusteringParity:
    @pytest.mark.parametrize(
        "graph_fn", [_hub, _two_components, _triangle_rich, _karate]
    )
    def test_clustering_matches_networkx(self, graph_fn):
        graph = graph_fn()
        runner, _, _ = _run(graph)
        out = _output(runner)
        expected = nx.clustering(graph)
        for uri, row in out.items():
            assert row["clustering"] == pytest.approx(expected[uri], abs=1e-9)

    def test_each_triangle_enumerated_once(self):
        graph = _triangle_rich()          # K5 has C(5,3) == 10 triangles
        runner, builder, _ = _run(graph)
        count = runner.scalar(f"SELECT COUNT(*) FROM {builder.triangles}")
        assert count == 10

    def test_star_graph_has_zero_clustering(self):
        runner, _, _ = _run(_hub())
        assert all(r["clustering"] == 0.0 for r in _output(runner).values())


class TestSummaryAndCleanup:
    def test_summary_row_matches_returned_stats(self):
        graph = _karate()
        runner, _, stats = _run(graph)
        row = runner.rows("SELECT * FROM out_summary")[0]
        assert row["node_count"] == stats["node_count"]
        assert row["edge_count"] == stats["edge_count"]
        assert row["component_count"] == stats["component_count"]
        assert row["source_table"] == "triples"

    def test_cleanup_drops_intermediate_tables(self):
        graph = _hub()
        runner = SqliteRunner(_triples_from_graph(graph))
        builder = GraphAnalyticsSQL(
            source_table="triples", work_prefix="w", output_table="out"
        )
        run_analysis(runner.execute, runner.scalar, builder, cleanup=True)
        existing = {
            r["name"]
            for r in runner.rows("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert not (existing & set(builder.work_tables()))
        # ...while the results survive.
        assert "out" in existing and "out_summary" in existing


class TestArgParsing:
    def test_required_arguments(self):
        args = parse_args(["--source-table", "c.s.t", "--output-table", "c.s.o"])
        assert args.source_table == "c.s.t"
        assert args.output_table == "c.s.o"
        assert args.keep_work_tables is False

    def test_iteration_overrides(self):
        args = parse_args(
            [
                "--source-table", "t",
                "--output-table", "o",
                "--pagerank-iterations", "7",
                "--component-iterations", "3",
                "--damping", "0.5",
            ]
        )
        assert args.pagerank_iterations == 7
        assert args.component_iterations == 3
        assert args.damping == 0.5


class TestSqlInjectionEscaping:
    def test_predicate_quotes_are_escaped(self):
        builder = GraphAnalyticsSQL(
            source_table="t",
            work_prefix="w",
            output_table="o",
            excluded_predicates=["http://ex.org/o'brien"],
        )
        sql = "\n".join(builder.build_edges())
        assert "o''brien" in sql

    def test_source_table_in_summary_is_escaped(self):
        builder = GraphAnalyticsSQL(
            source_table="it's", work_prefix="w", output_table="o"
        )
        sql = "\n".join(
            builder.write_summary(
                {
                    "node_count": 0,
                    "edge_count": 0,
                    "component_count": 0,
                    "components_converged": True,
                    "pagerank_iterations": 0,
                    "component_iterations": 0,
                    "source_table": "it's",
                }
            )
        )
        assert "it''s" in sql


class TestJobSqlDialects:
    """The generated statements must parse as Spark SQL and Postgres."""

    def _all_statements(self) -> List[str]:
        # Realistic names: identifiers are interpolated unquoted, so they must
        # not collide with dialect keywords (`out` is reserved in Spark).
        builder = GraphAnalyticsSQL(
            source_table="main.ontobricks.triplestore_demo_v1_graph",
            work_prefix="main.ontobricks.graph_metrics_demo_v1_work",
            output_table="main.ontobricks.graph_metrics_demo_v1",
        )
        statements = list(builder.build_edges())
        statements += builder.pagerank_init(10)
        statements += builder.pagerank_iteration("a", "b", 10)
        statements += builder.components_init()
        statements += builder.components_iteration("a", "b")
        statements += builder.clustering()
        statements += builder.write_output("b", "b", 10)
        statements += builder.write_summary(
            {
                "node_count": 1,
                "edge_count": 1,
                "component_count": 1,
                "components_converged": True,
                "pagerank_iterations": 1,
                "component_iterations": 1,
                "source_table": "cat.sch.triples",
            }
        )
        statements += [
            builder.node_count_query(),
            builder.edge_count_query(),
            builder.components_changed_query("a", "b"),
            builder.component_count_query("b"),
        ]
        statements += builder.drop_work_tables()
        return statements

    @pytest.mark.parametrize("dialect", ["databricks", "spark", "postgres"])
    def test_statements_parse(self, dialect):
        sqlglot = pytest.importorskip(
            "sqlglot", reason="sqlglot is a dev dependency for dialect checks"
        )
        for stmt in self._all_statements():
            try:
                sqlglot.parse_one(stmt, dialect=dialect)
            except Exception as exc:  # noqa: BLE001
                pytest.fail(f"{dialect} rejected:\n{stmt}\n{exc}")
