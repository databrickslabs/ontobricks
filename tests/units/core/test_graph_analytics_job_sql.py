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

import hashlib
import sqlite3
from typing import Dict, List, Optional

import networkx as nx
import pytest

from jobs.graph_analytics_job import (
    DEFAULT_EXCLUDED_PREDICATES,
    RDFS_LABEL,
    RDF_TYPE,
    GraphAnalyticsSQL,
    parse_args,
    run_analysis,
)

pytestmark = pytest.mark.unit

NS = "http://ex.org/"
REL = NS + "rel"
NAME = NS + "name"


class SqliteRunner:
    """Executes the job's SQL against SQLite, standing in for the Spark driver."""

    def __init__(self, triples: List[Dict[str, str]]) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        # The only two functions the generated SQL uses that SQLite lacks.
        self.conn.create_function("least", 2, lambda a, b: min(a, b))
        self.conn.create_function("greatest", 2, lambda a, b: max(a, b))
        # Spark and Postgres both ship md5; SQLite does not.
        self.conn.create_function(
            "md5", 1, lambda s: hashlib.md5((s or "").encode()).hexdigest()
        )
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


def _run_job(triples, *, class_filter=None) -> tuple:
    """Build a SQLite connection and builder, run the job, return (conn, builder).

    *triples* may be dicts or ``(subject, predicate, object)`` tuples.
    """
    normalised = [
        t if isinstance(t, dict) else {"subject": t[0], "predicate": t[1], "object": t[2]}
        for t in triples
    ]
    runner = SqliteRunner(normalised)
    builder = GraphAnalyticsSQL(
        source_table="triples",
        work_prefix="w",
        output_table="out",
        excluded_predicates=list(DEFAULT_EXCLUDED_PREDICATES),
        class_filter=class_filter or [],
    )
    run_analysis(runner.execute, runner.scalar, builder, cleanup=False)
    return runner.conn, builder


def _table_rows(triples, *, suffix="", class_filter=None) -> List[Dict[str, object]]:
    """Run the job over *triples*, return one output table as a list of dicts.

    ``suffix`` selects which output table to read: "" is the per-node table,
    "_summary" / "_type_profiles" / "_type_predicates" the others.
    """
    conn, builder = _run_job(triples, class_filter=class_filter)
    cur = conn.execute(f"SELECT * FROM {builder.output_table}{suffix}")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


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


class TestBetweennessAndClosenessParity:
    """Pivot sampling must be exact when the pivot set is every node.

    This is the whole reason the estimators are written the way they are. An
    approximation that cannot be checked against a known answer is a guess; by
    making the pivot set an input, ``pivots >= node_count`` collapses both
    estimators to the exact definition and can be compared to NetworkX
    directly. Only once that holds do the sampled cases mean anything.
    """

    @pytest.mark.parametrize(
        "graph_fn", [_hub, _two_components, _triangle_rich, _karate]
    )
    def test_all_pivots_matches_exact_betweenness(self, graph_fn):
        graph = graph_fn()
        runner, _, stats = _run(graph, pivots=graph.number_of_nodes())
        out = _output(runner)
        assert stats["pivot_count"] == graph.number_of_nodes()
        expected = nx.betweenness_centrality(graph, normalized=True)
        for uri, row in out.items():
            assert row["betweenness"] == pytest.approx(expected[uri], abs=1e-9)

    @pytest.mark.parametrize(
        "graph_fn", [_hub, _two_components, _triangle_rich, _karate]
    )
    def test_all_pivots_matches_exact_closeness(self, graph_fn):
        graph = graph_fn()
        runner, _, _ = _run(graph, pivots=graph.number_of_nodes())
        out = _output(runner)
        expected = nx.closeness_centrality(graph)
        for uri, row in out.items():
            assert row["closeness"] == pytest.approx(expected[uri], abs=1e-9)

    def test_closeness_handles_disconnected_components(self):
        # The two-component fixture is the case a naive 1/sum(d) estimator
        # gets wrong: nodes cannot reach the other component at all.
        graph = _two_components()
        runner, _, _ = _run(graph, pivots=graph.number_of_nodes())
        out = _output(runner)
        expected = nx.closeness_centrality(graph)
        for uri, row in out.items():
            assert row["closeness"] == pytest.approx(expected[uri], abs=1e-9)

    def test_sampled_pivots_surface_the_true_top_node_near_the_top(self):
        """A sample ranks the leaders roughly, not exactly.

        Deliberately *not* asserting that the top estimated node is the top
        exact node: on karate the top two are 0.438 and 0.304, and a 12-pivot
        sample of 34 nodes swaps them often. That is ordinary variance, so the
        honest guarantee is that the true leader stays near the top — which is
        what matters for a chart of the top N.
        """
        graph = _karate()
        runner, _, stats = _run(graph, pivots=12)
        assert stats["pivot_count"] == 12
        out = _output(runner)
        exact = nx.betweenness_centrality(graph, normalized=True)
        top_exact = max(exact, key=lambda n: exact[n])
        est_order = sorted(out, key=lambda n: -out[n]["betweenness"])
        assert top_exact in est_order[:3]

    def test_more_pivots_means_less_error(self):
        """The estimator must converge on the exact answer as k grows.

        This is the property that shows the ``n/k`` rescaling is right: a
        mis-scaled estimator would be consistently wrong at every k rather
        than improving.
        """
        graph = _karate()
        exact = nx.betweenness_centrality(graph, normalized=True)

        def mean_abs_error(k: int) -> float:
            out = _output(_run(graph, pivots=k)[0])
            return sum(abs(out[u]["betweenness"] - exact[u]) for u in out) / len(out)

        assert mean_abs_error(30) < mean_abs_error(4)
        # And a full pivot set has essentially no error at all.
        assert mean_abs_error(graph.number_of_nodes()) < 1e-9

    def test_sampled_estimate_is_in_the_right_ballpark(self):
        graph = _karate()
        runner, _, _ = _run(graph, pivots=20)
        out = _output(runner)
        exact = nx.betweenness_centrality(graph, normalized=True)
        # The n/k rescaling should keep the totals comparable; a factor-of-two
        # error here would mean the rescale is wrong rather than merely noisy.
        est_total = sum(r["betweenness"] for r in out.values())
        exact_total = sum(exact.values())
        assert 0.5 * exact_total < est_total < 2.0 * exact_total

    def test_pivot_sampling_is_deterministic(self):
        graph = _karate()
        first = _output(_run(graph, pivots=10)[0])
        second = _output(_run(graph, pivots=10)[0])
        for uri in first:
            assert first[uri]["betweenness"] == second[uri]["betweenness"]

    def test_pivots_zero_skips_both_metrics(self):
        graph = _karate()
        runner, _, stats = _run(graph, pivots=0)
        out = _output(runner)
        assert stats["pivot_count"] == 0
        assert all(r["betweenness"] == 0.0 for r in out.values())
        assert all(r["closeness"] == 0.0 for r in out.values())

    def test_depth_cap_is_reported_as_incomplete(self):
        # A long path cannot be searched in one level, and the run must say the
        # estimates are truncated rather than publish them as final.
        graph = _relabel(nx.path_graph(14))
        _, _, stats = _run(graph, pivots=graph.number_of_nodes(), max_depth=2)
        assert stats["bfs_complete"] is False

    def test_completed_bfs_is_reported_complete(self):
        graph = _karate()
        _, _, stats = _run(graph, pivots=graph.number_of_nodes(), max_depth=12)
        assert stats["bfs_complete"] is True

    def test_default_depth_covers_a_graph_deeper_than_a_dozen_levels(self):
        # Sparse knowledge graphs are chains, not small worlds: a 25-hop path
        # needs more levels than the cap this job originally shipped with, and
        # falling short costs betweenness and closeness for the whole run.
        graph = _relabel(nx.path_graph(26))
        _, _, stats = _run(graph, pivots=graph.number_of_nodes())
        assert stats["bfs_complete"] is True

    def test_headroom_costs_nothing_on_a_shallow_graph(self):
        # The cap is a runaway guard, not a budget: the search stops when the
        # frontier empties, so raising it must not add levels to a small graph.
        levels: List[int] = []
        runner = SqliteRunner(_triples_from_graph(_hub()))
        builder = GraphAnalyticsSQL(
            source_table="triples", work_prefix="w", output_table="out"
        )
        original = builder.bfs_iteration

        def counting(depth, read_slot, write_slot):
            levels.append(depth)
            return original(depth, read_slot, write_slot)

        builder.bfs_iteration = counting  # type: ignore[method-assign]
        run_analysis(runner.execute, runner.scalar, builder, pivots=7, cleanup=False)
        # A star is fully explored two hops out; level 3 is the probe that
        # finds the frontier empty and stops. The cap never comes into it.
        assert levels == [1, 2, 3]

    def test_sigma_counts_multiple_shortest_paths(self):
        # A 4-cycle gives two equal-length paths between opposite corners;
        # betweenness is only right if sigma counts both.
        graph = _relabel(nx.cycle_graph(4))
        runner, _, _ = _run(graph, pivots=4)
        out = _output(runner)
        expected = nx.betweenness_centrality(graph, normalized=True)
        for uri, row in out.items():
            assert row["betweenness"] == pytest.approx(expected[uri], abs=1e-9)


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


def test_output_carries_rdf_type_and_label():
    """The per-node output resolves one type and one label per node."""
    triples = [
        ("http://ex/a", "http://ex/knows", "http://ex/b"),
        ("http://ex/a", RDF_TYPE, "http://ex/Person"),
        ("http://ex/a", RDFS_LABEL, "Alice"),
        ("http://ex/b", RDF_TYPE, "http://ex/Person"),
        # A second type must not duplicate the node's output row.
        ("http://ex/b", RDF_TYPE, "http://ex/Agent"),
    ]
    rows = _table_rows(triples)

    by_uri = {r["node_uri"]: r for r in rows}
    assert len(rows) == 2
    assert by_uri["http://ex/a"]["type_uri"] == "http://ex/Person"
    assert by_uri["http://ex/a"]["label"] == "Alice"
    # MIN over {Agent, Person} — deterministic, not arbitrary.
    assert by_uri["http://ex/b"]["type_uri"] == "http://ex/Agent"
    assert by_uri["http://ex/b"]["label"] is None


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


def test_type_profiles_cover_isolated_and_connected_instances():
    """instance_count is the full population; the rest covers scored nodes."""
    triples = [
        ("http://ex/a", "http://ex/knows", "http://ex/b"),
        ("http://ex/a", RDF_TYPE, "http://ex/Person"),
        ("http://ex/b", RDF_TYPE, "http://ex/Person"),
        # Typed but with no entity-entity edge: counted, never scored.
        ("http://ex/c", RDF_TYPE, "http://ex/Person"),
        ("http://ex/c", RDFS_LABEL, "Carol"),
    ]
    profiles = _table_rows(triples, suffix="_type_profiles")

    assert len(profiles) == 1
    row = profiles[0]
    assert row["type_uri"] == "http://ex/Person"
    assert row["instance_count"] == 3     # a, b, c
    assert row["connected_count"] == 2    # a, b
    assert row["degree_sum"] == 2         # one undirected edge, two endpoints
    assert row["avg_clustering"] == 0.0


def test_type_predicates_are_distinct_and_exclude_metadata():
    """rdf:type and rdfs:label never count as relationship predicates."""
    triples = [
        ("http://ex/a", "http://ex/knows", "http://ex/b"),
        ("http://ex/a", "http://ex/knows", "http://ex/c"),
        ("http://ex/a", "http://ex/owns", "http://ex/b"),
        ("http://ex/a", RDF_TYPE, "http://ex/Person"),
        ("http://ex/a", RDFS_LABEL, "Alice"),
        # A literal object is not a relationship.
        ("http://ex/a", "http://ex/age", "41"),
    ]
    rows = _table_rows(triples, suffix="_type_predicates")

    pairs = {(r["type_uri"], r["predicate"]) for r in rows}
    assert pairs == {
        ("http://ex/Person", "http://ex/knows"),
        ("http://ex/Person", "http://ex/owns"),
    }


def test_summary_reports_total_and_connected_node_counts():
    """node_count is the scored graph; total_node_count is every subject."""
    triples = [
        ("http://ex/a", "http://ex/knows", "http://ex/b"),
        ("http://ex/c", RDF_TYPE, "http://ex/Person"),  # isolated subject
    ]
    summary = _table_rows(triples, suffix="_summary")[0]

    assert summary["node_count"] == 2        # a, b
    assert summary["total_node_count"] == 3  # a, b, c


def test_total_node_count_comes_from_source_not_degree_table():
    """total_node_count is stable under per-run predicate exclusions.

    ``obj_only`` appears only as an object of ``http://ex/rel``, never as a
    subject, so the subject-side of the union cannot count it.  Run 2 excludes
    ``http://ex/rel`` from edge construction.  Under the old implementation
    (using ``self.excluded_predicates`` on the object side) ``obj_only`` would
    vanish from run 2's total because the object side would filter it out;
    under the correct implementation (using ``DEFAULT_EXCLUDED_PREDICATES``) it
    is always counted, and both runs must agree.
    """
    triples = [
        {"subject": "http://ex/a", "predicate": "http://ex/rel", "object": "http://ex/b"},
        # obj_only never appears as a subject; it can only be counted via the
        # object side of the UNION — which must use the fixed metadata list,
        # not the per-run excluded_predicates.
        {"subject": "http://ex/a", "predicate": "http://ex/rel", "object": "http://ex/obj_only"},
    ]

    def _total(excluded):
        runner = SqliteRunner(triples)
        builder = GraphAnalyticsSQL(
            source_table="triples",
            work_prefix="w",
            output_table="out",
            excluded_predicates=excluded,
        )
        run_analysis(runner.execute, runner.scalar, builder, cleanup=False)
        return runner.conn.execute(
            "SELECT total_node_count FROM out_summary"
        ).fetchone()[0]

    default = list(DEFAULT_EXCLUDED_PREDICATES)
    # Excluding http://ex/rel eliminates every entity-entity edge, so the
    # degree table is empty on run 2.  The population total must not shrink.
    also_exclude_rel = default + ["http://ex/rel"]

    assert _total(default) == 3            # a, b, obj_only
    assert _total(also_exclude_rel) == 3   # same — obj_only is still in the source


def test_flat_source_still_gets_profiles():
    """A source with no entity-entity edges still reports its types."""
    triples = [
        ("http://ex/a", RDF_TYPE, "http://ex/Reading"),
        ("http://ex/a", "http://ex/value", "41"),
        ("http://ex/b", RDF_TYPE, "http://ex/Reading"),
    ]
    profiles = _table_rows(triples, suffix="_type_profiles")

    assert len(profiles) == 1
    assert profiles[0]["instance_count"] == 2
    assert profiles[0]["connected_count"] == 0
    assert _table_rows(triples, suffix="_type_predicates") == []


def test_class_filter_matches_networkx_induced_subgraph():
    """A class-filtered run equals NetworkX on graph.subgraph(typed nodes)."""
    triples = [
        ("http://ex/p1", "http://ex/knows", "http://ex/p2"),
        ("http://ex/p2", "http://ex/knows", "http://ex/p3"),
        ("http://ex/p1", "http://ex/knows", "http://ex/p3"),
        # An Order attached to a Person must vanish with the filter, taking
        # its edge with it.
        ("http://ex/p1", "http://ex/ordered", "http://ex/o1"),
        ("http://ex/p1", RDF_TYPE, "http://ex/Person"),
        ("http://ex/p2", RDF_TYPE, "http://ex/Person"),
        ("http://ex/p3", RDF_TYPE, "http://ex/Person"),
        ("http://ex/o1", RDF_TYPE, "http://ex/Order"),
    ]
    rows = _table_rows(triples, class_filter=["http://ex/Person"])

    g = nx.Graph()
    g.add_edges_from([
        ("http://ex/p1", "http://ex/p2"),
        ("http://ex/p2", "http://ex/p3"),
        ("http://ex/p1", "http://ex/p3"),
    ])
    expected_degree = nx.degree_centrality(g)
    expected_clustering = nx.clustering(g)

    assert {r["node_uri"] for r in rows} == set(g.nodes)
    for row in rows:
        uri = row["node_uri"]
        assert row["degree"] == pytest.approx(expected_degree[uri], abs=1e-6)
        assert row["clustering"] == pytest.approx(expected_clustering[uri], abs=1e-6)


def test_class_filter_narrows_type_profiles():
    """Only the selected types get a profile."""
    triples = [
        ("http://ex/p1", "http://ex/knows", "http://ex/p2"),
        ("http://ex/p1", RDF_TYPE, "http://ex/Person"),
        ("http://ex/p2", RDF_TYPE, "http://ex/Person"),
        ("http://ex/o1", RDF_TYPE, "http://ex/Order"),
    ]
    profiles = _table_rows(
        triples, suffix="_type_profiles",
        class_filter=["http://ex/Person"],
    )
    assert [r["type_uri"] for r in profiles] == ["http://ex/Person"]


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
        # Both: unfiltered (whole graph) and class-filtered (induced subgraph).
        statements = list(builder.build_edges())
        filtered = GraphAnalyticsSQL(
            source_table="main.ontobricks.triplestore_demo_v1_graph",
            work_prefix="main.ontobricks.graph_metrics_demo_v1_work",
            output_table="main.ontobricks.graph_metrics_demo_v1",
            class_filter=["http://example.org/Person"],
        )
        statements += filtered.build_edges()
        statements += builder.pagerank_init(10)
        statements += builder.pagerank_iteration("a", "b", 10)
        statements += builder.components_init()
        statements += builder.components_iteration("a", "b")
        statements += builder.clustering()
        statements += builder.build_pivots(64)
        statements += builder.bfs_init()
        statements += builder.bfs_iteration(1, "a", "b")
        statements += builder.delta_init()
        statements += builder.delta_iteration(3)
        statements += builder.build_centrality_rollups()
        # Both branches: with pivots (the estimator expressions) and without.
        statements += builder.write_output("b", "b", 10, pivot_count=64)
        statements += builder.write_output("b", "b", 10)
        statements += builder.write_summary(
            {
                "node_count": 1,
                "edge_count": 1,
                "component_count": 1,
                "components_converged": True,
                "pagerank_iterations": 1,
                "component_iterations": 1,
                "pivot_count": 64,
                "bfs_complete": True,
                "source_table": "cat.sch.triples",
            }
        )
        statements += builder.write_empty_output()
        statements += builder.type_profiles()
        statements += builder.type_predicates()
        statements += [
            builder.total_node_count_query(),
            builder.node_count_query(),
            builder.edge_count_query(),
            builder.components_changed_query("a", "b"),
            builder.component_count_query("b"),
            builder.pivot_count_query(),
            builder.frontier_count_query("a"),
            builder.max_depth_query(),
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
