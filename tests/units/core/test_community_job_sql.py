"""Label propagation in the Lakeflow job, run against SQLite.

Same harness as ``test_graph_analytics_job_sql.py``: the exact SQL and the
exact orchestration the Spark driver runs, executed on SQLite. Partitions are
checked on hand-built graphs whose communities are known; modularity is checked
against NetworkX on the partition the job produced.
"""

from typing import Dict, List, Set

import networkx as nx
import pytest

from jobs.graph_analytics_job import (
    COMMUNITY_ALGORITHM,
    PREVIEW_COMMUNITIES,
    PREVIEW_MAX_NODES,
    RDF_TYPE,
    RDFS_LABEL,
    CommunitySQL,
    modularity_from_sums,
    parse_args,
    run_communities,
)
from tests.units.core.test_graph_analytics_job_sql import (
    REL,
    SqliteRunner,
    _relabel,
    _triples_from_graph,
)

pytestmark = pytest.mark.unit


def _builder(**kwargs) -> CommunitySQL:
    return CommunitySQL(
        source_table="triples", work_prefix="w", output_table="out", **kwargs
    )


def _run(triples, **kwargs):
    runner = SqliteRunner(triples)
    builder = _builder(class_filter=kwargs.pop("class_filter", []))
    stats = run_communities(
        runner.execute, runner.scalar, builder, cleanup=kwargs.pop("cleanup", False), **kwargs
    )
    return runner, builder, stats


def _partition(runner: SqliteRunner) -> List[Set[str]]:
    groups: Dict[int, Set[str]] = {}
    for row in runner.rows("SELECT node_uri, community_rank FROM out"):
        groups.setdefault(int(row["community_rank"]), set()).add(row["node_uri"])
    return [groups[k] for k in sorted(groups)]


def _two_cliques() -> nx.Graph:
    g = nx.disjoint_union(nx.complete_graph(4), nx.complete_graph(5))
    return _relabel(g)


class TestPartitions:
    def test_disjoint_cliques_are_separate_communities(self):
        graph = _two_cliques()
        runner, _, stats = _run(_triples_from_graph(graph))
        parts = _partition(runner)
        expected = sorted(
            (set(c) for c in nx.connected_components(graph)), key=len, reverse=True
        )
        assert parts == expected
        assert stats["community_count"] == 2
        assert stats["converged"] is True

    def test_barbell_splits_at_the_bridge(self):
        graph = _relabel(nx.barbell_graph(5, 0))
        runner, _, stats = _run(_triples_from_graph(graph))
        parts = _partition(runner)
        assert len(parts) == 2
        assert {frozenset(p) for p in parts} == {
            frozenset(f"http://ex.org/n{i}" for i in range(5)),
            frozenset(f"http://ex.org/n{i}" for i in range(5, 10)),
        }

    def test_bipartite_star_settles(self):
        graph = _relabel(nx.star_graph(6))
        runner, _, stats = _run(_triples_from_graph(graph))
        assert stats["converged"] is True
        assert len(_partition(runner)) == 1

    def test_ranks_are_zero_based_and_size_descending(self):
        graph = _two_cliques()
        runner, _, _ = _run(_triples_from_graph(graph))
        sizes = runner.rows("SELECT community_rank, size FROM out_sizes ORDER BY community_rank")
        assert [r["community_rank"] for r in sizes] == [0, 1]
        assert [r["size"] for r in sizes] == [5, 4]

    def test_rerun_gives_identical_partition(self):
        triples = _triples_from_graph(_relabel(nx.karate_club_graph()))
        first, _, _ = _run(triples)
        second, _, _ = _run(triples)
        q = "SELECT node_uri, community_rank FROM out ORDER BY node_uri"
        assert first.rows(q) == second.rows(q)


class TestModularity:
    def test_matches_networkx_on_the_job_partition(self):
        graph = _relabel(nx.karate_club_graph())
        runner, _, stats = _run(_triples_from_graph(graph))
        expected = nx.community.modularity(graph, _partition(runner))
        assert stats["modularity"] == pytest.approx(expected, abs=1e-6)

    def test_zero_edges_is_zero(self):
        assert modularity_from_sums(0, 0, 0) == 0.0


class TestOutputs:
    def test_output_carries_type_label_and_degree(self):
        triples = [
            {"subject": "http://ex/a", "predicate": REL, "object": "http://ex/b"},
            {"subject": "http://ex/a", "predicate": RDF_TYPE, "object": "http://ex/Person"},
            {"subject": "http://ex/a", "predicate": RDFS_LABEL, "object": "Alice"},
        ]
        runner, _, _ = _run(triples)
        row = runner.rows("SELECT * FROM out WHERE node_uri = 'http://ex/a'")[0]
        assert row["type_uri"] == "http://ex/Person"
        assert row["label"] == "Alice"
        assert row["degree_raw"] == 1

    def test_summary_row(self):
        runner, _, stats = _run(
            _triples_from_graph(_two_cliques()), computed_at="2026-11-12T00:00:00Z"
        )
        row = runner.rows("SELECT * FROM out_summary")[0]
        assert row["node_count"] == 9
        assert row["community_count"] == 2
        assert row["algorithm"] == COMMUNITY_ALGORITHM
        assert row["computed_at"] == "2026-11-12T00:00:00Z"
        assert stats["edge_count"] == row["edge_count"]

    def test_preview_is_capped_per_community(self):
        # 30 disjoint triangles: only the 20 largest communities are sampled.
        g = nx.Graph()
        for k in range(30):
            g.add_edges_from([(3 * k, 3 * k + 1), (3 * k + 1, 3 * k + 2), (3 * k, 3 * k + 2)])
        runner, _, _ = _run(_triples_from_graph(_relabel(g)))
        nodes = runner.rows("SELECT * FROM out_preview_nodes")
        assert len(nodes) <= PREVIEW_MAX_NODES
        assert {r["community_rank"] for r in nodes} == set(range(PREVIEW_COMMUNITIES))
        edges = runner.rows("SELECT * FROM out_preview_edges")
        assert len(edges) == 3 * PREVIEW_COMMUNITIES

    def test_empty_graph_writes_empty_outputs_and_summary(self):
        triples = [{"subject": "http://ex/a", "predicate": RDFS_LABEL, "object": "x"}]
        runner, _, stats = _run(triples)
        assert stats["node_count"] == 0
        assert stats["community_count"] == 0
        assert runner.rows("SELECT * FROM out") == []
        assert runner.rows("SELECT * FROM out_summary")[0]["converged"] == 1

    def test_class_filter_restricts_to_typed_nodes(self):
        triples = [
            {"subject": "http://ex/p1", "predicate": REL, "object": "http://ex/p2"},
            {"subject": "http://ex/p1", "predicate": REL, "object": "http://ex/o1"},
            {"subject": "http://ex/p1", "predicate": RDF_TYPE, "object": "http://ex/Person"},
            {"subject": "http://ex/p2", "predicate": RDF_TYPE, "object": "http://ex/Person"},
            {"subject": "http://ex/o1", "predicate": RDF_TYPE, "object": "http://ex/Order"},
        ]
        runner, _, _ = _run(triples, class_filter=["http://ex/Person"])
        uris = {r["node_uri"] for r in runner.rows("SELECT node_uri FROM out")}
        assert uris == {"http://ex/p1", "http://ex/p2"}
        summary = runner.rows("SELECT class_filter FROM out_summary")[0]
        assert summary["class_filter"] == "http://ex/Person"


class TestCleanup:
    def test_work_tables_dropped_outputs_kept(self):
        runner, builder, _ = _run(_triples_from_graph(_two_cliques()), cleanup=True)
        names = {
            r["name"]
            for r in runner.rows("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        for table in builder.work_tables():
            assert table not in names
        for table in ("out", "out_sizes", "out_summary", "out_preview_nodes", "out_preview_edges"):
            assert table in names


class TestCli:
    def test_task_flag_selects_communities(self):
        ns = parse_args(
            ["--task", "communities", "--source-table", "a.b.c", "--output-table", "a.b.d"]
        )
        assert ns.task == "communities"
        assert ns.lp_iterations > 0

    def test_task_defaults_to_metrics(self):
        ns = parse_args(["--source-table", "a.b.c", "--output-table", "a.b.d"])
        assert ns.task == "metrics"


class TestDialects:
    @pytest.mark.parametrize("dialect", ["databricks", "spark", "postgres"])
    def test_statements_parse(self, dialect):
        sqlglot = pytest.importorskip("sqlglot")
        builder = CommunitySQL(
            source_table="main.ontobricks.triplestore_demo_v1_graph",
            work_prefix="main.ontobricks.graph_communities_demo_v1_work",
            output_table="main.ontobricks.graph_communities_demo_v1",
            class_filter=["http://example.org/Person"],
        )
        statements = (
            builder.lp_init()
            + builder.lp_iteration("a", "b", 0)
            + builder.lp_iteration("b", "a", 1)
            + builder.write_sizes("a")
            + builder.write_community_output("a")
            + builder.write_preview()
            + builder.write_community_summary(
                {
                    "node_count": 1, "edge_count": 1, "community_count": 1,
                    "modularity": 0.5, "iterations": 2, "converged": True,
                    "algorithm": COMMUNITY_ALGORITHM, "source_table": "a.b.c",
                    "computed_at": "now",
                }
            )
            + [
                builder.lp_changed_query("a", "b"),
                builder.community_count_query(),
                builder.intra_edges_query(),
                builder.degree_square_sum_query(),
            ]
        )
        for stmt in statements:
            try:
                sqlglot.parse_one(stmt, dialect=dialect)
            except Exception as exc:  # noqa: BLE001
                pytest.fail(f"{dialect} rejected:\n{stmt}\n{exc}")
