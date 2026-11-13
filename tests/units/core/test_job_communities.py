"""JobCommunities: run wiring and bounded read-back of the job's tables.

The tables are produced by the real job SQL on SQLite, so the read-back is
checked against exactly what the job writes.
"""

import sqlite3

import networkx as nx
import pytest

from back.core.errors import InfrastructureError
from back.core.graph_analysis.JobCommunities import (
    MEMBERS_PAGE_MAX,
    TOP_CLUSTERS_MAX,
    JobCommunities,
    community_output_table,
    members_query,
    top_clusters_query,
)
from jobs.graph_analytics_job import CommunitySQL, run_communities
from tests.units.core.test_graph_analytics_job_sql import (
    SqliteRunner,
    _relabel,
    _triples_from_graph,
)

pytestmark = pytest.mark.unit


class _Runner:
    def __init__(self, outcome=None):
        self.outcome = outcome or {"success": True, "run_page_url": "https://run/1"}
        self.params = None

    def run_params_and_wait(self, params, on_progress=None):
        self.params = params
        return self.outcome


def _query(runner: SqliteRunner):
    def query(sql):
        try:
            return runner.rows(sql)
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                raise RuntimeError(f"TABLE_OR_VIEW_NOT_FOUND {exc}") from exc
            raise
    return query


@pytest.fixture()
def detected():
    g = nx.disjoint_union(nx.complete_graph(4), nx.complete_graph(6))
    g.add_edges_from([(20, 21)])
    runner = SqliteRunner(_triples_from_graph(_relabel(g)))
    builder = CommunitySQL(source_table="triples", work_prefix="w", output_table="out")
    run_communities(runner.execute, runner.scalar, builder, computed_at="t0")
    return JobCommunities("triples", runner=_Runner(), query=_query(runner), output_table="out")


def test_output_table_name_is_sanitised():
    domain = type("D", (), {"uc_domain_folder": "My Dom-1", "current_version": "2"})()
    assert community_output_table("c.s", domain, "g") == "c.s.graph_communities_my_dom_1_2"


def test_latest_returns_stats_clusters_and_preview(detected):
    out = detected.latest()
    stats = out["stats"]
    assert stats["cluster_count"] == 3
    assert stats["node_count"] == 12
    assert stats["size_max"] == 6 and stats["size_min"] == 2
    assert stats["size_median"] == 4
    assert isinstance(stats["elapsed_ms"], int)
    assert stats["elapsed_ms"] >= 0
    assert [c["id"] for c in out["clusters"]] == [0, 1, 2]
    assert [c["size"] for c in out["clusters"]] == [6, 4, 2]
    assert out["graph"]["node_total"] == 12
    assert {n["cluster"] for n in out["graph"]["nodes"]} == {0, 1, 2}
    assert out["graph"]["capped"] is False


def test_latest_is_empty_before_any_run():
    runner = SqliteRunner([])
    jc = JobCommunities("t", runner=_Runner(), query=_query(runner), output_table="out")
    assert jc.latest() == {"clusters": [], "stats": None, "graph": None}


def test_members_pages_through_a_cluster(detected):
    first = detected.members(0, offset=0, limit=4)
    assert first["size"] == 6
    assert len(first["members"]) == 4
    assert first["has_more"] is True
    rest = detected.members(0, offset=4, limit=4)
    assert len(rest["members"]) == 2
    assert rest["has_more"] is False
    uris = {m["uri"] for m in first["members"] + rest["members"]}
    assert len(uris) == 6


def test_page_and_top_n_are_capped():
    assert f"LIMIT {MEMBERS_PAGE_MAX}" not in members_query("o", 0, 0, 10)
    jc = JobCommunities("t", runner=_Runner(), query=lambda sql: [], output_table="o")
    seen = []
    jc._query = lambda sql: seen.append(sql) or []
    jc.members(0, limit=10_000)
    assert f"LIMIT {MEMBERS_PAGE_MAX}" in seen[-1]
    jc.top_clusters(limit=10_000)
    assert seen[-1] == top_clusters_query("o", TOP_CLUSTERS_MAX)


def test_iter_memberships_covers_every_node_in_batches(detected):
    batches = list(detected.iter_memberships(batch_size=5))
    assert [len(b) for b in batches] == [5, 5, 2]
    flat = [uri for b in batches for uri, _ in b]
    assert flat == sorted(flat) and len(set(flat)) == 12


def test_iter_sizes(detected):
    assert [b for b in detected.iter_sizes(batch_size=2)] == [[(0, 6), (1, 4)], [(2, 2)]]


def test_run_passes_job_parameters_and_reads_summary(detected):
    stats = detected.run(class_filter=["http://ex/A"], exclude_predicates=["http://ex/p"])
    params = detected._runner.params
    assert params["class_filter"] == "http://ex/A"
    assert params["exclude_predicates"] == "http://ex/p"
    assert params["output_table"] == "out"
    assert params["lp_iterations"] == "30"
    assert params["computed_at"]
    assert stats["cluster_count"] == 3
    assert stats["run_page_url"] == "https://run/1"


def test_failed_run_raises_with_run_link():
    jc = JobCommunities(
        "t",
        runner=_Runner({"success": False, "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED", "run_page_url": "https://run/2"}),
        query=lambda sql: [],
        output_table="o",
    )
    with pytest.raises(InfrastructureError) as exc:
        jc.run()
    assert "https://run/2" in str(exc.value.detail)
