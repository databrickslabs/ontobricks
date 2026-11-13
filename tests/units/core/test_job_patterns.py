"""JobPatterns: run wiring and bounded read-back of the job's tables."""

import sqlite3

import pytest

from back.core.errors import InfrastructureError
from back.core.graph_analysis.JobPatterns import (
    MEMBERS_PAGE_MAX,
    TOP_PATTERNS_MAX,
    JobPatterns,
    members_query,
    pattern_output_table,
    top_patterns_query,
)
from jobs.graph_analytics_job import PatternSQL, run_patterns
from tests.units.core.test_pattern_job_sql import CUSTOMER, NS, alice_bob_carol
from tests.units.core.test_graph_analytics_job_sql import SqliteRunner

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
    runner = SqliteRunner(alice_bob_carol())
    builder = PatternSQL(
        source_table="triples",
        work_prefix="w",
        output_table="out",
        class_filter=[CUSTOMER],
        hops=2,
        min_support=1,
    )
    run_patterns(runner.execute, runner.scalar, builder, computed_at="t0", cleanup=False)
    return JobPatterns("triples", runner=_Runner(), query=_query(runner), output_table="out")


def test_output_table_name_is_sanitised():
    domain = type("D", (), {"uc_domain_folder": "My Dom-1", "current_version": "2"})()
    assert pattern_output_table("c.s", domain, "g") == "c.s.graph_patterns_my_dom_1_2"


def test_latest_returns_stats_patterns_and_preview(detected):
    out = detected.latest()
    stats = out["stats"]
    assert stats["pattern_count"] == 2
    assert stats["anchor_count"] == 3
    assert len(out["patterns"]) == 2
    supports = sorted(p["support"] for p in out["patterns"])
    assert supports == [1, 2]
    alice = NS + "Alice"
    bob = NS + "Bob"
    by_uri = {n["id"]: n["pattern"] for n in out["graph"]["nodes"]}
    assert by_uri[alice] == by_uri[bob]


def test_latest_is_empty_before_any_run():
    runner = SqliteRunner([])
    jp = JobPatterns("t", runner=_Runner(), query=_query(runner), output_table="out")
    empty = jp.latest()
    assert empty["stats"] is None
    assert empty["patterns"] == []
    assert empty["graph"]["nodes"] == []


def test_members_pages_through_a_pattern(detected):
    shared = next(p for p in detected.top_patterns() if p["support"] == 2)
    first = detected.members(shared["id"], offset=0, limit=1)
    assert first["total"] == 2
    assert len(first["members"]) == 1
    assert first["has_more"] is True
    rest = detected.members(shared["id"], offset=1, limit=1)
    assert len(rest["members"]) == 1
    assert rest["has_more"] is False


def test_shape_returns_tokens(detected):
    pid = detected.top_patterns()[0]["id"]
    shape = detected.shape(pid)
    assert shape is not None
    assert shape["pattern_id"] == pid
    assert isinstance(shape["shape"], list)
    assert shape["support"] >= 1


def test_page_and_top_n_are_capped():
    jp = JobPatterns("t", runner=_Runner(), query=lambda sql: [], output_table="o")
    seen = []
    jp._query = lambda sql: seen.append(sql) or []
    jp.members("abc", limit=10_000)
    assert f"LIMIT {MEMBERS_PAGE_MAX}" in seen[-1]
    jp.top_patterns(limit=10_000)
    assert seen[-1] == top_patterns_query("o", TOP_PATTERNS_MAX)
    assert "LIMIT 10" in members_query("o", "abc", 0, 10)


def test_run_passes_job_parameters_and_reads_summary(detected):
    stats = detected.run(class_filter=CUSTOMER, hops=2, refine_top=5)
    params = detected._runner.params
    assert params["class_filter"] == CUSTOMER
    assert params["pattern_hops"] == "2"
    assert params["refine_top"] == "5"
    assert params["output_table"] == "out"
    assert params["computed_at"]
    assert stats["pattern_count"] == 2
    assert stats["run_page_url"] == "https://run/1"


def test_run_requires_class_filter(detected):
    with pytest.raises(ValueError, match="class_filter"):
        detected.run(class_filter="")


def test_failed_run_raises_with_run_link():
    jp = JobPatterns(
        "t",
        runner=_Runner(
            {
                "success": False,
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "run_page_url": "https://run/2",
            }
        ),
        query=lambda sql: [],
        output_table="o",
    )
    with pytest.raises(InfrastructureError) as exc:
        jp.run(class_filter=CUSTOMER)
    assert "https://run/2" in str(exc.value.detail)
