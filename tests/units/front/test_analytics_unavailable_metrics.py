"""The analytics node-detail table must not print uncomputed metrics as 0.0000.

Regression: engine-side aggregation and the Databricks job both leave some
per-node metrics uncomputed, stored as ``0``. The charts already explain that,
but the PageRank node-detail table formatted every cell with ``toFixed(4)``, so
an uncomputed metric rendered as ``0.0000`` — indistinguishable from a genuine
measurement of zero. It also sorted by PageRank even when PageRank was one of
the uncomputed metrics, producing an arbitrary order under a header that implies
a ranking.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PANEL = REPO_ROOT / "src/front/templates/partials/dtwin/_query_analytics.html"


@pytest.fixture(scope="module")
def html() -> str:
    return PANEL.read_text()


@pytest.fixture(scope="module")
def table_fn(html: str) -> str:
    """Just the body of ``_renderPagerankTable``."""
    start = html.index("function _renderPagerankTable")
    end = html.index("window._analyticsDrillURI", start)
    return html[start:end]


class TestUnavailableCellsAreDashed:
    def test_cell_checks_the_unavailable_list(self, table_fn):
        assert "unavailable.indexOf(key)" in table_fn

    def test_dash_is_rendered_for_unavailable_metrics(self, table_fn):
        assert "&mdash;" in table_fn

    def test_dash_branch_returns_before_formatting_a_number(self, table_fn):
        """The dash must short-circuit, not fall through to ``toFixed``."""
        cell = table_fn[table_fn.index("function cell(key)"):]
        cell = cell[: cell.index("\n            }")]
        dash_at = cell.index("&mdash;")
        tofixed_at = cell.index("toFixed(4)")
        assert dash_at < tofixed_at, "the dash branch must come first"
        assert "return" in cell[:dash_at], "the dash branch must return early"

    def test_unavailable_cell_carries_an_explanatory_title(self, table_fn):
        assert "Not computed in this mode" in table_fn


class TestApproximateCellsAreMarked:
    def test_cell_checks_the_approximate_list(self, table_fn):
        assert "approximate.indexOf(key)" in table_fn

    def test_estimate_marker_is_prefixed_to_the_value(self, table_fn):
        assert "&asymp;" in table_fn


class TestRankingFallsBackWhenPagerankIsMissing:
    def test_rank_key_is_chosen_from_availability(self, table_fn):
        assert "unavailable.indexOf('pagerank')" in table_fn
        assert "'degree'" in table_fn

    def test_sort_uses_the_chosen_key_not_a_hard_coded_pagerank(self, table_fn):
        # A literal `.pagerank` in the comparator would defeat the fallback.
        sort_line = next(
            line for line in table_fn.splitlines() if ".sort(" in line and "allNodes" in line
        )
        assert "rankBy" in sort_line
        assert ".pagerank" not in sort_line


class TestTableNote:
    def test_note_element_exists(self, html):
        assert 'id="pagerankTableNote"' in html

    def test_note_is_populated_by_the_renderer(self, table_fn, html):
        assert "_setPagerankTableNote(" in table_fn
        assert "function _setPagerankTableNote" in html

    def test_note_explains_all_three_conditions(self, html):
        fn = html[html.index("function _setPagerankTableNote"):]
        fn = fn[: fn.index("function _renderPagerankTable")]
        assert "Ranked by" in fn
        assert "did not compute" in fn
        assert "sampled estimate" in fn

    def test_note_hides_itself_when_everything_is_exact(self, html):
        fn = html[html.index("function _setPagerankTableNote"):]
        fn = fn[: fn.index("function _renderPagerankTable")]
        assert "if (!parts.length)" in fn
        assert "classList.add('d-none')" in fn


class TestChartNoticesStillPresent:
    """The chart-level explanation must survive alongside the table changes."""

    def test_zero_notice_distinguishes_unavailable_from_genuine_zero(self, html):
        assert "Not computed for this graph." in html
        assert "All values are 0." in html

    def test_unavailable_metric_copy_names_the_depth_cap_remedy(self, html):
        # Job is the only path; the notice must name the depth-cap remedy and
        # not suggest an in-memory escape hatch that no longer exists.
        block = html[html.index("Not computed for this graph."):][:1400]
        assert re.search(r"depth cap", block)
        assert "Raise the analytics job" in block

    def test_no_in_memory_escape_hatch_in_unavailable_notice(self, html):
        # "Pick an Entity Type above to analyse a subgraph in memory" was the
        # pre-Lakeflow-only escape hatch.  It is false now and must be gone.
        block = html[html.index("Not computed for this graph."):][:1400]
        assert "analyse a subgraph in memory" not in block
