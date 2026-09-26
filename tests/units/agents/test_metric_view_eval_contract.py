"""Judge validation for the metric-view SQL-generation eval contract.

The scorer in ``tests/eval/metric_view_contract.py`` is the deterministic judge
shared by the dry-run and live paths. These tests prove it accepts the correct
reference answer for every dataset row and rejects the three canonical
metric-view mistakes (SELECT *, unwrapped measure, missing GROUP BY).
"""

import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
for _p in (ROOT / "src", ROOT / "tests" / "eval"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import metric_view_contract as mvc  # noqa: E402

DATASET = ROOT / "tests/eval/datasets/agent_mapping_pge/metric_view.jsonl"


def _examples():
    return mvc.load_examples(DATASET)


class TestTheDatasetIsWellFormed:
    def test_it_has_at_least_ten_metric_view_rows(self):
        assert len(_examples()) >= 10

    def test_every_constraint_kind_is_known(self):
        # load_examples raises on an unknown kind; reaching here means clean.
        assert _examples()


class TestTheJudgeAcceptsTheReferenceAnswer:
    def test_the_reference_sql_scores_perfectly_for_every_row(self):
        for example in _examples():
            sql, id_col = mvc.reference_observation(example)
            score = mvc.score_example(example, sql, id_col)
            assert score["weighted"] == pytest.approx(1.0), example["id"]

    def test_the_dry_run_aggregate_meets_the_threshold(self):
        agg = mvc.run_contract(
            agent_name="mapping_pge",
            dataset_path=DATASET,
            threshold=0.95,
            dry_run=True,
        )
        assert agg == pytest.approx(1.0)


class TestTheJudgeRejectsMetricViewMistakes:
    def _happy(self):
        return next(e for e in _examples() if e["id"] == "mv-happy-single-measure")

    def test_select_star_fails(self):
        ex = self._happy()
        score = mvc.score_example(ex, "SELECT * FROM cat.sales.region_metrics", "region")
        assert score["weighted"] < 1.0

    def test_unwrapped_measure_fails(self):
        ex = self._happy()
        bad = "SELECT region, revenue FROM cat.sales.region_metrics GROUP BY region"
        score = mvc.score_example(ex, bad, "region")
        assert score["weighted"] < 1.0

    def test_missing_group_by_fails(self):
        ex = self._happy()
        bad = "SELECT region, MEASURE(revenue) AS revenue FROM cat.sales.region_metrics"
        score = mvc.score_example(ex, bad, "region")
        assert score["weighted"] < 1.0

    def test_measure_as_id_fails(self):
        ex = next(e for e in _examples() if e["id"] == "mv-adversarial-measure-as-id")
        sql, _ = mvc.reference_observation(ex)
        # Choosing the measure as the id must trip metric_id_not_measure.
        score = mvc.score_example(ex, sql, "revenue")
        assert score["weighted"] < 1.0
