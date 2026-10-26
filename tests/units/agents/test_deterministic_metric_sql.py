"""Static metric-view SQL guard in the deterministic evaluator."""

from agents.agent_mapping_pge.evaluator.deterministic import check_metric_view_sql

_COLS = [
    {"name": "region", "role": "dimension"},
    {"name": "revenue", "role": "measure"},
]


def test_flat_select_star_rejected():
    f = check_metric_view_sql("SELECT * FROM c.s.mv", _COLS)
    assert f is not None
    assert "MEASURE" in f.hint


def test_measure_without_group_by_rejected():
    f = check_metric_view_sql("SELECT region, MEASURE(revenue) FROM c.s.mv", _COLS)
    assert f is not None
    assert "GROUP BY" in f.hint


def test_unwrapped_measure_rejected():
    f = check_metric_view_sql(
        "SELECT region, revenue FROM c.s.mv GROUP BY region", _COLS
    )
    assert f is not None
    assert "revenue" in f.hint


def test_correct_metric_sql_passes():
    sql = "SELECT region, MEASURE(revenue) AS revenue FROM c.s.mv GROUP BY region"
    assert check_metric_view_sql(sql, _COLS) is None


def test_backtick_quoted_measure_passes():
    sql = "SELECT `region`, MEASURE(`revenue`) AS `revenue` FROM c.s.mv GROUP BY `region`"
    assert check_metric_view_sql(sql, _COLS) is None


def test_dimension_only_view_no_group_by_ok():
    cols = [{"name": "region", "role": "dimension"}]
    assert check_metric_view_sql("SELECT region FROM c.s.mv", cols) is None
