"""Tests for the metric-aware base-SQL builder."""

from back.core.databricks.uc.metric_sql import build_metric_view_base_sql

_ENTRY = {
    "full_name": "cat.sales.region_metrics",
    "columns": [
        {"name": "region", "role": "dimension"},
        {"name": "country", "role": "dimension"},
        {"name": "revenue", "role": "measure"},
    ],
}


def test_builds_measure_and_group_by():
    sql = build_metric_view_base_sql(_ENTRY)
    assert "MEASURE(`revenue`) AS `revenue`" in sql
    assert "`region`" in sql and "`country`" in sql
    assert "GROUP BY" in sql
    assert "SELECT *" not in sql
    assert "`cat`.`sales`.`region_metrics`" in sql


def test_no_measures_is_dimension_only():
    entry = {
        "full_name": "c.s.dims_only",
        "columns": [{"name": "d", "role": "dimension"}],
    }
    sql = build_metric_view_base_sql(entry)
    assert "MEASURE(" not in sql
    assert "GROUP BY" not in sql
    assert "`d`" in sql


def test_selected_columns_subset():
    sql = build_metric_view_base_sql(_ENTRY, selected_columns=["region", "revenue"])
    assert "`country`" not in sql
    assert "GROUP BY `region`" in sql
    assert "MEASURE(`revenue`) AS `revenue`" in sql


def test_columns_missing_role_default_dimension():
    entry = {"full_name": "c.s.mv", "columns": [{"name": "a"}, {"name": "b"}]}
    sql = build_metric_view_base_sql(entry)
    assert "MEASURE(" not in sql
    assert "`a`" in sql and "`b`" in sql
