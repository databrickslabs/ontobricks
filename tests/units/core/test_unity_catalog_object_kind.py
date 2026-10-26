"""Tests for UC object-kind mapping and metric-view column classification."""

from back.core.databricks.uc.UnityCatalog import UnityCatalog


class _FakeAuth:
    """Minimal stand-in — UnityCatalog only stores it; tests monkeypatch I/O."""

    warehouse_id = "w"
    host = "https://example"

    def get_sql_connection_params(self):  # pragma: no cover - not exercised
        return {}


def test_object_kind_mapping():
    m = UnityCatalog.object_kind_for_table_type
    assert m("MANAGED") == "table"
    assert m("EXTERNAL") == "table"
    assert m("FOREIGN") == "table"
    assert m("MANAGED_SHALLOW_CLONE") == "table"
    assert m("VIEW") == "view"
    assert m("MATERIALIZED_VIEW") == "view"
    assert m("STREAMING_TABLE") == "view"
    assert m("METRIC_VIEW") == "metric_view"
    assert m("metric_view") == "metric_view"  # case-insensitive
    assert m("") == "table"  # unknown/blank defaults to table
    assert m("SomethingNew") == "table"


def test_metric_view_columns_with_roles(monkeypatch):
    uc = UnityCatalog(_FakeAuth())
    monkeypatch.setattr(
        uc,
        "get_table_columns",
        lambda c, s, n: [
            {"name": "region", "type": "string", "comment": ""},
            {"name": "revenue", "type": "double", "comment": ""},
        ],
    )
    monkeypatch.setattr(uc, "_metric_measure_names", lambda c, s, n: {"revenue"})
    cols = uc.get_metric_view_columns_with_roles("cat", "sales", "region_metrics")
    assert {c["name"]: c["role"] for c in cols} == {
        "region": "dimension",
        "revenue": "measure",
    }


def test_parse_measure_names_block_form():
    ddl = """
    CREATE VIEW cat.sales.mv WITH METRICS
    LANGUAGE YAML
    AS $$
    version: 0.1
    source: cat.sales.orders
    dimensions:
      - name: region
        expr: region
      - name: country
        expr: country
    measures:
      - name: revenue
        expr: SUM(amount)
      - name: order_count
        expr: COUNT(1)
    $$
    """
    assert UnityCatalog._parse_measure_names(ddl) == {"revenue", "order_count"}


def test_parse_measure_names_no_measures():
    ddl = "CREATE TABLE cat.s.t (a INT)"
    assert UnityCatalog._parse_measure_names(ddl) == set()
