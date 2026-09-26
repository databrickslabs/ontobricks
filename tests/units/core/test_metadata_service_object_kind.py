"""MetadataService tags object_kind and metric-view column roles on load."""

from back.core.databricks.uc.MetadataService import MetadataService
from back.core.databricks.uc.UnityCatalog import UnityCatalog


class _FakeCatalog:
    def list_tables_and_views(self, c, s):
        return [
            {"name": "orders", "full_name": "c.s.orders", "table_type": "MANAGED", "comment": ""},
            {"name": "rev_mv", "full_name": "c.s.rev_mv", "table_type": "METRIC_VIEW", "comment": "kpi"},
        ]

    def object_kind_for_table_type(self, t):
        return UnityCatalog.object_kind_for_table_type(t)

    def get_table_columns(self, c, s, n):
        return [{"name": "id", "type": "bigint", "comment": ""}]

    def get_table_comment(self, c, s, n):
        return ""

    def check_table_select_permission(self, c, s, n):
        return {"can_select": True, "error": None}

    def get_metric_view_columns_with_roles(self, c, s, n):
        return [
            {"name": "region", "type": "string", "comment": "", "role": "dimension"},
            {"name": "revenue", "type": "double", "comment": "", "role": "measure"},
        ]


def test_load_tags_object_kind_and_roles():
    svc = MetadataService(catalog_svc=_FakeCatalog())
    ok, msg, meta = svc.load_selected_tables("c", "s", ["orders", "rev_mv"])
    assert ok, msg
    by_name = {t["name"]: t for t in meta["tables"]}
    assert by_name["orders"]["object_kind"] == "table"
    assert all(
        col.get("role", "dimension") == "dimension"
        for col in by_name["orders"]["columns"]
    )
    assert by_name["rev_mv"]["object_kind"] == "metric_view"
    roles = {c["name"]: c["role"] for c in by_name["rev_mv"]["columns"]}
    assert roles == {"region": "dimension", "revenue": "measure"}


def test_load_schema_metadata_tags_kinds():
    svc = MetadataService(catalog_svc=_FakeCatalog())
    ok, msg, meta = svc.load_schema_metadata("c", "s")
    assert ok, msg
    kinds = {t["name"]: t["object_kind"] for t in meta["tables"]}
    assert kinds == {"orders": "table", "rev_mv": "metric_view"}


def test_legacy_metadata_without_object_kind_defaults_table():
    entry = {"name": "x", "full_name": "c.s.x", "columns": [{"name": "a", "type": "int"}]}
    assert entry.get("object_kind", "table") == "table"
