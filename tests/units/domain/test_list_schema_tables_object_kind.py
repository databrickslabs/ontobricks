"""list_schema_tables_result surfaces object_kind (metric views included)."""

import asyncio
import sys
from unittest.mock import MagicMock

from back.objects.domain import Domain
import back.objects.domain.Domain as _  # noqa: F401  (ensures module import)
from back.core.databricks.uc.UnityCatalog import UnityCatalog

domain_module = sys.modules["back.objects.domain.Domain"]


class _FakeCatalog:
    def object_kind_for_table_type(self, t):
        return UnityCatalog.object_kind_for_table_type(t)


class _FakeClient:
    def __init__(self, **kwargs):
        self.catalog = _FakeCatalog()

    def list_tables_and_views(self, catalog, schema):
        return [
            {"name": "orders", "table_type": "MANAGED"},
            {"name": "rev_mv", "table_type": "METRIC_VIEW"},
            {"name": "cust_v", "table_type": "VIEW"},
        ]

    def check_table_select_permission(self, catalog, schema, table):
        return {"can_select": True, "error": None}

    def probe_schema_has_tables(self, catalog, schema):
        return 0


def _domain(monkeypatch):
    monkeypatch.setattr(
        domain_module,
        "get_databricks_host_and_token",
        lambda session, settings: ("https://host", "token"),
    )
    monkeypatch.setattr(
        domain_module, "resolve_warehouse_id", lambda session, settings: "wh"
    )
    monkeypatch.setattr(domain_module, "DatabricksClient", _FakeClient)
    session = MagicMock()
    session.catalog_metadata = {"tables": []}
    return Domain(session, MagicMock())


def test_list_schema_tables_tags_object_kind(monkeypatch):
    domain = _domain(monkeypatch)
    result = asyncio.run(domain.list_schema_tables_result("cat", "sch"))
    assert result["success"] is True
    kinds = {t["name"]: t["object_kind"] for t in result["tables"]}
    assert kinds == {
        "orders": "table",
        "rev_mv": "metric_view",
        "cust_v": "view",
    }
