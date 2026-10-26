"""UC listing / DESCRIBE failures must not be reported as empty schemas."""

import pytest

from back.core.databricks.uc.MetadataService import MetadataService
from back.core.databricks.uc.UnityCatalog import UnityCatalog
from back.core.errors import InfrastructureError


class _FailingCatalog:
    def __init__(self, *, listing_fails=False, columns_fail=False):
        self._listing_fails = listing_fails
        self._columns_fail = columns_fail

    def list_tables_and_views(self, c, s):
        if self._listing_fails:
            raise InfrastructureError(
                f"Could not list tables and views in {c}.{s}",
                detail="403 PERMISSION_DENIED",
            )
        return [{"name": "orders", "table_type": "MANAGED"}]

    def object_kind_for_table_type(self, t):
        return UnityCatalog.object_kind_for_table_type(t)

    def get_table_columns(self, c, s, n):
        if self._columns_fail:
            raise InfrastructureError(
                f"Could not read columns of {c}.{s}.{n}", detail="no privilege"
            )
        return [{"name": "id", "type": "bigint", "comment": ""}]

    def get_table_comment(self, c, s, n):
        return ""

    def check_table_select_permission(self, c, s, n, **kwargs):
        return {"can_select": True, "error": None}


@pytest.mark.unit
class TestLoadSchemaMetadataListingFailure:
    def test_reports_failure_instead_of_no_tables(self):
        svc = MetadataService(catalog_svc=_FailingCatalog(listing_fails=True))
        ok, msg, meta = svc.load_schema_metadata("c", "s")
        assert ok is False
        assert msg == "Could not list tables and views in c.s"
        assert meta == {}


@pytest.mark.unit
class TestLoadSelectedTablesListingFailure:
    def test_degrades_object_kind_to_table(self):
        svc = MetadataService(catalog_svc=_FailingCatalog(listing_fails=True))
        ok, _msg, meta = svc.load_selected_tables("c", "s", ["orders"])
        assert ok is True
        assert meta["tables"][0]["object_kind"] == "table"


@pytest.mark.unit
class TestColumnReadFailure:
    def test_table_is_flagged_with_error_not_empty_columns(self):
        svc = MetadataService(catalog_svc=_FailingCatalog(columns_fail=True))
        _ok, _msg, meta = svc.load_selected_tables("c", "s", ["orders"])
        table = meta["tables"][0]
        assert table["error"] == "Could not read columns of c.s.orders"
        assert table["can_select"] is False
