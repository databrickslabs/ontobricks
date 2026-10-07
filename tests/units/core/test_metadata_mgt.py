"""Tests for back.core.databricks.uc.MetadataService — metadata management."""

import importlib
from unittest.mock import patch

_metadata_service_mod = importlib.import_module("back.core.databricks.uc.MetadataService")

from back.core.databricks.uc.UnityCatalog import UnityCatalog

from back.core.databricks import (
    MetadataService,
    build_metadata_dict,
    extract_catalog_schema_from_full_name,
    get_catalog_schema_from_metadata,
    has_metadata,
    validate_metadata,
)


class TestBuildMetadataDict:
    def test_basic(self):
        tables = [{"full_name": "c.s.t1"}, {"full_name": "c.s.t2"}]
        result = build_metadata_dict(tables)
        assert result["table_count"] == 2
        assert len(result["tables"]) == 2

    def test_empty(self):
        result = build_metadata_dict([])
        assert result["table_count"] == 0


class TestValidateMetadata:
    def test_valid(self):
        ok, msg = validate_metadata({"tables": [{"full_name": "c.s.t"}]})
        assert ok is True

    def test_empty_dict(self):
        ok, msg = validate_metadata({})
        assert ok is False

    def test_none(self):
        ok, msg = validate_metadata(None)
        assert ok is False


class TestHasMetadata:
    def test_with_tables(self):
        assert has_metadata({"tables": [{"name": "t"}]}) is True

    def test_empty_tables(self):
        assert has_metadata({"tables": []}) is False

    def test_none(self):
        assert has_metadata(None) is False

    def test_no_tables_key(self):
        assert has_metadata({"other": "data"}) is False


class TestExtractCatalogSchema:
    def test_three_parts(self):
        cat, sch, tbl = extract_catalog_schema_from_full_name("catalog.schema.table")
        assert cat == "catalog"
        assert sch == "schema"
        assert tbl == "table"

    def test_two_parts(self):
        cat, sch, tbl = extract_catalog_schema_from_full_name("schema.table")
        assert cat == ""
        assert sch == "schema"
        assert tbl == "table"

    def test_one_part(self):
        cat, sch, tbl = extract_catalog_schema_from_full_name("table")
        assert cat == ""
        assert sch == ""
        assert tbl == "table"


class TestGetCatalogSchemaFromMetadata:
    def test_with_full_name(self):
        metadata = {"tables": [{"full_name": "cat.sch.tbl"}]}
        cat, sch = get_catalog_schema_from_metadata(metadata)
        assert cat == "cat"
        assert sch == "sch"

    def test_empty_tables(self):
        cat, sch = get_catalog_schema_from_metadata({"tables": []})
        assert cat == ""
        assert sch == ""

    def test_no_full_name(self):
        cat, sch = get_catalog_schema_from_metadata({"tables": [{"name": "tbl"}]})
        assert cat == ""
        assert sch == ""


class TestMetadataService:
    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_load_schema_metadata(self, MockAuth, MockCatalog):
        cat_instance = MockCatalog.return_value
        cat_instance.list_tables_and_views.return_value = [
            {"name": "t1", "table_type": "MANAGED"},
            {"name": "t2", "table_type": "MANAGED"},
        ]
        cat_instance.object_kind_for_table_type.side_effect = (
            UnityCatalog.object_kind_for_table_type
        )
        cat_instance.get_table_columns.return_value = [
            {"name": "id", "type": "int", "comment": "PK"}
        ]
        cat_instance.get_table_comment.return_value = "Table desc"

        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, metadata = svc.load_schema_metadata("cat", "sch")
        assert ok is True
        assert metadata["table_count"] == 2
        assert metadata["tables"][0]["full_name"] == "cat.sch.t1"

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_load_schema_no_tables(self, MockAuth, MockCatalog):
        MockCatalog.return_value.get_tables.return_value = []
        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, metadata = svc.load_schema_metadata("cat", "sch")
        assert ok is False

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_load_with_existing(self, MockAuth, MockCatalog):
        cat_instance = MockCatalog.return_value
        cat_instance.list_tables_and_views.return_value = [
            {"name": "t1", "table_type": "MANAGED"},
            {"name": "t2", "table_type": "MANAGED"},
        ]
        cat_instance.object_kind_for_table_type.side_effect = (
            UnityCatalog.object_kind_for_table_type
        )
        cat_instance.get_table_columns.return_value = []
        cat_instance.get_table_comment.return_value = ""

        existing = {"tables": [{"name": "t1", "full_name": "c.s.t1", "columns": []}]}
        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, metadata = svc.load_schema_metadata("c", "s", existing)
        assert ok is True
        assert metadata["table_count"] == 2

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_load_selected_tables(self, MockAuth, MockCatalog):
        cat_instance = MockCatalog.return_value
        cat_instance.get_table_columns.return_value = []
        cat_instance.get_table_comment.return_value = ""

        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, metadata = svc.load_selected_tables("c", "s", ["t1"])
        assert ok is True
        assert metadata["table_count"] == 1

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_load_selected_no_tables(self, MockAuth, MockCatalog):
        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, metadata = svc.load_selected_tables("c", "s", [])
        assert ok is False

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_load_selected_all_existing(self, MockAuth, MockCatalog):
        existing = {"tables": [{"name": "t1", "full_name": "c.s.t1", "columns": []}]}
        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, metadata = svc.load_selected_tables("c", "s", ["t1"], existing)
        assert ok is True
        assert "already loaded" in msg.lower()

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_get_table_metadata(self, MockAuth, MockCatalog):
        cat_instance = MockCatalog.return_value
        cat_instance.get_table_columns.return_value = [{"name": "col1"}]
        cat_instance.get_table_comment.return_value = "desc"

        svc = MetadataService(host="h", token="t", warehouse_id="w")
        result = svc.get_table_metadata("c", "s", "t1")
        assert result["name"] == "t1"
        assert result["full_name"] == "c.s.t1"

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_refresh_table_metadata(self, MockAuth, MockCatalog):
        cat_instance = MockCatalog.return_value
        cat_instance.get_table_columns.return_value = [{"name": "new_col"}]
        cat_instance.get_table_comment.return_value = "updated"

        existing = {
            "tables": [
                {"name": "t1", "full_name": "c.s.t1", "columns": [{"name": "old"}]}
            ],
            "table_count": 1,
        }
        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, updated = svc.refresh_table_metadata("c", "s", "t1", existing)
        assert ok is True
        assert updated["tables"][0]["comment"] == "updated"

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_refresh_adds_new_table(self, MockAuth, MockCatalog):
        cat_instance = MockCatalog.return_value
        cat_instance.get_table_columns.return_value = []
        cat_instance.get_table_comment.return_value = ""

        existing = {"tables": [], "table_count": 0}
        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, updated = svc.refresh_table_metadata("c", "s", "new_tbl", existing)
        assert ok is True
        assert updated["table_count"] == 1

    def test_clamp_import_limit_defaults_to_40(self):
        assert MetadataService.DEFAULT_DATA_ASSETS_IMPORT_LIMIT == 40
        assert MetadataService.clamp_import_limit(None) == 40
        assert MetadataService.clamp_import_limit("40") == 40
        assert MetadataService.clamp_import_limit(0) == 1
        assert MetadataService.clamp_import_limit(9999) == 500

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_load_schema_metadata_caps_at_import_limit(self, MockAuth, MockCatalog):
        cat_instance = MockCatalog.return_value
        cat_instance.list_tables_and_views.return_value = [
            {"name": f"t{i}", "table_type": "MANAGED"} for i in range(45)
        ]
        cat_instance.object_kind_for_table_type.side_effect = (
            UnityCatalog.object_kind_for_table_type
        )
        cat_instance.get_table_columns.return_value = []
        cat_instance.get_table_comment.return_value = ""

        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, metadata = svc.load_schema_metadata("c", "s")
        assert ok is True
        assert metadata["table_count"] == 40
        assert "40" in msg
        assert "5" in msg

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_load_selected_tables_caps_new_names(self, MockAuth, MockCatalog):
        cat_instance = MockCatalog.return_value
        cat_instance.list_tables_and_views.return_value = []
        cat_instance.object_kind_for_table_type.side_effect = (
            UnityCatalog.object_kind_for_table_type
        )
        cat_instance.get_table_columns.return_value = []
        cat_instance.get_table_comment.return_value = ""

        existing = {
            "tables": [{"name": f"old{i}", "full_name": f"c.s.old{i}"} for i in range(38)]
        }
        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, metadata = svc.load_selected_tables(
            "c", "s", ["n1", "n2", "n3"], existing, import_limit=40
        )
        assert ok is True
        assert metadata["table_count"] == 40
        names = {t["name"] for t in metadata["tables"]}
        assert "n1" in names and "n2" in names
        assert "n3" not in names

    @patch.object(_metadata_service_mod, "UnityCatalog")
    @patch.object(_metadata_service_mod, "DatabricksAuth")
    def test_load_selected_tables_fails_when_limit_reached(self, MockAuth, MockCatalog):
        existing = {
            "tables": [{"name": f"t{i}", "full_name": f"c.s.t{i}"} for i in range(40)]
        }
        svc = MetadataService(host="h", token="t", warehouse_id="w")
        ok, msg, metadata = svc.load_selected_tables(
            "c", "s", ["extra"], existing, import_limit=40
        )
        assert ok is False
        assert "limit" in msg.lower()
        assert metadata["table_count"] == 40
