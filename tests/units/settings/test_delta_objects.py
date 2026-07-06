"""Tests for Delta triple-store UC object listing (Settings → Storage tab)."""

from unittest.mock import MagicMock, patch

from back.core.graphdb.delta.objects import (
    group_triplestore_objects,
    object_base,
    uc_object_kind,
)
from back.objects.domain.SettingsService import SettingsService

REGISTRY_CFG = {"catalog": "reg_cat", "schema": "reg_sch", "volume": "vol"}


class TestDeltaObjectHelpers:
    def test_object_base_strips_suffixes(self):
        assert object_base("triplestore_foo_V1") == "triplestore_foo_V1"
        assert object_base("triplestore_foo_V1_data") == "triplestore_foo_V1"
        assert object_base("triplestore_foo_V1_inferred") == "triplestore_foo_V1"
        assert object_base("triplestore_foo_V1_graph") == "triplestore_foo_V1"

    def test_uc_object_kind(self):
        assert uc_object_kind("VIEW") == "view"
        assert uc_object_kind("MANAGED") == "table"

    def test_group_triplestore_objects_groups_and_sorts(self):
        raw = [
            {"name": "triplestore_a_V1_data", "table_type": "MANAGED"},
            {"name": "triplestore_a_V1_graph", "table_type": "VIEW"},
            {"name": "triplestore_a_V1", "table_type": "VIEW"},
            {"name": "other_table", "table_type": "MANAGED"},
            {"name": "triplestore_a_V1_inferred", "table_type": "MANAGED"},
        ]
        groups = group_triplestore_objects(raw, "reg_cat", "reg_sch")
        assert set(groups) == {"triplestore_a_V1"}
        items = groups["triplestore_a_V1"]["sorted_items"]
        names = [i["name"] for i in items]
        assert names == [
            "triplestore_a_V1_graph",
            "triplestore_a_V1",
            "triplestore_a_V1_data",
            "triplestore_a_V1_inferred",
        ]
        assert items[0]["full_name"] == "reg_cat.reg_sch.triplestore_a_V1_graph"


class TestTripleStoreDatabricksObjectsResult:
    def test_returns_empty_when_registry_not_configured(self):
        session_mgr = MagicMock()
        settings = MagicMock()
        with patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "h", "t", {"catalog": "", "schema": ""}),
        ):
            out = SettingsService.triple_store_databricks_objects_result(session_mgr, settings)
        assert out["success"] is True
        assert out["registry_configured"] is False
        assert out["domains"] == []

    def test_lists_grouped_domains(self):
        session_mgr = MagicMock()
        settings = MagicMock()
        raw_tables = [
            {"name": "triplestore_x_V2", "table_type": "VIEW"},
            {"name": "triplestore_x_V2_data", "table_type": "MANAGED"},
        ]
        with patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
        ), patch(
            "back.core.graphdb.delta.objects.fetch_uc_schema_tables",
            return_value=raw_tables,
        ):
            out = SettingsService.triple_store_databricks_objects_result(session_mgr, settings)

        assert out["success"] is True
        assert out["registry_configured"] is True
        assert out["storage_location"] == "reg_cat.reg_sch"
        assert len(out["domains"]) == 1
        assert out["domains"][0]["base"] == "triplestore_x_V2"
        assert len(out["domains"][0]["items"]) == 2
