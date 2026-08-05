"""Tests for Delta triple-store UC object listing (Settings → Storage tab)."""

from unittest.mock import MagicMock, patch

from back.core.graphdb.delta.objects import (
    analytics_base,
    analytics_match_key,
    domain_match_key,
    group_analytics_objects,
    group_triplestore_objects,
    object_base,
    uc_object_kind,
)
from back.objects.domain.SettingsService import SettingsService

REGISTRY_CFG = {"catalog": "reg_cat", "schema": "reg_sch", "volume": "vol"}


def _settings(output_schema: str = ""):
    settings = MagicMock()
    settings.analytics_job_output_schema = output_schema
    return settings


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


class TestAnalyticsObjectHelpers:
    def test_analytics_base_strips_companions_and_work_tables(self):
        assert analytics_base("graph_metrics_foo_1") == "graph_metrics_foo_1"
        assert analytics_base("graph_metrics_foo_1_summary") == "graph_metrics_foo_1"
        assert analytics_base("graph_metrics_foo_1_type_profiles") == "graph_metrics_foo_1"
        assert analytics_base("graph_metrics_foo_1_type_predicates") == "graph_metrics_foo_1"
        assert analytics_base("graph_metrics_foo_1_work_edges") == "graph_metrics_foo_1"
        assert analytics_base("graph_metrics_foo_1_work") == "graph_metrics_foo_1"

    def test_match_keys_agree_for_an_ordinary_domain_name(self):
        assert domain_match_key("triplestore_foo_V1") == "foo_1"
        assert analytics_match_key("graph_metrics_foo_1_summary") == "foo_1"

    def test_match_keys_diverge_for_a_punctuated_domain_name(self):
        # The view name replaces "." with "_", sanitize_domain_folder drops it.
        # The mismatch is what puts such a group in the orphan card.
        assert domain_match_key("triplestore_my_domain_V1") == "my_domain_1"
        assert analytics_match_key("graph_metrics_mydomain_1") == "mydomain_1"

    def test_domain_match_key_ignores_unparseable_names(self):
        assert domain_match_key("other_table") == ""

    def test_analytics_match_key_ignores_non_analytics_names(self):
        assert analytics_match_key("triplestore_foo_V1") == ""

    def test_group_analytics_objects_groups_and_sorts(self):
        raw = [
            {"name": "graph_metrics_a_1", "table_type": "MANAGED"},
            {"name": "graph_metrics_a_1_summary", "table_type": "MANAGED"},
            {"name": "graph_metrics_a_1_type_profiles", "table_type": "MANAGED"},
            {"name": "graph_metrics_a_1_type_predicates", "table_type": "MANAGED"},
            {"name": "graph_metrics_a_1_work_edges", "table_type": "MANAGED"},
            {"name": "triplestore_a_V1", "table_type": "VIEW"},
        ]
        groups = group_analytics_objects(raw, "reg_cat", "reg_sch")
        assert set(groups) == {"a_1"}
        items = groups["a_1"]["sorted_items"]
        assert [i["name"] for i in items] == [
            "graph_metrics_a_1_work_edges",
            "graph_metrics_a_1_type_predicates",
            "graph_metrics_a_1_type_profiles",
            "graph_metrics_a_1_summary",
            "graph_metrics_a_1",
        ]
        assert items[0]["full_name"] == "reg_cat.reg_sch.graph_metrics_a_1_work_edges"
        assert groups["a_1"]["base"] == "graph_metrics_a_1"


class TestTripleStoreDatabricksObjectsResult:
    def test_returns_empty_when_registry_not_configured(self):
        session_mgr = MagicMock()
        settings = _settings()
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
        settings = _settings()
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
        assert out["domains"][0]["key"] == "x_2"
        assert len(out["domains"][0]["items"]) == 2

    def _run(self, raw_tables, settings=None, fetch=None):
        with patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
        ), patch(
            "back.core.graphdb.delta.objects.fetch_uc_schema_tables",
            side_effect=fetch,
            return_value=raw_tables,
        ):
            return SettingsService.triple_store_databricks_objects_result(
                MagicMock(), settings or _settings()
            )

    def test_matching_analytics_is_not_an_orphan(self):
        out = self._run(
            [
                {"name": "triplestore_x_V2", "table_type": "VIEW"},
                {"name": "graph_metrics_x_2", "table_type": "MANAGED"},
                {"name": "graph_metrics_x_2_summary", "table_type": "MANAGED"},
            ]
        )
        assert out["analytics_location"] == "reg_cat.reg_sch"
        assert out["analytics_message"] == ""
        assert [g["key"] for g in out["analytics"]] == ["x_2"]
        assert len(out["analytics"][0]["items"]) == 2
        assert out["orphans"] == []

    def test_unmatched_analytics_is_an_orphan(self):
        out = self._run(
            [
                {"name": "triplestore_x_V2", "table_type": "VIEW"},
                {"name": "graph_metrics_gone_9", "table_type": "MANAGED"},
            ]
        )
        assert [g["key"] for g in out["orphans"]] == ["gone_9"]
        assert out["domains"][0]["base"] == "triplestore_x_V2"

    def test_configured_output_schema_is_scanned_separately(self):
        registry = [{"name": "triplestore_x_V2", "table_type": "VIEW"}]
        analytics = [{"name": "graph_metrics_x_2", "table_type": "MANAGED"}]

        def fetch(catalog, schema):
            return analytics if (catalog, schema) == ("an_cat", "an_sch") else registry

        out = self._run(registry, settings=_settings("an_cat.an_sch"), fetch=fetch)
        assert out["analytics_location"] == "an_cat.an_sch"
        assert [g["key"] for g in out["analytics"]] == ["x_2"]
        assert out["analytics"][0]["items"][0]["full_name"] == "an_cat.an_sch.graph_metrics_x_2"

    def test_analytics_scan_failure_still_lists_domains(self):
        registry = [{"name": "triplestore_x_V2", "table_type": "VIEW"}]

        def fetch(catalog, schema):
            if (catalog, schema) == ("an_cat", "an_sch"):
                raise RuntimeError("PERMISSION_DENIED")
            return registry

        out = self._run(registry, settings=_settings("an_cat.an_sch"), fetch=fetch)
        assert out["success"] is True
        assert len(out["domains"]) == 1
        assert out["analytics"] == []
        assert out["orphans"] == []
        assert "an_cat.an_sch" in out["analytics_message"]
