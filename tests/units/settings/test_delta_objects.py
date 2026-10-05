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
        # An analytics snapshot only outlives its run when that run died, and
        # grouping it is what surfaces the leftover for purging.
        assert object_base("triplestore_foo_V1_analytics") == "triplestore_foo_V1"

    def test_object_base_strips_graph_index_companions(self):
        """Adjacency / search / props tables belong to the same domain group.

        Settings → Lakehouse Delete uses object_base() as the group key. If
        these suffixes are not stripped, a purge of triplestore_foo_V1 leaves
        the index tables behind and the next Knowledge Graph build collides.
        """
        for suffix in (
            "_adj_out",
            "_adj_in",
            "_entity_search",
            "_entity_search_asserted",
            "_props",
        ):
            assert object_base(f"triplestore_foo_V1{suffix}") == "triplestore_foo_V1"

    def test_group_includes_graph_index_companions(self):
        raw = [
            {"name": "triplestore_a_V1", "table_type": "VIEW"},
            {"name": "triplestore_a_V1_adj_out", "table_type": "MANAGED"},
            {"name": "triplestore_a_V1_adj_in", "table_type": "MANAGED"},
            {"name": "triplestore_a_V1_entity_search", "table_type": "MANAGED"},
            {"name": "triplestore_a_V1_entity_search_asserted", "table_type": "MANAGED"},
            {"name": "triplestore_a_V1_props", "table_type": "MANAGED"},
        ]
        groups = group_triplestore_objects(raw, "reg_cat", "reg_sch")
        assert set(groups) == {"triplestore_a_V1"}
        names = [i["name"] for i in groups["triplestore_a_V1"]["sorted_items"]]
        assert names[0] == "triplestore_a_V1"
        assert set(names[1:]) == {
            "triplestore_a_V1_adj_out",
            "triplestore_a_V1_adj_in",
            "triplestore_a_V1_entity_search",
            "triplestore_a_V1_entity_search_asserted",
            "triplestore_a_V1_props",
        }

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

    def test_a_view_only_domain_drops_data_before_the_gateway(self):
        """``_data`` is a view here, and it reads from the gateway view.

        Dropping the gateway first would leave ``_data`` dangling mid-purge,
        so it has to sort between ``_graph`` and the gateway.
        """
        raw = [
            {"name": "triplestore_a_V1", "table_type": "VIEW"},
            {"name": "triplestore_a_V1_data", "table_type": "VIEW"},
            {"name": "triplestore_a_V1_graph", "table_type": "VIEW"},
            {"name": "triplestore_a_V1_inferred", "table_type": "MANAGED"},
        ]
        groups = group_triplestore_objects(raw, "reg_cat", "reg_sch")
        names = [i["name"] for i in groups["triplestore_a_V1"]["sorted_items"]]
        assert names == [
            "triplestore_a_V1_graph",
            "triplestore_a_V1_data",
            "triplestore_a_V1",
            "triplestore_a_V1_inferred",
        ]


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
        ), patch.object(
            SettingsService,
            "_lakehouse_domain_version_keys",
            return_value={"x_2"},
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

    def test_lists_only_lakehouse_domain_versions(self):
        registry_service = MagicMock()
        registry_service.list_domain_details_cached.return_value = (
            True,
            [
                {
                    "name": "x",
                    "versions": [
                        {"version": "1", "graph_backend": "databricks"},
                        {"version": "2", "graph_backend": "lakebase"},
                    ],
                },
                {
                    "name": "y",
                    "versions": [
                        {"version": "1", "graph_backend": "neo4j"},
                    ],
                },
            ],
            "",
        )
        raw_tables = [
            {"name": "triplestore_x_V1", "table_type": "VIEW"},
            {"name": "triplestore_x_V2", "table_type": "VIEW"},
            {"name": "triplestore_y_V1", "table_type": "VIEW"},
            {"name": "graph_metrics_x_1", "table_type": "MANAGED"},
            {"name": "graph_metrics_x_2", "table_type": "MANAGED"},
            {"name": "graph_metrics_orphan_9", "table_type": "MANAGED"},
        ]

        with patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
        ), patch(
            "back.core.graphdb.delta.objects.fetch_uc_schema_tables",
            return_value=raw_tables,
        ), patch(
            "back.objects.domain.SettingsService.RegistryService.from_context",
            return_value=registry_service,
        ):
            out = SettingsService.triple_store_databricks_objects_result(
                MagicMock(), _settings()
            )

        assert [domain["key"] for domain in out["domains"]] == ["x_1"]
        assert [group["key"] for group in out["analytics"]] == ["x_1"]
        assert out["orphans"] == []

    def _run(self, raw_tables, settings=None, fetch=None):
        with patch.object(
            SettingsService,
            "_resolve_context",
            return_value=(MagicMock(), "h", "t", REGISTRY_CFG),
        ), patch.object(
            SettingsService,
            "_lakehouse_domain_version_keys",
            return_value={"x_2"},
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

    def test_unmatched_analytics_is_hidden(self):
        out = self._run(
            [
                {"name": "triplestore_x_V2", "table_type": "VIEW"},
                {"name": "graph_metrics_gone_9", "table_type": "MANAGED"},
            ]
        )
        assert out["analytics"] == []
        assert out["orphans"] == []
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
