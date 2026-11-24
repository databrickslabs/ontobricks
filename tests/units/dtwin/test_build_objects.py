"""Build → Objects: per-backend inventory of the current context version."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.objects.digitaltwin.build.BuildObjects import BuildObjects, usage_of

pytestmark = pytest.mark.unit

_FACTORY = "back.objects.digitaltwin.build.BuildObjects.GraphDBFactory"
_SETTINGS = "back.objects.domain.settings.SettingsService.SettingsService"


def _domain(backend, name="Sales", version="1"):
    return SimpleNamespace(
        info={"name": name, "graph_backend": backend, "neo4j_connection": "aura"},
        current_version=version,
        delta={"catalog": "main", "schema": "reg"},
    )


# --- naming rules ----------------------------------------------------------


@pytest.mark.parametrize(
    "name,kind,expected",
    [
        ("sales_v1", "view", "sales_v1"),
        ("sales_v1_sync", "table", "sales_v1"),
        ("sales_v1__app", "table", "sales_v1"),
        ("sales_v1_adj_out", "table", "sales_v1"),
        ("sales_v1_entity_search_asserted", "table", "sales_v1"),
        ("sales_v1__sim_ab12", "table", "sales_v1"),
        ("sales_v1__smu_ab12", "view", "sales_v1"),
        # a view named like a companion is still a view of its own base
        ("sales_v1_sync", "view", "sales_v1_sync"),
    ],
)
def test_pg_base(name, kind, expected):
    assert BuildObjects.pg_base(name, kind) == expected


def test_uc_sync_base_and_graph_key():
    assert BuildObjects.uc_sync_base("sales_v1_sync") == "sales_v1"
    assert BuildObjects.uc_sync_base("sales_v1") == "sales_v1"
    assert BuildObjects._graph_key("Sales_V12") == "sales_12"
    assert BuildObjects._graph_key("nope") == ""


# --- dispatch --------------------------------------------------------------


def test_graphless_domain_reports_a_message():
    with patch(_FACTORY) as factory:
        factory._resolve_graph_backend.return_value = "none"
        out = BuildObjects.list_for_domain(_domain("none"), MagicMock(), MagicMock())
    assert out["groups"] == []
    assert out["backend"] == "none"
    assert "no graph backend" in out["message"]


# --- Delta -----------------------------------------------------------------


def _delta_probe(raw, key="sales_1"):
    probe = MagicMock()
    probe.fetch_schema_tables.return_value = raw
    probe.domain_match_key.side_effect = lambda n: key if "sales" in n.lower() else "other_1"
    probe.group_triplestore_objects.return_value = {
        "triplestore_sales_V1": {
            "base": "triplestore_sales_V1",
            "sorted_items": [
                {"kind": "view", "name": "triplestore_sales_V1", "full_name": "main.reg.triplestore_sales_V1"},
                {"kind": "table", "name": "triplestore_sales_V1_data", "full_name": "main.reg.triplestore_sales_V1_data"},
            ],
        },
        "triplestore_other_V1": {
            "base": "triplestore_other_V1",
            "sorted_items": [
                {"kind": "view", "name": "triplestore_other_V1", "full_name": "main.reg.triplestore_other_V1"}
            ],
        },
    }
    return probe


def test_delta_lists_the_version_and_its_analytics_under_unity_catalog():
    probe = _delta_probe([])
    analytics = (
        "main.reg",
        [
            {"key": "sales_1", "items": [
                {"name": "graph_metrics_sales_1", "full_name": "main.reg.graph_metrics_sales_1"},
                {"name": "graph_metrics_sales_1_work_x", "full_name": "main.reg.graph_metrics_sales_1_work_x"},
            ]},
            {"key": "other_1", "items": [
                {"name": "graph_metrics_other_1", "full_name": "main.reg.graph_metrics_other_1"}
            ]},
        ],
        "",
    )
    with patch(_FACTORY) as factory, patch(
        f"{_SETTINGS}._analytics_objects", return_value=analytics
    ):
        factory._resolve_graph_backend.return_value = "databricks"
        factory.get_health_probe.return_value = probe
        out = BuildObjects.list_for_domain(
            _domain("databricks"), MagicMock(), MagicMock()
        )
    (store,) = out["groups"]
    assert out["backend"] == "databricks"
    assert (store["key"], store["title"]) == ("unity_catalog", "Unity Catalog")
    assert [(i["kind"], i["name"]) for i in store["items"]] == [
        ("view", "triplestore_sales_V1"),
        ("table", "triplestore_sales_V1_data"),
        ("metrics", "graph_metrics_sales_1"),
        ("work", "graph_metrics_sales_1_work_x"),
    ]
    assert store["items"][0]["drop"] == {
        "api": "uc",
        "full_name": "main.reg.triplestore_sales_V1",
        "is_sync": False,
    }


def test_delta_listing_failure_is_an_error_group_not_a_failed_tab():
    probe = MagicMock()
    probe.fetch_schema_tables.side_effect = RuntimeError("uc down")
    probe.domain_match_key.return_value = "sales_1"
    with patch(_FACTORY) as factory:
        factory._resolve_graph_backend.return_value = "databricks"
        factory.get_health_probe.return_value = probe
        out = BuildObjects.list_for_domain(
            _domain("databricks"), MagicMock(), MagicMock()
        )
    assert out["success"] is True
    assert out["groups"][0]["items"] == []
    assert "uc down" in out["groups"][0]["error"]


def test_analytics_scan_error_is_reported_on_the_unity_catalog_group():
    probe = _delta_probe([])
    with patch(_FACTORY) as factory, patch(
        f"{_SETTINGS}._analytics_objects",
        return_value=("main.out", [], "Could not list analytics tables in main.out"),
    ):
        factory._resolve_graph_backend.return_value = "databricks"
        factory.get_health_probe.return_value = probe
        out = BuildObjects.list_for_domain(
            _domain("databricks"), MagicMock(), MagicMock()
        )
    assert [g["key"] for g in out["groups"]] == ["unity_catalog"]
    assert "Could not list" in out["groups"][0]["error"]

    with patch(_FACTORY) as factory, patch(
        f"{_SETTINGS}._analytics_objects", return_value=("main.reg", [], "")
    ):
        factory._resolve_graph_backend.return_value = "databricks"
        factory.get_health_probe.return_value = probe
        out = BuildObjects.list_for_domain(
            _domain("databricks"), MagicMock(), MagicMock()
        )
    assert [g["key"] for g in out["groups"]] == ["unity_catalog"]
    assert out["groups"][0]["error"] == ""


# --- Lakebase --------------------------------------------------------------


def test_lakebase_scopes_postgres_and_uc_objects_to_the_version():
    pg = {
        "registry_schema": "ontobricks_registry",
        "views": [
            {"schema": "graph", "name": "sales_v1"},
            {"schema": "graph", "name": "other_v1"},
        ],
        "tables": [
            {"schema": "graph", "name": "sales_v1_sync"},
            {"schema": "graph", "name": "sales_v1__app"},
            {"schema": "graph", "name": "sales_v10_sync"},  # other version
            {"schema": "ontobricks_registry", "name": "sales_v1_sync"},  # hidden
        ],
    }
    uc = {
        "uc_catalog": "main",
        "uc_schema": "graph",
        "uc_tables": [
            {"name": "sales_v1", "full_name": "main.graph.sales_v1", "table_type": "MANAGED"},
            {
                "name": "sales_v1_sync",
                "full_name": "main.graph.sales_v1_sync",
                "is_sync": True,
                "state": "ONLINE",
            },
            {"name": "other_v1_sync", "full_name": "main.graph.other_v1_sync", "is_sync": True},
        ],
    }
    with patch(_FACTORY) as factory, patch(
        f"{_SETTINGS}.graph_engine_lakebase_objects_result", return_value=pg
    ), patch(
        f"{_SETTINGS}.graph_engine_lakebase_sync_objects_result", return_value=uc
    ), patch(
        f"{_SETTINGS}._analytics_objects", return_value=("main.reg", [], "")
    ):
        factory._resolve_graph_backend.return_value = "lakebase"
        factory.get_health_probe.return_value = _delta_probe([])
        out = BuildObjects.list_for_domain(
            _domain("lakebase"), MagicMock(), MagicMock()
        )
    postgres, unity = out["groups"]
    # views drop before the tables they read
    assert [(i["kind"], i["name"]) for i in postgres["items"]] == [
        ("view", "sales_v1"),
        ("table", "sales_v1__app"),
        ("table", "sales_v1_sync"),
    ]
    assert postgres["items"][0]["drop"] == {
        "api": "lakebase",
        "kind": "view",
        "schema": "graph",
        "name": "sales_v1",
    }
    # the Lakeflow registration drops before its source table
    assert [(i["kind"], i["drop"]["is_sync"]) for i in unity["items"]] == [
        ("sync", True),
        ("table", False),
    ]
    assert unity["items"][0]["state"] == "ONLINE"
    assert unity["location"] == "main.graph"


def test_lakebase_analytics_join_the_unity_catalog_group():
    uc = {"uc_catalog": "main", "uc_schema": "graph", "uc_tables": []}
    analytics = (
        "main.reg",
        [{"key": "sales_1", "items": [
            {"name": "graph_metrics_sales_1", "full_name": "main.reg.graph_metrics_sales_1"}
        ]}],
        "",
    )
    with patch(_FACTORY) as factory, patch(
        f"{_SETTINGS}.graph_engine_lakebase_objects_result", return_value={}
    ), patch(
        f"{_SETTINGS}.graph_engine_lakebase_sync_objects_result", return_value=uc
    ), patch(f"{_SETTINGS}._analytics_objects", return_value=analytics):
        factory._resolve_graph_backend.return_value = "lakebase"
        factory.get_health_probe.return_value = _delta_probe([])
        out = BuildObjects.list_for_domain(_domain("lakebase"), MagicMock(), MagicMock())
    assert [g["key"] for g in out["groups"]] == ["postgres", "unity_catalog"]
    unity = out["groups"][1]
    assert [i["name"] for i in unity["items"]] == ["graph_metrics_sales_1"]
    assert unity["location"] == "main.graph, main.reg"


# --- Neo4j -----------------------------------------------------------------


def test_neo4j_lists_only_the_versions_graph_with_its_connection():
    labels = {
        "database": "neo4j",
        "graphs": [
            {"label": "Sales_V1", "nodes": 12, "edges": 7},
            {"label": "Other_V1", "nodes": 1, "edges": 0},
        ],
    }
    with patch(_FACTORY) as factory, patch(
        f"{_SETTINGS}.graph_engine_neo4j_labels_result", return_value=labels
    ) as listing:
        factory._resolve_graph_backend.return_value = "neo4j"
        factory.get_builder.return_value.sanitise_label.return_value = "Sales_V1"
        out = BuildObjects.list_for_domain(_domain("neo4j"), MagicMock(), MagicMock())
    (graph,) = out["groups"]
    assert listing.call_args.kwargs["connection_name"] == "aura"
    assert graph["items"] == [
        {
            "kind": "graph",
            "name": "Sales_V1",
            "detail": "12 nodes · 7 edges",
            "usage": "graph",
            "drop": {"api": "neo4j", "label": "Sales_V1", "connection_name": "aura"},
        }
    ]
    assert [u["key"] for u in out["usages"]] == [
        "graph",
        "inferred",
        "cache",
        "simulation",
        "analytics",
    ]


# --- usage ----------------------------------------------------------------


@pytest.mark.parametrize(
    "name,usage",
    [
        ("sales_v1", "graph"),
        ("sales_v1_sync", "graph"),
        ("triplestore_sales_V1_data", "graph"),
        ("triplestore_sales_V1_graph", "graph"),
        ("Sales_V1", "graph"),
        ("sales_v1__app", "inferred"),
        ("triplestore_sales_V1_inferred", "inferred"),
        ("sales_v1_adj_out", "cache"),
        ("sales_v1_adj_in", "cache"),
        ("sales_v1_props", "cache"),
        ("sales_v1_entity_search", "cache"),
        ("sales_v1_entity_search_asserted", "cache"),
        ("sales_v1__sim_ab12", "simulation"),
        ("sales_v1_adj_out__smu_ab12", "simulation"),
        ("graph_metrics_sales_1", "analytics"),
        ("graph_metrics_sales_1_work_x", "analytics"),
        ("triplestore_sales_V1_analytics", "analytics"),
    ],
)
def test_usage_of(name, usage):
    assert usage_of(name) == usage


def test_pg_items_carry_their_usage_and_views_stay_first():
    pg = {
        "registry_schema": "ontobricks_registry",
        "views": [{"schema": "graph", "name": "sales_v1"}],
        "tables": [
            {"schema": "graph", "name": "sales_v1__app"},
            {"schema": "graph", "name": "sales_v1_adj_out"},
            {"schema": "graph", "name": "sales_v1_sync"},
        ],
    }
    with patch(_FACTORY) as factory, patch(
        f"{_SETTINGS}.graph_engine_lakebase_objects_result", return_value=pg
    ), patch(
        f"{_SETTINGS}.graph_engine_lakebase_sync_objects_result",
        return_value={"uc_tables": []},
    ), patch(f"{_SETTINGS}._analytics_objects", return_value=("main.reg", [], "")):
        factory._resolve_graph_backend.return_value = "lakebase"
        factory.get_health_probe.return_value = _delta_probe([])
        out = BuildObjects.list_for_domain(_domain("lakebase"), MagicMock(), MagicMock())
    items = out["groups"][0]["items"]
    assert [(i["name"], i["usage"]) for i in items] == [
        ("sales_v1", "graph"),
        ("sales_v1__app", "inferred"),
        ("sales_v1_adj_out", "cache"),
        ("sales_v1_sync", "graph"),
    ]
