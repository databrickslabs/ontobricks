"""Tests for back.core.databricks.DashboardService."""
import json
import pytest
from unittest.mock import MagicMock, patch

from back.core.databricks.DatabricksAuth import DatabricksAuth
from back.core.databricks.DashboardService import DashboardService


@pytest.fixture
def clean_databricks_env(monkeypatch):
    for key in (
        "DATABRICKS_APP_PORT",
        "DATABRICKS_TOKEN",
        "DATABRICKS_HOST",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
    ):
        monkeypatch.delenv(key, raising=False)


class TestGetDashboards:
    @patch("requests.get")
    def test_lakeview_and_legacy(self, mock_get, clean_databricks_env):
        def _side_effect(url, **kwargs):
            m = MagicMock()
            m.raise_for_status = MagicMock()
            params = kwargs.get("params") or {}
            if "lakeview/dashboards" in url and "page_token" not in params:
                m.json.return_value = {
                    "dashboards": [
                        {
                            "dashboard_id": "lv1",
                            "display_name": "Lake One",
                            "path": "/p1",
                        }
                    ],
                    "next_page_token": "t2",
                }
            elif "lakeview/dashboards" in url:
                m.json.return_value = {
                    "dashboards": [
                        {
                            "dashboard_id": "lv2",
                            "name": "Lake Two",
                            "warehouse_id": "wh",
                        }
                    ],
                    "next_page_token": None,
                }
            elif "preview/sql/dashboards" in url:
                m.json.return_value = {
                    "results": [
                        {
                            "id": "leg1",
                            "name": "Legacy",
                            "slug": "my-slug",
                        }
                    ]
                }
            else:
                m.json.return_value = {}
            return m

        mock_get.side_effect = _side_effect

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = DashboardService(auth)
        dashboards = svc.get_dashboards()
        assert len(dashboards) == 3
        lake = [d for d in dashboards if d["type"] == "lakeview"]
        legacy = [d for d in dashboards if d["type"] == "legacy"]
        assert len(lake) == 2
        assert len(legacy) == 1
        assert legacy[0]["id"] == "leg1"
        assert legacy[0]["url"] == "https://h.com/sql/dashboards/leg1"
        assert lake[0]["url"] == "https://h.com/dashboardsv3/lv1"

    def test_empty_without_auth(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="")
        svc = DashboardService(auth)
        assert svc.get_dashboards() == []


class TestGetDashboardParameters:
    @patch("requests.get")
    def test_extracts_parameters(self, mock_get, clean_databricks_env):
        dash_body = {
            "datasets": [
                {
                    "name": "ds",
                    "displayName": "DS",
                    "parameters": [
                        {
                            "keyword": "kw",
                            "displayName": "KW",
                            "name": "n",
                            "dataType": "STRING",
                            "id": "p1",
                        }
                    ],
                }
            ],
            "parameters": [],
            "pages": [],
        }
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.raise_for_status = MagicMock()
        mock_get.return_value.json.return_value = {
            "display_name": "Dash",
            "path": "/x",
            "serialized_dashboard": json.dumps(dash_body),
        }

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = DashboardService(auth)
        out = svc.get_dashboard_parameters("dash-1")
        assert out.get("error") is None
        assert out["id"] == "dash-1"
        assert out["embed_url"] == "https://h.com/embed/dashboardsv3/dash-1"
        assert len(out["parameters"]) >= 1
        keywords = {p.get("keyword") for p in out["parameters"]}
        assert "kw" in keywords

    def test_missing_id(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = DashboardService(auth)
        out = svc.get_dashboard_parameters("")
        assert out["parameters"] == []
        assert "missing" in out.get("error", "").lower()


class TestExtractParameters:
    def test_dataset_and_dashboard_level(self, clean_databricks_env):
        dash_def = {
            "datasets": [
                {
                    "name": "ds1",
                    "displayName": "DS One",
                    "parameters": [
                        {
                            "keyword": "ds_kw",
                            "displayName": "DS Param",
                            "name": "ds_kw",
                            "dataType": "INTEGER",
                            "id": "id1",
                        }
                    ],
                }
            ],
            "parameters": [
                {"name": "dash_only", "keyword": "dash_kw", "type": "date"}
            ],
        }
        params = DashboardService._extract_parameters(dash_def)
        by_kw = {p["keyword"]: p for p in params}
        assert "ds_kw" in by_kw
        assert by_kw["ds_kw"]["type"] == "integer"
        assert "dash_kw" in by_kw
        assert by_kw["dash_kw"]["dataset"] == ""


class TestLinkFilterWidgets:
    def test_links_widget_to_param(self, clean_databricks_env):
        dash_def = {
            "datasets": [],
            "pages": [
                {
                    "name": "PageA",
                    "layout": [
                        {
                            "widget": {
                                "name": "filterW",
                                "spec": {
                                    "widgetType": "dropdown_filter",
                                    "encodings": {
                                        "fields": [
                                            {"parameterName": "region"}
                                        ]
                                    },
                                },
                            }
                        }
                    ],
                }
            ],
        }
        parameters = [
            {"keyword": "region", "name": "Region"},
        ]
        DashboardService._link_filter_widgets(dash_def, parameters)
        assert parameters[0].get("pageId") == "PageA"
        assert parameters[0].get("widgetId") == "filterW"
