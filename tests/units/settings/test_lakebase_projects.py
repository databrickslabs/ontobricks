"""Tests for Lakebase project discovery in Settings."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from back.objects.domain.SettingsService import SettingsService


def test_graph_engine_lakebase_projects_returns_all_pages():
    class FakeApi:
        def do(self, method, path, *, query=None):
            assert method == "GET"
            assert path == "/api/2.0/postgres/projects"
            if (query or {}).get("page_token") == "page-2":
                return {
                    "projects": [
                        {
                            "name": "projects/second",
                            "status": {"state": "ACTIVE"},
                        }
                    ]
                }
            return {
                "projects": [
                    {
                        "name": "projects/first",
                        "status": {"state": "ACTIVE"},
                    }
                ],
                "next_page_token": "page-2",
            }

    workspace = MagicMock()
    workspace.api_client = FakeApi()

    with patch("databricks.sdk.WorkspaceClient", return_value=workspace):
        result = SettingsService.graph_engine_lakebase_projects_result(
            MagicMock(),
            MagicMock(),
        )

    assert [project["short_name"] for project in result["projects"]] == [
        "first",
        "second",
    ]
