"""Unit tests for paginated Lakebase project discovery."""

from __future__ import annotations


def test_list_projects_follows_next_page_token():
    from back.core.databricks.lakebase.LakebaseProjectService import (
        LakebaseProjectService,
    )

    class FakeApi:
        def __init__(self):
            self.queries = []

        def do(self, method, path, *, query):
            assert method == "GET"
            assert path == "/api/2.0/postgres/projects"
            self.queries.append(query)
            if query.get("page_token") == "page-2":
                return {"projects": [{"name": "projects/second"}]}
            return {
                "projects": [{"name": "projects/first"}],
                "next_page_token": "page-2",
            }

    api = FakeApi()

    projects = LakebaseProjectService.list_projects(api)

    assert [project["name"] for project in projects] == [
        "projects/first",
        "projects/second",
    ]
    assert api.queries == [
        {"page_size": 100},
        {"page_size": 100, "page_token": "page-2"},
    ]
