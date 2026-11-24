"""Unit tests for paginated Lakebase project discovery."""

from __future__ import annotations

import pytest
from databricks.sdk.errors import NotFound

from back.core.databricks.lakebase.LakebaseProjectService import LakebaseProjectService

pytestmark = pytest.mark.unit

_BRANCH = "projects/p/branches/production"


class _BranchApi:
    """Serve one branch's endpoint listing (or raise *error*)."""

    def __init__(self, endpoints=None, error=None):
        self.endpoints, self.error = endpoints or [], error

    def do(self, method, path, **kwargs):
        assert (method, path) == ("GET", f"/api/2.0/postgres/{_BRANCH}/endpoints")
        if self.error:
            raise self.error
        return {"endpoints": self.endpoints}


def _ep(name, host="", read_only_host=""):
    return {"name": f"{_BRANCH}/endpoints/{name}", "status": {"hosts": {"host": host, "read_only_host": read_only_host}}}


class TestBranchEndpoint:
    def test_matches_primary_host_case_insensitively(self):
        api = _BranchApi([_ep("other", "h2"), _ep("primary", "H1.example")])
        assert LakebaseProjectService.branch_endpoint(api, _BRANCH, "h1.example")["name"].endswith("/primary")

    def test_matches_read_only_host(self):
        api = _BranchApi([_ep("ro", "h1", read_only_host="ro.example")])
        assert LakebaseProjectService.branch_endpoint(api, _BRANCH, "ro.example")["name"].endswith("/ro")

    def test_empty_host_returns_first_endpoint_with_a_host(self):
        api = _BranchApi([_ep("starting"), _ep("primary", "h1")])
        assert LakebaseProjectService.branch_endpoint(api, _BRANCH, "")["name"].endswith("/primary")

    def test_unreadable_branch_is_no_match(self):
        assert LakebaseProjectService.branch_endpoint(_BranchApi(error=NotFound("gone")), _BRANCH, "h1") is None

    def test_other_errors_propagate(self):
        with pytest.raises(RuntimeError):
            LakebaseProjectService.branch_endpoint(_BranchApi(error=RuntimeError("503")), _BRANCH, "h1")


def test_list_projects_follows_next_page_token():
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
