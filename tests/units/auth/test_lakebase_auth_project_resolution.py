"""Project resolution order for :class:`LakebaseAuth`.

Large workspaces hold thousands of Lakebase projects, so resolving the
project from ``PGHOST`` must not list every project when the branch is
already known:

1. ``LAKEBASE_PROJECT`` + ``LAKEBASE_BRANCH`` env -> that branch only.
2. ``DATABRICKS_APP_NAME`` -> the app's bound ``postgres`` resource branch.
3. Fallback: walk every project, skipping projects/branches it can't read
   (``NotFound`` / ``PermissionDenied``). Any other API error propagates.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied

from back.core.databricks.lakebase import LakebaseAuth
from back.core.errors import ValidationError

pytestmark = pytest.mark.unit

_HOST = "lakebase-resolution-test.example.invalid"
_PROJECT = "proj-match"
_BRANCH_PATH = f"projects/{_PROJECT}/branches/production"
_ENDPOINT_PATH = f"{_BRANCH_PATH}/endpoints/primary"
_APP_NAME = "ontobricks-test-app"
_PROJECTS_PATH = "/api/2.0/postgres/projects"
_TOKEN = "lakebase-test-jwt-placeholder-not-secret"


class _FakeAPI:
    """Route ``api_client.do`` calls by ``(method, path)``; record every call."""

    def __init__(self, routes: dict) -> None:
        self.routes = routes
        self.calls: list = []

    def do(self, method, path, *args, **kwargs):
        self.calls.append((method, path))
        response = self.routes.get((method, path))
        if isinstance(response, Exception):
            raise response
        if response is None:
            raise AssertionError(f"unexpected call {method} {path}")
        return response

    def called(self, path: str) -> bool:
        return any(p == path for _, p in self.calls)


def _endpoints(branch_path: str, host: str) -> dict:
    return {
        "endpoints": [
            {
                "name": f"{branch_path}/endpoints/primary",
                "status": {"hosts": {"host": host}},
            }
        ]
    }


def _matching_branch_routes() -> dict:
    return {
        ("GET", f"/api/2.0/postgres/{_BRANCH_PATH}/endpoints"): _endpoints(_BRANCH_PATH, _HOST),
        ("POST", "/api/2.0/postgres/credentials"): {"token": _TOKEN},
    }


def _walk_routes() -> dict:
    """Two projects: the first is listed but unreadable, the second matches."""
    return {
        **_matching_branch_routes(),
        ("GET", _PROJECTS_PATH): {
            "projects": [{"name": "projects/deleted"}, {"name": f"projects/{_PROJECT}"}]
        },
        ("GET", "/api/2.0/postgres/projects/deleted/branches"): NotFound(
            "Project with name 'projects/deleted' not found"
        ),
        ("GET", f"/api/2.0/postgres/projects/{_PROJECT}/branches"): {
            "branches": [{"name": _BRANCH_PATH}]
        },
    }


def _auth_with(api: _FakeAPI) -> LakebaseAuth:
    auth = LakebaseAuth()
    auth._w = SimpleNamespace(api_client=api)
    return auth


@pytest.fixture(autouse=True)
def apps_pg_env(monkeypatch):
    monkeypatch.setenv("PGHOST", _HOST)
    monkeypatch.setenv("PGUSER", "sp-test-client-id-not-a-real-uuid")
    for var in ("LAKEBASE_PROJECT", "LAKEBASE_BRANCH", "DATABRICKS_APP_NAME"):
        monkeypatch.delenv(var, raising=False)


class TestEnvBranchResolution:
    def test_env_branch_resolves_without_listing_projects(self, monkeypatch):
        monkeypatch.setenv("LAKEBASE_PROJECT", _PROJECT)
        monkeypatch.setenv("LAKEBASE_BRANCH", "production")
        api = _FakeAPI(_matching_branch_routes())

        assert _auth_with(api).instance_name == _PROJECT
        assert not api.called(_PROJECTS_PATH)

    def test_env_branch_sets_endpoint_resource(self, monkeypatch):
        monkeypatch.setenv("LAKEBASE_PROJECT", _PROJECT)
        monkeypatch.setenv("LAKEBASE_BRANCH", "production")
        auth = _auth_with(_FakeAPI(_matching_branch_routes()))

        _ = auth.instance_name
        assert auth._endpoint_resource == _ENDPOINT_PATH

    def test_env_branch_miss_falls_back_to_walk(self, monkeypatch):
        monkeypatch.setenv("LAKEBASE_PROJECT", "stale")
        monkeypatch.setenv("LAKEBASE_BRANCH", "production")
        routes = {
            **_walk_routes(),
            ("GET", "/api/2.0/postgres/projects/stale/branches/production/endpoints"): {
                "endpoints": []
            },
        }

        assert _auth_with(_FakeAPI(routes)).instance_name == _PROJECT


class TestAppResourceResolution:
    def test_app_postgres_resource_resolves_without_listing_projects(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_NAME", _APP_NAME)
        routes = {
            **_matching_branch_routes(),
            ("GET", f"/api/2.0/apps/{_APP_NAME}"): {
                "resources": [
                    {"name": "sql-warehouse", "sql_warehouse": {"id": "abc"}},
                    {
                        "name": "database",
                        "postgres": {
                            "branch": _BRANCH_PATH,
                            "database": "ontobricks_registry",
                            "permission": "CAN_CONNECT_AND_CREATE",
                        },
                    },
                ]
            },
        }
        api = _FakeAPI(routes)

        assert _auth_with(api).instance_name == _PROJECT
        assert not api.called(_PROJECTS_PATH)

    def test_app_get_failure_falls_back_to_walk(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_NAME", _APP_NAME)
        routes = {
            **_walk_routes(),
            ("GET", f"/api/2.0/apps/{_APP_NAME}"): PermissionDenied("403 forbidden"),
        }

        assert _auth_with(_FakeAPI(routes)).instance_name == _PROJECT

    def test_app_without_postgres_resource_falls_back_to_walk(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_NAME", _APP_NAME)
        routes = {
            **_walk_routes(),
            ("GET", f"/api/2.0/apps/{_APP_NAME}"): {
                "resources": [{"name": "sql-warehouse", "sql_warehouse": {"id": "abc"}}]
            },
        }

        assert _auth_with(_FakeAPI(routes)).instance_name == _PROJECT


class TestTolerantWalk:
    def test_unreadable_project_is_skipped(self):
        assert _auth_with(_FakeAPI(_walk_routes())).instance_name == _PROJECT

    def test_unreadable_branch_endpoints_are_skipped(self):
        broken_branch = f"projects/{_PROJECT}/branches/broken"
        routes = {
            **_matching_branch_routes(),
            ("GET", _PROJECTS_PATH): {"projects": [{"name": f"projects/{_PROJECT}"}]},
            ("GET", f"/api/2.0/postgres/projects/{_PROJECT}/branches"): {
                "branches": [{"name": broken_branch}, {"name": _BRANCH_PATH}]
            },
            ("GET", f"/api/2.0/postgres/{broken_branch}/endpoints"): NotFound("branch gone"),
        }

        assert _auth_with(_FakeAPI(routes)).instance_name == _PROJECT

    def test_no_match_raises_validation_error(self, monkeypatch):
        monkeypatch.setenv("PGHOST", "unmatched-host.example.invalid")
        auth = _auth_with(_FakeAPI(_walk_routes()))

        with pytest.raises(ValidationError, match="No Lakebase Autoscaling endpoint matched"):
            _ = auth.instance_name

    def test_password_mints_for_walk_matched_endpoint(self):
        api = _FakeAPI(_walk_routes())
        auth = _auth_with(api)

        assert auth.password() == _TOKEN
        assert auth._endpoint_resource == _ENDPOINT_PATH


class TestFailedLookup:
    def test_transient_api_error_is_not_skipped(self):
        routes = {
            **_walk_routes(),
            ("GET", "/api/2.0/postgres/projects/deleted/branches"): RuntimeError("503 unavailable"),
        }
        auth = _auth_with(_FakeAPI(routes))

        with pytest.raises(ValidationError, match="503 unavailable"):
            _ = auth.instance_name

    def test_failed_lookup_is_not_retried_immediately(self, monkeypatch):
        monkeypatch.setenv("PGHOST", "unmatched-host.example.invalid")
        api = _FakeAPI(_walk_routes())
        auth = _auth_with(api)
        with pytest.raises(ValidationError):
            _ = auth.instance_name
        calls_after_first = len(api.calls)

        with pytest.raises(ValidationError):
            _ = auth.branch_path or auth.instance_name
        assert len(api.calls) == calls_after_first

    def test_failed_lookup_is_retried_after_the_backoff(self, monkeypatch):
        monkeypatch.setenv("PGHOST", "unmatched-host.example.invalid")
        api = _FakeAPI(_walk_routes())
        auth = _auth_with(api)
        with pytest.raises(ValidationError):
            _ = auth.instance_name
        auth._lookup_failed_at -= LakebaseAuth._LOOKUP_RETRY_S + 1
        calls_after_first = len(api.calls)

        with pytest.raises(ValidationError):
            _ = auth.instance_name
        assert len(api.calls) > calls_after_first


class TestHostFromEnvBranch:
    """Local dev: no PGHOST, the host comes from LAKEBASE_PROJECT + LAKEBASE_BRANCH."""

    @pytest.fixture(autouse=True)
    def env_branch_only(self, monkeypatch):
        monkeypatch.delenv("PGHOST")
        monkeypatch.setenv("LAKEBASE_PROJECT", _PROJECT)
        monkeypatch.setenv("LAKEBASE_BRANCH", "production")

    def test_host_is_the_branch_endpoint_host(self):
        assert _auth_with(_FakeAPI(_matching_branch_routes())).host == _HOST

    def test_branch_is_listed_once_for_host_and_password(self):
        api = _FakeAPI(_matching_branch_routes())
        auth = _auth_with(api)

        _ = auth.host
        assert auth.password() == _TOKEN
        assert api.calls.count(("GET", f"/api/2.0/postgres/{_BRANCH_PATH}/endpoints")) == 1

    def test_unknown_branch_reports_missing_host(self):
        routes = {("GET", f"/api/2.0/postgres/{_BRANCH_PATH}/endpoints"): NotFound("no branch")}

        with pytest.raises(ValidationError, match="Cannot determine Lakebase host"):
            _ = _auth_with(_FakeAPI(routes)).host
