"""The analytics run-history endpoint behind Knowledge Graph -> Runs.

The endpoint used to hard-scope to the domain's current version because it
backed a tab that only ever showed the current one. The Runs page has no
version filter, so it now spans versions unless asked otherwise.

Driven through a real ``TestClient`` request against ``GET /dtwin/metrics/
history``. The only two seams mocked are ``get_domain`` (constructing a real
``DomainSession`` needs a populated file-backed session) and
``RegistryService.from_context`` (building a real one needs Databricks/
Lakebase credentials) -- the same two seams every other test on this router
mocks (see ``tests/units/api/test_routes.py::TestNeighborsEndpoint``). No
mocking of the endpoint's own control flow.
"""

from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.unit

MODULE = "api.routers.internal.dtwin"


class _FakeDomain:
    def __init__(self, folder: str = "acme_folder", current_version: str = ""):
        self.uc_domain_folder = folder
        self.current_version = current_version


class _FakeRegistryService:
    """Records how the router called the registry."""

    def __init__(self, runs: Optional[List[Dict[str, Any]]] = None):
        self.runs = runs if runs is not None else []
        self.calls: List[Dict[str, Any]] = []

    def load_graph_analytics_runs(self, folder, version=None, *, limit=100):
        self.calls.append({"folder": folder, "version": version, "limit": limit})
        return self.runs


@pytest.fixture
def client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


class TestNoDomainLoaded:
    """The pre-existing "nothing loaded" branch, exercised for real."""

    def test_returns_empty_runs_without_a_folder(self, client):
        resp = client.get("/dtwin/metrics/history")
        assert resp.status_code == 200
        assert resp.json() == {"success": True, "runs": []}


class TestSpansVersionsByDefault:
    def test_omitting_version_passes_none_to_the_registry(self, client):
        fake_domain = _FakeDomain(current_version="")  # blank current_version
        fake_runs = [
            {"id": "1", "version": "3", "status": "success"},
            {"id": "2", "version": "1", "status": "success"},
        ]
        fake_svc = _FakeRegistryService(fake_runs)

        with patch(f"{MODULE}.get_domain", return_value=fake_domain), patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=fake_svc,
        ):
            resp = client.get("/dtwin/metrics/history")

        assert resp.status_code == 200
        assert resp.json() == {"success": True, "runs": fake_runs}
        assert len(fake_svc.calls) == 1
        call = fake_svc.calls[0]
        assert call["folder"] == "acme_folder"
        assert call["version"] is None, (
            "a blank current_version must not stop the history from spanning "
            "every version on file"
        )
        assert call["limit"] == 200

    def test_a_blank_current_version_still_returns_rows(self, client):
        """The old guard on ``not version`` would have short-circuited here."""
        fake_domain = _FakeDomain(current_version="")
        fake_svc = _FakeRegistryService([{"id": "1", "version": "2"}])

        with patch(f"{MODULE}.get_domain", return_value=fake_domain), patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=fake_svc,
        ):
            resp = client.get("/dtwin/metrics/history")

        assert resp.status_code == 200
        assert resp.json()["runs"] == [{"id": "1", "version": "2"}]


class TestVersionQueryParamStillScopes:
    def test_version_param_is_forwarded_to_the_registry(self, client):
        fake_domain = _FakeDomain()
        fake_svc = _FakeRegistryService()

        with patch(f"{MODULE}.get_domain", return_value=fake_domain), patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=fake_svc,
        ):
            resp = client.get("/dtwin/metrics/history", params={"version": "2"})

        assert resp.status_code == 200
        assert fake_svc.calls[0]["version"] == "2"


class TestLimitQueryParam:
    def test_default_limit_is_200(self, client):
        fake_domain = _FakeDomain()
        fake_svc = _FakeRegistryService()

        with patch(f"{MODULE}.get_domain", return_value=fake_domain), patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=fake_svc,
        ):
            client.get("/dtwin/metrics/history")

        assert fake_svc.calls[0]["limit"] == 200

    def test_explicit_limit_is_forwarded(self, client):
        fake_domain = _FakeDomain()
        fake_svc = _FakeRegistryService()

        with patch(f"{MODULE}.get_domain", return_value=fake_domain), patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=fake_svc,
        ):
            client.get("/dtwin/metrics/history", params={"limit": "5"})

        assert fake_svc.calls[0]["limit"] == 5

    def test_limit_over_1000_is_rejected(self, client):
        resp = client.get("/dtwin/metrics/history", params={"limit": "1001"})
        assert resp.status_code == 422

    def test_limit_under_1_is_rejected(self, client):
        resp = client.get("/dtwin/metrics/history", params={"limit": "0"})
        assert resp.status_code == 422
