"""The cross-domain, paginated run-history endpoints behind
Settings -> Automation -> Runs.

``GET /settings/runs/build`` and ``GET /settings/runs/analytics`` are the
registry-wide counterparts of the per-domain readers: they span every domain
unless a ``domain`` is given, and they page with ``limit`` / ``offset`` while
reporting the full ``total`` so the page can render "Showing X-Y of Z".

Driven through a real ``TestClient``. The only two seams mocked are
``get_domain`` (a real ``DomainSession`` needs a populated file-backed session)
and ``RegistryService.from_context`` (a real one needs Databricks / Lakebase
credentials) -- the same seams ``tests/units/dtwin/test_metrics_history.py``
mocks. None of the endpoints' own control flow is mocked.
"""

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.unit

SETTINGS_MODULE = "back.objects.domain.SettingsService"

BUILD_PATH = "/settings/runs/build"
ANALYTICS_PATH = "/settings/runs/analytics"


class _FakeCfg:
    def __init__(self, configured: bool = True):
        self.is_configured = configured


class _FakeDomain:
    uc_domain_folder = "acme_folder"
    current_version = "1"


class _FakeRegistryService:
    """Records how the service called the registry."""

    def __init__(
        self,
        rows: Optional[List[Dict[str, Any]]] = None,
        total: int = 0,
        configured: bool = True,
    ):
        self.rows = rows if rows is not None else []
        self.total = total
        self.cfg = _FakeCfg(configured)
        self.calls: List[Dict[str, Any]] = []

    def _record(self, folder, limit, offset) -> Tuple[List[Dict[str, Any]], int]:
        self.calls.append({"folder": folder, "limit": limit, "offset": offset})
        return self.rows, self.total

    def load_all_build_runs(self, *, folder=None, limit=25, offset=0):
        return self._record(folder, limit, offset)

    def load_all_graph_analytics_runs(self, *, folder=None, limit=25, offset=0):
        return self._record(folder, limit, offset)


@pytest.fixture
def client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


def _get(client, path, svc, **params):
    with patch(f"{SETTINGS_MODULE}.get_domain", return_value=_FakeDomain()), patch(
        "back.objects.registry.RegistryService.RegistryService.from_context",
        return_value=svc,
    ):
        return client.get(path, params=params)


@pytest.mark.parametrize("path", [BUILD_PATH, ANALYTICS_PATH])
class TestPaging:
    def test_defaults_to_the_first_page_of_25(self, client, path):
        svc = _FakeRegistryService(rows=[{"id": 1}], total=137)

        resp = _get(client, path, svc)

        assert resp.status_code == 200
        assert svc.calls == [{"folder": None, "limit": 25, "offset": 0}]

    def test_echoes_total_limit_and_offset(self, client, path):
        svc = _FakeRegistryService(rows=[{"id": 1}], total=137)

        body = _get(client, path, svc, limit=10, offset=20).json()

        assert body["success"] is True
        assert body["runs"] == [{"id": 1}]
        assert body["total"] == 137
        assert body["limit"] == 10
        assert body["offset"] == 20

    def test_offset_is_forwarded(self, client, path):
        svc = _FakeRegistryService()

        _get(client, path, svc, limit=50, offset=100)

        assert svc.calls[0] == {"folder": None, "limit": 50, "offset": 100}

    def test_limit_above_200_is_rejected(self, client, path):
        assert client.get(path, params={"limit": 201}).status_code == 422

    def test_limit_below_1_is_rejected(self, client, path):
        assert client.get(path, params={"limit": 0}).status_code == 422

    def test_negative_offset_is_rejected(self, client, path):
        assert client.get(path, params={"offset": -1}).status_code == 422


@pytest.mark.parametrize("path", [BUILD_PATH, ANALYTICS_PATH])
class TestDomainFilter:
    def test_no_domain_spans_every_domain(self, client, path):
        svc = _FakeRegistryService()

        _get(client, path, svc)

        assert svc.calls[0]["folder"] is None

    def test_domain_is_forwarded_as_the_folder(self, client, path):
        svc = _FakeRegistryService()

        body = _get(client, path, svc, domain="alpha").json()

        assert svc.calls[0]["folder"] == "alpha"
        assert body["domain"] == "alpha"

    def test_an_empty_domain_is_treated_as_all_domains(self, client, path):
        """The dropdown's "All domains" option submits an empty value, which
        must not be bound as a folder named ""."""
        svc = _FakeRegistryService()

        _get(client, path, svc, domain="")

        assert svc.calls[0]["folder"] is None

    def test_an_unknown_domain_is_an_empty_page_not_an_error(self, client, path):
        svc = _FakeRegistryService(rows=[], total=0)

        resp = _get(client, path, svc, domain="ghost")

        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert resp.json()["total"] == 0


@pytest.mark.parametrize("path", [BUILD_PATH, ANALYTICS_PATH])
class TestRegistryNotConfigured:
    def test_surfaces_a_validation_error(self, client, path):
        svc = _FakeRegistryService(configured=False)

        resp = _get(client, path, svc)

        assert resp.status_code == 400
