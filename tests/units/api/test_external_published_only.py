"""External read routes resolve PUBLISHED versions only; write routes are unchanged."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from back.core.errors import NotFoundError

pytestmark = pytest.mark.unit


@pytest.fixture
def client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


def _resolve_kwargs(client, method, path):
    with (
        patch(
            "api.routers.digitaltwin.DigitalTwin.resolve_domain",
            side_effect=NotFoundError("stop"),
        ) as resolve_dt,
        patch(
            "api.routers.domains.DigitalTwin.resolve_domain",
            side_effect=NotFoundError("stop"),
        ) as resolve_dom,
    ):
        sep = "&" if "?" in path else "?"
        getattr(client, method)(f"{path}{sep}domain_name=demo")
    called = resolve_dt if resolve_dt.called else resolve_dom
    assert called.called, path
    return called.call_args.kwargs


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/digitaltwin/status",
        "/api/v1/digitaltwin/stats",
        "/api/v1/digitaltwin/triples/find?search=x",
        "/api/v1/digitaltwin/triples",
        "/api/v1/digitaltwin/cohorts/rules",
        "/api/v1/domain/classes",
        "/api/v1/domain/ontology",
    ],
)
def test_external_reads_resolve_published_versions_only(client, path):
    assert _resolve_kwargs(client, "get", path).get("published_only") is True


def test_the_external_build_still_targets_the_latest_version(client):
    kwargs = _resolve_kwargs(client, "post", "/api/v1/digitaltwin/build")

    assert not kwargs.get("published_only")


def test_an_unpublished_version_is_not_found(client):
    with patch(
        "api.routers.digitaltwin.DigitalTwin.resolve_domain",
        side_effect=NotFoundError('Version 3 of context "demo" is not published'),
    ):
        response = client.get(
            "/api/v1/digitaltwin/status?domain_name=demo&domain_version=3"
        )

    assert response.status_code == 404
