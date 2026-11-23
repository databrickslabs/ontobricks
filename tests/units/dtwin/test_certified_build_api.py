"""API contract of the certified build start endpoint."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from back.core.errors import ConflictError

pytestmark = pytest.mark.unit

MODULE = "api.routers.internal.dtwin"


@pytest.fixture
def api_client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


def test_start_returns_the_task_id(api_client):
    domain = MagicMock()
    with (
        patch(f"{MODULE}.get_domain", return_value=domain),
        patch(f"{MODULE}.CertifiedBuild.start", return_value="t-7") as start,
    ):
        response = api_client.post("/dtwin/certified-build/start")

    assert response.status_code == 200
    assert response.json()["task_id"] == "t-7"
    assert start.call_args.args[0] is domain


def test_a_read_only_version_returns_409(api_client):
    with (
        patch(f"{MODULE}.get_domain", return_value=MagicMock()),
        patch(
            f"{MODULE}.CertifiedBuild.start",
            side_effect=ConflictError("Version 2 is PUBLISHED (read-only)."),
        ),
    ):
        response = api_client.post("/dtwin/certified-build/start")

    assert response.status_code == 409
