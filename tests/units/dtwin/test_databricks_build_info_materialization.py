"""``GET /dtwin/databricks-build/info`` says how the Original ``…_data`` is built.

The Build page labels the target object and explains its triple count from
this field. Without it the page calls a pass-through view a "Target Delta
table" and presents a live query as a stored row count — both wrong, and both
wrong in a way that hides the cost of the mode from whoever chose it.

Driven through a real ``TestClient`` request against a real ``DomainSession``.
The only seams mocked are ``get_domain`` (the router reads it from a
file-backed session) and the SQL warehouse client (a real one needs
credentials) — the same seams every other test on this router mocks.
"""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.unit

MODULE = "api.routers.internal.dtwin"


@pytest.fixture
def api_client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


def _configure(session, materialization=None, backend="databricks"):
    """An empty but well-formed Lakehouse domain named ``Dom`` at version 3.

    ``delta`` (catalog/schema) is derived from the registry config, so the
    resolved FQNs depend on the environment — these tests assert on the
    materialization field, which does not.
    """
    session.info["name"] = "Dom"
    session.info["graph_backend"] = backend
    if materialization is not None:
        session.info["lakehouse_materialization"] = materialization
    session.current_version = "3"
    return session


def _info(api_client, domain):
    # No warehouse: the probe degrades to an empty status, which leaves the
    # naming and materialization fields this test is about untouched.
    with patch(f"{MODULE}.get_domain", return_value=domain), patch(
        "back.core.graphdb.delta.Base.create_databricks_client",
        return_value=None,
    ):
        resp = api_client.get("/dtwin/databricks-build/info")
    assert resp.status_code == 200
    return resp.json()


@pytest.mark.parametrize(
    "materialization,backend",
    [
        ("view", "databricks"),
        ("table", "databricks"),
        (None, "databricks"),
        ("table", "lakebase"),
        ("view", "neo4j"),
    ],
)
def test_the_original_graph_data_is_always_a_view(
    api_client, domain_session, materialization, backend
):
    """``lakehouse_materialization`` qualifies the Certified Graph only."""
    payload = _info(
        api_client, _configure(domain_session, materialization, backend=backend)
    )
    assert payload["materialization"] == "view"
