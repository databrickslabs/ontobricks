"""``GET /dtwin/databricks-build/info`` has to say how ``…_data`` is built.

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
        "back.core.graphdb.delta.DeltaBase.create_databricks_client",
        return_value=None,
    ):
        resp = api_client.get("/dtwin/databricks-build/info")
    assert resp.status_code == 200
    return resp.json()


def test_a_view_only_domain_is_reported_as_such(api_client, domain_session):
    payload = _info(api_client, _configure(domain_session, "view"))
    assert payload["materialization"] == "view"


def test_a_materialized_domain_is_reported_as_such(api_client, domain_session):
    payload = _info(api_client, _configure(domain_session, "table"))
    assert payload["materialization"] == "table"


def test_the_field_defaults_to_table(api_client, domain_session):
    """A domain saved before the option existed must not look view-only."""
    assert _info(api_client, _configure(domain_session))["materialization"] == "table"


def test_a_non_lakehouse_domain_is_never_view_only(api_client, domain_session):
    """Its ..._data is still materialized, whatever lingers in ``info``."""
    payload = _info(
        api_client, _configure(domain_session, "view", backend="lakebase")
    )
    assert payload["triple_store_backend"] == "lakebase"
    assert payload["materialization"] == "table"
