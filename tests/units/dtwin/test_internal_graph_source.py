"""Internal graph reads follow the source picker: Original, Certified or default."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.unit

MODULE = "api.routers.internal.dtwin"


@pytest.fixture
def api_client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


def _variants_opened(api_client, source):
    get_graphdb = MagicMock(return_value=MagicMock())
    with (
        patch(f"{MODULE}.get_domain", return_value=MagicMock()),
        patch(f"{MODULE}.get_graphdb", get_graphdb),
        patch(f"{MODULE}._graph_query_table", return_value="c.s.t"),
    ):
        api_client.get(f"/dtwin/sync/stats?simulation_id={source}")
    return [c.kwargs.get("variant") for c in get_graphdb.call_args_list]


@pytest.mark.parametrize("source", ["original", "certified"])
def test_a_variant_token_opens_that_variant_store(api_client, source):
    variants = _variants_opened(api_client, source)

    assert variants and set(variants) == {source}


def test_the_live_token_keeps_the_default_store(api_client):
    variants = _variants_opened(api_client, "live")

    assert variants and set(variants) == {None}
