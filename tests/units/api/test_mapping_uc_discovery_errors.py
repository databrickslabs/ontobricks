"""Mapping table/column discovery surfaces UC failures as 502, not as empty."""

import asyncio
from types import SimpleNamespace

import pytest

import api.routers.internal.mapping as mapping_routes
from back.core.errors import InfrastructureError


class _Request:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


class _FailingClient:
    def get_tables(self, catalog, schema):
        raise InfrastructureError(
            f"Could not list tables in {catalog}.{schema}", detail="PERMISSION_DENIED"
        )

    def get_table_columns(self, catalog, schema, table):
        raise InfrastructureError(
            f"Could not read columns of {catalog}.{schema}.{table}",
            detail="PERMISSION_DENIED",
        )


@pytest.fixture
def failing_client(monkeypatch):
    monkeypatch.setattr(mapping_routes, "get_domain", lambda _sm: SimpleNamespace())
    monkeypatch.setattr(
        mapping_routes, "get_data_plane_client", lambda _d, _s: _FailingClient()
    )


@pytest.mark.unit
class TestDiscoveryErrors:
    def test_tables_route_raises_infrastructure_error(self, failing_client):
        request = _Request({"catalog": "c", "schema": "s"})
        with pytest.raises(InfrastructureError) as exc_info:
            asyncio.run(mapping_routes.get_tables(request, None, None))
        assert exc_info.value.status_code == 502
        assert exc_info.value.message == "Could not list tables in c.s"

    def test_table_columns_route_raises_infrastructure_error(self, failing_client):
        request = _Request({"catalog": "c", "schema": "s", "table": "t"})
        with pytest.raises(InfrastructureError) as exc_info:
            asyncio.run(mapping_routes.get_table_columns(request, None, None))
        assert exc_info.value.message == "Could not read columns of c.s.t"
