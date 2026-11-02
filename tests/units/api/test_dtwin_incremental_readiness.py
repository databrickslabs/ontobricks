"""``GET /dtwin/incremental/readiness`` and ``POST /dtwin/incremental/enable-row-tracking``."""

from __future__ import annotations

import pytest

from back.core.errors import ValidationError
from back.objects.digitaltwin.build import TwinIncrementalReadiness

pytestmark = pytest.mark.unit


@pytest.fixture
def route(monkeypatch):
    import back.core.helpers as helpers_pkg
    from api.routers.internal import dtwin
    from back.core.graphdb.delta import DeltaBase

    domain = object()
    seen: dict = {}
    monkeypatch.setattr(dtwin, "get_domain", lambda _mgr: domain)
    monkeypatch.setattr(helpers_pkg, "resolve_build_warehouse_id", lambda d, s: "wh-build")

    def create_client(d, s, **kwargs):
        seen["client_kwargs"] = kwargs
        return "client"

    monkeypatch.setattr(DeltaBase, "create_databricks_client", create_client)
    return dtwin, domain, seen


@pytest.mark.asyncio
async def test_readiness_returns_the_report_on_the_build_warehouse(monkeypatch, route):
    dtwin, domain, seen = route

    def report(d, s, client):
        seen.update(domain=d, client=client)
        return {"mode_requested": "incremental", "decision": {"effective": "full", "reason": "x"}}

    monkeypatch.setattr(TwinIncrementalReadiness, "report", staticmethod(report))

    resp = await dtwin.incremental_readiness(session_mgr=object(), settings=object())

    assert resp["success"] is True and resp["mode_requested"] == "incremental"
    assert seen["domain"] is domain and seen["client"] == "client"
    assert seen["client_kwargs"] == {"warehouse_id": "wh-build"}


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [{}, {"tables": ["main.sales.customers"]}], ids=["all", "subset"])
async def test_enable_row_tracking_forwards_the_tables(monkeypatch, route, body):
    dtwin, _domain, seen = route

    def enable(d, s, client, tables):
        seen["tables"] = tables
        return {"tables": []}

    monkeypatch.setattr(TwinIncrementalReadiness, "enable_row_tracking", staticmethod(enable))

    resp = await dtwin.enable_incremental_row_tracking(
        body=body, session_mgr=object(), settings=object()
    )

    assert resp == {"success": True, "tables": []}
    assert seen["tables"] == body.get("tables")


@pytest.mark.asyncio
@pytest.mark.parametrize("tables", ["main.sales.customers", [1, 2]], ids=["string", "not-names"])
async def test_enable_row_tracking_refuses_a_malformed_list(route, tables):
    dtwin, _domain, _seen = route

    with pytest.raises(ValidationError):
        await dtwin.enable_incremental_row_tracking(
            body={"tables": tables}, session_mgr=object(), settings=object()
        )


def test_both_routes_are_builder_gated():
    from api.routers.internal import dtwin

    gated = {
        route.path: route.dependencies
        for route in dtwin.router.routes
        if route.path.endswith(("/incremental/readiness", "/incremental/enable-row-tracking"))
    }
    assert set(gated) == {"/dtwin/incremental/readiness", "/dtwin/incremental/enable-row-tracking"}
    assert all(deps for deps in gated.values())


@pytest.mark.asyncio
async def test_status_reports_on_the_build_warehouse_with_the_route_store(monkeypatch, route):
    from back.objects.digitaltwin.build import TwinIncrementalStatus

    dtwin, domain, seen = route

    def report(d, s, client, *, get_store):
        seen.update(domain=d, client=client, get_store=get_store)
        return {"mode_requested": "incremental", "slot_pipeline": {"state": "absent"}}

    monkeypatch.setattr(TwinIncrementalStatus, "report", staticmethod(report))

    resp = await dtwin.incremental_status(session_mgr=object(), settings=object())

    assert resp["success"] is True and resp["slot_pipeline"] == {"state": "absent"}
    assert seen["domain"] is domain and seen["client"] == "client"
    assert seen["get_store"] is dtwin.get_graphdb
    assert seen["client_kwargs"] == {"warehouse_id": "wh-build"}


def test_the_status_route_is_builder_gated():
    from api.routers.internal import dtwin

    [status] = [r for r in dtwin.router.routes if r.path == "/dtwin/incremental/status"]
    assert status.dependencies
