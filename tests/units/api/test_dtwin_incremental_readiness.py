"""``GET /dtwin/incremental/readiness`` and ``POST /dtwin/incremental/enable-row-tracking``."""

from __future__ import annotations

import pytest

from back.core.errors import ValidationError
from back.objects.digitaltwin.build import TwinIncrementalReadiness
from back.objects.registry import ROLE_ADMIN, ROLE_BUILDER

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


_BUILDER_GATE = f"require_domain_{ROLE_BUILDER}"


@pytest.mark.parametrize(
    "path",
    [
        "/dtwin/incremental/readiness",
        "/dtwin/incremental/enable-row-tracking",
        "/dtwin/incremental/status",
        "/dtwin/incremental/reconcile",
    ],
)
def test_the_incremental_routes_need_the_builder_role(path):
    from api.routers.internal import dtwin

    [route] = [r for r in dtwin.router.routes if r.path == path]
    assert [dep.dependency.__name__ for dep in route.dependencies] == [_BUILDER_GATE]


def _overview_gate():
    from api.routers.internal import dtwin

    [route] = [r for r in dtwin.router.routes if r.path == "/dtwin/incremental/overview"]
    [dependency] = route.dependencies
    return dependency.dependency


def test_the_overview_needs_the_app_admin_role():
    """A registry-wide KPI: the loaded domain's builder role is not enough."""
    assert _overview_gate().__name__ == f"require_app_{ROLE_ADMIN}"


@pytest.mark.parametrize(
    "state",
    [
        {"user_role": "app_user", "user_domain_role": ROLE_BUILDER},
        {"user_role": "app_user", "user_domain_role": ROLE_ADMIN},
    ],
    ids=["domain-builder", "domain-admin"],
)
def test_a_domain_role_does_not_open_the_overview(state):
    from types import SimpleNamespace

    from back.core.errors import AuthorizationError

    with pytest.raises(AuthorizationError):
        _overview_gate()(SimpleNamespace(state=SimpleNamespace(**state)))


def test_an_app_admin_opens_the_overview():
    from types import SimpleNamespace

    request = SimpleNamespace(state=SimpleNamespace(user_role=ROLE_ADMIN, user_domain_role=""))

    assert _overview_gate()(request) == ROLE_ADMIN


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


@pytest.mark.asyncio
async def test_overview_aggregates_the_session_s_registry(monkeypatch, route):
    from back.objects.digitaltwin.build import TwinIncrementalStatus
    from back.objects.registry.RegistryService import RegistryService

    dtwin, domain, seen = route
    registry = object()
    monkeypatch.setattr(
        RegistryService,
        "from_context",
        classmethod(lambda cls, d, s: seen.update(registry_domain=d) or registry),
    )

    def overview(reg):
        seen["registry"] = reg
        return {"domains_per_mode": {"incremental": 1}, "fallbacks_by_reason": {}, "domains": []}

    monkeypatch.setattr(TwinIncrementalStatus, "overview", staticmethod(overview))

    resp = await dtwin.incremental_overview(session_mgr=object(), settings=object())

    assert resp == {
        "success": True,
        "domains_per_mode": {"incremental": 1},
        "fallbacks_by_reason": {},
        "domains": [],
    }
    assert seen["registry"] is registry and seen["registry_domain"] is domain
    assert "client_kwargs" not in seen


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["start", "stop"])
async def test_the_realtime_routes_delegate_to_the_stream(monkeypatch, route, action):
    from back.objects.digitaltwin.build import TwinRealtimeStream

    dtwin, domain, seen = route
    monkeypatch.setattr(
        TwinRealtimeStream,
        action,
        lambda self, d: seen.update(domain=d) or {"run_id": 7, "state": "PENDING"},
    )
    handler = getattr(dtwin, f"{action}_realtime_stream")

    resp = await handler(session_mgr=object(), settings=object())

    assert resp == {"success": True, "run_id": 7, "state": "PENDING"}
    assert seen["domain"] is domain


@pytest.mark.parametrize("action", ["start", "stop"])
def test_the_realtime_routes_need_the_builder_role(action):
    from api.routers.internal import dtwin

    [route] = [r for r in dtwin.router.routes if r.path == f"/dtwin/incremental/realtime/{action}"]
    assert [dep.dependency.__name__ for dep in route.dependencies] == [_BUILDER_GATE]
