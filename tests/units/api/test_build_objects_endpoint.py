"""GET /dtwin/build/objects: read-only for all, ``can_drop`` + drop routes admin-only."""

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from api.routers.internal import dtwin, settings as settings_router

pytestmark = pytest.mark.unit

_PAYLOAD = {"success": True, "backend": "lakebase", "graph_name": "Sales_V1", "groups": []}


def _call(role):
    request = SimpleNamespace(state=SimpleNamespace(user_role=role))

    async def _inline(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    with patch.object(dtwin, "get_domain", return_value=MagicMock()), patch.object(
        dtwin.BuildObjects, "list_for_domain", return_value=dict(_PAYLOAD)
    ), patch.object(dtwin, "run_blocking", _inline):
        return asyncio.run(dtwin.build_objects(request, MagicMock(), MagicMock()))


@pytest.mark.parametrize(
    "role,can_drop",
    [("admin", True), ("app_user", False), ("none", False), ("", False)],
)
def test_can_drop_is_true_only_for_app_admins(role, can_drop):
    out = _call(role)
    assert out["can_drop"] is can_drop
    assert out["backend"] == "lakebase"


def _guard_names(path):
    route = next(
        r for r in settings_router.router.routes if getattr(r, "path", "") == path
    )
    return [d.call.__name__ for d in route.dependant.dependencies]


@pytest.mark.parametrize(
    "path",
    [
        "/settings/graph-engine/drop-uc-object",
        "/settings/graph-engine/lakebase-drop-object",
        "/settings/graph-engine/neo4j-drop-label",
    ],
)
def test_drop_routes_require_app_admin(path):
    assert "require_app_admin" in _guard_names(path)
