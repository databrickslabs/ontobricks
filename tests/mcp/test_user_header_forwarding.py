"""End-user identity forwarding from the MCP server to the OntoBricks API.

The MCP server authenticates to the main app with its own M2M principal, but
for data-plane (UC/graph) routes the main app must resolve the *end user* so
OBO + Team gating apply. These tests lock the header-forwarding contract on the
``http_client`` boundary: every outbound ``_get`` / ``_post`` carries both the
M2M ``Authorization`` and the forwarded ``x-forwarded-*`` user identity.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import httpx
import pytest

_MCP_SRC = Path(__file__).resolve().parents[2] / "src" / "mcp-server"
if str(_MCP_SRC) not in sys.path:
    sys.path.insert(0, str(_MCP_SRC))

from server import http_client as hc  # noqa: E402

pytestmark = pytest.mark.mcp


class _Resp:
    status_code = 200

    def raise_for_status(self) -> None:  # noqa: D401
        return None

    def json(self) -> dict:
        return {}


@pytest.fixture(autouse=True)
def _reset_forwarded():
    hc.clear_forwarded_identity()
    yield
    hc.clear_forwarded_identity()


def test_forwarded_user_headers_attached_on_get(monkeypatch):
    captured: dict = {}

    async def _fake_get(self, url, params=None, headers=None, **_kw):
        captured["headers"] = headers or {}
        return _Resp()

    monkeypatch.setattr(httpx.AsyncClient, "get", _fake_get)
    monkeypatch.setattr(hc, "_get_auth_headers", lambda mode: {"Authorization": "Bearer M2M"})

    hc.set_forwarded_identity(email="u@x", user_token="UT")
    client = httpx.AsyncClient(base_url="https://remote")
    asyncio.run(hc._get(client, "/api/v1/graph/thing"))

    h = captured["headers"]
    assert h["Authorization"] == "Bearer M2M"
    assert h["x-forwarded-email"] == "u@x"
    assert h["x-forwarded-access-token"] == "UT"


def test_forwarded_user_headers_attached_on_post(monkeypatch):
    captured: dict = {}

    async def _fake_post(self, url, json=None, headers=None, **_kw):
        captured["headers"] = headers or {}
        return _Resp()

    monkeypatch.setattr(httpx.AsyncClient, "post", _fake_post)
    monkeypatch.setattr(hc, "_get_auth_headers", lambda mode: {"Authorization": "Bearer M2M"})

    hc.set_forwarded_identity(email="u@x", user_token="UT")
    client = httpx.AsyncClient(base_url="https://remote")
    asyncio.run(hc._post(client, "/api/v1/dt/node/action", json={"a": 1}))

    h = captured["headers"]
    assert h["Authorization"] == "Bearer M2M"
    assert h["x-forwarded-access-token"] == "UT"


def test_no_forwarded_identity_still_sends_m2m(monkeypatch):
    captured: dict = {}

    async def _fake_get(self, url, params=None, headers=None, **_kw):
        captured["headers"] = headers or {}
        return _Resp()

    monkeypatch.setattr(httpx.AsyncClient, "get", _fake_get)
    monkeypatch.setattr(hc, "_get_auth_headers", lambda mode: {"Authorization": "Bearer M2M"})

    client = httpx.AsyncClient(base_url="https://remote")
    asyncio.run(hc._get(client, "/api/v1/domains"))

    h = captured["headers"]
    assert h["Authorization"] == "Bearer M2M"
    assert "x-forwarded-access-token" not in h
    assert "x-forwarded-email" not in h
