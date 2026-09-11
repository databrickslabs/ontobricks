"""Unit tests for HTML-gated UI branding middleware."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from shared.fastapi.ui_branding import UIBrandingMiddleware, get_request_ui_branding

pytestmark = pytest.mark.unit


def _make_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(UIBrandingMiddleware)

    @app.get("/")
    async def home(request: Request):
        return JSONResponse({"has_branding": isinstance(getattr(request.state, "ui_branding", None), dict)})

    @app.get("/api/poll")
    async def poll(request: Request):
        return JSONResponse({"has_branding": isinstance(getattr(request.state, "ui_branding", None), dict)})

    @app.get("/access-denied")
    async def access_denied(request: Request):
        return JSONResponse({"has_branding": isinstance(getattr(request.state, "ui_branding", None), dict)})

    return app


def test_middleware_resolves_branding_for_html_navigation_requests():
    client = TestClient(_make_app())
    with patch(
        "shared.fastapi.ui_branding.resolve_request_ui_branding",
        return_value={"app_title": "Acme"},
    ) as resolver:
        resp = client.get("/", headers={"accept": "text/html"})
    assert resp.status_code == 200
    assert resp.json()["has_branding"] is True
    resolver.assert_called_once()


def test_middleware_skips_branding_resolution_for_json_polling():
    client = TestClient(_make_app())
    with patch(
        "shared.fastapi.ui_branding.resolve_request_ui_branding",
        return_value={"app_title": "Acme"},
    ) as resolver:
        resp = client.get("/api/poll", headers={"accept": "application/json"})
    assert resp.status_code == 200
    assert resp.json()["has_branding"] is False
    resolver.assert_not_called()


def test_middleware_keeps_access_denied_html_branding_resolution():
    client = TestClient(_make_app())
    with patch(
        "shared.fastapi.ui_branding.resolve_request_ui_branding",
        return_value={"app_title": "Acme"},
    ) as resolver:
        resp = client.get("/access-denied", headers={"accept": "text/html"})
    assert resp.status_code == 200
    assert resp.json()["has_branding"] is True
    resolver.assert_called_once()


def test_get_request_ui_branding_memoizes_normalized_value_on_state():
    request = SimpleNamespace(state=SimpleNamespace(ui_branding={"app_title": "Acme"}))
    with patch("shared.fastapi.ui_branding._normalize_branding", return_value={"app_title": "Acme"}) as normalizer:
        first = get_request_ui_branding(request)
        second = get_request_ui_branding(request)
    assert first == second == {"app_title": "Acme"}
    normalizer.assert_called_once()
