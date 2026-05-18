"""Tests for shared.fastapi.csrf – CSRF middleware."""

import pytest
from unittest.mock import patch
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from shared.fastapi.csrf import CSRFMiddleware


def _make_app(csrf_disabled: str = ""):
    """Create a tiny FastAPI app with the CSRF middleware for testing."""
    app = FastAPI()
    app.add_middleware(CSRFMiddleware)

    @app.get("/page")
    async def get_page():
        return {"ok": True}

    @app.post("/action")
    async def post_action():
        return {"ok": True}

    @app.post("/api/v1/query")
    async def api_query():
        return {"ok": True}

    @app.post("/graphql/execute")
    async def gql():
        return {"ok": True}

    return app


class TestCSRFMiddleware:
    def test_disabled_via_env(self, monkeypatch):
        monkeypatch.setenv("CSRF_DISABLED", "1")
        client = TestClient(_make_app())
        resp = client.post("/action")
        assert resp.status_code == 200

    def test_get_requests_pass(self, monkeypatch):
        monkeypatch.delenv("CSRF_DISABLED", raising=False)
        client = TestClient(_make_app())
        resp = client.get("/page")
        assert resp.status_code == 200

    def test_post_without_cookie_passes(self, monkeypatch):
        monkeypatch.delenv("CSRF_DISABLED", raising=False)
        client = TestClient(_make_app())
        resp = client.post("/action")
        assert resp.status_code == 200

    def test_post_with_mismatched_token_rejected(self, monkeypatch):
        monkeypatch.delenv("CSRF_DISABLED", raising=False)
        client = TestClient(_make_app(), cookies={"csrf_token": "abc123"})
        resp = client.post("/action", headers={"X-CSRF-Token": "wrong"})
        assert resp.status_code == 403
        assert "csrf" in resp.json().get("error", "")

    def test_post_with_matching_token_passes(self, monkeypatch):
        monkeypatch.delenv("CSRF_DISABLED", raising=False)
        token = "valid-token-123"
        client = TestClient(_make_app(), cookies={"csrf_token": token})
        resp = client.post("/action", headers={"X-CSRF-Token": token})
        assert resp.status_code == 200

    def test_api_prefix_bypassed(self, monkeypatch):
        monkeypatch.delenv("CSRF_DISABLED", raising=False)
        client = TestClient(_make_app(), cookies={"csrf_token": "abc"})
        resp = client.post("/api/v1/query", headers={"X-CSRF-Token": "different"})
        assert resp.status_code == 200

    def test_graphql_prefix_bypassed(self, monkeypatch):
        monkeypatch.delenv("CSRF_DISABLED", raising=False)
        client = TestClient(_make_app(), cookies={"csrf_token": "abc"})
        resp = client.post("/graphql/execute", headers={"X-CSRF-Token": "different"})
        assert resp.status_code == 200

    def test_sets_cookie_on_first_response(self, monkeypatch):
        monkeypatch.delenv("CSRF_DISABLED", raising=False)
        client = TestClient(_make_app())
        resp = client.get("/page")
        assert "csrf_token" in resp.cookies
