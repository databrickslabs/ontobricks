"""Tests for shared.fastapi.csrf – CSRF middleware (double-submit cookie)."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.fastapi.csrf import CSRFMiddleware

_TOKEN = "valid-token-123"


def _make_app():
    """Create a tiny FastAPI app with the CSRF middleware for testing."""
    app = FastAPI()
    app.add_middleware(CSRFMiddleware)

    @app.get("/page")
    async def get_page():
        return {"ok": True}

    @app.post("/action")
    async def post_action():
        return {"ok": True}

    @app.delete("/action")
    async def delete_action():
        return {"ok": True}

    @app.post("/api/v1/query")
    async def api_query():
        return {"ok": True}

    @app.post("/graphql/execute")
    async def gql():
        return {"ok": True}

    return app


@pytest.fixture
def client(monkeypatch):
    monkeypatch.delenv("CSRF_DISABLED", raising=False)
    return TestClient(_make_app())


class TestCSRFMiddleware:
    def test_disabled_via_env(self, monkeypatch):
        monkeypatch.setenv("CSRF_DISABLED", "1")
        client = TestClient(_make_app())
        resp = client.post("/action")
        assert resp.status_code == 200

    def test_get_requests_pass(self, client):
        resp = client.get("/page")
        assert resp.status_code == 200

    def test_sets_cookie_on_first_response(self, client):
        resp = client.get("/page")
        assert "csrf_token" in resp.cookies


class TestUnsafeMethodsRequireCookieAndHeader:
    def test_missing_cookie_rejected(self, client):
        resp = client.post("/action", headers={"X-CSRF-Token": _TOKEN})
        assert resp.status_code == 403
        assert resp.json()["error"] == "csrf"

    def test_missing_header_rejected(self, client):
        client.cookies.set("csrf_token", _TOKEN)
        resp = client.post("/action")
        assert resp.status_code == 403

    def test_missing_both_rejected(self, client):
        resp = client.post("/action")
        assert resp.status_code == 403

    def test_empty_cookie_and_header_rejected(self, client):
        client.cookies.set("csrf_token", "")
        resp = client.post("/action", headers={"X-CSRF-Token": ""})
        assert resp.status_code == 403

    def test_mismatched_token_rejected(self, client):
        client.cookies.set("csrf_token", "abc123")
        resp = client.post("/action", headers={"X-CSRF-Token": "wrong"})
        assert resp.status_code == 403
        assert "csrf" in resp.json().get("error", "")

    def test_non_ascii_header_rejected_not_crashing(self, client):
        client.cookies.set("csrf_token", _TOKEN)
        resp = client.post(
            "/action", headers={"X-CSRF-Token": "é".encode("latin-1")}
        )
        assert resp.status_code == 403

    def test_matching_token_passes(self, client):
        client.cookies.set("csrf_token", _TOKEN)
        resp = client.post("/action", headers={"X-CSRF-Token": _TOKEN})
        assert resp.status_code == 200

    def test_delete_is_also_checked(self, client):
        resp = client.delete("/action")
        assert resp.status_code == 403

    def test_bearer_header_is_not_an_exemption(self, client):
        resp = client.post("/action", headers={"Authorization": "Bearer t"})
        assert resp.status_code == 403


class TestBypassPrefixes:
    def test_api_prefix_bypassed_without_cookie(self, client):
        resp = client.post(
            "/api/v1/query", headers={"Authorization": "Bearer m2m-token"}
        )
        assert resp.status_code == 200

    def test_api_prefix_bypassed_with_mismatch(self, client):
        client.cookies.set("csrf_token", "abc")
        resp = client.post("/api/v1/query", headers={"X-CSRF-Token": "different"})
        assert resp.status_code == 200

    def test_graphql_prefix_bypassed(self, client):
        resp = client.post("/graphql/execute")
        assert resp.status_code == 200
