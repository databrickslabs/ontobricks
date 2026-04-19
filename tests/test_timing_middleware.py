"""Tests for shared.fastapi.timing – RequestTimingMiddleware."""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from shared.fastapi.timing import RequestTimingMiddleware


def _make_app():
    app = FastAPI()
    app.add_middleware(RequestTimingMiddleware)

    @app.get("/page")
    async def get_page():
        return {"ok": True}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/static/file.js")
    async def static_file():
        return "js"

    return app


class TestRequestTimingMiddleware:
    def test_normal_request(self):
        client = TestClient(_make_app())
        resp = client.get("/page")
        assert resp.status_code == 200

    def test_health_skipped(self):
        client = TestClient(_make_app())
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_static_skipped(self):
        client = TestClient(_make_app())
        resp = client.get("/static/file.js")
        assert resp.status_code == 200
