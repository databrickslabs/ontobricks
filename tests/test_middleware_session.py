"""Tests for back.objects.session.middleware — file-based session middleware."""
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from starlette.requests import Request
from starlette.responses import Response
from starlette.testclient import TestClient

from back.objects.session.middleware import (
    FileSessionMiddleware,
    get_session,
    save_to_session,
    get_from_session,
)


class TestSessionHelpers:
    def test_get_session_present(self):
        request = MagicMock()
        request.state.session = {"key": "value"}
        assert get_session(request) == {"key": "value"}

    def test_get_session_missing(self):
        """When request.state exists but has no 'session' attribute."""
        from starlette.datastructures import State
        request = MagicMock()
        request.state = State()
        assert get_session(request) == {}

    def test_save_to_session(self):
        request = MagicMock()
        request.state.session = {}
        request.state.session_modified = False
        save_to_session(request, "foo", "bar")
        assert request.state.session["foo"] == "bar"
        assert request.state.session_modified is True

    def test_save_to_session_no_session(self):
        """When request.state exists but has no 'session' attribute, it's a no-op."""
        from starlette.datastructures import State
        request = MagicMock()
        request.state = State()
        save_to_session(request, "foo", "bar")

    def test_get_from_session(self):
        request = MagicMock()
        request.state.session = {"key": 42}
        assert get_from_session(request, "key") == 42
        assert get_from_session(request, "missing", "default") == "default"

    def test_get_from_session_no_session(self):
        """When request.state has no 'session', returns default."""
        from starlette.datastructures import State
        request = MagicMock()
        request.state = State()
        assert get_from_session(request, "key", "fallback") == "fallback"


class TestFileSessionMiddleware:
    def test_init_creates_directory(self, tmp_path):
        from fastapi import FastAPI
        app = FastAPI()
        session_dir = tmp_path / "sessions"
        middleware = FileSessionMiddleware(
            app, secret_key="test-key", session_dir=str(session_dir)
        )
        assert session_dir.exists()
        assert middleware.session_cookie == "session"

    def test_generate_session_id(self, tmp_path):
        from fastapi import FastAPI
        app = FastAPI()
        middleware = FileSessionMiddleware(
            app, secret_key="key", session_dir=str(tmp_path)
        )
        sid = middleware._generate_session_id()
        assert len(sid) == 32
        assert "-" not in sid

    def test_save_and_load_session(self, tmp_path):
        from fastapi import FastAPI
        app = FastAPI()
        middleware = FileSessionMiddleware(
            app, secret_key="key", session_dir=str(tmp_path)
        )
        sid = middleware._generate_session_id()
        data = {"user": "test", "count": 5}
        middleware._save_session(sid, data)
        loaded = middleware._load_session(sid)
        assert loaded["user"] == "test"
        assert loaded["count"] == 5

    def test_load_missing_session(self, tmp_path):
        from fastapi import FastAPI
        app = FastAPI()
        middleware = FileSessionMiddleware(
            app, secret_key="key", session_dir=str(tmp_path)
        )
        loaded = middleware._load_session("nonexistent")
        assert loaded == {}

    def test_load_corrupted_session(self, tmp_path):
        from fastapi import FastAPI
        app = FastAPI()
        middleware = FileSessionMiddleware(
            app, secret_key="key", session_dir=str(tmp_path)
        )
        sid = "corrupted_session"
        (tmp_path / sid).write_text("not valid json{{{")
        loaded = middleware._load_session(sid)
        assert loaded == {}

    def test_get_session_id_from_cookie(self, tmp_path):
        from fastapi import FastAPI
        app = FastAPI()
        middleware = FileSessionMiddleware(
            app, secret_key="key", session_dir=str(tmp_path)
        )
        request = MagicMock()
        request.cookies = {"session": "abc123"}
        assert middleware._get_session_id_from_cookie(request) == "abc123"

    def test_get_session_id_no_cookie(self, tmp_path):
        from fastapi import FastAPI
        app = FastAPI()
        middleware = FileSessionMiddleware(
            app, secret_key="key", session_dir=str(tmp_path)
        )
        request = MagicMock()
        request.cookies = {}
        assert middleware._get_session_id_from_cookie(request) is None

    def test_dispatch_integration(self, tmp_path):
        """Test full middleware dispatch via TestClient."""
        from fastapi import FastAPI

        app = FastAPI()
        app.add_middleware(
            FileSessionMiddleware,
            secret_key="integration-key",
            session_dir=str(tmp_path),
        )

        @app.get("/ping")
        async def ping():
            return {"pong": True}

        client = TestClient(app)
        resp = client.get("/ping")
        assert resp.status_code == 200
        assert "session" in resp.cookies
