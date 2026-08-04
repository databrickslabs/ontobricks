"""Tests for back.objects.session.FileSessionMiddleware — file-based session middleware."""

import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from starlette.requests import Request
from starlette.responses import Response
from starlette.testclient import TestClient

from back.objects.session.FileSessionMiddleware import (
    FileSessionMiddleware,
    _SESSION_ID_RE,
    get_session,
)


def _app_with_sessions(tmp_path):
    """An app with two routes: one that only reads, one that modifies."""
    from fastapi import FastAPI

    app = FastAPI()
    app.add_middleware(
        FileSessionMiddleware, secret_key="key", session_dir=str(tmp_path)
    )

    @app.get("/read")
    async def read(request: Request):
        return {"sid": request.state.session_id, "session": request.state.session}

    @app.get("/write")
    async def write(request: Request):
        request.state.session["hello"] = "world"
        request.state.session_modified = True
        return {"sid": request.state.session_id}

    return app


_VALID_ID = "0123456789abcdef0123456789abcdef"


class TestSessionFilesAreCreatedLazily:
    def test_a_request_without_a_cookie_writes_no_file(self, tmp_path):
        client = TestClient(_app_with_sessions(tmp_path))
        resp = client.get("/read")
        assert resp.status_code == 200
        assert "session" in resp.cookies
        assert list(tmp_path.iterdir()) == []

    def test_a_modified_session_is_written_under_the_cookie_id(self, tmp_path):
        client = TestClient(_app_with_sessions(tmp_path))
        resp = client.get("/write")
        sid = resp.json()["sid"]
        files = list(tmp_path.iterdir())
        assert [f.name for f in files] == [sid]
        assert json.loads(files[0].read_text()) == {"hello": "world"}

    def test_many_cookieless_requests_write_no_files(self, tmp_path):
        """The regression test for 82,200 accumulated session files.

        A fresh TestClient per request, because a single client replays the
        cookie from the first response and the loop would assert nothing.
        """
        app = _app_with_sessions(tmp_path)
        for _ in range(25):
            assert TestClient(app).get("/read").status_code == 200
        assert list(tmp_path.iterdir()) == []


class TestSessionIdsAreStable:
    def test_a_valid_cookie_with_no_file_keeps_its_id(self, tmp_path):
        client = TestClient(_app_with_sessions(tmp_path))
        client.cookies.set("session", _VALID_ID)
        resp = client.get("/read")
        assert resp.json()["sid"] == _VALID_ID
        assert resp.json()["session"] == {}
        assert list(tmp_path.iterdir()) == []

    def test_a_valid_cookie_with_a_file_loads_it(self, tmp_path):
        (tmp_path / _VALID_ID).write_text(json.dumps({"kept": True}))
        client = TestClient(_app_with_sessions(tmp_path))
        client.cookies.set("session", _VALID_ID)
        resp = client.get("/read")
        assert resp.json()["sid"] == _VALID_ID
        assert resp.json()["session"] == {"kept": True}

    def test_a_malformed_cookie_gets_a_fresh_valid_id(self, tmp_path):
        client = TestClient(_app_with_sessions(tmp_path))
        client.cookies.set("session", "../../../../etc/passwd")
        resp = client.get("/read")
        sid = resp.json()["sid"]
        assert sid != "../../../../etc/passwd"
        assert _SESSION_ID_RE.fullmatch(sid)
        assert list(tmp_path.iterdir()) == []


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


class TestSessionIdValidation:
    """The cookie value becomes a filename, so its shape must be checked."""

    def _middleware(self, tmp_path):
        from fastapi import FastAPI

        return FileSessionMiddleware(
            FastAPI(), secret_key="key", session_dir=str(tmp_path)
        )

    def _request_with_cookie(self, value):
        request = MagicMock()
        request.cookies = {"session": value}
        return request

    def test_a_generated_id_is_accepted(self, tmp_path):
        middleware = self._middleware(tmp_path)
        sid = middleware._generate_session_id()
        assert (
            middleware._get_session_id_from_cookie(self._request_with_cookie(sid))
            == sid
        )

    @pytest.mark.parametrize(
        "cookie_value",
        [
            pytest.param("../../../../etc/passwd", id="traversal_path"),
            pytest.param("..", id="bare_parent_dir"),
            pytest.param(".0123456789abcdef0123456789abcde", id="leading_dot"),
            pytest.param("0123456789ABCDEF0123456789ABCDEF", id="uppercase_hex"),
            pytest.param("abc123", id="short_hex_previously_accepted"),
            pytest.param("0" * 31, id="one_char_too_short"),
            pytest.param("0" * 33, id="one_char_too_long"),
            pytest.param("0123456789abcdef0123456789abcdeg", id="non_hex_char"),
            pytest.param("0" * 32 + "\n", id="trailing_newline"),
            pytest.param("", id="empty"),
        ],
    )
    def test_a_malformed_cookie_is_rejected(self, tmp_path, cookie_value):
        """Each id names the rule it guards.

        ``short_hex_previously_accepted`` is the regression marker: "abc123" was
        returned verbatim as a session id before this change.
        ``trailing_newline`` is why the pattern is matched with ``fullmatch`` —
        Python's ``$`` also matches immediately before a trailing newline.
        """
        middleware = self._middleware(tmp_path)
        assert (
            middleware._get_session_id_from_cookie(
                self._request_with_cookie(cookie_value)
            )
            is None
        )


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
        sid = middleware._generate_session_id()
        request = MagicMock()
        request.cookies = {"session": sid}
        assert middleware._get_session_id_from_cookie(request) == sid

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
