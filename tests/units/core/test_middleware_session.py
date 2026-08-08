"""Tests for back.objects.session.FileSessionMiddleware — file-based session middleware."""

import json
import os
import time
import pytest
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch

from starlette.requests import Request
from starlette.responses import Response
from starlette.testclient import TestClient

from back.objects.session.FileSessionMiddleware import (
    FileSessionMiddleware,
    _SESSION_ID_RE,
    get_session,
    reap_expired_sessions,
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

    def test_traversal_cookie_on_write_creates_no_file_outside_session_dir(
        self, tmp_path
    ):
        """The arbitrary-file-write exploit is closed: a traversal-shaped cookie
        sent to a route that marks the session modified must not create or
        overwrite any file outside ``session_dir``.
        """
        client = TestClient(_app_with_sessions(tmp_path))
        client.cookies.set("session", "../evil")
        resp = client.get("/write")
        assert resp.status_code == 200
        # Nothing escaped the session directory.
        assert not (tmp_path.parent / "evil").exists()
        # The session was saved under a fresh valid id inside session_dir.
        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert _SESSION_ID_RE.fullmatch(files[0].name)


class TestSessionFilesAreTouchedOnUse:
    def _aged_file(self, tmp_path, seconds_old):
        path = tmp_path / _VALID_ID
        path.write_text("{}")
        stamp = time.time() - seconds_old
        os.utime(path, (stamp, stamp))
        return path

    def test_a_stale_file_is_touched(self, tmp_path):
        path = self._aged_file(tmp_path, 7200)
        before = path.stat().st_mtime
        client = TestClient(_app_with_sessions(tmp_path))
        client.cookies.set("session", _VALID_ID)
        assert client.get("/read").status_code == 200
        assert path.stat().st_mtime > before

    def test_a_fresh_file_is_not_touched(self, tmp_path):
        path = self._aged_file(tmp_path, 60)
        before = path.stat().st_mtime
        client = TestClient(_app_with_sessions(tmp_path))
        client.cookies.set("session", _VALID_ID)
        assert client.get("/read").status_code == 200
        assert path.stat().st_mtime == before

    def test_touching_never_creates_a_missing_file(self, tmp_path):
        from fastapi import FastAPI

        middleware = FileSessionMiddleware(
            FastAPI(), secret_key="key", session_dir=str(tmp_path)
        )
        middleware._touch_session(_VALID_ID)
        assert list(tmp_path.iterdir()) == []

    def test_a_failing_touch_does_not_break_the_request(self, tmp_path):
        self._aged_file(tmp_path, 7200)
        client = TestClient(_app_with_sessions(tmp_path))
        client.cookies.set("session", _VALID_ID)
        with patch("os.utime", side_effect=OSError("read-only filesystem")):
            resp = client.get("/read")
        assert resp.status_code == 200


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


class TestReapExpiredSessions:
    def _aged(self, path, seconds_old):
        path.write_text("{}")
        stamp = time.time() - seconds_old
        os.utime(path, (stamp, stamp))
        return path

    # The reaper cutoff is max_age + _SESSION_TOUCH_INTERVAL (90_000 for the
    # defaults 86_400 + 3_600).  Files aged beyond that threshold are deleted;
    # files inside it survive — including those in the grace window that would
    # have been wrongly reaped by the old `now - max_age` cutoff.

    def test_an_expired_session_file_is_removed(self, tmp_path):
        # 100_000 > 86_400 + 3_600 = 90_000 — genuinely beyond the grace window.
        expired = self._aged(tmp_path / ("a" * 32), 100_000)
        assert reap_expired_sessions(str(tmp_path), 86_400) == 1
        assert not expired.exists()

    def test_a_session_in_the_grace_window_survives(self, tmp_path):
        """A file whose mtime is between max_age and max_age+_SESSION_TOUCH_INTERVAL
        must not be deleted — its cookie may still be valid.

        This is the case the old ``now - max_age`` cutoff got wrong.
        """
        from back.objects.session.FileSessionMiddleware import _SESSION_TOUCH_INTERVAL

        # Age the file to max_age + half the touch interval: mtime looks
        # "expired" by the old logic, but the session may have been used
        # _SESSION_TOUCH_INTERVAL/2 seconds ago.
        age = 86_400 + _SESSION_TOUCH_INTERVAL // 2
        grace = self._aged(tmp_path / ("f" * 32), age)
        assert reap_expired_sessions(str(tmp_path), 86_400) == 0
        assert grace.exists()

    def test_a_recent_session_file_survives(self, tmp_path):
        fresh = self._aged(tmp_path / ("b" * 32), 60)
        assert reap_expired_sessions(str(tmp_path), 86_400) == 0
        assert fresh.exists()

    def test_an_unrelated_old_file_is_left_alone(self, tmp_path):
        other = self._aged(tmp_path / "notes.txt", 100_000)
        assert reap_expired_sessions(str(tmp_path), 86_400) == 0
        assert other.exists()

    def test_a_near_miss_name_is_left_alone(self, tmp_path):
        """31 hex chars — a sloppier pattern would match this."""
        near = self._aged(tmp_path / ("c" * 31), 100_000)
        assert reap_expired_sessions(str(tmp_path), 86_400) == 0
        assert near.exists()

    def test_one_undeletable_file_does_not_stop_the_sweep(self, tmp_path):
        # 100_000 > 90_000 grace window — both files are genuinely expired.
        stuck = self._aged(tmp_path / ("d" * 32), 100_000)
        other = self._aged(tmp_path / ("e" * 32), 100_000)
        real_unlink = Path.unlink

        def flaky_unlink(self, *args, **kwargs):
            if self.name == stuck.name:
                raise OSError("device busy")
            return real_unlink(self, *args, **kwargs)

        with patch.object(Path, "unlink", flaky_unlink):
            assert reap_expired_sessions(str(tmp_path), 86_400) == 1
        assert stuck.exists()
        assert not other.exists()

    def test_a_missing_directory_returns_zero(self, tmp_path):
        assert reap_expired_sessions(str(tmp_path / "nope"), 86_400) == 0

    def test_a_dangling_symlink_is_reaped_not_warned_forever(self, tmp_path):
        """A dangling symlink with a valid name must be removed, not skipped.

        Before the fix, entry.stat() followed the link, raised
        FileNotFoundError on every boot, and the link was never unlinked.
        entry.stat(follow_symlinks=False) sees the link itself, so it ages
        normally and is unlinked.
        """
        link = tmp_path / ("0" * 32)
        target = tmp_path / "gone_target"
        link.symlink_to(target)  # target does not exist — dangling
        stamp = time.time() - 100_000
        os.utime(link, (stamp, stamp), follow_symlinks=False)
        assert reap_expired_sessions(str(tmp_path), 86_400) == 1
        assert not link.is_symlink()

    def test_a_directory_named_with_32_hex_chars_survives(self, tmp_path):
        """A directory whose name matches the session pattern must not raise.

        unlink() on a directory raises IsADirectoryError (Linux) or
        PermissionError (macOS) — both OSError subclasses — so the per-entry
        handler logs and continues.  Assert on outcome, not exception type.
        """
        dir_entry = tmp_path / ("9" * 32)
        dir_entry.mkdir()
        stamp = time.time() - 100_000
        os.utime(dir_entry, (stamp, stamp))
        # Must not raise, and must leave the directory intact.
        assert reap_expired_sessions(str(tmp_path), 86_400) == 0
        assert dir_entry.exists() and dir_entry.is_dir()


class TestLifespanWiresReaper:
    """Ensure reap_expired_sessions is called during app startup.

    The test enters the real lifespan but patches its three heavy
    collaborators — reap_expired_sessions, setup_tracing (imported locally
    inside lifespan from agents.tracing), and get_scheduler (imported locally
    from back.objects.registry) — so only the wiring between them is
    exercised.  If the reap_expired_sessions call block is removed from
    lifespan, this test fails.
    """

    def test_reaper_is_called_at_startup_with_session_settings(self):
        import asyncio
        from shared.config.settings import get_settings

        settings = get_settings()

        mock_scheduler = MagicMock()
        mock_scheduler.start = MagicMock()
        mock_scheduler.stop = MagicMock()

        with patch(
            "shared.fastapi.main.reap_expired_sessions", return_value=0
        ) as mock_reap, patch(
            "agents.tracing.setup_tracing"
        ), patch(
            "back.objects.registry.get_scheduler", return_value=mock_scheduler
        ):
            # Import here so the patch is in place when the lifespan runs.
            from shared.fastapi.main import lifespan, app

            async def run():
                async with lifespan(app):
                    pass

            asyncio.run(run())

        mock_reap.assert_called_once_with(
            settings.session_dir, settings.session_max_age
        )
