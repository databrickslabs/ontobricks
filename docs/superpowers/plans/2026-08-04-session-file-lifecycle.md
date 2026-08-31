# Session File Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `FileSessionMiddleware` from creating a session file for every
request, delete expired ones at startup, and stop using the raw cookie value as a
filesystem path.

**Architecture:** All behaviour lives in one module,
`src/back/objects/session/FileSessionMiddleware.py`. A validated session-id
pattern gates both the cookie and the reaper. `dispatch` stops writing on mint
and keeps the client's id when its file is absent, so a file is only born on the
existing `session_modified` save path. A throttled `os.utime` makes the file's
`mtime` mean "last used" so a startup sweep can delete by age safely.

**Tech Stack:** Python 3.11, FastAPI/Starlette `BaseHTTPMiddleware`, pytest with
`starlette.testclient.TestClient`, `uv` for running tests.

Design spec:
`documentation/superpowers/specs/2026-08-04-session-file-lifecycle-design.md`

## Global Constraints

- Run tests with `uv run --frozen pytest ...`. The `--frozen` flag is mandatory —
  a bare `uv run` poisons `uv.lock` and breaks the next deploy.
- Valid session id shape: exactly 32 lowercase hex characters, matched with
  `re.fullmatch`. Never `match` with `^...$` — Python's `$` also matches before a
  trailing newline.
- Nothing in this work may raise into a request or into app startup. Every
  filesystem operation added here is wrapped and logged.
- The touch helper must never create a file that does not exist. Never use
  `Path.touch()`, which creates.
- No new entries in `Settings`. The touch interval is a module constant.
- Current version for changelog purposes is **0.7.0** (`pyproject.toml:3`), so
  changelog entries go in `changelogs/v0.7.0/benoitcayladbx_2026-08-04.log`,
  which already exists — **append** a section, do not overwrite.
- The in-memory `_session_cache` unbounded-growth bug is explicitly out of scope.
  Do not fix it here.
- Commit subjects are plain sentence case, matching this repo's history — no
  conventional-commit prefixes. `git log --oneline -8` if in doubt.
- Branch: `fix/session-file-lifecycle`, cut from `develop` at `5fc722c`. The
  suite was green at that commit: 4483 passed, 276 skipped, 5 deselected,
  1 xfailed.

---

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `src/back/objects/session/FileSessionMiddleware.py` | All new behaviour: id pattern, cookie validation, lazy creation, touch helper, reaper function | 1, 2, 3, 4 |
| `src/back/objects/session/__init__.py` | Re-export `reap_expired_sessions` | 4 |
| `src/shared/fastapi/main.py` | Call the sweep from the lifespan | 4 |
| `tests/units/core/test_middleware_session.py` | All tests; extends the existing file | 1, 2, 3, 4 |
| `documentation/code_organization.md` | One-line description of the middleware | 5 |
| `changelogs/v0.7.0/benoitcayladbx_2026-08-04.log` | Changelog | 5 |

The module is 205 lines and cohesive; it is not split.

---

### Task 1: Session id validation

Closes the path-traversal / arbitrary-file-write hole, and establishes the
pattern the later tasks depend on.

**Files:**
- Modify: `src/back/objects/session/FileSessionMiddleware.py:8-11` (imports),
  new module constant after `_SESSION_BYPASS_PREFIXES` (ends line 33),
  `_get_session_id_from_cookie` at `:65-70`
- Test: `tests/units/core/test_middleware_session.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `_SESSION_ID_RE: re.Pattern` (module-level, used by Tasks 2 and 4),
  and `_get_session_id_from_cookie(self, request) -> Optional[str]` which now
  returns `None` for a malformed value.

- [ ] **Step 1: Write the failing tests**

Add to `tests/units/core/test_middleware_session.py`. Put this class after
`TestSessionHelpers`.

```python
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
```

`pytest` is already imported at the top of the test file.

- [ ] **Step 2: Repoint the existing test that encodes the old behaviour**

`tests/units/core/test_middleware_session.py:91-100` currently asserts that
`"abc123"` is returned as a session id. Replace the body so it exercises a valid
id — the rejection of `"abc123"` is now covered by
`test_a_short_hex_string_is_rejected` above.

```python
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run --frozen pytest tests/units/core/test_middleware_session.py -v`

Expected: exactly nine of the ten parametrized cases FAIL, each returning the
input string instead of `None` — for example
`test_a_malformed_cookie_is_rejected[traversal_path]` fails with
`assert '../../../../etc/passwd' is None`.

Three PASS already and that is correct, not a mistake in the test list:
`[empty]` is caught by the existing `if not cookie_value` guard, and
`test_a_generated_id_is_accepted` plus the repointed
`test_get_session_id_from_cookie` use a well-formed id, which passes through the
unvalidated code too. They are there to stop a later change from breaking the
happy path.

- [ ] **Step 4: Write the implementation**

Add `re` to the imports at the top of
`src/back/objects/session/FileSessionMiddleware.py` (currently `json`, `uuid`,
`pathlib.Path`, `typing`):

```python
import json
import re
import uuid
```

Add the constant immediately after the `_SESSION_BYPASS_PREFIXES` tuple (which
ends on line 33):

```python
# A session ID becomes a filename under ``session_dir``, so a value arriving in
# a cookie is only trusted when it has the exact shape _generate_session_id
# produces. Matched with fullmatch, never with ^...$: Python's ``$`` also
# matches immediately before a trailing newline, which would let a 33-character
# value through and resolve to a different file than the one validated.
_SESSION_ID_RE = re.compile(r"[0-9a-f]{32}")
```

Replace `_get_session_id_from_cookie` (`:65-70`) entirely:

```python
    def _get_session_id_from_cookie(self, request: Request) -> Optional[str]:
        """Return the session ID from the cookie, or ``None`` if unusable.

        A malformed value is reported as no cookie at all, which makes the
        caller mint a fresh ID — the same outcome a user with a corrupted
        cookie already got, so nothing legitimate regresses.
        """
        cookie_value = request.cookies.get(self.session_cookie)
        if not cookie_value:
            return None
        if not _SESSION_ID_RE.fullmatch(cookie_value):
            logger.debug(
                "Rejected malformed session cookie (%d chars, starts %r)",
                len(cookie_value),
                cookie_value[:8],
            )
            return None
        return cookie_value
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run --frozen pytest tests/units/core/test_middleware_session.py -v`
Expected: all PASS.

- [ ] **Step 6: Verify the tests would catch a regression**

Temporarily change `fullmatch` to `match` and re-run. Expected:
`test_a_malformed_cookie_is_rejected[trailing_newline]` and
`[one_char_too_long]` both FAIL — the pattern carries no `^`/`$` anchors, so
`match` accepts any string whose first 32 characters are hex. Then temporarily
change the pattern to `r"[0-9a-fA-F]{32}"` and re-run. Expected: only
`[uppercase_hex]` FAILS. Revert both mutations and confirm the file is
byte-identical to its pre-mutation state.

- [ ] **Step 7: Commit**

```bash
git add src/back/objects/session/FileSessionMiddleware.py tests/units/core/test_middleware_session.py
git commit -m "Validate the session cookie before using it as a filename"
```

---

### Task 2: Lazy file creation and stable session ids

Stops the per-request empty file and the concurrent-mint multiplication.

**Files:**
- Modify: `src/back/objects/session/FileSessionMiddleware.py:117-157` (the
  branchy middle of `dispatch`)
- Test: `tests/units/core/test_middleware_session.py`

**Interfaces:**
- Consumes: `_get_session_id_from_cookie` returning `None` for malformed values
  (Task 1), and the existing `_load_session(self, session_id) -> Dict[str, Any]`
  which already returns `{}` when there is neither a cache entry nor a file.
- Produces: a module-level test helper `_app_with_sessions(tmp_path) -> FastAPI`
  exposing `GET /read` and `GET /write`, reused by Task 3.

- [ ] **Step 1: Add the shared test app helper**

Add at module level in `tests/units/core/test_middleware_session.py`, after the
imports. Task 3 reuses this, so it is a module function, not a fixture on one
class.

```python
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
```

- [ ] **Step 2: Write the failing tests**

```python
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
```

Extend the import at the top of the test file so `_SESSION_ID_RE` is available:

```python
from back.objects.session.FileSessionMiddleware import (
    FileSessionMiddleware,
    _SESSION_ID_RE,
    get_session,
)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run --frozen pytest tests/units/core/test_middleware_session.py -k "Lazily or Stable" -v`

Expected FAILs:
`test_a_request_without_a_cookie_writes_no_file` finds one file instead of zero;
`test_many_cookieless_requests_write_no_files` finds 25;
`test_a_valid_cookie_with_no_file_keeps_its_id` gets a different `sid`;
`test_a_malformed_cookie_gets_a_fresh_valid_id` finds a stray file.
`test_a_modified_session_is_written_under_the_cookie_id` and
`test_a_valid_cookie_with_a_file_loads_it` already PASS.

- [ ] **Step 4: Write the implementation**

Replace lines 117-157 of `dispatch` — everything from the `# Get or create
session ID` comment down to the end of the `else` block, stopping just before
`# Attach session to request state`:

```python
        session_id = self._get_session_id_from_cookie(request)

        if not session_id:
            # No file is written here. A session that is never modified never
            # reaches disk, which is what stops cookie-less traffic from
            # littering session_dir with empty {} files.
            session_id = self._generate_session_id()
            session_data = {}
            logger.info("NEW session created: %s...", session_id[:8])
        else:
            # The ID is reused even when its file is missing. Minting a
            # replacement here made N concurrent requests carrying the same
            # cookie produce N sessions. _load_session resolves cache hit,
            # disk hit, and neither — so file presence is not our concern, and
            # deferring to it keeps a live cached session that lost its file.
            session_data = self._load_session(session_id)
            pd = session_data.get("domain_data") or session_data.get(
                "project_data", {}
            )
            ontology_classes = len(pd.get("ontology", {}).get("classes", []))
            mapping_entities = len(pd.get("assignment", {}).get("entities", []))
            mapping_rels = len(pd.get("assignment", {}).get("relationships", []))
            logger.debug(
                "Session %s...: %d classes, %d entity mappings, %d rel mappings",
                session_id[:8],
                ontology_classes,
                mapping_entities,
                mapping_rels,
            )
```

Note what is deliberately gone: both `self._save_session(...)` calls, the
`session_file = self.session_dir / session_id` / `if not session_file.exists():`
branch, and the `is_new_session` local — which was assigned in three places and
never read. Leave the rest of `dispatch` untouched; the end-of-request
`session_modified` block is now the only place a file is created.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run --frozen pytest tests/units/core/test_middleware_session.py -v`
Expected: all PASS, including the pre-existing `test_dispatch_integration`.

- [ ] **Step 6: Commit**

```bash
git add src/back/objects/session/FileSessionMiddleware.py tests/units/core/test_middleware_session.py
git commit -m "Create session files lazily and keep the client's session id"
```

---

### Task 3: Idle refresh so mtime means last use

Without this, Task 4 would delete the files of users who are browsing but not
modifying their session, because the cookie's `max_age` is renewed on every
response while the file's `mtime` is not.

**Files:**
- Modify: `src/back/objects/session/FileSessionMiddleware.py` — imports, a new
  module constant, a new `_touch_session` method, one call in `dispatch`
- Test: `tests/units/core/test_middleware_session.py`

**Interfaces:**
- Consumes: `_app_with_sessions` and `_VALID_ID` from Task 2; the `else` branch
  of `dispatch` from Task 2.
- Produces: `_touch_session(self, session_id: str) -> None` and
  `_SESSION_TOUCH_INTERVAL: int` (seconds), both used by Task 4's reasoning but
  not called by it.

- [ ] **Step 1: Write the failing tests**

Add `import os` and `import time` to the test file's imports, then:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest tests/units/core/test_middleware_session.py -k Touched -v`

Expected: `test_a_stale_file_is_touched` FAILS on the mtime comparison,
`test_touching_never_creates_a_missing_file` FAILS with
`AttributeError: ... has no attribute '_touch_session'`. The other two PASS
vacuously today, which is fine — they are the guard rails, not the driver.

- [ ] **Step 3: Write the implementation**

Add to the imports:

```python
import json
import os
import re
import time
import uuid
```

Add the constant next to `_SESSION_ID_RE`:

```python
# How stale a session file's mtime must be before a read refreshes it. Bounds
# the metadata writes to one per session per interval; must stay well below
# Settings.session_max_age or reap_expired_sessions would delete live sessions.
_SESSION_TOUCH_INTERVAL = 3600
```

Add the method after `_save_session` (which ends at line 99):

```python
    def _touch_session(self, session_id: str) -> None:
        """Refresh a session file's mtime so it records last *use*, not last edit.

        ``reap_expired_sessions`` deletes by mtime, and the session cookie is
        renewed on every response — so without this a session that is read for
        days but never modified would be reaped while its cookie is still
        valid, and the user would silently land on an empty session.

        Never creates the file: ``Path.touch()`` would, ``os.utime`` on a
        missing path raises ``FileNotFoundError`` and is swallowed below. That
        also covers the file being unlinked between the stat and the utime.
        """
        session_file = self.session_dir / session_id
        try:
            if time.time() - session_file.stat().st_mtime < _SESSION_TOUCH_INTERVAL:
                return
            os.utime(session_file, None)
        except OSError as e:
            logger.debug("Could not touch session %s...: %s", session_id[:8], e)
```

In `dispatch`, call it as the first statement of the `else` branch added in
Task 2, before `_load_session`:

```python
        else:
            self._touch_session(session_id)
            session_data = self._load_session(session_id)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --frozen pytest tests/units/core/test_middleware_session.py -v`
Expected: all PASS.

- [ ] **Step 5: Verify the throttle is actually tested**

Temporarily delete the `if time.time() - ... < _SESSION_TOUCH_INTERVAL: return`
line and re-run. Expected: `test_a_fresh_file_is_not_touched` FAILS. Revert.

- [ ] **Step 6: Commit**

```bash
git add src/back/objects/session/FileSessionMiddleware.py tests/units/core/test_middleware_session.py
git commit -m "Track last use on session files so expiry means idle"
```

---

### Task 4: Startup sweep of expired session files

**Files:**
- Modify: `src/back/objects/session/FileSessionMiddleware.py` — new module-level
  function after the `FileSessionMiddleware` class, before `get_session` at `:202`
- Modify: `src/back/objects/session/__init__.py:3-6` and `:17-29`
- Modify: `src/shared/fastapi/main.py:21` and `:106`
- Test: `tests/units/core/test_middleware_session.py`

**Interfaces:**
- Consumes: `_SESSION_ID_RE` (Task 1), and the `import time` that Task 3 adds to
  `FileSessionMiddleware.py`. If you are executing this task without Task 3,
  add `import time` yourself — the reaper needs it.
- Produces: `reap_expired_sessions(session_dir: str, max_age: int) -> int`,
  exported from `back.objects.session`.

- [ ] **Step 1: Write the failing tests**

```python
class TestReapExpiredSessions:
    def _aged(self, path, seconds_old):
        path.write_text("{}")
        stamp = time.time() - seconds_old
        os.utime(path, (stamp, stamp))
        return path

    def test_an_expired_session_file_is_removed(self, tmp_path):
        expired = self._aged(tmp_path / ("a" * 32), 90_000)
        assert reap_expired_sessions(str(tmp_path), 86_400) == 1
        assert not expired.exists()

    def test_a_recent_session_file_survives(self, tmp_path):
        fresh = self._aged(tmp_path / ("b" * 32), 60)
        assert reap_expired_sessions(str(tmp_path), 86_400) == 0
        assert fresh.exists()

    def test_an_unrelated_old_file_is_left_alone(self, tmp_path):
        other = self._aged(tmp_path / "notes.txt", 90_000)
        assert reap_expired_sessions(str(tmp_path), 86_400) == 0
        assert other.exists()

    def test_a_near_miss_name_is_left_alone(self, tmp_path):
        """31 hex chars — a sloppier pattern would match this."""
        near = self._aged(tmp_path / ("c" * 31), 90_000)
        assert reap_expired_sessions(str(tmp_path), 86_400) == 0
        assert near.exists()

    def test_one_undeletable_file_does_not_stop_the_sweep(self, tmp_path):
        stuck = self._aged(tmp_path / ("d" * 32), 90_000)
        other = self._aged(tmp_path / ("e" * 32), 90_000)
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
```

Add `from pathlib import Path` to the test imports, and extend the middleware
import:

```python
from back.objects.session.FileSessionMiddleware import (
    FileSessionMiddleware,
    _SESSION_ID_RE,
    get_session,
    reap_expired_sessions,
)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest tests/units/core/test_middleware_session.py -k Reap -v`
Expected: collection error — `ImportError: cannot import name 'reap_expired_sessions'`.

- [ ] **Step 3: Write the implementation**

Add to `FileSessionMiddleware.py` after the class body and before `get_session`
(line 202):

```python
def reap_expired_sessions(session_dir: str, max_age: int) -> int:
    """Delete session files unused for longer than ``max_age`` seconds.

    Only names matching :data:`_SESSION_ID_RE` are considered — the directory is
    shared with whatever else lands there, and a reaper that deletes purely by
    age eventually deletes something it did not create.

    Returns the number of files removed. Never raises: this runs during app
    startup, and a sweep failure must not stop the app from booting.
    """
    directory = Path(session_dir)
    cutoff = time.time() - max_age
    removed = 0

    try:
        entries = list(directory.iterdir())
    except OSError as e:
        logger.warning("Could not scan session dir %s: %s", session_dir, e)
        return 0

    for entry in entries:
        if not _SESSION_ID_RE.fullmatch(entry.name):
            continue
        try:
            if entry.stat().st_mtime >= cutoff:
                continue
            entry.unlink()
            removed += 1
        except OSError as e:
            logger.warning("Could not remove expired session %s: %s", entry.name, e)

    return removed
```

- [ ] **Step 4: Export it**

In `src/back/objects/session/__init__.py`, extend the import at lines 3-6 and the
`__all__` list:

```python
from .FileSessionMiddleware import (
    FileSessionMiddleware,
    get_session,
    reap_expired_sessions,
)
```

and add `"reap_expired_sessions",` to `__all__` immediately after
`"get_session",`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run --frozen pytest tests/units/core/test_middleware_session.py -v`
Expected: all PASS.

- [ ] **Step 6: Verify the name filter is actually tested**

Temporarily change `if not _SESSION_ID_RE.fullmatch(entry.name): continue` to
`pass` and re-run. Expected: both
`test_an_unrelated_old_file_is_left_alone` and
`test_a_near_miss_name_is_left_alone` FAIL. Revert.

- [ ] **Step 7: Wire it into the lifespan**

In `src/shared/fastapi/main.py`, extend the import on line 21:

```python
from back.objects.session import FileSessionMiddleware, reap_expired_sessions
```

Then, in `lifespan`, immediately after
`os.makedirs(settings.session_dir, exist_ok=True)` on line 106 and before the
existing `logger.info("OntoBricks FastAPI starting — ...")`:

```python
    try:
        reaped = reap_expired_sessions(settings.session_dir, settings.session_max_age)
        if reaped:
            logger.info("Removed %d expired session file(s) at startup", reaped)
    except Exception as e:
        logger.warning("Session sweep failed, continuing startup: %s", e)
```

The broad `except Exception` is deliberate here even though the function already
swallows `OSError`: startup must survive anything, including a future refactor of
the helper.

- [ ] **Step 8: Verify the app still boots and the sweep runs**

Run: `uv run --frozen pytest tests/units/core/test_middleware_session.py -v`
Expected: all PASS.

Then confirm the lifespan is importable and the wiring is syntactically sound:

Run: `uv run --frozen python -c "from shared.fastapi.main import lifespan; print('ok')"`
Expected: `ok`

- [ ] **Step 9: Commit**

```bash
git add src/back/objects/session/FileSessionMiddleware.py src/back/objects/session/__init__.py src/shared/fastapi/main.py tests/units/core/test_middleware_session.py
git commit -m "Sweep expired session files at startup"
```

---

### Task 5: Full test run, docs, changelog

**Files:**
- Modify: `documentation/code_organization.md:153`
- Modify: `changelogs/v0.7.0/benoitcayladbx_2026-08-04.log` (append)

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Run the whole unit suite**

Run: `uv run --frozen pytest -q -m "not scenario"`

Expected: PASS. If `tests/units/mapping/test_pge_session_extras_retention.py`
fails, read it before changing anything — it exercises
`Mapping.save_mappings_to_session`, which reads session files directly and
already falls back to the in-memory session when the file is absent. The
expected outcome is that it passes unchanged; a failure means the fallback is
narrower than the spec assumed and needs discussion, not a quick fix.

- [ ] **Step 2: Update the middleware description in the docs**

`documentation/code_organization.md:153` currently reads:

```markdown
   - **FileSessionMiddleware** — cookie-backed **file sessions** (JSON on disk under `settings.session_dir`); skips static, docs, health, and `/tasks/*` so task polling does not churn session I/O.
```

Replace with:

```markdown
   - **FileSessionMiddleware** — cookie-backed **file sessions** (JSON on disk under `settings.session_dir`); skips static, docs, health, and `/tasks/*` so task polling does not churn session I/O. The cookie must be 32 lowercase hex characters, since it is used as a filename; anything else mints a fresh ID. Files are written only once a session is actually modified, their mtime tracks last use, and `reap_expired_sessions()` clears ones older than `settings.session_max_age` at startup.
```

- [ ] **Step 3: Append the changelog section**

Append to `changelogs/v0.7.0/benoitcayladbx_2026-08-04.log`, matching the
heading style already used in that file:

```markdown
## Stop session files from accumulating, and stop trusting the cookie as a path

**Context.** `settings.session_dir` had grown to 327 MB across 82,200 files on a
development machine, which is what made `make clean` fail with
`rm: fastapi_session: Directory not empty`. The count was not explained by user
count: `dispatch` wrote a 2-byte `{}` file for every minted ID, a cookie whose
file was missing was answered with a brand new ID rather than reusing the one the
client held (so N concurrent requests produced N sessions), and nothing ever
expired. Tracing that surfaced a second problem — the cookie value was used
verbatim as a filename, so a cookie of `../../../../etc/foo` would have had
session JSON written over that path on any request that modified the session.

**Changes.**

1. `src/back/objects/session/FileSessionMiddleware.py` — added `_SESSION_ID_RE`
   and made `_get_session_id_from_cookie` enforce it with `fullmatch`, so a
   cookie is only used as a filename when it is exactly 32 lowercase hex
   characters. A malformed value is treated as no cookie, which mints a fresh ID
   — the same outcome a corrupted cookie already produced. `fullmatch` rather
   than `^...$` because Python's `$` also matches before a trailing newline.
2. `src/back/objects/session/FileSessionMiddleware.py` — `dispatch` no longer
   writes on mint, and no longer replaces an ID whose file is missing. It now
   defers to `_load_session`, which already resolves cache hit, disk hit, and
   neither, so a live cached session that lost its file is preserved instead of
   reset. Files are born only on the existing `session_modified` save path.
   Removed the unused `is_new_session` local.
3. `src/back/objects/session/FileSessionMiddleware.py` — added `_touch_session`,
   which refreshes a session file's mtime on read when it is already older than
   an hour, so expiry means "idle" rather than "not edited". Necessary because
   the cookie's `max_age` is renewed on every response while the file's mtime is
   not, so a user browsing read-only for a day would otherwise be reaped while
   still holding a valid cookie.
4. `src/back/objects/session/FileSessionMiddleware.py` — added
   `reap_expired_sessions(session_dir, max_age)`, which deletes files older than
   `max_age`, considers only names matching the session-ID pattern, logs and
   skips per-file failures, and never raises.
5. `src/back/objects/session/__init__.py` — exported `reap_expired_sessions`.
6. `src/shared/fastapi/main.py` — the lifespan runs the sweep after creating the
   session directory, wrapped so a failure logs a warning and startup continues.
7. `tests/units/core/test_middleware_session.py` — repointed
   `test_get_session_id_from_cookie`, which asserted that a cookie of `"abc123"`
   was accepted as a session ID, and added coverage for validation, lazy
   creation, ID stability, the touch throttle, and the reaper.
8. `documentation/code_organization.md` — described the new lifecycle.

**Not addressed.** The in-memory `_session_cache` still grows unbounded for the
process lifetime. The cookie remains unsigned, so this narrows the blast radius
of a forged cookie to guessing another user's random 122-bit ID rather than
removing forgery as a category. The sweep runs at startup only, so a deployment
that never restarts does not reclaim disk from sessions that did expire — a
periodic sweep would first have to deal with the reaper knowing nothing about
`_session_cache`.

**Modified files.**
- `src/back/objects/session/FileSessionMiddleware.py`
- `src/back/objects/session/__init__.py`
- `src/shared/fastapi/main.py`
- `tests/units/core/test_middleware_session.py`
- `documentation/code_organization.md`

**Tests.** `uv run --frozen pytest -q -m "not scenario"` — fill in the actual
result here.
```

- [ ] **Step 4: Fill in the real test result**

Replace the placeholder line in the changelog with the actual pass/fail counts
from Step 1. Do not leave "fill in the actual result here" in the file.

- [ ] **Step 5: Commit**

```bash
git add documentation/code_organization.md changelogs/v0.7.0/benoitcayladbx_2026-08-04.log
git commit -m "Document the session file lifecycle and log the change"
```

---

## Manual verification

Not a test step, but worth doing once before deploying, because the payoff of
this work is a number on disk.

1. Note the current file count: `ls -1 <session_dir> | wc -l`.
2. Start the app, load a few pages, and count again. Before this change the count
   climbed with every request; it should now only grow when a session is
   actually modified (selecting a domain, editing an ontology).
3. Restart the app and check the startup log for
   `Removed N expired session file(s) at startup`.
4. Confirm your browser keeps the same `session` cookie value across a restart
   rather than being issued a new one.
