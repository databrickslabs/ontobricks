"""
Custom File-Based Session Middleware for FastAPI

This middleware provides file-based session storage for FastAPI.
It stores session data as JSON files in a configurable directory.
"""

import json
import os
import re
import time
import uuid
from pathlib import Path
from typing import Optional, Dict, Any

from back.core.logging import get_logger
from starlette.middleware.base import BaseHTTPMiddleware

logger = get_logger(__name__)
from starlette.requests import Request
from starlette.responses import Response


_SESSION_BYPASS_PREFIXES = (
    "/static/",
    "/tasks/",
    "/tasks",
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/api/docs",
    "/api/redoc",
    "/api/openapi.json",
    "/favicon.ico",
)

# A session ID becomes a filename under ``session_dir``, so a value arriving in
# a cookie is only trusted when it has the exact shape _generate_session_id
# produces. Matched with fullmatch, never with ^...$: Python's ``$`` also
# matches immediately before a trailing newline, which would let a 33-character
# value through and resolve to a different file than the one validated.
_SESSION_ID_RE = re.compile(r"[0-9a-f]{32}")

# How stale a session file's mtime must be before a read refreshes it. Bounds
# the metadata writes to one per session per interval; must stay well below
# Settings.session_max_age or reap_expired_sessions would delete live sessions.
_SESSION_TOUCH_INTERVAL = 3600


def is_valid_session_id(value: str) -> bool:
    """Return ``True`` iff *value* has the shape a generated session id has.

    A session id is exactly 32 lowercase hex characters — the format
    ``str(uuid.uuid4()).replace("-", "")`` produces.  Centralising the check
    here means there is one source of truth for the pattern wherever a
    caller-supplied id is used as a filesystem path.
    """
    return bool(_SESSION_ID_RE.fullmatch(value))


class FileSessionMiddleware(BaseHTTPMiddleware):
    """File-based session middleware for FastAPI.

    Register with ``app.add_middleware(FileSessionMiddleware, secret_key=...,
    session_dir=..., max_age=...)``.
    """

    def __init__(
        self,
        app,
        secret_key: str,
        session_dir: str = "./fastapi_session",
        session_cookie: str = "session",
        max_age: int = 86400,
        same_site: str = "lax",
        https_only: bool = False,
    ):
        super().__init__(app)
        self.secret_key = secret_key
        self.session_dir = Path(session_dir)
        self.session_cookie = session_cookie
        self.max_age = max_age
        self.same_site = same_site
        self.https_only = https_only
        self._session_cache: Dict[str, Dict[str, Any]] = {}

        # Ensure session directory exists
        self.session_dir.mkdir(parents=True, exist_ok=True)

    def _get_session_id_from_cookie(self, request: Request) -> Optional[str]:
        """Return the session ID from the cookie, or ``None`` if unusable.

        A malformed value is reported as no cookie at all, which makes the
        caller mint a fresh ID — the same outcome a user with a corrupted
        cookie already got, so nothing legitimate regresses.
        """
        cookie_value = request.cookies.get(self.session_cookie)
        if not cookie_value:
            return None
        if not is_valid_session_id(cookie_value):
            logger.debug(
                "Rejected malformed session cookie (%d chars, starts %r)",
                len(cookie_value),
                cookie_value[:8],
            )
            return None
        return cookie_value

    def _load_session(self, session_id: str) -> Dict[str, Any]:
        """Load session data, preferring the in-memory cache over disk."""
        cached = self._session_cache.get(session_id)
        if cached is not None:
            return cached

        session_file = self.session_dir / session_id
        if session_file.exists():
            try:
                content = session_file.read_text()
                if content.startswith("{"):
                    data = json.loads(content)
                    self._session_cache[session_id] = data
                    return data
                return {}
            except (json.JSONDecodeError, Exception) as e:
                logger.exception("Error loading session %s: %s", session_id, e)
                return {}
        return {}

    def _save_session(self, session_id: str, data: Dict[str, Any]):
        """Save session data to file and update in-memory cache."""
        self._session_cache[session_id] = data
        session_file = self.session_dir / session_id
        try:
            session_file.write_text(json.dumps(data, default=str))
        except Exception as e:
            logger.exception("Error saving session %s: %s", session_id, e)

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

    def _generate_session_id(self) -> str:
        """Generate a new session ID."""
        return str(uuid.uuid4()).replace("-", "")

    async def dispatch(self, request: Request, call_next) -> Response:
        """Process request with session handling."""
        # Raw routed path, not request.url.path: the latter is reconstructed
        # from the Host header and could be poisoned (BadHost / CVE-2026-48710).
        path = request.scope["path"]

        if any(path.startswith(p) for p in _SESSION_BYPASS_PREFIXES):
            request.state.session = {}
            request.state.session_id = ""
            request.state.session_modified = False
            return await call_next(request)

        session_id = self._get_session_id_from_cookie(request)

        if not session_id:
            # No file is written here. A session that is never modified never
            # reaches disk, which is what stops cookie-less traffic from
            # littering session_dir with empty {} files.
            session_id = self._generate_session_id()
            session_data = {}
            logger.info("NEW session created: %s...", session_id[:8])
        else:
            self._touch_session(session_id)
            # The ID is reused even when its file is missing. Minting a
            # replacement here made N concurrent requests carrying the same
            # cookie produce N sessions. _load_session resolves cache hit,
            # disk hit, and neither — so file presence is not our concern, and
            # deferring to it keeps a live cached session that lost its file.
            session_data = self._load_session(session_id)
            if session_data:
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

        # Attach session to request state
        request.state.session = session_data
        request.state.session_id = session_id
        request.state.session_modified = False

        # Process request
        response = await call_next(request)

        # ONLY save session if it was explicitly modified
        # This prevents race conditions where concurrent requests overwrite each other
        if (
            hasattr(request.state, "session_modified")
            and request.state.session_modified
        ):
            pd = request.state.session.get("domain_data") or request.state.session.get(
                "project_data", {}
            )
            ontology_classes = len(pd.get("ontology", {}).get("classes", []))
            mapping_entities = len(pd.get("assignment", {}).get("entities", []))
            mapping_rels = len(pd.get("assignment", {}).get("relationships", []))
            logger.info(
                "SAVING session %s... with %d ontology classes, %d entity mappings, %d rel mappings (modified=True)",
                session_id[:8],
                ontology_classes,
                mapping_entities,
                mapping_rels,
            )
            self._save_session(session_id, request.state.session)

        response.set_cookie(
            key=self.session_cookie,
            value=session_id,
            max_age=self.max_age,
            path="/",
            httponly=False,
            samesite=self.same_site,
            secure=self.https_only,
            domain=None,
        )

        return response


def reap_expired_sessions(session_dir: str, max_age: int) -> int:
    """Delete session files unused for longer than ``max_age`` seconds.

    Only names matching :data:`_SESSION_ID_RE` are considered — the directory is
    shared with whatever else lands there, and a reaper that deletes purely by
    age eventually deletes something it did not create.

    The cutoff includes a grace window of :data:`_SESSION_TOUCH_INTERVAL` because
    ``_touch_session`` only refreshes a file's mtime at most once per interval.
    A file whose mtime is ``max_age`` old may have been used up to
    ``_SESSION_TOUCH_INTERVAL`` seconds ago — exactly within the cookie's
    remaining lifetime.  Using ``max_age + _SESSION_TOUCH_INTERVAL`` as the
    threshold guarantees a file is not deleted while its cookie is still valid,
    regardless of the configured ``max_age``.

    Returns the number of files removed. Never raises: this runs during app
    startup, and a sweep failure must not stop the app from booting.
    """
    directory = Path(session_dir)
    cutoff = time.time() - max_age - _SESSION_TOUCH_INTERVAL
    removed = 0

    try:
        entries = list(directory.iterdir())
    except OSError as e:
        logger.warning("Could not scan session dir %s: %s", session_dir, e)
        return 0

    for entry in entries:
        if not is_valid_session_id(entry.name):
            continue
        try:
            if entry.stat(follow_symlinks=False).st_mtime >= cutoff:
                continue
            entry.unlink()
            removed += 1
        except OSError as e:
            logger.warning("Could not remove expired session %s: %s", entry.name, e)

    return removed


def get_session(request: Request) -> Dict[str, Any]:
    """Dependency that returns the session dict from ``request.state``."""
    return getattr(request.state, "session", {})
