"""
Custom File-Based Session Middleware for FastAPI

This middleware provides file-based session storage for FastAPI.
It stores session data as JSON files in a configurable directory.
"""
import json
import os
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
        https_only: bool = False
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
        """Extract and validate session ID from cookie."""
        cookie_value = request.cookies.get(self.session_cookie)
        if not cookie_value:
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
                if content.startswith('{'):
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
    
    def _generate_session_id(self) -> str:
        """Generate a new session ID."""
        return str(uuid.uuid4()).replace('-', '')
    
    async def dispatch(self, request: Request, call_next) -> Response:
        """Process request with session handling."""
        path = request.url.path

        if any(path.startswith(p) for p in _SESSION_BYPASS_PREFIXES):
            request.state.session = {}
            request.state.session_id = ""
            request.state.session_modified = False
            return await call_next(request)

        # Get or create session ID
        session_id = self._get_session_id_from_cookie(request)
        is_new_session = False
        
        if not session_id:
            session_id = self._generate_session_id()
            is_new_session = True
            session_data = {}
            # Create empty session file immediately
            self._save_session(session_id, session_data)
            logger.info("NEW session created: %s...", session_id[:8])
        else:
            # Check if session file exists
            session_file = self.session_dir / session_id
            if not session_file.exists():
                # Session cookie exists but file is missing - create new session
                session_id = self._generate_session_id()
                is_new_session = True
                session_data = {}
                # Create empty session file immediately
                self._save_session(session_id, session_data)
                logger.info("Old session file not found, created NEW session: %s...", session_id[:8])
            else:
                # Load existing session (served from in-memory cache after first hit)
                session_data = self._load_session(session_id)
                pd = session_data.get('project_data', {})
                ontology_classes = len(pd.get('ontology', {}).get('classes', []))
                mapping_entities = len(pd.get('assignment', {}).get('entities', []))
                mapping_rels = len(pd.get('assignment', {}).get('relationships', []))
                logger.debug(
                    "Session %s...: %d classes, %d entity mappings, %d rel mappings",
                    session_id[:8], ontology_classes, mapping_entities, mapping_rels
                )
        
        # Attach session to request state
        request.state.session = session_data
        request.state.session_id = session_id
        request.state.session_modified = False
        
        # Process request
        response = await call_next(request)
        
        # ONLY save session if it was explicitly modified
        # This prevents race conditions where concurrent requests overwrite each other
        if hasattr(request.state, 'session_modified') and request.state.session_modified:
            pd = request.state.session.get('project_data', {})
            ontology_classes = len(pd.get('ontology', {}).get('classes', []))
            mapping_entities = len(pd.get('assignment', {}).get('entities', []))
            mapping_rels = len(pd.get('assignment', {}).get('relationships', []))
            logger.info(
                "SAVING session %s... with %d ontology classes, %d entity mappings, %d rel mappings (modified=True)",
                session_id[:8], ontology_classes, mapping_entities, mapping_rels
            )
            self._save_session(session_id, request.state.session)
        
        # ALWAYS set session cookie to ensure browser has it
        # Note: httponly=False allows JavaScript to access the cookie if needed
        # samesite="lax" allows cookies to be sent with top-level navigations
        response.set_cookie(
            key=self.session_cookie,
            value=session_id,
            max_age=self.max_age,
            path="/",  # Ensure cookie is sent for all paths
            httponly=False,  # Allow JS to see the cookie for debugging
            samesite="lax",  # Allow GET requests from external sites
            secure=False,  # Allow HTTP (not just HTTPS)
            domain=None  # Let browser determine domain
        )
        
        return response


def get_session(request: Request) -> Dict[str, Any]:
    """Dependency that returns the session dict from ``request.state``."""
    return getattr(request.state, 'session', {})


def save_to_session(request: Request, key: str, value: Any):
    """Persist *value* under *key* in the request session and mark it modified."""
    if hasattr(request.state, 'session'):
        request.state.session[key] = value
        request.state.session_modified = True


def get_from_session(request: Request, key: str, default: Any = None) -> Any:
    """Return *key* from the request session, or *default* if missing."""
    if hasattr(request.state, 'session'):
        return request.state.session.get(key, default)
    return default
