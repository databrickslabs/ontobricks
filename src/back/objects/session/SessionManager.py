"""Session manager — request-scoped accessor for session data.

Wraps the raw ``request.state.session`` dictionary with ``get`` /
``set`` / ``delete`` / ``clear`` methods and automatically marks the
session as modified so the middleware persists it on response.
"""

from typing import Dict, Any

from fastapi import Request

from back.core.logging import get_logger

logger = get_logger(__name__)


class SessionManager:
    """Session manager dependency for modifying session data.

    Inject with ``SessionManager = Depends(get_session_manager)``, then call
    ``set``, ``get``, ``delete``, or ``clear`` and ``save`` when done.
    """

    def __init__(self, request: Request):
        self.request = request
        self._session = getattr(request.state, "session", {})

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from session."""
        return self._session.get(key, default)

    def set(self, key: str, value: Any):
        """Set a value in session."""
        self._session[key] = value
        self._mark_modified()

    def delete(self, key: str):
        """Delete a key from session."""
        if key in self._session:
            del self._session[key]
            self._mark_modified()

    def clear(self):
        """Clear all session data."""
        self._session.clear()
        self._mark_modified()

    def _mark_modified(self):
        """Mark session as modified for saving."""
        self.request.state.session_modified = True

    @property
    def data(self) -> Dict[str, Any]:
        """Get the raw session dictionary."""
        return self._session


def get_session_manager(request: Request) -> SessionManager:
    """Dependency to get session manager."""
    return SessionManager(request)
