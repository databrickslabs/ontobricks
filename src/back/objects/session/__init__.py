"""Centralized session management — middleware, managers, project state."""

from .middleware import (
    FileSessionMiddleware,
    get_session,
    save_to_session,
    get_from_session,
)
from .manager import SessionManager, get_session_manager
from .project_session import (
    ProjectSession,
    get_project,
    get_empty_project,
    sanitize_project_folder,
)
from .global_config import GlobalConfigService, global_config_service

__all__ = [
    "FileSessionMiddleware",
    "get_session",
    "save_to_session",
    "get_from_session",
    "SessionManager",
    "get_session_manager",
    "ProjectSession",
    "get_project",
    "get_empty_project",
    "sanitize_project_folder",
    "GlobalConfigService",
    "global_config_service",
]
