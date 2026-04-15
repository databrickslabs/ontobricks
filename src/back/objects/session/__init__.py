"""Centralized session management — middleware, managers, domain state."""

from .middleware import (
    FileSessionMiddleware,
    get_session,
    save_to_session,
    get_from_session,
)
from .manager import SessionManager, get_session_manager
from .domain_session import (
    DomainSession,
    get_domain,
    get_empty_domain,
    sanitize_domain_folder,
)
from .global_config import GlobalConfigService, global_config_service

__all__ = [
    "FileSessionMiddleware",
    "get_session",
    "save_to_session",
    "get_from_session",
    "SessionManager",
    "get_session_manager",
    "DomainSession",
    "get_domain",
    "get_empty_domain",
    "sanitize_domain_folder",
    "GlobalConfigService",
    "global_config_service",
]
