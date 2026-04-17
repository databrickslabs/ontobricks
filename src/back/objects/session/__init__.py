"""Centralized session management — middleware, managers, domain state."""

from .middleware import (
    FileSessionMiddleware,
    get_session,
)
from .SessionManager import SessionManager, get_session_manager
from .DomainSession import (
    DomainSession,
    get_domain,
    get_empty_domain,
    sanitize_domain_folder,
)
from .GlobalConfigService import GlobalConfigService, global_config_service

__all__ = [
    "FileSessionMiddleware",
    "get_session",
    "SessionManager",
    "get_session_manager",
    "DomainSession",
    "get_domain",
    "get_empty_domain",
    "sanitize_domain_folder",
    "GlobalConfigService",
    "global_config_service",
]
