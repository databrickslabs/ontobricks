"""OntoBricks shared configuration — settings and constants."""

from shared.config.settings import Settings, get_settings
from shared.config.constants import APP_VERSION, SESSION_COOKIE_NAME

__all__ = [
    "Settings",
    "get_settings",
    "APP_VERSION",
    "SESSION_COOKIE_NAME",
]
