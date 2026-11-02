"""Centralized error handling for OntoBricks.

Provides a custom exception hierarchy. The uniform error response model
(pydantic) is :class:`back.core.errors.ErrorResponse.ErrorResponse`, imported
by the API layer only, so raising an error does not import pydantic.
"""

from back.core.errors.OntoBricksError import OntoBricksError  # noqa: F401
from back.core.errors.NotFoundError import NotFoundError  # noqa: F401
from back.core.errors.ValidationError import ValidationError  # noqa: F401
from back.core.errors.AuthorizationError import AuthorizationError  # noqa: F401
from back.core.errors.InfrastructureError import InfrastructureError  # noqa: F401
from back.core.errors.ConflictError import ConflictError  # noqa: F401
from back.core.errors.GoneError import GoneError  # noqa: F401
from back.core.errors.OperationCancelledError import (  # noqa: F401
    OperationCancelledError,
)

_error_code_from_class = OntoBricksError.error_code_from_class


def register_exception_handlers(app) -> None:
    """Backward-compatible wrapper — delegates to ``shared.fastapi.error_handlers``."""
    from shared.fastapi.error_handlers import register_exception_handlers as _reg

    _reg(app)


__all__ = [
    "OntoBricksError",
    "NotFoundError",
    "ValidationError",
    "AuthorizationError",
    "InfrastructureError",
    "ConflictError",
    "GoneError",
    "OperationCancelledError",
    "_error_code_from_class",
    "register_exception_handlers",
]
