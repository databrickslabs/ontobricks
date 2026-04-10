"""Centralized error handling for OntoBricks.

Provides a custom exception hierarchy and a uniform error response model.
"""
from back.core.errors.OntoBricksError import OntoBricksError  # noqa: F401
from back.core.errors.NotFoundError import NotFoundError  # noqa: F401
from back.core.errors.ValidationError import ValidationError  # noqa: F401
from back.core.errors.AuthorizationError import AuthorizationError  # noqa: F401
from back.core.errors.InfrastructureError import InfrastructureError  # noqa: F401
from back.core.errors.ConflictError import ConflictError  # noqa: F401
from back.core.errors.ErrorResponse import ErrorResponse  # noqa: F401

# Backward-compatible wrappers
_error_code_from_class = OntoBricksError.error_code_from_class
register_exception_handlers = ErrorResponse.register_exception_handlers

__all__ = [
    "OntoBricksError",
    "NotFoundError",
    "ValidationError",
    "AuthorizationError",
    "InfrastructureError",
    "ConflictError",
    "ErrorResponse",
    "_error_code_from_class",
    "register_exception_handlers",
]
