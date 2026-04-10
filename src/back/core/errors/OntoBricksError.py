"""Base application error for OntoBricks."""
from __future__ import annotations

import re
from typing import Optional


class OntoBricksError(Exception):
    """Base for all application errors.

    Attributes:
        message:     Human-readable summary safe for clients.
        status_code: HTTP status code the global handler will use.
        detail:      Optional extra context (stripped in production).
    """

    _CAMEL_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
    _SANITIZE_RE = re.compile(r"[^a-z0-9]+")

    def __init__(
        self,
        message: str = "An unexpected error occurred",
        *,
        status_code: int = 500,
        detail: Optional[str] = None,
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.detail = detail

    @classmethod
    def error_code_from_class(cls, exc_cls: type) -> str:
        """Derive a machine-readable error code from the exception class name.

        ``NotFoundError`` -> ``"not_found"``
        ``OntoBricksError`` -> ``"onto_bricks"``
        """
        name = exc_cls.__name__.removesuffix("Error").removesuffix("Exception")
        if not name:
            return "internal_error"
        snake = cls._CAMEL_RE.sub("_", name).lower()
        return cls._SANITIZE_RE.sub("_", snake).strip("_") or "internal_error"
