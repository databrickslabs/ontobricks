"""Uniform error response model.

The ``ErrorResponse`` Pydantic model is framework-agnostic and lives in
``back.core``.  The FastAPI exception-handler registration has been moved
to :func:`shared.fastapi.error_handlers.register_exception_handlers` so
that ``back.core`` does not depend on FastAPI types.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    """JSON body returned for every error handled by the global handler."""

    error: str = Field(..., description="Machine-readable error code, e.g. 'not_found'")
    message: str = Field(..., description="Human-readable summary (safe for clients)")
    detail: Optional[str] = Field(
        None, description="Extra context (omitted in production)"
    )
    request_id: Optional[str] = Field(None, description="Correlation ID for log lookup")
