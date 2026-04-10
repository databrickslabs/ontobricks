"""Uniform error response model and exception handler registration."""
from __future__ import annotations

import uuid
from typing import Optional

from pydantic import BaseModel, Field

from back.core.logging import get_logger

logger = get_logger(__name__)


class ErrorResponse(BaseModel):
    """JSON body returned for every error handled by the global handler."""

    error: str = Field(..., description="Machine-readable error code, e.g. 'not_found'")
    message: str = Field(..., description="Human-readable summary (safe for clients)")
    detail: Optional[str] = Field(None, description="Extra context (omitted in production)")
    request_id: Optional[str] = Field(None, description="Correlation ID for log lookup")

    @staticmethod
    def register_exception_handlers(app) -> None:
        """Register ``OntoBricksError`` and catch-all ``Exception`` handlers on *app*.

        Call this from :func:`back.fastapi.main.create_app` after the app is created.
        """
        from fastapi import Request
        from fastapi.responses import JSONResponse
        from back.core.errors.OntoBricksError import OntoBricksError

        def _build_response(
            request: Request,
            status_code: int,
            error_code: str,
            message: str,
            detail: Optional[str],
        ) -> JSONResponse:
            from shared.config.settings import get_settings

            request_id = request.headers.get("x-request-id") or str(uuid.uuid4())

            show_detail = False
            try:
                show_detail = get_settings().debug
            except Exception:
                pass

            body = ErrorResponse(
                error=error_code,
                message=message,
                detail=detail if show_detail else None,
                request_id=request_id,
            )
            return JSONResponse(
                status_code=status_code,
                content=body.model_dump(exclude_none=True),
            )

        @app.exception_handler(OntoBricksError)
        async def _handle_ontobricks_error(request: Request, exc: OntoBricksError):
            logger.warning(
                "%s [%d]: %s (detail=%s)",
                type(exc).__name__, exc.status_code, exc.message, exc.detail,
            )
            return _build_response(
                request,
                status_code=exc.status_code,
                error_code=OntoBricksError.error_code_from_class(type(exc)),
                message=exc.message,
                detail=exc.detail,
            )

        @app.exception_handler(Exception)
        async def _handle_unexpected_error(request: Request, exc: Exception):
            logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
            return _build_response(
                request,
                status_code=500,
                error_code="internal_error",
                message="An internal error occurred",
                detail=str(exc),
            )
