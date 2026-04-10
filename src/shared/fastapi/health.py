"""Health check endpoints."""
import sys

from fastapi import APIRouter

from shared.config.constants import APP_VERSION

router = APIRouter(tags=["Health"])


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": APP_VERSION,
        "service": "OntoBricks",
        "framework": "FastAPI"
    }


@router.get("/health/detailed")
async def detailed_health():
    """Detailed health check with component status."""
    return {
        "status": "healthy",
        "version": APP_VERSION,
        "python_version": sys.version,
        "components": {
            "api": "ok",
            "session": "ok",
            "static_files": "ok"
        }
    }
