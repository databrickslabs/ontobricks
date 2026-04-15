"""
External (programmatic) REST API sub-application.

Mounted on the main OntoBricks app at :data:`api.constants.EXTERNAL_API_MOUNT_PREFIX`
so live URLs use :data:`api.constants.API_URL_PATH_VERSION` (e.g. ``/api/v1/...``).
Interactive docs live at ``/api/docs`` and ``/api/redoc``; OpenAPI JSON at ``/api/openapi.json``.

OpenAPI path keys are prefixed with :data:`api.constants.OPENAPI_PATH_PREFIX` so Swagger UI
resolves requests correctly when the schema is served from the mounted app.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from api.constants import (
    API_DIGITALTWIN_PREFIX,
    API_GRAPHQL_PREFIX,
    API_V1_PREFIX,
    API_VERSION,
    EXTERNAL_API_CONTACT,
    EXTERNAL_API_DESCRIPTION,
    EXTERNAL_API_LICENSE_INFO,
    EXTERNAL_API_TITLE,
    EXTERNAL_OPENAPI_TAGS,
    OPENAPI_PATH_PREFIX,
)
from back.core.errors import register_exception_handlers


def create_external_api_app() -> FastAPI:
    app = FastAPI(
        title=EXTERNAL_API_TITLE,
        description=EXTERNAL_API_DESCRIPTION,
        version=API_VERSION,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        openapi_tags=EXTERNAL_OPENAPI_TAGS,
        contact=EXTERNAL_API_CONTACT,
        license_info=EXTERNAL_API_LICENSE_INFO,
    )

    register_exception_handlers(app)

    from api.routers.v1 import router as api_v1_router
    from api.routers.digitaltwin import router as dt_api_router
    from api.routers.domains import router as domains_router
    from back.fastapi.graphql_routes import router as graphql_router

    app.include_router(api_v1_router, prefix=API_V1_PREFIX, tags=["API v1"])
    app.include_router(domains_router, prefix=API_V1_PREFIX, tags=["Domain"])
    app.include_router(dt_api_router, prefix=API_DIGITALTWIN_PREFIX, tags=["Digital Twin"])
    app.include_router(graphql_router, prefix=API_GRAPHQL_PREFIX, tags=["GraphQL"])

    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            description=app.description,
            routes=app.routes,
        )
        mount_prefix = OPENAPI_PATH_PREFIX
        raw_paths = openapi_schema.get("paths") or {}
        openapi_schema["paths"] = {
            (p if p.startswith(mount_prefix) else mount_prefix + p): spec
            for p, spec in raw_paths.items()
        }
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore[method-assign]

    return app
