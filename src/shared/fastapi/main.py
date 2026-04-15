"""
FastAPI Application Factory - OntoBricks

This is the main FastAPI application (UI, GraphQL, health).
Run with: uvicorn shared.fastapi.main:app --reload --port 8000
"""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from contextlib import asynccontextmanager
import os

from shared.config.settings import get_settings
from shared.config.constants import APP_VERSION, SESSION_COOKIE_NAME
from back.objects.session import FileSessionMiddleware
from back.core.logging import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


# OpenAPI Tags Metadata
tags_metadata = [
    {
        "name": "Health",
        "description": "Health check endpoints for monitoring and deployment verification.",
    },
    {
        "name": "Settings",
        "description": "Databricks connection configuration and application settings.",
    },
    {
        "name": "Ontology",
        "description": "Ontology management - Create and manage OWL ontologies with classes, "
                       "properties, relationships, constraints, and SWRL rules.",
    },
    {
        "name": "Mapping",
        "description": "Data source mapping - Map ontology entities to Databricks tables and columns. "
                       "Generate R2RML mappings for SPARQL-to-SQL translation.",
    },
    {
        "name": "Query",
        "description": "SPARQL query execution - Execute SPARQL queries against mapped data sources. "
                       "Queries are translated to SQL and executed on Databricks.",
    },
    {
        "name": "Domain",
        "description": "Domain management - Save, load, import/export complete domain configurations "
                       "including ontology, mappings, and settings.",
    },
    {
        "name": "GraphQL",
        "description": "**GraphQL API** - Auto-generated typed GraphQL schema from ontology. "
                       "Query the knowledge graph with nested traversal and introspection.",
    },
]

# API Description (Markdown)
API_DESCRIPTION = """
# OntoBricks API

**Knowledge Graph Builder for Databricks**

OntoBricks enables you to build knowledge graphs from Databricks tables using ontologies 
and R2RML mappings. Design an ontology, map it to your data, and materialize triples 
into a Delta triple store for visual exploration and quality validation.

## Features

- 🏗️ **Ontology Design** - Visual ontology editor with OWL export
- 🔗 **Data Mapping** - Map ontology concepts to Databricks tables
- 🔍 **SPARQL Queries** - Query data using W3C standard SPARQL
- 📊 **Digital Twin Knowledge Graph** - Interactive sigma.js WebGL graph exploration with SPARQL-based quality checks
- 📦 **Domain Management** - Save/load domains to Unity Catalog volumes
- 🔮 **GraphQL API** - Auto-generated typed schema from ontology with nested entity traversal

## Quick Start

1. Configure Databricks connection in **Settings**
2. Create or import an **Ontology**
3. **Map** ontology entities to your tables
4. Execute **SPARQL queries**

## External API

Programmatic REST endpoints live under `/api/v1/*`. **Swagger UI:** [`/api/docs`](/api/docs) · **OpenAPI JSON:** [`/api/openapi.json`](/api/openapi.json)

This page documents the **application** surface (UI JSON routes, GraphQL, settings). It is separate from the external contract.

---
*Built with FastAPI • Powered by Databricks*
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown events."""
    settings = get_settings()
    os.makedirs(settings.session_dir, exist_ok=True)
    logger.info("OntoBricks FastAPI starting — session_dir=%s", settings.session_dir)
    logger.info("App docs: /docs | External REST: /api/docs")

    from agents.tracing import setup_tracing
    setup_tracing()

    build_scheduler = None
    try:
        from back.objects.registry import get_scheduler
        build_scheduler = get_scheduler()
        build_scheduler.start(settings)
    except Exception as e:
        logger.warning("Could not start build scheduler: %s", e)

    yield

    if build_scheduler is not None:
        build_scheduler.stop()
    logger.info("OntoBricks FastAPI shutting down")


_PERM_BYPASS_PREFIXES = (
    "/static/", "/health", "/docs", "/redoc", "/openapi.json",
    "/access-denied", "/settings/permissions/me", "/settings/permissions/diag",
    "/api/", "/graphql/",
)

_VIEWER_BLOCKED_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

_PERM_ADMIN_ONLY_PREFIXES = ("/settings/permissions",)


class PermissionMiddleware(BaseHTTPMiddleware):
    """Enforce Viewer / Editor / Admin access rules.

    Only active when running as a Databricks App (``DATABRICKS_APP_PORT``
    is set).  In local-dev mode every request passes through.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        from back.core.databricks import is_databricks_app
        from back.objects.registry import ROLE_NONE, ROLE_VIEWER

        email = request.headers.get("x-forwarded-email", "")
        request.state.user_email = email

        if not is_databricks_app():
            request.state.user_role = "admin"
            return await call_next(request)

        path = request.url.path

        if any(path.startswith(p) for p in _PERM_BYPASS_PREFIXES):
            request.state.user_role = ""
            return await call_next(request)

        try:
            role = self._resolve_role(request, email)
        except Exception as exc:
            logger.error(
                "PermissionMiddleware: error resolving role for %s on %s: %s",
                email, path, exc, exc_info=True,
            )
            role = ROLE_NONE

        request.state.user_role = role
        logger.info(
            "PermissionMiddleware: %s %s email=%s → role=%s",
            request.method, path, email, role,
        )

        if role == ROLE_NONE:
            if self._wants_json(request):
                return self._forbidden_json(request, "Access denied")
            return RedirectResponse("/access-denied", status_code=302)

        if role == ROLE_VIEWER and request.method in _VIEWER_BLOCKED_METHODS:
            return self._forbidden_json(request, "Viewer role does not allow write operations")

        if request.state.user_role != "admin":
            for prefix in _PERM_ADMIN_ONLY_PREFIXES:
                if path.startswith(prefix):
                    if self._wants_json(request):
                        return self._forbidden_json(request, "Only administrators can access settings")
                    return RedirectResponse("/", status_code=302)

        return await call_next(request)

    @staticmethod
    def _forbidden_json(request: Request, message: str) -> JSONResponse:
        """Return a 403 response matching the standard ErrorResponse shape."""
        import uuid as _uuid
        request_id = request.headers.get("x-request-id") or str(_uuid.uuid4())
        return JSONResponse(
            {"error": "authorization", "message": message, "request_id": request_id},
            status_code=403,
        )

    @staticmethod
    def _wants_json(request: Request) -> bool:
        """True when the client expects JSON (fetch / XHR), not a page."""
        accept = request.headers.get("accept", "")
        if "application/json" in accept:
            return True
        if request.headers.get("x-requested-with", "").lower() == "xmlhttprequest":
            return True
        if request.headers.get("sec-fetch-dest", "") in ("empty", ""):
            sec_mode = request.headers.get("sec-fetch-mode", "")
            if sec_mode in ("cors", "no-cors", "same-origin"):
                return True
        return False

    @staticmethod
    def _resolve_role(request: Request, email: str) -> str:
        settings = get_settings()
        from back.objects.registry import permission_service
        from back.core.helpers import get_databricks_host_and_token
        from back.objects.session import get_domain, SessionManager

        session_mgr = SessionManager(request)
        domain = get_domain(session_mgr)
        host, token = get_databricks_host_and_token(domain, settings)
        user_token = request.headers.get("x-forwarded-access-token", "")

        from back.objects.registry import RegistryCfg
        registry_cfg = RegistryCfg.from_domain(domain, settings).as_dict()

        return permission_service.get_user_role(
            email, host, token, registry_cfg, settings.ontobricks_app_name,
            user_token=user_token,
        )


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="OntoBricks",
        description=API_DESCRIPTION,
        version=APP_VERSION,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        openapi_tags=tags_metadata,
        contact={
            "name": "OntoBricks Support",
            "url": "https://github.com/databricks/ontobricks",
        },
        license_info={
            "name": "Apache 2.0",
            "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
        },
        lifespan=lifespan,
    )

    # CORS middleware - allow credentials (cookies)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:8000", "http://localhost:8000", "*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Permission enforcement (runs after session is available)
    app.add_middleware(PermissionMiddleware)

    # Custom file-based session middleware
    app.add_middleware(
        FileSessionMiddleware,
        secret_key=settings.secret_key,
        session_dir=settings.session_dir,
        session_cookie=SESSION_COOKIE_NAME,
        max_age=settings.session_max_age,
        same_site="lax",
        https_only=False  # Set to True in production
    )

    # Static files -- served from front/static/
    _src_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    static_dir = os.path.join(_src_dir, "front", "static")
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    _register_routers(app)

    from api.constants import EXTERNAL_API_MOUNT_PREFIX
    from api.external_app import create_external_api_app

    app.mount(EXTERNAL_API_MOUNT_PREFIX, create_external_api_app())

    from back.core.errors import register_exception_handlers
    register_exception_handlers(app)

    return app


def _register_routers(app: FastAPI):
    """Register all routers: health, frontend HTML, internal API, GraphQL."""

    # --- Health ---
    from shared.fastapi.health import router as health_router
    app.include_router(health_router)

    # --- Frontend HTML routes (from src/front/) ---
    from front.routes import all_frontend_routers
    for router in all_frontend_routers:
        app.include_router(router)

    # --- Internal API (session-aware JSON, from src/api/routers/internal/) ---
    from api.routers.internal import all_internal_routers
    for router in all_internal_routers:
        app.include_router(router)

    # --- GraphQL ---
    from back.fastapi.graphql_routes import router as graphql_router
    app.include_router(graphql_router, prefix="/graphql", tags=["GraphQL"])


# Create the app instance
app = create_app()
