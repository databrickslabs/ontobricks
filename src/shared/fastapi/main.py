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

**Graph Viewer Builder for Databricks**

OntoBricks enables you to build knowledge graphs from Databricks tables using ontologies 
and R2RML mappings. Design an ontology, map it to your data, and materialize triples 
into a Delta triple store for visual exploration and quality validation.

## Features

- 🏗️ **Ontology Design** - Visual ontology editor with OWL export
- 🔗 **Data Mapping** - Map ontology concepts to Databricks tables
- 🔍 **SPARQL Queries** - Query data using W3C standard SPARQL
- 📊 **Knowledge Graph Graph Viewer** - Interactive sigma.js WebGL graph exploration with SPARQL-based quality checks
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
    "/static/",
    "/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/access-denied",
    "/settings/permissions/me",
    "/settings/permissions/diag",
    "/api/",
    "/graphql/",
)

_VIEWER_BLOCKED_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

_PERM_ADMIN_ONLY_PREFIXES = (
    "/settings",
)

# Endpoints under an admin-only prefix that non-admins must still be
# able to reach (read-only status endpoints consumed by the regular
# app flow, e.g. checking warehouse / registry configuration before
# opening the Load Domain dialog, or listing domains for the
# Registry Browse page). Map is exact-path → allowed HTTP methods,
# so write variants at the same path (e.g. ``POST /settings/registry``
# to change the registry location) remain admin-only, and so do
# deletes on sub-paths like ``/settings/registry/domains/<name>``.
_PERM_ADMIN_ONLY_EXCEPTIONS = {
    "/settings/current": {"GET"},
    "/settings/registry": {"GET"},
    "/settings/registry/domains": {"GET"},
    "/settings/registry/bridges": {"GET"},
    # Graph DB engine is global; non-admins need read access so the Settings UI
    # (and tab refresh) match persisted state. Writes remain POST + admin-only.
    "/settings/graph-engine": {"GET"},
    "/settings/graph-engine-config": {"GET"},
    "/settings/graph-engine/lakebase-health": {"GET"},
    "/settings/graph-engine/uc-catalogs": {"GET"},
}

# Routes that operate on a specific domain (the session's current domain).
# Non-admin users need a team entry on that domain (user_domain_role != NONE)
# to access these.
_DOMAIN_SCOPED_PREFIXES = (
    "/domain/",
    "/ontology/",
    "/mapping/",
    "/dtwin/",
)

# Routes that live under a domain-scoped prefix but enumerate
# registry-level data (all domains / all versions). They must not be
# gated by the *current* session domain's role, otherwise a user who
# happens to land on a domain they are not a member of cannot even
# reach the "Load Domain from Registry" picker.
_DOMAIN_SCOPED_EXCEPTIONS = (
    "/domain/list-projects",
    "/domain/list-versions",
    "/domain/load-from-uc",
    # Authorization is resolved against the *target* domain inside the
    # handler (a Builder may manage status for a domain they have not
    # loaded), so it must not be gated by the *session* domain's role.
    "/domain/set-version-status",
    # Closing only releases the caller's own edit lock and resets their
    # session — any user with app access (viewers included) may close the
    # domain they have open, so it is not gated by the session domain role.
    "/domain/close",
)

# ----------------------------------------------------------------------
# Lifecycle status edit-gate
# ----------------------------------------------------------------------
# A domain version's *design* is only editable while its lifecycle status
# is ``DRAFT``. Once it moves to ``IN-REVIEW`` or ``PUBLISHED`` the
# design-mutating endpoints below are blocked server-side (authoritative)
# for *all* roles. Read / query / generate / validate endpoints stay open
# so a locked version can still be browsed, queried and inspected.
#
# Data-refresh ops (KG build/sync, reasoning materialise) are NOT gated
# on the lifecycle status: they re-materialise triples from source data
# using the frozen design but do not change the design itself. A PUBLISHED
# domain's graph can therefore be refreshed without reverting to DRAFT.

# Write methods on these prefixes are pure model-editing surfaces.
_STATUS_GATE_EDIT_PREFIXES = (
    "/ontology/",
    "/mapping/",
)

# Specific mutating endpoints outside the editing prefixes (domain
# metadata/layout saves, document uploads) that persist into the loaded
# version's *design*.
# NOTE: KG data-refresh ops (/dtwin/sync/start, /dtwin/sync/load,
# /dtwin/reasoning/materialize) are intentionally NOT listed here — they
# re-materialise triples from source data using the frozen design but do
# not mutate the design itself. They must remain usable on PUBLISHED /
# IN-REVIEW versions so the graph can be refreshed without reverting to
# DRAFT. They remain builder-gated via their endpoint role decorators.
_STATUS_GATE_EDIT_PATHS = (
    "/domain/save",
    "/domain/info",
    "/domain/import",
    "/domain/reset",
    "/domain/clear",
    "/domain/config",
    "/domain/map-layout",
    "/domain/save-to-uc",
    "/domain/design-views/create",
    "/domain/design-views/rename",
    "/domain/design-views/delete",
    "/domain/metadata/",
    "/domain/documents/",
)

# Non-mutating POST endpoints that live under an editing prefix but only
# compute / validate / generate suggestions (no persistence). These stay
# open regardless of status so a locked version remains fully inspectable.
_STATUS_GATE_OPEN_SUFFIXES = (
    "/generate",
    "/generate-owl",
    "/generate-async",
    "/generate-sql",
    "/parse-owl",
    "/parse-rdfs",
    "/parse-r2rml",
    "/load-owl-file",
    "/analyze-import",
    "/validate",
    "/validate-sql",
    "/test-query",
    "/tables",
    "/table-columns",
    "/schema-context",
    "/analyze",
    "/chat",
    "/invoke",
    "/probe-write",
)

# Status values that allow editing the version content.
_STATUS_EDITABLE = "DRAFT"


def _is_status_gated_edit(path: str, method: str) -> bool:
    """True when *path*/*method* mutates the loaded version's content."""
    if method not in _VIEWER_BLOCKED_METHODS:
        return False
    if any(path.endswith(suffix) for suffix in _STATUS_GATE_OPEN_SUFFIXES):
        return False
    if "/wizard/" in path:
        return False
    if any(path.startswith(p) for p in _STATUS_GATE_EDIT_PREFIXES):
        return True
    if any(
        path == p or path.startswith(p) for p in _STATUS_GATE_EDIT_PATHS
    ):
        return True
    return False


class PermissionMiddleware(BaseHTTPMiddleware):
    """Enforce app- and domain-level access rules.

    New model:

    - **App access** requires the caller to be either an admin
      (CAN_MANAGE on the Databricks App) or an app user (present in the
      App's ACL, directly or via group).  Users with ``ROLE_NONE`` are
      redirected to ``/access-denied``.
    - **Domain access** (routes under ``/domain``, ``/ontology``,
      ``/mapping``, ``/dtwin``) additionally requires a non-empty entry
      in the domain's ``.domain_permissions.json``.  Admins bypass this.
    - **Viewer write-block**: a viewer team entry allows GETs on domain
      routes but not writes.
    - **Admin-only prefix** (`/settings`) is gated to admins even for
      app users. A small allow-list (see
      :data:`_PERM_ADMIN_ONLY_EXCEPTIONS`) keeps the read-only status
      endpoints that the regular app flow needs (warehouse/registry
      config) open to non-admins.

    Only active when running as a Databricks App (``DATABRICKS_APP_PORT``
    is set).  In local-dev mode every request passes through as admin.

    Sets ``request.state.user_role`` (app-level) and
    ``request.state.user_domain_role`` (effective role for the loaded domain).
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        from back.core.databricks import is_databricks_app
        from back.objects.registry import (
            ROLE_NONE,
            ROLE_VIEWER,
            ROLE_ADMIN,
            permission_service,
        )

        email = request.headers.get("x-forwarded-email", "")
        request.state.user_email = email

        if not is_databricks_app():
            # Local / PAT dev has no proxy identity header, so resolve the
            # developer's e-mail once via SCIM /Me. Without this, audit
            # attribution (review sign-offs, status changes) records an
            # empty actor and sign-off counts stay stuck at 0/N.
            if not email:
                from back.core.databricks import get_local_user_email

                request.state.user_email = get_local_user_email()
            request.state.user_role = "admin"
            request.state.user_domain_role = "admin"
            return await call_next(request)

        # Raw routed path, not request.url.path: the latter is reconstructed
        # from the Host header and could be poisoned (BadHost / CVE-2026-48710).
        path = request.scope["path"]

        if any(path.startswith(p) for p in _PERM_BYPASS_PREFIXES):
            request.state.user_role = ""
            request.state.user_domain_role = ""
            return await call_next(request)

        try:
            role, domain_role = self._resolve_roles(request, email)
        except Exception as exc:
            logger.error(
                "PermissionMiddleware: error resolving role for %s on %s: %s",
                email,
                path,
                exc,
                exc_info=True,
            )
            role = ROLE_NONE
            domain_role = ROLE_NONE

        request.state.user_role = role
        request.state.user_domain_role = domain_role
        logger.info(
            "PermissionMiddleware: %s %s email=%s → role=%s domain_role=%s",
            request.method,
            path,
            email,
            role,
            domain_role,
        )

        if role == ROLE_NONE:
            # First-deploy bootstrap: the app's service principal is not
            # allowed to read its own ACL, so *nobody* — not even CAN_MANAGE
            # users — can be resolved as admin/app-user.  Surface that as a
            # distinct reason so the access-denied page shows the fix.
            reason = "app"
            if permission_service.is_app_principals_forbidden():
                reason = "bootstrap"
                logger.warning(
                    "PermissionMiddleware: first-deploy bootstrap detected "
                    "(app SP cannot read its own ACL). Run "
                    "scripts/bootstrap-app-permissions.sh to fix."
                )
            if self._wants_json(request):
                return self._forbidden_json(request, "Access denied")
            return RedirectResponse(
                f"/access-denied?reason={reason}", status_code=302
            )

        if role != ROLE_ADMIN:
            is_domain_scoped = any(
                path.startswith(p) for p in _DOMAIN_SCOPED_PREFIXES
            ) and path not in _DOMAIN_SCOPED_EXCEPTIONS
            if is_domain_scoped and domain_role == ROLE_NONE:
                if self._wants_json(request):
                    return self._forbidden_json(
                        request,
                        "You are not a member of this domain's team",
                    )
                return RedirectResponse(
                    "/access-denied?reason=domain", status_code=302
                )

            if (
                is_domain_scoped
                and domain_role == ROLE_VIEWER
                and request.method in _VIEWER_BLOCKED_METHODS
            ):
                return self._forbidden_json(
                    request, "Viewer role does not allow write operations"
                )

            allowed_methods = _PERM_ADMIN_ONLY_EXCEPTIONS.get(path)
            is_admin_only = any(
                path.startswith(p) for p in _PERM_ADMIN_ONLY_PREFIXES
            ) and not (
                allowed_methods is not None and request.method in allowed_methods
            )
            if is_admin_only:
                if self._wants_json(request):
                    return self._forbidden_json(
                        request, "Only administrators can access settings"
                    )
                return RedirectResponse("/", status_code=302)

        # Lifecycle status edit-gate (applies to all roles, admins
        # included): block mutating edits unless the loaded version is
        # DRAFT. Locked versions must be set back to DRAFT to edit.
        if _is_status_gated_edit(path, request.method):
            status = self._session_version_status(request)
            if status and status != _STATUS_EDITABLE:
                return self._forbidden_json(
                    request,
                    f"This version is {status}; set it back to DRAFT to edit.",
                )
            # Single-editor lock (authoritative): on an editable (DRAFT)
            # version only the lock holder may mutate. Admins are NOT
            # auto-exempt — they must "take over" the lock first, which
            # preserves the single-writer invariant.
            holder = self._session_edit_lock_holder(request)
            if holder:
                return self._forbidden_json(
                    request,
                    f"This version is being edited by {holder}; you have "
                    "read-only access. Take over editing to make changes.",
                )

        return await call_next(request)

    @staticmethod
    def _session_edit_lock_holder(request: Request) -> str:
        """Name of *another* user editing the session's loaded version.

        Returns ``""`` when the request must not be blocked (lock free /
        held by this user / backend unavailable). Only invoked on the
        already-gated mutating edit routes, so it adds no per-request
        Lakebase cost to reads.
        """
        try:
            from back.objects.session import SessionManager
            from back.objects.registry.lockmgt import EditLockService

            session_mgr = SessionManager(request)
            return EditLockService.blocking_holder(
                request, session_mgr, get_settings()
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("could not resolve edit-lock holder: %s", exc)
            return ""

    @staticmethod
    def _session_version_status(request: Request) -> str:
        """Lifecycle status of the request's session-loaded domain version.

        Returns ``"DRAFT"`` (editable) when no domain is loaded or the
        status cannot be resolved, so a transient error never blocks an
        edit on a legitimately editable version.
        """
        try:
            from back.objects.session import get_domain, SessionManager

            session_mgr = SessionManager(request)
            domain = get_domain(session_mgr)
            return (domain.info or {}).get("status", "DRAFT") or "DRAFT"
        except Exception as exc:  # noqa: BLE001
            logger.debug("could not resolve session version status: %s", exc)
            return "DRAFT"

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
    def _resolve_roles(request: Request, email: str) -> tuple:
        """Return ``(app_role, domain_role)``."""
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

        app_role = permission_service.get_user_role(
            email,
            host,
            token,
            registry_cfg,
            settings.ontobricks_app_name,
            user_token=user_token,
        )

        domain_folder = getattr(domain, "domain_folder", "") or ""
        domain_role = permission_service.get_domain_role(
            email,
            host,
            token,
            registry_cfg,
            settings.ontobricks_app_name,
            domain_folder,
            user_token=user_token,
            app_role=app_role,
        )

        return app_role, domain_role


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
        allow_origins=["http://127.0.0.1:8000", "http://localhost:8000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Request duration logging (outermost — wraps everything)
    from shared.fastapi.timing import RequestTimingMiddleware

    app.add_middleware(RequestTimingMiddleware)

    # CSRF protection
    from shared.fastapi.csrf import CSRFMiddleware

    app.add_middleware(CSRFMiddleware)

    # Permission enforcement (runs after session is available)
    app.add_middleware(PermissionMiddleware)

    # Custom file-based session middleware
    is_app = bool(os.getenv("DATABRICKS_APP_PORT"))
    app.add_middleware(
        FileSessionMiddleware,
        secret_key=settings.secret_key,
        session_dir=settings.session_dir,
        session_cookie=SESSION_COOKIE_NAME,
        max_age=settings.session_max_age,
        same_site="lax",
        https_only=is_app,
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
