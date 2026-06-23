"""Frontend HTML routes -- Home, About, Settings page, Access Denied."""

from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse

from front.fastapi.dependencies import templates
from back.core.graphdb.neo4j.Neo4jStore import is_neo4j_password_from_secret
from back.core.logging import get_logger
from back.objects.domain.SettingsService import SettingsService
from back.objects.session import SessionManager, get_session_manager
from shared.config.constants import APP_VERSION
from shared.config.settings import Settings, get_settings

logger = get_logger(__name__)

router = APIRouter(tags=["Home"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def home_page(request: Request):
    """Home page."""
    return templates.TemplateResponse(
        request, "home.html", {"app_version": APP_VERSION}
    )


@router.get("/about", response_class=HTMLResponse, include_in_schema=False)
async def about_page(request: Request):
    """About page."""
    return templates.TemplateResponse(request, "about.html")


@router.get("/settings", response_class=HTMLResponse, include_in_schema=False)
async def settings_page(
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    """Settings page.

    Resolves the persisted graph engine server-side so the engine selector is
    rendered with the correct ``selected`` option on first paint — avoids the
    "Lakebase flashes before Neo4j loads" flicker flagged in Benoit's PR #47
    review (2026-06-18). Failure to load is non-fatal: the selector falls back
    to the HTML default and the existing JS reconciles on the lazy-load.
    """
    user_role = getattr(request.state, "user_role", "admin")
    graph_engine = "lakebase"
    try:
        result = SettingsService.get_graph_engine_result(session_mgr, settings)
        if result.get("success"):
            graph_engine = str(result.get("graph_engine") or "lakebase")
    except Exception as exc:  # noqa: BLE001 — degrade gracefully on Settings render
        logger.warning(
            "settings_page: graph engine resolution failed, falling back to default: %s",
            exc,
        )
    return templates.TemplateResponse(
        request,
        "settings.html",
        {
            "user_role": user_role,
            "neo4j_password_from_secret": is_neo4j_password_from_secret(),
            "graph_engine": graph_engine,
        },
    )


@router.get("/access-denied", response_class=HTMLResponse, include_in_schema=False)
async def access_denied_page(request: Request):
    """Access denied page shown when user has no permission.

    Accepts a ``reason`` query parameter that tailors the wording:

    - ``app`` (default) — user has no Databricks App permission; point them
      at their Databricks admin.
    - ``domain`` — user is signed in but lacks a team entry on the current
      domain; point them at the OntoBricks admin (Registry → Teams).
    - ``bootstrap`` — first-deploy chicken-and-egg: the app's service
      principal cannot read its own ACL yet.  Instruct the deployer to run
      ``scripts/bootstrap-app-permissions.sh`` (or ``make bootstrap-perms``).
    """
    email = getattr(request.state, "user_email", "") or request.headers.get(
        "x-forwarded-email", ""
    )
    reason = request.query_params.get("reason", "app")
    if reason not in {"app", "domain", "bootstrap"}:
        reason = "app"
    user_role = getattr(request.state, "user_role", "")
    user_domain_role = getattr(request.state, "user_domain_role", "")
    return templates.TemplateResponse(
        request,
        "access_denied.html",
        {
            "user_email": email,
            "reason": reason,
            "user_role": user_role,
            "user_domain_role": user_domain_role,
        },
    )
