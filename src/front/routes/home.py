"""Frontend HTML routes -- Home, About, Settings page, Access Denied."""

from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse

from front.fastapi.dependencies import templates
from back.objects.session import SessionManager, get_session_manager
from shared.config.constants import APP_VERSION

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
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    """Settings page."""
    user_role = getattr(request.state, "user_role", "admin")
    return templates.TemplateResponse(
        request, "settings.html", {"user_role": user_role}
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
