"""Frontend HTML route -- Registry page."""
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse

from front.fastapi.dependencies import templates
from back.objects.session import SessionManager, get_session_manager

router = APIRouter(prefix="/registry", tags=["Registry"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def registry_page(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Registry management page -- browse projects, configure registry location."""
    user_role = getattr(request.state, "user_role", "admin")
    return templates.TemplateResponse(request, "registry.html", {"user_role": user_role})
