"""Frontend HTML route -- Project page."""
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse

from front.fastapi.dependencies import templates
from back.objects.session import SessionManager, get_session_manager, get_project
from back.objects.project import Project

router = APIRouter(prefix="/project", tags=["Project"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def project_page(request: Request, session_mgr: SessionManager = Depends(get_session_manager)):
    """Project management page."""
    project = get_project(session_mgr)
    project_data = Project(project).get_project_template_data()
    return templates.TemplateResponse(request, "project.html", {"project": project_data})
