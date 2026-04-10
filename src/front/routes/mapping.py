"""Frontend HTML route -- Mapping page."""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from front.fastapi.dependencies import templates

router = APIRouter(prefix="/mapping", tags=["Mapping"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def mapping_page(request: Request):
    """Mapping management page."""
    return templates.TemplateResponse(request, "mapping.html")
