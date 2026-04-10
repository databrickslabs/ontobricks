"""Frontend HTML route -- Ontology page."""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from front.fastapi.dependencies import templates

router = APIRouter(prefix="/ontology", tags=["Ontology"])


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def ontology_page(request: Request):
    """Ontology management page."""
    return templates.TemplateResponse(request, "ontology.html")
