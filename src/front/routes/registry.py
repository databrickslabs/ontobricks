"""Frontend HTML route -- Registry (legacy URL → Home + modal)."""

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

router = APIRouter(prefix="/registry", tags=["Registry"])


@router.get("/", include_in_schema=False)
async def registry_page(request: Request):
    """Legacy /registry/ bookmarks open the Registry modal on Home."""
    return RedirectResponse(url="/?open=registry", status_code=302)
