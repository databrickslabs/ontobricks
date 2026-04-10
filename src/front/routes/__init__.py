"""Frontend HTML-only routers.

Each module serves a single HTML page via TemplateResponse.
All JSON/API endpoints live in api/routers/internal/.
"""
from front.routes.home import router as home_router
from front.routes.ontology import router as ontology_router
from front.routes.mapping import router as mapping_router
from front.routes.dtwin import router as dtwin_router
from front.routes.project import router as project_router

all_frontend_routers = [
    home_router,
    ontology_router,
    mapping_router,
    dtwin_router,
    project_router,
]

__all__ = ["all_frontend_routers"]
