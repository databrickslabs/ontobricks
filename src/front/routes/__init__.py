"""Frontend HTML-only routers.

Each module serves a single HTML page via TemplateResponse.
All JSON/API endpoints live in api/routers/internal/.
"""
from front.routes.home import router as home_router
from front.routes.ontology import router as ontology_router
from front.routes.mapping import router as mapping_router
from front.routes.dtwin import router as dtwin_router
from front.routes.domain import router as domain_router
from front.routes.registry import router as registry_router
from front.routes.resolve import router as resolve_router

all_frontend_routers = [
    home_router,
    ontology_router,
    mapping_router,
    dtwin_router,
    domain_router,
    registry_router,
    resolve_router,
]

__all__ = ["all_frontend_routers"]
