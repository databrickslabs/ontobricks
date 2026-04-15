"""Internal API routers -- session-aware JSON endpoints called by the frontend JS.

Block B of the API layer. Block A (external /api/v1/*) lives in
api/routers/v1.py, domains.py, digitaltwin.py and is unchanged.
"""
from api.routers.internal.home import router as home_router
from api.routers.internal.settings import router as settings_router
from api.routers.internal.ontology import router as ontology_router
from api.routers.internal.mapping import router as mapping_router
from api.routers.internal.dtwin import router as dtwin_router
from api.routers.internal.domain import router as domain_router
from api.routers.internal.tasks import router as tasks_router

all_internal_routers = [
    home_router,
    settings_router,
    ontology_router,
    mapping_router,
    dtwin_router,
    domain_router,
    tasks_router,
]

__all__ = ["all_internal_routers"]
