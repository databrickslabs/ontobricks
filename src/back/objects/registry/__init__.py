"""Registry — domain registry, permissions, and scheduled builds."""

from back.objects.registry.RegistryService import (
    RegistryCfg,
    RegistryService,
)
from back.objects.registry.PermissionService import (
    PermissionService,
    permission_service,
    ROLE_ADMIN,
    ROLE_BUILDER,
    ROLE_EDITOR,
    ROLE_VIEWER,
    ROLE_APP_USER,
    ROLE_NONE,
    ROLE_HIERARCHY,
    ASSIGNABLE_ROLES,
    role_level,
    min_role,
)
from back.objects.registry.guards import require
from back.objects.registry.registry_cache import (
    invalidate_registry_cache,
    get_registry_cache_snapshot,
    get_registry_cache_ttl,
    set_registry_cache_ttl,
)
from back.objects.registry import obx_format
from back.objects.registry.obx_format import CURRENT_OBX_FORMAT_VERSION

__all__ = [
    "RegistryCfg",
    "RegistryService",
    "ReviewService",
    "PermissionService",
    "permission_service",
    "ROLE_ADMIN",
    "ROLE_BUILDER",
    "ROLE_EDITOR",
    "ROLE_VIEWER",
    "ROLE_APP_USER",
    "ROLE_NONE",
    "ROLE_HIERARCHY",
    "ASSIGNABLE_ROLES",
    "role_level",
    "min_role",
    "require",
    "BuildScheduler",
    "get_scheduler",
    "invalidate_registry_cache",
    "get_registry_cache_snapshot",
    "get_registry_cache_ttl",
    "set_registry_cache_ttl",
    "obx_format",
    "CURRENT_OBX_FORMAT_VERSION",
]


def __getattr__(name: str):
    """Lazy-import scheduler (APScheduler) so tests and minimal envs can import RegistryCfg.

    ``ReviewService`` is also lazy: it imports ``back.objects.session`` at
    module load, and the ``session`` package imports
    ``back.objects.registry.registry_cache`` during its own init — so
    importing it eagerly here would create a fragile import cycle.
    """
    if name == "ReviewService":
        from back.objects.registry.ReviewService import (
            ReviewService as _ReviewService,
        )

        globals()["ReviewService"] = _ReviewService
        return _ReviewService
    if name == "BuildScheduler":
        from back.objects.registry.scheduler import BuildScheduler as _BuildScheduler

        globals()["BuildScheduler"] = _BuildScheduler
        return _BuildScheduler
    if name == "get_scheduler":
        from back.objects.registry.scheduler import get_scheduler as _get_scheduler

        globals()["get_scheduler"] = _get_scheduler
        return _get_scheduler
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
