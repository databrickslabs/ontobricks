"""Registry — domain registry, permissions, and scheduled builds."""
from back.objects.registry.service import (
    RegistryCfg,
    RegistryService,
)
from back.objects.registry.permissions import (
    PermissionService,
    permission_service,
    ROLE_ADMIN,
    ROLE_EDITOR,
    ROLE_VIEWER,
    ROLE_NONE,
)
from back.objects.registry.registry_cache import (
    invalidate_registry_cache,
    get_registry_cache_snapshot,
    get_registry_cache_ttl,
    set_registry_cache_ttl,
)

__all__ = [
    "RegistryCfg",
    "RegistryService",
    "PermissionService",
    "permission_service",
    "ROLE_ADMIN",
    "ROLE_EDITOR",
    "ROLE_VIEWER",
    "ROLE_NONE",
    "BuildScheduler",
    "get_scheduler",
    "invalidate_registry_cache",
    "get_registry_cache_snapshot",
    "get_registry_cache_ttl",
    "set_registry_cache_ttl",
]


def __getattr__(name: str):
    """Lazy-import scheduler (APScheduler) so tests and minimal envs can import RegistryCfg."""
    if name == "BuildScheduler":
        from back.objects.registry.scheduler import BuildScheduler as _BuildScheduler

        globals()["BuildScheduler"] = _BuildScheduler
        return _BuildScheduler
    if name == "get_scheduler":
        from back.objects.registry.scheduler import get_scheduler as _get_scheduler

        globals()["get_scheduler"] = _get_scheduler
        return _get_scheduler
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
