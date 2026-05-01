"""Registry data-store abstraction.

The :class:`RegistryStore` ABC is the single seam between the registry
services (:mod:`back.objects.registry.RegistryService`,
:mod:`back.objects.registry.PermissionService`,
:mod:`back.objects.registry.scheduler`,
:mod:`back.objects.session.GlobalConfigService`) and the underlying
storage backend.

Two backends are supported, each in its own subpackage:

- :mod:`back.objects.registry.store.volume` — JSON files on a Unity
  Catalog Volume (the original layout; remains the default).
- :mod:`back.objects.registry.store.lakebase` — PostgreSQL tables on
  Databricks Lakebase. Requires the optional ``lakebase`` extra
  (psycopg3 + psycopg-pool) and is loaded lazily only when selected.

Always go through :class:`RegistryFactory` to obtain a concrete store
— call sites must not import the ``volume`` / ``lakebase``
subpackages directly. This keeps the lazy-import discipline that
keeps volume-only deployments free of the optional Lakebase
dependencies.

Binary artifacts (``documents/`` and ``*.lbug.tar.gz``) always live on
the Unity Catalog Volume and are managed by
:class:`back.core.databricks.VolumeFileService` — the store handles
JSON-shaped data only.
"""

from __future__ import annotations

from .base import (
    DomainSummary,
    RegistryStore,
    ScheduleHistoryEntry,
    StoreError,
)
from .factory import RegistryFactory, build_store
from .migration import (
    MigrationReport,
    migrate_volume_to_lakebase,
    summarize as summarize_migration,
)
from .volume import VolumeRegistryStore

__all__ = [
    "DomainSummary",
    "LakebaseRegistryStore",
    "MigrationReport",
    "RegistryFactory",
    "RegistryStore",
    "ScheduleHistoryEntry",
    "StoreError",
    "VolumeRegistryStore",
    "build_store",
    "migrate_volume_to_lakebase",
    "summarize_migration",
]


def __getattr__(name: str):
    """Lazy-import :class:`LakebaseRegistryStore`.

    Pulling :mod:`psycopg` at package-load time would defeat the
    purpose of making Lakebase an optional extra. Importing the class
    via attribute access (``store.LakebaseRegistryStore``) defers the
    import until it is actually needed.
    """
    if name == "LakebaseRegistryStore":
        from .lakebase import LakebaseRegistryStore as _LakebaseRegistryStore

        globals()["LakebaseRegistryStore"] = _LakebaseRegistryStore
        return _LakebaseRegistryStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
