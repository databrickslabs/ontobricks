"""Registry data-store abstraction.

The :class:`RegistryStore` ABC is the single seam between the registry
services (:mod:`back.objects.registry.RegistryService`,
:mod:`back.objects.registry.PermissionService`,
:mod:`back.objects.registry.scheduler`,
:mod:`back.objects.session.GlobalConfigService`) and the underlying
Lakebase Postgres storage.

A single concrete backend is supported:

- :mod:`back.objects.registry.store.lakebase` — Postgres tables on
  Databricks Lakebase. Requires the ``lakebase`` extra (psycopg3 +
  psycopg-pool) and is imported lazily so that import failures surface
  only when a store is actually instantiated.

Always go through :class:`RegistryFactory` to obtain a concrete store
— call sites must not import the ``lakebase`` subpackage directly.

Domain-scoped binary artefacts (the ``documents/`` uploads imported by
the ontology designer) always live on the Unity Catalog Volume and are
managed by :class:`back.core.databricks.unity_catalog.VolumeFileService` — the store
handles JSON-shaped data only.

The historical JSON-on-Volume backend (``VolumeRegistryStore``) was
removed in v0.4.0. Operators with on-Volume registry data must run
``scripts/migrate-registry-to-lakebase.sh`` once before upgrading.
"""

from __future__ import annotations

from .base import (
    DomainSummary,
    RegistryStore,
    ScheduleHistoryEntry,
    StoreError,
)
from .factory import RegistryFactory

__all__ = [
    "DomainSummary",
    "LakebaseRegistryStore",
    "RegistryFactory",
    "RegistryStore",
    "ScheduleHistoryEntry",
    "StoreError",
]


def __getattr__(name: str):
    """Lazy-import :class:`LakebaseRegistryStore`.

    Pulling :mod:`psycopg` at package-load time would force callers
    that never touch the store (e.g. read-only path builders) to
    install the optional extra. Importing the class via attribute
    access (``store.LakebaseRegistryStore``) defers the import until
    it is actually needed.
    """
    if name == "LakebaseRegistryStore":
        from .lakebase import LakebaseRegistryStore as _LakebaseRegistryStore

        globals()["LakebaseRegistryStore"] = _LakebaseRegistryStore
        return _LakebaseRegistryStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
