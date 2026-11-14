"""Single facing entry point for obtaining a :class:`RegistryStore`.

Every call site that needs a registry store should go through
:class:`RegistryFactory`. The concrete store class
(:class:`LakebaseRegistryStore`) lives in its own subpackage and is
imported lazily so import failures (missing ``psycopg`` extra, etc.)
surface only when a Lakebase store is actually requested.

Why a class instead of a free function?

- Discoverability: one symbol (``RegistryFactory``) groups the
  store-construction primitives.
- Encapsulation: the factory hides which import path the store class
  lives at, so call sites never reach into ``store.lakebase``
  directly.
- Symmetry with the rest of the codebase
  (``RegistryService.from_context``, ``RegistryCfg.from_domain``…).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from .base import RegistryStore

if TYPE_CHECKING:  # pragma: no cover -- typing only
    from back.objects.registry.RegistryCfg import RegistryCfg


_DEFAULT_LAKEBASE_SCHEMA = "ontobricks_registry"
_BACKENDS = ("lakebase", "sqlite")


class RegistryFactory:
    """Build :class:`RegistryStore` instances.

    The class is stateless — every method is a ``@staticmethod`` /
    ``@classmethod``. It exists primarily as a namespace.

    Typical usage
    -------------
    >>> from back.objects.registry.store import RegistryFactory
    >>> store = RegistryFactory.lakebase(registry_cfg=cfg)
    >>> store.is_initialized()
    True
    """

    # ------------------------------------------------------------------
    # Lakebase store constructor
    # ------------------------------------------------------------------

    @staticmethod
    def lakebase(
        *,
        registry_cfg: "RegistryCfg",
        schema: str = _DEFAULT_LAKEBASE_SCHEMA,
        database: str = "",
    ) -> RegistryStore:
        """Build a Lakebase (Postgres) store.

        Lazily imports :mod:`psycopg` so import-time crashes surface
        only when a Lakebase store is actually requested. Raises
        :class:`back.core.errors.InfrastructureError` at instantiation
        time if the ``psycopg`` extra is missing.

        ``database`` (optional) overrides the bound ``PGDATABASE``;
        empty falls back to the runtime-injected database.
        """
        from .lakebase import LakebaseRegistryStore

        return LakebaseRegistryStore(
            registry_cfg=registry_cfg,
            schema=schema or _DEFAULT_LAKEBASE_SCHEMA,
            database=database,
        )

    # ------------------------------------------------------------------
    # SQLite store constructor (regions without Lakebase)
    # ------------------------------------------------------------------

    @staticmethod
    def sqlite(
        *,
        registry_cfg: "RegistryCfg",
        schema: str = _DEFAULT_LAKEBASE_SCHEMA,
    ) -> RegistryStore:
        """Build a SQLite store, snapshotted to the registry Volume when one is bound.

        The snapshot lives at ``$REGISTRY_SQLITE_SNAPSHOT_PATH`` or, by
        default, ``/Volumes/<catalog>/<schema>/<volume>/_registry/<schema>.sqlite``.
        """
        from back.core.databricks import VolumeFileService

        from .sqlite import SQLiteRegistryStore, SQLiteSnapshot

        schema = schema or _DEFAULT_LAKEBASE_SCHEMA
        local_path = SQLiteRegistryStore.default_local_path(schema)
        volume_path = RegistryFactory._sqlite_snapshot_path(registry_cfg, schema)
        snapshot = (
            SQLiteSnapshot.shared(local_path, volume_path, VolumeFileService)
            if volume_path
            else None
        )
        return SQLiteRegistryStore(
            registry_cfg=registry_cfg,
            schema=schema,
            local_path=local_path,
            snapshot=snapshot,
        )

    @staticmethod
    def _sqlite_snapshot_path(registry_cfg: "RegistryCfg", schema: str) -> str:
        configured = os.environ.get("REGISTRY_SQLITE_SNAPSHOT_PATH", "").strip()
        if configured:
            return configured
        c = registry_cfg
        if not (c.catalog and c.schema and c.volume):
            return ""
        return "/Volumes/%s/%s/%s/_registry/%s.sqlite" % (c.catalog, c.schema, c.volume, schema)

    # ------------------------------------------------------------------
    # High-level resolvers
    # ------------------------------------------------------------------

    @staticmethod
    def backend_name() -> str:
        """The configured registry backend: ``lakebase`` (default) or ``sqlite``."""
        from shared.config.settings import get_settings

        name = (get_settings().registry_backend or "lakebase").strip().lower()
        return name if name in _BACKENDS else "lakebase"

    @classmethod
    def from_cfg(
        cls,
        registry_cfg: "RegistryCfg",
    ) -> RegistryStore:
        """Build the configured store from a fully-populated :class:`RegistryCfg`."""
        if cls.backend_name() == "sqlite":
            return cls.sqlite(registry_cfg=registry_cfg, schema=registry_cfg.lakebase_schema)
        return cls.lakebase(
            registry_cfg=registry_cfg,
            schema=registry_cfg.lakebase_schema,
            database=getattr(registry_cfg, "lakebase_database", ""),
        )
