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

from typing import TYPE_CHECKING

from .base import RegistryStore

if TYPE_CHECKING:  # pragma: no cover -- typing only
    from back.objects.registry.RegistryCfg import RegistryCfg


_DEFAULT_LAKEBASE_SCHEMA = "ontobricks_registry"


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
    # High-level resolvers
    # ------------------------------------------------------------------

    @classmethod
    def from_cfg(
        cls,
        registry_cfg: "RegistryCfg",
    ) -> RegistryStore:
        """Build the Lakebase store from a fully-populated :class:`RegistryCfg`."""
        return cls.lakebase(
            registry_cfg=registry_cfg,
            schema=registry_cfg.lakebase_schema,
            database=getattr(registry_cfg, "lakebase_database", ""),
        )
