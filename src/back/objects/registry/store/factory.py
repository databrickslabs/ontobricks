"""Single facing entry point for obtaining a :class:`RegistryStore`.

Every call site that needs a registry store should go through
:class:`RegistryFactory`. Concrete store classes
(:class:`VolumeRegistryStore`, :class:`LakebaseRegistryStore`) live in
their own subpackages and are imported lazily by the factory — keeping
volume-only deployments free of the optional Lakebase dependencies.

Why a class instead of a free function?

- Discoverability: one symbol (``RegistryFactory``) groups every
  store-construction primitive — backend dispatch, explicit
  per-backend constructors, and ``from_*`` resolvers.
- Encapsulation: the factory hides which import path a store class
  lives at, so the call sites never reach into
  ``store.volume`` / ``store.lakebase`` directly.
- Symmetry with the rest of the codebase
  (``RegistryService.from_context``, ``RegistryCfg.from_domain``…).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .base import RegistryStore

if TYPE_CHECKING:  # pragma: no cover -- typing only
    from back.objects.registry.RegistryService import RegistryCfg


_DEFAULT_LAKEBASE_SCHEMA = "ontobricks_registry"


class RegistryFactory:
    """Build :class:`RegistryStore` instances.

    The class is stateless — every method is a ``@staticmethod`` /
    ``@classmethod``. It exists primarily as a namespace.

    Typical usage
    -------------
    >>> from back.objects.registry.store import RegistryFactory
    >>> store = RegistryFactory.for_backend("lakebase", registry_cfg=cfg)
    >>> store.is_initialized()
    True

    Or, when you already know the backend:

    >>> store = RegistryFactory.volume(registry_cfg=cfg, host=h, token=t)
    >>> store = RegistryFactory.lakebase(registry_cfg=cfg, schema="ontobricks_registry")
    """

    # ------------------------------------------------------------------
    # Backend-aware dispatch
    # ------------------------------------------------------------------

    @staticmethod
    def for_backend(
        backend: str,
        *,
        registry_cfg: "RegistryCfg",
        host: str = "",
        token: str = "",
        lakebase_schema: str = _DEFAULT_LAKEBASE_SCHEMA,
        lakebase_database: str = "",
    ) -> RegistryStore:
        """Return the right concrete store for *backend*.

        Parameters
        ----------
        backend:
            ``"lakebase"`` for the Postgres backend, anything else
            (including ``"volume"`` and unknown values) maps to the
            Volume backend.
        registry_cfg:
            :class:`back.objects.registry.RegistryService.RegistryCfg`
            — used by the Volume backend to build paths and by the
            Lakebase backend as the registry identity.
        host, token:
            Databricks workspace credentials — required by the
            Volume backend. Unused by the Lakebase backend (which
            authenticates via injected ``PG*`` env vars).
        lakebase_schema:
            Postgres schema where the registry tables live. Defaults
            to ``"ontobricks_registry"``.
        lakebase_database:
            Optional override of the Postgres database name. Empty
            (the default) means "use the bound ``PGDATABASE``". A
            non-empty value picks a different database on the same
            Lakebase instance — see :class:`LakebaseRegistryStore`.
        """
        backend_l = (backend or "volume").strip().lower()
        if backend_l == "lakebase":
            return RegistryFactory.lakebase(
                registry_cfg=registry_cfg,
                schema=lakebase_schema or _DEFAULT_LAKEBASE_SCHEMA,
                database=lakebase_database,
            )
        return RegistryFactory.volume(
            registry_cfg=registry_cfg, host=host, token=token
        )

    # ------------------------------------------------------------------
    # Explicit per-backend constructors
    # ------------------------------------------------------------------

    @staticmethod
    def volume(
        *,
        registry_cfg: "RegistryCfg",
        host: str = "",
        token: str = "",
    ) -> RegistryStore:
        """Build a JSON-on-Volume store (the default backend).

        ``host`` / ``token`` are passed through to the inner
        :class:`back.core.databricks.VolumeFileService`. Pass empty
        strings only when you intend to override the inner ``_uc``
        attribute yourself (e.g. test doubles, see
        :meth:`RegistryService._build_store`).
        """
        from .volume import VolumeRegistryStore

        return VolumeRegistryStore(
            registry_cfg=registry_cfg, host=host, token=token
        )

    @staticmethod
    def lakebase(
        *,
        registry_cfg: "RegistryCfg",
        schema: str = _DEFAULT_LAKEBASE_SCHEMA,
        database: str = "",
    ) -> RegistryStore:
        """Build a Lakebase (Postgres) store.

        Lazily imports :mod:`psycopg` so volume-only deployments do
        not need the ``lakebase`` extra installed. Raises
        :class:`back.core.errors.InfrastructureError` at instantiation
        time if the extra is missing.

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
        *,
        host: str = "",
        token: str = "",
    ) -> RegistryStore:
        """Build the store implied by ``registry_cfg.backend``.

        Convenience wrapper around :meth:`for_backend` for callers
        that already hold a fully-populated ``RegistryCfg``.
        """
        return cls.for_backend(
            registry_cfg.backend,
            registry_cfg=registry_cfg,
            host=host,
            token=token,
            lakebase_schema=registry_cfg.lakebase_schema,
            lakebase_database=getattr(registry_cfg, "lakebase_database", ""),
        )


def build_store(
    backend: str,
    *,
    registry_cfg: "RegistryCfg",
    host: str = "",
    token: str = "",
    lakebase_schema: str = _DEFAULT_LAKEBASE_SCHEMA,
    lakebase_database: str = "",
) -> RegistryStore:
    """Backwards-compatible alias for :meth:`RegistryFactory.for_backend`.

    .. deprecated::
        New code should call ``RegistryFactory.for_backend(...)``,
        ``RegistryFactory.volume(...)`` or
        ``RegistryFactory.lakebase(...)`` directly. This shim is
        retained so external callers (and any forgotten internal
        ones) keep working during the transition.
    """
    return RegistryFactory.for_backend(
        backend,
        registry_cfg=registry_cfg,
        host=host,
        token=token,
        lakebase_schema=lakebase_schema,
        lakebase_database=lakebase_database,
    )
