"""Lakebase-backed :class:`RegistryStore` implementation.

Stores registry-shaped data in PostgreSQL tables on a Databricks
Lakebase instance. Optional backend — ``psycopg`` is imported lazily,
so volume-only deployments do not need the ``lakebase`` extra
installed.

Submodules
----------
- :mod:`back.objects.registry.store.lakebase.store` — the
  :class:`LakebaseRegistryStore` class.
- ``schema.sql`` — idempotent DDL applied on first
  ``initialize()``; the schema name is parameterised via the
  ``__SCHEMA__`` token at runtime.

Authentication is handled by
:class:`back.core.databricks.LakebaseAuth` (sources ``PG*`` env vars
and mints short-lived Lakebase JWTs via the workspace SDK).
"""

from __future__ import annotations

from .store import LakebaseRegistryStore

__all__ = ["LakebaseRegistryStore"]
