"""Lakebase-backed :class:`RegistryStore` implementation.

Stores registry-shaped data in PostgreSQL tables on a Databricks
Lakebase instance. Optional backend — ``psycopg`` is imported lazily,
so volume-only deployments do not need the ``lakebase`` extra
installed.

Submodules
----------
- :mod:`back.objects.registry.store.lakebase.store` — the
  :class:`LakebaseRegistryStore` class.
- :mod:`back.objects.registry.store.lakebase.Documents`
  — document corpus mixin inherited by the store.
- :mod:`back.objects.registry.store.lakebase.Collab`
  — comments and tasks mixin.
- :mod:`back.objects.registry.store.lakebase.EditLocks`
  — single-editor lock mixin.
- :mod:`back.objects.registry.store.lakebase.Analytics`
  — graph analytics cache and run history mixin.
- :mod:`back.objects.registry.store.lakebase.ReviewEvents`
  — review/validation audit mixin.
- :mod:`back.objects.registry.store.lakebase.ChangeEvents`
  — ontology/mapping change audit mixin.
- :mod:`back.objects.registry.store.lakebase.BuildRuns`
  — Knowledge Graph build-run trace mixin.
- :mod:`back.objects.registry.store.lakebase.Catalog`
  — domain folders, versions, and permissions mixin.
- :mod:`back.objects.registry.store.lakebase.Schedules`
  — scheduled tasks and history mixin.
- :mod:`back.objects.registry.store.lakebase.Bootstrap`
  — initialize, grants, and permission diagnostics mixin.
- :mod:`back.objects.registry.store.lakebase.GlobalConfig`
  — instance-wide JSONB global_config mixin.
- ``schema.sql`` — idempotent DDL applied on first
  ``initialize()``; the schema name is parameterised via the
  ``__SCHEMA__`` token at runtime.

Authentication is handled by
:class:`back.core.databricks.lakebase.LakebaseAuth` (sources ``PG*`` env vars
and mints short-lived Lakebase JWTs via the workspace SDK).
"""

from __future__ import annotations

from .Analytics import Analytics
from .Bootstrap import Bootstrap
from .BuildRuns import BuildRuns
from .Catalog import Catalog
from .ChangeEvents import ChangeEvents
from .Collab import Collab
from .Documents import Documents
from .EditLocks import EditLocks
from .GlobalConfig import GlobalConfig
from .ReviewEvents import ReviewEvents
from .Schedules import Schedules
from .store import LakebaseRegistryStore

__all__ = [
    "Analytics",
    "Bootstrap",
    "BuildRuns",
    "Catalog",
    "ChangeEvents",
    "Collab",
    "Documents",
    "EditLocks",
    "GlobalConfig",
    "ReviewEvents",
    "Schedules",
    "LakebaseRegistryStore",
]
