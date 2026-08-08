"""Graph-DB connection profile for Lakebase (Postgres).

The generic connection machinery — the LIFO pool, JWT rotation, cold-start
retry and the ``psycopg`` import gate — lives in the shared technical layer
:mod:`back.core.databricks.lakebase`, which the registry store and the graph
triple store both build on (they remain two independent databases).

This module is the graph engine's *profile* of that shared layer: it binds
the pool to the ``ontobricks-graphdb`` workload label and the graph error
type, and exposes the ``psycopg`` gate under the name the graph package uses.
It is the single place that says "this is how the graph engine talks to
Lakebase" — the analogue of the registry store's own connection helpers.
"""

from __future__ import annotations

from typing import Any, Tuple

from back.core.databricks.lakebase.constants import APPLICATION_NAME_GRAPH
from back.core.databricks.lakebase.LakebaseConnectionPool import (
    LakebaseConnectionPool,
    get_lakebase_pool,
)
from back.core.databricks.lakebase.psycopg_gate import require_psycopg


class LakebaseGraphPoolError(RuntimeError):
    """Raised when the graph-db pool cannot serve a connection."""


def _require_psycopg() -> Tuple[Any, Any]:
    """Return ``(psycopg, psycopg.rows.dict_row)`` for the graph engine.

    Thin accessor over the shared :func:`back.core.databricks.lakebase.
    require_psycopg` gate, exposed here so every graph module imports the
    ``psycopg`` dependency through a single graph-owned name.
    """
    return require_psycopg()


def get_lakebase_graph_pool(
    auth: Any, schema: str, database: str = ""
) -> LakebaseConnectionPool:
    """Return the shared Lakebase pool bound to the graph workload.

    Wraps :func:`back.core.databricks.lakebase.get_lakebase_pool` with the
    ``ontobricks-graphdb`` application label and :class:`LakebaseGraphPoolError`
    so graph connection failures surface as a graph-specific error. The pool
    key is the full connection identity, so the graph engine never shares a
    pool with the registry even when both point at the same Lakebase instance.
    """
    return get_lakebase_pool(
        auth,
        schema,
        database,
        application_name=APPLICATION_NAME_GRAPH,
        error_factory=LakebaseGraphPoolError,
    )
