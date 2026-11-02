"""Lakebase Postgres implementation of the OntoBricks graph DB engine."""

from back.core.graphdb.lakebase.Base import (
    DEFAULT_GRAPH_SCHEMA,
    default_schema,
    normalize_sync_table_mode,
    validate_engine_config_keys,
    validate_graph_schema,
)

try:
    from back.core.graphdb.lakebase.pool import _require_psycopg

    _require_psycopg()
    from back.core.graphdb.lakebase.FlatStore import FlatStore  # noqa: F401
    from back.core.graphdb.lakebase.SimulationStore import (  # noqa: F401
        SimulationStore,
    )
    from back.core.graphdb.lakebase.SimulationUnion import (  # noqa: F401
        SimulationUnion,
    )

    LAKEBASE_AVAILABLE = True
except ImportError:  # pragma: no cover
    LAKEBASE_AVAILABLE = False
    FlatStore = None  # type: ignore[misc, assignment]
    SimulationStore = None  # type: ignore[misc, assignment]
    SimulationUnion = None  # type: ignore[misc, assignment]

from back.core.graphdb.lakebase.SyncedTableManager import SyncedTableManager  # noqa: E402

__all__ = [
    "DEFAULT_GRAPH_SCHEMA",
    "LAKEBASE_AVAILABLE",
    "FlatStore",
    "SimulationStore",
    "SimulationUnion",
    "SyncedTableManager",
    "default_schema",
    "normalize_sync_table_mode",
    "validate_engine_config_keys",
    "validate_graph_schema",
]

