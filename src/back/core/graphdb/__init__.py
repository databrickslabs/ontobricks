"""Graph database backend abstraction.

The single triple-store / graph DB layer for OntoBricks.  All backends operate
on the ``(subject, predicate, object)`` model and subclass one of the two hat
interfaces over the shared read root :class:`GraphBackend`:

* :class:`MaterializedGraphBackend` — copy-owning engines (Lakebase, Delta
  ``table``, Neo4j) with the full write / companion / sync surface.
* :class:`VirtualBackend` — read-only, no-copy pass-through engines (Delta
  ``view``).

Engines are pluggable under ``back/core/graphdb/<engine>/`` — the default is
Lakebase Postgres (``lakebase``), with a Databricks Delta engine (``delta``)
and a raw read-only view store (``view``).  The backend contract (hats +
capabilities) lives in :mod:`back.core.graphdb.interfaces`.
"""

from back.core.graphdb.GraphDBFactory import GraphDBFactory  # noqa: F401
from back.core.graphdb.MaterializationModeResolver import (  # noqa: F401
    MaterializationModeResolver,
)
from back.core.graphdb.ModeDecision import ModeDecision  # noqa: F401
from back.core.graphdb.RealtimeSource import RealtimeSource  # noqa: F401
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL  # noqa: F401

get_graphdb = GraphDBFactory.get_graphdb
GRAPHDB_AVAILABLE = GraphDBFactory.LAKEBASE_AVAILABLE

__all__ = [
    "GraphDBFactory",
    "MaterializationModeResolver",
    "ModeDecision",
    "RealtimeSource",
    "GRAPHDB_AVAILABLE",
    "get_graphdb",
    "RDF_TYPE",
    "RDFS_LABEL",
]
