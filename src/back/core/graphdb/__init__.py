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
and a raw read-only view store (``view``).  See ``_starter_kit/`` for a
copy-paste template.
"""

from back.core.graphdb.GraphBackend import GraphBackend  # noqa: F401
from back.core.graphdb.MaterializedGraphBackend import (  # noqa: F401
    MaterializedGraphBackend,
)
from back.core.graphdb.VirtualBackend import VirtualBackend  # noqa: F401
from back.core.graphdb.GraphBuilder import GraphBuilder  # noqa: F401
from back.core.graphdb.GraphHealthProbe import GraphHealthProbe  # noqa: F401
from back.core.graphdb.GraphProvisioner import GraphProvisioner  # noqa: F401
from back.core.graphdb.GraphSimulation import GraphSimulation  # noqa: F401
from back.core.graphdb.GraphDBFactory import GraphDBFactory  # noqa: F401
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL  # noqa: F401

get_graphdb = GraphDBFactory.get_graphdb
GRAPHDB_AVAILABLE = GraphDBFactory.LAKEBASE_AVAILABLE

__all__ = [
    "GraphBackend",
    "MaterializedGraphBackend",
    "VirtualBackend",
    "GraphBuilder",
    "GraphHealthProbe",
    "GraphProvisioner",
    "GraphSimulation",
    "GraphDBFactory",
    "GRAPHDB_AVAILABLE",
    "get_graphdb",
    "RDF_TYPE",
    "RDFS_LABEL",
]
