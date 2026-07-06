"""Graph database backend abstraction.

The single triple-store / graph DB layer for OntoBricks.  All backends operate
on the ``(subject, predicate, object)`` model and subclass
:class:`GraphDBBackend`.  Engines are pluggable under
``back/core/graphdb/<engine>/`` — the default is Lakebase Postgres
(``lakebase``), with a Databricks Delta engine (``delta``) and a raw read-only
view store (``view``).  See ``_starter_kit/`` for a copy-paste template.
"""

from back.core.graphdb.GraphDBBackend import GraphDBBackend  # noqa: F401
from back.core.graphdb.GraphDBFactory import GraphDBFactory  # noqa: F401
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL  # noqa: F401

get_graphdb = GraphDBFactory.get_graphdb
GRAPHDB_AVAILABLE = GraphDBFactory.LAKEBASE_AVAILABLE

__all__ = [
    "GraphDBBackend",
    "GraphDBFactory",
    "GRAPHDB_AVAILABLE",
    "get_graphdb",
    "RDF_TYPE",
    "RDFS_LABEL",
]
