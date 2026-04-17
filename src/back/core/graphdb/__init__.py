"""Graph database backend abstraction.

Provides a pluggable graph DB layer separate from the triple store
(Delta views in Unity Catalog).  The default engine is LadybugDB.
"""
from back.core.graphdb.GraphDBBackend import GraphDBBackend  # noqa: F401
from back.core.graphdb.GraphDBFactory import GraphDBFactory  # noqa: F401

get_graphdb = GraphDBFactory.get_graphdb
GRAPHDB_AVAILABLE = GraphDBFactory.LADYBUG_AVAILABLE

__all__ = [
    "GraphDBBackend",
    "GraphDBFactory",
    "get_graphdb",
    "GRAPHDB_AVAILABLE",
]
