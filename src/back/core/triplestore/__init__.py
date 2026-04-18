"""Triple store backend abstraction."""

from back.core.triplestore.TripleStoreBackend import TripleStoreBackend  # noqa: F401
from back.core.triplestore.delta import DeltaTripleStore  # noqa: F401
from back.core.triplestore.TripleStoreFactory import TripleStoreFactory  # noqa: F401
from back.core.triplestore.IncrementalBuildService import (
    IncrementalBuildService,
)  # noqa: F401
from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL  # noqa: F401

get_triplestore = TripleStoreFactory.get_triplestore
LADYBUG_AVAILABLE = TripleStoreFactory.LADYBUG_AVAILABLE

__all__ = [
    "TripleStoreBackend",
    "DeltaTripleStore",
    "TripleStoreFactory",
    "IncrementalBuildService",
    "get_triplestore",
    "LADYBUG_AVAILABLE",
    "RDF_TYPE",
    "RDFS_LABEL",
]
