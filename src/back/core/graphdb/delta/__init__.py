"""Databricks (Unity Catalog Delta) graph engine for OntoBricks."""

from back.core.graphdb.delta.ChangeFeedReader import ChangeFeedReader  # noqa: F401
from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore  # noqa: F401
from back.core.graphdb.delta.DeltaMergeMaterializer import (  # noqa: F401
    DeltaMergeMaterializer,
)
from back.core.graphdb.delta.DeltaTripleStoreBuildPipeline import (  # noqa: F401
    DeltaTripleStoreBuildPipeline,
)
from back.core.graphdb.delta.MaterializedViewBuilder import (  # noqa: F401
    MaterializedViewBuilder,
)

DELTA_AVAILABLE = True

__all__ = [
    "ChangeFeedReader",
    "DELTA_AVAILABLE",
    "DeltaFlatStore",
    "DeltaMergeMaterializer",
    "DeltaTripleStoreBuildPipeline",
    "MaterializedViewBuilder",
]
