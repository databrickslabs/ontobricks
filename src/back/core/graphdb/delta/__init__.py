"""Databricks (Unity Catalog Delta) graph engine for OntoBricks."""

from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore  # noqa: F401
from back.core.graphdb.delta.DeltaTripleStoreBuildPipeline import (  # noqa: F401
    DeltaTripleStoreBuildPipeline,
)

DELTA_AVAILABLE = True

__all__ = [
    "DELTA_AVAILABLE",
    "DeltaFlatStore",
    "DeltaTripleStoreBuildPipeline",
]
