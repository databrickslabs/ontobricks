"""Data models for graph community detection."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ClusterRequest:
    """Parameters for a community detection request."""

    algorithm: str = "louvain"
    resolution: float = 1.0
    predicate_filter: Optional[List[str]] = None
    class_filter: Optional[List[str]] = None
    max_triples: int = 500_000


@dataclass
class ClusterResult:
    """A single detected community cluster."""

    id: int
    members: List[str] = field(default_factory=list)
    size: int = 0


@dataclass
class DetectionStats:
    """Aggregate statistics from a community detection run."""

    node_count: int = 0
    edge_count: int = 0
    cluster_count: int = 0
    modularity: float = 0.0
    algorithm: str = "louvain"
    elapsed_ms: int = 0


@dataclass
class DetectionResult:
    """Full result of a community detection run."""

    clusters: List[ClusterResult] = field(default_factory=list)
    stats: DetectionStats = field(default_factory=DetectionStats)
