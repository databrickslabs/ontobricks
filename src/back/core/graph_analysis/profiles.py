"""Shared entity-type profiling helpers.

The temporal-predicate detection and the flat-dataset heuristic label the
per-type rollups the Databricks analytics job computes. They are pure string
heuristics over predicate names and instance counts, which is why they live in
the app rather than in the job's SQL — keeping one copy means the wording of
"flat" cannot drift.
"""

from __future__ import annotations

from typing import Iterable, List, Set

# Predicate local-name fragments that suggest time-series / temporal data.
TEMPORAL_KEYWORDS: Set[str] = {
    "time", "date", "timestamp", "ts", "at", "created", "modified", "dt",
    "start", "end", "recorded", "occurred", "measured",
}


def local_name(uri: str) -> str:
    """Extract the local name from a URI (last path/fragment segment, lower-cased)."""
    return (uri or "").rstrip("/").split("/")[-1].split("#")[-1].lower()


def has_temporal_predicates(predicates: Iterable[str]) -> bool:
    """Whether any predicate's local name hints at temporal data."""
    return any(
        kw in local_name(p) for p in predicates for kw in TEMPORAL_KEYWORDS
    )


def flat_reasons(instance_count: int, distinct_predicates: int) -> List[str]:
    """Return human-readable reasons this entity type looks like a flat dataset.

    Empty when the type looks properly connected.  Degree centrality is
    normalised by ``N-1``, which makes it useless as a scale-free signal, so
    predicate diversity is the primary heuristic.
    """
    reasons: List[str] = []
    if distinct_predicates == 0:
        reasons.append("no entity-entity relationships (fully isolated instances)")
    elif distinct_predicates == 1 and instance_count > 20:
        reasons.append(
            f"only 1 distinct relationship predicate across {instance_count} instances"
        )
    return reasons
