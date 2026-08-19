"""Shared entity-type profiling helpers.

The temporal-predicate detection and the flat-dataset heuristic label the
per-type rollups the Databricks analytics job computes. They are pure string
heuristics over predicate names and instance counts, which is why they live in
the app rather than in the job's SQL — keeping one copy means the wording of
"flat" cannot drift.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Set

# Predicate local-name *tokens* that suggest time-series / temporal data.
# Matched as whole camelCase / snake_case segments (not bare substrings), so
# short keywords like ``dt`` / ``at`` do not fire inside ``assignedTo`` /
# ``locatedIn``.
TEMPORAL_KEYWORDS: Set[str] = {
    "time", "date", "timestamp", "ts", "at", "created", "modified", "dt",
    "start", "end", "recorded", "occurred", "measured",
}

# Split camelCase / PascalCase / snake_case / kebab-case into tokens.
_TOKEN_SPLIT = re.compile(
    r"[_\-\s]+|(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])"
)


def local_name(uri: str) -> str:
    """Extract the local name from a URI (last path/fragment segment, lower-cased)."""
    return (uri or "").rstrip("/").split("/")[-1].split("#")[-1].lower()


def _local_tokens(uri: str) -> List[str]:
    """Lower-cased camelCase / snake_case tokens of a predicate local name."""
    raw = (uri or "").rstrip("/").split("/")[-1].split("#")[-1]
    if not raw:
        return []
    return [part.lower() for part in _TOKEN_SPLIT.split(raw) if part]


def has_temporal_predicates(predicates: Iterable[str]) -> bool:
    """Whether any predicate's local-name token hints at temporal data."""
    return any(
        token in TEMPORAL_KEYWORDS
        for predicate in predicates
        for token in _local_tokens(predicate)
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
