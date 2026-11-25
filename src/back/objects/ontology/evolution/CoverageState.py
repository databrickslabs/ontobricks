"""Outcome of one coverage obligation."""

from __future__ import annotations

from enum import Enum


class CoverageState(str, Enum):
    """Whether one requirement of a question is met by the ontology and mappings.

    Only RESOLVED counts toward readiness. The others say why not, and are
    never averaged away.
    """

    RESOLVED = "RESOLVED"
    AMBIGUOUS = "AMBIGUOUS"
    MISSING_SEMANTICS = "MISSING_SEMANTICS"
    INACCESSIBLE = "INACCESSIBLE"
    STALE = "STALE"
    UNVERIFIED = "UNVERIFIED"
    UNSUPPORTED = "UNSUPPORTED"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
