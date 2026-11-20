"""Final decision of an Ontology Evolve run."""

from __future__ import annotations

from enum import Enum


class Decision(str, Enum):
    """What the run concluded. Only PASS lets a candidate be accepted.

    INCONCLUSIVE covers unavailable sources, exhausted budgets and source
    data that changed between the two arms; it is never a pass.
    """

    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE = "INCONCLUSIVE"
    NO_CHANGE = "NO_CHANGE"
    BLOCKED = "BLOCKED"
    ABSTAIN = "ABSTAIN"
    STALE = "STALE"
    CANCELLED = "CANCELLED"
