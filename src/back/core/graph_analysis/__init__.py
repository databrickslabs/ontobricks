"""Graph analysis: community detection and cohort discovery."""

from back.core.graph_analysis.CohortVocabulary import CohortVocabulary
from back.core.graph_analysis.CommunityDetector import CommunityDetector
from back.core.graph_analysis.models import (
    ClusterRequest,
    ClusterResult,
    DetectionResult,
    DetectionStats,
    CohortHop,
    CohortLink,
    CohortCompat,
    CohortUCTarget,
    CohortOutput,
    CohortRule,
    CohortGroup,
    CohortStats,
    CohortResult,
    COHORT_COMPAT_TYPES,
    COHORT_GROUP_TYPES,
    COHORT_LINKS_COMBINE,
)

# ---------------------------------------------------------------------------
# Backward-compatible cohort vocabulary fragments and helper functions.
#
# The canonical home is :class:`CohortVocabulary`; these module-level
# names are thin wrappers so existing callers (and external imports
# from ``back.core.graph_analysis``) keep working.
# ---------------------------------------------------------------------------

COHORT_CLASS_FRAGMENT = CohortVocabulary.COHORT_CLASS_FRAGMENT
IN_COHORT_FRAGMENT = CohortVocabulary.IN_COHORT_FRAGMENT
FROM_RULE_FRAGMENT = CohortVocabulary.FROM_RULE_FRAGMENT
COHORT_SIZE_FRAGMENT = CohortVocabulary.COHORT_SIZE_FRAGMENT
COHORT_PATH_FRAGMENT = CohortVocabulary.COHORT_PATH_FRAGMENT


def cohort_class_uri(base_uri: str) -> str:
    """Return the ``Cohort`` class URI for a given domain.

    Backward-compatible wrapper around
    :meth:`CohortVocabulary.cohort_class`.
    """
    return CohortVocabulary.cohort_class(base_uri)


def in_cohort_predicate(base_uri: str) -> str:
    """Return the ``inCohort`` predicate URI for a given domain.

    Backward-compatible wrapper around :meth:`CohortVocabulary.in_cohort`.
    """
    return CohortVocabulary.in_cohort(base_uri)


def from_rule_predicate(base_uri: str) -> str:
    """Return the ``fromRule`` predicate URI for a given domain.

    Backward-compatible wrapper around :meth:`CohortVocabulary.from_rule`.
    """
    return CohortVocabulary.from_rule(base_uri)


def cohort_size_predicate(base_uri: str) -> str:
    """Return the ``cohortSize`` predicate URI for a given domain.

    Backward-compatible wrapper around :meth:`CohortVocabulary.cohort_size`.
    """
    return CohortVocabulary.cohort_size(base_uri)


def cohort_uri_prefix(base_uri: str, rule_id: str) -> str:
    """Return the URI prefix every cohort produced by *rule_id* shares.

    Backward-compatible wrapper around
    :meth:`CohortVocabulary.cohort_prefix`.
    """
    return CohortVocabulary.cohort_prefix(base_uri, rule_id)


def cohort_uri(base_uri: str, rule_id: str, content_hash: str) -> str:
    """Return the URI for a single cohort produced by a rule.

    Backward-compatible wrapper around :meth:`CohortVocabulary.cohort`.
    """
    return CohortVocabulary.cohort(base_uri, rule_id, content_hash)


__all__ = [
    "CohortVocabulary",
    "CommunityDetector",
    "ClusterRequest",
    "ClusterResult",
    "DetectionResult",
    "DetectionStats",
    "CohortHop",
    "CohortLink",
    "CohortCompat",
    "CohortUCTarget",
    "CohortOutput",
    "CohortRule",
    "CohortGroup",
    "CohortStats",
    "CohortResult",
    "COHORT_COMPAT_TYPES",
    "COHORT_GROUP_TYPES",
    "COHORT_LINKS_COMBINE",
    "COHORT_CLASS_FRAGMENT",
    "IN_COHORT_FRAGMENT",
    "FROM_RULE_FRAGMENT",
    "COHORT_SIZE_FRAGMENT",
    "COHORT_PATH_FRAGMENT",
    "cohort_class_uri",
    "in_cohort_predicate",
    "from_rule_predicate",
    "cohort_size_predicate",
    "cohort_uri_prefix",
    "cohort_uri",
]
