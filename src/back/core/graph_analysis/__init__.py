"""Graph analysis, grouped by type of analysis.

Topic sub-packages: ``community`` (community detection), ``cohort`` (cohort
discovery), ``pattern`` (pattern discovery) and ``metrics`` (graph metrics).
Shared job plumbing (``LakeflowRunner``, ``preflight``, ``models``,
``profiles``) stays at this level.
"""

from back.core.graph_analysis.cohort import CohortBuilder, CohortVocabulary
from back.core.graph_analysis.community import (
    CommunityMaterializer,
    CommunityVocabulary,
    JobCommunities,
    community_output_table,
)
from back.core.graph_analysis.LakeflowRunner import LakeflowRunner
from back.core.graph_analysis.metrics import (
    METRIC_SERIES_BUCKETS,
    METRIC_SERIES_COLUMNS,
    JobMetrics,
    analytics_snapshot,
    metric_series_query,
    resolve_analytics_source,
    validate_metric_series_column,
)
from back.core.graph_analysis.models import (
    COHORT_COMPAT_TYPES,
    COHORT_GROUP_TYPES,
    COHORT_LINKS_COMBINE,
    MODE_JOB,
    CohortCompat,
    CohortGroup,
    CohortHop,
    CohortLink,
    CohortOutput,
    CohortResult,
    CohortRule,
    CohortStats,
    CohortUCTarget,
    EntityTypeProfile,
    MetricsRequest,
    MetricsResult,
    MetricsStats,
    NodeMetrics,
)
from back.core.graph_analysis.pattern import (
    JobPatterns,
    SavedPatternStore,
    pattern_output_table,
    saved_pattern_table,
)
from back.core.graph_analysis.preflight import (
    analytics_job_configured,
    analytics_job_status,
    community_job_configured,
    community_job_status,
    pattern_job_configured,
    pattern_job_status,
    probe_data_table,
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
    "CommunityVocabulary",
    "CommunityMaterializer",
    "SavedPatternStore",
    "JobPatterns",
    "pattern_output_table",
    "saved_pattern_table",
    "JobCommunities",
    "community_output_table",
    "JobMetrics",
    "METRIC_SERIES_BUCKETS",
    "METRIC_SERIES_COLUMNS",
    "LakeflowRunner",
    "analytics_snapshot",
    "metric_series_query",
    "validate_metric_series_column",
    "resolve_analytics_source",
    "analytics_job_configured",
    "analytics_job_status",
    "community_job_configured",
    "community_job_status",
    "pattern_job_configured",
    "pattern_job_status",
    "probe_data_table",
    "EntityTypeProfile",
    "MetricsRequest",
    "NodeMetrics",
    "MetricsStats",
    "MetricsResult",
    "MODE_JOB",
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
