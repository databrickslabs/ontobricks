"""Classify materialised inferred triples by origin, at query time.

The companion / inferred table has no provenance column, so the category is
derived from the vocabulary written by the cohort and community features
(see ``CohortVocabulary`` and ``CommunityVocabulary``):

* ``cohorts``     - subject under ``<base>/cohort/``, predicate ``<base>/inCohort*``,
                    ``<base>/fromRule`` or ``<base>/cohortSize``
* ``communities`` - subject under ``<base>/cluster/``, predicate ``<base>/inCluster``,
                    ``<base>/clusterSize``, ``<base>/fromAlgorithm``,
                    ``<base>/modularity`` or ``<base>/resolution``
* ``reasoning``   - everything else

The first matching rule wins, in the order above.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

CATEGORIES: tuple[tuple[str, str], ...] = (
    ("reasoning", "Reasoning"),
    ("cohorts", "Cohorts"),
    ("communities", "Communities"),
)

# Fragments mirror CohortVocabulary / CommunityVocabulary. Keep them in sync;
# the oracle test asserts the equality.
_COHORT_PATH = "cohort"
_IN_COHORT = "inCohort"
_FROM_RULE = "fromRule"
_COHORT_SIZE = "cohortSize"
_CLUSTER_PATH = "cluster"
_IN_CLUSTER = "inCluster"
_CLUSTER_META = ("clusterSize", "fromAlgorithm", "modularity", "resolution")


def _join(base: str, fragment: str) -> str:
    """Join *base* and *fragment* with the same convention as the vocabularies."""
    if not base:
        return fragment
    if base.endswith("/") or base.endswith("#"):
        return base + fragment
    return base + "/" + fragment


def _vocabulary_case(
    base_uri: str,
    escape: Callable[[str], str],
    *,
    cohort: str,
    community: str,
    other: str,
) -> str:
    """``CASE`` classifying a row by the cohort / community vocabulary.

    The three outcomes are passed as SQL literals so the same rules can produce
    breakdown categories (``cohorts``) or override origins (``cohort``).
    """

    def lit(value: str) -> str:
        return f"'{escape(value)}'"

    def starts_with(column: str, prefix: str) -> str:
        # substr() rather than left(): same semantics, portable across
        # Spark SQL, Postgres and SQLite.
        return f"substr({column}, 1, {len(prefix)}) = {lit(prefix)}"

    cohort_subject = _join(base_uri, _COHORT_PATH) + "/"
    cluster_subject = _join(base_uri, _CLUSTER_PATH) + "/"
    in_cohort = _join(base_uri, _IN_COHORT)
    in_cluster = _join(base_uri, _IN_CLUSTER)
    cohort_meta = [_join(base_uri, _FROM_RULE), _join(base_uri, _COHORT_SIZE)]
    cluster_meta = [_join(base_uri, frag) for frag in _CLUSTER_META]
    return (
        "CASE"
        f" WHEN {starts_with('subject', cohort_subject)} THEN {cohort}"
        f" WHEN {starts_with('predicate', in_cohort)} THEN {cohort}"
        f" WHEN predicate IN ({', '.join(lit(p) for p in cohort_meta)}) THEN {cohort}"
        f" WHEN {starts_with('subject', cluster_subject)} THEN {community}"
        f" WHEN predicate IN ({', '.join(lit(p) for p in [in_cluster, *cluster_meta])})"
        f" THEN {community}"
        f" ELSE {other}"
        " END"
    )


def legacy_origin_expr(base_uri: str, escape: Callable[[str], str]) -> str:
    """SQL ``CASE`` giving the override ``origin`` of a row from its vocabulary.

    Used once, to migrate the legacy ``_inferred`` / ``__app`` rows (which carry
    no provenance column) into ``_overrides``, and as the fallback for rows
    whose ``origin`` is still NULL.
    """
    return _vocabulary_case(
        base_uri,
        escape,
        cohort="'cohort'",
        community="'community'",
        other="'reasoning'",
    )


def build_breakdown_sql(
    relation: str,
    base_uri: str,
    escape: Callable[[str], str],
    *,
    has_origin: bool = False,
) -> str:
    """Return one ``GROUP BY category`` statement over *relation*.

    *relation* is an already-resolved table identifier. *escape* is the
    backend's string-literal escaper (``GraphBackend._sql_escape``).

    With ``has_origin=False`` the category is derived from the vocabulary (a
    legacy companion with no provenance column). With ``has_origin=True`` the
    relation is an ``_overrides`` table: only derived ``add`` rows are counted
    and the ``origin`` column decides the category, the vocabulary remaining the
    fallback for a NULL ``origin``.

    Result columns: ``category, triples, entities, groups``.

    * ``triples``  - rows in the category
    * ``entities`` - distinct subjects in the category
    * ``groups``   - distinct cohort / cluster objects of membership rows
                     (NULL for reasoning rows, so they never count)
    """

    def lit(value: str) -> str:
        return f"'{escape(value)}'"

    def starts_with(column: str, prefix: str) -> str:
        return f"substr({column}, 1, {len(prefix)}) = {lit(prefix)}"

    in_cohort = _join(base_uri, _IN_COHORT)
    in_cluster = _join(base_uri, _IN_CLUSTER)

    vocabulary = _vocabulary_case(
        base_uri,
        escape,
        cohort="'cohorts'",
        community="'communities'",
        other="'reasoning'",
    )
    if has_origin:
        category_expr = (
            "CASE"
            " WHEN origin = 'cohort' THEN 'cohorts'"
            " WHEN origin = 'community' THEN 'communities'"
            " WHEN origin = 'reasoning' THEN 'reasoning'"
            f" ELSE {vocabulary}"
            " END"
        )
        where = " WHERE op = 'add' AND COALESCE(origin, '') <> 'user'"
    else:
        category_expr = vocabulary
        where = ""
    membership_expr = (
        f"CASE WHEN {starts_with('predicate', in_cohort)}"
        f" OR predicate = {lit(in_cluster)} THEN object END"
    )
    return (
        "SELECT category,"
        " COUNT(*) AS triples,"
        " COUNT(DISTINCT subject) AS entities,"
        " COUNT(DISTINCT grp) AS groups"
        " FROM ("
        f" SELECT subject, {category_expr} AS category, {membership_expr} AS grp"
        f" FROM {relation}{where}"
        ") AS classified"
        " GROUP BY category"
    )


def normalise_breakdown(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return one dict per category in display order, zero-filled when absent."""
    by_category = {str(r["category"]): r for r in rows}
    out: list[dict[str, Any]] = []
    for key, label in CATEGORIES:
        row = by_category.get(key)
        out.append(
            {
                "category": key,
                "label": label,
                "triples": int(row["triples"]) if row else 0,
                "entities": int(row["entities"]) if row else 0,
                "groups": None if key == "reasoning" else (int(row["groups"]) if row else 0),
            }
        )
    return out
