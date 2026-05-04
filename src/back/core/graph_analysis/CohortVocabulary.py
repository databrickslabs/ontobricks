"""CohortVocabulary -- URI helpers for the Cohort Discovery feature.

Centralises the small handful of URI fragments and predicates the
engine, materialiser, and frontend share. Exposes them through a
single :class:`CohortVocabulary` class with ``@staticmethod`` helpers
so callers can resolve a domain-specific URI from a ``base_uri`` and
optional ``rule_id`` / ``content_hash``.

Vocabulary
----------
* ``Cohort``        -- the OWL class minted under ``base_uri`` for every
                       cohort that materialises to the graph.
* ``inCohort``      -- subject predicate linking a member to its cohort.
* ``fromRule``      -- object predicate recording the rule that produced
                       a cohort (one rule may produce many cohorts).
* ``cohortSize``    -- datatype predicate carrying the cohort cardinality
                       so the UC table and graph stay in sync.
* ``cohort``        -- path segment under which every cohort URI lives,
                       always followed by the rule id and the content
                       hash (``…/cohort/<rule_id>/c-<hash>``).
"""

from __future__ import annotations

from typing import Final


class CohortVocabulary:
    """Domain-aware URI builder for cohort triples.

    All helpers are pure: they take a ``base_uri`` (and optionally a
    ``rule_id`` / ``content_hash``) and return a string. No I/O, no
    state -- safe to call from the engine, materialiser, route layer,
    or unit tests.

    The class-level ``*_FRAGMENT`` constants are the canonical short
    forms; everything else is derived from them, so a future rename
    is a single-place edit.
    """

    COHORT_CLASS_FRAGMENT: Final[str] = "Cohort"
    IN_COHORT_FRAGMENT: Final[str] = "inCohort"
    FROM_RULE_FRAGMENT: Final[str] = "fromRule"
    COHORT_SIZE_FRAGMENT: Final[str] = "cohortSize"
    COHORT_PATH_FRAGMENT: Final[str] = "cohort"

    @staticmethod
    def join(base: str, fragment: str) -> str:
        """Join *base* and a relative *fragment* with the right separator.

        Mirrors the convention used elsewhere in the codebase: when
        *base* already ends with ``/`` or ``#`` we trust the caller;
        otherwise we insert ``/``.
        """
        if not base:
            return fragment
        if base.endswith("/") or base.endswith("#"):
            return base + fragment
        return base + "/" + fragment

    @classmethod
    def cohort_class(cls, base_uri: str) -> str:
        """Return the ``Cohort`` class URI for a given domain."""
        return cls.join(base_uri, cls.COHORT_CLASS_FRAGMENT)

    @classmethod
    def in_cohort(cls, base_uri: str) -> str:
        """Return the ``inCohort`` predicate URI for a given domain."""
        return cls.join(base_uri, cls.IN_COHORT_FRAGMENT)

    @classmethod
    def from_rule(cls, base_uri: str) -> str:
        """Return the ``fromRule`` predicate URI for a given domain."""
        return cls.join(base_uri, cls.FROM_RULE_FRAGMENT)

    @classmethod
    def cohort_size(cls, base_uri: str) -> str:
        """Return the ``cohortSize`` predicate URI for a given domain."""
        return cls.join(base_uri, cls.COHORT_SIZE_FRAGMENT)

    @classmethod
    def cohort_prefix(cls, base_uri: str, rule_id: str) -> str:
        """Return the URI prefix every cohort produced by *rule_id* shares."""
        rule_segment = (rule_id or "").strip().replace(" ", "_")
        cohort_root = cls.join(base_uri, cls.COHORT_PATH_FRAGMENT)
        return f"{cohort_root}/{rule_segment}/"

    @classmethod
    def cohort(cls, base_uri: str, rule_id: str, content_hash: str) -> str:
        """Return the URI for a single cohort produced by a rule."""
        return f"{cls.cohort_prefix(base_uri, rule_id)}c-{content_hash}"
