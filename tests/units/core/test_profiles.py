"""Tests for the entity-type labelling heuristics.

These are the only graph-analytics computations left in the app: the Lakeflow job
supplies every number, and these turn a count and a predicate set into wording a
user reads. They used to be covered incidentally by the NetworkX/pushdown parity
suites, which this plan deleted, so they are pinned directly here.
"""

import pytest

from back.core.graph_analysis.profiles import (
    flat_reasons,
    has_temporal_predicates,
    local_name,
)

pytestmark = pytest.mark.unit

NS = "http://ex.org/"


class TestLocalName:
    def test_a_path_uri_keeps_only_its_last_segment(self):
        assert local_name(f"{NS}Order/placedAt") == "placedat"

    def test_a_fragment_uri_keeps_only_its_fragment(self):
        assert local_name("http://ex.org/onto#createdDate") == "createddate"

    def test_a_trailing_slash_does_not_produce_an_empty_name(self):
        assert local_name(f"{NS}Order/") == "order"

    def test_an_empty_uri_is_tolerated(self):
        assert local_name("") == ""


class TestTemporalDetection:
    @pytest.mark.parametrize(
        "predicate",
        [
            f"{NS}orderDate",
            f"{NS}createdAt",
            f"{NS}measuredTimestamp",
            f"{NS}startDt",
        ],
    )
    def test_time_flavoured_predicates_are_detected(self, predicate):
        assert has_temporal_predicates([predicate]) is True

    def test_one_temporal_predicate_among_many_is_enough(self):
        assert (
            has_temporal_predicates([f"{NS}assignedTo", f"{NS}orderDate"]) is True
        )

    def test_a_purely_structural_predicate_set_is_not_temporal(self):
        assert has_temporal_predicates([f"{NS}ownedBy"]) is False

    def test_short_keywords_should_not_match_inside_unrelated_words(self):
        # Keywords like "dt" / "at" must be whole tokens (camelCase / snake_case
        # segments), not bare substrings of unrelated predicates.
        assert has_temporal_predicates([f"{NS}assignedTo"]) is False
        assert has_temporal_predicates([f"{NS}locatedIn"]) is False
        assert has_temporal_predicates([f"{NS}status"]) is False

    def test_no_predicates_is_not_temporal(self):
        assert has_temporal_predicates([]) is False


class TestFlatReasons:
    def test_no_relationships_at_all_is_the_strongest_signal(self):
        reasons = flat_reasons(instance_count=5, distinct_predicates=0)
        assert len(reasons) == 1
        assert "fully isolated" in reasons[0]

    def test_a_single_predicate_over_many_instances_is_flat(self):
        reasons = flat_reasons(instance_count=25, distinct_predicates=1)
        assert "only 1 distinct relationship predicate across 25 instances" in reasons[0]

    def test_a_single_predicate_over_few_instances_is_not_flat(self):
        # A handful of instances sharing one predicate is a small graph, not a
        # flat dataset — the heuristic needs volume before it accuses.
        assert flat_reasons(instance_count=20, distinct_predicates=1) == []

    def test_predicate_diversity_clears_the_type(self):
        assert flat_reasons(instance_count=1000, distinct_predicates=2) == []
