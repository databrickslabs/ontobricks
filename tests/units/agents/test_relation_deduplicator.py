"""Semantic deduplication of Stage-3 Generate relations.

Paraphrases, passive voice, and inverse restatements of one fact collapse to
a single active-voice relation. Distinct predicates on the same pair of
entities are kept. Short/ambiguous stems are kept rather than collapsed.
"""

from __future__ import annotations

import pytest

from agents.agent_owl_generator.RelationDeduplicator import RelationDeduplicator

pytestmark = pytest.mark.unit


def _rel(label, domain, range_):
    return {"label": label, "domain": domain, "range": range_}


def _labels(rels):
    return [r["label"] for r in rels]


class TestSemanticCollapse:
    def test_passive_paraphrases_same_direction_collapse(self):
        rels = [
            _rel("settledBy", "Contract", "Payment"),
            _rel("isSettledBy", "Contract", "Payment"),
        ]
        assert len(RelationDeduplicator.dedupe(rels)) == 1

    def test_inverse_restatement_keeps_active_voice(self):
        rels = [
            _rel("settledBy", "Contract", "Payment"),
            _rel("settles", "Payment", "Contract"),
            _rel("isSettledBy", "Contract", "Payment"),
        ]
        kept = RelationDeduplicator.dedupe(rels)
        assert kept == [_rel("settles", "Payment", "Contract")]

    def test_handles_and_handled_collapse(self):
        rels = [
            _rel("handled", "Claim", "Agent"),
            _rel("handles", "Agent", "Claim"),
        ]
        assert RelationDeduplicator.dedupe(rels) == [_rel("handles", "Agent", "Claim")]

    def test_base_form_matches_inflected_form(self):
        rels = [
            _rel("places", "Customer", "Order"),
            _rel("place", "Customer", "Order"),
        ]
        assert len(RelationDeduplicator.dedupe(rels)) == 1

    def test_self_relation_paraphrases_collapse(self):
        rels = [
            _rel("manages", "Employee", "Employee"),
            _rel("managedBy", "Employee", "Employee"),
        ]
        assert _labels(RelationDeduplicator.dedupe(rels)) == ["manages"]


class TestDistinctFactsKept:
    def test_distinct_predicates_same_pair_kept(self):
        rels = [
            _rel("places", "Customer", "Order"),
            _rel("cancels", "Customer", "Order"),
        ]
        assert _labels(RelationDeduplicator.dedupe(rels)) == ["places", "cancels"]

    def test_distinct_predicates_opposite_directions_kept(self):
        rels = [
            _rel("issues", "Company", "Invoice"),
            _rel("references", "Invoice", "Company"),
        ]
        assert len(RelationDeduplicator.dedupe(rels)) == 2

    def test_short_stems_are_not_collapsed(self):
        rels = [
            _rel("owns", "Agent", "Asset"),
            _rel("owned", "Asset", "Agent"),
        ]
        assert len(RelationDeduplicator.dedupe(rels)) == 2

    def test_same_label_different_pairs_kept(self):
        rels = [
            _rel("settles", "Payment", "Contract"),
            _rel("settles", "Payment", "Invoice"),
        ]
        assert len(RelationDeduplicator.dedupe(rels)) == 2

    def test_empty_list(self):
        assert RelationDeduplicator.dedupe([]) == []


class TestExistingRelations:
    def test_paraphrase_of_existing_relation_dropped(self):
        existing = [_rel("settles", "Payment", "Contract")]
        rels = [
            _rel("isSettledBy", "Contract", "Payment"),
            _rel("covers", "Contract", "Customer"),
        ]
        assert RelationDeduplicator.dedupe(rels, existing=existing) == [
            _rel("covers", "Contract", "Customer")
        ]

    def test_existing_relations_are_not_returned(self):
        existing = [_rel("settles", "Payment", "Contract")]
        assert RelationDeduplicator.dedupe([], existing=existing) == []
