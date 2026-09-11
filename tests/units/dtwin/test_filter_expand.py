"""Contracts for optimized and fallback Explorer graph expansion."""

from unittest.mock import MagicMock

import pytest

from back.objects.digitaltwin.DigitalTwin import DigitalTwin


class OptimizedStore:
    def __init__(self):
        self.calls = []

    def expand_and_fetch_subgraph(self, *args):
        self.calls.append(args)
        return {
            "results": [{"subject": "s", "predicate": "p", "object": "o"}],
            "count": 1,
            "expanded_count": 2,
            "capped": False,
            "timeout_capped": False,
        }

    @staticmethod
    def expand_entity_neighbors(*_):
        raise AssertionError("iterative expansion must not run")

    @staticmethod
    def get_triples_for_subjects(*_):
        raise AssertionError("batched fetch must not run")


def test_filter_expand_uses_single_statement_capability():
    store = OptimizedStore()

    result = DigitalTwin.filter_expand(
        store,
        "graph",
        ["seed"],
        depth=2,
        max_entities=50,
        max_triples=100,
        batch_size=10,
    )

    assert len(store.calls) == 1
    assert result["initial_count"] == 1
    assert result["expanded_count"] == 2
    assert result["phase"] == "expand"


def test_filter_expand_disables_traversal_when_relationships_are_excluded():
    store = OptimizedStore()

    DigitalTwin.filter_expand(
        store,
        "graph",
        ["seed"],
        include_rels=False,
        depth=2,
        max_entities=50,
        max_triples=100,
        batch_size=10,
    )

    assert store.calls[0][2] == 0


def test_filter_expand_keeps_iterative_fallback():
    store = MagicMock(
        spec=["expand_entity_neighbors", "get_triples_for_subjects"]
    )
    store.expand_entity_neighbors.return_value = {"neighbor"}
    store.get_triples_for_subjects.return_value = [
        {"subject": "seed", "predicate": "p", "object": "neighbor"}
    ]

    result = DigitalTwin.filter_expand(
        store,
        "graph",
        ["seed"],
        depth=1,
        max_entities=50,
        max_triples=100,
        batch_size=10,
    )

    store.expand_entity_neighbors.assert_called_once()
    store.get_triples_for_subjects.assert_called_once()
    assert result["initial_count"] == 1


def test_filter_expand_does_not_retry_after_single_statement_failure():
    store = OptimizedStore()
    store.expand_and_fetch_subgraph = MagicMock(
        side_effect=RuntimeError("timeout")
    )

    with pytest.raises(RuntimeError, match="timeout"):
        DigitalTwin.filter_expand(
            store,
            "graph",
            ["seed"],
            depth=2,
            max_entities=50,
            max_triples=100,
            batch_size=10,
        )

    store.expand_and_fetch_subgraph.assert_called_once()
