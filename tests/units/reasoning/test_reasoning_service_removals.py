"""``ReasoningService.materialize_inferred`` applies removal rules after the inserts."""

from unittest.mock import MagicMock

from back.core.graphdb.overrides import ORIGIN_REASONING
from back.core.reasoning.ReasoningService import ReasoningService
from back.core.reasoning.results.InferredTriple import InferredTriple
from back.core.reasoning.results.ReasoningResult import ReasoningResult

T = {"subject": "http://e/m1", "predicate": "http://e/p", "object": "http://e/X"}


def _service(store):
    svc = ReasoningService.__new__(ReasoningService)
    svc._store = store
    svc._get_graph_name = lambda: "graph"
    svc._domain = None
    return svc


def test_removals_are_applied_with_reasoning_origin():
    store = MagicMock()
    store.insert_triples.return_value = 0
    result = ReasoningResult()
    result.removed_triples.append(T)
    _service(store).materialize_inferred(result)
    store.delete_triples.assert_called_once()
    args, kwargs = store.delete_triples.call_args
    assert kwargs["origin"] == ORIGIN_REASONING
    assert args[1] == [T]


def test_removals_run_even_without_inferred_triples():
    store = MagicMock()
    result = ReasoningResult()
    result.removed_triples.append(T)
    _service(store).materialize_inferred(result)
    store.insert_triples.assert_not_called()
    store.delete_triples.assert_called_once()


def test_removals_run_after_inserts():
    calls = []
    store = MagicMock()
    store.insert_triples.side_effect = lambda *a, **k: calls.append("insert") or 1
    store.delete_triples.side_effect = lambda *a, **k: calls.append("delete") or 1
    result = ReasoningResult()
    result.inferred_triples.append(
        InferredTriple(T["subject"], T["predicate"], T["object"], "test")
    )
    result.removed_triples.append(T)
    _service(store).materialize_inferred(result)
    assert calls == ["insert", "delete"]


def test_duplicate_removals_are_collapsed():
    store = MagicMock()
    result = ReasoningResult()
    result.removed_triples.extend([T, dict(T)])
    _service(store).materialize_inferred(result)
    args, _ = store.delete_triples.call_args
    assert len(args[1]) == 1


def test_insert_count_is_returned_not_the_removal_count():
    store = MagicMock()
    store.insert_triples.return_value = 3
    store.delete_triples.return_value = 7
    result = ReasoningResult()
    result.inferred_triples.append(InferredTriple("s", "p", "o", "test"))
    result.removed_triples.append(T)
    assert _service(store).materialize_inferred(result) == 3


def test_no_store_is_a_no_op():
    result = ReasoningResult()
    result.removed_triples.append(T)
    assert _service(None).materialize_inferred(result) == 0
