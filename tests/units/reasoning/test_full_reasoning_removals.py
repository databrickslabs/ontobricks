"""``run_full_reasoning`` must carry each phase's removals into its result.

The async reasoning task stores ``result.to_dict()`` and the append step
reads ``removed_triples`` back from it. A phase that selects triples to hide
(IF-THEN retraction, SPARQL removal rule) is only effective if the merged
result still holds them.
"""

from unittest.mock import MagicMock

import pytest

import back.objects  # noqa: F401  (avoids a pre-existing circular import)
from back.core.reasoning.ReasoningService import ReasoningService
from back.core.reasoning.results.InferredTriple import InferredTriple
from back.core.reasoning.results.ReasoningResult import ReasoningResult

pytestmark = pytest.mark.unit

IF_THEN_REMOVED = {"subject": "http://e/m1", "predicate": "http://e/meterOf", "object": "http://e/c1"}
SPARQL_REMOVED = {"subject": "http://e/m2", "predicate": "http://e/status", "object": "closed"}
ONLY_RULES = {"tbox": False, "graph": False, "constraints": False}


def _service(if_then=None, sparql=None):
    svc = ReasoningService.__new__(ReasoningService)
    svc._store = MagicMock()
    svc._domain = None
    svc.run_if_then_rules = lambda **_kw: if_then or ReasoningResult()
    svc.run_sparql_rules = lambda **_kw: sparql or ReasoningResult()
    return svc


def _result(removed=(), inferred=()):
    result = ReasoningResult()
    result.removed_triples.extend(dict(t) for t in removed)
    result.inferred_triples.extend(inferred)
    return result


def test_if_then_retraction_survives_the_full_run():
    svc = _service(if_then=_result(removed=[IF_THEN_REMOVED]))
    result = svc.run_full_reasoning({**ONLY_RULES, "sparql_rules": False})
    assert result.removed_triples == [IF_THEN_REMOVED]


def test_sparql_removal_survives_the_full_run():
    svc = _service(sparql=_result(removed=[SPARQL_REMOVED]))
    result = svc.run_full_reasoning({**ONLY_RULES, "if_then": False, "sparql_rules": True})
    assert result.removed_triples == [SPARQL_REMOVED]


def test_removals_from_both_phases_are_combined_and_serialised():
    svc = _service(
        if_then=_result(removed=[IF_THEN_REMOVED]),
        sparql=_result(removed=[SPARQL_REMOVED]),
    )
    result = svc.run_full_reasoning({**ONLY_RULES, "sparql_rules": True})
    assert result.to_dict()["removed_triples"] == [IF_THEN_REMOVED, SPARQL_REMOVED]


def test_inferred_triples_are_still_collected():
    inferred = InferredTriple("s", "p", "o", "if_then:r", "r")
    svc = _service(if_then=_result(inferred=[inferred], removed=[IF_THEN_REMOVED]))
    result = svc.run_full_reasoning({**ONLY_RULES, "sparql_rules": False})
    assert result.inferred_triples == [inferred]
    assert result.removed_triples == [IF_THEN_REMOVED]
