"""Tests for the OWL 2 RL reasoner and reasoning models."""

import importlib

import pytest

from back.core.reasoning.models import InferredTriple, ReasoningResult, RuleViolation

_has_owlrl = importlib.util.find_spec("owlrl") is not None
requires_owlrl = pytest.mark.skipif(not _has_owlrl, reason="owlrl not installed")


# -- Model tests ----------------------------------------------------------


class TestReasoningModels:
    def test_inferred_triple_creation(self):
        t = InferredTriple(
            subject="http://ex.org/A",
            predicate="http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            object="http://ex.org/B",
            provenance="owlrl",
        )
        assert t.subject == "http://ex.org/A"
        assert t.rule_name == ""

    def test_rule_violation_creation(self):
        v = RuleViolation(
            rule_name="test_rule",
            subject="http://ex.org/X",
            message="failed",
            check_type="swrl",
        )
        assert v.check_type == "swrl"

    def test_reasoning_result_merge(self):
        r1 = ReasoningResult(
            inferred_triples=[
                InferredTriple("s1", "p1", "o1", "owlrl"),
            ],
            stats={"count": 1},
        )
        r2 = ReasoningResult(
            inferred_triples=[
                InferredTriple("s2", "p2", "o2", "swrl"),
            ],
            violations=[
                RuleViolation("r", "s", "m", "swrl"),
            ],
            stats={"count": 2},
        )
        r1.merge(r2)
        assert len(r1.inferred_triples) == 2
        assert len(r1.violations) == 1
        assert r1.stats["count"] == 3

    def test_reasoning_result_to_dict(self):
        r = ReasoningResult(
            inferred_triples=[
                InferredTriple("s1", "p1", "o1", "owlrl", "rule1"),
            ],
            violations=[
                RuleViolation("r1", "s1", "msg", "swrl"),
            ],
            stats={"duration": 1.2},
        )
        d = r.to_dict()
        assert len(d["inferred_triples"]) == 1
        assert d["inferred_triples"][0]["provenance"] == "owlrl"
        assert len(d["violations"]) == 1
        assert d["stats"]["duration"] == 1.2


# -- OWL RL Reasoner tests -----------------------------------------------

SMALL_ONTOLOGY = """
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex: <http://example.org/> .

ex:Animal a owl:Class .
ex:Dog a owl:Class ;
    rdfs:subClassOf ex:Animal .
ex:Labrador a owl:Class ;
    rdfs:subClassOf ex:Dog .

ex:hasSibling a owl:ObjectProperty, owl:SymmetricProperty .
ex:isPartOf a owl:ObjectProperty, owl:TransitiveProperty .
"""


@requires_owlrl
class TestOWLRLReasoner:
    def test_compute_closure_basic(self):
        from back.core.reasoning.OWLRLReasoner import OWLRLReasoner

        reasoner = OWLRLReasoner()
        result = reasoner.compute_closure(SMALL_ONTOLOGY)

        assert isinstance(result, ReasoningResult)
        assert result.stats["phase"] == "tbox"
        assert result.stats["original_count"] > 0
        assert result.stats["after_count"] >= result.stats["original_count"]

    def test_closure_infers_subclass_chain(self):
        from back.core.reasoning.OWLRLReasoner import OWLRLReasoner

        reasoner = OWLRLReasoner()
        result = reasoner.compute_closure(SMALL_ONTOLOGY)

        subjects = {(t.subject, t.predicate, t.object) for t in result.inferred_triples}
        subclass_of = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
        assert (
            "http://example.org/Labrador",
            subclass_of,
            "http://example.org/Animal",
        ) in subjects, "Labrador should be inferred as subClassOf Animal"

    def test_compute_closure_with_instances(self):
        from back.core.reasoning.OWLRLReasoner import OWLRLReasoner

        reasoner = OWLRLReasoner()
        instances = [
            {
                "subject": "http://example.org/fido",
                "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "object": "http://example.org/Labrador",
            }
        ]
        result = reasoner.compute_closure_with_instances(SMALL_ONTOLOGY, instances)
        assert result.stats["phase"] == "tbox_instances"

        types = {
            t.object
            for t in result.inferred_triples
            if t.subject == "http://example.org/fido"
            and t.predicate == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        }
        assert "http://example.org/Dog" in types or "http://example.org/Animal" in types

    def test_empty_ontology(self):
        from back.core.reasoning.OWLRLReasoner import OWLRLReasoner

        reasoner = OWLRLReasoner()
        result = reasoner.compute_closure(
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
        )
        assert isinstance(result, ReasoningResult)
