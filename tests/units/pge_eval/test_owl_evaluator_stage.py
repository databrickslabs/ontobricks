"""The owl-generator Evaluator stage (§3.5) — deterministic Stage-1 checks
feeding retry_hints, with a bounded retry cap."""

from agents.agent_owl_generator import engine as owl_engine

_CLEAN_TTL = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://test.org/o#> .

<http://test.org/o> a owl:Ontology .

:Customer a owl:Class ; rdfs:label "Customer" .
:Order a owl:Class ; rdfs:label "Order" .

:placesOrder a owl:ObjectProperty ; rdfs:domain :Customer ; rdfs:range :Order .
:firstName a owl:DatatypeProperty ; rdfs:domain :Customer ; rdfs:range xsd:string .
:orderDate a owl:DatatypeProperty ; rdfs:domain :Order ; rdfs:range xsd:string .
"""

_ORPHAN_TTL = _CLEAN_TTL + """
:Ghost a owl:Class ; rdfs:label "Ghost" .
"""


def test_clean_ontology_returns_no_retry_hint():
    assert owl_engine._evaluate_ontology_stage(_CLEAN_TTL, {}, 1) is None


def test_orphan_class_yields_retry_hint():
    hint = owl_engine._evaluate_ontology_stage(_ORPHAN_TTL, {}, 1)
    assert hint is not None
    assert "Ghost" in hint
    assert "orphan" in hint.lower()


_PROSE_PREFIXED = (
    "No database tables are available. I have what I need from the guidelines.\n\n"
    + _ORPHAN_TTL
)

_FENCED = "```turtle\n" + _ORPHAN_TTL + "```"


def test_prose_preamble_is_stripped_before_parsing():
    # Regression: the model sometimes prepends a sentence before @prefix. The
    # evaluator must clean it (like the downstream registry) and still run,
    # not skip. Found via a live Chrome DevTools generation run.
    hint = owl_engine._evaluate_ontology_stage(_PROSE_PREFIXED, {}, 1)
    assert hint is not None
    assert "Ghost" in hint


def test_markdown_fenced_turtle_is_parsed():
    hint = owl_engine._evaluate_ontology_stage(_FENCED, {}, 1)
    assert hint is not None
    assert "Ghost" in hint


def test_parse_error_fails_open():
    # Garbage in -> None (never blocks OWL delivery).
    assert owl_engine._evaluate_ontology_stage("not turtle at all {{{", {}, 1) is None


def test_evaluator_loop_is_bounded():
    # The Evaluator retry cap exists and is finite (real PGE discipline).
    assert owl_engine.MAX_OWL_EVAL_ROUNDS >= 1
    assert owl_engine.MAX_OWL_EVAL_ROUNDS < 10
