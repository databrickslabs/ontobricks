"""Unit tests for Spark SPARQL capability validation."""

from __future__ import annotations

import pytest

from back.core.errors import ValidationError
from back.core.w3c.sparql.SparqlCapabilityValidator import SparqlCapabilityValidator


def test_accepts_simple_select() -> None:
    query = "SELECT ?s WHERE { ?s <http://ex/p> ?o }"
    SparqlCapabilityValidator.validate(query)


def test_rejects_invalid_syntax() -> None:
    with pytest.raises(ValidationError, match="Invalid SPARQL query."):
        SparqlCapabilityValidator.validate("SELECT WHERE { ?s ?p ?o")


def test_accepts_limit_clause() -> None:
    query = "SELECT ?s WHERE { ?s <http://ex/p> ?o } LIMIT 10"
    SparqlCapabilityValidator.validate(query)


def test_accepts_optional_pattern_left_join_shape() -> None:
    query = (
        "SELECT ?s ?label WHERE { "
        "?s <http://ex/p> ?o . "
        "OPTIONAL { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label } "
        "}"
    )
    SparqlCapabilityValidator.validate(query)


@pytest.mark.parametrize(
    "query",
    [
        (
            "SELECT ?s WHERE { "
            "?s <http://ex/name> ?name "
            "FILTER(CONTAINS(LCASE(STR(?name)), \"ann\")) "
            "}"
        ),
        (
            "SELECT ?s WHERE { "
            "?s <http://ex/name> ?name "
            "FILTER(STR(?name) = \"Ann\") "
            "}"
        ),
        (
            "SELECT ?s WHERE { "
            "?s <http://ex/name> ?name "
            "FILTER(STRSTARTS(LCASE(STR(?name)), \"an\")) "
            "}"
        ),
        (
            "SELECT ?s WHERE { "
            "?s <http://ex/name> ?name "
            "FILTER(STRENDS(LCASE(STR(?name)), \"nn\")) "
            "}"
        ),
        (
            "SELECT ?s ?predicate WHERE { "
            "?s ?predicate ?o "
            "FILTER(?predicate IN (<http://ex/p>, <http://ex/q>)) "
            "}"
        ),
    ],
)
def test_accepts_supported_filter_shapes(query: str) -> None:
    SparqlCapabilityValidator.validate(query)


def test_accepts_specialized_relationship_union_shape() -> None:
    query = (
        "SELECT ?subject ?predicate ?object WHERE { "
        "{ ?subject <http://ex/worksWith> ?object . "
        "BIND(<http://ex/worksWith> AS ?predicate) } "
        "UNION "
        "{ ?subject <http://ex/manages> ?object . "
        "BIND(<http://ex/manages> AS ?predicate) } "
        "}"
    )
    SparqlCapabilityValidator.validate(query)


@pytest.mark.parametrize(
    ("query", "feature"),
    [
        (
            "SELECT ?s (COUNT(?o) AS ?n) WHERE { ?s <http://ex/p> ?o } GROUP BY ?s",
            "GROUP BY",
        ),
        ("SELECT ?s WHERE { ?s <http://ex/p> ?o } ORDER BY ?s", "ORDER BY"),
        ("SELECT ?s WHERE { ?s <http://ex/p> ?o FILTER(?o > 5) }", "numeric FILTER"),
        ("SELECT ?s WHERE { ?s <http://ex/p>+ ?o }", "property paths"),
        (
            "SELECT ?s WHERE { ?s <http://ex/p> ?o . { SELECT ?o WHERE { ?o ?x ?y } } }",
            "subquery",
        ),
        (
            "SELECT ?s WHERE { ?s <http://ex/p> ?o MINUS { ?s <http://ex/q> ?o } }",
            "MINUS",
        ),
        ("SELECT ?s WHERE { ?s <http://ex/p> ?o } VALUES ?s { <http://ex/a> }", "VALUES"),
        ("SELECT ?s WHERE { SERVICE <http://ex/sparql> { ?s ?p ?o } }", "SERVICE"),
        ("SELECT ?s WHERE { GRAPH <http://ex/g> { ?s ?p ?o } }", "GRAPH"),
        (
            "SELECT ?s WHERE { { ?s <http://ex/p> ?o } UNION { ?s <http://ex/q> ?o } }",
            "UNION",
        ),
        ("SELECT ?s WHERE { ?s <http://ex/p> ?o BIND(STR(?o) AS ?x) }", "non-literal BIND"),
        ("SELECT ?s WHERE { ?s <http://ex/p> ?o } OFFSET 5", "OFFSET"),
    ],
)
def test_rejects_unsupported_constructs(query: str, feature: str) -> None:
    with pytest.raises(ValidationError, match=feature):
        SparqlCapabilityValidator.validate(query)
