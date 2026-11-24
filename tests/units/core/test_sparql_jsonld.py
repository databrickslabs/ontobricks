"""SPARQL CONSTRUCT/DESCRIBE JSON-LD output format."""

from __future__ import annotations

import pytest

from back.core.errors.ValidationError import ValidationError
from back.core.w3c.sparql.SparqlQueryRunner import SparqlQueryRunner


_RDF = """
@prefix ex: <http://example.org/domain#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:Asset a owl:Class .
ex:pump a ex:Asset ;
    ex:temperature "84.5"^^xsd:double ;
    ex:locatedIn ex:facB .
ex:facB a ex:Facility .
"""


def test_construct_jsonld_returns_valid_document():
    result = SparqlQueryRunner.execute_local_query(
        query="""
        PREFIX ex: <http://example.org/domain#>
        CONSTRUCT { ?s ?p ?o }
        WHERE { ?s ?p ?o }
        """,
        rdf_content=_RDF,
        limit=100,
        output_format="json-ld",
    )
    assert result["success"] is True
    assert result["format"] == "json-ld"
    assert result["count"] > 0
    assert isinstance(result["jsonld"], (dict, list))
    assert result["results"] == []


def test_construct_jsonld_respects_limit():
    result = SparqlQueryRunner.execute_local_query(
        query="""
        PREFIX ex: <http://example.org/domain#>
        CONSTRUCT { ?s ?p ?o }
        WHERE { ?s ?p ?o }
        """,
        rdf_content=_RDF,
        limit=2,
        output_format="json-ld",
    )
    assert result["count"] == 2


def test_select_jsonld_raises_validation_error():
    with pytest.raises(ValidationError, match="CONSTRUCT and DESCRIBE"):
        SparqlQueryRunner.execute_local_query(
            query="SELECT ?s WHERE { ?s ?p ?o }",
            rdf_content=_RDF,
            limit=10,
            output_format="json-ld",
        )


def test_construct_default_rows_unchanged():
    result = SparqlQueryRunner.execute_local_query(
        query="""
        PREFIX ex: <http://example.org/domain#>
        CONSTRUCT { ?s ?p ?o }
        WHERE { ?s ?p ?o }
        """,
        rdf_content=_RDF,
        limit=100,
    )
    assert "jsonld" not in result
    assert result["columns"] == ["subject", "predicate", "object"]
    assert result["count"] > 0
    assert all(
        set(row) == {"subject", "predicate", "object"} for row in result["results"]
    )
