"""Read-only SPARQL safety boundary."""

import pytest

from back.core.errors import ValidationError
from back.core.w3c.sparql import require_read_only_sparql


@pytest.mark.parametrize(
    "query",
    [
        "SELECT ?s WHERE { ?s ?p ?o }",
        "ASK { ?s ?p ?o }",
        "DESCRIBE <https://example.org/resource>",
        "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
    ],
)
def test_read_only_forms_are_returned_stripped(query):
    assert require_read_only_sparql(f"  {query}\n") == query


@pytest.mark.parametrize(
    "query",
    [
        "SELECT ?add WHERE { ?s ?p ?add }",
        'SELECT ?s WHERE { ?s ?p ?o . FILTER(CONTAINS(STR(?o), "DELETE")) }',
        "PREFIX ont: <https://example.com/> SELECT ?s WHERE { ?s ont:add ?o }",
    ],
)
def test_mutation_words_in_valid_queries_are_not_treated_as_updates(query):
    assert require_read_only_sparql(f"\n{query}  ") == query


@pytest.mark.parametrize(
    "query",
    [
        "DROP GRAPH <g>",
        "DELETE WHERE { ?s ?p ?o }",
        "INSERT DATA { <a> <b> <c> }",
        "CREATE GRAPH <g>",
        "CLEAR ALL",
        "LOAD <https://example.org/data>",
        "COPY GRAPH <a> TO GRAPH <b>",
        "MOVE GRAPH <a> TO GRAPH <b>",
        "ADD GRAPH <a> TO GRAPH <b>",
        "delete where { ?s ?p ?o }",
        (
            "PREFIX ont: <https://example.com/> "
            "INSERT DATA { ont:subject ont:predicate ont:object }"
        ),
        (
            "WITH <https://example.com/graph> "
            "DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }"
        ),
    ],
)
def test_mutating_forms_raise_validation_error(query):
    with pytest.raises(ValidationError, match="read-only"):
        require_read_only_sparql(query)


@pytest.mark.parametrize("query", ["", "   ", None])
def test_missing_query_raises_validation_error(query):
    with pytest.raises(ValidationError, match="No SPARQL query"):
        require_read_only_sparql(query)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT WHERE {",
        "this is neither a SPARQL query nor an update",
    ],
)
def test_malformed_input_is_rejected_without_being_mislabelled_as_mutation(query):
    with pytest.raises(ValidationError, match="Invalid SPARQL query") as exc_info:
        require_read_only_sparql(query)

    assert "read-only" not in str(exc_info.value)
