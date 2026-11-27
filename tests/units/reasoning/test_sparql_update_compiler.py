import pytest

from back.core.errors import ValidationError
from back.core.reasoning.rules.SparqlUpdateCompiler import SparqlUpdateCompiler

PFX = "PREFIX : <http://ex.org/onto#>\n"
BASE = "http://ex.org/onto#"


def _compile(query: str) -> str:
    return SparqlUpdateCompiler(base_uri=BASE).compile_removal(query, "triples")


def test_delete_where_compiles_to_select_over_triples():
    sql = _compile(PFX + "DELETE WHERE { ?m :meterOf :Customer_42 . ?m :status ?o }")
    assert "SELECT DISTINCT" in sql
    assert "FROM triples" in sql
    assert "Customer_42" in sql


def test_delete_data_compiles_to_one_select_per_triple():
    sql = _compile(PFX + "DELETE DATA { :m1 :meterOf :Customer_42 . :m2 :meterOf :Customer_42 }")
    assert sql.count("UNION ALL") == 1
    assert "m1" in sql and "m2" in sql


def test_delete_template_with_where_compiles():
    sql = _compile(
        PFX + "DELETE { ?m :status ?o } WHERE { ?m :meterOf :Customer_42 . ?m :status ?o }"
    )
    assert "SELECT DISTINCT" in sql


def test_delete_template_with_where_and_filter_compiles():
    sql = _compile(
        PFX
        + "DELETE { ?m :reading ?v } WHERE { ?m :meterOf :Customer_42 . ?m :reading ?v . "
        + "FILTER(?v > 10) }"
    )
    assert "SELECT DISTINCT" in sql


@pytest.mark.parametrize(
    "query",
    [
        PFX + "DELETE WHERE { ?s ?p ?o }",  # variable predicate
        PFX + "DELETE WHERE { ?s :p ?o }",  # whole-predicate wipe (D4)
        PFX + "INSERT DATA { :a :b :c }",  # insert forms are not removals
        PFX + "DELETE { ?s :p ?o } INSERT { ?s :p :x } WHERE { ?s :p ?o . ?s :q :X }",
        PFX + "DELETE WHERE { GRAPH :g { ?s :p :X } }",  # named graphs
        PFX + "DELETE WHERE { ?s :p/:q :X }",  # property paths
        PFX + "CONSTRUCT { ?s :p ?o } WHERE { ?s :p ?o . ?s :q :X }",  # not an update
    ],
)
def test_rejected_forms_raise_validation_error(query):
    with pytest.raises(ValidationError):
        _compile(query)


def test_allowed_wipe_with_bound_object_is_accepted():
    sql = _compile(PFX + "DELETE WHERE { ?s :p :X }")
    assert "SELECT DISTINCT" in sql


def test_data_triples_returns_resolved_constants():
    triples = SparqlUpdateCompiler(base_uri=BASE).data_triples(
        PFX + "DELETE DATA { :m1 :meterOf :Customer_42 }"
    )
    assert triples == [(BASE + "m1", BASE + "meterOf", BASE + "Customer_42")]


def test_data_triples_rejects_other_forms():
    with pytest.raises(ValidationError):
        SparqlUpdateCompiler(base_uri=BASE).data_triples(
            PFX + "DELETE WHERE { ?m :meterOf :Customer_42 }"
        )
