"""Complex rules: SPARQL CONSTRUCT compiled to SQL on the flat triple table.

Rules without numeric filters are executed for real on an in-memory SQLite
triple table (with the Postgres dialect, whose quoting SQLite shares), so the
tests assert the inferred triples rather than the SQL text.
"""

import sqlite3

# Importing the package root first avoids a pre-existing circular import
# between ``back.core.reasoning`` and ``back.core.w3c``.
import back.objects  # noqa: F401
import pytest

from back.core.errors import ValidationError
from back.core.reasoning.rules.SparqlConstructCompiler import SparqlConstructCompiler

pytestmark = pytest.mark.unit

NS = "http://t.org/onto#"
DATA = "http://t.org/onto/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
URI_MAP = {"total": DATA + "total", "paid": DATA + "paid", "name": DATA + "name"}

TRIPLES = [
    ("o1", RDF_TYPE, NS + "Order"),
    ("o1", DATA + "name", "big_box"),
    ("o1", DATA + "paid", "p1"),
    ("p1", RDF_TYPE, NS + "Payment"),
    ("o2", RDF_TYPE, NS + "Order"),
    ("o2", DATA + "name", "bigger"),
    ("o3", RDF_TYPE, NS + "Order"),
    ("o3", DATA + "name", "small"),
    ("o3", RDF_TYPE, NS + "Flagged"),
    ("c1", RDF_TYPE, NS + "Customer"),
]


def _compiler(dialect="postgres"):
    return SparqlConstructCompiler(base_uri=NS, uri_map=URI_MAP, dialect=dialect)


def _infer(query, triples=TRIPLES):
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE g (subject TEXT, predicate TEXT, object TEXT)")
    db.executemany("INSERT INTO g VALUES (?, ?, ?)", triples)
    return set(db.execute(_compiler().compile(query, "g")).fetchall())


def _flagged(*subjects):
    return {(s, RDF_TYPE, NS + "Flagged") for s in subjects}


class TestInference:
    def test_type_pattern_infers_a_class(self):
        assert _infer("CONSTRUCT { ?o a :Flagged } WHERE { ?o a :Order }") == _flagged(
            "o1", "o2"
        )

    def test_already_known_triples_are_not_returned(self):
        assert ("o3", RDF_TYPE, NS + "Flagged") not in _infer(
            "CONSTRUCT { ?o a :Flagged } WHERE { ?o a :Order }"
        )

    def test_every_head_triple_is_inferred(self):
        out = _infer(
            'CONSTRUCT { ?o a :Flagged . ?o :status "seen" } '
            "WHERE { ?o a :Order . ?o :paid ?p }"
        )
        assert out == {
            ("o1", RDF_TYPE, NS + "Flagged"),
            ("o1", NS + "status", "seen"),
        }

    def test_join_through_a_shared_variable(self):
        out = _infer(
            "CONSTRUCT { ?p :settles ?o } WHERE { ?o a :Order . ?o :paid ?p . ?p a :Payment }"
        )
        assert out == {("p1", NS + "settles", "o1")}

    def test_not_exists_with_several_triples(self):
        out = _infer(
            "CONSTRUCT { ?o a :Flagged } WHERE { ?o a :Order "
            "FILTER NOT EXISTS { ?o :paid ?p . ?p a :Payment } }"
        )
        assert out == _flagged("o2")

    @pytest.mark.parametrize(
        "function, value, expected",
        [
            ("CONTAINS", "_", {"o1"}),
            ("STRSTARTS", "big", {"o1", "o2"}),
            ("STRENDS", "er", {"o2"}),
        ],
    )
    def test_string_filters_match_literally(self, function, value, expected):
        out = _infer(
            "CONSTRUCT { ?o a :Flagged } WHERE { ?o a :Order . ?o :name ?n "
            f'FILTER({function}(?n, "{value}")) }}'
        )
        assert out == _flagged(*expected)

    def test_or_and_equality_filters(self):
        out = _infer(
            "CONSTRUCT { ?o a :Flagged } WHERE { ?o a :Order . ?o :name ?n "
            'FILTER(?n = "bigger" || ?n = "big_box") }'
        )
        assert out == _flagged("o1", "o2")

    def test_declared_prefix_keeps_its_iri(self):
        out = _infer(
            "PREFIX ex: <http://other.org/> "
            "CONSTRUCT { ?o a ex:Seen } WHERE { ?o a :Order . ?o :paid ?p }"
        )
        assert out == {("o1", RDF_TYPE, "http://other.org/Seen")}


class TestSqlShape:
    def test_numeric_filter_casts_on_spark(self):
        sql = _compiler("spark").compile(
            "CONSTRUCT { ?o a :Big } WHERE { ?o a :Order . ?o :total ?t FILTER(?t > 10000) }",
            "g",
        )
        assert "TRY_CAST(t2.object AS DOUBLE) > 10000" in sql

    def test_numeric_filter_has_no_try_cast_on_postgres(self):
        sql = _compiler("postgres").compile(
            "CONSTRUCT { ?o a :Big } WHERE { ?o a :Order . ?o :total ?t FILTER(10000 < ?t) }",
            "g",
        )
        assert "TRY_CAST" not in sql
        assert "::float8 END) > 10000" in sql


class TestFailClosed:
    @pytest.mark.parametrize(
        "query, reason",
        [
            ("SELECT * WHERE { ?s ?p ?o }", "CONSTRUCT"),
            ("garbage", "Invalid SPARQL"),
            ("CONSTRUCT { ?o a :X } WHERE { ?o a :Order OPTIONAL { ?o :n ?n } }", "OPTIONAL"),
            ("CONSTRUCT { ?o a :X } WHERE { { ?o a :A } UNION { ?o a :B } }", "UNION"),
            ("CONSTRUCT { ?o a :X } WHERE { ?o a :Order . BIND(1 AS ?k) }", "BIND"),
            ("CONSTRUCT { ?o a :X } WHERE { ?o a :Order } LIMIT 5", "LIMIT"),
            ("CONSTRUCT { ?o a :X } WHERE { ?o :p/:q ?z }", "property paths"),
            ("CONSTRUCT { ?o a :X } WHERE { ?o ?p ?z }", "variable predicates"),
            ("CONSTRUCT { ?o a :X } WHERE { ?o a :Order . ?z a :Customer }", "not connected"),
            ("CONSTRUCT { ?q a :X } WHERE { ?o a :Order }", "?q"),
            ("CONSTRUCT { ?o a :X } WHERE { ?o a :Order FILTER(?t > 3) }", "?t"),
            ("CONSTRUCT { ?o a :X } WHERE { ?o :t ?a . ?o :u ?b FILTER(?a < ?b) }", "two variables"),
            ("CONSTRUCT { ?o a :X } WHERE { ?o :t ?a FILTER(?a > 1 + 2) }", "computed"),
            ("CONSTRUCT { ?o a :X } WHERE { ?o a :O FILTER EXISTS { ?o :p ?v } }", "EXISTS"),
        ],
    )
    def test_unsupported_query_is_rejected(self, query, reason):
        with pytest.raises(ValidationError) as exc:
            _compiler().compile(query, "g")
        assert reason in exc.value.message
