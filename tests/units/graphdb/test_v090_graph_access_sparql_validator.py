"""0.9.0 SPARQL playground: fail-closed read-only SELECT capability validation."""

from __future__ import annotations

import pytest
from rdflib.paths import SequencePath
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib.term import BNode, Literal, URIRef, Variable

from back.core.errors import ValidationError
from back.core.w3c.sparql.SparqlCapabilityValidator import SparqlCapabilityValidator as V

pytestmark = pytest.mark.unit

EX = "PREFIX ex: <http://ex.org/> "


def _reject(query: str) -> str:
    with pytest.raises(ValidationError) as exc:
        V.validate(EX + query)
    return exc.value.message


class TestAcceptedSelects:
    @pytest.mark.parametrize(
        "query",
        [
            "SELECT ?s ?o WHERE { ?s ex:p ?o }",
            "SELECT * WHERE { ?s ex:p ?o }",
            "SELECT DISTINCT ?s WHERE { ?s ex:p ?o }",
            "SELECT ?s WHERE { ?s ex:p ?o } LIMIT 10",
            "SELECT ?s WHERE { ?s ex:p ?o } LIMIT 10 OFFSET 0",
            "SELECT ?s ?l WHERE { ?s ex:p ?o OPTIONAL { ?s ex:l ?l } }",
            "SELECT ?s WHERE { ?s ex:p ?o FILTER(STR(?o) = 'x') }",
            "SELECT ?s WHERE { ?s ex:p ?o FILTER(CONTAINS(LCASE(STR(?o)), 'x')) }",
            "SELECT ?s WHERE { ?s ex:p ?o FILTER(STRSTARTS(LCASE(STR(?o)), 'x')) }",
            "SELECT ?s WHERE { ?s ex:p ?o FILTER(STRENDS(LCASE(STR(?o)), 'x')) }",
            "SELECT ?s WHERE { ?s ?p ?o FILTER(?p IN (ex:a, ex:b)) }",
            "SELECT ?s ?x WHERE { ?s ex:p ?o BIND('a' AS ?x) }",
            "SELECT ?s WHERE { ?s ex:p 'literal' }",
            (
                "SELECT ?subject ?predicate ?object WHERE { "
                "{ ?subject ex:a ?object BIND(ex:a AS ?predicate) } UNION "
                "{ ?subject ex:b ?object BIND(ex:b AS ?predicate) } UNION "
                "{ ?subject ex:c ?object BIND(ex:c AS ?predicate) } }"
            ),
        ],
    )
    def test_supported_select_passes(self, query):
        V.validate(EX + query)


class TestReadOnlyAndQueryForm:
    @pytest.mark.parametrize(
        "query",
        [
            "INSERT DATA { <http://a> <http://b> <http://c> }",
            "DELETE WHERE { ?s ?p ?o }",
            "DROP GRAPH <http://g>",
            "SELEC nope",
        ],
    )
    def test_updates_and_garbage_are_invalid(self, query):
        with pytest.raises(ValidationError) as exc:
            V.validate(query)
        assert exc.value.message == "Invalid SPARQL query."
        assert exc.value.detail

    @pytest.mark.parametrize(
        "query",
        [
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
            "ASK { ?s ?p ?o }",
            "DESCRIBE <http://a>",
        ],
    )
    def test_non_select_forms_fail_closed(self, query):
        assert _reject(query) == "Spark SPARQL does not support unsupported SPARQL construct."


class TestUnsupportedFeatures:
    @pytest.mark.parametrize(
        "query,feature",
        [
            ("SELECT ?s WHERE { ?s ex:p ?o } LIMIT 10 OFFSET 5", "OFFSET"),
            ("SELECT ?s WHERE { ?s ex:p ?o } ORDER BY ?s", "ORDER BY"),
            ("SELECT ?s (COUNT(?o) AS ?c) WHERE { ?s ex:p ?o } GROUP BY ?s", "GROUP BY"),
            ("SELECT (COUNT(?o) AS ?c) WHERE { ?s ex:p ?o }", "GROUP BY"),
            ("SELECT ?s WHERE { ?s ex:p ?o MINUS { ?s ex:q ?o } }", "MINUS"),
            ("SELECT ?s WHERE { GRAPH ex:g { ?s ex:p ?o } }", "GRAPH"),
            ("SELECT ?s WHERE { SERVICE <http://x/sparql> { ?s ex:p ?o } }", "SERVICE"),
            ("SELECT ?s WHERE { VALUES ?s { ex:a } ?s ex:p ?o }", "VALUES"),
            ("SELECT ?s WHERE { VALUES ?s { ex:a } }", "VALUES"),
            ("SELECT ?s WHERE { ?s ex:p ?o { SELECT ?s WHERE { ?s ex:q ?x } } }", "subquery"),
            ("SELECT ?s WHERE { { SELECT ?s WHERE { ?s ex:q ?x } } }", "subquery"),
            ("SELECT ?s WHERE { ?s ex:p/ex:q ?o }", "property paths"),
            ("SELECT ?s WHERE { ?s ex:p ?o FILTER(?o > 3) }", "numeric FILTER"),
            ("SELECT ?s WHERE { ?s ex:p ?o FILTER(?o <= 3) }", "numeric FILTER"),
            ("SELECT ?s WHERE { ?s ex:p ?o FILTER(STR(?o) != 'x') }", "complex FILTER"),
            ("SELECT ?s WHERE { ?s ex:p ?o FILTER(CONTAINS(STR(?o), 'x')) }", "complex FILTER"),
            ("SELECT ?s WHERE { ?s ex:p ?o FILTER(BOUND(?o)) }", "complex FILTER"),
            ("SELECT ?s WHERE { ?s ex:p ?o FILTER(?o) }", "complex FILTER"),
            ("SELECT ?s WHERE { ?s ?p ?o FILTER(?p IN (ex:a, 'b')) }", "complex FILTER"),
            ("SELECT ?s WHERE { ?s ?q ?o FILTER(?q IN (ex:a)) }", "complex FILTER"),
            ("SELECT ?s WHERE { ?s ex:p ?o FILTER EXISTS { ?s ex:q ?x } }", "complex FILTER"),
            ("SELECT ?s ?x WHERE { ?s ex:p ?o BIND(?o AS ?x) }", "non-literal BIND"),
            (
                "SELECT ?s ?l WHERE { ?s ex:p ?o OPTIONAL { ?s ex:l ?l FILTER(?l > 3) } }",
                "unsupported SPARQL construct",
            ),
            ("SELECT ?s WHERE { ?s ex:p [ ex:q ?o ] }", "unsupported SPARQL construct"),
        ],
    )
    def test_rejects_with_feature_message(self, query, feature):
        assert _reject(query) == f"Spark SPARQL does not support {feature}."

    def test_having_is_rejected(self):
        _reject("SELECT ?s WHERE { ?s ex:p ?o } GROUP BY ?s HAVING (COUNT(?o) > 1)")


class TestUnionShape:
    _HEAD = "SELECT ?subject ?predicate ?object WHERE { "
    _OK_B = "{ ?subject ex:b ?object BIND(ex:b AS ?predicate) }"

    @pytest.mark.parametrize(
        "branch",
        [
            "{ ?subject ex:a ?object }",
            "{ ?subject ex:a ?object BIND(ex:b AS ?predicate) }",
            "{ ?subject ex:a ?object BIND('x' AS ?predicate) }",
            "{ ?subject ex:a ?object . ?subject ex:c ?object BIND(ex:a AS ?predicate) }",
            "{ ?x ex:a ?object BIND(ex:a AS ?predicate) }",
            "{ ex:s ex:a ?object BIND(ex:a AS ?predicate) }",
            "{ ?subject ex:a/ex:c ?object BIND(ex:a AS ?predicate) }",
            "{ ?subject ?pp ?object BIND(ex:a AS ?predicate) }",
            "{ ?subject ex:a ?object BIND(ex:a AS ?pred) }",
        ],
    )
    def test_malformed_relationship_branch_rejected(self, branch):
        q = self._HEAD + branch + " UNION " + self._OK_B + " }"
        assert _reject(q) == "Spark SPARQL does not support UNION."

    def test_union_requires_spo_projection(self):
        q = "SELECT ?s ?o WHERE { { ?s ex:a ?o } UNION { ?s ex:b ?o } }"
        assert _reject(q) == "Spark SPARQL does not support UNION."


class TestCraftedAlgebra:
    """Branches rdflib's parser cannot produce but the validator guards anyway."""

    def test_list_children_are_walked(self):
        bad = CompValue("Nope")
        with pytest.raises(ValidationError, match="unsupported SPARQL construct"):
            V._validate_node([CompValue("BGP", triples=[]), bad])

    def test_unknown_scalars_are_ignored(self):
        V._validate_node("plain string")

    def test_bgp_rejects_non_triple(self):
        with pytest.raises(ValidationError, match="unsupported SPARQL construct"):
            V._validate_bgp(CompValue("BGP", triples=[(Variable("s"), URIRef("http://p"))]))

    @pytest.mark.parametrize("pos", [0, 1, 2])
    def test_bgp_rejects_blank_node_in_any_position(self, pos):
        triple = [Variable("s"), URIRef("http://p"), Variable("o")]
        triple[pos] = BNode()
        with pytest.raises(ValidationError, match="unsupported SPARQL construct"):
            V._validate_bgp(CompValue("BGP", triples=[tuple(triple)]))

    def test_extend_over_group_is_group_by(self):
        node = CompValue("Extend", expr=Literal("x"), var=Variable("v"), p=CompValue("Group"))
        with pytest.raises(ValidationError, match="GROUP BY"):
            V._validate_node(node)

    def test_toplevel_feature_name_maps_to_feature(self):
        with pytest.raises(ValidationError, match="ORDER BY"):
            V._validate_node(CompValue("OrderCondition"))

    def test_filter_expr_must_be_comp_value(self):
        assert V._is_allowed_filter_expr(Literal("x")) is False

    @pytest.mark.parametrize("values", [[], "notalist", [URIRef("http://a"), Literal("b")]])
    def test_in_list_must_be_non_empty_uris(self, values):
        assert V._all_uris(values) is False

    def test_projection_with_non_variable_is_unknown(self):
        assert V._projected_vars_from_node([Variable("a"), "x"]) is None
        assert V._projected_vars_from_node(None) is None

    def test_join_without_multiset_is_subquery(self):
        node = CompValue("Join", p1=CompValue("BGP"), p2=CompValue("BGP"))
        assert V._join_feature(node) == "subquery"

    def test_union_with_non_comp_value_branch_rejected(self):
        with pytest.raises(ValidationError, match="UNION"):
            V._collect_union_branches(CompValue("Union", p1="x", p2=CompValue("BGP")))

    def test_union_with_no_branches_rejected(self, monkeypatch):
        monkeypatch.setattr(V, "_collect_union_branches", classmethod(lambda cls, n: []))
        with pytest.raises(ValidationError, match="UNION"):
            V._validate_union(CompValue("Union"), {"subject", "predicate", "object"})

    def _branch(self, triples, expr=URIRef("http://p"), var=Variable("predicate")):
        return CompValue("Extend", expr=expr, var=var, p=CompValue("BGP", triples=triples))

    def test_branch_with_non_bgp_body_rejected(self):
        b = CompValue("Extend", expr=URIRef("http://p"), var=Variable("predicate"), p=CompValue("Filter"))
        assert V._is_allowed_relationship_union_branch(b) is False

    def test_branch_with_short_triple_rejected(self):
        assert V._is_allowed_relationship_union_branch(
            self._branch([(Variable("subject"), URIRef("http://p"))])
        ) is False

    def test_branch_with_path_predicate_rejected(self):
        path = SequencePath(URIRef("http://p"), URIRef("http://q"))
        assert V._is_allowed_relationship_union_branch(
            self._branch([(Variable("subject"), path, Variable("object"))])
        ) is False

    def test_branch_with_literal_predicate_rejected(self):
        assert V._is_allowed_relationship_union_branch(
            self._branch([(Variable("subject"), Literal("p"), Variable("object"))])
        ) is False

    def test_well_formed_branch_accepted(self):
        assert V._is_allowed_relationship_union_branch(
            self._branch([(Variable("subject"), URIRef("http://p"), Variable("object"))])
        ) is True

    def test_contains_comp_value_name_walks_lists(self):
        assert V._contains_comp_value_name([("a", CompValue("Group"))], {"Group"}) is True
        assert V._contains_comp_value_name(["a"], {"Group"}) is False
