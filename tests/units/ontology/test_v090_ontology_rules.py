"""Rule-reference validation, SHACL shape checks and business-rule refs (release 0.9.0)."""

import pytest
from rdflib import Graph, URIRef
from rdflib.namespace import RDF, SH

from back.objects.ontology.Ontology import Ontology
from back.objects.ontology.Rules import Rules

pytestmark = pytest.mark.unit

CLASSES = {"customer", "order", "vip"}
PROPS = {"places", "amount", "status"}


class TestBusinessRuleRefs:
    def test_rename_updates_every_matching_reference(self):
        classes = [
            {"name": "A", "business_rules": [{"name": "r1"}, {"name": "r2"}]},
            {"name": "B", "business_rules": [{"name": "r1"}, "legacy-string"]},
            {"name": "C"},
        ]
        assert Rules.rename_business_rule_refs(classes, "r1", "rule_one") == 2
        assert classes[0]["business_rules"] == [{"name": "rule_one"}, {"name": "r2"}]
        assert classes[1]["business_rules"] == [{"name": "rule_one"}, "legacy-string"]

    @pytest.mark.parametrize("old, new", [("", "x"), ("r1", "r1")])
    def test_rename_noop_for_empty_or_identical_name(self, old, new):
        classes = [{"business_rules": [{"name": "r1"}]}]
        assert Rules.rename_business_rule_refs(classes, old, new) == 0
        assert classes == [{"business_rules": [{"name": "r1"}]}]

    def test_drop_removes_only_matching_refs(self):
        classes = [
            {"business_rules": [{"name": "r1"}, {"name": "r2"}, {"name": "r1"}]},
            {"business_rules": [{"name": "r2"}]},
        ]
        assert Rules.drop_business_rule_refs(classes, "r1") == 2
        assert classes == [
            {"business_rules": [{"name": "r2"}]},
            {"business_rules": [{"name": "r2"}]},
        ]


class TestRefLocalName:
    @pytest.mark.parametrize(
        "term",
        ["", "   ", "?x", "a", '"literal"', "'x'", "+1", "-2", "42", "<http://x.org#A>",
         "rdf:type", "owl:Thing", "xsd:string", "swrlb:greaterThan"],
    )
    def test_non_ontology_terms_not_checkable(self, term):
        assert Rules._ref_local_name(term) == (False, "")

    @pytest.mark.parametrize(
        "term, local",
        [(":Customer", "Customer"), ("ex:places", "places"), ("Order", "Order"),
         ("http://x.org/Thing", "http://x.org/Thing")],
    )
    def test_ontology_terms_return_local_name(self, term, local):
        assert Rules._ref_local_name(term) == (True, local)

    def test_facade_delegates(self):
        assert Ontology._ref_local_name("ex:Order") == (True, "Order")


class TestSparqlReferences:
    def test_unknown_class_and_property_reported_in_head_and_where(self):
        rule = {
            "query": "CONSTRUCT { ?c a :Ghost . } WHERE { ?c a :Customer . ?c :unknownProp ?o . }"
        }
        errors = Rules.sparql_reference_errors(rule, CLASSES, PROPS)
        assert "query references unknown entity 'Ghost'" in errors
        assert "query references unknown relationship/property 'unknownProp'" in errors
        assert len(errors) == 2

    def test_known_terms_and_builtins_pass(self):
        rule = {
            "query": "CONSTRUCT { ?c a :Vip . } WHERE { ?c rdf:type :Customer . ?c :amount ?v . }"
        }
        assert Rules.sparql_reference_errors(rule, CLASSES, PROPS) == []

    def test_malformed_query_left_to_structural_validator(self):
        assert Rules.sparql_reference_errors({"query": "SELECT * WHERE {}"}, CLASSES, PROPS) == []
        assert Rules.sparql_reference_errors({}, CLASSES, PROPS) == []


class TestRuleReferenceDispatch:
    def test_dispatches_to_each_rule_kind(self):
        assert Rules.rule_reference_errors(
            "decision_tables", {"target_class": "Ghost"}, CLASSES, PROPS
        ) == ["target class 'Ghost' does not exist in the ontology"]
        assert Rules.rule_reference_errors(
            "aggregate_rules", {"result_class": "Ghost"}, CLASSES, PROPS
        ) == ["result_class 'Ghost' does not exist in the ontology"]
        assert Rules.rule_reference_errors(
            "sparql_rules", {"query": "CONSTRUCT { ?x a :Ghost . } WHERE { ?x a :Order . }"},
            CLASSES, PROPS,
        ) == ["query references unknown entity 'Ghost'"]

    def test_unknown_rule_kind_has_no_errors(self):
        assert Rules.rule_reference_errors("mystery", {"target_class": "Ghost"}, CLASSES, PROPS) == []

    def test_facade_dispatch(self):
        assert Ontology.rule_reference_errors("mystery", {}, CLASSES, PROPS) == []


class TestValidateShape:
    def _shape(self, **overrides):
        shape = {
            "category": "completeness",
            "shacl_type": "sh:minCount",
            "property_path": "email",
            "parameters": {"sh:minCount": 1},
        }
        shape.update(overrides)
        return shape

    def test_valid_shape(self):
        assert Rules.validate_shape(self._shape()) is None

    def test_invalid_category(self):
        error = Rules.validate_shape(self._shape(category="bogus"))
        assert error.startswith("Invalid category 'bogus'")
        assert "completeness" in error

    def test_missing_shacl_type(self):
        assert Rules.validate_shape(self._shape(shacl_type="")) == "shacl_type is required"

    def test_property_required_except_for_sparql_and_closed(self):
        error = Rules.validate_shape(self._shape(property_path="", property_uri=""))
        assert error == "A property path or URI is required for this constraint type"
        assert Rules.validate_shape(
            self._shape(shacl_type="sh:closed", property_path="", parameters={})
        ) is None

    def test_property_uri_alone_is_enough(self):
        assert Rules.validate_shape(
            self._shape(property_path="", property_uri="http://x#email")
        ) is None

    @pytest.mark.parametrize("key", ["sh:minCount", "sh:maxCount"])
    def test_non_integer_counts_rejected(self, key):
        shape = self._shape(shacl_type="sh:maxCount", parameters={key: "many"})
        assert Rules.validate_shape(shape) == f"{key} must be an integer"

    def test_facade_delegates(self):
        assert Ontology.validate_shape(self._shape(category="")).startswith("Invalid category")


class TestGenerateShacl:
    def test_emits_node_shape_for_target_class(self):
        shapes = [{
            "id": "s1", "category": "completeness", "label": "email required",
            "target_class": "Customer", "target_class_uri": "http://x.org/onto#Customer",
            "property_path": "email", "property_uri": "http://x.org/onto#email",
            "shacl_type": "sh:minCount", "parameters": {"sh:minCount": 1},
            "severity": "sh:Violation", "message": "", "enabled": True,
        }]
        turtle = Rules.generate_shacl(shapes, "http://x.org/onto#")
        g = Graph()
        g.parse(data=turtle, format="turtle")
        targets = set(g.objects(None, SH.targetClass))
        assert URIRef("http://x.org/onto#Customer") in targets
        assert any(True for _ in g.subjects(RDF.type, SH.NodeShape))

    def test_default_base_uri_used_when_missing(self):
        turtle = Ontology.generate_shacl([], "")
        Graph().parse(data=turtle, format="turtle")
        assert isinstance(turtle, str)
