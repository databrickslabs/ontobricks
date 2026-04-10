"""Tests for OWL ontology parser."""
import pytest
from back.core.w3c.owl.OntologyParser import OntologyParser


SAMPLE_TURTLE = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://test.org/ontology#> .
@prefix ontobricks: <http://ontobricks.com/schema#> .

<http://test.org/ontology> a owl:Ontology ;
    rdfs:label "TestOntology" ;
    rdfs:comment "A test ontology" .

:Customer a owl:Class ;
    rdfs:label "Customer" ;
    rdfs:comment "A customer entity" ;
    ontobricks:icon "👤" .

:Order a owl:Class ;
    rdfs:label "Order" ;
    rdfs:comment "A sales order" .

:VIPCustomer a owl:Class ;
    rdfs:label "VIP Customer" ;
    rdfs:subClassOf :Customer .

:hasOrder a owl:ObjectProperty ;
    rdfs:label "has Order" ;
    rdfs:domain :Customer ;
    rdfs:range :Order .

:firstName a owl:DatatypeProperty ;
    rdfs:label "firstName" ;
    rdfs:domain :Customer ;
    rdfs:range xsd:string .

:lastName a owl:DatatypeProperty ;
    rdfs:label "lastName" ;
    rdfs:domain :Customer ;
    rdfs:range xsd:string .

:hasOrder a owl:FunctionalProperty .
"""

TURTLE_WITH_CONSTRAINTS = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://test.org/ontology#> .

<http://test.org/ontology> a owl:Ontology .

:Customer a owl:Class .
:Order a owl:Class .

:hasOrder a owl:ObjectProperty, owl:FunctionalProperty ;
    rdfs:domain :Customer ;
    rdfs:range :Order .

:Customer rdfs:subClassOf [
    a owl:Restriction ;
    owl:onProperty :hasOrder ;
    owl:minCardinality "1"^^xsd:nonNegativeInteger
] .
"""

TURTLE_WITH_SWRL = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix : <http://test.org/ontology#> .
@prefix ontobricks: <http://ontobricks.com/schema#> .

<http://test.org/ontology> a owl:Ontology .
:Customer a owl:Class .

:_swrlRule_CheckAge a ontobricks:SWRLRule ;
    rdfs:label "CheckAge" ;
    rdfs:comment "Age must be positive" ;
    ontobricks:antecedent "Customer(?c) ^ age(?c, ?a) ^ lessThan(?a, 0)" ;
    ontobricks:consequent "InvalidAge(?c)" .
"""

TURTLE_WITH_AXIOMS = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix : <http://test.org/ontology#> .

<http://test.org/ontology> a owl:Ontology .

:Customer a owl:Class .
:Client a owl:Class .
:Individual a owl:Class .
:Company a owl:Class .

:Customer owl:equivalentClass :Client .
:Individual owl:disjointWith :Company .
"""


class TestOntologyParserInit:
    def test_parse_turtle(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        assert parser.graph is not None

    def test_parse_empty_returns_no_classes(self):
        parser = OntologyParser("")
        assert parser.get_classes() == []

    def test_parse_invalid_raises(self):
        with pytest.raises((ValueError, Exception)):
            OntologyParser("this is not valid owl content at all xyz")


class TestToCamelCase:
    def test_spaces_pascal(self):
        assert OntologyParser._to_camel_case("Contract ID") == "ContractId"

    def test_spaces_camel(self):
        assert OntologyParser._to_camel_case("street address") == "streetAddress"

    def test_underscore(self):
        assert OntologyParser._to_camel_case("first_name") == "firstName"

    def test_single_word_unchanged(self):
        assert OntologyParser._to_camel_case("Customer") == "Customer"

    def test_empty_string(self):
        assert OntologyParser._to_camel_case("") == ""

    def test_hyphen(self):
        assert OntologyParser._to_camel_case("order-date") == "orderDate"


class TestGetClasses:
    def test_extracts_classes(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        classes = parser.get_classes()
        names = [c['name'] for c in classes]
        assert 'Customer' in names
        assert 'Order' in names
        assert 'VIPCustomer' in names

    def test_class_fields(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        classes = {c['name']: c for c in parser.get_classes()}
        cust = classes['Customer']
        assert cust['label'] == 'Customer'
        assert cust['comment'] == 'A customer entity'
        assert cust['emoji'] == '👤'

    def test_parent_class(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        classes = {c['name']: c for c in parser.get_classes()}
        assert classes['VIPCustomer']['parent'] == 'Customer'
        assert classes['Customer']['parent'] == ''

    def test_data_properties_assigned(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        classes = {c['name']: c for c in parser.get_classes()}
        dp_names = [dp['name'] for dp in classes['Customer']['dataProperties']]
        assert 'firstName' in dp_names
        assert 'lastName' in dp_names

    def test_sorted_by_name(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        classes = parser.get_classes()
        names = [c['name'] for c in classes]
        assert names == sorted(names)


class TestGetProperties:
    def test_extracts_properties(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        props = parser.get_properties()
        names = [p['name'] for p in props]
        assert 'hasOrder' in names
        assert 'firstName' in names

    def test_property_type(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        props = {p['name']: p for p in parser.get_properties()}
        assert props['hasOrder']['type'] == 'ObjectProperty'
        assert props['firstName']['type'] == 'DatatypeProperty'

    def test_domain_range(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        props = {p['name']: p for p in parser.get_properties()}
        assert props['hasOrder']['domain'] == 'Customer'
        assert props['hasOrder']['range'] == 'Order'


class TestGetOntologyInfo:
    def test_basic_info(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        info = parser.get_ontology_info()
        assert info['uri'] == 'http://test.org/ontology'
        assert info['label'] == 'TestOntology'
        assert info['comment'] == 'A test ontology'

    def test_namespace_has_separator(self):
        parser = OntologyParser(SAMPLE_TURTLE)
        info = parser.get_ontology_info()
        assert info['namespace'].endswith('#') or info['namespace'].endswith('/')


class TestGetConstraints:
    def test_functional_property(self):
        parser = OntologyParser(TURTLE_WITH_CONSTRAINTS)
        constraints = parser.get_constraints()
        functional = [c for c in constraints if c['type'] == 'functional']
        assert len(functional) == 1
        assert functional[0]['property'] == 'hasOrder'

    def test_min_cardinality(self):
        parser = OntologyParser(TURTLE_WITH_CONSTRAINTS)
        constraints = parser.get_constraints()
        min_card = [c for c in constraints if c['type'] == 'minCardinality']
        assert len(min_card) == 1
        assert min_card[0]['className'] == 'Customer'
        assert min_card[0]['cardinalityValue'] == 1


class TestGetSwrlRules:
    def test_extract_swrl_rule(self):
        parser = OntologyParser(TURTLE_WITH_SWRL)
        rules = parser.get_swrl_rules()
        assert len(rules) == 1
        assert rules[0]['name'] == 'CheckAge'
        assert 'age' in rules[0]['antecedent']
        assert 'InvalidAge' in rules[0]['consequent']


class TestGetAxioms:
    def test_equivalent_class(self):
        parser = OntologyParser(TURTLE_WITH_AXIOMS)
        axioms = parser.get_axioms()
        equiv = [a for a in axioms if a['type'] == 'equivalentClass']
        assert len(equiv) == 1
        assert equiv[0]['subject'] == 'Customer'
        assert 'Client' in equiv[0]['objects']

    def test_disjoint_with(self):
        parser = OntologyParser(TURTLE_WITH_AXIOMS)
        axioms = parser.get_axioms()
        disj = [a for a in axioms if a['type'] == 'disjointWith']
        assert len(disj) == 1
        assert disj[0]['subject'] == 'Individual'
        assert 'Company' in disj[0]['objects']

    def test_get_axioms_and_expressions_split(self):
        parser = OntologyParser(TURTLE_WITH_AXIOMS)
        result = parser.get_axioms_and_expressions()
        assert isinstance(result, dict)
        assert 'axioms' in result
        assert 'expressions' in result
        assert len(result['axioms']) == 2
        assert len(result['expressions']) == 0
