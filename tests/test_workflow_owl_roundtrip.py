"""Workflow tests: OWL import -> parse -> generate -> parse -> verify."""

import pytest
from back.core.w3c.owl.OntologyParser import OntologyParser
from back.core.w3c.owl.OntologyGenerator import OntologyGenerator


FULL_OWL = """@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix : <http://test.org/ontology#> .
@prefix ontobricks: <http://ontobricks.com/schema#> .

<http://test.org/ontology> a owl:Ontology ;
    rdfs:label "FullTest" .

:Customer a owl:Class ;
    rdfs:label "Customer" ;
    rdfs:comment "A customer" ;
    ontobricks:icon "👤" .

:Order a owl:Class ;
    rdfs:label "Order" ;
    rdfs:comment "An order" .

:Product a owl:Class ;
    rdfs:label "Product" ;
    rdfs:subClassOf :Order .

:hasOrder a owl:ObjectProperty, owl:FunctionalProperty ;
    rdfs:label "has Order" ;
    rdfs:domain :Customer ;
    rdfs:range :Order .

:firstName a owl:DatatypeProperty ;
    rdfs:label "firstName" ;
    rdfs:domain :Customer ;
    rdfs:range xsd:string .

:Customer rdfs:subClassOf [
    a owl:Restriction ;
    owl:onProperty :hasOrder ;
    owl:minCardinality "1"^^xsd:nonNegativeInteger
] .

:_swrlRule_ValidateAge a ontobricks:SWRLRule ;
    rdfs:label "ValidateAge" ;
    rdfs:comment "Ensure age positive" ;
    ontobricks:antecedent "Customer(?c) ^ age(?c, ?a)" ;
    ontobricks:consequent "ValidAge(?c)" .

:Customer owl:equivalentClass :Client .
:Client a owl:Class ; rdfs:label "Client" .
"""


class TestOWLRoundtrip:
    def test_parse_and_regenerate(self):
        parser1 = OntologyParser(FULL_OWL)
        info = parser1.get_ontology_info()
        classes = parser1.get_classes()
        properties = parser1.get_properties()
        constraints = parser1.get_constraints()
        swrl_rules = parser1.get_swrl_rules()
        split = parser1.get_axioms_and_expressions()
        axioms = split["axioms"]
        expressions = split["expressions"]

        assert info["label"] == "FullTest"
        assert len(classes) >= 3
        class_names = {c["name"] for c in classes}
        assert "Customer" in class_names
        assert "Order" in class_names

        gen = OntologyGenerator(
            base_uri=info["namespace"],
            ontology_name=info["label"],
            classes=classes,
            properties=properties,
            constraints=constraints,
            swrl_rules=swrl_rules,
            axioms=axioms,
            expressions=expressions,
        )
        regenerated_owl = gen.generate()
        assert "@prefix" in regenerated_owl

        parser2 = OntologyParser(regenerated_owl)
        classes2 = parser2.get_classes()
        class_names2 = {c["name"] for c in classes2}
        assert "Customer" in class_names2
        assert "Order" in class_names2

    def test_constraints_preserved(self):
        parser = OntologyParser(FULL_OWL)
        constraints = parser.get_constraints()
        functional = [c for c in constraints if c["type"] == "functional"]
        assert len(functional) >= 1

        info = parser.get_ontology_info()
        gen = OntologyGenerator(
            base_uri=info["namespace"],
            ontology_name=info["label"],
            classes=parser.get_classes(),
            properties=parser.get_properties(),
            constraints=constraints,
        )
        owl2 = gen.generate()
        parser2 = OntologyParser(owl2)
        constraints2 = parser2.get_constraints()
        functional2 = [c for c in constraints2 if c["type"] == "functional"]
        assert len(functional2) >= 1

    def test_swrl_rules_preserved(self):
        parser = OntologyParser(FULL_OWL)
        rules = parser.get_swrl_rules()
        assert len(rules) >= 1

        info = parser.get_ontology_info()
        gen = OntologyGenerator(
            base_uri=info["namespace"],
            ontology_name=info["label"],
            classes=parser.get_classes(),
            properties=parser.get_properties(),
            swrl_rules=rules,
        )
        owl2 = gen.generate()
        parser2 = OntologyParser(owl2)
        rules2 = parser2.get_swrl_rules()
        assert len(rules2) >= 1
        assert rules2[0]["name"] == "ValidateAge"

    def test_axioms_preserved(self):
        parser = OntologyParser(FULL_OWL)
        split = parser.get_axioms_and_expressions()
        axioms = split["axioms"]
        expressions = split["expressions"]
        equiv = [a for a in axioms if a["type"] == "equivalentClass"]
        assert len(equiv) >= 1

        info = parser.get_ontology_info()
        gen = OntologyGenerator(
            base_uri=info["namespace"],
            ontology_name=info["label"],
            classes=parser.get_classes(),
            properties=parser.get_properties(),
            axioms=axioms,
            expressions=expressions,
        )
        owl2 = gen.generate()
        parser2 = OntologyParser(owl2)
        split2 = parser2.get_axioms_and_expressions()
        equiv2 = [a for a in split2["axioms"] if a["type"] == "equivalentClass"]
        assert len(equiv2) >= 1
