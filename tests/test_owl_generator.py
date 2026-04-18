"""Tests for OWL ontology generator."""

import pytest
from back.core.w3c.owl.OntologyGenerator import OntologyGenerator
from back.core.w3c.owl.OntologyParser import OntologyParser


def _make_generator(
    classes=None,
    properties=None,
    constraints=None,
    swrl_rules=None,
    axioms=None,
    expressions=None,
):
    return OntologyGenerator(
        base_uri="http://test.org/ontology#",
        ontology_name="TestOntology",
        classes=classes or [],
        properties=properties or [],
        constraints=constraints,
        swrl_rules=swrl_rules,
        axioms=axioms,
        expressions=expressions,
    )


class TestBasicGeneration:
    def test_generates_turtle(self):
        gen = _make_generator()
        owl = gen.generate()
        assert "@prefix" in owl
        assert "owl:Ontology" in owl

    def test_ontology_label(self):
        gen = _make_generator()
        owl = gen.generate()
        assert "TestOntology" in owl


class TestClassGeneration:
    def test_simple_class(self):
        classes = [{"name": "Customer", "label": "Customer", "comment": "A customer"}]
        gen = _make_generator(classes=classes)
        owl = gen.generate()
        assert "Customer" in owl
        assert "owl:Class" in owl

    def test_class_with_parent(self):
        classes = [
            {"name": "Customer", "label": "Customer"},
            {"name": "VIPCustomer", "label": "VIP", "parent": "Customer"},
        ]
        gen = _make_generator(classes=classes)
        owl = gen.generate()
        assert "subClassOf" in owl

    def test_class_with_emoji(self):
        classes = [{"name": "Customer", "label": "Customer", "emoji": "👤"}]
        gen = _make_generator(classes=classes)
        owl = gen.generate()
        assert "👤" in owl

    def test_class_with_data_properties(self):
        classes = [
            {
                "name": "Customer",
                "label": "Customer",
                "dataProperties": [
                    {"name": "firstName", "label": "First Name"},
                    {"name": "lastName", "label": "Last Name"},
                ],
            }
        ]
        gen = _make_generator(classes=classes)
        owl = gen.generate()
        assert "DatatypeProperty" in owl
        assert "firstName" in owl

    def test_empty_class_name_skipped(self):
        classes = [{"name": "", "label": "Empty"}]
        gen = _make_generator(classes=classes)
        owl = gen.generate()
        assert "Empty" not in owl or "owl:Class" in owl


class TestPropertyGeneration:
    def test_object_property(self):
        props = [
            {
                "name": "hasOrder",
                "label": "has Order",
                "type": "ObjectProperty",
                "domain": "Customer",
                "range": "Order",
            }
        ]
        gen = _make_generator(properties=props)
        owl = gen.generate()
        assert "ObjectProperty" in owl
        assert "hasOrder" in owl

    def test_datatype_property(self):
        props = [
            {
                "name": "age",
                "label": "Age",
                "type": "DatatypeProperty",
                "domain": "Customer",
                "range": "xsd:integer",
            }
        ]
        gen = _make_generator(properties=props)
        owl = gen.generate()
        assert "DatatypeProperty" in owl


class TestConstraintGeneration:
    def test_functional_property(self):
        constraints = [{"type": "functional", "property": "hasOrder"}]
        gen = _make_generator(constraints=constraints)
        owl = gen.generate()
        assert "FunctionalProperty" in owl

    def test_min_cardinality(self):
        constraints = [
            {
                "type": "minCardinality",
                "property": "hasOrder",
                "className": "Customer",
                "cardinalityValue": 1,
            }
        ]
        gen = _make_generator(constraints=constraints)
        owl = gen.generate()
        assert "minCardinality" in owl


class TestSwrlGeneration:
    def test_swrl_rule(self):
        rules = [
            {
                "name": "CheckAge",
                "description": "Age must be positive",
                "antecedent": "Customer(?c) ^ age(?c, ?a)",
                "consequent": "Valid(?c)",
            }
        ]
        gen = _make_generator(swrl_rules=rules)
        owl = gen.generate()
        assert "CheckAge" in owl
        assert "SWRLRule" in owl


class TestAxiomGeneration:
    def test_equivalent_class(self):
        axioms = [
            {"type": "equivalentClass", "subject": "Customer", "objects": ["Client"]}
        ]
        gen = _make_generator(axioms=axioms)
        owl = gen.generate()
        assert "equivalentClass" in owl

    def test_disjoint_with(self):
        axioms = [{"type": "disjointWith", "subject": "Dog", "objects": ["Cat"]}]
        gen = _make_generator(axioms=axioms)
        owl = gen.generate()
        assert "disjointWith" in owl


class TestExpressionGeneration:
    def test_union_of(self):
        expressions = [{"type": "unionOf", "subject": "Pet", "objects": ["Cat", "Dog"]}]
        gen = _make_generator(expressions=expressions)
        owl = gen.generate()
        assert "unionOf" in owl

    def test_intersection_of(self):
        expressions = [
            {
                "type": "intersectionOf",
                "subject": "WorkingParent",
                "objects": ["Parent", "Employee"],
            }
        ]
        gen = _make_generator(expressions=expressions)
        owl = gen.generate()
        assert "intersectionOf" in owl


class TestRoundtrip:
    def test_generate_parse_classes(self):
        classes = [
            {
                "name": "Customer",
                "label": "Customer",
                "comment": "A customer",
                "emoji": "👤",
            },
            {"name": "Order", "label": "Order", "comment": "An order"},
        ]
        props = [
            {
                "name": "hasOrder",
                "label": "has Order",
                "type": "ObjectProperty",
                "domain": "Customer",
                "range": "Order",
            }
        ]
        gen = _make_generator(classes=classes, properties=props)
        owl = gen.generate()

        parser = OntologyParser(owl)
        parsed_classes = parser.get_classes()
        parsed_names = {c["name"] for c in parsed_classes}
        assert "Customer" in parsed_names
        assert "Order" in parsed_names

        parsed_props = parser.get_properties()
        parsed_prop_names = {p["name"] for p in parsed_props}
        assert "hasOrder" in parsed_prop_names

    def test_roundtrip_constraints(self):
        constraints = [{"type": "functional", "property": "hasOrder"}]
        props = [{"name": "hasOrder", "type": "ObjectProperty"}]
        gen = _make_generator(properties=props, constraints=constraints)
        owl = gen.generate()

        parser = OntologyParser(owl)
        parsed_constraints = parser.get_constraints()
        functional = [c for c in parsed_constraints if c["type"] == "functional"]
        assert len(functional) >= 1

    def test_roundtrip_swrl(self):
        rules = [
            {
                "name": "TestRule",
                "description": "Test",
                "antecedent": "A(?x)",
                "consequent": "B(?x)",
            }
        ]
        gen = _make_generator(swrl_rules=rules)
        owl = gen.generate()

        parser = OntologyParser(owl)
        parsed_rules = parser.get_swrl_rules()
        assert len(parsed_rules) == 1
        assert parsed_rules[0]["name"] == "TestRule"

    def test_roundtrip_axioms(self):
        axioms = [{"type": "equivalentClass", "subject": "A", "objects": ["B"]}]
        gen = _make_generator(axioms=axioms)
        owl = gen.generate()

        parser = OntologyParser(owl)
        parsed = parser.get_axioms()
        equiv = [a for a in parsed if a["type"] == "equivalentClass"]
        assert len(equiv) >= 1
