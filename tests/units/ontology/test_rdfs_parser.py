"""Tests for back.core.rdfs.rdfs_parser — RDFS vocabulary parsing."""

import pytest

from back.core.w3c.rdfs.RDFSParser import RDFSParser
from back.core.errors import ValidationError


SAMPLE_RDFS = """
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:   <http://example.org/schema#> .

ex: rdfs:label "Example Schema" ;
    rdfs:comment "A test RDFS vocabulary" .

ex:Person a rdfs:Class ;
    rdfs:label "Person" ;
    rdfs:comment "A human being" .

ex:Organization a rdfs:Class ;
    rdfs:label "Organization" ;
    rdfs:comment "An organization" .

ex:Employee a rdfs:Class ;
    rdfs:label "Employee" ;
    rdfs:subClassOf ex:Person .

ex:name a rdf:Property ;
    rdfs:label "name" ;
    rdfs:comment "The name of a person" ;
    rdfs:domain ex:Person ;
    rdfs:range xsd:string .

ex:worksFor a rdf:Property ;
    rdfs:label "worksFor" ;
    rdfs:comment "Employment relationship" ;
    rdfs:domain ex:Person ;
    rdfs:range ex:Organization .

ex:age a rdf:Property ;
    rdfs:label "age" ;
    rdfs:domain ex:Person ;
    rdfs:range xsd:integer .
"""


class TestInit:
    def test_valid_turtle(self):
        parser = RDFSParser(SAMPLE_RDFS)
        assert parser.graph is not None
        assert len(parser.graph) > 0

    def test_invalid_content_raises(self):
        with pytest.raises(ValidationError, match="Could not parse"):
            RDFSParser("this is not RDF at all")


class TestGetClasses:
    def test_finds_classes(self):
        parser = RDFSParser(SAMPLE_RDFS)
        classes = parser.get_classes()
        names = [c["name"] for c in classes]
        assert "Person" in names
        assert "Organization" in names
        assert "Employee" in names

    def test_class_labels(self):
        parser = RDFSParser(SAMPLE_RDFS)
        classes = {c["name"]: c for c in parser.get_classes()}
        assert classes["Person"]["label"] == "Person"
        assert classes["Person"]["description"] == "A human being"

    def test_subclass(self):
        parser = RDFSParser(SAMPLE_RDFS)
        classes = {c["name"]: c for c in parser.get_classes()}
        assert classes["Employee"]["parent"] == "Person"

    def test_sorted(self):
        parser = RDFSParser(SAMPLE_RDFS)
        classes = parser.get_classes()
        names = [c["name"] for c in classes]
        assert names == sorted(names)


class TestGetProperties:
    def test_finds_properties(self):
        parser = RDFSParser(SAMPLE_RDFS)
        props = parser.get_properties()
        names = [p["name"] for p in props]
        assert "name" in names
        assert "worksFor" in names
        assert "age" in names

    def test_datatype_property_detection(self):
        parser = RDFSParser(SAMPLE_RDFS)
        props = {p["name"]: p for p in parser.get_properties()}
        assert props["name"]["type"] == "DatatypeProperty"
        assert props["age"]["type"] == "DatatypeProperty"

    def test_object_property_detection(self):
        parser = RDFSParser(SAMPLE_RDFS)
        props = {p["name"]: p for p in parser.get_properties()}
        assert props["worksFor"]["type"] == "ObjectProperty"

    def test_domain_and_range(self):
        parser = RDFSParser(SAMPLE_RDFS)
        props = {p["name"]: p for p in parser.get_properties()}
        assert props["name"]["domain"] == "Person"
        assert props["name"]["range"] == "string"
        assert props["worksFor"]["range"] == "Organization"

    def test_sorted(self):
        parser = RDFSParser(SAMPLE_RDFS)
        props = parser.get_properties()
        names = [p["name"] for p in props]
        assert names == sorted(names)


class TestGetOntologyInfo:
    def test_gets_info(self):
        parser = RDFSParser(SAMPLE_RDFS)
        info = parser.get_ontology_info()
        assert info["label"]
        assert "namespace" in info

    def test_fallback_for_minimal(self):
        minimal = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex: <http://minimal.org/> .
ex:Thing a rdfs:Class .
"""
        parser = RDFSParser(minimal)
        info = parser.get_ontology_info()
        assert info["label"]


class TestExtractLocalName:
    def test_hash(self):
        parser = RDFSParser(SAMPLE_RDFS)
        assert parser._extract_local_name("http://ex.org/schema#Foo") == "Foo"

    def test_slash(self):
        parser = RDFSParser(SAMPLE_RDFS)
        assert parser._extract_local_name("http://ex.org/schema/Bar") == "Bar"

    def test_empty(self):
        parser = RDFSParser(SAMPLE_RDFS)
        assert parser._extract_local_name("") == ""
