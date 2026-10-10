# tests/units/ontology/test_owl_import_fidelity.py
"""Round-trip fidelity for GitHub issue #194."""

import pytest
from rdflib import DCTERMS, OWL, RDF, RDFS, Graph, Literal, URIRef
from rdflib.namespace import SKOS, XSD, Namespace

from back.objects.ontology.OntologyOwl import OntologyOwl

PROV = Namespace("http://www.w3.org/ns/prov#")
EX = Namespace("http://example.org/onto#")
OTHER = URIRef("http://example.org/other-onto#Party")

ISSUE_194_TTL = """@prefix ex:      <http://example.org/onto#> .
@prefix owl:     <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:    <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:     <http://www.w3.org/2001/XMLSchema#> .
@prefix skos:    <http://www.w3.org/2004/02/skos/core#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix prov:    <http://www.w3.org/ns/prov#> .

<http://example.org/onto> a owl:Ontology ;
    rdfs:label "Test Ontology"@en ;
    dcterms:description "Used to test import fidelity."@en ;
    dcterms:source <http://example.org/other-onto> ;
    owl:versionInfo "1.0" .

ex:Customer a owl:Class ;
    rdfs:label "Customer"@en ;
    skos:definition "A party that buys goods."@en ;
    skos:editorialNote "Review naming."@en ;
    dcterms:created "2026-01-01"^^xsd:date ;
    prov:wasDerivedFrom <http://example.org/other-onto#Party> ;
    rdfs:subClassOf <http://example.org/other-onto#Party> .

ex:hasName a owl:DatatypeProperty ;
    rdfs:domain ex:Customer ;
    rdfs:range xsd:string ;
    skos:definition "The customer's legal name."@en .
"""


def _roundtrip() -> Graph:
    info, classes, properties, constraints, swrl, axioms, expressions, groups = (
        OntologyOwl.parse_owl(ISSUE_194_TTL, extract_advanced=True)
    )
    turtle = OntologyOwl.generate_owl(
        {
            "base_uri": info.get("namespace") or info.get("uri"),
            "name": info.get("label") or info.get("name") or "",
            "label_lang": info.get("label_lang"),
            "classes": classes,
            "properties": properties,
            "rdf_extras": info.get("rdf_extras") or {},
        },
        constraints=constraints,
        swrl_rules=swrl,
        axioms=axioms,
        expressions=expressions,
        groups=groups,
    )
    g = Graph()
    g.parse(data=turtle, format="turtle")
    return g


def test_generate_owl_reads_rdf_extras_from_config():
    info, classes, properties, *_rest = OntologyOwl.parse_owl(
        ISSUE_194_TTL, extract_advanced=True
    )
    turtle = OntologyOwl.generate_owl(
        {
            "base_uri": info["namespace"],
            "name": info["label"],
            "label_lang": info.get("label_lang"),
            "classes": classes,
            "properties": properties,
            "rdf_extras": info["rdf_extras"],
        }
    )
    graph = Graph()
    graph.parse(data=turtle, format="turtle")

    assert (
        EX.Customer,
        SKOS.definition,
        Literal("A party that buys goods.", lang="en"),
    ) in graph


@pytest.mark.unit
class TestIssue194ImportFidelity:
    def test_ontology_header_and_annotations(self):
        g = _roundtrip()
        onto = URIRef("http://example.org/onto")
        assert (onto, RDF.type, OWL.Ontology) in g
        assert (onto, RDFS.label, Literal("Test Ontology", lang="en")) in g
        assert (
            onto,
            DCTERMS.description,
            Literal("Used to test import fidelity.", lang="en"),
        ) in g
        assert (onto, DCTERMS.source, URIRef("http://example.org/other-onto")) in g
        assert (onto, OWL.versionInfo, Literal("1.0")) in g

    def test_class_external_parent_and_skos_dcterms_prov(self):
        g = _roundtrip()
        cls = EX.Customer
        assert (cls, RDF.type, OWL.Class) in g
        assert (cls, RDFS.label, Literal("Customer", lang="en")) in g
        assert (
            cls,
            SKOS.definition,
            Literal("A party that buys goods.", lang="en"),
        ) in g
        assert (
            cls,
            SKOS.editorialNote,
            Literal("Review naming.", lang="en"),
        ) in g
        assert (cls, DCTERMS.created, Literal("2026-01-01", datatype=XSD.date)) in g
        assert (cls, PROV.wasDerivedFrom, OTHER) in g
        assert (cls, RDFS.subClassOf, OTHER) in g

    def test_property_skos_definition(self):
        g = _roundtrip()
        prop = EX.hasName
        assert (prop, RDF.type, OWL.DatatypeProperty) in g
        assert (
            prop,
            SKOS.definition,
            Literal("The customer's legal name.", lang="en"),
        ) in g
