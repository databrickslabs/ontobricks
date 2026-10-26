"""Invalid stored URIs (spaces, ...) must not break Turtle OWL generation."""

from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF

from back.core.w3c.owl import OntologyGenerator
from back.core.w3c.owl.iri import escape_iri, serialize_turtle


def test_escape_iri_only_touches_illegal_characters():
    assert escape_iri("https://ex.com/WRFM - Shell#A b") == (
        "https://ex.com/WRFM%20-%20Shell#A%20b"
    )
    assert escape_iri("https://ex.com/a<b>{c}|d") == "https://ex.com/a%3Cb%3E%7Bc%7D%7Cd"
    assert escape_iri("https://ex.com/café#Été") == "https://ex.com/café#Été"
    assert escape_iri("https://ex.com/a%20b") == "https://ex.com/a%20b"


def test_valid_graph_is_serialized_unchanged():
    g = Graph()
    g.add((URIRef("https://ex.com/o#A"), RDF.type, OWL.Class))
    assert serialize_turtle(g) == g.serialize(format="turtle")


def test_generator_escapes_spaces_in_base_and_entity_uris():
    ttl = OntologyGenerator(
        base_uri="https://databricks-ontology.com/WRFM - Shell#",
        ontology_name="WRFM",
        classes=[
            {
                "name": "Affected asset",
                "uri": "https://ontobricks.com/ontology/cafds#Affected asset",
                "label": "Affected asset",
            }
        ],
        properties=[
            {
                "name": "HasSensor",
                "uri": "https://databricks-ontology.com/WRFM - Shell#HasSensor",
                "domain": "Affected asset",
                "range": "Affected asset",
            }
        ],
    ).generate()

    g = Graph().parse(data=ttl, format="turtle")
    cls = URIRef("https://ontobricks.com/ontology/cafds#Affected%20asset")
    prop = URIRef("https://databricks-ontology.com/WRFM%20-%20Shell#HasSensor")
    assert (cls, RDF.type, OWL.Class) in g
    assert (prop, RDF.type, OWL.ObjectProperty) in g
    assert (URIRef("https://databricks-ontology.com/WRFM%20-%20Shell"), RDF.type, OWL.Ontology) in g
