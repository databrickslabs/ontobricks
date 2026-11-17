"""JSON-LD export/import round-trip for OWL generation."""

from __future__ import annotations

import json

import pytest
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.compare import isomorphic
from rdflib.namespace import OWL, RDF, RDFS, XSD

from back.core.errors.ValidationError import ValidationError
from back.core.w3c.owl.OntologyGenerator import OntologyGenerator
from back.core.w3c.owl.OntologyParser import OntologyParser
from back.core.w3c.owl.iri import serialize_graph
from back.core.w3c.rdf_utils import parse_rdf_flexible


def _sample_ontology() -> OntologyGenerator:
    return OntologyGenerator(
        base_uri="http://example.org/domain#",
        ontology_name="DemoOntology",
        classes=[
            {
                "name": "Asset",
                "uri": "http://example.org/domain#Asset",
                "label": "Asset",
                "dataProperties": [
                    {
                        "name": "temperature",
                        "uri": "http://example.org/domain#temperature",
                        "datatype": "xsd:double",
                    }
                ],
            },
            {
                "name": "Facility",
                "uri": "http://example.org/domain#Facility",
                "label": "Facility",
            },
        ],
        properties=[
            {
                "name": "locatedIn",
                "uri": "http://example.org/domain#locatedIn",
                "type": "object",
                "domain": "http://example.org/domain#Asset",
                "range": "http://example.org/domain#Facility",
            }
        ],
    )


def test_jsonld_export_is_parseable_and_has_context():
    owl = _sample_ontology().generate(output_format="json-ld")
    payload = json.loads(owl)
    assert "@context" in payload
    context = payload["@context"]
    assert isinstance(context, dict)
    # Prefixes from the generator namespaces should appear.
    assert any(
        "example.org/domain" in str(v) or str(v).endswith("domain#")
        for v in context.values()
    )
    graph = parse_rdf_flexible(owl, formats=("json-ld",))
    assert len(graph) > 0


def test_jsonld_roundtrip_is_isomorphic():
    gen = _sample_ontology()
    turtle = gen.generate(output_format="turtle")
    # Rebuild so we get a fresh graph for JSON-LD (generate mutates self.graph).
    gen2 = _sample_ontology()
    jsonld = gen2.generate(output_format="json-ld")

    g_turtle = parse_rdf_flexible(turtle, formats=("turtle",))
    g_jsonld = parse_rdf_flexible(jsonld, formats=("json-ld",))
    assert isomorphic(g_turtle, g_jsonld)

    # Re-import through OntologyParser (json-ld among accepted formats).
    parsed = OntologyParser(jsonld)
    assert any(
        c.get("name") == "Asset" or "Asset" in (c.get("uri") or "")
        for c in parsed.get_classes()
    )


def test_serialize_graph_preserves_typed_literal_datatype():
    EX = Namespace("http://example.org/domain#")
    g = Graph()
    g.bind("ex", EX)
    g.bind("xsd", XSD)
    g.add((EX.pump, RDF.type, EX.Asset))
    g.add((EX.pump, EX.temperature, Literal(84.5, datatype=XSD.double)))

    jsonld = serialize_graph(g, "json-ld")
    roundtrip = parse_rdf_flexible(jsonld, formats=("json-ld",))
    temps = list(roundtrip.objects(EX.pump, EX.temperature))
    assert len(temps) == 1
    assert temps[0].datatype == XSD.double


def test_serialize_graph_rejects_unsupported_format():
    g = Graph()
    with pytest.raises(ValidationError, match="Unsupported RDF serialization format"):
        serialize_graph(g, "n3")


def test_salvage_truncated_owl_leaves_jsonld_intact():
    from back.core.w3c.owl.OntologyParser import _salvage_truncated_owl

    doc = '{\n  "@context": {"ex": "http://ex/"},\n  "@id": "ex:A"\n}'
    assert _salvage_truncated_owl(doc) == doc
