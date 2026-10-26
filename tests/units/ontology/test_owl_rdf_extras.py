from rdflib import OWL, RDF, RDFS, Graph, Literal, URIRef
from rdflib.namespace import SKOS

from back.core.w3c.owl.OntologyRdfExtras import OntologyRdfExtras


def test_skos_definition_is_leftover_rdfs_label_is_not():
    g = Graph()
    cls = URIRef("http://example.org/onto#Customer")
    g.add((cls, RDF.type, OWL.Class))
    g.add((cls, RDFS.label, Literal("Customer")))
    g.add((cls, SKOS.definition, Literal("A party that buys goods.", lang="en")))
    extras = OntologyRdfExtras().extract(
        g,
        ontology_uri="http://example.org/onto",
        class_uris={str(cls)},
        property_uris=set(),
    )
    preds = {t["p"] for t in extras["triples"]}
    assert str(SKOS.definition) in preds
    assert str(RDFS.label) not in preds
    assert str(RDF.type) not in preds


def test_apply_skips_deleted_subject():
    extras = {
        "prefixes": {},
        "triples": [
            {
                "s": "http://example.org/onto#Gone",
                "p": str(SKOS.definition),
                "o": "x",
                "o_kind": "literal",
                "lang": None,
                "datatype": None,
            }
        ],
    }
    g = Graph()
    OntologyRdfExtras().apply(g, extras, live_subjects=set())
    assert len(g) == 0
