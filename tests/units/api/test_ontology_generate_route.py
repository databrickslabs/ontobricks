"""Unit coverage for ontology OWL generation routes."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import SKOS

from back.objects.ontology.OntologyOwl import OntologyOwl

pytestmark = pytest.mark.unit


class _Request:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


async def test_generate_owl_uses_session_extras_missing_from_payload(monkeypatch):
    from api.routers.internal import ontology as routes

    source = """
        @prefix ex: <http://example.org/onto#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix skos: <http://www.w3.org/2004/02/skos/core#> .

        <http://example.org/onto> a owl:Ontology .
        ex:Customer a owl:Class ;
            skos:definition "A customer preserved from import."@en .
    """
    info, classes, properties, *_ = OntologyOwl.parse_owl(
        source, extract_advanced=True
    )
    domain = SimpleNamespace(
        ontology={
            "base_uri": info["namespace"],
            "name": "Imported",
            "classes": classes,
            "properties": properties,
            "rdf_extras": info["rdf_extras"],
        },
        constraints=[],
        swrl_rules=[],
        axioms=[],
        expressions=[],
        groups=[],
        generated={},
        save=MagicMock(),
    )
    monkeypatch.setattr(routes, "get_domain", lambda _manager: domain)

    result = await routes.generate_owl_endpoint(
        _Request(
            {
                "base_uri": info["namespace"],
                "name": "Studio payload",
                "classes": classes,
                "properties": properties,
            }
        ),
        session_mgr=object(),
    )

    graph = Graph()
    graph.parse(data=result["owl"], format="turtle")
    assert (
        URIRef("http://example.org/onto#Customer"),
        SKOS.definition,
        Literal("A customer preserved from import.", lang="en"),
    ) in graph
