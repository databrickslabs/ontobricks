"""IRI escaping for Turtle serialization.

Ontologies created by hand or imported from loose sources can store URIs such
as ``https://ex.com/WRFM - Shell#Affected asset``. rdflib refuses to serialize
those as Turtle, so the characters RFC 3987 forbids in an IRI are
percent-encoded right before serialization. Valid IRIs (including non-ASCII
ones) are left untouched.
"""

import re
from urllib.parse import quote

from rdflib import Graph, URIRef

from back.core.logging import get_logger

logger = get_logger(__name__)

_IRI_UNSAFE = re.compile(r'[\s<>"{}|\\^`]')


def escape_iri(uri: str) -> str:
    """Percent-encode IRI-illegal characters (spaces, ``<>"{}|\\^```)."""
    return _IRI_UNSAFE.sub(lambda m: quote(m.group(), safe=""), uri)


def _fix(term):
    if isinstance(term, URIRef) and _IRI_UNSAFE.search(term):
        return URIRef(escape_iri(str(term)))
    return term


def _safe_graph(graph: Graph) -> Graph:
    """Return *graph*, or a copy with IRI-illegal characters percent-encoded."""
    if not any(_fix(t) is not t for triple in graph for t in triple):
        return graph
    safe = Graph()
    for prefix, ns in graph.namespaces():
        safe.bind(prefix, URIRef(escape_iri(str(ns))), override=True, replace=True)
    for s, p, o in graph:
        safe.add((_fix(s), _fix(p), _fix(o)))
    return safe


def serialize_graph(graph: Graph, fmt: str = "turtle") -> str:
    """Serialize *graph* as Turtle or JSON-LD, escaping invalid IRIs.

    Args:
        graph: RDFLib graph to serialize.
        fmt: ``turtle`` (default) or ``json-ld``.

    Returns:
        Serialized RDF as a string.

    Raises:
        ValidationError: If *fmt* is not supported.
    """
    if fmt not in ("turtle", "json-ld"):
        from back.core.errors.ValidationError import ValidationError

        raise ValidationError(
            f"Unsupported RDF serialization format: {fmt}",
            detail="Supported formats: turtle, json-ld",
        )

    source = _safe_graph(graph)
    if source is not graph:
        logger.warning("Percent-encoded invalid IRIs (e.g. spaces) for %s output", fmt)

    if fmt == "turtle":
        return source.serialize(format="turtle")

    context = {prefix: str(ns) for prefix, ns in source.namespaces() if prefix}
    return source.serialize(format="json-ld", context=context, indent=2)


def serialize_turtle(graph: Graph) -> str:
    """Serialize *graph* as Turtle, escaping invalid IRIs instead of failing."""
    return serialize_graph(graph, "turtle")
