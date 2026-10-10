"""Preserve RDF triples outside OntoBricks' structured ontology model."""

from typing import Any

from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, SKOS


class OntologyRdfExtras:
    """Extract and restore unmodeled RDF annotations for live ontology entities."""

    CONSUMED_PREDICATES: frozenset[str] = frozenset(
        {
            str(RDF.type),
            str(RDFS.label),
            str(RDFS.comment),
            str(RDFS.subClassOf),
            str(RDFS.domain),
            str(RDFS.range),
            str(SKOS.altLabel),
        }
    )
    _ONTOBRICKS_PREDICATE_PREFIX = "http://ontobricks.com/schema#"
    _BUILTIN_PREFIXES = frozenset({"owl", "rdf", "rdfs", "xsd", "xml"})

    def extract(
        self,
        graph: Graph,
        *,
        ontology_uri: str,
        class_uris: set[str],
        property_uris: set[str],
    ) -> dict:
        """Return namespace bindings and unconsumed triples for known subjects."""
        known_subjects = {ontology_uri, *class_uris, *property_uris}
        prefixes = {
            str(prefix): str(namespace)
            for prefix, namespace in graph.namespace_manager.namespaces()
            if prefix not in self._BUILTIN_PREFIXES
        }
        triples = []

        for subject, predicate, obj in graph:
            predicate_uri = str(predicate)
            if (
                isinstance(subject, BNode)
                or isinstance(obj, BNode)
                or str(subject) not in known_subjects
                or self._is_consumed(predicate_uri)
            ):
                continue

            is_literal = isinstance(obj, Literal)
            triples.append(
                {
                    "s": str(subject),
                    "p": predicate_uri,
                    "o": str(obj),
                    "o_kind": "literal" if is_literal else "uri",
                    "lang": obj.language if is_literal else None,
                    "datatype": (
                        str(obj.datatype) if is_literal and obj.datatype else None
                    ),
                }
            )

        return {"prefixes": prefixes, "triples": triples}

    def apply(
        self,
        graph: Graph,
        extras: dict,
        *,
        live_subjects: set[str],
    ) -> None:
        """Add safe leftover triples whose subjects still exist in the graph."""
        for prefix, namespace in (extras.get("prefixes") or {}).items():
            graph.bind(prefix, namespace)

        for triple in extras.get("triples") or []:
            subject = triple.get("s")
            predicate = triple.get("p")
            if (
                subject not in live_subjects
                or not predicate
                or self._is_consumed(predicate)
            ):
                continue

            obj = self._to_object(triple)
            graph.add((URIRef(subject), URIRef(predicate), obj))

    def _is_consumed(self, predicate: str) -> bool:
        return (
            predicate in self.CONSUMED_PREDICATES
            or predicate.startswith(self._ONTOBRICKS_PREDICATE_PREFIX)
        )

    @staticmethod
    def _to_object(triple: dict[str, Any]):
        if triple.get("o_kind") == "uri":
            return URIRef(triple.get("o", ""))
        return Literal(
            triple.get("o", ""),
            lang=triple.get("lang"),
            datatype=triple.get("datatype"),
        )
