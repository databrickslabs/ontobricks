"""TripleFactory — build RDF triples + small graphs for triplestore tests.

Wraps `rdflib` so most code paths can stay synthetic. Use `graph_with(...)` for
property tests on the SPARQL translator that need a small but well-shaped graph.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

try:  # rdflib is a hard dep of OntoBricks but guard for the rare CI path that skips it.
    from rdflib import Graph, Literal, Namespace, URIRef
    from rdflib.namespace import RDF, RDFS, XSD

    _RDFLIB = True
except ImportError:  # pragma: no cover - exercised only when rdflib is missing
    _RDFLIB = False
    Graph = None  # type: ignore[assignment]
    Literal = None  # type: ignore[assignment]
    Namespace = None  # type: ignore[assignment]
    URIRef = None  # type: ignore[assignment]
    RDF = None  # type: ignore[assignment]
    RDFS = None  # type: ignore[assignment]
    XSD = None  # type: ignore[assignment]


@dataclass(frozen=True)
class TripleFactory:
    """Triple + small-graph builder.

    Use the class methods directly; instantiating is rarely useful.
    """

    @staticmethod
    def triple(subject: str, predicate: str, obj: str | int | float) -> tuple[Any, Any, Any]:
        """Build one (s, p, o) triple. Object becomes a Literal unless it looks like a URI."""
        if not _RDFLIB:
            raise RuntimeError("rdflib not installed")
        s = URIRef(subject)
        p = URIRef(predicate)
        if isinstance(obj, str) and (obj.startswith("http://") or obj.startswith("https://")):
            o = URIRef(obj)
        else:
            o = Literal(obj)
        return (s, p, o)

    @staticmethod
    def graph_with(triples: Iterable[tuple[str, str, Any]]) -> Any:
        """Build a Graph populated with the supplied triples.

        Example:

            g = TripleFactory.graph_with([
                ("http://ex/alice", "http://ex/age", 30),
                ("http://ex/alice", "http://ex/name", "Alice"),
            ])
            assert len(g) == 2
        """
        if not _RDFLIB:
            raise RuntimeError("rdflib not installed")
        g = Graph()
        for s, p, o in triples:
            g.add(TripleFactory.triple(s, p, o))
        return g

    @staticmethod
    def small_graph(
        base: str = "http://ex/",
        people: int = 3,
        with_inferences: bool = False,
    ) -> Any:
        """Build a small canonical graph for SPARQL-translator parity tests.

        Shape: N Persons each with a name and an age; if with_inferences is True,
        a `:knows` chain is added so transitive-closure tests can run.
        """
        if not _RDFLIB:
            raise RuntimeError("rdflib not installed")
        ex = Namespace(base)
        g = Graph()
        g.bind("ex", ex)
        for i in range(people):
            person = URIRef(f"{base}person{i}")
            g.add((person, RDF.type, ex.Person))
            g.add((person, ex.name, Literal(f"Person {i}")))
            g.add((person, ex.age, Literal(20 + i, datatype=XSD.integer)))
            g.add((person, RDFS.label, Literal(f"Person {i}")))
        if with_inferences and people >= 2:
            for i in range(people - 1):
                g.add(
                    (
                        URIRef(f"{base}person{i}"),
                        ex.knows,
                        URIRef(f"{base}person{i + 1}"),
                    )
                )
        return g
