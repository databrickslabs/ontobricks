"""T-Box reasoning using owlrl (OWL 2 RL profile) on RDFLib graphs.

Runs a forward-chaining deductive closure over OWL Turtle content and
returns the inferred triples that were not present in the original graph.
"""

import time
from typing import List, Set, Tuple

from rdflib import Graph, Literal, URIRef, BNode

from back.core.logging import get_logger
from back.core.reasoning.models import InferredTriple, ReasoningResult
from back.core.reasoning.constants import (
    OWLRL_PROVENANCE,
    AXIOMATIC_PREFIXES,
    TAUTOLOGICAL_PREDICATES,
    RDF_TYPE,
    NOISE_TYPES,
)

logger = get_logger(__name__)


class OWLRLReasoner:
    """Compute OWL 2 RL deductive closure over RDFLib graphs."""

    def compute_closure(self, owl_content: str) -> ReasoningResult:
        """Parse OWL Turtle and run OWL 2 RL forward chaining.

        Returns a :class:`ReasoningResult` containing only the *new*
        (inferred) triples that the closure added to the graph.
        """
        graph = self._parse_graph(owl_content)
        return self._run_closure(graph, phase="tbox")

    def compute_closure_with_instances(
        self,
        owl_content: str,
        instance_triples: List[dict],
    ) -> ReasoningResult:
        """Run OWL 2 RL over ontology + instance data.

        Suitable for small instance datasets (< ~50 000 triples).
        For large datasets, use graph or SQL reasoning instead.
        """
        graph = self._parse_graph(owl_content)
        for t in instance_triples:
            obj_val = t["object"]
            obj_node = (
                URIRef(obj_val) if obj_val.startswith("http") else Literal(obj_val)
            )
            graph.add((URIRef(t["subject"]), URIRef(t["predicate"]), obj_node))
        return self._run_closure(graph, phase="tbox_instances")

    def _run_closure(self, graph: Graph, phase: str) -> ReasoningResult:
        """Run deductive closure on *graph* and return inferred triples."""
        from owlrl import DeductiveClosure, OWLRL_Semantics

        t0 = time.time()
        before: Set[Tuple] = set(graph)
        before_count = len(before)

        label = "ontology + instances" if phase == "tbox_instances" else "ontology"
        logger.info("Running OWL 2 RL closure on %d triples (%s)", before_count, label)
        DeductiveClosure(OWLRL_Semantics).expand(graph)

        after: Set[Tuple] = set(graph)
        inferred = self._filter_inferred(after - before)
        duration = time.time() - t0

        logger.info(
            "OWL 2 RL closure: %d original, %d after, %d inferred (%.2fs)",
            before_count,
            len(after),
            len(inferred),
            duration,
        )
        return ReasoningResult(
            inferred_triples=inferred,
            stats={
                "phase": phase,
                "original_count": before_count,
                "after_count": len(after),
                "inferred_count": len(inferred),
                "duration_seconds": round(duration, 3),
            },
        )

    # -- Internal helpers -------------------------------------------------

    @staticmethod
    def _parse_graph(owl_content: str) -> Graph:
        """Parse Turtle or RDF/XML into an rdflib Graph."""
        from back.core.w3c.rdf_utils import parse_rdf_flexible

        return parse_rdf_flexible(owl_content, formats=("turtle", "xml"))

    @staticmethod
    def _is_axiomatic(triple: Tuple) -> bool:
        """Return True if the triple only touches W3C axiomatic vocabulary."""
        s, p, o = triple
        s_str = str(s)
        o_str = str(o)
        return any(s_str.startswith(pfx) for pfx in AXIOMATIC_PREFIXES) and any(
            o_str.startswith(pfx) for pfx in AXIOMATIC_PREFIXES
        )

    @staticmethod
    def _is_tautological(triple: Tuple) -> bool:
        """Return True for reflexive tautologies like X sameAs X, X subClassOf X."""
        s, p, o = triple
        if str(s) == str(o) and str(p) in TAUTOLOGICAL_PREDICATES:
            return True
        return False

    @classmethod
    def _filter_inferred(cls, new_triples: Set[Tuple]) -> List[InferredTriple]:
        """Convert raw RDFLib triples to InferredTriple, filtering noise.

        Filters out:
        - Blank-node triples
        - Purely axiomatic (both S and O in W3C namespaces)
        - Reflexive tautologies (X sameAs X, X subClassOf X, etc.)
        - Redundant type declarations (X rdf:type owl:Class / rdfs:Resource / owl:Thing)
        """
        results: List[InferredTriple] = []
        for s, p, o in new_triples:
            if isinstance(s, BNode) or isinstance(o, BNode):
                continue
            if cls._is_axiomatic((s, p, o)):
                continue
            if cls._is_tautological((s, p, o)):
                continue
            p_str = str(p)
            o_str = str(o)
            if p_str == RDF_TYPE and o_str in NOISE_TYPES:
                continue
            results.append(
                InferredTriple(
                    subject=str(s),
                    predicate=p_str,
                    object=o_str,
                    provenance=OWLRL_PROVENANCE,
                )
            )
        return results
