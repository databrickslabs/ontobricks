"""Neo4j write plan: node and edge operations derived from RDF triples."""

from typing import Dict, List, NamedTuple, Tuple


class NodeOp(NamedTuple):
    """An upsert for one node, keyed on its full URI."""

    uri: str
    labels: Tuple[str, ...]          # class labels (from rdf:type), sanitised
    label_uris: Tuple[str, ...]      # matching class URIs (for reverse map)
    properties: Dict[str, str]       # literal predicates → values (incl. ``name``)
    property_uris: Dict[str, str]    # sanitised prop key → full predicate URI


class EdgeOp(NamedTuple):
    """A relationship ``(subject)-[:reltype]->(object)``."""

    subject: str
    reltype: str
    predicate_uri: str
    object: str


class PlannedWrites(NamedTuple):
    """The full set of graph operations derived from a triple batch."""

    nodes: List[NodeOp]
    edges: List[EdgeOp]
    label_map: Dict[str, str]        # sanitised label → class URI (reverse)
    reltype_map: Dict[str, str]      # sanitised reltype → predicate URI (reverse)
    prop_map: Dict[str, str]         # sanitised prop key → predicate URI (reverse)
