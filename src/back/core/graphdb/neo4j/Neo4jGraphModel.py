"""Pure RDF-triple ↔ property-graph transform for the Neo4j backend.

This module holds the *logic* of the typed-node model with **no Neo4j
dependency** so it is fully unit-testable without a driver. :class:`Neo4jWriteOps`
and :class:`Neo4jReadOps` execute what these functions produce.

Model (confirmed with Benoit 2026-07-28 — Strategy A):

* A node is keyed on its **full subject/object URI** (``_entity_key(uri) == uri``);
  the URI is a globally unique id. No class-segment stripping, no id-merge.
* ``rdf:type``   → a Neo4j **label** on the node (a node may carry several).
* ``rdfs:label`` → the node's ``name`` property.
* a predicate whose **object is a literal** → a **property** on the subject node.
* a predicate whose **object is a URI**     → a **relationship**
  ``(subject)-[:<reltype>]->(object)``.

Every node also carries:

* a reserved **marker label** ``:<graph_label>`` (the sanitised graph/table name)
  so a whole domain graph is one ``MATCH`` away for count/drop/isolation;
* a ``uri`` property (its identity) — reads reconstruct SPO triples from it.

Reverse mappings (label→class URI, reltype→predicate URI) are needed to
reconstruct exact SPO triples on read; they are persisted by
:class:`Neo4jSchemaMap`. This module only *derives* the forward names.
"""

from typing import Dict, List, NamedTuple, Tuple

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL

__all__ = [
    "is_uri",
    "reltype_from_predicate",
    "label_from_class_uri",
    "NodeOp",
    "EdgeOp",
    "plan_writes",
    "PlannedWrites",
]


def is_uri(value: str) -> bool:
    """True when *value* is an entity reference (URI), not a literal.

    OntoBricks mints all entity URIs under an ``http(s)://`` base
    (R2RMLGenerator ``{base}{Class}/{id}``), so the ``http`` prefix test is
    the same rule used across the W3C layer (OntologyGenerator, SHACLService).
    """
    return isinstance(value, str) and (
        value.startswith("http://") or value.startswith("https://")
    )


def _local_name(uri: str) -> str:
    """Fragment or last path segment of *uri* (mirrors rdf_utils.uri_local_name)."""
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    return uri.rsplit("/", 1)[-1]


def _sanitise_ident(name: str) -> str:
    """Sanitise to a Neo4j-safe identifier ``[A-Za-z0-9_]`` (backtick-free)."""
    cleaned = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    # Neo4j identifiers may not start with a digit; prefix if needed.
    if cleaned and cleaned[0].isdigit():
        cleaned = "_" + cleaned
    return cleaned or "_"


def label_from_class_uri(class_uri: str) -> str:
    """Neo4j node label for an ``rdf:type`` object (class URI)."""
    return _sanitise_ident(_local_name(class_uri))


def reltype_from_predicate(predicate_uri: str) -> str:
    """Neo4j relationship type for an object-property predicate URI."""
    return _sanitise_ident(_local_name(predicate_uri))


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


def plan_writes(triples: List[Dict[str, str]]) -> PlannedWrites:
    """Transform a flat SPO triple list into node/edge operations.

    Deterministic and side-effect-free. Nodes are merged per subject/object
    URI so shared entities collapse to one :class:`NodeOp`. rdf:type becomes
    labels, rdfs:label becomes ``name``, literal predicates become properties,
    URI-object predicates become edges. Also returns reverse maps so reads can
    rebuild the exact original predicate/class URIs.
    """
    nodes: Dict[str, Dict] = {}
    edges: List[EdgeOp] = []
    label_map: Dict[str, str] = {}
    reltype_map: Dict[str, str] = {}
    prop_map: Dict[str, str] = {}

    def _node(uri: str) -> Dict:
        n = nodes.get(uri)
        if n is None:
            n = {
                "uri": uri,
                "labels": {},      # sanitised label → class_uri (ordered by insertion)
                "properties": {},  # sanitised key → value
                "property_uris": {},
            }
            nodes[uri] = n
        return n

    for t in triples:
        s = t.get("subject", "")
        p = t.get("predicate", "")
        o = t.get("object", "")
        if not s or not p:
            continue
        subj = _node(s)

        if p == RDF_TYPE:
            lbl = label_from_class_uri(o)
            subj["labels"][lbl] = o
            label_map[lbl] = o
        elif p == RDFS_LABEL:
            subj["properties"]["name"] = o
            subj["property_uris"]["name"] = RDFS_LABEL
        elif is_uri(o):
            rel = reltype_from_predicate(p)
            reltype_map[rel] = p
            edges.append(EdgeOp(subject=s, reltype=rel, predicate_uri=p, object=o))
            _node(o)  # ensure the target node exists even with no own triples
        else:
            key = _sanitise_ident(_local_name(p))
            subj["properties"][key] = o
            subj["property_uris"][key] = p
            prop_map[key] = p

    node_ops = [
        NodeOp(
            uri=n["uri"],
            labels=tuple(n["labels"].keys()),
            label_uris=tuple(n["labels"].values()),
            properties=dict(n["properties"]),
            property_uris=dict(n["property_uris"]),
        )
        for n in nodes.values()
    ]
    return PlannedWrites(
        nodes=node_ops,
        edges=edges,
        label_map=label_map,
        reltype_map=reltype_map,
        prop_map=prop_map,
    )
