"""SHACL shapes graph generator.

Builds an RDFLib graph of ``sh:NodeShape`` / ``sh:PropertyShape``
resources from the internal shape-dict representation and serialises
the result to Turtle.
"""
from collections import defaultdict
from typing import Dict, List, Optional

from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, XSD

from back.core.logging import get_logger
from back.core.w3c.shacl.constants import DATATYPE_MAP, SEVERITY_MAP, SH

logger = get_logger(__name__)


class SHACLGenerator:
    """Build a SHACL shapes graph from internal shape dicts."""

    def __init__(self, base_uri: str):
        sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"
        self._base = base_uri
        self._ns = Namespace(base_uri + sep)
        self._data_ns = Namespace(base_uri.rstrip("#").rstrip("/") + "/")

    def generate(self, shapes: List[Dict], base_uri: Optional[str] = None) -> str:
        """Return SHACL Turtle for the given shape dicts.

        Args:
            shapes: List of internal shape dicts (see ``SHACLService``).
            base_uri: Override the base URI used at construction time.

        Returns:
            SHACL Turtle string.
        """
        if base_uri:
            sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"
            self._ns = Namespace(base_uri + sep)
            self._data_ns = Namespace(base_uri.rstrip("#").rstrip("/") + "/")

        g = Graph()
        g.bind("sh", SH)
        g.bind("xsd", XSD)
        g.bind("rdfs", RDFS)
        g.bind("", self._ns)
        g.bind("data", self._data_ns)

        enabled = [s for s in shapes if s.get("enabled", True)]
        if not enabled:
            return g.serialize(format="turtle")

        by_class = defaultdict(list)
        for s in enabled:
            by_class[s.get("target_class_uri", "")].append(s)

        for cls_uri, cls_shapes in by_class.items():
            if not cls_uri:
                self._add_global_shapes(g, cls_shapes)
                continue
            node_shape_uri = self._node_shape_uri(cls_uri)
            g.add((node_shape_uri, RDF.type, SH.NodeShape))
            g.add((node_shape_uri, SH.targetClass, URIRef(cls_uri)))

            cls_label = cls_shapes[0].get("target_class", "")
            if cls_label:
                g.add((node_shape_uri, RDFS.label, Literal(f"Shape for {cls_label}")))

            for s in cls_shapes:
                self._add_property_shape(g, node_shape_uri, s)

        return g.serialize(format="turtle")

    def _node_shape_uri(self, cls_uri: str) -> URIRef:
        local = cls_uri.rsplit("#", 1)[-1] if "#" in cls_uri else cls_uri.rsplit("/", 1)[-1]
        return URIRef(str(self._ns) + local + "Shape")

    def _resolve_property_uri(self, shape: Dict) -> Optional[URIRef]:
        uri = shape.get("property_uri")
        if uri:
            return URIRef(uri)
        path = shape.get("property_path")
        if path:
            return URIRef(str(self._data_ns) + path)
        return None

    def _add_property_shape(self, g: Graph, node_shape: URIRef, shape: Dict) -> None:
        prop_uri = self._resolve_property_uri(shape)
        shacl_type = shape.get("shacl_type", "")
        params = shape.get("parameters", {})

        if shacl_type == "sh:sparql":
            self._add_sparql_constraint(g, node_shape, shape)
            return

        if shacl_type == "sh:closed":
            g.add((node_shape, SH.closed, Literal(True)))
            return

        if not prop_uri:
            logger.warning("Shape %s has no property URI, skipping", shape.get("id"))
            return

        ps = BNode()
        g.add((node_shape, SH.property, ps))
        g.add((ps, SH.path, prop_uri))

        severity = SEVERITY_MAP.get(shape.get("severity", "sh:Violation"), SH.Violation)
        g.add((ps, SH.severity, severity))

        msg = shape.get("message", "")
        if msg:
            g.add((ps, SH.message, Literal(msg)))

        for param, value in params.items():
            self._set_param(g, ps, param, value)

    def _set_param(self, g: Graph, ps: BNode, param: str, value) -> None:
        mapping = {
            "sh:minCount": (SH.minCount, lambda v: Literal(int(v))),
            "sh:maxCount": (SH.maxCount, lambda v: Literal(int(v))),
            "sh:pattern": (SH.pattern, Literal),
            "sh:flags": (SH.flags, Literal),
            "sh:minInclusive": (SH.minInclusive, lambda v: Literal(float(v))),
            "sh:maxInclusive": (SH.maxInclusive, lambda v: Literal(float(v))),
            "sh:minExclusive": (SH.minExclusive, lambda v: Literal(float(v))),
            "sh:maxExclusive": (SH.maxExclusive, lambda v: Literal(float(v))),
            "sh:minLength": (SH.minLength, lambda v: Literal(int(v))),
            "sh:maxLength": (SH.maxLength, lambda v: Literal(int(v))),
            "sh:hasValue": (SH.hasValue, Literal),
            "sh:class": (SH["class"], lambda v: URIRef(v)),
            "sh:node": (SH.node, lambda v: URIRef(v)),
            "sh:datatype": (SH.datatype, lambda v: DATATYPE_MAP.get(str(v), URIRef(v))),
        }
        if param == "sh:in":
            lst_node = BNode()
            g.add((lst_node, RDF.type, RDF.List))
            self._build_rdf_list(g, lst_node, value if isinstance(value, list) else [value])
            return

        entry = mapping.get(param)
        if entry:
            pred, converter = entry
            try:
                g.add((ps, pred, converter(value)))
            except (ValueError, TypeError) as exc:
                logger.warning("Cannot set SHACL param %s=%s: %s", param, value, exc)
        else:
            logger.debug("Unknown SHACL parameter %s, skipping", param)

    def _build_rdf_list(self, g: Graph, head: BNode, items: list) -> None:
        current = head
        for i, item in enumerate(items):
            g.add((current, RDF.first, Literal(item)))
            if i < len(items) - 1:
                nxt = BNode()
                g.add((current, RDF.rest, nxt))
                current = nxt
            else:
                g.add((current, RDF.rest, RDF.nil))

    def _add_sparql_constraint(self, g: Graph, node_shape: URIRef, shape: Dict) -> None:
        params = shape.get("parameters", {})
        query = params.get("sh:select", "")
        if not query:
            return
        constraint = BNode()
        g.add((node_shape, SH.sparql, constraint))
        g.add((constraint, SH.select, Literal(query)))
        msg = shape.get("message", "")
        if msg:
            g.add((constraint, SH.message, Literal(msg)))
        severity = SEVERITY_MAP.get(shape.get("severity", "sh:Violation"), SH.Violation)
        g.add((constraint, SH.severity, severity))

    def _add_global_shapes(self, g: Graph, shapes: List[Dict]) -> None:
        """Handle shapes that have no target class (graph-level SPARQL checks)."""
        for s in shapes:
            shape_id = s.get("id", "GlobalShape")
            node = URIRef(str(self._ns) + shape_id)
            g.add((node, RDF.type, SH.NodeShape))
            shacl_type = s.get("shacl_type", "")
            if shacl_type == "sh:sparql":
                self._add_sparql_constraint(g, node, s)
            elif shacl_type == "sh:closed":
                g.add((node, SH.closed, Literal(True)))
