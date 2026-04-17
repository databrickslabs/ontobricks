"""SHACL shapes graph parser.

Reads SHACL Turtle (or other RDF serialisations) and extracts internal
shape dicts compatible with ``SHACLService``.
"""
import uuid
from typing import Dict, List, Optional

from rdflib import Graph
from rdflib.namespace import RDF

from back.core.logging import get_logger
from back.core.w3c.shacl.constants import PARAM_CATEGORY_HINTS, SEVERITY_REVERSE, SH

logger = get_logger(__name__)


class SHACLParser:
    """Parse a SHACL shapes graph into internal shape dicts."""

    @staticmethod
    def _local_name(uri: str) -> str:
        from back.core.helpers import extract_local_name
        return extract_local_name(uri)

    def parse(self, turtle_content: str, format: str = "turtle") -> List[Dict]:
        """Parse SHACL Turtle and return a list of shape dicts.

        Args:
            turtle_content: SHACL serialised as Turtle (or another RDF format).
            format: RDFLib parse format (default ``turtle``).

        Returns:
            List of internal shape dicts.
        """
        g = Graph()
        try:
            g.parse(data=turtle_content, format=format)
        except Exception as exc:
            logger.error("Failed to parse SHACL content: %s", exc)
            return []

        shapes: List[Dict] = []

        for node_shape in g.subjects(RDF.type, SH.NodeShape):
            target_class_uri = ""
            target_class = ""
            for tc in g.objects(node_shape, SH.targetClass):
                target_class_uri = str(tc)
                target_class = self._local_name(target_class_uri)
                break

            for ps in g.objects(node_shape, SH.property):
                shape = self._parse_property_shape(g, ps, target_class, target_class_uri)
                if shape:
                    shapes.append(shape)

            for sparql_node in g.objects(node_shape, SH.sparql):
                shape = self._parse_sparql_constraint(g, sparql_node, target_class, target_class_uri)
                if shape:
                    shapes.append(shape)

            closed = None
            for val in g.objects(node_shape, SH.closed):
                if str(val).lower() == "true":
                    closed = True
            if closed:
                shapes.append({
                    "id": f"shape_structural_{target_class or 'global'}_closed_{uuid.uuid4().hex[:6]}",
                    "category": "structural",
                    "label": f"{target_class or 'Global'} is a closed shape",
                    "target_class": target_class,
                    "target_class_uri": target_class_uri,
                    "property_path": "",
                    "property_uri": "",
                    "shacl_type": "sh:closed",
                    "parameters": {},
                    "severity": "sh:Violation",
                    "message": f"{target_class or 'Shape'} does not allow unexpected properties",
                    "enabled": True,
                })

        return shapes

    def _parse_property_shape(
        self, g: Graph, ps, target_class: str, target_class_uri: str,
    ) -> Optional[Dict]:
        prop_uri = ""
        prop_path = ""
        for path in g.objects(ps, SH.path):
            prop_uri = str(path)
            prop_path = self._local_name(prop_uri)
            break

        if not prop_uri:
            return None

        params: Dict = {}
        shacl_type = ""
        category = ""

        param_predicates = [
            (SH.minCount, "sh:minCount", int),
            (SH.maxCount, "sh:maxCount", int),
            (SH.pattern, "sh:pattern", str),
            (SH.flags, "sh:flags", str),
            (SH.hasValue, "sh:hasValue", str),
            (SH.minInclusive, "sh:minInclusive", float),
            (SH.maxInclusive, "sh:maxInclusive", float),
            (SH.minExclusive, "sh:minExclusive", float),
            (SH.maxExclusive, "sh:maxExclusive", float),
            (SH.minLength, "sh:minLength", int),
            (SH.maxLength, "sh:maxLength", int),
            (SH["class"], "sh:class", str),
            (SH.node, "sh:node", str),
            (SH.datatype, "sh:datatype", str),
        ]

        for pred, key, conv in param_predicates:
            for val in g.objects(ps, pred):
                try:
                    params[key] = conv(val)
                except (ValueError, TypeError):
                    params[key] = str(val)
                if not shacl_type:
                    shacl_type = key
                hint = PARAM_CATEGORY_HINTS.get(str(pred), "")
                if hint and not category:
                    category = hint

        category = self._refine_category(category, params)
        if not shacl_type:
            shacl_type = "sh:minCount"

        severity = "sh:Violation"
        for sev in g.objects(ps, SH.severity):
            severity = SEVERITY_REVERSE.get(str(sev), "sh:Violation")
            break

        message = ""
        for msg in g.objects(ps, SH.message):
            message = str(msg)
            break

        label = message or f"{shacl_type} on {target_class}.{prop_path}"

        return {
            "id": f"shape_{category}_{target_class}_{prop_path}_{uuid.uuid4().hex[:6]}",
            "category": category or "conformance",
            "label": label,
            "target_class": target_class,
            "target_class_uri": target_class_uri,
            "property_path": prop_path,
            "property_uri": prop_uri,
            "shacl_type": shacl_type,
            "parameters": params,
            "severity": severity,
            "message": message,
            "enabled": True,
        }

    def _parse_sparql_constraint(
        self, g: Graph, sparql_node, target_class: str, target_class_uri: str,
    ) -> Optional[Dict]:
        query = ""
        for sel in g.objects(sparql_node, SH.select):
            query = str(sel)
            break
        if not query:
            return None

        message = ""
        for msg in g.objects(sparql_node, SH.message):
            message = str(msg)
            break

        severity = "sh:Violation"
        for sev in g.objects(sparql_node, SH.severity):
            severity = SEVERITY_REVERSE.get(str(sev), "sh:Violation")
            break

        category = self._infer_sparql_category(query, message)

        return {
            "id": f"shape_{category}_{target_class or 'global'}_sparql_{uuid.uuid4().hex[:6]}",
            "category": category,
            "label": message or f"SPARQL constraint on {target_class or 'graph'}",
            "target_class": target_class,
            "target_class_uri": target_class_uri,
            "property_path": "",
            "property_uri": "",
            "shacl_type": "sh:sparql",
            "parameters": {"sh:select": query},
            "severity": severity,
            "message": message,
            "enabled": True,
        }

    @staticmethod
    def _refine_category(category: str, params: Dict) -> str:
        """Refine the auto-detected category using parameter combinations."""
        has_min = "sh:minCount" in params
        has_max = "sh:maxCount" in params
        if has_min and not has_max:
            min_val = params.get("sh:minCount", 0)
            if min_val == 1 and len(params) == 1:
                return "completeness"
            return "cardinality"
        if has_max and not has_min:
            return "cardinality"
        if has_min and has_max:
            return "cardinality"
        return category or "conformance"

    @staticmethod
    def _infer_sparql_category(query: str, message: str) -> str:
        """Guess the quality dimension from a SPARQL constraint's content."""
        lower_q = (query + " " + message).lower()
        if "orphan" in lower_q or "no relationship" in lower_q:
            return "structural"
        if "unique" in lower_q or "duplicate" in lower_q or "distinct" in lower_q:
            return "uniqueness"
        if "missing" in lower_q or "required" in lower_q:
            return "completeness"
        return "structural"
