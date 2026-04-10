"""SHACL service — shape CRUD, Turtle generation, validation, migration.

Central orchestrator for all SHACL data-quality operations.  Routes and
UI code should only import ``SHACLService``; the generator and parser
are internal implementation details.
"""
import hashlib
import re
import uuid
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
from back.core.w3c.shacl.constants import QUALITY_CATEGORIES, RDFS_LABEL, XSD_TO_SPARK_TYPE
from back.core.w3c.shacl.SHACLGenerator import SHACLGenerator
from back.core.w3c.shacl.SHACLParser import SHACLParser

logger = get_logger(__name__)


class SHACLService:
    """Manage SHACL shapes for a project's data-quality rules.

    Constructor receives the base URI for the ontology so that the
    generator can produce correct shape URIs.
    """

    def __init__(self, base_uri: str = "http://example.org/ontology#"):
        self._base_uri = base_uri
        self._generator = SHACLGenerator(base_uri)
        self._parser = SHACLParser()

    # ------------------------------------------------------------------
    # Shape CRUD
    # ------------------------------------------------------------------

    @staticmethod
    def create_shape(
        category: str,
        target_class: str,
        target_class_uri: str,
        property_path: str = "",
        property_uri: str = "",
        shacl_type: str = "sh:minCount",
        parameters: Optional[Dict] = None,
        severity: str = "sh:Violation",
        message: str = "",
        label: str = "",
        enabled: bool = True,
        shape_id: Optional[str] = None,
    ) -> Dict:
        """Build a new shape dict (not yet persisted).

        When *shape_id* is ``None`` (default) a random UUID suffix is used,
        which is correct for user-created shapes.  Callers that need a
        **deterministic** ID (e.g. legacy-constraint migration) should
        pass an explicit *shape_id*.
        """
        cat = category if category in QUALITY_CATEGORIES else "conformance"
        safe_cls = re.sub(r"[^a-zA-Z0-9_]", "", target_class or "global")
        safe_prop = re.sub(r"[^a-zA-Z0-9_]", "", property_path or shacl_type.replace("sh:", ""))
        if not shape_id:
            shape_id = f"shape_{cat}_{safe_cls}_{safe_prop}_{uuid.uuid4().hex[:6]}"
        return {
            "id": shape_id,
            "category": cat,
            "label": label or message or f"{shacl_type} on {target_class}.{property_path}",
            "target_class": target_class,
            "target_class_uri": target_class_uri,
            "property_path": property_path,
            "property_uri": property_uri,
            "shacl_type": shacl_type,
            "parameters": parameters or {},
            "severity": severity,
            "message": message,
            "enabled": enabled,
        }

    @staticmethod
    def update_shape(shapes: List[Dict], shape_id: str, updates: Dict) -> List[Dict]:
        """Return a new list with the matching shape updated."""
        result = []
        for s in shapes:
            if s["id"] == shape_id:
                merged = {**s, **updates}
                merged["id"] = shape_id
                result.append(merged)
            else:
                result.append(s)
        return result

    @staticmethod
    def delete_shape(shapes: List[Dict], shape_id: str) -> List[Dict]:
        return [s for s in shapes if s["id"] != shape_id]

    @staticmethod
    def _migration_id(constraint: Dict) -> str:
        """Build a deterministic shape ID from a legacy constraint dict.

        Uses a stable hash of the constraint's content so that repeated
        migrations of the same constraint always yield the same ID —
        preventing the duplicate-on-every-call bug.
        """
        key_parts = [
            constraint.get("type", ""),
            constraint.get("className", ""),
            constraint.get("property", ""),
            constraint.get("attributeName", ""),
            constraint.get("checkType", ""),
            constraint.get("checkValue", ""),
            constraint.get("valueClass", ""),
            constraint.get("hasValue", ""),
            constraint.get("ruleName", ""),
            str(constraint.get("cardinalityValue", "")),
        ]
        digest = hashlib.sha256("|".join(key_parts).encode()).hexdigest()[:10]
        ctype = re.sub(r"[^a-zA-Z0-9_]", "", constraint.get("type", "unknown"))
        cls = re.sub(r"[^a-zA-Z0-9_]", "", constraint.get("className", "") or constraint.get("attributeName", "") or "global")
        return f"migrated_{ctype}_{cls}_{digest}"

    # ------------------------------------------------------------------
    # Turtle generation / parsing
    # ------------------------------------------------------------------

    def generate_turtle(self, shapes: List[Dict], base_uri: Optional[str] = None) -> str:
        """Generate SHACL Turtle from the shapes list."""
        uri = base_uri or self._base_uri
        return self._generator.generate(shapes, base_uri=uri)

    def import_shapes(self, turtle_content: str, fmt: str = "turtle") -> List[Dict]:
        """Parse SHACL Turtle and return shape dicts."""
        return self._parser.parse(turtle_content, format=fmt)

    # ------------------------------------------------------------------
    # SHACL-AF Rule Inference (sh:rule)
    # ------------------------------------------------------------------

    def run_inference(
        self, data_turtle: str, rule_shapes: List[Dict],
        base_uri: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run SHACL-AF inference rules and return newly inferred triples.

        Uses ``pyshacl.validate(advanced=True, inplace=True)`` to execute
        ``sh:TripleRule`` and ``sh:SPARQLRule`` shapes.  The data graph is
        expanded in-place and then diffed against the original to extract
        the inferred triples.

        Returns:
            Dict with ``inferred_triples`` (list of triple dicts) and
            ``count`` (int).
        """
        try:
            import pyshacl
            from rdflib import Graph as RG
        except ImportError:
            logger.error("pyshacl/rdflib not installed — cannot run SHACL rule inference")
            return {"inferred_triples": [], "count": 0, "error": "pyshacl not available"}

        shapes_turtle = self._generate_rule_shapes_turtle(rule_shapes, base_uri)
        if not shapes_turtle:
            return {"inferred_triples": [], "count": 0}

        data_graph = RG()
        try:
            data_graph.parse(data=data_turtle, format="turtle")
        except Exception as e:
            logger.error("Failed to parse data graph for SHACL rules: %s", e)
            return {"inferred_triples": [], "count": 0, "error": str(e)}

        original_count = len(data_graph)

        try:
            pyshacl.validate(
                data_graph=data_graph,
                shacl_graph=shapes_turtle,
                data_graph_format="turtle",
                shacl_graph_format="turtle",
                advanced=True,
                inplace=True,
                inference="none",
            )
        except Exception as e:
            logger.error("SHACL-AF rule execution failed: %s", e)
            return {"inferred_triples": [], "count": 0, "error": str(e)}

        new_count = len(data_graph)
        inferred = new_count - original_count
        logger.info("SHACL rules inferred %d new triples (%d → %d)", inferred, original_count, new_count)

        inferred_triples = []
        if inferred > 0:
            original_set = set()
            orig = RG()
            orig.parse(data=data_turtle, format="turtle")
            for s, p, o in orig:
                original_set.add((str(s), str(p), str(o)))
            for s, p, o in data_graph:
                key = (str(s), str(p), str(o))
                if key not in original_set:
                    inferred_triples.append({
                        "subject": str(s), "predicate": str(p), "object": str(o),
                    })

        return {"inferred_triples": inferred_triples, "count": len(inferred_triples)}

    def _generate_rule_shapes_turtle(
        self, rule_shapes: List[Dict], base_uri: Optional[str] = None,
    ) -> str:
        """Generate Turtle for SHACL rule shapes (sh:TripleRule / sh:SPARQLRule)."""
        uri = base_uri or self._base_uri
        lines = [
            f"@prefix sh: <http://www.w3.org/ns/shacl#> .",
            f"@prefix ex: <{uri}> .",
            f"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
            f"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
            "",
        ]
        for shape in rule_shapes:
            shape_id = shape.get("id", "rule_shape")
            target = shape.get("target_class_uri", "")
            shacl_type = shape.get("shacl_type", "")

            lines.append(f"ex:{shape_id} a sh:NodeShape ;")
            if target:
                lines.append(f"    sh:targetClass <{target}> ;")

            if shacl_type == "sh:TripleRule":
                subj = shape.get("parameters", {}).get("subject", "sh:this")
                pred = shape.get("parameters", {}).get("predicate", "")
                obj = shape.get("parameters", {}).get("object", "")
                lines.append(f"    sh:rule [")
                lines.append(f"        a sh:TripleRule ;")
                if subj == "sh:this":
                    lines.append(f"        sh:subject sh:this ;")
                else:
                    lines.append(f"        sh:subject <{subj}> ;")
                lines.append(f"        sh:predicate <{pred}> ;")
                if obj.startswith("http"):
                    lines.append(f"        sh:object <{obj}> ;")
                else:
                    lines.append(f'        sh:object "{obj}" ;')
                cond_path = shape.get("parameters", {}).get("condition_path", "")
                if cond_path:
                    lines.append(f"        sh:condition [")
                    lines.append(f"            sh:path <{cond_path}> ;")
                    lines.append(f"            sh:minCount 1 ;")
                    lines.append(f"        ] ;")
                lines.append(f"    ] .")
            elif shacl_type == "sh:SPARQLRule":
                construct = shape.get("parameters", {}).get("construct", "")
                if construct:
                    lines.append(f"    sh:rule [")
                    lines.append(f"        a sh:SPARQLRule ;")
                    lines.append(f'        sh:construct """{construct}""" ;')
                    lines.append(f"    ] .")
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # PySHACL validation
    # ------------------------------------------------------------------

    def validate_graph(
        self, data_turtle: str, shapes: List[Dict], base_uri: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run PySHACL validation and return a result dict.

        Args:
            data_turtle: The data graph serialised as Turtle.
            shapes: Internal shape dicts to validate against.
            base_uri: Optional override for shape URI generation.

        Returns:
            Dict with ``conforms`` (bool), ``violations`` (list of dicts),
            and ``report_text`` (human-readable report).
        """
        try:
            import pyshacl
        except ImportError:
            logger.error("pyshacl is not installed — cannot run SHACL validation")
            return {
                "conforms": False,
                "violations": [],
                "report_text": "pyshacl library is not installed",
                "error": "pyshacl not available",
            }

        shapes_turtle = self.generate_turtle(shapes, base_uri=base_uri)
        try:
            conforms, results_graph, results_text = pyshacl.validate(
                data_graph=data_turtle,
                shacl_graph=shapes_turtle,
                data_graph_format="turtle",
                shacl_graph_format="turtle",
                inference="none",
                serialize_report_graph="turtle",
            )
        except Exception as exc:
            logger.error("PySHACL validation failed: %s", exc)
            return {
                "conforms": False,
                "violations": [],
                "report_text": str(exc),
                "error": str(exc),
            }

        violations = self._extract_violations(results_graph)
        return {
            "conforms": conforms,
            "violations": violations,
            "report_text": results_text,
        }

    @staticmethod
    def _extract_violations(report_turtle: str) -> List[Dict]:
        """Parse the PySHACL validation report into violation dicts."""
        from rdflib import Graph as RG, URIRef
        from rdflib.namespace import Namespace as NS

        SH_NS = NS("http://www.w3.org/ns/shacl#")
        g = RG()
        try:
            g.parse(data=report_turtle, format="turtle")
        except Exception:
            return []

        RDF_TYPE = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        violations = []
        for result in g.subjects(RDF_TYPE, SH_NS.ValidationResult):
            focus = ""
            for f in g.objects(result, SH_NS.focusNode):
                focus = str(f)
            path = ""
            for p in g.objects(result, SH_NS.resultPath):
                path = str(p)
            message = ""
            for m in g.objects(result, SH_NS.resultMessage):
                message = str(m)
            severity = "sh:Violation"
            for s in g.objects(result, SH_NS.resultSeverity):
                sev_str = str(s)
                if "Warning" in sev_str:
                    severity = "sh:Warning"
                elif "Info" in sev_str:
                    severity = "sh:Info"

            violations.append({
                "focus_node": focus,
                "result_path": path,
                "message": message,
                "severity": severity,
            })
        return violations

    # ------------------------------------------------------------------
    # Legacy constraint migration
    # ------------------------------------------------------------------

    def migrate_legacy_constraints(self, constraints: List[Dict], base_uri: str = "") -> List[Dict]:
        """Convert old OntoBricks constraint dicts to SHACL shape dicts.

        Handles all types stored in ``ontology.constraints``:
        cardinality, functional, inverseFunctional, transitive, symmetric,
        asymmetric, reflexive, irreflexive, allValuesFrom, someValuesFrom,
        hasValue, valueCheck, entityValueCheck, entityLabelCheck, and
        globalRule.
        """
        uri = base_uri or self._base_uri
        sep = "" if uri.endswith("#") or uri.endswith("/") else "#"
        data_ns = uri.rstrip("#").rstrip("/") + "/"
        shapes: List[Dict] = []

        for c in constraints:
            ctype = c.get("type", "")
            handler = self._MIGRATION_MAP.get(ctype)
            if handler:
                stable_id = self._migration_id(c)
                shape = handler(self, c, uri, sep, data_ns, stable_id)
                if shape:
                    shapes.append(shape)
            else:
                logger.debug("Skipping unmapped legacy constraint type: %s", ctype)

        return shapes

    def _migrate_cardinality(self, c: Dict, uri: str, sep: str, data_ns: str, stable_id: str = "") -> Optional[Dict]:
        ctype = c["type"]
        cls = c.get("className", "")
        cls_uri = c.get("classUri", f"{uri}{sep}{cls}" if cls else "")
        prop = c.get("property", "")
        prop_uri = c.get("propertyUri", f"{data_ns}{prop}" if prop else "")
        val = int(c.get("cardinalityValue", 0))

        params: Dict[str, Any] = {}
        if ctype == "minCardinality":
            params["sh:minCount"] = val
        elif ctype == "maxCardinality":
            params["sh:maxCount"] = val
        elif ctype == "exactCardinality":
            params["sh:minCount"] = val
            params["sh:maxCount"] = val

        return self.create_shape(
            category="structural",
            target_class=cls, target_class_uri=cls_uri,
            property_path=prop, property_uri=prop_uri,
            shacl_type=list(params.keys())[0] if params else "sh:minCount",
            parameters=params,
            message=f"{ctype}({prop}={val}) on {cls}",
            shape_id=stable_id,
        )

    def _migrate_functional(self, c: Dict, uri: str, sep: str, data_ns: str, stable_id: str = "") -> Optional[Dict]:
        prop = c.get("property", "")
        prop_uri = c.get("propertyUri", f"{data_ns}{prop}" if prop else "")
        return self.create_shape(
            category="consistency",
            target_class="", target_class_uri="",
            property_path=prop, property_uri=prop_uri,
            shacl_type="sh:maxCount", parameters={"sh:maxCount": 1},
            message=f"Functional property {prop}: at most one value per subject",
            shape_id=stable_id,
        )

    def _migrate_inverse_functional(self, c: Dict, uri: str, sep: str, data_ns: str, stable_id: str = "") -> Optional[Dict]:
        prop = c.get("property", "")
        prop_uri = c.get("propertyUri", f"{data_ns}{prop}" if prop else "")
        query = (
            f"SELECT $this WHERE {{ $this <{prop_uri}> ?val . "
            f"?other <{prop_uri}> ?val . FILTER ($this != ?other) }}"
        )
        return self.create_shape(
            category="consistency",
            target_class="", target_class_uri="",
            property_path=prop, property_uri=prop_uri,
            shacl_type="sh:sparql", parameters={"sh:select": query},
            message=f"Inverse-functional property {prop}: each value maps to at most one subject",
            shape_id=stable_id,
        )

    def _migrate_value_check(self, c: Dict, uri: str, sep: str, data_ns: str, stable_id: str = "") -> Optional[Dict]:
        cls = c.get("className", "")
        cls_uri = f"{uri}{sep}{cls}" if cls else ""
        attr = c.get("attributeName", "")
        attr_uri = f"{data_ns}{attr}" if attr else ""
        check_type = c.get("checkType", "")
        check_value = c.get("checkValue", "")

        if check_type == "notNull":
            return self.create_shape(
                category="consistency",
                target_class=cls, target_class_uri=cls_uri,
                property_path=attr, property_uri=attr_uri,
                shacl_type="sh:minCount", parameters={"sh:minCount": 1},
                message=f"{cls}.{attr} must not be empty",
                shape_id=stable_id,
            )

        pattern_map = {
            "startsWith": f"^{re.escape(check_value)}",
            "endsWith": f"{re.escape(check_value)}$",
            "contains": re.escape(check_value),
            "matches": check_value,
        }
        if check_type in pattern_map:
            return self.create_shape(
                category="consistency",
                target_class=cls, target_class_uri=cls_uri,
                property_path=attr, property_uri=attr_uri,
                shacl_type="sh:pattern",
                parameters={"sh:pattern": pattern_map[check_type], "sh:flags": "i"},
                message=f"{cls}.{attr} must match {check_type} '{check_value}'",
                shape_id=stable_id,
            )

        if check_type == "equals":
            return self.create_shape(
                category="consistency",
                target_class=cls, target_class_uri=cls_uri,
                property_path=attr, property_uri=attr_uri,
                shacl_type="sh:hasValue", parameters={"sh:hasValue": check_value},
                message=f"{cls}.{attr} must equal '{check_value}'",
                shape_id=stable_id,
            )

        if check_type == "notEquals":
            query = (
                f"SELECT $this WHERE {{ $this <{attr_uri}> ?val . "
                f'FILTER (str(?val) = "{check_value}") }}'
            )
            return self.create_shape(
                category="consistency",
                target_class=cls, target_class_uri=cls_uri,
                property_path=attr, property_uri=attr_uri,
                shacl_type="sh:sparql", parameters={"sh:select": query},
                message=f"{cls}.{attr} must not equal '{check_value}'",
                shape_id=stable_id,
            )

        return None

    def _migrate_all_values_from(self, c: Dict, uri: str, sep: str, data_ns: str, stable_id: str = "") -> Optional[Dict]:
        cls = c.get("className", "")
        cls_uri = c.get("classUri", f"{uri}{sep}{cls}" if cls else "")
        prop = c.get("property", "")
        prop_uri = c.get("propertyUri", f"{data_ns}{prop}" if prop else "")
        val_class = c.get("valueClass", "")
        val_class_uri = f"{uri}{sep}{val_class}" if val_class else ""
        return self.create_shape(
            category="consistency",
            target_class=cls, target_class_uri=cls_uri,
            property_path=prop, property_uri=prop_uri,
            shacl_type="sh:class", parameters={"sh:class": val_class_uri},
            message=f"All values of {cls}.{prop} must be of type {val_class}",
            shape_id=stable_id,
        )

    def _migrate_some_values_from(self, c: Dict, uri: str, sep: str, data_ns: str, stable_id: str = "") -> Optional[Dict]:
        cls = c.get("className", "")
        cls_uri = c.get("classUri", f"{uri}{sep}{cls}" if cls else "")
        prop = c.get("property", "")
        prop_uri = c.get("propertyUri", f"{data_ns}{prop}" if prop else "")
        val_class = c.get("valueClass", "")
        val_class_uri = f"{uri}{sep}{val_class}" if val_class else ""
        query = (
            f"SELECT $this WHERE {{ $this <{prop_uri}> ?val . "
            f"FILTER NOT EXISTS {{ ?val a <{val_class_uri}> }} }}"
        )
        return self.create_shape(
            category="consistency",
            target_class=cls, target_class_uri=cls_uri,
            property_path=prop, property_uri=prop_uri,
            shacl_type="sh:sparql", parameters={"sh:select": query},
            message=f"{cls}.{prop} must have at least one value of type {val_class}",
            shape_id=stable_id,
        )

    def _migrate_has_value(self, c: Dict, uri: str, sep: str, data_ns: str, stable_id: str = "") -> Optional[Dict]:
        cls = c.get("className", "")
        cls_uri = c.get("classUri", f"{uri}{sep}{cls}" if cls else "")
        prop = c.get("property", "")
        prop_uri = c.get("propertyUri", f"{data_ns}{prop}" if prop else "")
        val = c.get("hasValue", "")
        return self.create_shape(
            category="conformance",
            target_class=cls, target_class_uri=cls_uri,
            property_path=prop, property_uri=prop_uri,
            shacl_type="sh:hasValue", parameters={"sh:hasValue": val},
            message=f"{cls}.{prop} must have value '{val}'",
            shape_id=stable_id,
        )

    def _migrate_global_rule(self, c: Dict, uri: str, sep: str, data_ns: str, stable_id: str = "") -> Optional[Dict]:
        rule = c.get("ruleName", "")
        if rule == "noOrphans":
            query = (
                "SELECT $this WHERE { "
                "$this a ?type . "
                "FILTER NOT EXISTS { $this ?p ?o . FILTER (?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>) } "
                "FILTER NOT EXISTS { ?s ?p2 $this . FILTER (?p2 != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>) } "
                "}"
            )
            return self.create_shape(
                category="structural",
                target_class="", target_class_uri="",
                shacl_type="sh:sparql", parameters={"sh:select": query},
                message="Every entity must have at least one relationship (no orphans)",
                shape_id=stable_id,
            )
        if rule == "requireLabels":
            return self.create_shape(
                category="structural",
                target_class="", target_class_uri="",
                property_path="label",
                property_uri="http://www.w3.org/2000/01/rdf-schema#label",
                shacl_type="sh:minCount", parameters={"sh:minCount": 1},
                message="Every entity must have an rdfs:label",
                shape_id=stable_id,
            )
        if rule == "uniqueIds":
            query = (
                "SELECT $this WHERE { "
                "$this a ?type . ?other a ?type2 . "
                "FILTER ($this = ?other && ?type != ?type2) }"
            )
            return self.create_shape(
                category="structural",
                target_class="", target_class_uri="",
                shacl_type="sh:sparql", parameters={"sh:select": query},
                message="All entity identifiers must be unique",
                shape_id=stable_id,
            )
        return None

    def _migrate_noop(self, c: Dict, uri: str, sep: str, data_ns: str, stable_id: str = "") -> Optional[Dict]:
        """Skip property characteristics that are handled by the reasoning engine, not DQ."""
        return None

    _MIGRATION_MAP = {
        "minCardinality": _migrate_cardinality,
        "maxCardinality": _migrate_cardinality,
        "exactCardinality": _migrate_cardinality,
        "functional": _migrate_functional,
        "inverseFunctional": _migrate_inverse_functional,
        "valueCheck": _migrate_value_check,
        "entityValueCheck": _migrate_value_check,
        "entityLabelCheck": _migrate_value_check,
        "allValuesFrom": _migrate_all_values_from,
        "someValuesFrom": _migrate_some_values_from,
        "hasValue": _migrate_has_value,
        "globalRule": _migrate_global_rule,
        "transitive": _migrate_noop,
        "symmetric": _migrate_noop,
        "asymmetric": _migrate_noop,
        "reflexive": _migrate_noop,
        "irreflexive": _migrate_noop,
    }

    # ------------------------------------------------------------------
    # SHACL-to-SQL translation (for Digital Twin execution)
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_prop_uri(uri: str) -> str:
        """Normalize an ontology property URI from ``#``-separator to ``/``-separator.

        The OWL generator stores property URIs with ``#`` (e.g.
        ``http://…/ontology#prop``), but the R2RML generator and triplestore
        mapping code normalise to ``/`` (``http://…/ontology/prop``).  This
        helper converts the ``#`` form to ``/`` so that SQL queries match
        the predicates stored in the triple-store table.

        Standard W3C URIs (containing ``w3.org``) are left untouched.
        """
        if not uri or "w3.org" in uri:
            return uri
        if "#" in uri:
            base, local = uri.rsplit("#", 1)
            if local:
                return base.rstrip("/") + "/" + local
        return uri

    @staticmethod
    def resolve_prop_uri(prop_uri: str, available_predicates: set) -> str:
        """Return *prop_uri* or a matching variant from *available_predicates*.

        Tries, in order:
        1. Exact match.
        2. ``#`` ↔ ``/`` separator swap.
        3. Local-name match (for short-name property URIs without a scheme).
        """
        if not prop_uri:
            return prop_uri
        if prop_uri in available_predicates:
            return prop_uri

        if "#" in prop_uri:
            base, local = prop_uri.rsplit("#", 1)
            alt = base.rstrip("/") + "/" + local
            if alt in available_predicates:
                logger.info("URI fallback (#→/): '%s' → '%s'", prop_uri, alt)
                return alt
        elif "/" in prop_uri:
            base, local = prop_uri.rsplit("/", 1)
            if local:
                alt = base + "#" + local
                if alt in available_predicates:
                    logger.info("URI fallback (/→#): '%s' → '%s'", prop_uri, alt)
                    return alt

        def _local(u: str) -> str:
            if "#" in u:
                return u.rsplit("#", 1)[-1]
            if "/" in u:
                return u.rsplit("/", 1)[-1]
            return u

        target_local = _local(prop_uri).lower()
        if target_local:
            for pred in available_predicates:
                if "w3.org" in pred:
                    continue
                if _local(pred).lower() == target_local:
                    logger.info(
                        "URI fallback (local-name): '%s' → '%s'", prop_uri, pred,
                    )
                    return pred

        logger.warning(
            "Property URI '%s' not found among %d graph predicates",
            prop_uri,
            len(available_predicates),
        )
        return prop_uri

    @staticmethod
    def _mem_cardinality(
        cls_uri: str, prop_uri: str, params: Dict,
        class_instances_fn, subj_by_pred: Dict,
    ) -> List[Dict]:
        min_count = params.get("sh:minCount")
        max_count = params.get("sh:maxCount")
        violations = []

        if not cls_uri and prop_uri:
            if max_count is not None:
                prop_vals = subj_by_pred.get(prop_uri, {})
                for s, vals in prop_vals.items():
                    if len(set(vals)) > int(max_count):
                        violations.append({"s": s, "cnt": len(set(vals))})
            return violations

        if not cls_uri or not prop_uri:
            return []

        instances = class_instances_fn(cls_uri)
        prop_vals = subj_by_pred.get(prop_uri, {})

        has_any_values = bool(prop_vals)
        if not has_any_values and instances:
            logger.warning(
                "Cardinality check: predicate '%s' has NO values for ANY of %d "
                "instances of '%s' — possible URI mismatch between shape and graph",
                prop_uri, len(instances), cls_uri,
            )

        for s in instances:
            cnt = len(prop_vals.get(s, []))
            if min_count is not None and cnt < int(min_count):
                violations.append({"s": s, "count": cnt})
            elif max_count is not None and cnt > int(max_count):
                violations.append({"s": s, "count": cnt})
        return violations

    @staticmethod
    def _sql_cardinality(
        table: str, cls_uri: str, prop_uri: str, params: Dict, rdf_type: str, esc,
    ) -> Optional[str]:
        min_count = params.get("sh:minCount")
        max_count = params.get("sh:maxCount")

        if not cls_uri or not prop_uri:
            if not cls_uri and prop_uri:
                if max_count is not None:
                    return (
                        f"SELECT t.subject AS s, COUNT(DISTINCT t.object) AS cnt\n"
                        f"FROM {table} t\n"
                        f"WHERE t.predicate = '{esc(prop_uri)}'\n"
                        f"GROUP BY t.subject\n"
                        f"HAVING COUNT(DISTINCT t.object) > {int(max_count)}"
                    )
            return None

        havings = []
        if min_count is not None:
            havings.append(f"COUNT(t2.object) < {int(min_count)}")
        if max_count is not None:
            havings.append(f"COUNT(t2.object) > {int(max_count)}")

        if not havings:
            return None

        use_left_join = min_count is not None
        join_kw = "LEFT JOIN" if use_left_join else "JOIN"

        return (
            f"SELECT t1.subject AS s, COUNT(t2.object) AS count\n"
            f"FROM {table} t1\n"
            f"{join_kw} {table} t2 ON t1.subject = t2.subject AND t2.predicate = '{esc(prop_uri)}'\n"
            f"WHERE t1.predicate = '{rdf_type}' AND t1.object = '{esc(cls_uri)}'\n"
            f"GROUP BY t1.subject\n"
            f"HAVING {' OR '.join(havings)}"
        )

    @staticmethod
    def _sql_datatype(
        table: str, cls_uri: str, prop_uri: str, params: Dict, rdf_type: str, esc,
    ) -> Optional[str]:
        """Generate SQL to find values that don't match the expected XSD datatype.

        Uses Spark's ``TRY_CAST`` which returns NULL for unparseable values.
        """
        raw_dt = str(params.get("sh:datatype", ""))
        if not raw_dt or not cls_uri or not prop_uri:
            return None

        spark_type = XSD_TO_SPARK_TYPE.get(raw_dt)
        if spark_type is None:
            return None

        return (
            f"SELECT t1.subject AS s, t2.object AS val\n"
            f"FROM {table} t1\n"
            f"JOIN {table} t2 ON t1.subject = t2.subject AND t2.predicate = '{esc(prop_uri)}'\n"
            f"WHERE t1.predicate = '{rdf_type}' AND t1.object = '{esc(cls_uri)}'\n"
            f"  AND t2.object IS NOT NULL\n"
            f"  AND TRY_CAST(t2.object AS {spark_type}) IS NULL"
        )

    @staticmethod
    def _sql_sparql_wellknown(
        table: str, params: Dict, rdf_type: str, esc,
    ) -> Optional[str]:
        """Translate well-known sh:sparql patterns to native SQL.

        Recognises:
        - **noOrphans** — entities with no predicates other than rdf:type / rdfs:label
        - **requireLabels** — entities without an rdfs:label
        - **uniqueIds** — entities with more than one rdf:type

        Returns ``None`` for unrecognised SPARQL queries.
        """
        query = (params.get("sh:select", "") or "").lower()
        if not query:
            return None

        if "filter not exists" in query and "?p" in query and "!=" in query:
            return (
                f"SELECT t1.subject AS s\n"
                f"FROM {table} t1\n"
                f"WHERE t1.predicate = '{rdf_type}'\n"
                f"  AND NOT EXISTS (\n"
                f"    SELECT 1 FROM {table} t2\n"
                f"    WHERE (t2.subject = t1.subject OR t2.object = t1.subject)\n"
                f"      AND t2.predicate != '{rdf_type}'\n"
                f"      AND t2.predicate != '{esc(RDFS_LABEL)}'\n"
                f"  )"
            )

        if "label" in query and "filter not exists" in query:
            return (
                f"SELECT t1.subject AS s\n"
                f"FROM {table} t1\n"
                f"LEFT JOIN {table} t2 ON t1.subject = t2.subject "
                f"AND t2.predicate = '{esc(RDFS_LABEL)}'\n"
                f"WHERE t1.predicate = '{rdf_type}'\n"
                f"  AND t2.subject IS NULL"
            )

        if "?type" in query and "?type2" in query and "filter" in query:
            return (
                f"SELECT t1.subject AS s, COUNT(DISTINCT t1.object) AS type_count\n"
                f"FROM {table} t1\n"
                f"WHERE t1.predicate = '{rdf_type}'\n"
                f"GROUP BY t1.subject\n"
                f"HAVING COUNT(DISTINCT t1.object) > 1"
            )

        return None

    @staticmethod
    def shape_to_sql(shape: Dict, table: str, base_uri: str = "") -> Optional[str]:
        """Translate a single SHACL shape dict into Spark SQL.

        The SQL checks against a triple store table with columns
        ``(subject, predicate, object)``.

        Returns:
            SQL string, or ``None`` if the shape cannot be translated.
        """
        RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        shacl_type = shape.get("shacl_type", "")
        params = shape.get("parameters", {})
        cls_uri = shape.get("target_class_uri", "")
        prop_uri = SHACLService._normalize_prop_uri(shape.get("property_uri", ""))

        def esc(v: str) -> str:
            return v.replace("'", "''")

        if shacl_type in ("sh:minCount", "sh:maxCount") or (
            "sh:minCount" in params or "sh:maxCount" in params
        ):
            return SHACLService._sql_cardinality(table, cls_uri, prop_uri, params, RDF_TYPE, esc)

        if shacl_type == "sh:pattern":
            pattern = params.get("sh:pattern", "")
            if not pattern or not cls_uri or not prop_uri:
                return None
            return (
                f"SELECT t1.subject AS s, t2.object AS val\n"
                f"FROM {table} t1\n"
                f"JOIN {table} t2 ON t1.subject = t2.subject AND t2.predicate = '{esc(prop_uri)}'\n"
                f"WHERE t1.predicate = '{RDF_TYPE}' AND t1.object = '{esc(cls_uri)}'\n"
                f"  AND NOT t2.object RLIKE '{esc(pattern)}'"
            )

        if shacl_type == "sh:hasValue":
            val = str(params.get("sh:hasValue", ""))
            if not val or not cls_uri or not prop_uri:
                return None
            return (
                f"SELECT t1.subject AS s\n"
                f"FROM {table} t1\n"
                f"LEFT JOIN {table} t2 ON t1.subject = t2.subject "
                f"AND t2.predicate = '{esc(prop_uri)}' AND t2.object = '{esc(val)}'\n"
                f"WHERE t1.predicate = '{RDF_TYPE}' AND t1.object = '{esc(cls_uri)}'\n"
                f"  AND t2.subject IS NULL"
            )

        if shacl_type == "sh:class":
            target_type = str(params.get("sh:class", ""))
            if not target_type or not cls_uri or not prop_uri:
                return None
            return (
                f"SELECT t1.subject AS s, t2.object AS target\n"
                f"FROM {table} t1\n"
                f"JOIN {table} t2 ON t1.subject = t2.subject AND t2.predicate = '{esc(prop_uri)}'\n"
                f"LEFT JOIN {table} t3 ON t2.object = t3.subject "
                f"AND t3.predicate = '{RDF_TYPE}' AND t3.object = '{esc(target_type)}'\n"
                f"WHERE t1.predicate = '{RDF_TYPE}' AND t1.object = '{esc(cls_uri)}'\n"
                f"  AND t3.subject IS NULL"
            )

        if shacl_type == "sh:datatype":
            return SHACLService._sql_datatype(table, cls_uri, prop_uri, params, RDF_TYPE, esc)

        if shacl_type == "sh:closed":
            return None

        if shacl_type == "sh:sparql":
            return SHACLService._sql_sparql_wellknown(table, params, RDF_TYPE, esc)

        return None


    @staticmethod
    def evaluate_shape_in_memory(
        shape: Dict, triples: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        """Evaluate a single SHACL shape against an in-memory list of triples.

        Each triple is ``{"subject": ..., "predicate": ..., "object": ...}``.
        Returns a list of violation dicts (empty if no violations).
        """
        RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        shacl_type = shape.get("shacl_type", "")
        params = shape.get("parameters", {})
        cls_uri = shape.get("target_class_uri", "")
        prop_uri = shape.get("property_uri", "")

        subj_by_pred: Dict[str, Dict[str, List[str]]] = {}
        type_map: Dict[str, set] = {}
        for t in triples:
            s, p, o = t.get("subject", ""), t.get("predicate", ""), t.get("object", "")
            if not s or not p:
                continue
            subj_by_pred.setdefault(p, {}).setdefault(s, []).append(o)
            if p == RDF_TYPE:
                type_map.setdefault(s, set()).add(o)

        # Resolve property URI against actual predicates in the graph
        # (handles #/slash mismatch between ontology and triplestore)
        prop_uri = SHACLService.resolve_prop_uri(prop_uri, set(subj_by_pred.keys()))

        def _class_instances(c_uri: str) -> set:
            return {s for s, types in type_map.items() if c_uri in types}

        if shacl_type in ("sh:minCount", "sh:maxCount") or (
            "sh:minCount" in params or "sh:maxCount" in params
        ):
            return SHACLService._mem_cardinality(cls_uri, prop_uri, params, _class_instances, subj_by_pred)

        if shacl_type == "sh:pattern":
            import re as _re
            pattern = params.get("sh:pattern", "")
            if not pattern or not cls_uri or not prop_uri:
                return []
            instances = _class_instances(cls_uri)
            prop_vals = subj_by_pred.get(prop_uri, {})
            violations = []
            try:
                regex = _re.compile(pattern, _re.IGNORECASE if params.get("sh:flags", "") == "i" else 0)
            except _re.error:
                return []
            for s in instances:
                for val in prop_vals.get(s, []):
                    if not regex.search(val):
                        violations.append({"s": s, "val": val})
            return violations

        if shacl_type == "sh:hasValue":
            expected = str(params.get("sh:hasValue", ""))
            if not expected or not cls_uri or not prop_uri:
                return []
            instances = _class_instances(cls_uri)
            prop_vals = subj_by_pred.get(prop_uri, {})
            return [{"s": s} for s in instances if expected not in prop_vals.get(s, [])]

        if shacl_type == "sh:class":
            target_type = str(params.get("sh:class", ""))
            if not target_type or not cls_uri or not prop_uri:
                return []
            instances = _class_instances(cls_uri)
            prop_vals = subj_by_pred.get(prop_uri, {})
            target_instances = _class_instances(target_type)
            violations = []
            for s in instances:
                for target in prop_vals.get(s, []):
                    if target not in target_instances:
                        violations.append({"s": s, "target": target})
            return violations

        return []
