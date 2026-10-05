"""SWRL/SHACL/constraint validation extracted from :class:`Ontology`.

Fowler Extract Class. ``Ontology`` keeps one-line delegators.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set

from back.core.reasoning.DecisionTableEngine import ASSIGN_CLASS
from back.core.w3c.shacl.constants import QUALITY_CATEGORIES

class OntologyRules:
    """Validate SWRL, SPARQL, decision-table, aggregate, and SHACL shapes."""

    _SWRL_ATOM_RE = re.compile(r"(?:(\w+):)?([A-Za-z_]\w*)\s*\(([^)]*)\)")

    _SWRL_BUILTIN_PREFIXES = frozenset({"swrlb", "xsd", "rdf", "rdfs", "owl", "sqwrl"})

    @staticmethod
    def validate_swrl_rule(rule: Dict[str, Any]) -> List[str]:
        """Validate a SWRL rule dict, return list of error strings (empty = valid)."""
        errors: List[str] = []
        if not rule.get("name"):
            errors.append("Rule name is required")
        if not rule.get("antecedent"):
            errors.append("Rule antecedent is required")
        if not rule.get("consequent"):
            errors.append("Rule consequent is required")
        return errors

    @staticmethod
    def swrl_reference_errors(
        rule: Dict[str, Any],
        class_names: Set[str],
        property_names: Set[str],
    ) -> List[str]:
        """Return errors for SWRL atoms referencing terms absent from the ontology.

        ``class_names`` / ``property_names`` are sets of lowercased local names.
        EVERY class and property atom — in both the antecedent AND the
        consequent — must already exist in the ontology. Inventing a new
        consequent class (e.g. a "derived subtype") is NOT allowed: a rule may
        only classify an instance into an existing ontology class. Namespaced
        builtins (``swrlb:``, ``xsd:``…) are ignored.
        """

        def _atoms(text: str):
            for m in OntologyRules._SWRL_ATOM_RE.finditer(text or ""):
                prefix = (m.group(1) or "").lower()
                name = m.group(2)
                args = [a.strip() for a in m.group(3).split(",") if a.strip()]
                yield prefix, name, args

        errors: List[str] = []
        for part in ("antecedent", "consequent"):
            for prefix, name, args in _atoms(rule.get(part, "")):
                if prefix:
                    continue
                if len(args) <= 1:
                    if name.lower() not in class_names:
                        errors.append(f"{part} references unknown entity '{name}'")
                elif name.lower() not in property_names:
                    errors.append(
                        f"{part} references unknown relationship/property '{name}'"
                    )

        # Tautology gate: a rule whose consequent only restates atoms already
        # present in the antecedent infers nothing (e.g. "… → Invoice(?i)" when
        # "Invoice(?i)" is already in the IF). Reject it. Builtin/datatype atoms
        # (prefixed) are ignored — only ontology class/property atoms count.
        def _norm(text: str):
            return {
                (name.lower(), tuple(args))
                for prefix, name, args in _atoms(text)
                if not prefix
            }

        ant = _norm(rule.get("antecedent", ""))
        con = _norm(rule.get("consequent", ""))
        if con and con.issubset(ant):
            errors.append(
                "consequent only repeats the antecedent and infers nothing new"
            )
        return errors

    # -- Entity business rules (SWRL rules referenced from a class) ---------

    @staticmethod
    def _swrl_class_atoms(rule: Dict[str, Any]):
        """Yield ``(name, variable)`` for every unprefixed unary atom of *rule*."""
        for part in ("antecedent", "consequent"):
            for m in OntologyRules._SWRL_ATOM_RE.finditer(rule.get(part, "") or ""):
                if m.group(1):
                    continue
                args = [a.strip() for a in m.group(3).split(",") if a.strip()]
                if len(args) == 1:
                    yield m.group(2), args[0]

    @staticmethod
    def swrl_rule_classes(rule: Dict[str, Any]) -> Set[str]:
        """Lowercased local names of the classes a SWRL rule references."""
        return {name.lower() for name, _ in OntologyRules._swrl_class_atoms(rule)}

    @staticmethod
    def class_lineage(classes: List[Dict[str, Any]], class_name: str) -> List[str]:
        """*class_name* followed by its ancestors, nearest first."""
        from back.objects.ontology.OntologyClassModel import OntologyClassModel

        return [class_name] + OntologyClassModel.ancestor_names(classes, class_name)

    @staticmethod
    def rules_for_class(
        class_name: str,
        swrl_rules: List[Dict[str, Any]],
        classes: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """SWRL rules whose class atoms cite *class_name* or one of its ancestors."""
        lineage = {n.lower() for n in OntologyRules.class_lineage(classes, class_name)}
        return [
            r
            for r in swrl_rules or []
            if isinstance(r, dict) and OntologyRules.swrl_rule_classes(r) & lineage
        ]

    @staticmethod
    def swrl_focus_variables(
        rule: Dict[str, Any], class_names: List[str]
    ) -> List[str]:
        """Variables typed by one of *class_names* in *rule*, in atom order."""
        wanted = {n.lower() for n in class_names}
        out: List[str] = []
        for name, var in OntologyRules._swrl_class_atoms(rule):
            if name.lower() in wanted and var.startswith("?") and var not in out:
                out.append(var)
        return out

    @staticmethod
    def rename_business_rule_refs(
        classes: List[Dict[str, Any]], old_name: str, new_name: str
    ) -> int:
        """Point every class ``business_rules`` entry named *old_name* to *new_name*."""
        if not old_name or old_name == new_name:
            return 0
        changed = 0
        for cls in classes or []:
            for ref in cls.get("business_rules") or []:
                if isinstance(ref, dict) and ref.get("name") == old_name:
                    ref["name"] = new_name
                    changed += 1
        return changed

    @staticmethod
    def drop_business_rule_refs(classes: List[Dict[str, Any]], name: str) -> int:
        """Remove every class ``business_rules`` entry named *name*."""
        changed = 0
        for cls in classes or []:
            refs = cls.get("business_rules") or []
            kept = [r for r in refs if not (isinstance(r, dict) and r.get("name") == name)]
            if len(kept) != len(refs):
                changed += len(refs) - len(kept)
                cls["business_rules"] = kept
        return changed

    @staticmethod
    def _ref_local_name(term: str):
        """Return ``(checkable, local_name)`` for a SPARQL/CURIE term.

        ``checkable`` is False for variables, literals, full URIs and terms in a
        builtin namespace (``rdf:``, ``owl:``…) — those are never ontology terms.
        """
        t = (term or "").strip()
        if not t or t.startswith("?") or t == "a":
            return False, ""
        if t[0] in "\"'+-" or t[0].isdigit():
            return False, ""
        if t.startswith("<") and t.endswith(">"):
            return False, ""
        if ":" in t and not t.lower().startswith("http"):
            prefix, local = t.split(":", 1)
            if prefix.lower() in OntologyRules._SWRL_BUILTIN_PREFIXES:
                return False, ""
            return True, local
        return True, t

    @staticmethod
    def decision_table_reference_errors(
        rule: Dict[str, Any], class_names: Set[str], property_names: Set[str]
    ) -> List[str]:
        """Flag a decision table referencing unknown classes/properties.

        Target class, every input-column property, the output-column
        property and any class assigned by an ``assign_class`` output must
        already exist in the ontology.
        """

        errors: List[str] = []
        target = rule.get("target_class", "")
        if target and target.lower() not in class_names:
            errors.append(f"target class '{target}' does not exist in the ontology")
        for col in rule.get("input_columns", []) or []:
            prop = (col or {}).get("property", "")
            if prop and prop.lower() not in property_names:
                errors.append(f"input column references unknown property '{prop}'")
        out = rule.get("output_column") or {}
        out_prop = out.get("property", "")
        if out_prop and out_prop.lower() not in property_names:
            errors.append(f"output column references unknown property '{out_prop}'")
        if out.get("action") == ASSIGN_CLASS:
            assigned = [out.get("value", "")] + [
                r.get("action_value", "") for r in rule.get("rows", []) or []
            ]
            for cls in filter(None, assigned):
                if cls.lower() not in class_names:
                    errors.append(f"output assigns unknown class '{cls}'")
        return errors

    @staticmethod
    def aggregate_reference_errors(
        rule: Dict[str, Any], class_names: Set[str], property_names: Set[str]
    ) -> List[str]:
        """Flag an aggregate rule referencing unknown classes/properties.

        Both ``target_class`` and ``result_class`` must already exist, as must
        the grouped/aggregated properties.
        """
        errors: List[str] = []
        for cls_field in ("target_class", "result_class"):
            cls = rule.get(cls_field, "")
            if cls and cls.lower() not in class_names:
                errors.append(f"{cls_field} '{cls}' does not exist in the ontology")
        for field in ("group_by_property", "aggregate_property"):
            prop = rule.get(field, "")
            if prop and prop.lower() not in property_names:
                errors.append(f"{field} references unknown property '{prop}'")
        return errors

    @staticmethod
    def sparql_reference_errors(
        rule: Dict[str, Any], class_names: Set[str], property_names: Set[str]
    ) -> List[str]:
        """Flag a CONSTRUCT rule referencing unknown terms.

        In BOTH the CONSTRUCT head and the WHERE pattern, predicates must be
        known properties and ``a``/``rdf:type`` objects must be known classes.
        No new (invented) class may be asserted in the CONSTRUCT head.
        """
        from back.core.reasoning.constants import CONSTRUCT_RE, TRIPLE_PATTERN_RE

        errors: List[str] = []
        query = rule.get("query", "") or ""
        m = CONSTRUCT_RE.search(query)
        if not m:
            return errors  # structural validator already reports a bad shape
        for part in (m.group(1), m.group(2)):  # CONSTRUCT head, then WHERE
            for _s, p, o in TRIPLE_PATTERN_RE.findall(part):
                is_type = p == "a" or p.lower() == "rdf:type"
                if is_type:
                    ok, local = OntologyRules._ref_local_name(o)
                    if ok and local.lower() not in class_names:
                        errors.append(f"query references unknown entity '{local}'")
                else:
                    ok, local = OntologyRules._ref_local_name(p)
                    if ok and local.lower() not in property_names:
                        errors.append(
                            f"query references unknown relationship/property '{local}'"
                        )
        return errors

    @staticmethod
    def rule_reference_errors(
        key: str,
        rule: Dict[str, Any],
        class_names: Set[str],
        property_names: Set[str],
    ) -> List[str]:
        """Dispatch existence validation for any of the four rule-list types."""
        if key == "swrl_rules":
            return OntologyRules.swrl_reference_errors(rule, class_names, property_names)
        if key == "decision_tables":
            return OntologyRules.decision_table_reference_errors(
                rule, class_names, property_names
            )
        if key == "sparql_rules":
            return OntologyRules.sparql_reference_errors(rule, class_names, property_names)
        if key == "aggregate_rules":
            return OntologyRules.aggregate_reference_errors(
                rule, class_names, property_names
            )
        return []

    @staticmethod
    def validate_constraint(constraint: Dict[str, Any]) -> Optional[str]:
        """Validate constraint and return error message if invalid.

        Args:
            constraint: Constraint data

        Returns:
            str: Error message if invalid, None if valid
        """
        constraint_type = constraint.get("type")
        if not constraint_type:
            return "Constraint type is required"

        property_characteristics = [
            "functional",
            "inverseFunctional",
            "transitive",
            "symmetric",
            "asymmetric",
            "reflexive",
            "irreflexive",
        ]
        cardinality_constraints = [
            "minCardinality",
            "maxCardinality",
            "exactCardinality",
        ]
        value_constraints = [
            "valueCheck",
            "entityValueCheck",
            "entityLabelCheck",
            "attributeConstraint",
            "globalRule",
        ]

        if constraint_type in cardinality_constraints:
            if not constraint.get("property"):
                return "Relationship (property) is required for cardinality constraints"
            if constraint.get("cardinalityValue") is None:
                return "Cardinality value is required"
        elif constraint_type in value_constraints:
            if constraint_type != "globalRule" and not constraint.get("className"):
                return "Entity (className) is required for value constraints"
        elif constraint_type in property_characteristics:
            if not constraint.get("property"):
                return "Property is required for property characteristics"

        return None

    @staticmethod
    def validate_shape(shape: Dict[str, Any]) -> Optional[str]:
        """Validate a SHACL shape dict, return error message or None."""
        category = shape.get("category", "")
        if category not in QUALITY_CATEGORIES:
            return f"Invalid category '{category}'. Must be one of: {', '.join(QUALITY_CATEGORIES)}"

        shacl_type = shape.get("shacl_type", "")
        if not shacl_type:
            return "shacl_type is required"

        if shacl_type not in ("sh:sparql", "sh:closed"):
            if not shape.get("property_path") and not shape.get("property_uri"):
                return "A property path or URI is required for this constraint type"

        params = shape.get("parameters", {})
        if shacl_type in ("sh:minCount", "sh:maxCount"):
            for key in ("sh:minCount", "sh:maxCount"):
                if key in params:
                    try:
                        int(params[key])
                    except (ValueError, TypeError):
                        return f"{key} must be an integer"

        from back.core.w3c.shacl import ShapeConditions

        return ShapeConditions.validate(
            shape.get("conditions"),
            shape.get("condition_logic", "and"),
            category,
            shape.get("target_class_uri", ""),
        )

    @staticmethod
    def generate_shacl(shapes: list, base_uri: str = "") -> str:
        """Generate SHACL Turtle from shape dicts."""
        from back.core.w3c import SHACLService

        svc = SHACLService(base_uri=base_uri or "http://example.org/ontology#")
        return svc.generate_turtle(shapes, base_uri=base_uri or None)
