"""Ontology management (non-HTTP).

Use :class:`Ontology` with a :class:`~back.objects.session.DomainSession` for
operations that persist to the session; use static methods for pure transforms.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Literal, Optional, Set

from back.core.logging import get_logger
from back.core.errors import ValidationError, InfrastructureError
from shared.config.constants import DEFAULT_BASE_URI
from back.core.industry import (
    fetch_and_parse_cdisc,
    fetch_and_parse_fibo,
    fetch_and_parse_iof,
)
from back.core.w3c import OntologyGenerator, OntologyParser
from back.core.w3c.shacl.constants import QUALITY_CATEGORIES

if TYPE_CHECKING:
    from agents.agent_auto_icon_assign.engine import AgentResult as IconAssignAgentResult
    from agents.agent_owl_generator.engine import AgentResult
    from back.objects.session.domain_session import DomainSession

IndustryKind = Literal["fibo", "cdisc", "iof"]

logger = get_logger(__name__)

_INDUSTRY_EMPTY_MESSAGE: Dict[IndustryKind, str] = {
    "fibo": "No FIBO domains selected.",
    "cdisc": "No CDISC domains selected.",
    "iof": "No IOF domains selected.",
}

_INDUSTRY_LOG_LABEL: Dict[IndustryKind, str] = {
    "fibo": "FIBO",
    "cdisc": "CDISC",
    "iof": "IOF",
}

_INDUSTRY_FETCH = {
    "fibo": fetch_and_parse_fibo,
    "cdisc": fetch_and_parse_cdisc,
    "iof": fetch_and_parse_iof,
}


class Ontology:
    """Ontology operations for the current domain session or as static helpers."""

    def __init__(self, session: "DomainSession") -> None:
        self._domain = session

    def generate_with_agent(
        self,
        *,
        host: str,
        token: str,
        endpoint_name: str,
        metadata: Dict[str, Any],
        guidelines: str = "",
        options: Optional[Dict[str, Any]] = None,
        selected_docs: Optional[List[str]] = None,
        on_step: Optional[Callable[[str], None]] = None,
    ) -> "AgentResult":
        """Run ``agent_owl_generator`` for this project (blocking).

        Resolves ``base_uri``, registry, project folder, and table selection from
        the session and the provided ``metadata`` (same rules as the wizard).

        Typical use: call from a background thread; poll task status from HTTP.
        """
        from agents.agent_owl_generator import run_agent

        s = self._domain
        ont = s.ontology
        base_uri = (
            ont.get("base_uri")
            or ont.get("info", {}).get("base_uri")
            or DEFAULT_BASE_URI
        )
        selected_tables = [
            t.get("full_name") or t.get("name")
            for t in (metadata or {}).get("tables", [])
        ]
        return run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            registry=dict(s.registry),
            metadata=metadata or {},
            guidelines=guidelines or "",
            options=options or {},
            base_uri=base_uri,
            domain_name=s.info.get("name", ""),
            domain_folder=s.domain_folder,
            domain_version=s.current_version,
            selected_tables=selected_tables,
            selected_docs=list(selected_docs or []),
            on_step=on_step,
        )

    def agent_ontology_context(self) -> Dict[str, Any]:
        """Ontology snapshot for agents: entities (classes + attributes) + object-property rels."""
        s = self._domain
        classes = s.get_classes()
        properties = s.get_properties()
        return {
            "entities": [
                {
                    "name": c.get("name", ""),
                    "uri": c.get("uri", ""),
                    "attributes": [
                        dp.get("name", "") for dp in c.get("dataProperties", [])
                    ],
                }
                for c in classes
            ],
            "relationships": [
                {
                    "name": p.get("name", ""),
                    "domain": p.get("domain", ""),
                    "range": p.get("range", ""),
                }
                for p in properties
                if p.get("type") in ("ObjectProperty", "owl:ObjectProperty", None)
            ],
        }

    def assign_icons_with_agent(
        self,
        *,
        host: str,
        token: str,
        endpoint_name: str,
        entity_names: List[str],
        on_step: Optional[Callable[[str], None]] = None,
    ) -> "IconAssignAgentResult":
        """Run ``agent_auto_icon_assign`` for this project (blocking).

        Uses session ontology classes/properties and ``catalog_metadata`` as agent context.
        """
        from agents.agent_auto_icon_assign import run_agent

        return run_agent(
            host=host,
            token=token,
            endpoint_name=endpoint_name,
            entity_names=entity_names,
            metadata=self._domain.catalog_metadata,
            ontology=self.agent_ontology_context(),
            on_step=on_step,
        )

    @staticmethod
    def ensure_uris(config: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure all classes and properties have URIs.
        
        Args:
            config: Ontology configuration dict
            
        Returns:
            dict: Configuration with URIs ensured
        """
        base_uri = config.get('base_uri', 'http://example.org/')
        if not base_uri.endswith('#') and not base_uri.endswith('/'):
            base_uri = base_uri + '#'
        
        for cls in config.get('classes', []):
            if not cls.get('uri') and cls.get('name'):
                cls['uri'] = base_uri + cls['name']
            if not cls.get('localName') and cls.get('name'):
                cls['localName'] = cls['name']
        
        for prop in config.get('properties', []):
            if not prop.get('uri') and prop.get('name'):
                prop['uri'] = base_uri + prop['name']
            if not prop.get('localName') and prop.get('name'):
                prop['localName'] = prop['name']
        
        return config

    @staticmethod
    def get_ontology_stats(config: Dict[str, Any]) -> Dict[str, int]:
        """Get statistics from ontology configuration.
        
        Args:
            config: Ontology configuration dict
            
        Returns:
            dict: Stats with counts
        """
        return {
            'classes': len(config.get('classes', [])),
            'properties': len(config.get('properties', [])),
            'constraints': len(config.get('constraints', [])),
            'swrl_rules': len(config.get('swrl_rules', [])),
            'axioms': len(config.get('axioms', [])),
            'expressions': len(config.get('expressions', [])),
        }

    @staticmethod
    def normalize_property_domain_range(
        ontology_config: Dict[str, Any],
        *,
        on_replace: Optional[Callable[[Dict[str, Any], str, Any, Any], None]] = None,
    ) -> bool:
        """Align property ``domain`` / ``range`` with canonical class names (case-insensitive).

        Mutates ``ontology_config['properties']`` in place. If ``on_replace`` is set, it is
        called as ``(prop_dict, field_name, old_value, new_value)`` for each change.

        Returns:
            True if any property field was updated.
        """
        classes = ontology_config.get("classes", [])
        properties = ontology_config.get("properties", [])
        class_name_lookup = {c["name"].lower(): c["name"] for c in classes if c.get("name")}
        modified = False
        for prop in properties:
            for field in ("domain", "range"):
                val = prop.get(field, "")
                if val and val not in class_name_lookup.values():
                    canonical = class_name_lookup.get(str(val).lower())
                    if canonical:
                        if on_replace is not None:
                            on_replace(prop, field, val, canonical)
                        prop[field] = canonical
                        modified = True
        return modified

    def prune_mappings_to_ontology_uris(
        self,
        class_uris: Set[str],
        property_uris: Set[str],
    ) -> Dict[str, int]:
        """Drop entity/relationship mappings whose URIs are not in the given sets.

        Updates session assignment only when rows are removed.

        Returns:
            Counts ``entity_mappings_removed`` and ``relationship_mappings_removed``.
        """
        s = self._domain
        entity_mappings = s.get_entity_mappings()
        cleaned_entity = [m for m in entity_mappings if m.get("ontology_class") in class_uris]
        removed_entity = len(entity_mappings) - len(cleaned_entity)

        rel_mappings = s.get_relationship_mappings()
        cleaned_rel = [m for m in rel_mappings if m.get("property") in property_uris]
        removed_rel = len(rel_mappings) - len(cleaned_rel)

        if removed_entity > 0:
            s._data["assignment"]["entities"] = cleaned_entity
        if removed_rel > 0:
            s._data["assignment"]["relationships"] = cleaned_rel

        return {
            "entity_mappings_removed": removed_entity,
            "relationship_mappings_removed": removed_rel,
        }

    def save_ontology_config_from_editor(self, raw_body: Dict[str, Any]) -> Dict[str, Any]:
        """Persist ontology from the visual editor API (wrapped or bare config dict)."""
        s = self._domain
        ontology_config = raw_body.get("config", raw_body)
        ontology_config = Ontology.ensure_uris(ontology_config)

        def _on_replace(prop: Dict[str, Any], field: str, old: Any, new: Any) -> None:
            logger.debug(
                "Normalizing property %s.%s: %r → %r",
                prop.get("name"),
                field,
                old,
                new,
            )

        Ontology.normalize_property_domain_range(ontology_config, on_replace=_on_replace)

        existing_constraints = s.constraints
        existing_swrl_rules = s.swrl_rules
        existing_axioms = s.axioms
        existing_expressions = s.expressions

        new_class_uris = {c.get("uri") for c in ontology_config.get("classes", []) if c.get("uri")}
        new_property_uris = {p.get("uri") for p in ontology_config.get("properties", []) if p.get("uri")}

        entity_before = s.get_entity_mappings()
        removed_counts = self.prune_mappings_to_ontology_uris(new_class_uris, new_property_uris)
        removed_entity = removed_counts["entity_mappings_removed"]
        removed_rel = removed_counts["relationship_mappings_removed"]

        if removed_entity > 0 or removed_rel > 0:
            orphaned_uris = [
                m.get("ontology_class")
                for m in entity_before
                if m.get("ontology_class") not in new_class_uris
            ]
            logger.warning(
                "Orphan cleanup: removing %d entity mappings (orphan URIs: %s) and %d rel mappings. "
                "New class URIs: %s",
                removed_entity,
                orphaned_uris,
                removed_rel,
                list(new_class_uris)[:10],
            )

        s.clear_generated_content()
        canonical_name = s.info.get("name", "").lower() or ontology_config.get("name", "")
        s.ontology.update({
            "name": canonical_name,
            "base_uri": ontology_config.get("base_uri", ""),
            "description": ontology_config.get("description", ""),
            "classes": ontology_config.get("classes", []),
            "properties": ontology_config.get("properties", []),
            "constraints": ontology_config.get("constraints", existing_constraints),
            "swrl_rules": ontology_config.get("swrl_rules", existing_swrl_rules),
            "axioms": ontology_config.get("axioms", existing_axioms),
            "expressions": ontology_config.get("expressions", existing_expressions),
        })
        s.save()

        return {
            "success": True,
            "message": "Ontology saved",
            "stats": Ontology.get_ontology_stats(ontology_config),
            "mappings_cleaned": {
                "entity_mappings_removed": removed_entity,
                "relationship_mappings_removed": removed_rel,
            },
        }

    def delete_class_by_uri(self, class_uri: Optional[str]) -> Dict[str, Any]:
        """Remove a class by URI and drop entity mappings that reference it."""
        if not class_uri:
            return {"success": False, "message": "Class not found"}
        s = self._domain
        classes = list(s.get_classes())
        original_len = len(classes)
        classes = [c for c in classes if c.get("uri") != class_uri]
        if len(classes) >= original_len:
            return {"success": False, "message": "Class not found"}

        s.ontology["classes"] = classes
        entity_mappings = s.get_entity_mappings()
        original_mapping_len = len(entity_mappings)
        entity_mappings = [m for m in entity_mappings if m.get("ontology_class") != class_uri]
        if len(entity_mappings) < original_mapping_len:
            s._data["assignment"]["entities"] = entity_mappings

        s.clear_generated_content()
        s.save()
        return {
            "success": True,
            "mapping_removed": len(entity_mappings) < original_mapping_len,
        }

    def delete_property_by_uri(self, property_uri: Optional[str]) -> Dict[str, Any]:
        """Remove an object property by URI and drop relationship mappings that reference it."""
        if not property_uri:
            return {"success": False, "message": "Property not found"}
        s = self._domain
        properties = list(s.get_properties())
        original_len = len(properties)
        properties = [p for p in properties if p.get("uri") != property_uri]
        if len(properties) >= original_len:
            return {"success": False, "message": "Property not found"}

        s.ontology["properties"] = properties
        rel_mappings = s.get_relationship_mappings()
        original_mapping_len = len(rel_mappings)
        rel_mappings = [m for m in rel_mappings if m.get("property") != property_uri]
        if len(rel_mappings) < original_mapping_len:
            s._data["assignment"]["relationships"] = rel_mappings

        s.clear_generated_content()
        s.save()
        return {
            "success": True,
            "mapping_removed": len(rel_mappings) < original_mapping_len,
        }

    def add_class(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Build a class from *data*, append if unique URI, clear cache and save."""
        s = self._domain
        classes = list(s.get_classes())
        new_class = Ontology.build_class_from_data(data)
        if any(c.get("uri") == new_class["uri"] for c in classes):
            return {"success": False, "message": "Class with this URI already exists"}
        classes.append(new_class)
        s.ontology["classes"] = classes
        s.clear_generated_content()
        s.save()
        return {"success": True, "class": new_class}

    def update_class(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Find class by *uri* in data, merge updates, clear cache and save."""
        s = self._domain
        classes = list(s.get_classes())
        class_uri = data.get("uri")
        for i, cls in enumerate(classes):
            if cls.get("uri") == class_uri:
                classes[i] = Ontology.build_class_from_data(data, cls)
                s.ontology["classes"] = classes
                s.clear_generated_content()
                s.save()
                return {"success": True, "class": classes[i]}
        return {"success": False, "message": "Class not found"}

    def add_property(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Build a property from *data*, append if unique URI, clear cache and save."""
        s = self._domain
        properties = list(s.get_properties())
        new_property = Ontology.build_property_from_data(data)
        if any(p.get("uri") == new_property["uri"] for p in properties):
            return {"success": False, "message": "Property with this URI already exists"}
        properties.append(new_property)
        s.ontology["properties"] = properties
        s.clear_generated_content()
        s.save()
        return {"success": True, "property": new_property}

    def update_property(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Find property by *uri* in data, merge updates, clear cache and save."""
        s = self._domain
        properties = list(s.get_properties())
        property_uri = data.get("uri")
        for i, prop in enumerate(properties):
            if prop.get("uri") == property_uri:
                properties[i] = Ontology.build_property_from_data(data, prop)
                s.ontology["properties"] = properties
                s.clear_generated_content()
                s.save()
                return {"success": True, "property": properties[i]}
        return {"success": False, "message": "Property not found"}

    def ingest_owl(
        self,
        owl_content: str,
        *,
        name_fallback_to_domain: bool = True,
        outcome: str = "import",
    ) -> Dict[str, Any]:
        """Parse OWL content, apply to project, return the appropriate success payload.

        ``outcome`` controls the response shape:
        - ``"import"``  → :meth:`build_import_owl_success_payload`
        - ``"parse"``   → :meth:`build_parse_owl_success_payload`
        - ``"load_file"``→ :meth:`build_load_owl_file_success_payload`
        """
        result = Ontology.parse_owl(owl_content, extract_advanced=True)
        ontology_info, classes, properties, constraints, swrl_rules, axioms, expressions, groups = result

        resolved_name = self.apply_parsed_owl_to_domain(
            ontology_info, classes, properties, constraints, swrl_rules, axioms, expressions,
            groups=groups,
            name_fallback_to_domain=name_fallback_to_domain,
        )

        if outcome == "parse":
            return self.build_parse_owl_success_payload(
                ontology_info, classes, properties, constraints,
                swrl_rules, axioms, expressions, resolved_name,
            )
        if outcome == "load_file":
            return self.build_load_owl_file_success_payload(
                classes, properties, constraints, swrl_rules, axioms, expressions,
            )
        return self.build_import_owl_success_payload(classes, properties, constraints)

    def apply_parsed_rdfs_to_domain(
        self,
        rdfs_content: str,
    ) -> Dict[str, Any]:
        """Parse RDFS content, apply to project, return success payload."""
        ontology_info, classes, properties = Ontology.parse_rdfs(rdfs_content)
        self._domain.ontology.update({
            "name": ontology_info.get("label", "Imported Vocabulary"),
            "base_uri": ontology_info.get("namespace", ontology_info.get("uri", "")),
            "classes": classes,
            "properties": properties,
        })
        self._domain.save()
        return {
            "success": True,
            "ontology": {
                "info": ontology_info,
                "classes": classes,
                "properties": properties,
            },
            "config": self._domain.ontology,
            "stats": {"classes": len(classes), "properties": len(properties)},
        }

    def rename_relationship_references(
        self, old_name: str, new_name: str
    ) -> Dict[str, int]:
        """Rename a relationship across mappings, constraints, and axioms. Saves session."""
        s = self._domain
        updates: Dict[str, int] = {"mappings_updated": 0, "constraints_updated": 0, "axioms_updated": 0}
        for rel_mapping in s.get_relationship_mappings():
            if rel_mapping.get("property_label") == old_name:
                rel_mapping["property_label"] = new_name
                updates["mappings_updated"] += 1
        for constraint in s.constraints:
            if constraint.get("property") == old_name:
                constraint["property"] = new_name
                updates["constraints_updated"] += 1
        for axiom in s.axioms:
            if axiom.get("property") == old_name:
                axiom["property"] = new_name
                updates["axioms_updated"] += 1
        s.save()
        return updates

    def apply_agent_ontology_changes(
        self,
        classes: List[Dict[str, Any]],
        properties: List[Dict[str, Any]],
        *,
        prune_orphan_mappings: bool = True,
    ) -> Dict[str, Any]:
        """Normalize + persist ontology from an assistant agent result.

        Returns the config dict suitable for ``response["config"]``.
        """
        s = self._domain
        base_uri = s.ontology.get("base_uri") or DEFAULT_BASE_URI
        ontology_config = {
            "name": s.ontology.get("name", ""),
            "base_uri": base_uri,
            "description": s.ontology.get("description", ""),
            "classes": classes,
            "properties": properties,
        }
        ontology_config = Ontology.ensure_uris(ontology_config)

        if prune_orphan_mappings:
            new_class_uris = {c.get("uri") for c in ontology_config["classes"] if c.get("uri")}
            new_property_uris = {p.get("uri") for p in ontology_config["properties"] if p.get("uri")}
            self.prune_mappings_to_ontology_uris(new_class_uris, new_property_uris)

        s.clear_generated_content()
        s.ontology.update({
            "classes": ontology_config["classes"],
            "properties": ontology_config["properties"],
        })
        s.save()

        return {
            "name": s.ontology.get("name", ""),
            "base_uri": base_uri,
            "description": s.ontology.get("description", ""),
            "classes": ontology_config["classes"],
            "properties": ontology_config["properties"],
        }

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
    def merge_icon_suggestions(
        entity_names: List[str], icons: Dict[str, str]
    ) -> Dict[str, str]:
        """Case-insensitive merge of agent icon suggestions into a final map."""
        normalized: Dict[str, str] = {}
        for key, emoji in icons.items():
            normalized[key] = emoji
            normalized[key.lower()] = emoji
        return {
            name: emoji
            for name in entity_names
            if (emoji := normalized.get(name) or normalized.get(name.lower()))
        }

    @staticmethod
    def postprocess_generated_owl(content: str) -> tuple:
        """Clean LLM output and compute stats in one step. Returns ``(turtle, stats)``."""
        turtle = Ontology.clean_owl_output(content)
        stats = Ontology.calculate_owl_stats(turtle)
        return turtle, stats

    @staticmethod
    def build_class_from_data(data: Dict[str, Any], existing: Dict[str, Any] = None) -> Dict[str, Any]:
        """Build a class dict from request data.
        
        Args:
            data: Request data
            existing: Existing class data (for updates)
            
        Returns:
            dict: Built class
        """
        existing = existing or {}
        return {
            'uri': data.get('uri', existing.get('uri', '')),
            'name': data.get('name', existing.get('name', '')),
            'label': data.get('label', data.get('name', existing.get('label', ''))),
            'description': data.get('description', existing.get('description', '')),
            'parent': data.get('parent', existing.get('parent', '')),
            'emoji': data.get('emoji', existing.get('emoji', '📦')),
            'properties': data.get('properties', existing.get('properties', [])),
            'dataProperties': data.get('dataProperties', existing.get('dataProperties', [])),
            'dashboard': data.get('dashboard', existing.get('dashboard', '')),
            'dashboardParams': data.get('dashboardParams', existing.get('dashboardParams', {})),
            'bridges': data.get('bridges', existing.get('bridges', []))
        }

    @staticmethod
    def build_property_from_data(data: Dict[str, Any], existing: Dict[str, Any] = None) -> Dict[str, Any]:
        """Build a property dict from request data.
        
        Args:
            data: Request data
            existing: Existing property data (for updates)
            
        Returns:
            dict: Built property
        """
        existing = existing or {}
        return {
            'uri': data.get('uri', existing.get('uri', '')),
            'name': data.get('name', existing.get('name', '')),
            'label': data.get('label', data.get('name', existing.get('label', ''))),
            'description': data.get('description', existing.get('description', '')),
            'type': data.get('type', existing.get('type', '')),
            'domain': data.get('domain', existing.get('domain', '')),
            'range': data.get('range', existing.get('range', '')),
            'direction': data.get('direction', existing.get('direction', 'forward')),
            'properties': data.get('properties', existing.get('properties', []))
        }

    @staticmethod
    def validate_constraint(constraint: Dict[str, Any]) -> Optional[str]:
        """Validate constraint and return error message if invalid.
        
        Args:
            constraint: Constraint data
            
        Returns:
            str: Error message if invalid, None if valid
        """
        constraint_type = constraint.get('type')
        if not constraint_type:
            return 'Constraint type is required'
        
        property_characteristics = ['functional', 'inverseFunctional', 'transitive', 
                                    'symmetric', 'asymmetric', 'reflexive', 'irreflexive']
        cardinality_constraints = ['minCardinality', 'maxCardinality', 'exactCardinality']
        value_constraints = ['valueCheck', 'entityValueCheck', 'entityLabelCheck', 
                            'attributeConstraint', 'globalRule']
        
        if constraint_type in cardinality_constraints:
            if not constraint.get('property'):
                return 'Relationship (property) is required for cardinality constraints'
            if constraint.get('cardinalityValue') is None:
                return 'Cardinality value is required'
        elif constraint_type in value_constraints:
            if constraint_type != 'globalRule' and not constraint.get('className'):
                return 'Entity (className) is required for value constraints'
        elif constraint_type in property_characteristics:
            if not constraint.get('property'):
                return 'Property is required for property characteristics'
        
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
    
        return None

    @staticmethod
    def validate_classes(classes: List[Dict[str, Any]]) -> tuple:
        """Check ontology classes for completeness.

        Returns:
            ``(is_valid, issues)`` where *issues* is a list of human-readable
            strings and *is_valid* is ``True`` when ``classes`` is non-empty
            and all entries have at least a URI, name, or localName.
        """
        issues: List[str] = []
        for cls in classes:
            if not cls.get('uri') and not cls.get('name') and not cls.get('localName'):
                issues.append(f"Entity '{cls.get('label', 'Unknown')}' has no URI")
        if not classes:
            issues.append('No entities defined')
        return (len(classes) > 0 and len(issues) == 0), issues

    @staticmethod
    def generate_shacl(shapes: list, base_uri: str = "") -> str:
        """Generate SHACL Turtle from shape dicts."""
        from back.core.w3c import SHACLService
        svc = SHACLService(base_uri=base_uri or "http://example.org/ontology#")
        return svc.generate_turtle(shapes, base_uri=base_uri or None)

    @staticmethod
    def generate_owl(data, constraints=None, swrl_rules=None, axioms=None,
                     expressions=None, groups=None):
        """Generate OWL from ontology configuration.

        Args:
            data: dict with base_uri, name, classes, properties
            constraints: list of property constraints (optional)
            swrl_rules: list of SWRL rules (optional)
            axioms: list of OWL axioms (optional)
            expressions: list of OWL class expressions (optional)
            groups: list of entity group definitions (optional)

        Returns:
            str: Generated OWL content
        """
        generator = OntologyGenerator(
            base_uri=data.get('base_uri', DEFAULT_BASE_URI),
            ontology_name=data.get('name', 'MyOntology'),
            classes=data.get('classes', []),
            properties=data.get('properties', []),
            constraints=constraints,
            swrl_rules=swrl_rules,
            axioms=axioms,
            expressions=expressions,
            groups=groups,
        )
        return generator.generate()

    @staticmethod
    def parse_owl(content, extract_advanced=True):
        """Parse OWL content and return structured data.

        Args:
            content: OWL/Turtle content
            extract_advanced: If True, also extract constraints, SWRL rules, axioms, expressions, and groups

        Returns:
            tuple: (ontology_info, classes, properties) or
                   (ontology_info, classes, properties, constraints, swrl_rules, axioms, expressions, groups)
                   if extract_advanced=True
        """
        parser = OntologyParser(content)
        ontology_info = parser.get_ontology_info()
        classes = parser.get_classes()
        properties = parser.get_properties()

        if extract_advanced:
            constraints = parser.get_constraints()
            swrl_rules = parser.get_swrl_rules()
            split = parser.get_axioms_and_expressions()
            groups = parser.get_groups()
            return ontology_info, classes, properties, constraints, swrl_rules, split['axioms'], split['expressions'], groups

        return ontology_info, classes, properties

    @staticmethod
    def parse_rdfs(content):
        """Parse RDFS content and return structured data.
        
        Args:
            content: RDFS content (Turtle, RDF/XML, N3, etc.)
            
        Returns:
            tuple: (ontology_info, classes, properties)
        """
        from back.core.w3c import RDFSParser
        
        parser = RDFSParser(content)
        ontology_info = parser.get_ontology_info()
        classes = parser.get_classes()
        properties = parser.get_properties()
        
        return ontology_info, classes, properties

    def import_industry_ontology(self, kind: IndustryKind, domain_keys: List[str]) -> Dict[str, Any]:
        """Fetch industry modules, merge, parse, persist to project session.
    
        Returns the same dict shape as the former /import-fibo|cdisc|iof handlers.
        """
        if not domain_keys:
            return {"success": False, "message": _INDUSTRY_EMPTY_MESSAGE[kind]}
    
        try:
            result = _INDUSTRY_FETCH[kind](domain_keys)
    
            if not result["success"]:
                return {
                    "success": False,
                    "message": result["message"],
                    "failed": result.get("failed", []),
                }
    
            info = result["ontology_info"]
            if kind == "cdisc":
                ont_name = info.get("label", "CDISC")
                base_uri = info.get("uri", "http://rdf.cdisc.org/")
                desc_prefix = "CDISC Foundational Standards in RDF – "
            elif kind == "fibo":
                ont_name = info.get("name", "FIBO")
                base_uri = info.get("uri", "https://spec.edmcouncil.org/fibo/ontology/")
                desc_prefix = "Financial Industry Business Ontology (FIBO) – "
            else:
                ont_name = info.get("name", "IOF")
                base_uri = info.get("uri", "https://spec.industrialontologies.org/ontology/")
                desc_prefix = "Industrial Ontologies Foundry (IOF) – "
    
            self._domain.ontology.update({
                "name": ont_name,
                "base_uri": base_uri,
                "description": desc_prefix + ", ".join(domain_keys),
                "classes": result["classes"],
                "properties": result["properties"],
                "constraints": result["constraints"],
                "swrl_rules": result["swrl_rules"],
                "axioms": result["axioms"],
                "expressions": result["expressions"],
            })
            self._domain.save()
    
            return {
                "success": True,
                "message": result["message"],
                "stats": result["stats"],
                "failed": result["failed"],
            }
        except Exception as exc:
            logger.exception("%s import failed: %s", _INDUSTRY_LOG_LABEL[kind], exc)
            raise InfrastructureError(
                f"{_INDUSTRY_LOG_LABEL[kind]} import failed",
                detail=str(exc),
            ) from exc

    def apply_parsed_owl_to_domain(
        self,
        ontology_info: Dict[str, Any],
        classes: list,
        properties: list,
        constraints: list,
        swrl_rules: list,
        axioms: list,
        expressions: list = None,
        *,
        groups: list = None,
        name_fallback_to_domain: bool = True,
    ) -> str:
        """Write parse result into self._domain.ontology and save. Returns resolved ontology name."""
        if name_fallback_to_domain:
            default_name = self._domain.info.get("name", "")
            resolved_name = ontology_info.get("name", "") or default_name
        else:
            resolved_name = ontology_info.get("name", "")

        self._domain.ontology.update({
            "name": resolved_name,
            "base_uri": ontology_info.get("uri", ""),
            "classes": classes,
            "properties": properties,
            "constraints": constraints,
            "swrl_rules": swrl_rules,
            "axioms": axioms,
            "expressions": expressions or [],
            "groups": groups or [],
        })
        self._domain.save()
        return resolved_name

    def build_import_owl_success_payload(
        self,
        classes: list,
        properties: list,
        constraints: list,
    ) -> Dict[str, Any]:
        return {
            "success": True,
            "config": self._domain.ontology,
            "stats": {
                "classes": len(classes),
                "properties": len(properties),
                "constraints": len(constraints),
            },
        }

    def build_parse_owl_success_payload(
        self,
        ontology_info: Dict[str, Any],
        classes: list,
        properties: list,
        constraints: list,
        swrl_rules: list,
        axioms: list,
        expressions: list = None,
        resolved_name: str = "",
    ) -> Dict[str, Any]:
        return {
            "success": True,
            "ontology": {
                "info": {
                    "label": resolved_name,
                    "namespace": ontology_info.get("uri", ""),
                    "uri": ontology_info.get("uri", ""),
                },
                "classes": classes,
                "properties": properties,
            },
            "config": self._domain.ontology,
            "stats": {
                "classes": len(classes),
                "properties": len(properties),
                "constraints": len(constraints),
                "swrl_rules": len(swrl_rules),
                "axioms": len(axioms),
                "expressions": len(expressions or []),
            },
        }

    def build_load_owl_file_success_payload(
        self,
        classes: list,
        properties: list,
        constraints: list,
        swrl_rules: list,
        axioms: list,
        expressions: list = None,
    ) -> Dict[str, Any]:
        return {
            "success": True,
            "ontology": self._domain.ontology,
            "stats": {
                "classes": len(classes),
                "properties": len(properties),
                "constraints": len(constraints),
                "swrl_rules": len(swrl_rules),
                "axioms": len(axioms),
                "expressions": len(expressions or []),
            },
        }

    @staticmethod
    def _turtle_to_camel(words: list, is_pascal: bool) -> str:
        """Convert a list of words to camelCase or PascalCase."""
        if not words:
            return ''
        if is_pascal:
            return ''.join(w.capitalize() for w in words if w)
        result = words[0].lower()
        for w in words[1:]:
            if w:
                result += w.capitalize()
        return result

    @staticmethod
    def _fix_snake_kebab_local_names(content: str) -> str:
        """Convert snake_case / kebab-case local names to camelCase in Turtle."""
        import re

        def _fix_match(match):
            prefix = match.group(1)
            name = match.group(2)
            words = re.split(r'[_-]+', name)
            if len(words) <= 1:
                return match.group(0)
            is_pascal = words[0] and words[0][0].isupper()
            return prefix + Ontology._turtle_to_camel(words, is_pascal)

        pattern = r'(?<![a-zA-Z])(:)([a-zA-Z][a-zA-Z0-9]*(?:[_-][a-zA-Z][a-zA-Z0-9]*)+)'
        return re.sub(pattern, _fix_match, content)

    @staticmethod
    def _fix_spaced_local_names(content: str) -> str:
        """Join space-separated words in bare ``:LocalName`` tokens."""
        _TURTLE_KEYWORDS = frozenset({'a', 'rdf', 'rdfs', 'owl', 'xsd', 'xml', 'true', 'false'})

        def _fix_line(line: str) -> str:
            stripped = line.strip()
            if stripped.startswith('#') or stripped.startswith('@prefix') or stripped.startswith('@base'):
                return line

            result: list = []
            i = 0
            while i < len(line):
                if line[i] == ':' and (i == 0 or line[i - 1] in ' \t;.,()[]'):
                    j = i + 1
                    if j >= len(line):
                        result.append(line[i]); i += 1; continue

                    words: list = []
                    current_word = ''
                    while j < len(line):
                        ch = line[j]
                        if ch.isalnum():
                            current_word += ch; j += 1
                        elif ch == ' ' and current_word:
                            k = j + 1
                            while k < len(line) and line[k] == ' ':
                                k += 1
                            if k < len(line) and line[k].isalpha():
                                nwe = k
                                while nwe < len(line) and line[nwe].isalnum():
                                    nwe += 1
                                nw = line[k:nwe]
                                an = nwe
                                while an < len(line) and line[an] == ' ':
                                    an += 1
                                if (an < len(line) and line[an] == ':') or nw.lower() in _TURTLE_KEYWORDS or len(nw) == 1:
                                    words.append(current_word); break
                                words.append(current_word); current_word = ''; j = k
                            elif k < len(line):
                                words.append(current_word); break
                            else:
                                words.append(current_word); break
                        else:
                            if current_word:
                                words.append(current_word)
                            break

                    if words:
                        is_pascal = words[0] and words[0][0].isupper()
                        result.append(':')
                        result.append(Ontology._turtle_to_camel(words, is_pascal))
                        i = j
                    else:
                        result.append(line[i]); i += 1
                else:
                    result.append(line[i]); i += 1
            return ''.join(result)

        return '\n'.join(_fix_line(ln) for ln in content.split('\n'))

    @staticmethod
    def _fix_local_names(content: str) -> str:
        """Fix local names with spaces, underscores, or hyphens in Turtle content.

        Converts patterns like:
        - :street address -> :streetAddress
        - :Street Address -> :StreetAddress
        - :first_name -> :firstName
        - :customer-id -> :customerId
        """
        content = Ontology._fix_snake_kebab_local_names(content)
        return Ontology._fix_spaced_local_names(content)

    @staticmethod
    def clean_owl_output(content: str) -> str:
        """Clean up LLM output to extract valid Turtle content."""
        content = content.strip()
    
        # Remove markdown code fences
        if '```' in content:
            import re
            m = re.search(r'```(?:turtle|ttl|sparql|rdf)?\s*\n(.*?)```', content, re.DOTALL)
            if m:
                content = m.group(1).strip()
            elif content.startswith('```'):
                lines = content.split('\n')
                lines = lines[1:]
                if lines and lines[-1].strip() == '```':
                    lines = lines[:-1]
                content = '\n'.join(lines)
    
        content = content.strip()
    
        # Strip any natural-language preamble before the first @prefix or @base
        prefix_idx = content.find('@prefix')
        base_idx = content.find('@base')
        candidates = [i for i in (prefix_idx, base_idx) if i > 0]
        if candidates:
            content = content[min(candidates):]
    
        content = content.strip()
    
        # Fix any local names with spaces, underscores, or hyphens
        content = Ontology._fix_local_names(content)

        return content

    # ------------------------------------------------------------------
    # Group management (entity groups modelled as owl:unionOf)
    # ------------------------------------------------------------------

    def save_group(self, group: Dict, index: int = -1) -> List[Dict]:
        """Create or update an entity group.

        Args:
            group: Group dict with keys *name*, *label*, *description*, *color*,
                   *icon*, *members*.
            index: When ``>= 0`` the group at that position is replaced;
                   otherwise a new group is appended (duplicate names are rejected).

        Returns:
            The updated list of all groups.

        Raises:
            ValidationError: if the name is missing or already exists (on create).
        """
        name = (group.get('name') or '').strip()
        if not name:
            raise ValidationError("Group name is required")

        groups = self._domain.groups

        if 0 <= index < len(groups):
            groups[index] = group
        else:
            if any(g.get('name') == name for g in groups):
                raise ValidationError(f'Group "{name}" already exists')
            groups.append(group)

        self._enforce_exclusive_membership(groups, name)
        self._domainync_class_group_field(groups)
        self._domain.groups = groups
        self._domain.save()
        return self._domain.groups

    def delete_group(self, *, index: int = -1, name: str = '') -> List[Dict]:
        """Delete an entity group by *index* or *name*.

        Returns:
            The updated list of all groups.

        Raises:
            ValidationError: if neither *index* nor *name* identifies a group.
        """
        groups = self._domain.groups

        if 0 <= index < len(groups):
            groups.pop(index)
        elif name:
            groups[:] = [g for g in groups if g.get('name') != name]
        else:
            raise ValidationError("Provide index or name to identify the group")

        self._domainync_class_group_field(groups)
        self._domain.groups = groups
        self._domain.save()
        return self._domain.groups

    def update_group_members(self, group_name: str, *,
                             add: List[str] = None,
                             remove: List[str] = None) -> List[Dict]:
        """Add or remove members from the group identified by *group_name*.

        Returns:
            The updated list of all groups.

        Raises:
            ValidationError: if *group_name* is empty or not found.
        """
        if not group_name:
            raise ValidationError("Group name is required")

        groups = self._domain.groups
        target = next((g for g in groups if g.get('name') == group_name), None)
        if target is None:
            raise ValidationError(f'Group "{group_name}" not found')

        to_remove = set(remove or [])
        members = [m for m in target.get('members', []) if m not in to_remove]
        existing = set(members)
        for m in (add or []):
            if m and m not in existing:
                members.append(m)
                existing.add(m)
        target['members'] = members

        self._enforce_exclusive_membership(groups, group_name)
        self._domainync_class_group_field(groups)
        self._domain.groups = groups
        self._domain.save()
        return self._domain.groups

    @staticmethod
    def _enforce_exclusive_membership(groups: List[Dict],
                                      authoritative_group_name: str) -> None:
        """Ensure every entity belongs to at most one group.

        After the group identified by *authoritative_group_name* has been
        updated, remove any of its members that appear in other groups.
        """
        target = next((g for g in groups if g.get('name') == authoritative_group_name), None)
        if target is None:
            return
        owner_members = set(target.get('members', []))
        for g in groups:
            if g.get('name') == authoritative_group_name:
                continue
            g['members'] = [m for m in g.get('members', []) if m not in owner_members]

    def _sync_class_group_field(self, groups: List[Dict]) -> None:
        """Keep each class's ``group`` field in sync with the groups list."""
        class_to_group: Dict[str, str] = {}
        for g in groups:
            for m in g.get('members', []):
                class_to_group[m] = g.get('name', '')
        for cls in self._domain.get_classes():
            cls['group'] = class_to_group.get(cls.get('name', ''), '')

    @staticmethod
    def calculate_owl_stats(owl_content: str) -> Dict:
        """Calculate statistics from OWL content."""
        stats = {
            'classes': 0,
            'properties': 0,
            'dataProperties': 0
        }
        
        try:
            # Count owl:Class declarations
            stats['classes'] = owl_content.count('a owl:Class') + owl_content.count('rdf:type owl:Class')
            
            # Count owl:ObjectProperty declarations
            stats['properties'] = owl_content.count('a owl:ObjectProperty') + owl_content.count('rdf:type owl:ObjectProperty')
            
            # Count owl:DatatypeProperty declarations
            stats['dataProperties'] = owl_content.count('a owl:DatatypeProperty') + owl_content.count('rdf:type owl:DatatypeProperty')
        except Exception as exc:
            logger.warning("OWL stats calculation error: %s", exc)
        
        return stats
