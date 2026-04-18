"""OWL ontology generator."""

import json
import re
from rdflib import BNode, Graph, Namespace, URIRef, Literal
from rdflib.collection import Collection
from rdflib.namespace import RDF, RDFS, OWL, XSD
from typing import List, Dict

from back.core.logging import get_logger
from shared.config.constants import ONTOBRICKS_NS

logger = get_logger(__name__)


class OntologyGenerator:
    """Generate OWL ontologies from configuration."""

    def __init__(
        self,
        base_uri: str,
        ontology_name: str,
        classes: List[Dict],
        properties: List[Dict],
        constraints: List[Dict] = None,
        swrl_rules: List[Dict] = None,
        axioms: List[Dict] = None,
        expressions: List[Dict] = None,
        groups: List[Dict] = None,
    ):
        """Initialize the OWL generator.

        Args:
            base_uri: Base URI for the ontology (e.g., http://example.org/ontology#)
            ontology_name: Name of the ontology
            classes: List of class definitions
            properties: List of property definitions
            constraints: List of property constraints (cardinality, characteristics)
            swrl_rules: List of SWRL rules
            axioms: List of OWL axioms (logical assertions)
            expressions: List of OWL class expressions (unionOf, intersectionOf, etc.)
            groups: List of entity group definitions (name, label, color, icon, members)
        """
        self.base_uri = base_uri.rstrip("#") + "#"
        self.ontology_name = ontology_name
        self.classes = classes or []
        self.properties = properties or []
        self.constraints = constraints or []
        self.swrl_rules = swrl_rules or []
        self.axioms = axioms or []
        self.expressions = expressions or []
        self.groups = groups or []

        self.graph = Graph()
        self.ns = Namespace(self.base_uri)

        # Bind namespaces
        self.graph.bind("owl", OWL)
        self.graph.bind("rdf", RDF)
        self.graph.bind("rdfs", RDFS)
        self.graph.bind("xsd", XSD)
        self.graph.bind("ontobricks", ONTOBRICKS_NS)
        self.graph.bind("", self.ns)

    def generate(self) -> str:
        """Generate OWL content.

        Returns:
            OWL content as string (Turtle format)
        """
        # Create ontology
        ontology_uri = URIRef(self.base_uri.rstrip("#"))
        self.graph.add((ontology_uri, RDF.type, OWL.Ontology))

        if self.ontology_name:
            self.graph.add((ontology_uri, RDFS.label, Literal(self.ontology_name)))

        # Add classes and their data properties (attributes)
        for cls in self.classes:
            self._add_class(cls)

        # Add properties (object properties and explicit datatype properties)
        for prop in self.properties:
            self._add_property(prop)

        # Add property constraints
        for constraint in self.constraints:
            self._add_constraint(constraint)

        # Add SWRL rules
        for rule in self.swrl_rules:
            self._add_swrl_rule(rule)

        # Add OWL axioms and class expressions
        for axiom in self.axioms:
            self._add_axiom(axiom)
        for expr in self.expressions:
            self._add_axiom(expr)

        # Add entity groups (defined classes with owl:unionOf)
        self._add_groups()

        # Serialize to Turtle format
        return self.graph.serialize(format="turtle")

    def _resolve_uri(self, ref: str):
        """Convert a name or full URI string to a URIRef, or None if empty."""
        if not ref:
            return None
        if ref.startswith("http://") or ref.startswith("https://"):
            return URIRef(ref)
        return URIRef(self.base_uri + ref)

    def _collect_uris(self, refs: list) -> list:
        """Resolve a list of name/URI strings to URIRefs, skipping empty values."""
        return [u for u in (self._resolve_uri(r) for r in refs if r) if u]

    def _add_constraint(self, constraint: Dict):
        """Add a property constraint to the ontology.

        Args:
            constraint: Constraint definition with 'type', 'property', 'className', 'value', etc.
        """
        logger.debug("Processing constraint: %s", constraint)
        constraint_type = constraint.get("type", "")
        property_ref = constraint.get("property", "")
        class_ref = constraint.get("className", "")
        # Support both 'value' and 'cardinalityValue' keys
        value = constraint.get("value") or constraint.get("cardinalityValue", "")
        value_class = constraint.get("valueClass", "")
        has_value = constraint.get("hasValue", "")

        if not constraint_type:
            return

        get_uri = self._resolve_uri

        # Handle property characteristics (applied directly to property)
        property_characteristics = {
            "functional": OWL.FunctionalProperty,
            "inverseFunctional": OWL.InverseFunctionalProperty,
            "transitive": OWL.TransitiveProperty,
            "symmetric": OWL.SymmetricProperty,
            "asymmetric": OWL.AsymmetricProperty,
            "reflexive": OWL.ReflexiveProperty,
            "irreflexive": OWL.IrreflexiveProperty,
        }

        if constraint_type in property_characteristics:
            if property_ref:
                prop_uri = get_uri(property_ref)
                self.graph.add(
                    (prop_uri, RDF.type, property_characteristics[constraint_type])
                )
                logger.debug(
                    "Added property characteristic: %s a %s",
                    prop_uri,
                    property_characteristics[constraint_type],
                )
            return

        # Handle cardinality restrictions (applied to class via subClassOf)
        if (
            constraint_type in ["minCardinality", "maxCardinality", "exactCardinality"]
            and property_ref
        ):
            self._add_cardinality_restriction_uri(
                constraint_type, get_uri(property_ref), get_uri(class_ref), value
            )
            return

        # Handle allValuesFrom and someValuesFrom restrictions
        if constraint_type in ["allValuesFrom", "someValuesFrom"] and property_ref:
            target_class = value_class or value  # Support both keys
            self._add_values_restriction_uri(
                constraint_type,
                get_uri(property_ref),
                get_uri(class_ref),
                get_uri(target_class),
            )
            return

        # Handle hasValue restriction
        if constraint_type == "hasValue" and property_ref:
            val = has_value or value  # Support both keys
            self._add_has_value_restriction_uri(
                get_uri(property_ref), get_uri(class_ref), val
            )
            return

        # Handle value check constraints (custom OntoBricks annotations)
        if constraint_type == "valueCheck":
            self._add_value_check_constraint(constraint)
            return

        # Handle global rules (custom OntoBricks annotations)
        if constraint_type == "globalRule":
            self._add_global_rule_constraint(constraint)
            return

    def _add_cardinality_restriction_uri(
        self, restriction_type: str, prop_uri: URIRef, class_uri: URIRef, value
    ):
        """Add a cardinality restriction as a subClassOf restriction (URI version).

        Args:
            restriction_type: 'minCardinality', 'maxCardinality', or 'exactCardinality'
            prop_uri: URIRef of the property
            class_uri: URIRef of the class to restrict
            value: Cardinality value (integer)
        """
        if not class_uri or not prop_uri or value is None:
            logger.debug(
                "Missing required values: class_uri=%s, prop_uri=%s, value=%s",
                class_uri,
                prop_uri,
                value,
            )
            return

        try:
            card_value = int(value)
        except (ValueError, TypeError):
            logger.debug("Invalid cardinality value: %s", value)
            return

        # Extract local names for restriction naming
        class_local = str(class_uri).split("#")[-1].split("/")[-1]
        prop_local = str(prop_uri).split("#")[-1].split("/")[-1]

        # Create a URI for the restriction
        restriction = URIRef(
            self.base_uri
            + f"_restriction_{class_local}_{prop_local}_{restriction_type}"
        )

        self.graph.add((restriction, RDF.type, OWL.Restriction))
        self.graph.add((restriction, OWL.onProperty, prop_uri))

        if restriction_type == "minCardinality":
            self.graph.add(
                (
                    restriction,
                    OWL.minCardinality,
                    Literal(card_value, datatype=XSD.nonNegativeInteger),
                )
            )
        elif restriction_type == "maxCardinality":
            self.graph.add(
                (
                    restriction,
                    OWL.maxCardinality,
                    Literal(card_value, datatype=XSD.nonNegativeInteger),
                )
            )
        elif restriction_type == "exactCardinality":
            self.graph.add(
                (
                    restriction,
                    OWL.cardinality,
                    Literal(card_value, datatype=XSD.nonNegativeInteger),
                )
            )

        # Add the restriction as a subClassOf the class
        self.graph.add((class_uri, RDFS.subClassOf, restriction))
        logger.debug(
            "Added cardinality restriction: %s subClassOf %s", class_uri, restriction
        )

    def _add_values_restriction_uri(
        self,
        restriction_type: str,
        prop_uri: URIRef,
        class_uri: URIRef,
        value_class_uri: URIRef,
    ):
        """Add an allValuesFrom or someValuesFrom restriction (URI version).

        Args:
            restriction_type: 'allValuesFrom' or 'someValuesFrom'
            prop_uri: URIRef of the property
            class_uri: URIRef of the class to restrict
            value_class_uri: URIRef of the value class
        """
        if not class_uri or not value_class_uri or not prop_uri:
            return

        # Extract local names for restriction naming
        class_local = str(class_uri).split("#")[-1].split("/")[-1]
        prop_local = str(prop_uri).split("#")[-1].split("/")[-1]

        restriction = URIRef(
            self.base_uri
            + f"_restriction_{class_local}_{prop_local}_{restriction_type}"
        )

        self.graph.add((restriction, RDF.type, OWL.Restriction))
        self.graph.add((restriction, OWL.onProperty, prop_uri))

        if restriction_type == "allValuesFrom":
            self.graph.add((restriction, OWL.allValuesFrom, value_class_uri))
        elif restriction_type == "someValuesFrom":
            self.graph.add((restriction, OWL.someValuesFrom, value_class_uri))

        self.graph.add((class_uri, RDFS.subClassOf, restriction))
        logger.debug(
            "Added values restriction: %s subClassOf %s", class_uri, restriction
        )

    def _add_has_value_restriction_uri(
        self, prop_uri: URIRef, class_uri: URIRef, value: str
    ):
        """Add a hasValue restriction (URI version).

        Args:
            prop_uri: URIRef of the property
            class_uri: URIRef of the class to restrict
            value: The specific value
        """
        if not class_uri or not value or not prop_uri:
            return

        # Extract local names for restriction naming
        class_local = str(class_uri).split("#")[-1].split("/")[-1]
        prop_local = str(prop_uri).split("#")[-1].split("/")[-1]

        restriction = URIRef(
            self.base_uri + f"_restriction_{class_local}_{prop_local}_hasValue"
        )

        self.graph.add((restriction, RDF.type, OWL.Restriction))
        self.graph.add((restriction, OWL.onProperty, prop_uri))
        self.graph.add((restriction, OWL.hasValue, Literal(value)))

        self.graph.add((class_uri, RDFS.subClassOf, restriction))
        logger.debug(
            "Added hasValue restriction: %s subClassOf %s", class_uri, restriction
        )

    def _add_value_check_constraint(self, constraint: Dict):
        """Add a value check constraint as OntoBricks annotation.

        Args:
            constraint: Constraint definition with 'className', 'attributeName', 'checkType', 'checkValue', etc.
        """
        class_ref = constraint.get("className", "")
        attribute_name = constraint.get("attributeName", "")
        check_type = constraint.get("checkType", "")
        check_value = constraint.get("checkValue", "")
        case_sensitive = constraint.get("caseSensitive", False)

        if not class_ref or not attribute_name or not check_type:
            logger.debug("Missing required values for valueCheck: %s", constraint)
            return

        class_uri = self._resolve_uri(class_ref)

        # Extract class local name for constraint naming
        class_local = str(class_uri).split("#")[-1].split("/")[-1]

        # Sanitize attribute name for use in URI (replace spaces and special chars)
        attr_safe = re.sub(r"[^a-zA-Z0-9_]", "_", attribute_name)

        # Create a constraint annotation resource
        constraint_uri = URIRef(
            self.base_uri + f"_valueConstraint_{class_local}_{attr_safe}_{check_type}"
        )

        # Add constraint as an annotation class
        self.graph.add((constraint_uri, RDF.type, ONTOBRICKS_NS.ValueConstraint))
        self.graph.add((constraint_uri, ONTOBRICKS_NS.appliesTo, class_uri))
        self.graph.add(
            (constraint_uri, ONTOBRICKS_NS.onAttribute, Literal(attribute_name))
        )
        self.graph.add((constraint_uri, ONTOBRICKS_NS.checkType, Literal(check_type)))

        if check_value:
            self.graph.add(
                (constraint_uri, ONTOBRICKS_NS.checkValue, Literal(check_value))
            )

        self.graph.add(
            (
                constraint_uri,
                ONTOBRICKS_NS.caseSensitive,
                Literal(case_sensitive, datatype=XSD.boolean),
            )
        )

        # Also add annotation to the class for easy discovery
        self.graph.add((class_uri, ONTOBRICKS_NS.hasValueConstraint, constraint_uri))

        logger.debug(
            "Added value check constraint: %s.%s %s '%s'",
            class_local,
            attribute_name,
            check_type,
            check_value,
        )

    def _add_global_rule_constraint(self, constraint: Dict):
        """Add a global rule constraint as OntoBricks annotation.

        Args:
            constraint: Constraint definition with 'ruleName'
        """
        rule_name = constraint.get("ruleName", "")

        if not rule_name:
            return

        # Create a global rule resource
        rule_uri = URIRef(self.base_uri + f"_globalRule_{rule_name}")

        self.graph.add((rule_uri, RDF.type, ONTOBRICKS_NS.GlobalRule))
        self.graph.add((rule_uri, ONTOBRICKS_NS.ruleName, Literal(rule_name)))

        # Add description based on rule type
        rule_descriptions = {
            "noOrphans": "Every entity must have at least one relationship",
            "requireLabels": "Every entity must have an rdfs:label",
            "uniqueIds": "All entity identifiers must be unique",
        }
        if rule_name in rule_descriptions:
            self.graph.add(
                (rule_uri, RDFS.comment, Literal(rule_descriptions[rule_name]))
            )

        logger.debug("Added global rule: %s", rule_name)

    def _add_class(self, cls: Dict):
        """Add a class to the ontology.

        Args:
            cls: Class definition with 'name', 'label', 'comment', 'parent', 'emoji', 'dashboard', 'dataProperties'
        """
        class_name = cls.get("name", "").strip()
        if not class_name:
            return

        logger.debug(
            "Processing class: %s, parent: %s", class_name, cls.get("parent", "NONE")
        )

        class_uri = URIRef(self.base_uri + class_name)

        # Define as OWL Class
        self.graph.add((class_uri, RDF.type, OWL.Class))

        # Add label
        label = cls.get("label", class_name)
        if label:
            self.graph.add((class_uri, RDFS.label, Literal(label)))

        # Add comment/description
        comment = cls.get("comment", "") or cls.get("description", "")
        if comment:
            self.graph.add((class_uri, RDFS.comment, Literal(comment)))

        # Add emoji/icon using custom OntoBricks property
        emoji = cls.get("emoji", "")
        if emoji:
            self.graph.add((class_uri, ONTOBRICKS_NS.icon, Literal(emoji)))

        # Add dashboard URL using custom OntoBricks property
        dashboard = cls.get("dashboard", "")
        if dashboard:
            self.graph.add(
                (
                    class_uri,
                    ONTOBRICKS_NS.dashboard,
                    Literal(dashboard, datatype=XSD.anyURI),
                )
            )

        # Add dashboard parameters as JSON string
        dashboard_params = cls.get("dashboardParams", {})
        if dashboard_params:
            self.graph.add(
                (
                    class_uri,
                    ONTOBRICKS_NS.dashboardParams,
                    Literal(json.dumps(dashboard_params)),
                )
            )

        # Add cross-project bridges as JSON string
        bridges = cls.get("bridges", [])
        if bridges:
            self.graph.add(
                (class_uri, ONTOBRICKS_NS.bridges, Literal(json.dumps(bridges)))
            )

        # Add parent class (subClassOf)
        parent = cls.get("parent", "").strip() if cls.get("parent") else ""
        if parent:
            logger.debug("Adding subClassOf: %s -> %s", class_name, parent)
            if parent.startswith("http://") or parent.startswith("https://"):
                parent_uri = URIRef(parent)
            else:
                parent_uri = URIRef(self.base_uri + parent)
            self.graph.add((class_uri, RDFS.subClassOf, parent_uri))

        # Add data properties (attributes) for this class
        data_props = cls.get("dataProperties", [])
        for data_prop in data_props:
            self._add_data_property_for_class(data_prop, class_name)

    def _sanitize_name(self, name: str) -> str:
        """Sanitize a name to be URI-safe.

        Args:
            name: Original name (may contain spaces, special chars)

        Returns:
            URI-safe name
        """
        if not name:
            return name
        # Replace spaces with underscores
        sanitized = name.replace(" ", "_")
        # Remove or replace other problematic characters
        sanitized = "".join(c if c.isalnum() or c == "_" else "_" for c in sanitized)
        # Remove consecutive underscores
        while "__" in sanitized:
            sanitized = sanitized.replace("__", "_")
        # Remove leading/trailing underscores
        sanitized = sanitized.strip("_")
        return sanitized

    def _add_data_property_for_class(self, data_prop: Dict, class_name: str):
        """Add a data property (attribute) for a specific class.

        Args:
            data_prop: Data property definition with 'name', 'type', etc.
            class_name: Name of the class this property belongs to
        """
        # Handle different formats - can be string or dict
        if isinstance(data_prop, str):
            prop_name = data_prop.strip()
        else:
            prop_name = (
                data_prop.get("name", "") or data_prop.get("localName", "")
            ).strip()

        if not prop_name:
            return

        # Store original name for label
        original_name = prop_name

        # Sanitize property name for URI
        prop_name = self._sanitize_name(prop_name)
        if not prop_name:
            return

        # Create a unique property name to avoid conflicts
        # Use format: className_propertyName or just propertyName
        prop_uri = URIRef(self.base_uri + prop_name)

        if (prop_uri, RDF.type, OWL.DatatypeProperty) in self.graph:
            return

        # Define as OWL DatatypeProperty
        self.graph.add((prop_uri, RDF.type, OWL.DatatypeProperty))

        # Add label (use original name for human readability)
        self.graph.add((prop_uri, RDFS.label, Literal(original_name)))

        # Add domain (the class this property belongs to)
        domain_uri = URIRef(self.base_uri + class_name)
        self.graph.add((prop_uri, RDFS.domain, domain_uri))

        # Add range (default to xsd:string)
        prop_type = (
            data_prop.get("type", "string") if isinstance(data_prop, dict) else "string"
        )
        range_uri = self._get_xsd_type(prop_type)
        self.graph.add((prop_uri, RDFS.range, range_uri))

    def _get_xsd_type(self, type_name: str) -> URIRef:
        """Convert a type name to XSD URI.

        Args:
            type_name: Type name (string, integer, date, etc.)

        Returns:
            XSD URI for the type
        """
        type_mapping = {
            "string": XSD.string,
            "text": XSD.string,
            "integer": XSD.integer,
            "int": XSD.integer,
            "number": XSD.decimal,
            "decimal": XSD.decimal,
            "float": XSD.float,
            "double": XSD.double,
            "boolean": XSD.boolean,
            "bool": XSD.boolean,
            "date": XSD.date,
            "datetime": XSD.dateTime,
            "time": XSD.time,
            "uri": XSD.anyURI,
            "url": XSD.anyURI,
        }
        return type_mapping.get(type_name.lower(), XSD.string)

    def _add_property(self, prop: Dict):
        """Add a property to the ontology.

        Args:
            prop: Property definition with 'name', 'label', 'comment', 'type', 'domain', 'range', 'properties'
        """
        prop_name = prop.get("name", "").strip()
        if not prop_name:
            return

        prop_uri = URIRef(self.base_uri + prop_name)

        # Determine property type
        prop_type = prop.get("type", "ObjectProperty")
        if prop_type == "DatatypeProperty":
            self.graph.add((prop_uri, RDF.type, OWL.DatatypeProperty))
        else:
            self.graph.add((prop_uri, RDF.type, OWL.ObjectProperty))

        # Add label
        label = prop.get("label", prop_name)
        if label:
            self.graph.add((prop_uri, RDFS.label, Literal(label)))

        # Add comment
        comment = prop.get("comment", "")
        if comment:
            self.graph.add((prop_uri, RDFS.comment, Literal(comment)))

        # Add direction as custom property
        direction = prop.get("direction", "forward")
        if direction:
            self.graph.add((prop_uri, ONTOBRICKS_NS.direction, Literal(direction)))

        # Add domain
        domain = prop.get("domain", "")
        if domain:
            domain = domain.strip()
            if domain.startswith("http://") or domain.startswith("https://"):
                domain_uri = URIRef(domain)
            else:
                domain_uri = URIRef(self.base_uri + domain)
            self.graph.add((prop_uri, RDFS.domain, domain_uri))

        # Add range
        range_val = prop.get("range", "")
        if range_val:
            range_val = range_val.strip()
            if range_val.startswith("http://") or range_val.startswith("https://"):
                range_uri = URIRef(range_val)
            elif range_val.startswith("xsd:"):
                # Handle XSD datatypes
                datatype = range_val.replace("xsd:", "")
                range_uri = self._get_xsd_type(datatype)
            else:
                range_uri = URIRef(self.base_uri + range_val)
            self.graph.add((prop_uri, RDFS.range, range_uri))

        # Note: Relationship attributes are not supported - relationships are simple ObjectProperties

    def _add_swrl_rule(self, rule: Dict):
        """Add a SWRL rule to the ontology as OntoBricks annotation.

        Note: Full SWRL requires RIF or SWRL XML format. Here we store as annotations
        that can be parsed back and used by OntoBricks for quality checks.

        Args:
            rule: Rule definition with 'name', 'description', 'antecedent', 'consequent'
        """
        name = rule.get("name", "").strip()
        if not name:
            return

        # Sanitize name for URI
        safe_name = self._sanitize_name(name)
        rule_uri = URIRef(self.base_uri + f"_swrlRule_{safe_name}")

        # Add rule as OntoBricks SWRL rule resource
        self.graph.add((rule_uri, RDF.type, ONTOBRICKS_NS.SWRLRule))
        self.graph.add((rule_uri, RDFS.label, Literal(name)))

        # Add description
        description = rule.get("description", "")
        if description:
            self.graph.add((rule_uri, RDFS.comment, Literal(description)))

        # Add antecedent (IF part)
        antecedent = rule.get("antecedent", "")
        if antecedent:
            self.graph.add((rule_uri, ONTOBRICKS_NS.antecedent, Literal(antecedent)))

        # Add consequent (THEN part)
        consequent = rule.get("consequent", "")
        if consequent:
            self.graph.add((rule_uri, ONTOBRICKS_NS.consequent, Literal(consequent)))

        logger.debug("Added SWRL rule: %s", name)

    def _add_axiom(self, axiom: Dict):
        """Add an OWL axiom to the ontology.

        Expressions (unionOf, intersectionOf, complementOf, oneOf) are wrapped
        in an anonymous owl:Class node linked via owl:equivalentClass, per the
        OWL 2 specification.

        Args:
            axiom: Axiom definition with 'type', 'subject', 'objects', etc.
        """
        axiom_type = axiom.get("type", "")
        subject = axiom.get("subject", "")
        objects = axiom.get("objects", [])

        if not axiom_type:
            return

        get_uri = self._resolve_uri
        collect_uris = self._collect_uris

        subject_uri = get_uri(subject) if subject else None

        # ── Class axioms ─────────────────────────────────────────

        if axiom_type == "equivalentClass" and subject_uri and objects:
            for obj_uri in collect_uris(objects):
                self.graph.add((subject_uri, OWL.equivalentClass, obj_uri))
            logger.debug("Added equivalentClass: %s = %s", subject, objects)

        elif axiom_type == "disjointWith" and subject_uri and objects:
            for obj_uri in collect_uris(objects):
                self.graph.add((subject_uri, OWL.disjointWith, obj_uri))
            logger.debug("Added disjointWith: %s disjoint %s", subject, objects)

        elif axiom_type == "disjointUnion" and subject_uri and objects:
            obj_uris = collect_uris(objects)
            if obj_uris:
                members = BNode()
                Collection(self.graph, members, obj_uris)
                self.graph.add((subject_uri, OWL.disjointUnionOf, members))
                logger.debug(
                    "Added disjointUnion: %s disjointUnionOf %s", subject, objects
                )

        # ── Property axioms ──────────────────────────────────────

        elif axiom_type == "equivalentProperty" and subject_uri and objects:
            for obj_uri in collect_uris(objects):
                self.graph.add((subject_uri, OWL.equivalentProperty, obj_uri))
            logger.debug("Added equivalentProperty: %s = %s", subject, objects)

        elif axiom_type == "inverseOf" and subject_uri and objects:
            for obj_uri in collect_uris(objects):
                self.graph.add((subject_uri, OWL.inverseOf, obj_uri))
            logger.debug("Added inverseOf: %s inverse %s", subject, objects)

        elif axiom_type == "disjointProperties" and subject_uri and objects:
            for obj_uri in collect_uris(objects):
                self.graph.add((subject_uri, OWL.propertyDisjointWith, obj_uri))
            logger.debug("Added disjointProperties: %s disjoint %s", subject, objects)

        elif axiom_type == "propertyChain":
            chain = axiom.get("chain", [])
            result_property = axiom.get("resultProperty", "")
            if len(chain) >= 2 and result_property:
                result_uri = get_uri(result_property)
                chain_uris = collect_uris(chain)
                if result_uri and len(chain_uris) >= 2:
                    chain_list = BNode()
                    Collection(self.graph, chain_list, chain_uris)
                    self.graph.add((result_uri, OWL.propertyChainAxiom, chain_list))
                    logger.debug("Added propertyChain: %s = %s", result_property, chain)

        # ── Class expressions (wrapped via owl:equivalentClass) ──

        elif axiom_type == "unionOf" and subject_uri and objects:
            obj_uris = collect_uris(objects)
            if obj_uris:
                members = BNode()
                Collection(self.graph, members, obj_uris)
                anon = BNode()
                self.graph.add((anon, RDF.type, OWL.Class))
                self.graph.add((anon, OWL.unionOf, members))
                self.graph.add((subject_uri, OWL.equivalentClass, anon))
                logger.debug(
                    "Added unionOf: %s equivalentClass unionOf(%s)", subject, objects
                )

        elif axiom_type == "intersectionOf" and subject_uri and objects:
            obj_uris = collect_uris(objects)
            if obj_uris:
                members = BNode()
                Collection(self.graph, members, obj_uris)
                anon = BNode()
                self.graph.add((anon, RDF.type, OWL.Class))
                self.graph.add((anon, OWL.intersectionOf, members))
                self.graph.add((subject_uri, OWL.equivalentClass, anon))
                logger.debug(
                    "Added intersectionOf: %s equivalentClass intersectionOf(%s)",
                    subject,
                    objects,
                )

        elif axiom_type == "complementOf" and subject_uri and objects:
            obj_uri = get_uri(objects[0])
            if obj_uri:
                anon = BNode()
                self.graph.add((anon, RDF.type, OWL.Class))
                self.graph.add((anon, OWL.complementOf, obj_uri))
                self.graph.add((subject_uri, OWL.equivalentClass, anon))
                logger.debug(
                    "Added complementOf: %s equivalentClass complementOf(%s)",
                    subject,
                    objects[0],
                )

        elif axiom_type == "oneOf" and subject_uri:
            individuals_raw = axiom.get("individuals", "")
            if isinstance(individuals_raw, str):
                individuals = [
                    i.strip() for i in individuals_raw.split(",") if i.strip()
                ]
            else:
                individuals = individuals_raw or []
            ind_uris = collect_uris(individuals)
            if ind_uris:
                members = BNode()
                Collection(self.graph, members, ind_uris)
                anon = BNode()
                self.graph.add((anon, RDF.type, OWL.Class))
                self.graph.add((anon, OWL.oneOf, members))
                self.graph.add((subject_uri, OWL.equivalentClass, anon))
                logger.debug(
                    "Added oneOf: %s equivalentClass oneOf(%s)", subject, individuals
                )

    def _add_groups(self):
        """Add entity groups as OWL defined classes using owl:equivalentClass + owl:unionOf.

        Each group becomes an ``owl:Class`` annotated with ``ontobricks:isGroup true``
        and linked via ``owl:equivalentClass`` to an anonymous class whose
        ``owl:unionOf`` lists the member classes.  Annotation properties
        ``ontobricks:isGroup`` and ``ontobricks:groupColor`` are declared once.
        """
        if not self.groups:
            return

        # Declare annotation properties once
        is_group_prop = ONTOBRICKS_NS.isGroup
        group_color_prop = ONTOBRICKS_NS.groupColor
        self.graph.add((is_group_prop, RDF.type, OWL.AnnotationProperty))
        self.graph.add((group_color_prop, RDF.type, OWL.AnnotationProperty))

        for group in self.groups:
            name = group.get("name", "").strip()
            if not name:
                continue
            member_names = [
                m.strip() for m in group.get("members", []) if m and m.strip()
            ]
            if not member_names:
                logger.debug("Skipping group '%s' — no members", name)
                continue

            group_uri = URIRef(self.base_uri + name)

            self.graph.add((group_uri, RDF.type, OWL.Class))
            self.graph.add(
                (group_uri, ONTOBRICKS_NS.isGroup, Literal(True, datatype=XSD.boolean))
            )

            label = group.get("label", name)
            if label:
                self.graph.add((group_uri, RDFS.label, Literal(label)))

            description = group.get("description", "")
            if description:
                self.graph.add((group_uri, RDFS.comment, Literal(description)))

            color = group.get("color", "")
            if color:
                self.graph.add((group_uri, ONTOBRICKS_NS.groupColor, Literal(color)))

            icon = group.get("icon", "")
            if icon:
                self.graph.add((group_uri, ONTOBRICKS_NS.icon, Literal(icon)))

            member_uris = []
            for m in member_names:
                if m.startswith("http://") or m.startswith("https://"):
                    member_uris.append(URIRef(m))
                else:
                    member_uris.append(URIRef(self.base_uri + m))

            members_list = BNode()
            Collection(self.graph, members_list, member_uris)
            union_class = BNode()
            self.graph.add((union_class, RDF.type, OWL.Class))
            self.graph.add((union_class, OWL.unionOf, members_list))
            self.graph.add((group_uri, OWL.equivalentClass, union_class))

            logger.debug(
                "Added group '%s' with %d members: %s",
                name,
                len(member_uris),
                member_names,
            )
