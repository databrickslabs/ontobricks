"""OWL ontology parser."""
import json
from rdflib import Graph, RDF, RDFS, OWL, BNode
from typing import List, Dict, Optional

from back.core.logging import get_logger
from back.core.errors import ValidationError
from shared.config.constants import DEFAULT_BASE_URI, ONTOBRICKS_NS

logger = get_logger(__name__)


class OntologyParser:
    """Parse OWL ontologies to extract classes and properties."""
    
    def __init__(self, owl_content: str):
        """Initialize the parser with OWL content.
        
        Args:
            owl_content: OWL content (Turtle, RDF/XML, etc.)
        """
        self.graph = Graph()
        
        # Check for truncated content (common with LLM generation)
        content_stripped = owl_content.strip()
        if content_stripped and not (content_stripped.endswith('.') or content_stripped.endswith(']') or content_stripped.endswith('>')):
            # Content appears truncated - try to salvage by removing incomplete last line
            lines = content_stripped.split('\n')
            # Remove lines until we find one ending with a valid terminator
            while lines and not (lines[-1].strip().endswith('.') or lines[-1].strip().endswith(']') or lines[-1].strip() == '' or lines[-1].strip().startswith('#')):
                lines.pop()
            if lines:
                owl_content = '\n'.join(lines)
                logger.warning("Content appeared truncated, removed incomplete statements")
        
        from back.core.w3c.rdf_utils import parse_rdf_flexible
        try:
            self.graph = parse_rdf_flexible(owl_content, formats=("turtle", "xml"))
        except ValueError as e:
            raise ValidationError(f"Failed to parse OWL content: {e}") from e
    
    @staticmethod
    def _to_camel_case(name: str) -> str:
        """Convert a name with spaces/underscores/hyphens to camelCase or PascalCase.
        
        Preserves the case of the first character:
        - "Contract ID" → "ContractId" (PascalCase if starts uppercase)
        - "street address" → "streetAddress" (camelCase if starts lowercase)
        - "first_name" → "firstName"
        
        Args:
            name: Raw name that may contain spaces, underscores, or hyphens
            
        Returns:
            camelCase or PascalCase name
        """
        import re
        if not name:
            return name
        
        # Split by spaces, underscores, or hyphens
        words = re.split(r'[\s_-]+', name.strip())
        words = [w for w in words if w]  # Remove empty strings
        
        if not words:
            return name
        
        # If already a single word with no separators, return as-is
        if len(words) == 1:
            return words[0]
        
        # Check if PascalCase (first char uppercase) or camelCase
        is_pascal = words[0][0].isupper()
        
        if is_pascal:
            return ''.join(w.capitalize() for w in words)
        else:
            return words[0].lower() + ''.join(w.capitalize() for w in words[1:])
    
    def _extract_local_name(self, uri: str) -> str:
        """Extract the local name from a URI and ensure camelCase/PascalCase.
        
        Args:
            uri: Full URI like http://example.org/ontology#ClassName
            
        Returns:
            Local name in camelCase/PascalCase like ClassName
        """
        if not uri:
            return ''
        raw_name = uri.split('#')[-1].split('/')[-1]
        return self._to_camel_case(raw_name)
        
    def _get_group_class_uris(self) -> set:
        """Return the set of class URI strings that are marked as groups."""
        group_uris = set()
        for cls in self.graph.subjects(ONTOBRICKS_NS.isGroup, None):
            if isinstance(cls, BNode):
                continue
            for val in self.graph.objects(cls, ONTOBRICKS_NS.isGroup):
                if str(val).lower() == 'true':
                    group_uris.add(str(cls))
                    break
        return group_uris

    def get_groups(self) -> List[Dict]:
        """Extract entity groups from the ontology.

        Groups are OWL classes annotated with ``ontobricks:isGroup true`` whose
        ``owl:equivalentClass`` points to an anonymous ``owl:unionOf`` list.

        Returns:
            List of group dicts with name, label, description, color, icon, members.
        """
        groups = []
        group_uris = self._get_group_class_uris()

        for group_uri_str in group_uris:
            group_ref = None
            for s in self.graph.subjects(RDF.type, OWL.Class):
                if not isinstance(s, BNode) and str(s) == group_uri_str:
                    group_ref = s
                    break
            if group_ref is None:
                continue

            name = self._extract_local_name(group_uri_str)

            label = None
            for lbl in self.graph.objects(group_ref, RDFS.label):
                label = str(lbl)
                break

            description = None
            for cmt in self.graph.objects(group_ref, RDFS.comment):
                description = str(cmt)
                break

            color = None
            for c in self.graph.objects(group_ref, ONTOBRICKS_NS.groupColor):
                color = str(c)
                break

            icon = None
            for i in self.graph.objects(group_ref, ONTOBRICKS_NS.icon):
                icon = str(i)
                break

            members = []
            for eq in self.graph.objects(group_ref, OWL.equivalentClass):
                for union_node in self.graph.objects(eq, OWL.unionOf):
                    member_uris = self._parse_rdf_list(union_node)
                    members = [self._extract_local_name(u) for u in member_uris]
                    break
                if members:
                    break

            groups.append({
                'name': name,
                'label': label or name,
                'description': description or '',
                'color': color or '',
                'icon': icon or '',
                'members': members,
            })

        return sorted(groups, key=lambda x: x['name'])

    def get_classes(self) -> List[Dict[str, str]]:
        """Extract all OWL classes from the ontology.

        Classes annotated with ``ontobricks:isGroup true`` are excluded (they
        are returned by :meth:`get_groups` instead).
        
        Returns:
            List of dicts with 'uri', 'name', 'label', 'comment', 'emoji', 'parent', 'group', 'dashboard', 'dashboardParams', 'dataProperties'
        """
        classes = []
        group_uris = self._get_group_class_uris()

        # Build a reverse lookup: class URI → group name
        class_to_group = {}
        for g_uri in group_uris:
            g_ref = None
            for s in self.graph.subjects(RDF.type, OWL.Class):
                if not isinstance(s, BNode) and str(s) == g_uri:
                    g_ref = s
                    break
            if g_ref is None:
                continue
            g_name = self._extract_local_name(g_uri)
            for eq in self.graph.objects(g_ref, OWL.equivalentClass):
                for union_node in self.graph.objects(eq, OWL.unionOf):
                    for member_uri in self._parse_rdf_list(union_node):
                        class_to_group[member_uri] = g_name
                    break
        
        # First, collect all DatatypeProperties with their domains
        # to reconstruct class attributes (dataProperties)
        domain_to_dataprops = {}
        for prop in self.graph.subjects(RDF.type, OWL.DatatypeProperty):
            # Skip blank nodes
            if isinstance(prop, BNode):
                continue
            prop_uri = str(prop)
            
            # Get the domain (which class this property belongs to)
            for domain in self.graph.objects(prop, RDFS.domain):
                # Skip blank nodes
                if isinstance(domain, BNode):
                    continue
                domain_uri = str(domain)
                
                # Get property label
                prop_label = None
                for lbl in self.graph.objects(prop, RDFS.label):
                    prop_label = str(lbl)
                    break
                
                prop_local_name = self._extract_local_name(prop_uri)
                
                if domain_uri not in domain_to_dataprops:
                    domain_to_dataprops[domain_uri] = []
                domain_to_dataprops[domain_uri].append({
                    'name': prop_local_name,
                    'localName': prop_local_name,
                    'label': prop_label or prop_local_name,
                    'uri': prop_uri
                })
        
        for cls in self.graph.subjects(RDF.type, OWL.Class):
            # Skip blank nodes (anonymous classes like restrictions, unions, etc.)
            if isinstance(cls, BNode):
                continue
            
            uri = str(cls)

            # Skip group classes
            if uri in group_uris:
                continue
            
            name = self._extract_local_name(uri)
            
            # Get label
            label = None
            for lbl in self.graph.objects(cls, RDFS.label):
                label = str(lbl)
                break
            
            # Get comment
            comment = None
            for cmt in self.graph.objects(cls, RDFS.comment):
                comment = str(cmt)
                break
            
            # Get emoji/icon from OntoBricks custom property
            emoji = None
            for icon_val in self.graph.objects(cls, ONTOBRICKS_NS.icon):
                emoji = str(icon_val)
                break
            
            # Get dashboard URL from OntoBricks custom property
            dashboard = None
            for dash in self.graph.objects(cls, ONTOBRICKS_NS.dashboard):
                dashboard = str(dash)
                break
            
            # Get dashboard parameters from OntoBricks custom property
            dashboard_params = {}
            for params in self.graph.objects(cls, ONTOBRICKS_NS.dashboardParams):
                try:
                    dashboard_params = json.loads(str(params))
                except (json.JSONDecodeError, ValueError):
                    pass
                break

            # Get cross-project bridges from OntoBricks custom property
            bridges = []
            for b in self.graph.objects(cls, ONTOBRICKS_NS.bridges):
                try:
                    bridges = json.loads(str(b))
                except (json.JSONDecodeError, ValueError):
                    pass
                break
            
            # Get parent class (subClassOf)
            parent = None
            for parent_cls in self.graph.objects(cls, RDFS.subClassOf):
                parent_uri = str(parent_cls)
                # Skip blank nodes and Thing
                if not isinstance(parent_cls, BNode) and not parent_uri.endswith('Thing'):
                    parent = self._extract_local_name(parent_uri)
                    break
            
            # Get dataProperties (attributes) for this class
            data_properties = domain_to_dataprops.get(uri, [])

            # Determine group membership
            group = class_to_group.get(uri, '')
            
            classes.append({
                'uri': uri,
                'name': name,
                'label': label or name,
                'comment': comment or '',
                'emoji': emoji or '',
                'parent': parent or '',
                'group': group,
                'dashboard': dashboard or '',
                'dashboardParams': dashboard_params,
                'bridges': bridges,
                'dataProperties': data_properties
            })
        
        return sorted(classes, key=lambda x: x['name'])
    
    def get_properties(self) -> List[Dict[str, str]]:
        """Extract all OWL properties from the ontology.
        
        Returns:
            List of dicts with 'uri', 'name', 'label', 'comment', 'type', 'domain', 'range'
        """
        properties = []
        
        # Get all object properties and datatype properties
        prop_types = [
            (OWL.ObjectProperty, 'ObjectProperty'),
            (OWL.DatatypeProperty, 'DatatypeProperty')
        ]
        
        for prop_class, prop_type in prop_types:
            for prop in self.graph.subjects(RDF.type, prop_class):
                # Skip blank nodes
                if isinstance(prop, BNode):
                    continue
                
                # Get local name
                uri = str(prop)
                name = self._extract_local_name(uri)
                
                # Get label
                label = None
                for lbl in self.graph.objects(prop, RDFS.label):
                    label = str(lbl)
                    break
                
                # Get comment
                comment = None
                for cmt in self.graph.objects(prop, RDFS.comment):
                    comment = str(cmt)
                    break
                
                # Get domain - extract local name
                domain = None
                for dom in self.graph.objects(prop, RDFS.domain):
                    domain = self._extract_local_name(str(dom))
                    break
                
                # Get range - extract local name
                range_val = None
                for rng in self.graph.objects(prop, RDFS.range):
                    range_val = self._extract_local_name(str(rng))
                    break
                
                properties.append({
                    'uri': uri,
                    'name': name,
                    'label': label or name,
                    'comment': comment or '',
                    'type': prop_type,
                    'domain': domain or '',
                    'range': range_val or ''
                })
        
        return sorted(properties, key=lambda x: x['name'])
    
    def get_ontology_info(self) -> Dict[str, str]:
        """Get basic ontology information.
        
        Returns:
            Dict with 'uri', 'label', 'comment', 'namespace'
        """
        # Find ontology resource
        for onto in self.graph.subjects(RDF.type, OWL.Ontology):
            uri = str(onto)
            
            # Get label
            label = None
            for lbl in self.graph.objects(onto, RDFS.label):
                label = str(lbl)
                break
            
            # Get comment
            comment = None
            for cmt in self.graph.objects(onto, RDFS.comment):
                comment = str(cmt)
                break
            
            # Determine namespace (add # if not present)
            namespace = uri
            if not namespace.endswith('#') and not namespace.endswith('/'):
                namespace = namespace + '#'
            
            return {
                'uri': uri,
                'label': label or self._extract_local_name(uri) or 'Ontology',
                'comment': comment or '',
                'namespace': namespace
            }
        
        return {
            'uri': 'Unknown',
            'label': 'Unknown Ontology',
            'comment': '',
            'namespace': DEFAULT_BASE_URI
        }


    def get_constraints(self) -> List[Dict]:
        """Extract property constraints from the ontology.
        
        Returns:
            List of constraint dicts with 'type', 'property', 'className', 'value', etc.
        """
        constraints = []
        
        # Extract property characteristics
        property_characteristics = [
            (OWL.FunctionalProperty, 'functional'),
            (OWL.InverseFunctionalProperty, 'inverseFunctional'),
            (OWL.TransitiveProperty, 'transitive'),
            (OWL.SymmetricProperty, 'symmetric'),
            (OWL.AsymmetricProperty, 'asymmetric'),
            (OWL.ReflexiveProperty, 'reflexive'),
            (OWL.IrreflexiveProperty, 'irreflexive'),
        ]
        
        for prop_class, constraint_type in property_characteristics:
            for prop in self.graph.subjects(RDF.type, prop_class):
                prop_uri = str(prop)
                if not prop_uri.startswith('_:'):
                    prop_name = self._extract_local_name(prop_uri)
                    constraints.append({
                        'type': constraint_type,
                        'property': prop_name,
                        'propertyUri': prop_uri
                    })
        
        # Extract cardinality and value restrictions from subClassOf
        for cls in self.graph.subjects(RDF.type, OWL.Class):
            cls_uri = str(cls)
            if cls_uri.startswith('_:'):
                continue
            cls_name = self._extract_local_name(cls_uri)
            
            for restriction in self.graph.objects(cls, RDFS.subClassOf):
                # Check if it's a restriction
                if (restriction, RDF.type, OWL.Restriction) not in self.graph:
                    continue
                
                # Get the property
                prop_uri = None
                for p in self.graph.objects(restriction, OWL.onProperty):
                    prop_uri = str(p)
                    break
                
                if not prop_uri:
                    continue
                
                prop_name = self._extract_local_name(prop_uri)
                
                # Check for cardinality constraints
                for card_val in self.graph.objects(restriction, OWL.minCardinality):
                    constraints.append({
                        'type': 'minCardinality',
                        'property': prop_name,
                        'propertyUri': prop_uri,
                        'className': cls_name,
                        'classUri': cls_uri,
                        'cardinalityValue': int(card_val)
                    })
                
                for card_val in self.graph.objects(restriction, OWL.maxCardinality):
                    constraints.append({
                        'type': 'maxCardinality',
                        'property': prop_name,
                        'propertyUri': prop_uri,
                        'className': cls_name,
                        'classUri': cls_uri,
                        'cardinalityValue': int(card_val)
                    })
                
                for card_val in self.graph.objects(restriction, OWL.cardinality):
                    constraints.append({
                        'type': 'exactCardinality',
                        'property': prop_name,
                        'propertyUri': prop_uri,
                        'className': cls_name,
                        'classUri': cls_uri,
                        'cardinalityValue': int(card_val)
                    })
                
                # Check for allValuesFrom
                for val_class in self.graph.objects(restriction, OWL.allValuesFrom):
                    val_class_uri = str(val_class)
                    if not val_class_uri.startswith('_:'):
                        constraints.append({
                            'type': 'allValuesFrom',
                            'property': prop_name,
                            'propertyUri': prop_uri,
                            'className': cls_name,
                            'classUri': cls_uri,
                            'valueClass': self._extract_local_name(val_class_uri)
                        })
                
                # Check for someValuesFrom
                for val_class in self.graph.objects(restriction, OWL.someValuesFrom):
                    val_class_uri = str(val_class)
                    if not val_class_uri.startswith('_:'):
                        constraints.append({
                            'type': 'someValuesFrom',
                            'property': prop_name,
                            'propertyUri': prop_uri,
                            'className': cls_name,
                            'classUri': cls_uri,
                            'valueClass': self._extract_local_name(val_class_uri)
                        })
                
                # Check for hasValue
                for val in self.graph.objects(restriction, OWL.hasValue):
                    constraints.append({
                        'type': 'hasValue',
                        'property': prop_name,
                        'propertyUri': prop_uri,
                        'className': cls_name,
                        'classUri': cls_uri,
                        'hasValue': str(val)
                    })
        
        # Extract OntoBricks value constraints
        for constraint_res in self.graph.subjects(RDF.type, ONTOBRICKS_NS.ValueConstraint):
            constraint = {'type': 'valueCheck'}
            
            for cls in self.graph.objects(constraint_res, ONTOBRICKS_NS.appliesTo):
                constraint['className'] = self._extract_local_name(str(cls))
            
            for attr in self.graph.objects(constraint_res, ONTOBRICKS_NS.onAttribute):
                constraint['attributeName'] = str(attr)
            
            for check_type in self.graph.objects(constraint_res, ONTOBRICKS_NS.checkType):
                constraint['checkType'] = str(check_type)
            
            for check_val in self.graph.objects(constraint_res, ONTOBRICKS_NS.checkValue):
                constraint['checkValue'] = str(check_val)
            
            for case_sens in self.graph.objects(constraint_res, ONTOBRICKS_NS.caseSensitive):
                constraint['caseSensitive'] = str(case_sens).lower() == 'true'
            
            if constraint.get('className') and constraint.get('checkType'):
                constraints.append(constraint)
        
        # Extract OntoBricks global rules
        for rule_res in self.graph.subjects(RDF.type, ONTOBRICKS_NS.GlobalRule):
            for rule_name in self.graph.objects(rule_res, ONTOBRICKS_NS.ruleName):
                constraints.append({
                    'type': 'globalRule',
                    'ruleName': str(rule_name)
                })
        
        return constraints
    
    def get_swrl_rules(self) -> List[Dict]:
        """Extract SWRL rules from the ontology.
        
        Returns:
            List of rule dicts with 'name', 'description', 'antecedent', 'consequent'
        """
        rules = []
        
        # Extract OntoBricks SWRL rules
        for rule_res in self.graph.subjects(RDF.type, ONTOBRICKS_NS.SWRLRule):
            rule = {}
            
            for label in self.graph.objects(rule_res, RDFS.label):
                rule['name'] = str(label)
            
            for comment in self.graph.objects(rule_res, RDFS.comment):
                rule['description'] = str(comment)
            
            for ant in self.graph.objects(rule_res, ONTOBRICKS_NS.antecedent):
                rule['antecedent'] = str(ant)
            
            for cons in self.graph.objects(rule_res, ONTOBRICKS_NS.consequent):
                rule['consequent'] = str(cons)
            
            if rule.get('name') and rule.get('antecedent') and rule.get('consequent'):
                rules.append(rule)
        
        return rules
    
    _EXPRESSION_TYPES = frozenset({'unionOf', 'intersectionOf', 'complementOf', 'oneOf'})

    def get_axioms_and_expressions(self) -> Dict[str, List[Dict]]:
        """Extract OWL axioms and class expressions as two separate lists.

        Returns:
            Dict with ``'axioms'`` (logical assertions) and ``'expressions'``
            (class compositions: unionOf, intersectionOf, complementOf, oneOf).
        """
        all_items = self._get_all_axiom_items()
        axioms = [a for a in all_items if a.get('type') not in self._EXPRESSION_TYPES]
        expressions = [a for a in all_items if a.get('type') in self._EXPRESSION_TYPES]
        return {'axioms': axioms, 'expressions': expressions}

    def get_axioms(self) -> List[Dict]:
        """Extract OWL axioms from the ontology (backward-compat: returns all items).
        
        Returns:
            List of axiom dicts with 'type', 'subject', 'objects', etc.
        """
        return self._get_all_axiom_items()

    def _get_all_axiom_items(self) -> List[Dict]:
        """Internal: extract all axiom-like items (axioms + expressions) from the graph."""
        axioms = []
        
        # Extract equivalentClass axioms
        for subj in self.graph.subjects(OWL.equivalentClass, None):
            subj_uri = str(subj)
            if subj_uri.startswith('_:'):
                continue
            
            objects = []
            for obj in self.graph.objects(subj, OWL.equivalentClass):
                obj_uri = str(obj)
                if not obj_uri.startswith('_:'):
                    objects.append(self._extract_local_name(obj_uri))
            
            if objects:
                axioms.append({
                    'type': 'equivalentClass',
                    'subject': self._extract_local_name(subj_uri),
                    'subjectUri': subj_uri,
                    'objects': objects
                })
        
        # Extract disjointWith axioms
        for subj in self.graph.subjects(OWL.disjointWith, None):
            subj_uri = str(subj)
            if subj_uri.startswith('_:'):
                continue
            
            objects = []
            for obj in self.graph.objects(subj, OWL.disjointWith):
                obj_uri = str(obj)
                if not obj_uri.startswith('_:'):
                    objects.append(self._extract_local_name(obj_uri))
            
            if objects:
                axioms.append({
                    'type': 'disjointWith',
                    'subject': self._extract_local_name(subj_uri),
                    'subjectUri': subj_uri,
                    'objects': objects
                })
        
        # Extract inverseOf axioms
        for subj in self.graph.subjects(OWL.inverseOf, None):
            subj_uri = str(subj)
            if subj_uri.startswith('_:'):
                continue
            
            objects = []
            for obj in self.graph.objects(subj, OWL.inverseOf):
                obj_uri = str(obj)
                if not obj_uri.startswith('_:'):
                    objects.append(self._extract_local_name(obj_uri))
            
            if objects:
                axioms.append({
                    'type': 'inverseOf',
                    'subject': self._extract_local_name(subj_uri),
                    'subjectUri': subj_uri,
                    'objects': objects
                })
        
        # Extract propertyDisjointWith axioms
        for subj in self.graph.subjects(OWL.propertyDisjointWith, None):
            subj_uri = str(subj)
            if subj_uri.startswith('_:'):
                continue
            
            objects = []
            for obj in self.graph.objects(subj, OWL.propertyDisjointWith):
                obj_uri = str(obj)
                if not obj_uri.startswith('_:'):
                    objects.append(self._extract_local_name(obj_uri))
            
            if objects:
                axioms.append({
                    'type': 'disjointProperties',
                    'subject': self._extract_local_name(subj_uri),
                    'subjectUri': subj_uri,
                    'objects': objects
                })
        
        # Extract propertyChainAxiom
        for subj in self.graph.subjects(OWL.propertyChainAxiom, None):
            subj_uri = str(subj)
            if subj_uri.startswith('_:'):
                continue
            
            for chain_node in self.graph.objects(subj, OWL.propertyChainAxiom):
                # Parse RDF list
                chain = self._parse_rdf_list(chain_node)
                if len(chain) >= 2:
                    axioms.append({
                        'type': 'propertyChain',
                        'resultProperty': self._extract_local_name(subj_uri),
                        'resultPropertyUri': subj_uri,
                        'chain': [self._extract_local_name(p) for p in chain]
                    })
        
        # Extract unionOf
        for subj in self.graph.subjects(OWL.unionOf, None):
            subj_uri = str(subj)
            if subj_uri.startswith('_:'):
                continue
            
            for list_node in self.graph.objects(subj, OWL.unionOf):
                members = self._parse_rdf_list(list_node)
                if members:
                    axioms.append({
                        'type': 'unionOf',
                        'subject': self._extract_local_name(subj_uri),
                        'subjectUri': subj_uri,
                        'objects': [self._extract_local_name(m) for m in members]
                    })
        
        # Extract intersectionOf
        for subj in self.graph.subjects(OWL.intersectionOf, None):
            subj_uri = str(subj)
            if subj_uri.startswith('_:'):
                continue
            
            for list_node in self.graph.objects(subj, OWL.intersectionOf):
                members = self._parse_rdf_list(list_node)
                if members:
                    axioms.append({
                        'type': 'intersectionOf',
                        'subject': self._extract_local_name(subj_uri),
                        'subjectUri': subj_uri,
                        'objects': [self._extract_local_name(m) for m in members]
                    })
        
        # Extract complementOf
        for subj in self.graph.subjects(OWL.complementOf, None):
            subj_uri = str(subj)
            if subj_uri.startswith('_:'):
                continue
            
            for obj in self.graph.objects(subj, OWL.complementOf):
                obj_uri = str(obj)
                if not obj_uri.startswith('_:'):
                    axioms.append({
                        'type': 'complementOf',
                        'subject': self._extract_local_name(subj_uri),
                        'subjectUri': subj_uri,
                        'objects': [self._extract_local_name(obj_uri)]
                    })
        
        # Extract oneOf (enumeration)
        for subj in self.graph.subjects(OWL.oneOf, None):
            subj_uri = str(subj)
            if subj_uri.startswith('_:'):
                continue
            
            for list_node in self.graph.objects(subj, OWL.oneOf):
                individuals = self._parse_rdf_list(list_node)
                if individuals:
                    axioms.append({
                        'type': 'oneOf',
                        'subject': self._extract_local_name(subj_uri),
                        'subjectUri': subj_uri,
                        'individuals': [self._extract_local_name(i) for i in individuals]
                    })
        
        return axioms
    
    def _parse_rdf_list(self, node) -> List[str]:
        """Parse an RDF list (collection) and return its items as URIs.
        
        Args:
            node: Starting node of the RDF list
            
        Returns:
            List of URI strings
        """
        from rdflib import RDF as RDF_NS
        
        items = []
        current = node
        
        # Handle RDF nil
        nil_uri = str(RDF_NS.nil)
        
        while current and str(current) != nil_uri:
            # Get first item
            for first in self.graph.objects(current, RDF_NS.first):
                item_uri = str(first)
                if not item_uri.startswith('_:'):
                    items.append(item_uri)
            
            # Move to rest
            rest = None
            for r in self.graph.objects(current, RDF_NS.rest):
                rest = r
                break
            
            current = rest
        
        return items
