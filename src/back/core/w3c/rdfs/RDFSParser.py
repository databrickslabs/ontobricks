"""RDFS vocabulary parser.

Parses RDFS (RDF Schema) files to extract classes and properties.
RDFS is simpler than OWL and uses rdfs:Class instead of owl:Class.
"""

from rdflib import Graph, RDF, RDFS
from typing import List, Dict

from back.core.errors import ValidationError


class RDFSParser:
    """Parse RDFS vocabularies to extract classes and properties."""

    def __init__(self, rdfs_content: str):
        """Initialize the parser with RDFS content.

        Args:
            rdfs_content: RDFS content (Turtle, RDF/XML, N-Triples, etc.)
        """
        self.graph = Graph()
        self._parse_content(rdfs_content)

    def _parse_content(self, content: str):
        """Try to parse the content in various RDF formats.

        Args:
            content: RDF content string
        """
        from back.core.w3c.rdf_utils import parse_rdf_flexible

        try:
            self.graph = parse_rdf_flexible(content)
        except ValueError:
            raise ValidationError(
                "Could not parse RDFS content. Supported formats: Turtle, RDF/XML, N3, N-Triples, JSON-LD"
            )

    def _extract_local_name(self, uri: str) -> str:
        """Extract the local name from a URI."""
        from back.core.helpers import extract_local_name

        return extract_local_name(uri)

    def _get_base_uri(self) -> str:
        """Try to determine the base URI from the graph.

        Returns:
            Base URI string
        """
        # Try to find from namespace bindings
        for prefix, namespace in self.graph.namespaces():
            ns_str = str(namespace)
            # Skip common prefixes
            if prefix in ["rdf", "rdfs", "owl", "xsd", "xml"]:
                continue
            if ns_str and (ns_str.endswith("#") or ns_str.endswith("/")):
                return ns_str

        # Try to extract from first class or property URI
        for cls in self.graph.subjects(RDF.type, RDFS.Class):
            uri = str(cls)
            if "#" in uri:
                return uri.rsplit("#", 1)[0] + "#"
            elif "/" in uri:
                return uri.rsplit("/", 1)[0] + "/"

        return "http://example.org/schema#"

    def get_classes(self) -> List[Dict[str, str]]:
        """Extract all RDFS classes from the vocabulary.

        Returns:
            List of dicts with 'uri', 'name', 'label', 'comment', 'emoji', 'parent'
        """
        classes = []
        seen_uris = set()

        # Get rdfs:Class instances
        for cls in self.graph.subjects(RDF.type, RDFS.Class):
            uri = str(cls)

            # Skip blank nodes and already seen
            if uri.startswith("_:") or uri in seen_uris:
                continue

            # Skip RDFS/RDF built-in classes
            if uri.startswith(
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            ) or uri.startswith("http://www.w3.org/2000/01/rdf-schema#"):
                continue

            seen_uris.add(uri)
            classes.append(self._extract_class_info(cls, uri))

        # Also check for owl:Class (some RDFS files use OWL declarations)
        try:
            from rdflib import OWL

            for cls in self.graph.subjects(RDF.type, OWL.Class):
                uri = str(cls)
                if uri.startswith("_:") or uri in seen_uris:
                    continue
                seen_uris.add(uri)
                classes.append(self._extract_class_info(cls, uri))
        except ImportError:
            pass

        return sorted(classes, key=lambda x: x["name"])

    def _extract_class_info(self, cls, uri: str) -> Dict[str, str]:
        """Extract information about a class.

        Args:
            cls: RDFLib class node
            uri: Class URI

        Returns:
            Dict with class information
        """
        name = self._extract_local_name(uri)

        # Get label (try multiple languages, prefer English or no language tag)
        label = None
        for lbl in self.graph.objects(cls, RDFS.label):
            lbl_str = str(lbl)
            # Prefer label without language tag or English
            if not hasattr(lbl, "language") or lbl.language in [None, "en"]:
                label = lbl_str
                break
            if label is None:
                label = lbl_str

        # Get comment/description
        comment = None
        for cmt in self.graph.objects(cls, RDFS.comment):
            cmt_str = str(cmt)
            if not hasattr(cmt, "language") or cmt.language in [None, "en"]:
                comment = cmt_str
                break
            if comment is None:
                comment = cmt_str

        # Get parent class (subClassOf)
        parent = None
        for parent_cls in self.graph.objects(cls, RDFS.subClassOf):
            parent_uri = str(parent_cls)
            # Skip blank nodes and Resource/Thing
            if (
                not parent_uri.startswith("_:")
                and not parent_uri.endswith("Resource")
                and not parent_uri.endswith("Thing")
            ):
                parent = self._extract_local_name(parent_uri)
                break

        return {
            "uri": uri,
            "name": name,
            "label": label or name,
            "description": comment or "",
            "emoji": "",  # RDFS doesn't have icons
            "parent": parent or "",
        }

    def get_properties(self) -> List[Dict[str, str]]:
        """Extract all RDFS properties from the vocabulary.

        Returns:
            List of dicts with 'uri', 'name', 'label', 'comment', 'type', 'domain', 'range'
        """
        properties = []
        seen_uris = set()

        # Get rdf:Property instances
        for prop in self.graph.subjects(RDF.type, RDF.Property):
            uri = str(prop)

            # Skip blank nodes and already seen
            if uri.startswith("_:") or uri in seen_uris:
                continue

            # Skip RDF/RDFS built-in properties
            if uri.startswith(
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            ) or uri.startswith("http://www.w3.org/2000/01/rdf-schema#"):
                continue

            seen_uris.add(uri)
            properties.append(self._extract_property_info(prop, uri, "Property"))

        # Also check for owl:ObjectProperty and owl:DatatypeProperty
        try:
            from rdflib import OWL

            for prop in self.graph.subjects(RDF.type, OWL.ObjectProperty):
                uri = str(prop)
                if uri.startswith("_:") or uri in seen_uris:
                    continue
                seen_uris.add(uri)
                properties.append(
                    self._extract_property_info(prop, uri, "ObjectProperty")
                )

            for prop in self.graph.subjects(RDF.type, OWL.DatatypeProperty):
                uri = str(prop)
                if uri.startswith("_:") or uri in seen_uris:
                    continue
                seen_uris.add(uri)
                properties.append(
                    self._extract_property_info(prop, uri, "DatatypeProperty")
                )
        except ImportError:
            pass

        return sorted(properties, key=lambda x: x["name"])

    def _extract_property_info(self, prop, uri: str, prop_type: str) -> Dict[str, str]:
        """Extract information about a property.

        Args:
            prop: RDFLib property node
            uri: Property URI
            prop_type: Type of property (Property, ObjectProperty, DatatypeProperty)

        Returns:
            Dict with property information
        """
        name = self._extract_local_name(uri)

        # Get label
        label = None
        for lbl in self.graph.objects(prop, RDFS.label):
            lbl_str = str(lbl)
            if not hasattr(lbl, "language") or lbl.language in [None, "en"]:
                label = lbl_str
                break
            if label is None:
                label = lbl_str

        # Get comment
        comment = None
        for cmt in self.graph.objects(prop, RDFS.comment):
            cmt_str = str(cmt)
            if not hasattr(cmt, "language") or cmt.language in [None, "en"]:
                comment = cmt_str
                break
            if comment is None:
                comment = cmt_str

        # Get domain
        domain = None
        for dom in self.graph.objects(prop, RDFS.domain):
            dom_uri = str(dom)
            if not dom_uri.startswith("_:"):
                domain = self._extract_local_name(dom_uri)
                break

        # Get range
        range_val = None
        for rng in self.graph.objects(prop, RDFS.range):
            rng_uri = str(rng)
            if not rng_uri.startswith("_:"):
                range_val = self._extract_local_name(rng_uri)
                break

        # Determine if it's a datatype property or object property
        # XSD datatypes and common literal types indicate a DatatypeProperty
        xsd_types = {
            "string",
            "integer",
            "int",
            "float",
            "double",
            "decimal",
            "boolean",
            "bool",
            "date",
            "dateTime",
            "time",
            "duration",
            "gYear",
            "gMonth",
            "gDay",
            "hexBinary",
            "base64Binary",
            "anyURI",
            "QName",
            "NOTATION",
            "normalizedString",
            "token",
            "language",
            "NMTOKEN",
            "Name",
            "NCName",
            "nonPositiveInteger",
            "negativeInteger",
            "long",
            "short",
            "byte",
            "nonNegativeInteger",
            "unsignedLong",
            "unsignedInt",
            "unsignedShort",
            "unsignedByte",
            "positiveInteger",
            "Literal",
            "langString",
        }

        is_datatype_property = False
        if range_val:
            # Check for XSD namespace indicators
            if range_val.startswith("xsd:") or "XMLSchema" in str(range_val):
                is_datatype_property = True
            # Check for common XSD type names
            elif range_val in xsd_types:
                is_datatype_property = True
            # Check for RDFS Literal
            elif range_val in ["Literal", "langString"] or "Literal" in range_val:
                is_datatype_property = True

        # Determine final type
        if prop_type == "Property":
            if is_datatype_property:
                prop_type = "DatatypeProperty"
            elif domain and range_val:
                # Has both domain and range, and range is not a datatype -> ObjectProperty
                prop_type = "ObjectProperty"
            else:
                # Default to DatatypeProperty if unclear
                prop_type = "DatatypeProperty"

        return {
            "uri": uri,
            "name": name,
            "label": label or name,
            "description": comment or "",
            "type": prop_type,
            "domain": domain or "",
            "range": range_val or "",
        }

    def get_ontology_info(self) -> Dict[str, str]:
        """Get basic vocabulary information.

        Returns:
            Dict with 'uri', 'label', 'comment', 'namespace'
        """
        # Try to find ontology declaration (some RDFS files have this)
        try:
            from rdflib import OWL

            for onto in self.graph.subjects(RDF.type, OWL.Ontology):
                return self._extract_ontology_info(onto)
        except ImportError:
            pass

        # Try to find a named graph or use base URI
        base_uri = self._get_base_uri()

        # Look for a resource that might be the vocabulary itself
        # (some vocabularies declare themselves)
        for subj in self.graph.subjects():
            uri = str(subj)
            if uri.endswith("#") or uri.endswith("/"):
                # This might be the vocabulary URI
                label = None
                for lbl in self.graph.objects(subj, RDFS.label):
                    label = str(lbl)
                    break

                comment = None
                for cmt in self.graph.objects(subj, RDFS.comment):
                    comment = str(cmt)
                    break

                if label or comment:
                    return {
                        "uri": uri,
                        "label": label or self._extract_local_name(uri) or "Vocabulary",
                        "comment": comment or "",
                        "namespace": uri,
                    }

        # Default fallback
        return {
            "uri": base_uri.rstrip("#/"),
            "label": "Imported Vocabulary",
            "comment": "",
            "namespace": base_uri,
        }

    def _extract_ontology_info(self, onto) -> Dict[str, str]:
        """Extract ontology information from an OWL ontology declaration.

        Args:
            onto: RDFLib ontology node

        Returns:
            Dict with ontology information
        """
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

        # Determine namespace
        namespace = uri
        if not namespace.endswith("#") and not namespace.endswith("/"):
            namespace = namespace + "#"

        return {
            "uri": uri,
            "label": label or self._extract_local_name(uri) or "Ontology",
            "comment": comment or "",
            "namespace": namespace,
        }
