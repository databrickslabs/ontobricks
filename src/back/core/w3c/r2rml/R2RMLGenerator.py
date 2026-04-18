"""R2RML mapping generator."""

from typing import Dict, Any
from rdflib import Graph, Namespace, Literal, URIRef, BNode
from rdflib.namespace import RDF, RDFS, XSD
import re

from back.core.logging import get_logger
from shared.config.constants import DEFAULT_BASE_URI

logger = get_logger(__name__)


class R2RMLGenerator:
    """Generate R2RML mappings from configuration."""

    def __init__(self, base_uri: str = DEFAULT_BASE_URI):
        """Initialize the R2RML generator."""
        self.base_uri = base_uri.rstrip("/").rstrip("#") + "/"
        self.rr = Namespace("http://www.w3.org/ns/r2rml#")
        self.ont = Namespace(self.base_uri)

    def generate_mapping(
        self, mapping_config: Dict[str, Any], ontology_config: Dict[str, Any] = None
    ) -> str:
        """Generate R2RML mapping from configuration.

        Args:
            mapping_config: Dictionary containing:
                - entities: List of entity mappings (with sql_query and attribute_mappings)
                - relationships: List of relationship mappings with SQL queries
            ontology_config: Optional dictionary containing:
                - properties: List of ontology properties with 'uri', 'domain', 'range'
                - classes: List of ontology classes with 'uri', 'label', 'name'

        Returns:
            R2RML mapping as Turtle string
        """
        g = Graph()
        g.bind("rr", self.rr)
        g.bind("ont", self.ont)
        g.bind("xsd", XSD)
        g.bind("rdfs", RDFS)

        # Filter out excluded items (excluded flag lives on each mapping entry)
        all_dsm = mapping_config.get(
            "entities", mapping_config.get("data_source_mappings", [])
        )
        excluded_class_uris = {
            m.get("ontology_class") for m in all_dsm if m.get("excluded")
        }

        excluded_class_names = set()
        if ontology_config:
            for c in ontology_config.get("classes", []):
                if c.get("uri") in excluded_class_uris:
                    if c.get("name"):
                        excluded_class_names.add(c["name"])
                    if c.get("localName"):
                        excluded_class_names.add(c["localName"])

        all_rm = mapping_config.get(
            "relationships", mapping_config.get("relationship_mappings", [])
        )
        excluded_prop_uris = {m.get("property") for m in all_rm if m.get("excluded")}
        if ontology_config:
            for p in ontology_config.get("properties", []):
                if (
                    p.get("domain") in excluded_class_names
                    or p.get("range") in excluded_class_names
                ):
                    if p.get("uri"):
                        excluded_prop_uris.add(p["uri"])

        data_source_mappings = [m for m in all_dsm if not m.get("excluded")]
        relationship_mappings = [
            m
            for m in all_rm
            if not m.get("excluded")
            and m.get("property") not in excluded_prop_uris
            and m.get("source_class") not in excluded_class_uris
            and m.get("target_class") not in excluded_class_uris
        ]

        # Create a lookup for entity mappings by class label, URI, and local name
        entity_lookup = {}
        for m in data_source_mappings:
            # Support both old (table-based) and new (SQL-based) mappings
            table = m.get("table") or m.get("table_name")
            class_label = m.get("ontology_class_label")
            class_uri = m.get("ontology_class")

            if table:
                entity_lookup[table] = m
            if class_label:
                entity_lookup[class_label] = m
                entity_lookup[class_label.lower()] = m  # case-insensitive
            if class_uri:
                entity_lookup[class_uri] = m
                # Also store by local name extracted from URI
                local_name = self._extract_local_name(class_uri)
                if local_name:
                    entity_lookup[local_name] = m
                    entity_lookup[local_name.lower()] = m

        # Build property lookup from ontology: predicate URI -> {domain, range}
        # This allows resolving source/target entity names from the ontology
        # when the relationship config doesn't have class info
        property_lookup = self._build_property_lookup(ontology_config)

        # Build data-property URI lookup so attribute predicates use the
        # exact URIs defined in the ontology (preserving # vs / separator).
        data_prop_uri_lookup = self._build_data_property_uri_lookup(ontology_config)

        # Generate class/entity mappings (TriplesMap for each mapped entity)
        for idx, entity_mapping in enumerate(data_source_mappings):
            self._add_entity_mapping(g, entity_mapping, idx, data_prop_uri_lookup)

        # Generate relationship mappings using SQL queries
        for idx, rel_mapping in enumerate(relationship_mappings):
            self._add_relationship_mapping(
                g, rel_mapping, entity_lookup, property_lookup, idx
            )

        return g.serialize(format="turtle")

    def _build_property_lookup(
        self, ontology_config: Dict[str, Any] = None
    ) -> Dict[str, Dict[str, str]]:
        """Build a lookup from ontology property URI to its domain/range names.

        Returns:
            Dict mapping property URI -> {'domain': str, 'range': str}
        """
        lookup = {}
        if not ontology_config:
            return lookup

        properties = ontology_config.get("properties", [])
        for prop in properties:
            prop_uri = prop.get("uri", "")
            prop_label = prop.get("label", "") or prop.get("name", "")
            domain = prop.get("domain", "")  # e.g. "Customer"
            range_val = prop.get("range", "")  # e.g. "Contract"

            info = {"domain": domain, "range": range_val}
            if prop_uri:
                lookup[prop_uri] = info
            if prop_label:
                lookup[prop_label] = info

        return lookup

    def _build_data_property_uri_lookup(
        self, ontology_config: Dict[str, Any] = None
    ) -> Dict[str, Dict[str, str]]:
        """Build a lookup: class_uri → {attr_name_lower: property_uri}.

        Used by ``_add_entity_mapping`` so that attribute predicate URIs in
        the generated R2RML match the ontology (preserving ``#`` vs ``/``).
        """
        lookup: Dict[str, Dict[str, str]] = {}
        if not ontology_config:
            return lookup
        for cls in ontology_config.get("classes", []):
            cls_uri = cls.get("uri", "")
            if not cls_uri:
                continue
            props_map: Dict[str, str] = {}
            for key in ("dataProperties", "properties", "attributes"):
                items = cls.get(key, [])
                if not isinstance(items, list) or not items:
                    continue
                for item in items:
                    if isinstance(item, dict):
                        name = item.get("name", "") or item.get("localName", "")
                        uri = item.get("uri", "")
                    elif isinstance(item, str):
                        name = item
                        uri = ""
                    else:
                        continue
                    if name and uri:
                        props_map[name.lower()] = uri
                if props_map:
                    break
            if props_map:
                lookup[cls_uri] = props_map
        return lookup

    def _add_entity_mapping(
        self,
        g: Graph,
        config: Dict[str, Any],
        idx: int,
        data_prop_uri_lookup: Dict[str, Dict[str, str]] = None,
    ):
        """Add an entity/class mapping to the graph.

        Expected config (new SQL-based format):
            - sql_query: str (SQL query)
            - ontology_class: str (URI)
            - ontology_class_label: str
            - id_column: str
            - label_column: str (optional)
            - attribute_mappings: Dict[str, str] (attribute_name -> column_name)

        Also supports legacy format:
            - catalog: str
            - schema: str
            - table: str (or table_name)
        """
        sql_query = config.get("sql_query", "").strip()
        catalog = config.get("catalog", "")
        schema = config.get("schema", "")
        table = config.get("table") or config.get("table_name", "")
        class_uri = config.get("ontology_class", "")
        class_label = config.get("ontology_class_label", "")
        id_column = config.get("id_column", "")
        label_column = config.get("label_column", "")
        attribute_mappings = config.get("attribute_mappings", {})

        if not id_column:
            return

        # Need either SQL query or table name
        if not sql_query and not table:
            return

        # Create unique identifier for this TriplesMap
        map_name = self._sanitize_name(class_label or table or f"Entity_{idx}")

        # Create TriplesMap
        triples_map = URIRef(f"{self.base_uri}TriplesMap_{map_name}")
        g.add((triples_map, RDF.type, self.rr.TriplesMap))

        # Add comment for clarity
        comment = f"Mapping for {class_label or table} to {class_uri}"
        g.add((triples_map, RDFS.comment, Literal(comment)))

        # Logical Table - using SQL query or table name
        logical_table = BNode()
        g.add((triples_map, self.rr.logicalTable, logical_table))

        if sql_query:
            # New SQL-based mapping
            g.add((logical_table, self.rr.sqlQuery, Literal(sql_query)))
        else:
            # Legacy table-based mapping
            g.add(
                (
                    logical_table,
                    self.rr.tableName,
                    Literal(f"{catalog}.{schema}.{table}"),
                )
            )

        # Subject Map
        subject_map = BNode()
        g.add((triples_map, self.rr.subjectMap, subject_map))

        # Template for subject URI - ALWAYS use taxonomy base URI
        class_name = (
            self._extract_local_name(class_uri) if class_uri else (class_label or table)
        )
        class_name = self._sanitize_name(class_name)

        g.add(
            (
                subject_map,
                self.rr.template,
                Literal(f"{self.base_uri}{class_name}/{{{id_column}}}"),
            )
        )

        # Add class if specified
        if class_uri:
            if class_uri.startswith("http://") or class_uri.startswith("https://"):
                g.add((subject_map, self.rr["class"], URIRef(class_uri)))
            else:
                g.add(
                    (
                        subject_map,
                        self.rr["class"],
                        URIRef(f"{self.base_uri}{class_uri}"),
                    )
                )

        # Add label column mapping if specified
        if label_column:
            pom = BNode()
            g.add((triples_map, self.rr.predicateObjectMap, pom))
            g.add((pom, self.rr.predicate, RDFS.label))

            obj_map = BNode()
            g.add((pom, self.rr.objectMap, obj_map))
            g.add((obj_map, self.rr.column, Literal(label_column)))

        # Ontology property-URI lookup for this class
        ont_props = (data_prop_uri_lookup or {}).get(class_uri, {})

        # Add attribute mappings (DatatypeProperty mappings)
        if attribute_mappings:
            for attr_name, column_name in attribute_mappings.items():
                if not column_name:
                    continue

                pom = BNode()
                g.add((triples_map, self.rr.predicateObjectMap, pom))

                # Prefer the ontology property URI when it matches the
                # current base_uri so that separator (# vs /) is
                # preserved.  When it belongs to a *different* base
                # (stale URI after a base-URI change), rebuild from
                # the current base to avoid mixed-prefix predicates.
                ont_uri = ont_props.get(attr_name.lower())
                if ont_uri and ont_uri.startswith(self.base_uri):
                    attr_uri = URIRef(ont_uri)
                else:
                    attr_uri = URIRef(
                        f"{self.base_uri}{self._sanitize_name(attr_name)}"
                    )
                g.add((pom, self.rr.predicate, attr_uri))

                obj_map = BNode()
                g.add((pom, self.rr.objectMap, obj_map))
                g.add((obj_map, self.rr.column, Literal(column_name)))
                g.add((obj_map, self.rr.datatype, XSD.string))  # Default to string

    def _add_relationship_mapping(
        self,
        g: Graph,
        config: Dict[str, Any],
        entity_lookup: Dict[str, Any],
        property_lookup: Dict[str, Dict[str, str]],
        idx: int,
    ):
        """Add a relationship mapping using SQL query.

        Expected config:
            - property: str (URI)
            - property_label: str
            - source_table: str
            - target_table: str
            - source_id_column: str (from SQL query result)
            - target_id_column: str (from SQL query result)
            - sql_query: str
            - attribute_mappings: Dict[str, str] (attribute_name -> column_name)
            - direction: str ('forward' or 'reverse') - determines triple direction
        """
        property_uri = config.get("property", "")
        property_label = config.get("property_label", config.get("property_name", ""))
        source_table = config.get("source_table", "")
        target_table = config.get("target_table", "")
        source_column = config.get("source_column") or config.get(
            "source_id_column", ""
        )
        target_column = config.get("target_column") or config.get(
            "target_id_column", ""
        )
        sql_query = config.get("sql_query", "").strip()
        source_class_label = config.get("source_class_label", "")
        target_class_label = config.get("target_class_label", "")
        # Also get class URIs (from auto-assign)
        source_class_uri = config.get("source_class", "")
        target_class_uri = config.get("target_class", "")
        attribute_mappings = config.get("attribute_mappings", {})
        direction = config.get("direction", "forward")  # Get relationship direction

        if not property_uri:
            return

        # Enrich missing class labels from ontology property domain/range
        # This is the key fallback: when the relationship config has no class info,
        # we look up the property's domain/range from the ontology
        if (
            not source_class_label
            or not target_class_label
            or not source_class_uri
            or not target_class_uri
        ):
            prop_info = (
                property_lookup.get(property_uri)
                or property_lookup.get(property_label)
                or {}
            )
            ont_domain = prop_info.get("domain", "")  # e.g. "Customer"
            ont_range = prop_info.get("range", "")  # e.g. "Contract"

            if not source_class_label:
                source_class_label = ont_domain
            if not target_class_label:
                target_class_label = ont_range
            # Also fill missing class URIs with ontology domain/range names
            # so that entity_lookup can find them by local name
            if not source_class_uri and ont_domain:
                source_class_uri = ont_domain
            if not target_class_uri and ont_range:
                target_class_uri = ont_range

        # If direction is 'reverse', swap ALL source/target info before any lookups
        if direction == "reverse":
            source_table, target_table = target_table, source_table
            source_column, target_column = target_column, source_column
            source_class_label, target_class_label = (
                target_class_label,
                source_class_label,
            )
            source_class_uri, target_class_uri = target_class_uri, source_class_uri

        # Get entity info for source and target
        # Try multiple lookup keys: table name, class label, class URI, and local name from URI
        source_entity = self._lookup_entity(
            entity_lookup, source_table, source_class_label, source_class_uri
        )
        target_entity = self._lookup_entity(
            entity_lookup, target_table, target_class_label, target_class_uri
        )

        # Get ontology class URIs - prefer entity lookup, fallback to config values
        source_class = source_entity.get("ontology_class", "") or source_class_uri
        target_class = target_entity.get("ontology_class", "") or target_class_uri
        source_id = source_entity.get("id_column", source_column)
        target_id = target_entity.get("id_column", target_column)

        # Create unique identifier for this TriplesMap
        prop_name = self._sanitize_name(
            property_label or self._extract_local_name(property_uri)
        )
        map_name = f"Rel_{prop_name}_{idx}"

        # Create TriplesMap for the relationship
        triples_map = URIRef(f"{self.base_uri}TriplesMap_{map_name}")
        g.add((triples_map, RDF.type, self.rr.TriplesMap))

        # Add comment for clarity
        source_label = source_class_label or source_table or "source"
        target_label = target_class_label or target_table or "target"
        g.add(
            (
                triples_map,
                RDFS.comment,
                Literal(
                    f"Relationship mapping: {source_label} --{property_label}--> {target_label}"
                ),
            )
        )

        # Logical Table - using SQL query if provided
        logical_table = BNode()
        g.add((triples_map, self.rr.logicalTable, logical_table))

        if sql_query:
            g.add((logical_table, self.rr.sqlQuery, Literal(sql_query)))
        else:
            # Fallback to source entity's SQL or table if no SQL query
            source_sql = source_entity.get("sql_query", "").strip()
            if source_sql:
                g.add((logical_table, self.rr.sqlQuery, Literal(source_sql)))
            else:
                catalog = source_entity.get("catalog", "")
                schema = source_entity.get("schema", "")
                if catalog and schema and source_table:
                    g.add(
                        (
                            logical_table,
                            self.rr.tableName,
                            Literal(f"{catalog}.{schema}.{source_table}"),
                        )
                    )

        # Subject Map - references the source entity
        subject_map = BNode()
        g.add((triples_map, self.rr.subjectMap, subject_map))

        source_class_name = self._resolve_class_name(
            source_class,
            source_class_uri,
            source_class_label,
            source_entity,
            source_table,
        )

        # ALWAYS use taxonomy base URI for subject template
        g.add(
            (
                subject_map,
                self.rr.template,
                Literal(f"{self.base_uri}{source_class_name}/{{{source_column}}}"),
            )
        )

        # Predicate Object Map
        pom = BNode()
        g.add((triples_map, self.rr.predicateObjectMap, pom))

        # Predicate Map - the relationship property.
        # When the stored URI belongs to a different base (stale after a
        # base-URI change), rebuild it from the current base_uri.
        if property_uri.startswith("http://") or property_uri.startswith("https://"):
            if property_uri.startswith(self.base_uri):
                g.add((pom, self.rr.predicate, URIRef(property_uri)))
            else:
                local = self._extract_local_name(property_uri)
                g.add(
                    (
                        pom,
                        self.rr.predicate,
                        URIRef(f"{self.base_uri}{self._sanitize_name(local)}"),
                    )
                )
        else:
            g.add((pom, self.rr.predicate, URIRef(f"{self.base_uri}{property_uri}")))

        # Object Map - reference to target entity
        obj_map = BNode()
        g.add((pom, self.rr.objectMap, obj_map))

        target_class_name = self._resolve_class_name(
            target_class,
            target_class_uri,
            target_class_label,
            target_entity,
            target_table,
        )

        # ALWAYS use taxonomy base URI for object template
        g.add(
            (
                obj_map,
                self.rr.template,
                Literal(f"{self.base_uri}{target_class_name}/{{{target_column}}}"),
            )
        )

        # Note: Relationship attributes are not supported - relationships are simple

    def _lookup_entity(
        self, entity_lookup: Dict, table: str, class_label: str, class_uri: str
    ) -> Dict:
        """Look up an entity mapping using multiple keys.

        Tries in order: class URI (exact), local name from URI, class label
        (case-sensitive and insensitive), table name.
        """
        result = None
        # Try class URI first (most specific)
        if class_uri:
            result = entity_lookup.get(class_uri)
            if not result:
                local_name = self._extract_local_name(class_uri)
                if local_name:
                    result = entity_lookup.get(local_name) or entity_lookup.get(
                        local_name.lower()
                    )
        # Try class label
        if not result and class_label:
            result = entity_lookup.get(class_label) or entity_lookup.get(
                class_label.lower()
            )
        # Try table name
        if not result and table:
            result = entity_lookup.get(table)
        return result or {}

    def _resolve_class_name(
        self,
        class_from_entity: str,
        class_uri: str,
        class_label: str,
        entity: Dict,
        table: str,
    ) -> str:
        """Resolve the actual entity class name for a URI template.

        Returns the real entity name (e.g. 'Customer', 'Contract') by trying
        multiple sources. Never returns generic placeholders like 'Source' or 'Target'.

        Priority:
        1. Entity's ontology_class_label (from matched entity mapping — most reliable)
        2. Extract local name from resolved class URI (entity lookup or config)
        3. Class label from relationship config
        4. Extract local name from class_uri directly
        5. Table name
        """
        # Priority 1: Entity's own label (e.g. "Customer") — most reliable
        entity_label = entity.get("ontology_class_label", "")
        if entity_label:
            return self._sanitize_name(entity_label)

        # Priority 2: Extract from resolved class URI (from entity lookup + config fallback)
        if class_from_entity:
            name = self._extract_local_name(class_from_entity)
            if name:
                return self._sanitize_name(name)

        # Priority 3: Class label from relationship config
        if class_label:
            # If class_label is actually a URI, extract the local name from it
            if class_label.startswith("http://") or class_label.startswith("https://"):
                name = self._extract_local_name(class_label)
                if name:
                    return self._sanitize_name(name)
            else:
                return self._sanitize_name(class_label)

        # Priority 4: Extract local name from the class URI directly
        if class_uri:
            name = self._extract_local_name(class_uri)
            if name:
                return self._sanitize_name(name)

        # Priority 5: Table name
        if table:
            return self._sanitize_name(table)

        # Should not reach here — log a warning
        logger.warning(
            "Could not resolve entity class name. class_uri='%s', class_label='%s', table='%s'",
            class_uri,
            class_label,
            table,
        )
        return "UnknownEntity"

    def _sanitize_name(self, name: str) -> str:
        """Sanitize a name for use in URIs."""
        if name is None:
            return "unknown"
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", str(name))
        return sanitized or "unknown"

    def _extract_local_name(self, uri: str) -> str:
        """Extract the local name from a URI."""
        from back.core.helpers import extract_local_name

        return extract_local_name(uri) or str(uri or "").strip()

    @staticmethod
    def generate_r2rml_from_config(
        mapping_config: Dict[str, Any],
        ontology_config: Dict[str, Any] = None,
    ) -> str:
        """Generate R2RML mapping from mapping configuration and ontology.

        Args:
            mapping_config: Mapping configuration with entities and relationships
            ontology_config: Ontology with base_uri, classes, and properties (info.uri)

        Returns:
            R2RML mapping in Turtle format
        """
        base_uri = None

        if ontology_config:
            if ontology_config.get("info"):
                base_uri = ontology_config["info"].get("uri")
            if not base_uri:
                base_uri = ontology_config.get("base_uri")

        if not base_uri or "example.org" in base_uri:
            base_uri = DEFAULT_BASE_URI

        generator = R2RMLGenerator(base_uri)
        return generator.generate_mapping(mapping_config, ontology_config)
