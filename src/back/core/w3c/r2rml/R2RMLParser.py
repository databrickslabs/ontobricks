"""R2RML parsing service for extracting mappings from R2RML content."""

import re
from rdflib import Graph, Namespace
from rdflib.namespace import RDF

from back.core.errors import ValidationError


class R2RMLParser:
    """Parser for R2RML mapping files."""

    def __init__(self, content):
        """Initialize parser with R2RML content.

        Args:
            content: R2RML content in Turtle or XML format
        """
        self.graph = Graph()
        self._parse_content(content)
        self.RR = Namespace("http://www.w3.org/ns/r2rml#")

    def _parse_content(self, content):
        """Parse the R2RML content."""
        from back.core.w3c.rdf_utils import parse_rdf_flexible

        try:
            self.graph = parse_rdf_flexible(content, formats=("turtle", "xml"))
        except ValueError as e:
            raise ValidationError("Failed to parse R2RML content", detail=str(e)) from e

    def extract_mappings(self):
        """Extract entity and relationship mappings from R2RML.

        Returns:
            tuple: (entity_mappings list, relationship_mappings list)
        """
        entity_mappings = []
        relationship_mappings = []

        for triples_map in self.graph.subjects(RDF.type, self.RR.TriplesMap):
            mapping_info = self._extract_triples_map(triples_map)

            if mapping_info["has_class"] and mapping_info["entity"] is not None:
                entity_mappings.append(mapping_info["entity"])

            relationship_mappings.extend(mapping_info["relationships"])

        return entity_mappings, relationship_mappings

    def _extract_triples_map(self, triples_map):
        """Extract info from a single TriplesMap."""
        info = {"has_class": False, "entity": None, "relationships": []}

        # Get logical table info
        table_name = None
        sql_query = None

        for logical_table in self.graph.objects(triples_map, self.RR.logicalTable):
            for tn in self.graph.objects(logical_table, self.RR.tableName):
                table_name = str(tn)
            for sq in self.graph.objects(logical_table, self.RR.sqlQuery):
                sql_query = str(sq)

        # Get subject map info
        subject_template = None
        subject_class = None
        id_column = None

        for subject_map in self.graph.objects(triples_map, self.RR.subjectMap):
            for template in self.graph.objects(subject_map, self.RR.template):
                subject_template = str(template)
                match = re.search(r"\{([^}]+)\}", subject_template)
                if match:
                    id_column = self._unquote_column(match.group(1))

            for cls in self.graph.objects(subject_map, self.RR["class"]):
                subject_class = str(cls)
                info["has_class"] = True

        # Parse table name parts
        catalog, schema, table = self._parse_table_name(table_name)

        # Get class name from URI
        class_name = None
        if subject_class:
            class_name = (
                subject_class.split("#")[-1]
                if "#" in subject_class
                else subject_class.split("/")[-1]
            )

        # Extract source class name from subject template for relationship TriplesMaps
        # Template format: "{base_uri}{ClassName}/{id_col}" → capture ClassName
        source_class_name = None
        if subject_template:
            match = re.search(r"/([^/]+)/\{[^}]+\}$", subject_template)
            if match:
                source_class_name = match.group(1)

        # Process predicate-object maps
        label_column = None
        attribute_mappings = {}

        for pom in self.graph.objects(triples_map, self.RR.predicateObjectMap):
            predicate = None
            object_template = None
            object_column = None

            for pred in self.graph.objects(pom, self.RR.predicate):
                predicate = str(pred)

            for obj_map in self.graph.objects(pom, self.RR.objectMap):
                for template in self.graph.objects(obj_map, self.RR.template):
                    object_template = str(template)
                for col in self.graph.objects(obj_map, self.RR.column):
                    object_column = self._unquote_column(str(col))

            if not predicate:
                continue

            if predicate.endswith("label"):
                label_column = object_column
            elif object_template:
                # Relationship: objectMap uses a template pointing to another entity
                rel_mapping = self._extract_relationship(
                    predicate, object_template, id_column, sql_query, source_class_name
                )
                if rel_mapping:
                    info["relationships"].append(rel_mapping)
            elif object_column:
                # Attribute: objectMap uses a plain column reference
                attr_name = (
                    predicate.split("#")[-1]
                    if "#" in predicate
                    else predicate.split("/")[-1]
                )
                attribute_mappings[attr_name] = object_column

        # Build entity mapping - works for both rr:tableName and rr:sqlQuery
        if info["has_class"] and (table or sql_query):
            info["entity"] = {
                "ontology_class": subject_class,
                "ontology_class_label": class_name,
                "catalog": catalog or "",
                "schema": schema or "",
                "table": table or "",
                "sql_query": sql_query or "",
                "id_column": id_column or "",
                "label_column": label_column or "",
                "attribute_mappings": attribute_mappings,
            }

        return info

    def _unquote_column(self, col: str) -> str:
        """Strip surrounding double-quotes from a column name.

        R2RMLGenerator always emits double-quoted column names. When parsing
        back, we strip the quotes so callers receive the bare name.
        """
        if col and col.startswith('"') and col.endswith('"') and len(col) > 1:
            return col[1:-1].replace('""', '"')
        return col

    def _parse_table_name(self, table_name):
        """Parse table name into catalog, schema, table."""
        if not table_name:
            return None, None, None

        parts = table_name.split(".")
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        elif len(parts) == 2:
            return None, parts[0], parts[1]
        else:
            return None, None, table_name

    def _extract_relationship(
        self, predicate, object_template, source_id_column, sql_query, source_class_name=None
    ):
        """Extract relationship mapping from predicate-object map."""
        template_match = re.search(r"/([^/]+)/\{([^}]+)\}", object_template)
        if not template_match:
            return None

        target_class = template_match.group(1)
        target_id_column = self._unquote_column(template_match.group(2))
        prop_name = (
            predicate.split("#")[-1] if "#" in predicate else predicate.split("/")[-1]
        )

        return {
            "property": predicate,
            "property_label": prop_name,
            "property_name": prop_name,
            "source_class": source_class_name or "",
            "source_class_label": source_class_name or "",
            "target_class": target_class,
            "target_class_label": target_class,
            "source_table": source_class_name or "",
            "target_table": target_class,
            "source_id_column": source_id_column or "",
            "target_id_column": target_id_column,
            "sql_query": sql_query or "",
            "attribute_mappings": {},
            "direction": "forward",
        }

    @staticmethod
    def parse_r2rml_content(content):
        """Parse R2RML content and return entity and relationship mappings.

        Args:
            content: R2RML content string

        Returns:
            tuple: (entity_mappings, relationship_mappings)
        """
        parser = R2RMLParser(content)
        return parser.extract_mappings()
