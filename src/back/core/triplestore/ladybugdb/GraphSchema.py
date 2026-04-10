"""LadybugDB graph schema instance: node/rel tables and triple classification."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from back.core.helpers import extract_local_name, safe_identifier as _helpers_safe_id
from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL
from back.core.triplestore.ladybugdb.models import NodeTableDef, RelTableDef

logger = get_logger(__name__)


class GraphSchema:
    """Holds the generated graph schema: node tables, relationship tables,
    and helper mappings for triple insertion.
    """

    def __init__(self) -> None:
        self.node_tables: Dict[str, NodeTableDef] = {}
        self.rel_tables: Dict[str, RelTableDef] = {}
        self.class_uri_to_table: Dict[str, str] = {}
        self.property_uri_to_table: Dict[str, str] = {}
        self.fallback_node_table: str = "Resource"

    @staticmethod
    def safe_identifier(name: str) -> str:
        """Convert an ontology name to a valid LadybugDB identifier."""
        if not name:
            return "Unknown"
        result = _helpers_safe_id(name, prefix="N")
        return result or "Unknown"

    @staticmethod
    def extract_local_name(uri: str) -> str:
        """Extract the local name from a URI (after ``#`` or last ``/``)."""
        return extract_local_name(uri)

    def get_node_table(self, class_uri: str) -> Optional[str]:
        return self.class_uri_to_table.get(class_uri)

    def get_rel_table(self, property_uri: str) -> Optional[str]:
        return self.property_uri_to_table.get(property_uri)

    def resolve_data_property(
        self, subject_table: str, predicate_uri: str
    ) -> Optional[str]:
        """Map a data-property predicate URI to a column name on *subject_table*."""
        node_def = self.node_tables.get(subject_table)
        if not node_def:
            return None
        for col, uri in node_def.property_uris.items():
            if uri == predicate_uri:
                return col
        local = self.extract_local_name(predicate_uri)
        safe = self.safe_identifier(local)
        if safe in [self.safe_identifier(p) for p in node_def.properties]:
            return safe
        return None

    @classmethod
    def from_ontology(
        cls,
        classes: List[Dict[str, Any]],
        properties: List[Dict[str, Any]],
        relationships: Optional[List[Dict[str, Any]]] = None,
        base_uri: str = "",
    ) -> GraphSchema:
        """Build a ``GraphSchema`` from an OntoBricks ontology."""
        schema = cls()

        name_to_uri: Dict[str, str] = {}
        for c in classes:
            uri = c.get("uri", "")
            local = c.get("localName") or c.get("name") or cls.extract_local_name(uri)
            if uri:
                name_to_uri[local.lower()] = uri

        dp_uri_lookup: Dict[str, str] = {}
        for prop in properties:
            ptype = prop.get("type", "")
            if ptype in ("DatatypeProperty", "owl:DatatypeProperty"):
                p_uri = prop.get("uri", "")
                p_name = prop.get("name") or prop.get("localName") or cls.extract_local_name(p_uri)
                if p_name and p_uri:
                    dp_uri_lookup[p_name.lower()] = p_uri

        norm_base = (base_uri.rstrip("/").rstrip("#") + "/") if base_uri else ""

        for c in classes:
            uri = c.get("uri", "")
            if not uri:
                continue
            local = c.get("localName") or c.get("name") or cls.extract_local_name(uri)
            safe = cls.safe_identifier(local)

            data_props = c.get("dataProperties", [])
            prop_names: List[str] = []
            prop_uris: Dict[str, str] = {}
            for dp in data_props:
                dp_name = dp.get("name") or dp.get("localName", "")
                if not dp_name:
                    continue
                prop_names.append(dp_name)
                col = cls.safe_identifier(dp_name)
                dp_uri = dp.get("uri", "") or dp_uri_lookup.get(dp_name.lower(), "")
                if not dp_uri and norm_base:
                    dp_uri = f"{norm_base}{dp_name}"
                if dp_uri:
                    prop_uris[col] = dp_uri

            node_def = NodeTableDef(
                name=safe, class_uri=uri, properties=prop_names,
                property_uris=prop_uris,
            )
            schema.node_tables[safe] = node_def
            schema.class_uri_to_table[uri] = safe

        resource_def = NodeTableDef(
            name=schema.fallback_node_table,
            class_uri="",
            properties=[],
        )
        schema.node_tables[schema.fallback_node_table] = resource_def

        for prop in properties:
            prop_type = prop.get("type", "")
            if prop_type not in ("ObjectProperty", "owl:ObjectProperty"):
                continue
            prop_uri = prop.get("uri", "")
            if not prop_uri:
                continue
            prop_name = prop.get("name") or prop.get("localName") or cls.extract_local_name(prop_uri)
            safe_name = cls.safe_identifier(prop_name)

            domain_name = (prop.get("domain") or "").strip()
            range_name = (prop.get("range") or "").strip()

            from_table = cls._resolve_table_name(domain_name, name_to_uri, schema, base_uri)
            to_table = cls._resolve_table_name(range_name, name_to_uri, schema, base_uri)

            rel_def = RelTableDef(
                name=safe_name,
                property_uri=prop_uri,
                from_table=from_table,
                to_table=to_table,
            )
            schema.rel_tables[safe_name] = rel_def
            schema.property_uri_to_table[prop_uri] = safe_name
            if norm_base and prop_uri and not prop_uri.startswith(norm_base):
                local = prop_uri.rsplit("#", 1)[-1] if "#" in prop_uri else prop_uri.rsplit("/", 1)[-1]
                schema.property_uri_to_table[norm_base + local] = safe_name

        logger.info(
            "Generated graph schema: %d node tables, %d relationship tables",
            len(schema.node_tables),
            len(schema.rel_tables),
        )
        return schema

    @staticmethod
    def _resolve_table_name(
        name: str,
        name_to_uri: Dict[str, str],
        schema: GraphSchema,
        base_uri: str,
    ) -> str:
        """Resolve a domain/range name to a node table name."""
        if not name:
            return schema.fallback_node_table
        uri = name_to_uri.get(name.lower())
        if uri:
            table = schema.class_uri_to_table.get(uri)
            if table:
                return table
        safe = GraphSchema.safe_identifier(name)
        if safe in schema.node_tables:
            return safe
        return schema.fallback_node_table

    def generate_ddl(self) -> List[str]:
        """Return a list of Cypher DDL statements to create the graph schema."""
        stmts: List[str] = []
        for node_def in self.node_tables.values():
            stmts.append(node_def.to_cypher())
        for rel_def in self.rel_tables.values():
            stmts.append(rel_def.to_cypher())
        return stmts

    def classify_triples(
        self,
        triples: List[Dict[str, str]],
    ) -> Tuple[
        Dict[str, List[Dict[str, Any]]],
        List[Dict[str, Any]],
        Dict[str, List[Dict[str, str]]],
    ]:
        """Classify triples into node inserts, relationship inserts, and
        datatype-property updates.
        """
        subject_types: Dict[str, str] = {}
        subject_labels: Dict[str, str] = {}
        subject_attrs: Dict[str, List[Dict[str, str]]] = {}
        rel_triples: List[Dict[str, str]] = []

        for t in triples:
            s, p, o = t.get("subject", ""), t.get("predicate", ""), t.get("object", "")
            if p == RDF_TYPE:
                subject_types[s] = o
            elif p == RDFS_LABEL:
                subject_labels[s] = o
            elif self.get_rel_table(p):
                rel_triples.append(t)
            else:
                subject_attrs.setdefault(s, []).append({"predicate": p, "value": o})

        node_inserts: Dict[str, List[Dict[str, Any]]] = {}
        for subj, type_uri in subject_types.items():
            table = self.get_node_table(type_uri) or self.fallback_node_table
            entry: Dict[str, Any] = {"uri": subj, "label": subject_labels.get(subj, "")}
            node_inserts.setdefault(table, []).append(entry)

        for subj in set(subject_labels.keys()) - set(subject_types.keys()):
            entry_fb: Dict[str, Any] = {"uri": subj, "label": subject_labels[subj]}
            node_inserts.setdefault(self.fallback_node_table, []).append(entry_fb)

        rel_inserts: List[Dict[str, Any]] = []
        for t in rel_triples:
            rel_table = self.get_rel_table(t["predicate"])
            if rel_table:
                rel_inserts.append({
                    "rel_table": rel_table,
                    "from_uri": t["subject"],
                    "to_uri": t["object"],
                })

        return node_inserts, rel_inserts, subject_attrs
