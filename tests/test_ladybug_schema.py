"""Tests for the LadybugDB graph schema generation module."""
import pytest

from back.core.graphdb.ladybugdb import (
    GraphSchema,
    NodeTableDef,
    RelTableDef,
    _extract_local_name,
    _safe_identifier,
    classify_triples,
    generate_ddl,
    generate_graph_schema,
)


class TestSafeIdentifier:
    def test_simple(self):
        assert _safe_identifier("Customer") == "Customer"

    def test_special_chars(self):
        assert _safe_identifier("has-order!") == "has_order_"

    def test_starts_with_digit(self):
        assert _safe_identifier("3DModel") == "N3DModel"

    def test_empty(self):
        assert _safe_identifier("") == "Unknown"


class TestExtractLocalName:
    def test_hash_separator(self):
        assert _extract_local_name("http://example.org/ont#Customer") == "Customer"

    def test_slash_separator(self):
        assert _extract_local_name("http://example.org/ont/Customer") == "Customer"

    def test_no_separator(self):
        assert _extract_local_name("Customer") == "Customer"


class TestNodeTableDef:
    def test_to_cypher_basic(self):
        node = NodeTableDef("Customer", "http://ex.org/Customer", [])
        cypher = node.to_cypher()
        assert "CREATE NODE TABLE IF NOT EXISTS Customer" in cypher
        assert "uri STRING PRIMARY KEY" in cypher
        assert "label STRING" in cypher

    def test_to_cypher_with_properties(self):
        node = NodeTableDef("Customer", "http://ex.org/Customer", ["firstName", "age"])
        cypher = node.to_cypher()
        assert "firstName STRING" in cypher
        assert "age STRING" in cypher


class TestRelTableDef:
    def test_to_cypher(self):
        rel = RelTableDef("hasOrder", "http://ex.org/hasOrder", "Customer", "Order")
        cypher = rel.to_cypher()
        assert "CREATE REL TABLE IF NOT EXISTS hasOrder" in cypher
        assert "FROM Customer TO Order" in cypher


class TestGraphSchema:
    def test_get_node_table(self):
        schema = GraphSchema()
        schema.class_uri_to_table["http://ex.org/C"] = "C"
        assert schema.get_node_table("http://ex.org/C") == "C"
        assert schema.get_node_table("http://ex.org/Missing") is None

    def test_get_rel_table(self):
        schema = GraphSchema()
        schema.property_uri_to_table["http://ex.org/hasX"] = "hasX"
        assert schema.get_rel_table("http://ex.org/hasX") == "hasX"
        assert schema.get_rel_table("http://ex.org/Missing") is None

    def test_fallback_node_table(self):
        schema = GraphSchema()
        assert schema.fallback_node_table == "Resource"


CLASSES = [
    {
        "uri": "http://test.org/ont#Customer",
        "name": "Customer",
        "localName": "Customer",
        "dataProperties": [
            {"name": "firstName"},
            {"name": "lastName"},
        ],
    },
    {
        "uri": "http://test.org/ont#Order",
        "name": "Order",
        "localName": "Order",
        "dataProperties": [{"name": "orderDate"}],
    },
]

PROPERTIES = [
    {
        "uri": "http://test.org/ont#hasOrder",
        "name": "hasOrder",
        "type": "ObjectProperty",
        "domain": "Customer",
        "range": "Order",
    },
    {
        "uri": "http://test.org/ont#firstName",
        "name": "firstName",
        "type": "DatatypeProperty",
        "domain": "Customer",
        "range": "xsd:string",
    },
]


class TestGenerateGraphSchema:
    def test_node_tables_created(self):
        schema = generate_graph_schema(CLASSES, PROPERTIES)
        assert "Customer" in schema.node_tables
        assert "Order" in schema.node_tables
        assert schema.fallback_node_table in schema.node_tables

    def test_class_uri_mapping(self):
        schema = generate_graph_schema(CLASSES, PROPERTIES)
        assert schema.class_uri_to_table["http://test.org/ont#Customer"] == "Customer"
        assert schema.class_uri_to_table["http://test.org/ont#Order"] == "Order"

    def test_object_property_becomes_rel(self):
        schema = generate_graph_schema(CLASSES, PROPERTIES)
        assert "hasOrder" in schema.rel_tables
        rel = schema.rel_tables["hasOrder"]
        assert rel.from_table == "Customer"
        assert rel.to_table == "Order"
        assert rel.property_uri == "http://test.org/ont#hasOrder"

    def test_datatype_property_not_a_rel(self):
        schema = generate_graph_schema(CLASSES, PROPERTIES)
        assert "firstName" not in schema.rel_tables

    def test_unknown_domain_falls_back_to_resource(self):
        props = [
            {
                "uri": "http://test.org/ont#orphanRel",
                "name": "orphanRel",
                "type": "ObjectProperty",
                "domain": "UnknownClass",
                "range": "Order",
            },
        ]
        schema = generate_graph_schema(CLASSES, props)
        rel = schema.rel_tables["orphanRel"]
        assert rel.from_table == schema.fallback_node_table

    def test_empty_domain_falls_back_to_resource(self):
        props = [
            {
                "uri": "http://test.org/ont#noDomain",
                "name": "noDomain",
                "type": "ObjectProperty",
                "domain": "",
                "range": "",
            },
        ]
        schema = generate_graph_schema(CLASSES, props)
        rel = schema.rel_tables["noDomain"]
        assert rel.from_table == schema.fallback_node_table
        assert rel.to_table == schema.fallback_node_table

    def test_node_data_properties(self):
        schema = generate_graph_schema(CLASSES, PROPERTIES)
        customer = schema.node_tables["Customer"]
        assert "firstName" in customer.properties
        assert "lastName" in customer.properties

    def test_empty_classes(self):
        schema = generate_graph_schema([], [])
        assert schema.fallback_node_table in schema.node_tables
        assert len(schema.node_tables) == 1
        assert len(schema.rel_tables) == 0


class TestGenerateDDL:
    def test_ddl_statements(self):
        schema = generate_graph_schema(CLASSES, PROPERTIES)
        stmts = generate_ddl(schema)
        node_stmts = [s for s in stmts if "NODE TABLE" in s]
        rel_stmts = [s for s in stmts if "REL TABLE" in s]
        assert len(node_stmts) == 3  # Customer, Order, Resource
        assert len(rel_stmts) == 1  # hasOrder


class TestClassifyTriples:
    def test_classify_basic(self):
        schema = generate_graph_schema(CLASSES, PROPERTIES)
        triples = [
            {"subject": "http://test.org/c1", "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "object": "http://test.org/ont#Customer"},
            {"subject": "http://test.org/c1", "predicate": "http://www.w3.org/2000/01/rdf-schema#label", "object": "Alice"},
            {"subject": "http://test.org/o1", "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "object": "http://test.org/ont#Order"},
            {"subject": "http://test.org/c1", "predicate": "http://test.org/ont#hasOrder", "object": "http://test.org/o1"},
        ]

        node_inserts, rel_inserts, attr_updates = classify_triples(triples, schema)

        assert "Customer" in node_inserts
        assert len(node_inserts["Customer"]) == 1
        assert node_inserts["Customer"][0]["uri"] == "http://test.org/c1"
        assert node_inserts["Customer"][0]["label"] == "Alice"

        assert "Order" in node_inserts
        assert len(node_inserts["Order"]) == 1

        assert len(rel_inserts) == 1
        assert rel_inserts[0]["rel_table"] == "hasOrder"
        assert rel_inserts[0]["from_uri"] == "http://test.org/c1"
        assert rel_inserts[0]["to_uri"] == "http://test.org/o1"

    def test_label_without_type_goes_to_fallback(self):
        schema = generate_graph_schema(CLASSES, PROPERTIES)
        triples = [
            {"subject": "http://test.org/unknown", "predicate": "http://www.w3.org/2000/01/rdf-schema#label", "object": "Unknown Entity"},
        ]
        node_inserts, _, _ = classify_triples(triples, schema)
        assert schema.fallback_node_table in node_inserts
        assert node_inserts[schema.fallback_node_table][0]["uri"] == "http://test.org/unknown"

    def test_unknown_predicate_becomes_attr(self):
        schema = generate_graph_schema(CLASSES, PROPERTIES)
        triples = [
            {"subject": "http://test.org/c1", "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "object": "http://test.org/ont#Customer"},
            {"subject": "http://test.org/c1", "predicate": "http://test.org/ont#firstName", "object": "Alice"},
        ]
        _, _, attr_updates = classify_triples(triples, schema)
        assert "http://test.org/c1" in attr_updates
        assert attr_updates["http://test.org/c1"][0]["predicate"] == "http://test.org/ont#firstName"
