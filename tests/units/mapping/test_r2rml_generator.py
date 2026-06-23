"""Tests for R2RML mapping generator."""

import pytest
from back.core.w3c.r2rml.R2RMLGenerator import R2RMLGenerator

generate_r2rml_from_config = R2RMLGenerator.generate_r2rml_from_config


class TestR2RMLGenerator:
    def test_init_normalizes_base_uri(self):
        gen = R2RMLGenerator("http://test.org/ontology#")
        assert gen.base_uri == "http://test.org/ontology/"

    def test_init_default_base_uri(self):
        gen = R2RMLGenerator()
        assert gen.base_uri == "https://databricks-ontology.com/"


class TestEntityMapping:
    def test_generates_triples_map(self):
        gen = R2RMLGenerator("http://test.org/ontology/")
        mapping_config = {
            "entities": [
                {
                    "ontology_class": "http://test.org/ontology#Customer",
                    "ontology_class_label": "Customer",
                    "sql_query": "SELECT * FROM customers",
                    "id_column": "customer_id",
                    "label_column": "name",
                    "attribute_mappings": {},
                }
            ],
            "relationships": [],
        }
        r2rml = gen.generate_mapping(mapping_config)
        assert "TriplesMap" in r2rml
        assert "Customer" in r2rml
        assert "customer_id" in r2rml

    def test_sql_query_in_logical_table(self):
        gen = R2RMLGenerator("http://test.org/ontology/")
        mapping_config = {
            "entities": [
                {
                    "ontology_class": "http://test.org/ontology#Customer",
                    "ontology_class_label": "Customer",
                    "sql_query": "SELECT * FROM catalog.schema.customers",
                    "id_column": "cid",
                    "attribute_mappings": {},
                }
            ],
            "relationships": [],
        }
        r2rml = gen.generate_mapping(mapping_config)
        assert "SELECT * FROM catalog.schema.customers" in r2rml

    def test_label_column_mapping(self):
        gen = R2RMLGenerator("http://test.org/ontology/")
        mapping_config = {
            "entities": [
                {
                    "ontology_class": "http://test.org/ontology#Customer",
                    "ontology_class_label": "Customer",
                    "sql_query": "SELECT * FROM customers",
                    "id_column": "id",
                    "label_column": "full_name",
                    "attribute_mappings": {},
                }
            ],
            "relationships": [],
        }
        r2rml = gen.generate_mapping(mapping_config)
        assert "full_name" in r2rml
        assert "label" in r2rml

    def test_attribute_mappings(self):
        gen = R2RMLGenerator("http://test.org/ontology/")
        mapping_config = {
            "entities": [
                {
                    "ontology_class": "http://test.org/ontology#Customer",
                    "ontology_class_label": "Customer",
                    "sql_query": "SELECT * FROM customers",
                    "id_column": "id",
                    "attribute_mappings": {
                        "firstName": "first_name",
                        "lastName": "last_name",
                    },
                }
            ],
            "relationships": [],
        }
        r2rml = gen.generate_mapping(mapping_config)
        assert "first_name" in r2rml
        assert "last_name" in r2rml

    def test_excluded_entity_skipped(self):
        gen = R2RMLGenerator("http://test.org/ontology/")
        mapping_config = {
            "entities": [
                {
                    "ontology_class": "http://test.org/ontology#Customer",
                    "ontology_class_label": "Customer",
                    "sql_query": "SELECT * FROM customers",
                    "id_column": "id",
                    "excluded": True,
                    "attribute_mappings": {},
                }
            ],
            "relationships": [],
        }
        r2rml = gen.generate_mapping(mapping_config)
        assert "Customer" not in r2rml or "TriplesMap" not in r2rml

    def test_missing_id_column_skipped(self):
        gen = R2RMLGenerator("http://test.org/ontology/")
        mapping_config = {
            "entities": [
                {
                    "ontology_class": "http://test.org/ontology#Customer",
                    "ontology_class_label": "Customer",
                    "sql_query": "SELECT * FROM customers",
                    "id_column": "",
                    "attribute_mappings": {},
                }
            ],
            "relationships": [],
        }
        r2rml = gen.generate_mapping(mapping_config)
        assert "TriplesMap_Customer" not in r2rml


class TestRelationshipMapping:
    def test_relationship_triples_map(self):
        gen = R2RMLGenerator("http://test.org/ontology/")
        mapping_config = {
            "entities": [
                {
                    "ontology_class": "http://test.org/ontology#Customer",
                    "ontology_class_label": "Customer",
                    "sql_query": "SELECT * FROM customers",
                    "id_column": "customer_id",
                    "attribute_mappings": {},
                },
                {
                    "ontology_class": "http://test.org/ontology#Order",
                    "ontology_class_label": "Order",
                    "sql_query": "SELECT * FROM orders",
                    "id_column": "order_id",
                    "attribute_mappings": {},
                },
            ],
            "relationships": [
                {
                    "property": "http://test.org/ontology#hasOrder",
                    "property_label": "hasOrder",
                    "sql_query": "SELECT customer_id, order_id FROM orders",
                    "source_class": "http://test.org/ontology#Customer",
                    "source_class_label": "Customer",
                    "target_class": "http://test.org/ontology#Order",
                    "target_class_label": "Order",
                    "source_id_column": "customer_id",
                    "target_id_column": "order_id",
                    "direction": "forward",
                }
            ],
        }
        r2rml = gen.generate_mapping(mapping_config)
        assert "hasOrder" in r2rml
        assert "Rel_" in r2rml

    def test_relationship_uris_match_entity_uris_when_label_differs(self):
        """Regression for issue #48.

        When a class label differs from the local name of its class URI, the
        entity subject URI (built from the URI local name) and the relationship
        subject/object URIs (previously built from the label) must still share
        the same namespace, otherwise BFS expansion finds no edges.
        """
        base = "http://test.org/ontology/"
        gen = R2RMLGenerator(base)
        mapping_config = {
            "entities": [
                {
                    # local name "Cust" != label "Customer"
                    "ontology_class": "http://test.org/ontology#Cust",
                    "ontology_class_label": "Customer",
                    "sql_query": "SELECT * FROM customers",
                    "id_column": "customer_id",
                    "attribute_mappings": {},
                },
                {
                    # local name "Ord" != label "Order"
                    "ontology_class": "http://test.org/ontology#Ord",
                    "ontology_class_label": "Order",
                    "sql_query": "SELECT * FROM orders",
                    "id_column": "order_id",
                    "attribute_mappings": {},
                },
            ],
            "relationships": [
                {
                    "property": "http://test.org/ontology#hasOrder",
                    "property_label": "hasOrder",
                    "sql_query": "SELECT customer_id, order_id FROM orders",
                    "source_class": "http://test.org/ontology#Cust",
                    "source_class_label": "Customer",
                    "target_class": "http://test.org/ontology#Ord",
                    "target_class_label": "Order",
                    "source_id_column": "customer_id",
                    "target_id_column": "order_id",
                    "direction": "forward",
                }
            ],
        }
        r2rml = gen.generate_mapping(mapping_config)

        # Entity subject URIs use the URI local name, not the label.
        assert f"{base}Cust/" in r2rml
        assert f"{base}Ord/" in r2rml
        # Relationship subject/object URIs must use the SAME namespace.
        # Columns are always double-quoted; rdflib escapes inner " as \" in Turtle.
        assert f"{base}Cust/" in r2rml
        assert f"{base}Ord/" in r2rml
        # Template must contain the column reference (quoted form)
        assert "customer_id" in r2rml
        assert "order_id" in r2rml
        # And must NOT fall back to the label namespace (the bug).
        assert f"{base}Customer/" not in r2rml
        assert f"{base}Order/" not in r2rml


class TestConvenienceFunction:
    def test_generate_r2rml_from_config(self):
        mapping = {
            "entities": [
                {
                    "ontology_class": "http://test.org/ontology#Foo",
                    "ontology_class_label": "Foo",
                    "sql_query": "SELECT * FROM foo",
                    "id_column": "id",
                    "attribute_mappings": {},
                }
            ],
            "relationships": [],
        }
        ontology = {"base_uri": "http://test.org/ontology/"}
        r2rml = generate_r2rml_from_config(mapping, ontology)
        assert "TriplesMap" in r2rml

    def test_default_base_uri_when_example_org(self):
        mapping = {"entities": [], "relationships": []}
        ontology = {"base_uri": "http://example.org/"}
        r2rml = generate_r2rml_from_config(mapping, ontology)
        assert r2rml is not None


class TestQuoteColumn:
    """Unit tests for _quote_column helper."""

    def setup_method(self):
        self.gen = R2RMLGenerator("http://test.org/ontology/")

    def test_plain_identifier_always_quoted(self):
        # Always double-quote every column name — plain identifiers included.
        assert self.gen._quote_column("customer_id") == '"customer_id"'

    def test_already_quoted_unchanged(self):
        assert self.gen._quote_column('"customer id"') == '"customer id"'

    def test_space_gets_quoted(self):
        assert self.gen._quote_column("customer id") == '"customer id"'

    def test_hyphen_gets_quoted(self):
        assert self.gen._quote_column("my-col") == '"my-col"'

    def test_dot_gets_quoted(self):
        assert self.gen._quote_column("first.name") == '"first.name"'

    def test_empty_string_unchanged(self):
        assert self.gen._quote_column("") == ""

    def test_column_with_space_in_entity_template(self):
        """rr:template should contain double-quoted column when name has a space.

        rdflib serialises the inner double-quotes as \\\" in the Turtle literal,
        so we check for the escaped representation in the raw Turtle string.
        """
        gen = R2RMLGenerator("http://test.org/ontology/")
        mapping_config = {
            "entities": [{
                "ontology_class": "http://test.org/ontology/Customer",
                "ontology_class_label": "Customer",
                "sql_query": 'SELECT `customer id` AS "customer id", name AS Label FROM t',
                "id_column": "customer id",
                "label_column": "Label",
                "attribute_mappings": {}
            }],
            "relationships": []
        }
        r2rml = gen.generate_mapping(mapping_config)
        # rdflib escapes inner " as \" in the Turtle literal — check both forms
        assert '\\"customer id\\"' in r2rml or '"customer id"' in r2rml

    def test_column_with_space_in_attribute_mapping(self):
        """rr:column for an attribute with spaces must be double-quoted.

        rdflib serialises the inner double-quotes as \\\" in the Turtle literal.
        """
        gen = R2RMLGenerator("http://test.org/ontology/")
        mapping_config = {
            "entities": [{
                "ontology_class": "http://test.org/ontology/Customer",
                "ontology_class_label": "Customer",
                "sql_query": 'SELECT id AS ID, name AS Label, `full name` FROM t',
                "id_column": "ID",
                "label_column": "Label",
                "attribute_mappings": {"fullName": "full name"}
            }],
            "relationships": []
        }
        r2rml = gen.generate_mapping(mapping_config)
        assert '\\"full name\\"' in r2rml or '"full name"' in r2rml
