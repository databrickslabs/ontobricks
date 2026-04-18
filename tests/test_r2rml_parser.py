"""Tests for R2RML parser."""

import pytest
from back.core.errors import ValidationError
from back.core.w3c.r2rml.R2RMLParser import R2RMLParser
from back.core.w3c.r2rml.R2RMLGenerator import R2RMLGenerator

parse_r2rml_content = R2RMLParser.parse_r2rml_content


SAMPLE_R2RML = """@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ont: <http://test.org/ontology/> .

ont:TriplesMap_Customer a rr:TriplesMap ;
    rdfs:comment "Mapping for Customer" ;
    rr:logicalTable [ rr:tableName "catalog.schema.customers" ] ;
    rr:subjectMap [
        rr:template "http://test.org/ontology/Customer/{customer_id}" ;
        rr:class ont:Customer
    ] ;
    rr:predicateObjectMap [
        rr:predicate rdfs:label ;
        rr:objectMap [ rr:column "name" ]
    ] .
"""


class TestR2RMLParser:
    def test_parse_valid(self):
        parser = R2RMLParser(SAMPLE_R2RML)
        assert parser.graph is not None

    def test_parse_invalid_raises(self):
        with pytest.raises(ValidationError):
            R2RMLParser("this is not valid r2rml")

    def test_extract_entity_mappings(self):
        parser = R2RMLParser(SAMPLE_R2RML)
        entities, relationships = parser.extract_mappings()
        assert len(entities) >= 1
        entity = entities[0]
        assert entity["ontology_class_label"] == "Customer"
        assert entity["id_column"] == "customer_id"
        assert entity["table"] == "customers"

    def test_parse_table_name_three_parts(self):
        parser = R2RMLParser(SAMPLE_R2RML)
        cat, schema, table = parser._parse_table_name("cat.sch.tbl")
        assert cat == "cat"
        assert schema == "sch"
        assert table == "tbl"

    def test_parse_table_name_two_parts(self):
        parser = R2RMLParser(SAMPLE_R2RML)
        cat, schema, table = parser._parse_table_name("sch.tbl")
        assert cat is None
        assert schema == "sch"
        assert table == "tbl"

    def test_parse_table_name_one_part(self):
        parser = R2RMLParser(SAMPLE_R2RML)
        cat, schema, table = parser._parse_table_name("tbl")
        assert cat is None
        assert schema is None
        assert table == "tbl"

    def test_parse_table_name_none(self):
        parser = R2RMLParser(SAMPLE_R2RML)
        cat, schema, table = parser._parse_table_name(None)
        assert cat is None and schema is None and table is None


class TestParseR2RMLContent:
    def test_convenience_function(self):
        entities, rels = parse_r2rml_content(SAMPLE_R2RML)
        assert len(entities) >= 1


class TestRoundtrip:
    def test_generate_then_parse(self):
        gen = R2RMLGenerator("http://test.org/ontology/")
        mapping_config = {
            "entities": [
                {
                    "ontology_class": "http://test.org/ontology/Customer",
                    "ontology_class_label": "Customer",
                    "sql_query": "",
                    "id_column": "customer_id",
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "customers",
                    "attribute_mappings": {},
                }
            ],
            "relationships": [],
        }
        r2rml = gen.generate_mapping(mapping_config)
        entities, rels = parse_r2rml_content(r2rml)
        assert len(entities) >= 1
        assert entities[0]["id_column"] == "customer_id"
