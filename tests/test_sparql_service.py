"""Tests for SPARQL translation and execution service."""
import pytest

from back.core.errors import ValidationError
from back.core.w3c.sparql.constants import DIALECT_SPARK
from back.core.w3c.sparql.SparqlQueryRunner import SparqlQueryRunner
from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator

translate_sparql_to_spark = SparqlTranslator.translate_sparql_to_spark
execute_local_query = SparqlQueryRunner.execute_local_query
extract_r2rml_mappings = SparqlQueryRunner.extract_r2rml_mappings
_string_type = SparqlTranslator._string_type
_cast_str = SparqlTranslator._cast_str


SAMPLE_R2RML = """@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ont: <http://test.org/ontology/> .

ont:TriplesMap_Customer a rr:TriplesMap ;
    rr:logicalTable [ rr:sqlQuery "SELECT * FROM catalog.schema.customers" ] ;
    rr:subjectMap [
        rr:template "http://test.org/ontology/Customer/{customer_id}" ;
        rr:class <http://test.org/ontology/Customer>
    ] ;
    rr:predicateObjectMap [
        rr:predicate rdfs:label ;
        rr:objectMap [ rr:column "name" ]
    ] .
"""

SAMPLE_TURTLE_DATA = """@prefix : <http://test.org/ontology#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .

:Customer1 a :Customer ;
    rdfs:label "Alice" .

:Customer2 a :Customer ;
    rdfs:label "Bob" .

:Order1 a :Order ;
    rdfs:label "Order-001" .

:Customer a owl:Class .
:Order a owl:Class .
"""


class TestDialectHelpers:
    def test_string_type_spark(self):
        assert _string_type(DIALECT_SPARK) == "STRING"

    def test_cast_str_spark(self):
        result = _cast_str("col", DIALECT_SPARK)
        assert "CAST(col AS STRING)" == result


class TestExtractR2RMLMappings:
    def test_extracts_entity_mappings(self):
        entities, rels = extract_r2rml_mappings(SAMPLE_R2RML)
        assert len(entities) >= 1
        key = list(entities.keys())[0]
        assert 'Customer' in key
        mapping = entities[key]
        assert mapping['id_column'] == 'customer_id'
        assert mapping['sql_query'] is not None


class TestExecuteLocalQuery:
    def test_select_query(self):
        query = "SELECT ?s ?label WHERE { ?s <http://www.w3.org/2000/01/rdf-schema#label> ?label }"
        result = execute_local_query(query, SAMPLE_TURTLE_DATA, limit=100)
        assert result['success'] is True
        assert result['count'] >= 2
        assert 's' in result['columns']
        assert 'label' in result['columns']

    def test_select_with_limit(self):
        query = "SELECT ?s WHERE { ?s a ?type }"
        result = execute_local_query(query, SAMPLE_TURTLE_DATA, limit=1)
        assert result['count'] <= 1


class TestTranslateSparqlToSpark:
    def test_non_select_rejected(self):
        entities, _ = extract_r2rml_mappings(SAMPLE_R2RML)
        with pytest.raises(ValidationError):
            translate_sparql_to_spark(
                "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
                entities, limit=100,
            )

    def test_basic_select(self):
        entities, rels = extract_r2rml_mappings(SAMPLE_R2RML)
        sparql = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        result = translate_sparql_to_spark(sparql, entities, limit=100, relationship_mappings=rels)
        assert 'success' in result
