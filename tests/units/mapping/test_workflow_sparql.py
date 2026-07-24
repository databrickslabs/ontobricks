"""Workflow tests: ontology + mapping -> R2RML -> extract -> translate SPARQL."""

import pytest
from back.core.w3c.r2rml.R2RMLGenerator import R2RMLGenerator
from back.core.w3c.sparql.SparqlQueryRunner import SparqlQueryRunner
from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator

extract_r2rml_mappings = SparqlQueryRunner.extract_r2rml_mappings
translate_sparql_to_spark = SparqlTranslator.translate_sparql_to_spark


class TestSPARQLTranslationPipeline:
    def test_full_pipeline(self, sample_ontology_config, sample_mapping_config):
        base_uri = sample_ontology_config["base_uri"]
        gen = R2RMLGenerator(base_uri)
        r2rml = gen.generate_mapping(sample_mapping_config, sample_ontology_config)

        entity_mappings, relationship_mappings = extract_r2rml_mappings(r2rml)
        assert len(entity_mappings) >= 1

        sparql = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        result = translate_sparql_to_spark(
            sparql,
            entity_mappings,
            limit=100,
            relationship_mappings=relationship_mappings,
        )
        assert "success" in result

    def test_select_label_query(self, sample_ontology_config, sample_mapping_config):
        base_uri = sample_ontology_config["base_uri"]
        gen = R2RMLGenerator(base_uri)
        r2rml = gen.generate_mapping(sample_mapping_config, sample_ontology_config)

        entity_mappings, relationship_mappings = extract_r2rml_mappings(r2rml)

        sparql = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?s ?label WHERE { ?s rdfs:label ?label }
        """
        result = translate_sparql_to_spark(
            sparql,
            entity_mappings,
            limit=100,
            relationship_mappings=relationship_mappings,
        )
        assert "success" in result

    def test_id_column_not_none_after_always_quoted_template(
        self, sample_ontology_config, sample_mapping_config
    ):
        """R2RMLGenerator now always double-quotes column names inside rr:template
        (e.g. {"customer_id"}).  SparqlQueryRunner must still extract a non-None
        id_column so the view builder doesn't emit 'Column None not found'.
        """
        base_uri = sample_ontology_config["base_uri"]
        gen = R2RMLGenerator(base_uri)
        r2rml = gen.generate_mapping(sample_mapping_config, sample_ontology_config)

        entity_mappings, _ = extract_r2rml_mappings(r2rml)

        for class_uri, mapping in entity_mappings.items():
            assert mapping.get("id_column") is not None, (
                f"id_column is None for {class_uri} — double-quoted template "
                f"was not parsed correctly"
            )
            assert mapping["id_column"] != "None", (
                f"id_column is the string 'None' for {class_uri}"
            )
