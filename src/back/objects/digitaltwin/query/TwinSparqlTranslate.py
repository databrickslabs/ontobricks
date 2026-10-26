"""Translate SPARQL to Spark SQL using the domain R2RML mapping.

Extracted from ``api.routers.internal.dtwin.translate_sparql``.
"""

from __future__ import annotations

from back.core.errors import ValidationError
from back.core.w3c import sparql


def _dt():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    return DigitalTwin


class TwinSparqlTranslate:
    """Compile a read-only SPARQL query against the session mapping."""

    @staticmethod
    def translate(domain, sparql_query: str, limit, default_base_uri: str):
        domain.ensure_generated_content()
        r2rml_content = domain.get_r2rml()
        if not r2rml_content:
            raise ValidationError(
                "No R2RML mapping available. Please configure mappings first."
            )

        entity_mappings, relationship_mappings = sparql.extract_r2rml_mappings(
            r2rml_content
        )
        base_uri = domain.ontology.get("base_uri", default_base_uri)
        entity_mappings = _dt().augment_mappings_from_config(
            entity_mappings, domain.assignment, base_uri, domain.ontology
        )
        relationship_mappings = _dt().augment_relationships_from_config(
            relationship_mappings, domain.assignment, base_uri, domain.ontology
        )
        return sparql.translate_sparql_to_spark(
            sparql_query, entity_mappings, limit, relationship_mappings
        )
