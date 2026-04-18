"""SPARQL utilities for query translation and execution."""

from back.core.w3c.sparql.constants import DIALECT_SPARK
from back.core.w3c.sparql.DomainQueryService import DomainQueryService
from back.core.w3c.sparql.SparqlQueryRunner import SparqlQueryRunner
from back.core.w3c.sparql.SparqlTranslator import SparqlTranslator


def execute_local_query(query, rdf_content, limit):
    """Backward-compatible wrapper for :meth:`SparqlQueryRunner.execute_local_query`."""
    return SparqlQueryRunner.execute_local_query(query, rdf_content, limit)


def extract_r2rml_mappings(r2rml_content):
    """Backward-compatible wrapper for :meth:`SparqlQueryRunner.extract_r2rml_mappings`."""
    return SparqlQueryRunner.extract_r2rml_mappings(r2rml_content)


def translate_sparql_to_spark(
    sparql_query,
    entity_mappings,
    limit,
    relationship_mappings=None,
    dialect=DIALECT_SPARK,
):
    """Backward-compatible wrapper for :meth:`SparqlTranslator.translate_sparql_to_spark`."""
    return SparqlTranslator.translate_sparql_to_spark(
        sparql_query,
        entity_mappings,
        limit,
        relationship_mappings,
        dialect=dialect,
    )


__all__ = [
    "DIALECT_SPARK",
    "execute_local_query",
    "extract_r2rml_mappings",
    "translate_sparql_to_spark",
    "DomainQueryService",
    "SparqlQueryRunner",
    "SparqlTranslator",
]
