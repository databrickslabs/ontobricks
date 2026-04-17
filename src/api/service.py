"""
External REST API service facade.

Implementation lives under ``back.core`` and ``back.objects``; this module re-exports
stable names for ``api.routers.v1`` and integration tests.
"""
from back.core.databricks import list_domains_from_uc, load_domain_from_uc
from back.core.w3c import DomainQueryService
from back.objects.domain import get_domain_info
from back.objects.mapping import get_mapping_info
from back.objects.ontology import (
    get_ontology_classes,
    get_ontology_info,
    get_ontology_properties,
)

validate_sparql_query = DomainQueryService.validate_sparql_query
execute_sparql_query = DomainQueryService.execute_sparql_query
generate_sample_queries = DomainQueryService.generate_sample_queries

__all__ = [
    "list_domains_from_uc",
    "load_domain_from_uc",
    "get_domain_info",
    "get_ontology_info",
    "get_ontology_classes",
    "get_ontology_properties",
    "get_mapping_info",
    "validate_sparql_query",
    "execute_sparql_query",
    "generate_sample_queries",
]
