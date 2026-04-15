"""
External REST API service facade.

Implementation lives under ``back.core`` and ``back.objects``; this module re-exports
stable names for ``api.routers.v1`` and integration tests.
"""
from back.core.databricks import list_domains_from_uc, load_domain_from_uc
from back.core.w3c.sparql.DomainQueryService import DomainQueryService

validate_sparql_query = DomainQueryService.validate_sparql_query
execute_sparql_query = DomainQueryService.execute_sparql_query
generate_sample_queries = DomainQueryService.generate_sample_queries
from back.objects.mapping.json_views import get_mapping_info
from back.objects.ontology.json_views import (
    get_ontology_classes,
    get_ontology_info,
    get_ontology_properties,
)
from back.objects.domain.payload import get_domain_info

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
