"""
External REST API service facade.

Implementation lives under ``back.core`` and ``back.objects``; this module re-exports
stable names for ``api.routers.v1`` and integration tests.
"""
from back.core.databricks import list_projects_from_uc, load_project_from_uc
from back.core.w3c.sparql.ProjectQueryService import ProjectQueryService

validate_sparql_query = ProjectQueryService.validate_sparql_query
execute_sparql_query = ProjectQueryService.execute_sparql_query
generate_sample_queries = ProjectQueryService.generate_sample_queries
from back.objects.mapping.json_views import get_mapping_info
from back.objects.ontology.json_views import (
    get_ontology_classes,
    get_ontology_info,
    get_ontology_properties,
)
from back.objects.project.payload import get_project_info

__all__ = [
    "list_projects_from_uc",
    "load_project_from_uc",
    "get_project_info",
    "get_ontology_info",
    "get_ontology_classes",
    "get_ontology_properties",
    "get_mapping_info",
    "validate_sparql_query",
    "execute_sparql_query",
    "generate_sample_queries",
]
