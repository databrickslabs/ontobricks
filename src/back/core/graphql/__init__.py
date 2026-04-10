"""GraphQL schema generation from OntoBricks ontologies."""
from back.core.graphql.constants import RDF_TYPE, RDFS_LABEL, DEFAULT_DEPTH, MAX_DEPTH  # noqa: F401
from back.core.graphql.models import TypeInfo  # noqa: F401
from back.core.graphql.SchemaMetadata import SchemaMetadata  # noqa: F401
from back.core.graphql.ResolverFactory import ResolverFactory  # noqa: F401
from back.core.graphql.GraphQLSchemaBuilder import (  # noqa: F401
    GraphQLSchemaBuilder,
    build_schema_for_project,
    invalidate_cache,
)

# Backward-compat wrappers
make_list_resolver = ResolverFactory.make_list_resolver
make_single_resolver = ResolverFactory.make_single_resolver

__all__ = [
    "GraphQLSchemaBuilder", "SchemaMetadata", "TypeInfo", "ResolverFactory",
    "build_schema_for_project", "invalidate_cache",
    "make_list_resolver", "make_single_resolver",
    "RDF_TYPE", "RDFS_LABEL", "DEFAULT_DEPTH", "MAX_DEPTH",
]
