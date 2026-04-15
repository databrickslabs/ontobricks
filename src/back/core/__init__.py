"""Core functionality for OntoBricks.

Heavy sub-packages (w3c, objects) are imported lazily to avoid circular
dependencies at module-init time.
"""
from back.core.databricks import DatabricksClient, VolumeFileService
from back.core.helpers import (
    get_databricks_client,
    get_databricks_credentials,
    get_databricks_host_and_token,
    sql_escape,
    effective_view_table,
    effective_graph_name,
    validate_table_name,
)
from back.core.errors import (
    OntoBricksError,
    NotFoundError,
    ValidationError,
    AuthorizationError,
    InfrastructureError,
    ConflictError,
    ErrorResponse,
)


def __getattr__(name):
    if name == "sparql":
        from back.core.w3c import sparql as _sparql
        return _sparql
    if name in ("DomainSession", "get_domain", "get_empty_domain"):
        import back.objects.session as _sess
        return getattr(_sess, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    'DatabricksClient',
    'VolumeFileService',
    'sparql',
    'DomainSession',
    'get_domain',
    'get_empty_domain',
    'get_databricks_client',
    'get_databricks_credentials',
    'get_databricks_host_and_token',
    'sql_escape',
    'effective_view_table',
    'effective_graph_name',
    'validate_table_name',
    'OntoBricksError',
    'NotFoundError',
    'ValidationError',
    'AuthorizationError',
    'InfrastructureError',
    'ConflictError',
    'ErrorResponse',
]
