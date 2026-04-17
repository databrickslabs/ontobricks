"""Core helper functions used across the application."""
from back.core.helpers.DatabricksHelpers import (  # noqa: F401
    DatabricksHelpers,
    effective_uc_version_path,
    resolve_ladybug_local_path,
)
from back.core.helpers.SQLHelpers import SQLHelpers  # noqa: F401
from back.core.helpers.URIHelpers import URIHelpers  # noqa: F401

# SQL / URI helpers must be bound before importing databricks: transitive
# imports can pull ``back.core.helpers`` again while this module is still
# initializing (e.g. triplestore → helpers for ``sql_escape``).
sql_escape = SQLHelpers.sql_escape
validate_table_name = SQLHelpers.validate_table_name
effective_view_table = SQLHelpers.effective_view_table
effective_graph_name = SQLHelpers.effective_graph_name

is_uri = URIHelpers.is_uri
extract_local_name = URIHelpers.extract_local_name
safe_identifier = URIHelpers.safe_identifier

from back.core.databricks import (  # noqa: F401  — re-exported for backward compat
    get_workspace_host,
    is_databricks_app,
    normalize_host,
)

# Backward-compatible function wrappers
run_blocking = DatabricksHelpers.run_blocking
resolve_warehouse_id = DatabricksHelpers.resolve_warehouse_id
resolve_default_base_uri = DatabricksHelpers.resolve_default_base_uri
resolve_default_emoji = DatabricksHelpers.resolve_default_emoji
get_databricks_client = DatabricksHelpers.get_databricks_client
get_databricks_credentials = DatabricksHelpers.get_databricks_credentials
get_databricks_host_and_token = DatabricksHelpers.get_databricks_host_and_token
require_serving_llm = DatabricksHelpers.require_serving_llm

__all__ = [
    "DatabricksHelpers",
    "SQLHelpers",
    "URIHelpers",
    "run_blocking",
    "resolve_warehouse_id",
    "resolve_default_base_uri",
    "resolve_default_emoji",
    "get_databricks_client",
    "get_databricks_credentials",
    "get_databricks_host_and_token",
    "require_serving_llm",
    "effective_uc_version_path",
    "resolve_ladybug_local_path",
    "sql_escape",
    "validate_table_name",
    "effective_view_table",
    "effective_graph_name",
    "is_uri",
    "extract_local_name",
    "safe_identifier",
]
