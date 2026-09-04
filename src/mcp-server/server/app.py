"""
OntoBricks MCP Server — public facade.

Exposes Domain registry metadata and Knowledge Graph triple-store capabilities
as MCP tools and resources. HTTP calls target the OntoBricks **external REST**
surface (``/api/v1/...``) and in-app GraphQL (``/graphql/...``).

REST layout (see ``api.external_app``):

- **Domain** — ``GET /api/v1/domains``, ``/api/v1/domain/versions``,
  ``/api/v1/domain/design-status``, ``/api/v1/domain/ontology``, etc.
- **Knowledge Graph** — ``GET /api/v1/digitaltwin/registry``, ``status``,
  ``stats``, ``triples/find``, build, quality, inference, …

Workflow:
  1. ``list_domains`` — discover available domains (knowledge graphs).
  2. ``list_domain_versions`` / ``get_design_status`` (optional) —
     versions and design readiness before heavy queries.
  3. ``select_domain`` — choose which domain to work with.
  4. ``list_entity_types`` / ``describe_entity`` / ``get_status`` —
     query the selected domain's Knowledge Graph.

Three operating modes controlled by the ``mode`` argument:

  - ``"databricks"``  : Databricks App entry point — builds a combined
                         FastAPI + FastMCP application served by uvicorn.
                         ``ONTOBRICKS_URL`` env var points to the main app.
                         Uses the app's service principal token for auth.
  - ``"standalone"``   : Separate process for LLM clients (stdio / HTTP).
                         ``ONTOBRICKS_URL`` env var points to the main app
                         (default ``http://localhost:8000``).
  - ``"mounted"``      : Embedded inside the main OntoBricks FastAPI process.
                         Calls back via ``http://localhost:<DATABRICKS_APP_PORT>``.

Implementation note
-------------------
The server is split by topic into sibling modules; this module is a thin
re-export facade so existing import paths (``from server.app import …``) and
the uvicorn entry point (``server.app:combined_app``) stay stable:

- :mod:`server.constants`   — tool-set mirrors, REST paths, RDF vocabulary.
- :mod:`server.uri_helpers` — URI → local-name / label helpers.
- :mod:`server.formatting`  — JSON → LLM-friendly text renderers.
- :mod:`server.http_client` — base URL, auth headers, retrying GET/POST.
- :mod:`server.session`     — per-server state + registry/policy gating.
- :mod:`server.tools`       — ``@mcp.tool`` handlers.
- :mod:`server.resources`   — ``@mcp.resource`` handlers.
- :mod:`server.factory`     — ``create_mcp_server`` / ``create_databricks_app``.
"""

from __future__ import annotations

# ``httpx`` is re-exported so tests can patch ``server.app.httpx.AsyncClient``.
import httpx  # noqa: F401

from server.constants import (  # noqa: F401
    API_V1_DOMAIN_CLASSES,
    API_V1_DOMAIN_DESIGN_STATUS,
    API_V1_DOMAIN_ONTOLOGY,
    API_V1_DOMAIN_VERSIONS,
    API_V1_DOMAINS,
    API_V1_DT_NODE_ACTION,
    API_V1_DT_NODE_CONTEXT,
    API_V1_DT_NODE_VIRTUAL_ATTRIBUTES,
    API_V1_DT_REGISTRY,
    API_V1_DT_STATS,
    API_V1_DT_STATUS,
    API_V1_DT_TRIPLES_FIND,
    GRAPH_TOOLS,
    MAX_DEPTH,
    RDF_TYPE,
    RDFS_LABEL,
    REGISTRY_TOOLS,
    _USER_AGENT,
)
from server.factory import create_databricks_app, create_mcp_server  # noqa: F401
from server.formatting import (  # noqa: F401
    _format_class_context_block,
    _format_entity_block,
    _format_find_response,
    _format_graphql_entity,
    _format_graphql_response,
    _format_node_action_response,
    _format_node_context_response,
    _format_virtual_attribute_lines,
    _format_virtual_attributes_response,
    _hint,
    _merge_uri_aliases,
    _preferred,
)
from server.http_client import (  # noqa: F401
    _OAUTH_TOKEN_TTL,
    _RETRY_DELAYS,
    _RETRYABLE_STATUSES,
    _base_url,
    _get,
    _get_auth_headers,
    _oauth_cache,
    _post,
    _retry_delays_for,
    _retryable,
)
from server.uri_helpers import (  # noqa: F401
    _is_label_predicate,
    _is_uri,
    _local_name,
    _pretty_predicate,
)

# Built at import time so ``server.app:combined_app`` (uvicorn / main.py)
# resolves without an extra factory call, matching the pre-split behaviour.
combined_app = create_databricks_app()

__all__ = [
    "create_mcp_server",
    "create_databricks_app",
    "combined_app",
    "REGISTRY_TOOLS",
    "GRAPH_TOOLS",
    "MAX_DEPTH",
    "RDF_TYPE",
    "RDFS_LABEL",
    "API_V1_DOMAINS",
    "API_V1_DOMAIN_VERSIONS",
    "API_V1_DOMAIN_DESIGN_STATUS",
    "API_V1_DT_REGISTRY",
    "API_V1_DT_STATUS",
    "API_V1_DT_STATS",
    "API_V1_DT_TRIPLES_FIND",
    "API_V1_DOMAIN_CLASSES",
    "API_V1_DOMAIN_ONTOLOGY",
    "API_V1_DT_NODE_CONTEXT",
    "API_V1_DT_NODE_ACTION",
    "API_V1_DT_NODE_VIRTUAL_ATTRIBUTES",
    "_base_url",
    "_get_auth_headers",
    "_get",
    "_post",
    "_local_name",
    "_pretty_predicate",
    "_is_uri",
    "_is_label_predicate",
    "_preferred",
    "_hint",
    "_format_entity_block",
    "_format_class_context_block",
    "_format_node_context_response",
    "_format_virtual_attribute_lines",
    "_format_virtual_attributes_response",
    "_format_node_action_response",
    "_merge_uri_aliases",
    "_format_find_response",
    "_format_graphql_response",
    "_format_graphql_entity",
]
