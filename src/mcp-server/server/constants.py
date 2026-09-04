"""Module-level constants shared across the OntoBricks MCP server.

Tool-set mirrors, OntoBricks external-REST paths, RDF/RDFS vocabulary URIs and
traversal limits. This module has no dependencies so every other server module
can import it freely.
"""

from __future__ import annotations

# Tools usable before a domain is selected. A per-domain policy cannot govern
# them, so they are never hidden. Mirrors ``MCP_REGISTRY_TOOLS`` in
# ``back/core/mcp_tools.py`` — this process cannot import the app package.
REGISTRY_TOOLS = frozenset(
    {"list_domains", "select_domain", "list_domain_versions", "get_design_status"}
)

# Domain-scoped tools that need a built graph. A domain published with only an
# ontology (no build) hides every one of these and exposes ``describe_ontology``
# alone. Mirrors ``MCP_DOMAIN_TOOLS`` minus ``describe_ontology`` in
# ``back/core/mcp_tools.py`` — this process cannot import the app package.
GRAPH_TOOLS = frozenset(
    {
        "list_entity_types",
        "describe_entity",
        "get_status",
        "get_graphql_schema",
        "query_graphql",
        "get_entity_context",
        "invoke_entity_action",
        "compute_virtual_attributes",
    }
)

_USER_AGENT = "ontobricks"

# REST paths — keep in sync with ``api.external_app`` / ``api.routers.*``
API_V1_DOMAINS = "/api/v1/domains"
API_V1_DOMAIN_VERSIONS = "/api/v1/domain/versions"
API_V1_DOMAIN_DESIGN_STATUS = "/api/v1/domain/design-status"
API_V1_DT_REGISTRY = "/api/v1/digitaltwin/registry"
API_V1_DT_STATUS = "/api/v1/digitaltwin/status"
API_V1_DT_STATS = "/api/v1/digitaltwin/stats"
API_V1_DT_TRIPLES_FIND = "/api/v1/digitaltwin/triples/find"
API_V1_DOMAIN_CLASSES = "/api/v1/domain/classes"
API_V1_DOMAIN_ONTOLOGY = "/api/v1/domain/ontology"
API_V1_DT_NODE_CONTEXT = "/api/v1/digitaltwin/nodes/context"
API_V1_DT_NODE_ACTION = "/api/v1/digitaltwin/nodes/action"
API_V1_DT_NODE_VIRTUAL_ATTRIBUTES = "/api/v1/digitaltwin/nodes/virtual-attributes"

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
MAX_DEPTH = 1  # Maximum depth of the BFS traversal
