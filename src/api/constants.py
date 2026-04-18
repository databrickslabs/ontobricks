"""
External API package (``api``) — shared constants and OpenAPI metadata.

``API_VERSION`` is the version string published on the external OpenAPI document
(aligned with the application release by default). Path routing uses
``API_URL_PATH_VERSION`` (e.g. ``v1`` → ``/api/v1/...``).
"""

from __future__ import annotations

from typing import Dict, List

from shared.config.constants import APP_VERSION, DEFAULT_BASE_URI, DEFAULT_GRAPH_NAME

# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------

API_VERSION: str = APP_VERSION
"""OpenAPI ``info.version`` for the mounted external API (``/api/docs``)."""

API_URL_PATH_VERSION: str = "v1"
"""URL segment for versioned external routes (``/api/{API_URL_PATH_VERSION}/...``)."""

# ---------------------------------------------------------------------------
# Mount & OpenAPI path rewriting
# ---------------------------------------------------------------------------

EXTERNAL_API_MOUNT_PREFIX: str = "/api"
"""Where :func:`api.external_app.create_external_api_app` is mounted on the main app."""

OPENAPI_PATH_PREFIX: str = EXTERNAL_API_MOUNT_PREFIX
"""Prefix prepended to OpenAPI path keys so Swagger ``Try it`` hits the real URLs."""

# ---------------------------------------------------------------------------
# Derived path prefixes (routers on the sub-application, before mount)
# ---------------------------------------------------------------------------

API_V1_PREFIX: str = f"/{API_URL_PATH_VERSION}"
API_DIGITALTWIN_PREFIX: str = f"/{API_URL_PATH_VERSION}/digitaltwin"
API_GRAPHQL_PREFIX: str = f"/{API_URL_PATH_VERSION}/graphql"

EXTERNAL_GRAPHQL_PUBLIC_PREFIX: str = (
    f"{EXTERNAL_API_MOUNT_PREFIX}/{API_URL_PATH_VERSION}/graphql"
)
"""Full URL path prefix for GraphQL on the mounted external app (e.g. ``/api/v1/graphql``)."""

# ---------------------------------------------------------------------------
# OpenAPI document (external sub-app)
# ---------------------------------------------------------------------------

EXTERNAL_API_TITLE: str = "OntoBricks External API"

EXTERNAL_API_DESCRIPTION: str = """
# OntoBricks External API

REST and GraphQL under `/api/v1/...` (GraphQL at `/api/v1/graphql/...`; same handlers as in-app `/graphql/...`).

## Authentication

Pass Databricks credentials in the JSON body or headers as documented per REST operation. GraphQL uses the browser session when called from the UI; external tools should use the same host and session cookie or call from an authenticated context.

---
*OpenAPI for programmatic access only — UI HTML routes are not listed here.*
"""

EXTERNAL_OPENAPI_TAGS: List[Dict[str, str]] = [
    {
        "name": "Health",
        "description": "Health check endpoints for the external API surface.",
    },
    {
        "name": "API v1",
        "description": "**External REST API** — Stateless endpoints for programmatic access. "
        "Authentication via request body or headers.",
    },
    {
        "name": "Domain",
        "description": "**Domain API** — Registry domain list and domain design artifacts "
        "(OWL ontology, R2RML, generated Spark SQL).",
    },
    {
        "name": "Digital Twin",
        "description": "**Digital Twin API** — Registry discovery, triple store "
        "status, build trigger, and triple retrieval.",
    },
    {
        "name": "GraphQL",
        "description": "**GraphQL API** — List domains, GraphiQL playground, execute queries, and SDL "
        "for the auto-generated ontology-backed schema.",
    },
]

EXTERNAL_API_CONTACT: Dict[str, str] = {
    "name": "OntoBricks Support",
    "url": "https://github.com/databricks/ontobricks",
}

EXTERNAL_API_LICENSE_INFO: Dict[str, str] = {
    "name": "Apache 2.0",
    "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
}

__all__ = [
    "API_VERSION",
    "API_URL_PATH_VERSION",
    "EXTERNAL_API_MOUNT_PREFIX",
    "OPENAPI_PATH_PREFIX",
    "API_V1_PREFIX",
    "API_DIGITALTWIN_PREFIX",
    "API_GRAPHQL_PREFIX",
    "EXTERNAL_GRAPHQL_PUBLIC_PREFIX",
    "EXTERNAL_API_TITLE",
    "EXTERNAL_API_DESCRIPTION",
    "EXTERNAL_OPENAPI_TAGS",
    "EXTERNAL_API_CONTACT",
    "EXTERNAL_API_LICENSE_INFO",
    "DEFAULT_BASE_URI",
    "DEFAULT_GRAPH_NAME",
]
