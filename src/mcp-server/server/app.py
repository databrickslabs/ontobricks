"""
OntoBricks MCP Server

Exposes Project registry metadata and Digital Twin triple-store capabilities
as MCP tools and resources. HTTP calls target the OntoBricks **external REST**
surface (``/api/v1/...``) and in-app GraphQL (``/graphql/...``).

REST layout (see ``api.external_app``):

- **Project** — ``GET /api/v1/projects``, ``/api/v1/project/versions``,
  ``/api/v1/project/design-status``, ``/api/v1/project/ontology``, etc.
- **Digital Twin** — ``GET /api/v1/digitaltwin/registry``, ``status``,
  ``stats``, ``triples/find``, build, quality, inference, …

Workflow:
  1. ``list_projects`` — discover available projects (knowledge graphs).
  2. ``list_project_versions`` / ``get_design_status`` (optional) —
     versions and design readiness before heavy queries.
  3. ``select_project`` — choose which project to work with.
  4. ``list_entity_types`` / ``describe_entity`` / ``get_status`` —
     query the selected project's Digital Twin.

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
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Optional

import httpx
from fastmcp import FastMCP

logger = logging.getLogger(__name__)

# Cached M2M OAuth token (module-level to survive across _get_auth_headers calls)
_oauth_cache: dict = {"token": "", "ts": 0.0}
_OAUTH_TOKEN_TTL = 3000  # refresh well before the typical 3600 s expiry

# REST paths — keep in sync with ``api.external_app`` / ``api.routers.*``
API_V1_PROJECTS = "/api/v1/projects"
API_V1_PROJECT_VERSIONS = "/api/v1/project/versions"
API_V1_PROJECT_DESIGN_STATUS = "/api/v1/project/design-status"
API_V1_DT_REGISTRY = "/api/v1/digitaltwin/registry"
API_V1_DT_STATUS = "/api/v1/digitaltwin/status"
API_V1_DT_STATS = "/api/v1/digitaltwin/stats"
API_V1_DT_TRIPLES_FIND = "/api/v1/digitaltwin/triples/find"

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
MAX_DEPTH = 1 # Maximum depth of the BFS traversal

# ── URI helpers ───────────────────────────────────────────────────────────

def _local_name(uri: str) -> str:
    """Extract the human-readable local name from a URI.

    ``https://ontobricks.com/ontology/Customer/CUST00094``  →  ``CUST00094``
    ``http://www.w3.org/1999/02/22-rdf-syntax-ns#type``     →  ``type``
    """
    for sep in ("#", "/"):
        idx = uri.rfind(sep)
        if idx >= 0 and idx < len(uri) - 1:
            return uri[idx + 1:]
    return uri


def _pretty_predicate(uri: str) -> str:
    """Turn a predicate URI into a readable attribute name.

    ``https://ontobricks.com/ontologylastname``  →  ``lastname``
    Handles both ``#``-separated and path-separated URIs, and also bare
    camelCase concatenation (``ontologylastname`` → ``lastname``).
    """
    name = _local_name(uri)
    m = re.match(r"^ontology(.+)$", name, re.IGNORECASE)
    if m:
        name = m.group(1)
    name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    return name.replace("_", " ").strip()


def _is_uri(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def _is_label_predicate(pred: str) -> bool:
    ln = _local_name(pred).lower()
    return ln in ("label", "name") or pred == RDFS_LABEL


# ── Triple formatting ────────────────────────────────────────────────────

def _format_entity_block(entity_uri: str, triples: list[dict]) -> str:
    """Build a human-readable text block for one entity."""
    lines: list[str] = []
    entity_label = _local_name(entity_uri)
    types: list[str] = []
    labels: list[str] = []
    attributes: list[tuple[str, str]] = []
    relationships: list[tuple[str, str]] = []

    for t in triples:
        pred = t["predicate"]
        obj = t["object"]

        if pred == RDF_TYPE:
            types.append(_local_name(obj))
        elif _is_label_predicate(pred):
            labels.append(obj)
        elif _is_uri(obj):
            relationships.append((_pretty_predicate(pred), _local_name(obj)))
        else:
            attributes.append((_pretty_predicate(pred), obj))

    display_name = labels[0] if labels else entity_label
    type_str = ", ".join(types) if types else "Unknown type"
    lines.append(f"■ {display_name}  ({type_str})")
    lines.append(f"  URI: {entity_uri}")

    if labels and len(labels) > 1:
        for lbl in labels[1:]:
            lines.append(f"  Also known as: {lbl}")

    if attributes:
        lines.append("  Attributes:")
        for attr_name, attr_val in attributes:
            lines.append(f"    • {attr_name}: {attr_val}")

    if relationships:
        lines.append("  Relationships:")
        for rel_name, target in relationships:
            lines.append(f"    → {rel_name}: {target}")

    return "\n".join(lines)


def _merge_uri_aliases(by_subject: dict[str, list[dict]]) -> dict[str, list[dict]]:
    """Merge triples from URI aliases into a single entity.

    R2RML mappings may produce different URI patterns for the same
    entity (e.g. ``…/Customer/CUST00094`` and ``…/CUST00094``).  Group
    them by their local identifier and pick the richest URI as the
    canonical one.
    """
    groups: dict[str, list[str]] = {}
    for uri in by_subject:
        lid = _local_name(uri)
        groups.setdefault(lid, []).append(uri)

    merged: dict[str, list[dict]] = {}
    for lid, uris in groups.items():
        canonical = max(uris, key=lambda u: len(by_subject.get(u, [])))
        combined: list[dict] = []
        seen: set[tuple] = set()
        for u in uris:
            for t in by_subject.get(u, []):
                key = (t["predicate"], t["object"])
                if key not in seen:
                    seen.add(key)
                    combined.append(t)
        merged[canonical] = combined
    return merged


def _format_find_response(data: dict) -> str:
    """Convert a /triples/find JSON response into a full-text description."""
    if not data.get("success"):
        return data.get("message", "Search failed.")

    seed_count = data.get("seed_count", 0)
    if seed_count == 0:
        return data.get("message") or "No matching entities found."

    triples = data.get("triples", [])
    depth = data.get("depth", 1)
    total = data.get("total", len(triples))

    by_subject: dict[str, list[dict]] = {}
    for t in triples:
        by_subject.setdefault(t["subject"], []).append(t)

    by_subject = _merge_uri_aliases(by_subject)

    seed_uris: set[str] = set()
    related_uris: set[str] = set()
    for uri, subj_triples in by_subject.items():
        has_attributes = any(
            not _is_uri(t["object"]) and t["predicate"] != RDF_TYPE
            for t in subj_triples
        )
        if has_attributes and len(seed_uris) < seed_count:
            seed_uris.add(uri)
        else:
            related_uris.add(uri)

    if not seed_uris:
        seed_uris = set(list(by_subject.keys())[:seed_count])
        related_uris = set(by_subject.keys()) - seed_uris

    unique_entities = len(by_subject)
    parts: list[str] = []
    parts.append(f"Found {seed_count} matching entit{'y' if seed_count == 1 else 'ies'} "
                 f"({total} triples across {unique_entities} entities, depth={depth})\n")

    parts.append("── Matching Entities ──")
    for uri in seed_uris:
        parts.append(_format_entity_block(uri, by_subject.get(uri, [])))
        parts.append("")

    if related_uris:
        parts.append("── Related Entities (neighbors) ──")
        for uri in related_uris:
            parts.append(_format_entity_block(uri, by_subject.get(uri, [])))
            parts.append("")

    if total > len(triples):
        parts.append(f"(Showing {len(triples)} of {total} triples — "
                     f"increase limit or use pagination for more)")

    return "\n".join(parts)


def _format_graphql_response(data: dict, project_name: str) -> str:
    """Convert a GraphQL JSON response into LLM-friendly text."""
    errors = data.get("errors")
    result_data = data.get("data")

    if errors and not result_data:
        error_lines = [f"  • {e.get('message', str(e))}" for e in errors]
        return "GraphQL errors:\n" + "\n".join(error_lines)

    if not result_data:
        return "GraphQL query returned no data."

    lines: list[str] = []
    lines.append(f"GraphQL Result — {project_name}")
    lines.append("=" * 50)

    for field_name, field_data in result_data.items():
        if isinstance(field_data, list):
            lines.append(f"\n{field_name} ({len(field_data)} results)")
            lines.append("-" * 40)
            for i, item in enumerate(field_data):
                if isinstance(item, dict):
                    _format_graphql_entity(lines, item, indent=2)
                else:
                    lines.append(f"  {item}")
                if i < len(field_data) - 1:
                    lines.append("")
        elif isinstance(field_data, dict):
            lines.append(f"\n{field_name}")
            lines.append("-" * 40)
            _format_graphql_entity(lines, field_data, indent=2)
        elif field_data is None:
            lines.append(f"\n{field_name}: (not found)")
        else:
            lines.append(f"\n{field_name}: {field_data}")

    if errors:
        lines.append("\nWarnings:")
        for e in errors:
            lines.append(f"  • {e.get('message', str(e))}")

    return "\n".join(lines)


def _format_graphql_entity(lines: list[str], entity: dict, indent: int = 0) -> None:
    """Recursively format a GraphQL entity dict as readable text."""
    prefix = " " * indent
    for key, value in entity.items():
        if value is None:
            continue
        if isinstance(value, list):
            if not value:
                continue
            if isinstance(value[0], dict):
                lines.append(f"{prefix}{key}:")
                for sub in value:
                    _format_graphql_entity(lines, sub, indent=indent + 4)
                    lines.append(f"{prefix}    ---")
                if lines[-1].endswith("---"):
                    lines.pop()
            else:
                lines.append(f"{prefix}{key}: {', '.join(str(v) for v in value)}")
        elif isinstance(value, dict):
            lines.append(f"{prefix}{key}:")
            _format_graphql_entity(lines, value, indent=indent + 4)
        else:
            lines.append(f"{prefix}{key}: {value}")


# ── HTTP helpers ──────────────────────────────────────────────────────────

def _base_url(mode: str) -> str:
    """Resolve the OntoBricks REST API base URL for the given mode."""
    if mode == "mounted":
        port = os.getenv("DATABRICKS_APP_PORT", "8000")
        return f"http://localhost:{port}"
    return os.getenv("ONTOBRICKS_URL", "http://localhost:8000")


def _get_auth_headers(mode: str) -> dict:
    """Get authorization headers for the target OntoBricks app.

    In ``databricks`` mode the app's service principal obtains a fresh
    M2M OAuth token.  The token is cached for ``_OAUTH_TOKEN_TTL``
    seconds to avoid hitting the token endpoint on every request.

    Strategy (in order):
    1. Direct OIDC client-credentials grant using ``DATABRICKS_CLIENT_ID``
       / ``DATABRICKS_CLIENT_SECRET`` (most reliable in Apps runtime).
    2. Databricks SDK ``WorkspaceClient().config.authenticate()`` fallback.
    """
    if mode != "databricks":
        return {}

    now = time.time()
    if _oauth_cache["token"] and (now - _oauth_cache["ts"]) < _OAUTH_TOKEN_TTL:
        return {"Authorization": f"Bearer {_oauth_cache['token']}"}

    # --- Strategy 1: direct M2M OAuth via OIDC endpoint ---
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "")
    host = os.getenv("DATABRICKS_HOST", "")

    if client_id and client_secret and host:
        try:
            h = host.strip().rstrip("/")
            if not h.startswith("http"):
                h = f"https://{h}"
            token_url = f"{h}/oidc/v1/token"
            logger.info("Requesting M2M OAuth token from %s", token_url)
            with httpx.Client(timeout=10) as c:
                resp = c.post(
                    token_url,
                    data={"grant_type": "client_credentials", "scope": "all-apis"},
                    auth=(client_id, client_secret),
                )
                resp.raise_for_status()
                token = resp.json()["access_token"]
            _oauth_cache["token"] = token
            _oauth_cache["ts"] = time.time()
            logger.info("M2M OAuth token obtained and cached (%d chars)", len(token))
            return {"Authorization": f"Bearer {token}"}
        except Exception as exc:
            logger.warning("M2M OAuth token request failed: %s", exc, exc_info=True)
    else:
        logger.info(
            "M2M OAuth env vars not all set (client_id=%s, client_secret=%s, host=%s)",
            bool(client_id), bool(client_secret), bool(host),
        )

    # --- Strategy 2: Databricks SDK header factory ---
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        result = w.config.authenticate()

        headers: dict = {}
        if isinstance(result, dict) and result:
            headers = result
        elif callable(result):
            try:
                out = result()
                if isinstance(out, dict) and out:
                    headers = out
            except TypeError:
                buf: dict = {}
                result(buf)
                if buf:
                    headers = buf

        if headers:
            logger.info("Auth headers obtained via SDK (%s)",
                        ", ".join(headers.keys()))
            auth_val = headers.get("Authorization", "")
            if auth_val.startswith("Bearer "):
                _oauth_cache["token"] = auth_val[7:]
                _oauth_cache["ts"] = time.time()
            return headers
    except Exception as exc:
        logger.warning("SDK auth fallback failed: %s", exc, exc_info=True)

    logger.error("Could not obtain any Databricks auth token (mode=%s)", mode)
    return {}


async def _get(client: httpx.AsyncClient, path: str,
               params: dict | None = None) -> dict:
    resp = await client.get(path, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


# ── Factory ───────────────────────────────────────────────────────────────

def create_mcp_server(mode: str = "standalone") -> FastMCP:
    """Build a configured :class:`FastMCP` instance.

    Args:
        mode: ``"databricks"`` | ``"standalone"`` | ``"mounted"``.
    """
    base = _base_url(mode)
    logger.info("Creating MCP server — mode=%s, base_url=%s", mode, base)

    _selected_project: dict = {"name": None}
    _registry: dict = {"catalog": "", "schema": "", "volume": "OntoBricksRegistry", "_loaded": False}

    def _client() -> httpx.AsyncClient:
        """Create an httpx client with base URL and auth headers."""
        headers = _get_auth_headers(mode)
        return httpx.AsyncClient(base_url=base, headers=headers)

    async def _ensure_registry() -> dict:
        """Resolve registry config: volume path → env vars → main app API."""
        if _registry["_loaded"]:
            return _registry

        vol_path = os.getenv("REGISTRY_VOLUME_PATH", "")
        if vol_path:
            parts = vol_path.strip("/").split("/")
            if len(parts) >= 4 and parts[0].lower() == "volumes":
                _registry["catalog"] = parts[1]
                _registry["schema"] = parts[2]
                _registry["volume"] = parts[3]
                _registry["_loaded"] = True
                logger.info("Registry from volume resource: %s.%s.%s",
                            _registry["catalog"], _registry["schema"], _registry["volume"])
                return _registry
            logger.warning("Cannot parse REGISTRY_VOLUME_PATH '%s'", vol_path)

        env_cat = os.getenv("REGISTRY_CATALOG", "")
        env_sch = os.getenv("REGISTRY_SCHEMA", "")
        env_vol = os.getenv("REGISTRY_VOLUME", "")

        if env_cat and env_sch:
            _registry["catalog"] = env_cat
            _registry["schema"] = env_sch
            _registry["volume"] = env_vol or "OntoBricksRegistry"
            _registry["_loaded"] = True
            logger.info("Registry from env vars: %s.%s.%s",
                        _registry["catalog"], _registry["schema"], _registry["volume"])
            return _registry

        try:
            async with _client() as client:
                data = await _get(client, API_V1_DT_REGISTRY)
            _registry["catalog"] = data.get("catalog", "")
            _registry["schema"] = data.get("schema", "")
            _registry["volume"] = data.get("volume", "OntoBricksRegistry")
            _registry["_loaded"] = True
            logger.info("Registry from main app: %s.%s.%s",
                        _registry["catalog"], _registry["schema"], _registry["volume"])
        except Exception as exc:
            logger.warning("Could not fetch registry config: %s", exc)
        return _registry

    def _registry_params() -> dict:
        """Build registry query params from cached registry config."""
        params: dict = {}
        if _registry["catalog"]:
            params["registry_catalog"] = _registry["catalog"]
        if _registry["schema"]:
            params["registry_schema"] = _registry["schema"]
        if _registry["volume"] and _registry["volume"] != "OntoBricksRegistry":
            params["registry_volume"] = _registry["volume"]
        return params

    def _project_params(extra: dict | None = None) -> dict:
        """Build query params, injecting project_name and registry when set."""
        params = _registry_params()
        if extra:
            params.update(extra)
        if _selected_project["name"]:
            params["project_name"] = _selected_project["name"]
        return params

    mcp = FastMCP(
        "OntoBricks",
        instructions=(
            "You are connected to OntoBricks: project registry + Digital Twin "
            "(triple store) over external REST at /api/v1.\n"
            "Workflow:\n"
            "1. Call 'list_projects' to see available projects.\n"
            "2. Optionally call 'list_project_versions' or 'get_design_status' "
            "to inspect versions or design readiness (ontology, mappings, build_ready).\n"
            "3. Call 'select_project' with the project name that best matches "
            "the user's question.\n"
            "4. Use 'list_entity_types' and 'describe_entity' for exploration, "
            "or GraphQL tools for typed queries.\n\n"
            "**GraphQL** (after select_project): 'get_graphql_schema', then "
            "'query_graphql' for nested relationships.\n\n"
            "Always select a project before entity/triple/GraphQL queries. "
            "If the user's question maps clearly to one project, select it automatically."
        ),
    )

    # ── Tools — Project selection ─────────────────────────────────────

    @mcp.tool()
    async def list_projects() -> str:
        """List all projects (knowledge graphs) available in the registry.

        Returns each project's name and description so you can choose
        the right one for the user's question.

        Always call this first before any other tool.
        """
        await _ensure_registry()

        async with _client() as client:
            data = await _get(client, API_V1_PROJECTS,
                              params=_registry_params())

        if not data.get("success"):
            return data.get("message", "Could not retrieve projects.")

        projects = data.get("projects", [])
        if not projects:
            return "No projects found in the registry."

        lines: list[str] = []
        lines.append(f"Available Projects ({len(projects)})")
        lines.append("=" * 40)
        for p in projects:
            name = p.get("name", "")
            desc = p.get("description", "")
            lines.append(f"  • {name}")
            if desc:
                lines.append(f"    {desc}")
        lines.append("")

        current = _selected_project["name"]
        if current:
            lines.append(f"Currently selected: {current}")
        else:
            lines.append("No project selected yet — call select_project(<name>) next.")

        return "\n".join(lines)

    @mcp.tool()
    async def list_project_versions(project_name: str) -> str:
        """List registry versions for a project (latest first).

        Uses ``GET /api/v1/project/versions``. Call after ``list_projects``
        to see which versions exist before selecting or building.

        Args:
            project_name: Exact project name as returned by ``list_projects``.
        """
        await _ensure_registry()
        params = _registry_params()
        params["project_name"] = project_name

        async with _client() as client:
            data = await _get(client, API_V1_PROJECT_VERSIONS, params=params)

        if not data.get("success"):
            return data.get("message", "Could not list versions.")

        versions = data.get("versions", [])
        latest = data.get("latest_version", "")
        if not versions:
            return f"No versions returned for '{project_name}'."

        lines = [
            f"Versions — {project_name}",
            "=" * 40,
            f"Latest: {latest}",
            "",
        ]
        for v in versions:
            ver = v.get("version", "")
            tag = " (latest)" if v.get("is_latest") else ""
            lines.append(f"  • {ver}{tag}")
        return "\n".join(lines)

    @mcp.tool()
    async def get_design_status(project_name: Optional[str] = None) -> str:
        """Design pipeline readiness: ontology, metadata, assignment, build_ready.

        Uses ``GET /api/v1/project/design-status``. If ``project_name`` is
        omitted, uses the currently selected project (after ``select_project``).

        Args:
            project_name: Registry project name, or omit to use selected project.
        """
        await _ensure_registry()
        name = project_name or _selected_project["name"]
        if not name:
            return (
                "Provide project_name or call select_project first "
                "to set the active project."
            )
        params = _registry_params()
        params["project_name"] = name

        async with _client() as client:
            data = await _get(client, API_V1_PROJECT_DESIGN_STATUS, params=params)

        if not data.get("success"):
            return data.get("message", "Could not load design status.")

        lines = [f"Design status — {name}", "=" * 40]

        ont = data.get("ontology") or {}
        if ont:
            lines.append(
                f"Ontology:  ready={ont.get('ready', False)} "
                f"has_owl={ont.get('has_owl', False)} "
                f"classes={ont.get('class_count', 0)} "
                f"props={ont.get('property_count', 0)}"
            )

        meta = data.get("metadata") or {}
        if meta:
            lines.append(
                f"Metadata:  tables={meta.get('table_count', 0)} "
                f"ready={meta.get('ready', False)}"
            )

        asn = data.get("assignment") or {}
        if asn:
            lines.append(
                f"Assignment: progress={asn.get('progress_percent', 0)}% "
                f"status={asn.get('status', 'n/a')} "
                f"has_r2rml={asn.get('has_r2rml', False)}"
            )

        lines.append(f"build_ready: {data.get('build_ready', False)}")
        if data.get("message"):
            lines.append(f"Note: {data['message']}")

        return "\n".join(lines)

    @mcp.tool()
    async def select_project(project_name: str) -> str:
        """Select a project (knowledge graph) to work with.

        After calling ``list_projects`` to see what is available, call
        this tool with the exact project name. All subsequent calls to
        ``list_entity_types``, ``describe_entity``, and ``get_status``
        will operate on this project's Digital Twin.

        Args:
            project_name: Exact project name as shown by ``list_projects``.
        """
        await _ensure_registry()

        params = _registry_params()
        params["project_name"] = project_name

        async with _client() as client:
            data = await _get(client, API_V1_DT_STATUS,
                              params=params)

        if not data.get("success") and data.get("message"):
            return f"Error selecting project: {data['message']}"

        _selected_project["name"] = project_name

        has_data = data.get("has_data", False)
        count = data.get("count", 0)
        graph_name = data.get("graph_name", "N/A")
        view_table = data.get("view_table", "N/A")

        return (
            f"Project '{project_name}' selected.\n"
            f"View:  {view_table}\n"
            f"Graph: {graph_name}\n"
            f"Data:  {'Yes' if has_data else 'No'} ({count:,} triples)\n\n"
            f"You can now use list_entity_types and describe_entity."
        )

    # ── Tools — Knowledge graph queries ───────────────────────────────

    @mcp.tool()
    async def list_entity_types() -> str:
        """List all entity types available in the selected project's knowledge graph.

        Returns a readable summary of every entity type (rdf:type) present
        in the triple store together with instance counts, plus overall
        statistics (total triples, distinct subjects, etc.).

        A project must be selected first via ``select_project``.
        """
        if not _selected_project["name"]:
            return ("No project selected. Call list_projects first, "
                    "then select_project to choose one.")

        async with _client() as client:
            data = await _get(client, API_V1_DT_STATS,
                              params=_project_params())

        if not data.get("success"):
            return data.get("message", "Could not retrieve statistics.")

        lines: list[str] = []
        lines.append(f"Knowledge Graph — {_selected_project['name']}")
        lines.append("=" * 40)
        lines.append(f"Total triples:       {data.get('total_triples', 0):,}")
        lines.append(f"Distinct entities:   {data.get('distinct_subjects', 0):,}")
        lines.append(f"Distinct predicates: {data.get('distinct_predicates', 0):,}")
        lines.append(f"Labels:              {data.get('label_count', 0):,}")
        lines.append(f"Type assertions:     {data.get('type_assertion_count', 0):,}")
        lines.append(f"Relationships:       {data.get('relationship_count', 0):,}")
        lines.append("")

        entity_types = data.get("entity_types", [])
        if entity_types:
            lines.append("Entity Types")
            lines.append("-" * 40)
            for et in entity_types:
                uri = et.get("uri", "")
                count = et.get("count", 0)
                name = _local_name(uri)
                lines.append(f"  • {name}  ({count:,} instances)")
                lines.append(f"    URI: {uri}")
            lines.append("")

        top_predicates = data.get("top_predicates", [])
        if top_predicates:
            lines.append("Predicates (attributes & relationships)")
            lines.append("-" * 40)
            for tp in top_predicates:
                uri = tp.get("uri", "")
                count = tp.get("count", 0)
                name = _pretty_predicate(uri)
                lines.append(f"  • {name}  ({count:,} usages)")

        return "\n".join(lines)

    @mcp.tool()
    async def describe_entity(
        search: Optional[str] = None,
        entity_type: Optional[str] = None,
        depth: int = MAX_DEPTH,
    ) -> str:
        """Search for an entity and return a full-text description.

        Finds entities matching the search text and/or type in the
        selected project's knowledge graph, then traverses their
        relationships hop-by-hop and returns a human-readable description
        including:
          - Entity identity (name, type, URI)
          - All attributes (e.g. email, phone, city …)
          - All relationships to other entities
          - Related entities discovered at each traversal depth

        A project must be selected first via ``select_project``.
        At least one of ``search`` or ``entity_type`` must be provided.

        Args:
            search: Text to search for in entity names / labels / URIs.
                Example: ``"Jacob Martinez"``, ``"CUST00094"``.
            entity_type: Entity type to filter by (local name,
                case-insensitive). Example: ``"Customer"``, ``"Order"``.
            depth: How many hops to traverse (1 = direct neighbors only,
                default 1, max 10).

        Returns:
            A full-text description of the matching entities, their
            attributes, and their relationships, organized hop by hop.
        """
        if not _selected_project["name"]:
            return ("No project selected. Call list_projects first, "
                    "then select_project to choose one.")
        if not search and not entity_type:
            return "Please provide at least a search term or an entity type."

        params = _project_params({
            "depth": min(max(depth, 1), 10),
            "limit": 500,
            "offset": 0,
        })
        if search:
            params["search"] = search
        if entity_type:
            params["entity_type"] = entity_type

        async with _client() as client:
            data = await _get(client, API_V1_DT_TRIPLES_FIND,
                              params=params)

        return _format_find_response(data)

    @mcp.tool()
    async def get_status() -> str:
        """Check whether the selected project's knowledge graph is ready.

        Returns view name, graph name, whether data exists, and row count.
        Call this if other tools report errors to diagnose configuration issues.

        A project must be selected first via ``select_project``.
        """
        if not _selected_project["name"]:
            return ("No project selected. Call list_projects first, "
                    "then select_project to choose one.")

        async with _client() as client:
            data = await _get(client, API_V1_DT_STATUS,
                              params=_project_params())
        status = data.get("reason") or "OK"
        has_data = data.get("has_data", False)
        count = data.get("count", 0)
        graph_name = data.get("graph_name", "N/A")
        view_table = data.get("view_table", "N/A")
        return (
            f"Project: {_selected_project['name']}\n"
            f"View:    {view_table}\n"
            f"Graph:   {graph_name}\n"
            f"Status:  {status}\n"
            f"Data:    {'Yes' if has_data else 'No'} ({count:,} triples)"
        )

    # ── Tools — GraphQL queries ─────────────────────────────────────

    @mcp.tool()
    async def get_graphql_schema() -> str:
        """Get the GraphQL schema (SDL) for the selected project.

        Returns the auto-generated schema in Schema Definition Language
        format, showing all available types, fields, and relationships.
        Use this to understand what data you can query before calling
        ``query_graphql``.

        A project must be selected first via ``select_project``.
        """
        if not _selected_project["name"]:
            return ("No project selected. Call list_projects first, "
                    "then select_project to choose one.")

        project_name = _selected_project["name"]
        try:
            async with _client() as client:
                resp = await client.get(
                    f"/graphql/{project_name}/schema",
                    params=_registry_params(),
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("GraphQL schema request failed: %s %s", exc.response.status_code, exc)
            body_text = exc.response.text[:500]
            return f"Could not retrieve GraphQL schema ({exc.response.status_code}): {body_text}"
        except Exception as exc:
            logger.warning("GraphQL schema request error: %s", exc)
            return f"Error fetching GraphQL schema: {exc}"

        sdl = data.get("sdl", "")
        if not sdl:
            return "GraphQL schema is empty — the project may have no ontology classes."

        lines: list[str] = []
        lines.append(f"GraphQL Schema — {project_name}")
        lines.append("=" * 50)
        lines.append("")
        lines.append(sdl)
        lines.append("")
        lines.append("Use query_graphql to execute queries against this schema.")
        return "\n".join(lines)

    @mcp.tool()
    async def query_graphql(
        query: str,
        variables: Optional[str] = None,
    ) -> str:
        """Execute a GraphQL query against the selected project's knowledge graph.

        The schema is auto-generated from the project's ontology.
        Call ``get_graphql_schema`` first to discover available types
        and fields.

        This tool is ideal for:
          - Fetching specific fields (no over-fetching)
          - Nested relationship traversal in a single request
          - Filtering and pagination (``limit``, ``offset``, ``search``)

        A project must be selected first via ``select_project``.

        Args:
            query: A valid GraphQL query string.
                Example: ``{ allCustomer(limit: 5) { id label email } }``
            variables: Optional JSON string of query variables.
                Example: ``{"limit": 10}``

        Returns:
            The query result as formatted text, or an error message.
        """
        if not _selected_project["name"]:
            return ("No project selected. Call list_projects first, "
                    "then select_project to choose one.")

        project_name = _selected_project["name"]

        body: dict = {"query": query}
        if variables:
            try:
                body["variables"] = json.loads(variables)
            except json.JSONDecodeError:
                return "Invalid JSON in 'variables' parameter."

        try:
            async with _client() as client:
                resp = await client.post(
                    f"/graphql/{project_name}",
                    json=body,
                    params=_registry_params(),
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("GraphQL query failed: %s %s", exc.response.status_code, exc)
            body_text = exc.response.text[:500]
            return f"GraphQL query failed ({exc.response.status_code}): {body_text}"
        except Exception as exc:
            logger.warning("GraphQL query error: %s", exc)
            return f"Error executing GraphQL query: {exc}"

        return _format_graphql_response(data, project_name)

    # ── Resources ─────────────────────────────────────────────────────

    @mcp.resource("ontobricks://projects")
    async def resource_projects() -> str:
        """List of projects in the registry."""
        async with _client() as client:
            data = await _get(client, API_V1_PROJECTS,
                              params=_registry_params())
        return json.dumps(data, indent=2)

    @mcp.resource("ontobricks://status")
    async def resource_status() -> str:
        """Current triple store configuration and status."""
        async with _client() as client:
            data = await _get(client, API_V1_DT_STATUS,
                              params=_project_params())
        return json.dumps(data, indent=2)

    @mcp.resource("ontobricks://stats")
    async def resource_stats() -> str:
        """Triple store content statistics."""
        async with _client() as client:
            data = await _get(client, API_V1_DT_STATS,
                              params=_project_params())
        return json.dumps(data, indent=2)

    @mcp.resource("ontobricks://graphql-schema")
    async def resource_graphql_schema() -> str:
        """GraphQL schema (SDL) for the selected project."""
        project_name = _selected_project.get("name")
        if not project_name:
            return json.dumps({"error": "No project selected"})
        try:
            async with _client() as client:
                resp = await client.get(
                    f"/graphql/{project_name}/schema",
                    params=_registry_params(),
                    timeout=120,
                )
                resp.raise_for_status()
                return json.dumps(resp.json(), indent=2)
        except Exception as exc:
            logger.warning("GraphQL schema resource error: %s", exc)
            return json.dumps({"error": str(exc)})

    return mcp


# ── Databricks App (combined FastAPI + MCP) ───────────────────────────────

def create_databricks_app():
    """Build the combined FastAPI application for Databricks deployment."""
    from fastapi import FastAPI

    mcp = create_mcp_server(mode="databricks")
    mcp_app = mcp.http_app()
    ontobricks_url = os.getenv("ONTOBRICKS_URL", "http://localhost:8000")

    app = FastAPI(
        title="mcp-ontobricks",
        description="OntoBricks MCP Server — Knowledge graph tools for "
                    "Databricks Playground",
        version="1.0.0",
        lifespan=mcp_app.lifespan,
    )

    @app.get("/", include_in_schema=False)
    async def health():
        vol_path = os.getenv("REGISTRY_VOLUME_PATH", "")
        if vol_path:
            registry_display = vol_path
        else:
            reg_cat = os.getenv("REGISTRY_CATALOG", "")
            reg_sch = os.getenv("REGISTRY_SCHEMA", "")
            reg_vol = os.getenv("REGISTRY_VOLUME", "OntoBricksRegistry")
            registry_display = f"{reg_cat}.{reg_sch}.{reg_vol}" if reg_cat and reg_sch else "auto-discover"
        return {
            "status": "healthy",
            "service": "mcp-ontobricks",
            "ontobricks_url": ontobricks_url,
            "warehouse_id": os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", ""),
            "registry": registry_display,
        }

    combined = FastAPI(
        title="mcp-ontobricks",
        routes=[*mcp_app.routes, *app.routes],
        lifespan=mcp_app.lifespan,
    )

    return combined


combined_app = create_databricks_app()
