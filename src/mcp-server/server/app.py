"""
OntoBricks MCP Server

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
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from typing import Callable, Optional

import httpx
from fastmcp import Context, FastMCP

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

logger = logging.getLogger(__name__)

_USER_AGENT = "ontobricks"

# Cached M2M OAuth token (module-level to survive across _get_auth_headers calls)
_oauth_cache: dict = {"token": "", "ts": 0.0}
_OAUTH_TOKEN_TTL = 3000  # refresh well before the typical 3600 s expiry

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

# ── URI helpers ───────────────────────────────────────────────────────────


def _local_name(uri: str) -> str:
    """Extract the human-readable local name from a URI.

    ``https://ontobricks.com/ontology/Customer/CUST00094``  →  ``CUST00094``
    ``http://www.w3.org/1999/02/22-rdf-syntax-ns#type``     →  ``type``
    """
    for sep in ("#", "/"):
        idx = uri.rfind(sep)
        if idx >= 0 and idx < len(uri) - 1:
            return uri[idx + 1 :]
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


def _format_entity_block(
    entity_uri: str,
    triples: list[dict],
    label_or_local: "Callable[[str], str] | None" = None,
) -> str:
    """Build a human-readable text block for one entity."""
    _resolve = label_or_local or _local_name
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
            types.append(_resolve(obj))
        elif _is_label_predicate(pred):
            labels.append(obj)
        elif _is_uri(obj):
            relationships.append((_resolve(pred), _local_name(obj)))
        else:
            attributes.append((_resolve(pred), obj))

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


def _preferred(context_policy: Optional[dict], feature: str) -> bool:
    """True when the domain marked *feature* as preferred.

    Preferred only changes the wording of the follow-up hint the model reads:
    a neutral "you may" mention becomes a directive instruction. Section
    order and payload content are untouched.
    """
    return bool(context_policy) and context_policy.get(feature) == "preferred"


def _hint(
    context_policy: Optional[dict], feature: str, *, directive: str, neutral: str
) -> str:
    """Pick the wording of a follow-up hint for *feature*."""
    return directive if _preferred(context_policy, feature) else neutral


def _format_class_context_block(
    local_id: str, cls_actions: dict, context_policy: Optional[dict] = None
) -> str:
    """Append a [Context] block for a node's class Actions metadata."""
    dataset = cls_actions.get("dataset")
    bridges = cls_actions.get("bridges") or []
    actions = cls_actions.get("actions") or []
    virtual = cls_actions.get("virtualAttributes") or []
    if not dataset and not bridges and not actions and not virtual:
        return ""

    lines: list[str] = []
    lines.append(f"  [Context — class: {cls_actions.get('name', '')}]")

    if dataset and dataset.get("fullName"):
        key_col = dataset.get("key_column")
        if key_col:
            lines.append(f"  Dataset: {dataset['fullName']}  (key: {key_col} = '{local_id}')")
            lines.append(
                _hint(
                    context_policy,
                    "dataset",
                    directive="    → ALWAYS call get_entity_context("
                    "fetch_dataset_rows=True) to retrieve the rows before "
                    "answering — this dataset is the authoritative source for "
                    "this entity",
                    neutral="    → call get_entity_context(fetch_dataset_rows=True) "
                    "to retrieve rows",
                )
            )
        else:
            lines.append(f"  Dataset: {dataset['fullName']}  (key_column not configured)")
        purpose = (dataset.get("description") or "").strip()
        if purpose:
            lines.append(f"  Description: {purpose}")

    if bridges:
        lines.append("  Bridges:")
        for b in bridges:
            target_domain = b.get("target_domain", "")
            target = f"{target_domain} / {b.get('target_class_name', '')}"
            label = f"  \"{b['label']}\"" if b.get("label") else ""
            lines.append(f"    → {target}{label}")
            target_desc = (b.get("target_domain_description") or "").strip()
            if target_desc:
                lines.append(f"      Target domain: {target_desc}")
        lines.append(
            _hint(
                context_policy,
                "bridges",
                directive="    → ALWAYS follow these bridges before concluding: "
                "the answer likely spans the target domain. Call "
                "select_domain(<target_domain>) then re-run describe_entity or "
                "GraphQL there. get_entity_context(follow_bridges=True) only "
                "peeks — it does NOT switch the session.",
                neutral="    → to query the target domain, call "
                "select_domain(<target_domain>) then re-run describe_entity or "
                "GraphQL there. get_entity_context(follow_bridges=True) only "
                "peeks — it does NOT switch the session.",
            )
        )

    if actions:
        lines.append("  Actions:")
        for a in actions:
            lines.append(f"    → {a.get('fullName', '')}: {a.get('description', '')}")
        lines.append(
            _hint(
                context_policy,
                "actions",
                directive="    → PREFER invoke_entity_action(entity_uri, action) "
                "over answering from the graph alone — these actions return "
                "live, authoritative results",
                neutral="    → call invoke_entity_action(entity_uri, action) to run one",
            )
        )

    if virtual:
        names = [
            attr.get("name", "")
            for group in virtual
            for attr in (group.get("attributes") or [])
        ]
        lines.append(
            f"  Virtual attributes ({len(names)}, computed on demand): "
            + ", ".join(n for n in names if n)
        )
        lines.append(
            _hint(
                context_policy,
                "virtual_attributes",
                directive="    → ALWAYS call compute_virtual_attributes("
                "entity_uri) before answering — these values are not in the "
                "graph and are computed live",
                neutral="    → call compute_virtual_attributes(entity_uri) "
                "to compute their values",
            )
        )

    return "\n".join(lines)


def _format_node_context_response(
    data: dict, context_policy: Optional[dict] = None
) -> str:
    """Format the /nodes/context JSON response as LLM-friendly text."""
    if not data.get("success"):
        return data.get("message", "Could not retrieve node context.")

    entity_uri = data.get("entity_uri", "")
    local_id = data.get("entity_local_id", "") or _local_name(entity_uri)
    class_name = data.get("class_name", "Unknown")

    lines: list[str] = [
        f"Node Context — {local_id}  ({class_name})",
        f"URI: {entity_uri}",
        "",
    ]

    dataset = data.get("dataset")
    if dataset:
        lines.append(f"Dataset: {dataset.get('fullName', '')}")
        key_col = dataset.get("key_column")
        if key_col:
            lines.append(f"  Key: {key_col} = '{local_id}'")
        purpose = (dataset.get("description") or "").strip()
        if purpose:
            lines.append(f"  Description: {purpose}")
        if dataset.get("key_column_missing"):
            lines.append("  ⚠ key_column not configured — row fetch skipped")
        rows = dataset.get("rows")
        if rows:
            lines.append(f"  Rows ({len(rows)}):")
            for row in rows:
                lines.append("    " + "  |  ".join(f"{k}: {v}" for k, v in row.items()))
        lines.append("")

    bridges = data.get("bridges") or []
    if bridges:
        lines.append("Cross-domain Bridges:")
        for b in bridges:
            target_domain = b.get("target_domain", "")
            target = f"{target_domain} / {b.get('target_class_name', '')}"
            label = f"  \"{b['label']}\"" if b.get("label") else ""
            lines.append(f"  → {target}{label}")
            target_desc = (b.get("target_domain_description") or "").strip()
            if target_desc:
                lines.append(f"    Target domain: {target_desc}")
            entities = b.get("entities")
            if entities:
                lines.append(f"    Entities ({len(entities)}):")
                for e in entities:
                    lines.append(f"      • {_local_name(e.get('uri', ''))}  {e.get('predicate', '')} → {e.get('object', '')}")
        lines.append(
            _hint(
                context_policy,
                "bridges",
                directive="  → ALWAYS follow these bridges before concluding: "
                "call select_domain(<target_domain>) then describe_entity / "
                "GraphQL there. follow_bridges only peeks; it does not switch "
                "the session.",
                neutral="  → to actually query one of these targets, call "
                "select_domain(<target_domain>) then describe_entity / GraphQL "
                "there. follow_bridges only peeks; it does not switch the session.",
            )
        )
        lines.append("")

    actions = data.get("actions") or []
    if actions:
        lines.append("Actions (Unity Catalog functions):")
        for a in actions:
            lines.append(f"  → {a.get('fullName', '')}")
            desc = (a.get("description") or "").strip()
            if desc:
                lines.append(f"    Description: {desc}")
        lines.append(
            _hint(
                context_policy,
                "actions",
                directive="  → PREFER running one of these actions over answering "
                "from the graph alone — call invoke_entity_action(entity_uri, "
                f"action) with the entity's ID ('{local_id}')",
                neutral="  → call invoke_entity_action(entity_uri, action) with "
                f"the entity's ID ('{local_id}')",
            )
        )
        lines.append("")

    virtual = data.get("virtual_attributes") or []
    if virtual:
        lines.extend(_format_virtual_attribute_lines(virtual, context_policy))
        lines.append("")

    return "\n".join(lines)


def _format_virtual_attribute_lines(
    groups: list, context_policy: Optional[dict] = None
) -> list:
    """Render the virtual attribute block of a node-context response.

    A group carries ``values`` only once computed, so the same block serves
    both the declaration-only listing and the computed one; the trailing hint
    is emitted only while at least one group is still uncomputed, otherwise it
    would ask the model to redo work it just did.
    """
    lines = ["Virtual Attributes (computed on demand):"]
    pending = False
    for group in groups:
        values = group.get("values")
        computed = isinstance(values, dict) and bool(values)
        lines.append(f"  → {group.get('fullName', '')}")
        desc = (group.get("description") or "").strip()
        if desc:
            lines.append(f"    Description: {desc}")
        for attr in group.get("attributes") or []:
            name = attr.get("name", "")
            dtype = attr.get("dataType")
            suffix = f"  ({dtype})" if dtype else ""
            if computed:
                lines.append(f"    {name} = {values.get(name)!r}{suffix}")
            else:
                lines.append(f"    {name}{suffix}  — not computed")
        error = (group.get("error") or "").strip()
        if error:
            lines.append(f"    ⚠ computation failed: {error}")
        message = (group.get("message") or "").strip()
        if message:
            lines.append(f"    {message}")
        if not computed and not error:
            pending = True

    if pending:
        lines.append(
            _hint(
                context_policy,
                "virtual_attributes",
                directive="  → ALWAYS call compute_virtual_attributes("
                "entity_uri) before answering: these values are not stored "
                "in the graph and only a computation can produce them",
                neutral="  → call compute_virtual_attributes(entity_uri) "
                "to compute these values",
            )
        )
    return lines


def _format_virtual_attributes_response(
    data: dict, context_policy: Optional[dict] = None
) -> str:
    """Format the dedicated virtual-attribute compute endpoint for LLMs."""
    if not data.get("success"):
        return (
            data.get("message")
            or (data.get("error") if isinstance(data.get("error"), str) else None)
            or "Could not compute virtual attributes."
        )

    entity_uri = data.get("entity_uri", "")
    local_id = data.get("entity_local_id", "") or _local_name(entity_uri)
    class_name = data.get("class_name", "Unknown")

    lines: list[str] = [
        f"Virtual Attributes — {local_id}  ({class_name})",
        f"URI: {entity_uri}",
        "",
    ]
    virtual = data.get("virtual_attributes") or []
    if virtual:
        lines.extend(_format_virtual_attribute_lines(virtual, context_policy))
    else:
        lines.append("No virtual attributes declared on this class.")
    return "\n".join(lines)


def _format_node_action_response(data: dict) -> str:
    """Format the /nodes/action JSON response as LLM-friendly text."""
    if not data.get("success"):
        return (
            data.get("message")
            or (data.get("error") if isinstance(data.get("error"), str) else None)
            or "Could not invoke the action."
        )

    local_id = data.get("entity_local_id", "")
    lines: list[str] = [
        f"Action: {data.get('action', '')}",
        f"Entity: {local_id}  ({data.get('class_name', 'Unknown')})",
        "",
    ]

    rows = data.get("rows") or []
    if not rows:
        lines.append("Completed — no result rows returned.")
        return "\n".join(lines)

    lines.append(f"Result ({len(rows)} row{'s' if len(rows) != 1 else ''}):")
    for row in rows:
        lines.append("  " + "  |  ".join(f"{k}: {v}" for k, v in row.items()))
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


def _format_find_response(
    data: dict,
    label_or_local: "Callable[[str], str] | None" = None,
    class_actions: "dict | None" = None,
    context_policy: Optional[dict] = None,
) -> str:
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
    parts.append(
        f"Found {seed_count} matching entit{'y' if seed_count == 1 else 'ies'} "
        f"({total} triples across {unique_entities} entities, depth={depth})\n"
    )

    parts.append("── Matching Entities ──")
    for uri in seed_uris:
        block = _format_entity_block(uri, by_subject.get(uri, []), label_or_local)
        parts.append(block)
        # Append class Actions context if available
        if class_actions:
            triples_for_uri = by_subject.get(uri, [])
            type_uris = [t["object"] for t in triples_for_uri if t["predicate"] == RDF_TYPE]
            for type_uri in type_uris:
                if type_uri in class_actions:
                    ctx = _format_class_context_block(
                        _local_name(uri), class_actions[type_uri], context_policy
                    )
                    if ctx:
                        parts.append(ctx)
                    break
        parts.append("")

    if related_uris:
        parts.append("── Related Entities (neighbors) ──")
        for uri in related_uris:
            parts.append(_format_entity_block(uri, by_subject.get(uri, []), label_or_local))
            parts.append("")

    if total > len(triples):
        parts.append(
            f"(Showing {len(triples)} of {total} triples — "
            f"increase limit or use pagination for more)"
        )

    return "\n".join(parts)


def _format_graphql_response(data: dict, domain_name: str) -> str:
    """Convert a GraphQL JSON response into LLM-friendly text."""
    errors = data.get("errors")
    result_data = data.get("data")

    if errors and not result_data:
        error_lines = [f"  • {e.get('message', str(e))}" for e in errors]
        return "GraphQL errors:\n" + "\n".join(error_lines)

    if not result_data:
        return "GraphQL query returned no data."

    lines: list[str] = []
    lines.append(f"GraphQL Result — {domain_name}")
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
        logger.debug("Auth: mode=%s, no headers attached", mode)
        return {}

    now = time.time()
    if _oauth_cache["token"] and (now - _oauth_cache["ts"]) < _OAUTH_TOKEN_TTL:
        age = int(now - _oauth_cache["ts"])
        logger.debug(
            "Auth: reusing cached M2M token (age=%ds, ttl=%ds)",
            age,
            _OAUTH_TOKEN_TTL,
        )
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
            with httpx.Client(timeout=10, headers={"User-Agent": _USER_AGENT}) as c:
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
            bool(client_id),
            bool(client_secret),
            bool(host),
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
            logger.info("Auth headers obtained via SDK (%s)", ", ".join(headers.keys()))
            auth_val = headers.get("Authorization", "")
            if auth_val.startswith("Bearer "):
                _oauth_cache["token"] = auth_val[7:]
                _oauth_cache["ts"] = time.time()
            return headers
    except Exception as exc:
        logger.warning("SDK auth fallback failed: %s", exc, exc_info=True)

    logger.error("Could not obtain any Databricks auth token (mode=%s)", mode)
    return {}


_RETRYABLE_STATUSES = {502, 503}
_RETRY_DELAYS = (2, 5, 10)  # seconds between successive attempts (3 retries)


def _retryable(status: int) -> bool:
    return status in _RETRYABLE_STATUSES


def _retry_delays_for(client: httpx.AsyncClient) -> list[int]:
    """Retry schedule for *client*.

    502/503 retries exist to ride out Databricks Apps cold-start / proxy
    transients on the *remote* app hop. When we talk to a same-host app
    (``mounted`` mode, ``localhost``) there is no proxy in front, so a
    5xx is a real error — retrying only stacks latency. Disable retries
    there.
    """
    if "localhost" in str(client.base_url) or "127.0.0.1" in str(client.base_url):
        return []
    return list(_RETRY_DELAYS)


async def _get(
    client: httpx.AsyncClient, path: str, params: dict | None = None
) -> dict:
    """GET *path* on *client* and return the JSON body.

    Logs the full effective URL and response status so deployed-app
    debugging surfaces auth failures, registry overrides, and silent
    empty payloads in the Apps log stream. On non-2xx responses we
    log a body excerpt before re-raising so the caller (and the LLM)
    sees an actionable error instead of a bare ``HTTPStatusError``.

    502/503 responses (Databricks Apps cold-start / proxy transient
    errors) are retried up to 3 times with increasing delays before
    the error is propagated.
    """
    delays = _retry_delays_for(client)
    attempt = 0
    while True:
        logger.info(
            "GET %s%s params=%s (attempt %d)", client.base_url, path, params or {}, attempt + 1
        )
        started = time.monotonic()
        resp = await client.get(path, params=params, timeout=120)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        if resp.status_code >= 400:
            body_excerpt = resp.text[:500].replace("\n", " ") if resp.text else ""
            logger.warning(
                "GET %s%s → %s in %dms body=%r",
                client.base_url,
                path,
                resp.status_code,
                elapsed_ms,
                body_excerpt,
            )
            if _retryable(resp.status_code) and delays:
                delay = delays.pop(0)
                logger.info(
                    "Retrying in %ds (status=%s, attempt %d)…",
                    delay,
                    resp.status_code,
                    attempt + 1,
                )
                await asyncio.sleep(delay)
                attempt += 1
                continue
        else:
            logger.info("GET %s%s → %s in %dms", client.base_url, path, resp.status_code, elapsed_ms)
        resp.raise_for_status()
        return resp.json()


async def _post(
    client: httpx.AsyncClient, path: str, json: dict | None = None
) -> dict:
    """POST *path* on *client* with optional JSON body and return the JSON response.

    502/503 responses are retried up to 3 times with increasing delays.
    """
    delays = _retry_delays_for(client)
    attempt = 0
    while True:
        logger.info("POST %s%s (attempt %d)", client.base_url, path, attempt + 1)
        started = time.monotonic()
        resp = await client.post(path, json=json or {}, timeout=120)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        if resp.status_code >= 400:
            body_excerpt = resp.text[:500].replace("\n", " ") if resp.text else ""
            logger.warning(
                "POST %s%s → %s in %dms body=%r",
                client.base_url,
                path,
                resp.status_code,
                elapsed_ms,
                body_excerpt,
            )
            if _retryable(resp.status_code) and delays:
                delay = delays.pop(0)
                logger.info(
                    "Retrying in %ds (status=%s, attempt %d)…",
                    delay,
                    resp.status_code,
                    attempt + 1,
                )
                await asyncio.sleep(delay)
                attempt += 1
                continue
        else:
            logger.info("POST %s%s → %s in %dms", client.base_url, path, resp.status_code, elapsed_ms)
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
    logger.info(
        "Env snapshot — REGISTRY_VOLUME_PATH=%r REGISTRY_CATALOG=%r "
        "REGISTRY_SCHEMA=%r REGISTRY_VOLUME=%r DATABRICKS_HOST=%r "
        "DATABRICKS_CLIENT_ID=%s DATABRICKS_CLIENT_SECRET=%s "
        "DATABRICKS_SQL_WAREHOUSE_ID=%r",
        os.getenv("REGISTRY_VOLUME_PATH", ""),
        os.getenv("REGISTRY_CATALOG", ""),
        os.getenv("REGISTRY_SCHEMA", ""),
        os.getenv("REGISTRY_VOLUME", ""),
        os.getenv("DATABRICKS_HOST", ""),
        "set" if os.getenv("DATABRICKS_CLIENT_ID") else "unset",
        "set" if os.getenv("DATABRICKS_CLIENT_SECRET") else "unset",
        os.getenv("DATABRICKS_SQL_WAREHOUSE_ID", ""),
    )

    _selected_domain: dict = {"name": None}
    # Per-domain MCP policy, keyed by domain name, as published by
    # ``GET /api/v1/domains``. Filled by ``list_domains`` and lazily by
    # ``_ensure_domain_policies``.
    _domain_policy: dict[str, dict] = {}
    # Per-domain "has a built graph" flag, same provenance as ``_domain_policy``.
    # A domain absent from this map (or mapped True) keeps the full surface; a
    # False value hides every ``GRAPH_TOOLS`` entry for that domain.
    _domain_has_graph: dict[str, bool] = {}
    _ontology_labels: dict[str, str] = {}   # uri/name (lower) → display label
    _class_actions: dict[str, dict] = {}    # class URI → {"dataset": {...}, "bridges": [...]}
    _registry: dict = {
        "catalog": "",
        "schema": "",
        "volume": "OntoBricksRegistry",
        "_loaded": False,
    }

    # Single shared client per server so HTTP keep-alive / the connection
    # pool are reused across tool calls instead of paying a fresh
    # handshake (and, in databricks mode, a fresh MCP-App → OntoBricks-App
    # network hop) on every request.
    _shared_client: dict = {"client": None}

    @asynccontextmanager
    async def _client():
        """Yield the shared httpx client with fresh auth headers.

        Intentionally does **not** close the client on exit — it is
        pooled for the lifetime of the process. Auth headers are
        refreshed per call (the underlying M2M token is itself cached).
        """
        c = _shared_client["client"]
        if c is None or c.is_closed:
            c = httpx.AsyncClient(
                base_url=base,
                headers={"User-Agent": _USER_AGENT},
                timeout=120,
                limits=httpx.Limits(
                    max_keepalive_connections=10, max_connections=20
                ),
            )
            _shared_client["client"] = c
        auth = _get_auth_headers(mode)
        if auth:
            c.headers.update(auth)
        yield c

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
                logger.info(
                    "Registry from volume resource: %s.%s.%s",
                    _registry["catalog"],
                    _registry["schema"],
                    _registry["volume"],
                )
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
            logger.info(
                "Registry from env vars: %s.%s.%s",
                _registry["catalog"],
                _registry["schema"],
                _registry["volume"],
            )
            return _registry

        try:
            async with _client() as client:
                data = await _get(client, API_V1_DT_REGISTRY)
            _registry["catalog"] = data.get("catalog", "")
            _registry["schema"] = data.get("schema", "")
            _registry["volume"] = data.get("volume", "OntoBricksRegistry")
            _registry["_loaded"] = True
            logger.info(
                "Registry from main app: %s.%s.%s",
                _registry["catalog"],
                _registry["schema"],
                _registry["volume"],
            )
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

    def _domain_params(extra: dict | None = None) -> dict:
        """Build query params, injecting domain registry name and registry when set."""
        params = _registry_params()
        if extra:
            params.update(extra)
        if _selected_domain["name"]:
            params["domain_name"] = _selected_domain["name"]
        return params

    def _label_or_local(uri: str) -> str:
        """Return the ontology label for a URI, falling back to its local name."""
        key = _local_name(uri).lower()
        return _ontology_labels.get(uri, _ontology_labels.get(key, _local_name(uri)))

    mcp = FastMCP(
        "OntoBricks",
        instructions=(
            "You are connected to OntoBricks: domain registry + Knowledge Graph "
            "(triple store) over external REST at /api/v1.\n\n"
            "Workflow:\n"
            "1. Call 'list_domains' to see available domains.\n"
            "2. Optionally call 'list_domain_versions' or 'get_design_status' "
            "to inspect versions or design readiness (ontology, mappings, build_ready).\n"
            "3. Call 'select_domain' with the domain name that best matches "
            "the user's question.\n"
            "4. Use 'list_entity_types' and 'describe_entity' for exploration, "
            "or GraphQL tools for typed queries.\n"
            "   Use 'describe_ontology' to read the domain's ontology structure "
            "(classes, attributes, relationships, OWL). It needs no graph, so a "
            "domain published with only an ontology exposes 'describe_ontology' "
            "as its sole domain tool.\n\n"
            "DATA SOURCES — three tools, three different scopes:\n"
            "- 'describe_entity': GROUND TRUTH. Queries the raw triple store "
            "(union of synced data AND inferred/materialised triples). Returns "
            "ALL relationships including those added by reasoning, regardless of "
            "whether their predicate is declared in the ontology schema. "
            "Use this as the PRIMARY tool whenever you need to know what "
            "relationships or attributes an entity has, especially after inference "
            "has been run.\n"
            "- 'query_graphql': Reads the SAME graph store but filtered through "
            "the ontology schema layer. Only predicates declared in the ontology "
            "appear as fields. Inferred/materialised triples whose predicate is "
            "NOT in the ontology schema are silently invisible. Use only for "
            "bulk typed look-ups where you already know the schema covers the data.\n"
            "- 'list_entity_types': Aggregate stats over the full graph store "
            "(union view) — reflects both synced and inferred entity counts.\n\n"
            "DECISION RULE: For any question about a specific entity or its "
            "relationships, always start with 'describe_entity'. Only fall back "
            "to 'query_graphql' for bulk/typed queries after confirming the schema "
            "covers the predicates you need.\n\n"
            "CROSS-DOMAIN BRIDGES: describe_entity and get_entity_context expose "
            "bridges configured on an entity's class. Each bridge carries the "
            "target domain's name AND description, and only MCP-visible targets "
            "are shown. When a bridge is relevant to the user's question, "
            "call select_domain(<target_domain>) to switch — the previously "
            "selected domain is replaced — then describe_entity or GraphQL in "
            "the target. get_entity_context(follow_bridges=True) only PEEKS at "
            "the target graph; it does NOT switch the session, so subsequent "
            "tools stay on the origin domain.\n\n"
            "Always select a domain before entity/triple/GraphQL queries. "
            "If the user's question maps clearly to one domain, select it automatically."
        ),
    )

    async def _ensure_domain_policies() -> dict[str, dict]:
        """Populate the policy cache if a tool ran before ``list_domains``.

        Well-behaved clients call ``list_domains`` first, but nothing forces
        them to, and ``select_domain`` must know the policy to compute the
        tool set. Failures are swallowed: an empty policy means "everything
        exposed", which is the safe pre-policy behaviour.
        """
        if _domain_policy:
            return _domain_policy
        try:
            await _ensure_registry()
            async with _client() as client:
                data = await _get(client, API_V1_DOMAINS, params=_registry_params())
            for d in data.get("domains", []) or []:
                if d.get("name"):
                    _domain_policy[d["name"]] = d.get("mcp_policy") or {}
                    _domain_has_graph[d["name"]] = bool(d.get("has_graph", True))
        except Exception as exc:  # noqa: BLE001
            logger.warning("could not preload domain MCP policies: %s", exc)
        return _domain_policy

    def _active_policy() -> dict:
        """Policy of the currently selected domain (empty when none)."""
        name = _selected_domain.get("name")
        return _domain_policy.get(name, {}) if name else {}

    def _active_context_policy() -> dict:
        """``{feature: mode}`` mapping for the selected domain."""
        return _active_policy().get("context") or {}

    def _disabled_tools(policy: dict) -> set[str]:
        """Configurable tools the policy hides, registry tools excluded."""
        raw = policy.get("disabled_tools")
        if not isinstance(raw, list):
            return set()
        return {t for t in raw if isinstance(t, str)} - REGISTRY_TOOLS

    def _has_graph(name: Optional[str]) -> bool:
        """Whether *name* serves a built graph (default True when unknown)."""
        return _domain_has_graph.get(name, True) if name else True

    def _graph_hidden_for(name: Optional[str]) -> set[str]:
        """Graph tools to hide for *name* — all of them when it has no graph."""
        return set(GRAPH_TOOLS) if not _has_graph(name) else set()

    def _hidden_for(name: Optional[str]) -> set[str]:
        """Full hidden set for *name*: policy-disabled tools + graph tools when
        the domain is ontology-only."""
        return _disabled_tools(_domain_policy.get(name, {})) | _graph_hidden_for(
            name
        )

    def _ensure_tool_allowed(tool_name: str) -> Optional[str]:
        """Return a refusal message when *tool_name* is disabled, else None.

        Hiding a tool from ``tools/list`` is only a hint: a client that
        ignores ``ToolListChangedNotification`` (or cached an older list) can
        still call it. The policy is therefore re-checked on every call.
        """
        name = _selected_domain.get("name")
        if tool_name in _disabled_tools(_active_policy()):
            return (
                f"The tool '{tool_name}' is not available for domain "
                f"'{name}' — its MCP policy does not expose it."
            )
        if tool_name in _graph_hidden_for(name):
            return (
                f"The tool '{tool_name}' is not available for domain "
                f"'{name}' — it is published with an ontology only (no graph). "
                "Use describe_ontology to read its structure."
            )
        return None

    def _require_domain(tool_name: str) -> Optional[str]:
        """Single entry guard for every domain-scoped tool.

        Returns the message to hand back to the model, or None to proceed.
        """
        if not _selected_domain["name"]:
            return (
                "No domain selected. Call list_domains first, "
                "then select_domain to choose one."
            )
        return _ensure_tool_allowed(tool_name)

    def _ensure_context_allowed(feature: str, label: str) -> Optional[str]:
        """Return a refusal message when *feature* is disabled, else None."""
        if _active_context_policy().get(feature) != "disabled":
            return None
        return (
            f"{label} are disabled for domain '{_selected_domain.get('name')}' "
            "by its MCP policy."
        )

    # ── Tools — Domain selection ──────────────────────────────────────

    @mcp.tool()
    async def list_domains() -> str:
        """List all domains (knowledge graphs) available in the registry.

        Returns each domain's name and description so you can choose
        the right one for the user's question.

        Always call this first before any other tool.
        """
        logger.info("Tool list_domains called")
        await _ensure_registry()
        params = _registry_params()
        logger.info(
            "list_domains → calling %s%s with override params=%s",
            base,
            API_V1_DOMAINS,
            params,
        )

        async with _client() as client:
            data = await _get(client, API_V1_DOMAINS, params=params)

        if not data.get("success"):
            return data.get("message", "Could not retrieve domains.")

        domains = data.get("domains", [])
        if not domains:
            return "No domains found in the registry."

        _domain_policy.clear()
        _domain_has_graph.clear()
        for d in domains:
            if d.get("name"):
                _domain_policy[d["name"]] = d.get("mcp_policy") or {}
                _domain_has_graph[d["name"]] = bool(d.get("has_graph", True))

        lines: list[str] = []
        lines.append(f"Available Domains ({len(domains)})")
        lines.append("=" * 40)
        for d in domains:
            name = d.get("name", "")
            desc = d.get("description", "")
            tag = "" if d.get("has_graph", True) else "  (ontology-only)"
            lines.append(f"  • {name}{tag}")
            if desc:
                lines.append(f"    {desc}")
        lines.append("")

        current = _selected_domain["name"]
        if current:
            lines.append(f"Currently selected: {current}")
        else:
            lines.append("No domain selected yet — call select_domain(<name>) next.")

        return "\n".join(lines)

    @mcp.tool()
    async def list_domain_versions(domain_name: str) -> str:
        """List registry versions for a domain (latest first).

        Uses ``GET /api/v1/domain/versions``. Call after ``list_domains``
        to see which versions exist before selecting or building.

        Args:
            domain_name: Exact domain name as returned by ``list_domains``.
        """
        await _ensure_registry()
        params = _registry_params()
        params["domain_name"] = domain_name

        async with _client() as client:
            data = await _get(client, API_V1_DOMAIN_VERSIONS, params=params)

        if not data.get("success"):
            return data.get("message", "Could not list versions.")

        versions = data.get("versions", [])
        latest = data.get("latest_version", "")
        if not versions:
            return f"No versions returned for '{domain_name}'."

        lines = [
            f"Versions — {domain_name}",
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
    async def get_design_status(domain_name: Optional[str] = None) -> str:
        """Design pipeline readiness: ontology, metadata, assignment, build_ready.

        Uses ``GET /api/v1/domain/design-status``. If ``domain_name`` is
        omitted, uses the currently selected domain (after ``select_domain``).

        Args:
            domain_name: Registry domain name, or omit to use selected domain.
        """
        await _ensure_registry()
        name = domain_name or _selected_domain["name"]
        if not name:
            return (
                "Provide domain_name or call select_domain first "
                "to set the active domain."
            )
        params = _registry_params()
        params["domain_name"] = name

        async with _client() as client:
            data = await _get(client, API_V1_DOMAIN_DESIGN_STATUS, params=params)

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
    async def select_domain(domain_name: str, ctx: Context) -> str:
        """Select a domain (knowledge graph) to work with.

        After calling ``list_domains`` to see what is available, call
        this tool with the exact domain name. All subsequent calls to
        ``list_entity_types``, ``describe_entity``, and ``get_status``
        will operate on this domain's Knowledge Graph.

        Each domain publishes its own set of tools, so the tool list is
        recomputed here: tools the domain does not expose disappear for this
        session, and previously hidden ones come back.

        Args:
            domain_name: Exact domain name as shown by ``list_domains``.
        """
        await _ensure_registry()
        await _ensure_domain_policies()

        params = _registry_params()
        params["domain_name"] = domain_name

        async with _client() as client:
            data = await _get(client, API_V1_DT_STATUS, params=params)
            if not data.get("success") and data.get("message"):
                return f"Error selecting domain: {data['message']}"
            _selected_domain["name"] = domain_name
            # Ontology labels are resolved lazily via ``_label_or_local``
            # (local-name fallback). A dedicated read-only label endpoint
            # can repopulate ``_ontology_labels`` here in the future; the
            # previous eager POST hit the legacy UC handler (wrong
            # contract) and only added a wasted round trip.
            _ontology_labels.clear()
            _class_actions.clear()
            # Fetch class Actions for the selected domain — reuse the same client
            try:
                cls_data = await _get(
                    client,
                    API_V1_DOMAIN_CLASSES,
                    params={**_registry_params(), "domain_name": domain_name},
                )
                for cls in cls_data.get("classes", []):
                    uri = cls.get("uri", "")
                    if uri:
                        _class_actions[uri] = {
                            "name": cls.get("name", ""),
                            "dataset": cls.get("dataset") or None,
                            "bridges": cls.get("bridges") or [],
                            "actions": cls.get("actions") or [],
                            "virtualAttributes": cls.get("virtualAttributes") or [],
                        }
                logger.info(
                    "select_domain: loaded class Actions for %d classes", len(_class_actions)
                )
            except Exception as exc:
                logger.warning("select_domain: could not load class Actions: %s", exc)

        # Recompute the session tool set for the new domain. reset_visibility
        # first, otherwise rules accumulate and a tool hidden by a previously
        # selected domain would stay hidden here. An ontology-only domain (no
        # built graph) additionally hides every GRAPH_TOOLS entry, leaving
        # describe_ontology as its sole domain tool.
        hidden = _hidden_for(domain_name)
        try:
            await ctx.reset_visibility()
            if hidden:
                await ctx.disable_components(names=hidden, components={"tool"})
        except Exception as exc:  # noqa: BLE001
            # Visibility is a presentation concern; _ensure_tool_allowed still
            # refuses the calls, so a failure here must not break selection.
            logger.warning("select_domain: could not apply tool visibility: %s", exc)

        has_data = data.get("has_data", False)
        count = data.get("count", 0)
        graph_name = data.get("graph_name", "N/A")
        view_table = data.get("view_table", "N/A")

        lines = [
            f"Domain '{domain_name}' selected.",
            f"View:  {view_table}",
            f"Graph: {graph_name}",
            f"Data:  {'Yes' if has_data else 'No'} ({count:,} triples)",
            "",
        ]
        if hidden:
            lines.append(
                "Not available for this domain: " + ", ".join(sorted(hidden))
            )
            lines.append("")
        if not _has_graph(domain_name):
            lines.append(
                "This domain is published with an ontology only (no graph). "
                "Use describe_ontology to read its structure."
            )
        else:
            lines.append(
                "You can now use list_entity_types and describe_entity."
            )
        return "\n".join(lines)

    # ── Tools — Ontology structure ────────────────────────────────────

    @mcp.tool()
    async def describe_ontology(domain_name: Optional[str] = None) -> str:
        """Describe the selected domain's ontology structure.

        Returns a structured summary (classes and their attachments) plus the
        raw OWL/Turtle document, which carries the full class, attribute and
        relationship (domain/range) detail. This is the ground truth for the
        ontology *schema* — it does not touch the graph store, so it works even
        for an ontology-only domain that has never been built. For such a
        domain this is the only tool available.

        Args:
            domain_name: Registry domain name, or omit to use the selected one.
        """
        name = domain_name or _selected_domain["name"]
        if not name:
            return (
                "No domain selected. Call list_domains first, then "
                "select_domain to choose one (or pass domain_name)."
            )

        await _ensure_registry()
        await _ensure_domain_policies()
        # describe_ontology is never gated by has_graph (it is the fallback for
        # ontology-only domains), but a domain can still hide it via policy.
        if "describe_ontology" in _disabled_tools(_domain_policy.get(name, {})):
            return (
                f"The tool 'describe_ontology' is not available for domain "
                f"'{name}' — its MCP policy does not expose it."
            )
        params = {**_registry_params(), "domain_name": name}

        async with _client() as client:
            owl_data = await _get(client, API_V1_DOMAIN_ONTOLOGY, params=params)
            classes_data = await _get(
                client, API_V1_DOMAIN_CLASSES, params=params
            )

        if not owl_data.get("success"):
            return owl_data.get("message", "Could not load the ontology.")

        base_uri = owl_data.get("base_uri") or "N/A"
        class_count = owl_data.get("class_count", 0)
        property_count = owl_data.get("property_count", 0)
        owl_content = owl_data.get("content", "")

        lines: list[str] = [
            f"Ontology — {name}",
            "=" * 50,
            f"Base URI:   {base_uri}",
            f"Classes:    {class_count}",
            f"Properties: {property_count}",
            "",
        ]

        classes = classes_data.get("classes", []) if isinstance(classes_data, dict) else []
        if classes:
            lines.append("Classes")
            lines.append("-" * 50)
            for cls in classes:
                cls_name = cls.get("name") or _local_name(cls.get("uri", ""))
                tags: list[str] = []
                if cls.get("dataset"):
                    tags.append("dataset")
                if cls.get("bridges"):
                    tags.append(f"{len(cls['bridges'])} bridge(s)")
                if cls.get("actions"):
                    tags.append(f"{len(cls['actions'])} action(s)")
                if cls.get("virtualAttributes"):
                    tags.append(f"{len(cls['virtualAttributes'])} virtual attr")
                suffix = f"  [{', '.join(tags)}]" if tags else ""
                lines.append(f"  • {cls_name}{suffix}")
            lines.append("")

        if owl_content:
            lines.append("OWL (Turtle)")
            lines.append("-" * 50)
            lines.append(owl_content)
        else:
            lines.append("(OWL document unavailable.)")

        return "\n".join(lines)

    # ── Tools — Knowledge graph queries ───────────────────────────────

    @mcp.tool()
    async def list_entity_types() -> str:
        """List all entity types available in the selected domain's knowledge graph.

        Returns a readable summary of every entity type (rdf:type) present
        in the triple store together with instance counts, plus overall
        statistics (total triples, distinct subjects, etc.).

        When a type has a linked external dataset in the ontology, also
        includes the dataset full name and Description.

        A domain must be selected first via ``select_domain``.
        """
        blocked = _require_domain("list_entity_types")
        if blocked:
            return blocked

        async with _client() as client:
            data = await _get(client, API_V1_DT_STATS, params=_domain_params())

        if not data.get("success"):
            return data.get("message", "Could not retrieve statistics.")

        lines: list[str] = []
        lines.append(f"Graph Viewer — {_selected_domain['name']}")
        lines.append("=" * 40)
        inferred = data.get("inferred_triples", 0)
        lines.append(f"Total triples:       {data.get('total_triples', 0):,}")
        lines.append(f"Distinct entities:   {data.get('distinct_subjects', 0):,}")
        lines.append(f"Distinct predicates: {data.get('distinct_predicates', 0):,}")
        lines.append(f"Labels:              {data.get('label_count', 0):,}")
        lines.append(f"Type assertions:     {data.get('type_assertion_count', 0):,}")
        lines.append(f"Relationships:       {data.get('relationship_count', 0):,}")
        if inferred > 0:
            lines.append(
                f"Inferred triples:    {inferred:,}  "
                f"[reasoning output — ONLY visible via describe_entity, "
                f"NOT via query_graphql]"
            )
        lines.append("")

        entity_types = data.get("entity_types", [])
        if entity_types:
            lines.append("Entity Types")
            lines.append("-" * 40)
            for et in entity_types:
                uri = et.get("uri", "")
                count = et.get("count", 0)
                name = _label_or_local(uri)
                lines.append(f"  • {name}  ({count:,} instances)")
                lines.append(f"    URI: {uri}")
                actions = _class_actions.get(uri) or {}
                dataset = actions.get("dataset") or {}
                if dataset.get("fullName"):
                    lines.append(f"    Dataset: {dataset['fullName']}")
                    desc = (dataset.get("description") or "").strip()
                    if desc:
                        lines.append(f"    Description: {desc}")
                for fn_action in actions.get("actions") or []:
                    fn_desc = (fn_action.get("description") or "").strip()
                    suffix = f" — {fn_desc}" if fn_desc else ""
                    lines.append(f"    Action: {fn_action.get('fullName', '')}{suffix}")
            lines.append("")

        top_predicates = data.get("top_predicates", [])
        if top_predicates:
            lines.append("Predicates (attributes & relationships)")
            lines.append("-" * 40)
            for tp in top_predicates:
                uri = tp.get("uri", "")
                count = tp.get("count", 0)
                name = _label_or_local(uri) or _pretty_predicate(uri)
                lines.append(f"  • {name}  ({count:,} usages)")

        return "\n".join(lines)

    @mcp.tool()
    async def describe_entity(
        search: Optional[str] = None,
        entity_type: Optional[str] = None,
        depth: int = MAX_DEPTH,
    ) -> str:
        """Search for an entity and return a full-text description.

        Queries the RAW TRIPLE STORE (union of synced data AND
        inferred/materialised triples added by reasoning). This is the
        GROUND TRUTH tool — it returns ALL triples regardless of the
        ontology schema, including relationships added by inference that
        are not declared as ontology predicates.

        Finds entities matching the search text and/or type in the
        selected domain's knowledge graph, then traverses their
        relationships hop-by-hop and returns a human-readable description
        including:
          - Entity identity (name, type, URI)
          - All attributes (e.g. email, phone, city …)
          - All relationships to other entities, including inferred ones
          - Related entities discovered at each traversal depth
          - Linked external dataset name, key column, and description when
            configured on the ontology class

        Use this as the PRIMARY tool for any question about a specific
        entity. Do NOT rely on ``query_graphql`` alone — it may miss
        inferred/materialised relationships.

        A domain must be selected first via ``select_domain``.
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
        blocked = _require_domain("describe_entity")
        if blocked:
            return blocked
        if not search and not entity_type:
            return "Please provide at least a search term or an entity type."

        params = _domain_params(
            {
                "depth": min(max(depth, 1), 10),
                # Keep the LLM payload tight: 100 triples is plenty to
                # describe an entity + its immediate neighbours, and cuts
                # both backend fetch size and token cost. The backend still
                # reports ``total`` so the model can page for more.
                "limit": 100,
                "offset": 0,
            }
        )
        if search:
            params["search"] = search
        if entity_type:
            params["entity_type"] = entity_type

        async with _client() as client:
            data = await _get(client, API_V1_DT_TRIPLES_FIND, params=params)

        return _format_find_response(
            data,
            _label_or_local,
            class_actions=_class_actions,
            context_policy=_active_context_policy(),
        )

    @mcp.tool()
    async def get_status() -> str:
        """Check whether the selected domain's knowledge graph is ready.

        Returns view name, graph name, whether data exists, and row count.
        Call this if other tools report errors to diagnose configuration issues.

        A domain must be selected first via ``select_domain``.
        """
        blocked = _require_domain("get_status")
        if blocked:
            return blocked

        async with _client() as client:
            data = await _get(client, API_V1_DT_STATUS, params=_domain_params())
        status = data.get("reason") or "OK"
        has_data = data.get("has_data", False)
        count = data.get("count", 0)
        graph_name = data.get("graph_name", "N/A")
        view_table = data.get("view_table", "N/A")
        return (
            f"Domain: {_selected_domain['name']}\n"
            f"View:    {view_table}\n"
            f"Graph:   {graph_name}\n"
            f"Status:  {status}\n"
            f"Data:    {'Yes' if has_data else 'No'} ({count:,} triples)"
        )

    # ── Tools — GraphQL queries ─────────────────────────────────────

    @mcp.tool()
    async def get_graphql_schema() -> str:
        """Get the GraphQL schema (SDL) for the selected domain.

        Returns the auto-generated schema in Schema Definition Language
        format, showing all available types, fields, and relationships.
        Use this to understand what data you can query before calling
        ``query_graphql``.

        A domain must be selected first via ``select_domain``.
        """
        blocked = _require_domain("get_graphql_schema")
        if blocked:
            return blocked

        domain_name = _selected_domain["name"]
        try:
            async with _client() as client:
                resp = await client.get(
                    f"/graphql/{domain_name}/schema",
                    params=_registry_params(),
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "GraphQL schema request failed: %s %s", exc.response.status_code, exc
            )
            body_text = exc.response.text[:500]
            return f"Could not retrieve GraphQL schema ({exc.response.status_code}): {body_text}"
        except Exception as exc:
            logger.warning("GraphQL schema request error: %s", exc)
            return f"Error fetching GraphQL schema: {exc}"

        sdl = data.get("sdl", "")
        if not sdl:
            return "GraphQL schema is empty — the domain may have no ontology classes."

        lines: list[str] = []
        lines.append(f"GraphQL Schema — {domain_name}")
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
        """Execute a GraphQL query against the selected domain's knowledge graph.

        Reads the graph store through the ONTOLOGY SCHEMA layer.
        WARNING: only predicates declared in the ontology appear as
        GraphQL fields. Inferred/materialised triples whose predicate is
        NOT in the ontology schema are silently invisible here.
        Use ``describe_entity`` when you need to see ALL relationships
        including inferred ones.

        The schema is auto-generated from the domain's ontology.
        Call ``get_graphql_schema`` first to discover available types
        and fields.

        This tool is ideal for:
          - Bulk typed look-ups where the schema covers the data you need
          - Fetching specific fields (no over-fetching)
          - Nested relationship traversal in a single request
          - Filtering and pagination (``limit``, ``offset``, ``search``)

        A domain must be selected first via ``select_domain``.

        Args:
            query: A valid GraphQL query string.
                Example: ``{ allCustomer(limit: 5) { id label email } }``
            variables: Optional JSON string of query variables.
                Example: ``{"limit": 10}``

        Returns:
            The query result as formatted text, or an error message.
        """
        blocked = _require_domain("query_graphql")
        if blocked:
            return blocked

        domain_name = _selected_domain["name"]

        body: dict = {"query": query}
        if variables:
            try:
                body["variables"] = json.loads(variables)
            except json.JSONDecodeError:
                return "Invalid JSON in 'variables' parameter."

        try:
            async with _client() as client:
                resp = await client.post(
                    f"/graphql/{domain_name}",
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

        return _format_graphql_response(data, domain_name)

    @mcp.tool()
    async def get_entity_context(
        entity_uri: str,
        fetch_dataset_rows: bool = False,
        dataset_row_limit: int = 5,
        follow_bridges: bool = False,
        compute_virtual_attributes: bool = False,
    ) -> str:
        """Return complete context for an entity node: linked dataset rows,
        cross-domain bridge entities and/or computed virtual attributes.

        Requires a domain to be selected first via select_domain.
        The class must have dataset / bridges configured in the ontology.
        Use the entity URI from describe_entity output.

        When a dataset is linked, the response includes its full name, key
        column, and ontology-authored Description (from the class dataset
        ``description`` field).

        Bridges include the target domain's description so an interrogating
        agent can decide whether to hop. To actually run queries in the
        target domain, call ``select_domain(<target_domain>)`` and then
        ``describe_entity`` / GraphQL there — ``follow_bridges=True`` only
        peeks at the target graph and does NOT switch the selected domain.
        Bridges whose target is not exposed on MCP are omitted from the
        response.

        Virtual attributes are class attributes that are not stored in the
        graph: a Unity Catalog function computes them on demand. They are
        always *listed* so you know what is available, but their values cost a
        warehouse round-trip. When the user asks for a virtual attribute's
        value, call ``compute_virtual_attributes(entity_uri)`` — not this tool
        alone. The ``compute_virtual_attributes=True`` flag here remains for
        callers that want declarations and values in one response.

        Args:
            entity_uri: Full URI of the entity (e.g. from describe_entity).
            fetch_dataset_rows: If true, query the linked UC table/view for rows.
            dataset_row_limit: Max rows to return (1–20, default 5).
            follow_bridges: If true, peek at bridge target domains (read-only —
                does NOT change the selected domain). Prefer ``select_domain``
                for a real hop.
            compute_virtual_attributes: If true, run the class's virtual
                attribute functions and return their values.
        """
        blocked = _require_domain("get_entity_context")
        if blocked:
            return blocked

        # Refuse the argument rather than silently returning nothing, so the
        # model learns the element is off instead of retrying the same call.
        if fetch_dataset_rows:
            blocked = _ensure_context_allowed("dataset", "Datasets")
            if blocked:
                return blocked
        if follow_bridges:
            blocked = _ensure_context_allowed("bridges", "Bridges")
            if blocked:
                return blocked
        if compute_virtual_attributes:
            blocked = _ensure_context_allowed(
                "virtual_attributes", "Virtual attributes"
            )
            if blocked:
                return blocked

        params = _domain_params(
            {
                "entity_uri": entity_uri,
                "fetch_dataset_rows": str(fetch_dataset_rows).lower(),
                "dataset_row_limit": min(max(dataset_row_limit, 1), 20),
                "follow_bridges": str(follow_bridges).lower(),
                "compute_virtual_attributes": str(
                    compute_virtual_attributes
                ).lower(),
            }
        )

        async with _client() as client:
            data = await _get(client, API_V1_DT_NODE_CONTEXT, params=params)

        return _format_node_context_response(data, _active_context_policy())

    @mcp.tool()
    async def compute_virtual_attributes(
        entity_uri: str,
        function: Optional[str] = None,
    ) -> str:
        """Compute one or all virtual attributes declared on an entity's class.

        Requires a domain to be selected first via select_domain. Use this
        tool whenever the user asks about the **value** of a virtual attribute
        — risk score, live standing, distance, and so on. Those values are not
        stored in the knowledge graph; only a Unity Catalog function can
        produce them.

        Discover what is available with ``describe_entity`` or
        ``get_entity_context`` (declarations only). Then call this tool to run
        the bound UC function and read the result. Only functions declared on
        the entity's ontology class can be invoked.

        The function receives exactly one argument server-side: the entity's
        local ID, derived from *entity_uri*. Omit *function* to compute every
        group on the class; pass a fully qualified name
        (``catalog.schema.function``) to compute one group only.

        Args:
            entity_uri: Full URI of the entity (e.g. from describe_entity).
            function: Optional fully qualified UC function name. When omitted,
                every virtual attribute group declared on the class is computed.
        """
        blocked = _require_domain("compute_virtual_attributes")
        if blocked:
            return blocked
        blocked = _ensure_context_allowed(
            "virtual_attributes", "Virtual attributes"
        )
        if blocked:
            return blocked

        params = _domain_params({"entity_uri": entity_uri})
        if function:
            params["function"] = function

        try:
            async with _client() as client:
                data = await _get(
                    client, API_V1_DT_NODE_VIRTUAL_ATTRIBUTES, params=params
                )
        except httpx.HTTPStatusError as exc:
            try:
                err_body = exc.response.json()
            except Exception:
                err_body = {}
            return (
                err_body.get("message")
                or err_body.get("error")
                or (
                    f"Could not compute virtual attributes "
                    f"(HTTP {exc.response.status_code})."
                )
            )

        return _format_virtual_attributes_response(data, _active_context_policy())

    @mcp.tool()
    async def invoke_entity_action(entity_uri: str, action: str) -> str:
        """Run a Unity Catalog function action configured on an entity's class.

        Requires a domain to be selected first via select_domain. Discover the
        available actions with get_entity_context or describe_entity — only
        functions declared on the entity's ontology class can be invoked.

        The function is called with exactly one argument: the entity's local ID,
        derived server-side from *entity_uri*.

        Args:
            entity_uri: Full URI of the entity (e.g. from describe_entity).
            action: Fully qualified function name (catalog.schema.function).
        """
        blocked = _require_domain("invoke_entity_action")
        if blocked:
            return blocked
        # Disabling the actions element also stops invocation, even when the
        # tool itself is still exposed.
        blocked = _ensure_context_allowed("actions", "Actions")
        if blocked:
            return blocked

        body: dict = {"entity_uri": entity_uri, "action_full_name": action}
        body.update(_registry_params())
        body["domain_name"] = _selected_domain["name"]

        try:
            async with _client() as client:
                data = await _post(client, API_V1_DT_NODE_ACTION, json=body)
        except httpx.HTTPStatusError as exc:
            try:
                err_body = exc.response.json()
            except Exception:
                err_body = {}
            return (
                err_body.get("message")
                or err_body.get("error")
                or f"Could not invoke the action (HTTP {exc.response.status_code})."
            )

        return _format_node_action_response(data)

    # ── Resources ─────────────────────────────────────────────────────

    @mcp.resource("ontobricks://domains")
    async def resource_domains() -> str:
        """List of domains in the registry (raw JSON from GET /api/v1/domains)."""
        async with _client() as client:
            data = await _get(client, API_V1_DOMAINS, params=_registry_params())
        return json.dumps(data, indent=2)

    @mcp.resource("ontobricks://status")
    async def resource_status() -> str:
        """Current triple store configuration and status."""
        async with _client() as client:
            data = await _get(client, API_V1_DT_STATUS, params=_domain_params())
        return json.dumps(data, indent=2)

    @mcp.resource("ontobricks://stats")
    async def resource_stats() -> str:
        """Triple store content statistics."""
        async with _client() as client:
            data = await _get(client, API_V1_DT_STATS, params=_domain_params())
        return json.dumps(data, indent=2)

    @mcp.resource("ontobricks://graphql-schema")
    async def resource_graphql_schema() -> str:
        """GraphQL schema (SDL) for the selected domain."""
        domain_name = _selected_domain.get("name")
        if not domain_name:
            return json.dumps({"error": "No domain selected"})
        try:
            async with _client() as client:
                resp = await client.get(
                    f"/graphql/{domain_name}/schema",
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
            registry_display = (
                f"{reg_cat}.{reg_sch}.{reg_vol}"
                if reg_cat and reg_sch
                else "auto-discover"
            )
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
