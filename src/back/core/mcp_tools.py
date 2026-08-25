"""Catalog of the MCP surface a domain can configure, plus policy coercion.

The MCP server (``src/mcp-server/``) is a separate process that only ever
*applies* a policy it receives over REST; it deliberately knows nothing about
this catalog. Everything that needs to enumerate or validate the surface —
the Domain / Information MCP tab, the domain service, the external REST
contract — resolves it here so the two packages cannot drift.

Two independent settings make up a domain's MCP policy:

* **tools** — which MCP tools the domain publishes. The registry-level tools
  in :data:`MCP_REGISTRY_TOOLS` run *before* a domain is selected, so a
  per-domain policy cannot govern them and they are always exposed. Only the
  domain-scoped tools in :data:`MCP_DOMAIN_TOOLS` are configurable.
* **context** — how the ontology attachments (dataset, bridges, actions,
  virtual attributes) are surfaced, via :data:`MCP_CONTEXT_MODES`.

The persisted shape is a single JSONB blob on ``domains.mcp_policy``::

    {"disabled_tools": ["query_graphql"],
     "context": {"dataset": "normal", "bridges": "preferred",
                 "actions": "disabled"}}

Storing *disabled* tools (rather than enabled ones) and treating a missing
context key as ``normal`` means the empty blob ``{}`` reproduces today's
behaviour exactly, so existing domains need no backfill.
"""

from __future__ import annotations

from typing import Any, Dict

# Registry-level tools: usable before ``select_domain`` resolves a domain, so
# no per-domain policy can hide them. Kept as a guard rather than a UI choice.
MCP_REGISTRY_TOOLS: frozenset[str] = frozenset(
    {
        "list_domains",
        "select_domain",
        "list_domain_versions",
        "get_design_status",
    }
)

# Domain-scoped tools, in the order the MCP tab renders them.
MCP_DOMAIN_TOOLS: tuple[Dict[str, str], ...] = (
    {
        "name": "list_entity_types",
        "label": "List entity types",
        "description": "Graph overview: entity types with instance counts and "
        "predicate usage.",
    },
    {
        "name": "describe_entity",
        "label": "Describe entity",
        "description": "Ground-truth lookup of one entity and its neighbours, "
        "including inferred triples.",
    },
    {
        "name": "get_status",
        "label": "Get status",
        "description": "Diagnostic: view table, graph name, triple count.",
    },
    {
        "name": "get_graphql_schema",
        "label": "Get GraphQL schema",
        "description": "Auto-generated GraphQL schema (SDL) for the domain.",
    },
    {
        "name": "query_graphql",
        "label": "Query GraphQL",
        "description": "Typed, schema-filtered queries with nested traversal.",
    },
    {
        "name": "get_entity_context",
        "label": "Get entity context",
        "description": "A node's dataset rows, cross-domain bridges and "
        "available actions.",
    },
    {
        "name": "invoke_entity_action",
        "label": "Invoke entity action",
        "description": "Run a Unity Catalog function declared on the entity's "
        "class.",
    },
)

MCP_DOMAIN_TOOL_NAMES: frozenset[str] = frozenset(
    t["name"] for t in MCP_DOMAIN_TOOLS
)

# Ontology attachments surfaced in the ``[Context]`` block, in render order.
MCP_CONTEXT_FEATURES: tuple[Dict[str, str], ...] = (
    {
        "name": "dataset",
        "label": "Datasets",
        "description": "The Unity Catalog table or view linked to a class.",
    },
    {
        "name": "bridges",
        "label": "Bridges",
        "description": "Cross-domain links declared between ontology classes.",
    },
    {
        "name": "actions",
        "label": "Actions",
        "description": "Unity Catalog functions declared on a class. Disabling "
        "this also refuses invocation, even when the tool stays enabled.",
    },
    {
        "name": "virtual_attributes",
        "label": "Virtual attributes",
        "description": "Class attributes computed on demand by a Unity Catalog "
        "function instead of being mapped. Disabling this also refuses "
        "computation, not just the listing.",
    },
)

MCP_CONTEXT_FEATURE_NAMES: frozenset[str] = frozenset(
    f["name"] for f in MCP_CONTEXT_FEATURES
)

# ``preferred`` only changes the wording of the hint the LLM reads: the line
# becomes directive instead of a neutral mention. It never reorders sections.
MCP_CONTEXT_MODES: tuple[str, ...] = ("preferred", "normal", "disabled")
MCP_CONTEXT_MODE_DEFAULT = "normal"


def coerce_mcp_policy(raw: Any) -> Dict[str, Any]:
    """Normalize a persisted or user-submitted policy blob.

    Unknown tool names, registry-level tools and unknown context features are
    dropped; an unrecognised mode falls back to ``normal``. A malformed blob
    yields ``{}`` rather than raising, so a hand-edited registry row can never
    break domain loading.
    """
    if not isinstance(raw, dict):
        return {}

    disabled = raw.get("disabled_tools")
    tools = sorted(
        {
            name
            for name in (disabled if isinstance(disabled, list) else [])
            if isinstance(name, str) and name in MCP_DOMAIN_TOOL_NAMES
        }
    )

    raw_context = raw.get("context")
    context: Dict[str, str] = {}
    if isinstance(raw_context, dict):
        for name, mode in raw_context.items():
            if name not in MCP_CONTEXT_FEATURE_NAMES:
                continue
            context[name] = (
                mode if mode in MCP_CONTEXT_MODES else MCP_CONTEXT_MODE_DEFAULT
            )

    policy: Dict[str, Any] = {}
    if tools:
        policy["disabled_tools"] = tools
    # Only persist context entries that differ from the implicit default, so a
    # fully default policy stays the empty blob.
    non_default = {
        k: v for k, v in context.items() if v != MCP_CONTEXT_MODE_DEFAULT
    }
    if non_default:
        policy["context"] = non_default
    return policy
