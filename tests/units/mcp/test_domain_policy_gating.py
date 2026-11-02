"""Per-domain MCP tool gating, exercised against a real FastMCP server.

The gating helpers are closures inside ``create_mcp_server``, so these tests
build an actual server and drive its registered tools with a stubbed HTTP
layer. That is also the honest boundary: what matters is what a client
observes after ``select_domain``, not the shape of an internal helper.

Each test gets a fresh server because the selected domain and the policy
cache are per-instance closure state.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_SRC = REPO_ROOT / "src" / "mcp-server"

if str(MCP_SRC) not in sys.path:
    sys.path.insert(0, str(MCP_SRC))


# Policies used across the suite.
_HIDES_GRAPHQL = {"disabled_tools": ["query_graphql"]}
_HIDES_NOTHING: dict = {}
_ACTIONS_OFF = {"context": {"actions": "disabled"}}
_BRIDGES_OFF = {"context": {"bridges": "disabled"}}
_DATASET_OFF = {"context": {"dataset": "disabled"}}
_VIRTUAL_OFF = {"context": {"virtual_attributes": "disabled"}}


class FakeContext:
    """Records the visibility calls ``select_domain`` makes, in order."""

    def __init__(self, explode: bool = False):
        self.calls: list[tuple] = []
        self._explode = explode

    async def reset_visibility(self) -> None:
        if self._explode:
            raise RuntimeError("transport gone")
        self.calls.append(("reset",))

    async def disable_components(self, *, names=None, components=None) -> None:
        if self._explode:
            raise RuntimeError("transport gone")
        self.calls.append(("disable", set(names or ()), set(components or ())))


@pytest.fixture
def mcp_env(monkeypatch):
    """Build a server with the HTTP layer stubbed out.

    Yields ``(tools, state)`` where *tools* maps a tool name to its raw
    function and *state* lets a test change what the fake registry returns.
    """
    try:
        from server import app as mcp_app  # type: ignore[import-not-found]
        from server import http_client as mcp_http  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - env without fastmcp
        pytest.skip(f"MCP server not importable: {exc}")

    state = {
        "domains": [
            {"name": "customer360", "description": "C360", "mcp_policy": _HIDES_GRAPHQL},
            {"name": "finance", "description": "Fin", "mcp_policy": _HIDES_NOTHING},
        ],
        "node_context": {
            "success": True,
            "entity_uri": "https://example.com/Customer/CUST001",
            "entity_local_id": "CUST001",
            "class_name": "Customer",
            "dataset": {"fullName": "main.crm.customers", "key_column": "id"},
        },
        "virtual_attributes": {
            "success": True,
            "entity_uri": "https://example.com/Customer/CUST001",
            "entity_local_id": "CUST001",
            "class_name": "Customer",
            "virtual_attributes": [
                {
                    "fullName": "main.kg.customer_risk",
                    "attributes": [{"name": "risk_score", "dataType": "DOUBLE"}],
                    "values": {"risk_score": 0.82},
                }
            ],
        },
        "shacl_facets": {
            "success": True,
            "entity_uri": "https://example.com/Customer/CUST001",
            "entity_local_id": "CUST001",
            "classes": [{"name": "Customer", "uri": "https://example.com/Customer"}],
            "lineage": ["Customer"],
            "summary": {
                "total": 1, "pass": 0, "fail": 1, "not_applicable": 0,
                "not_evaluated": 0, "error": 0,
                "fail_by_severity": {"sh:Violation": 1, "sh:Warning": 0, "sh:Info": 0},
            },
            "facets": [{
                "shape_id": "s_email", "label": "Customer.email must not be empty",
                "category": "completeness", "severity": "sh:Violation",
                "property_path": "email", "constraint": "sh:minCount 1",
                "status": "fail", "values": [], "values_truncated": False,
                "details": [], "inherited_from": None,
            }],
        },
        # Consumed by ``describe_ontology`` (and ``select_domain`` for the
        # class-action cache). Defaults empty so existing tests are unaffected.
        "classes": [],
        "ontology": {
            "success": True,
            "base_uri": "https://example.com/customer360#",
            "class_count": 2,
            "property_count": 1,
            "content": (
                "@prefix : <https://example.com/customer360#> .\n"
                ":Customer a owl:Class .\n:Contract a owl:Class ."
            ),
        },
        "status": {
            "success": True, "has_data": True, "count": 3,
            "graph_name": "g", "view_table": "v",
        },
        "get_calls": [],
    }

    async def fake_get(client, path, params=None):
        state["get_calls"].append(path)
        if path == mcp_app.API_V1_DOMAINS:
            return {"success": True, "domains": state["domains"]}
        if path == mcp_app.API_V1_DT_STATUS:
            return state["status"]
        if path == mcp_app.API_V1_DOMAIN_CLASSES:
            return {"success": True, "classes": state["classes"]}
        if path == mcp_app.API_V1_DOMAIN_ONTOLOGY:
            return state["ontology"]
        if path == mcp_app.API_V1_DT_NODE_CONTEXT:
            return state["node_context"]
        if path == mcp_app.API_V1_DT_NODE_VIRTUAL_ATTRIBUTES:
            return state["virtual_attributes"]
        if path == getattr(mcp_app, "API_V1_DT_NODE_SHACL", None):
            return state["shacl_facets"]
        return {"success": True}

    async def fake_post(client, path, json=None, params=None):
        state["get_calls"].append(path)
        return {"success": True, "entity_uri": "", "action": "", "rows": []}

    # The tool handlers resolve ``_get`` / ``_post`` through the
    # ``server.http_client`` module object (late binding), so patch them there.
    monkeypatch.setattr(mcp_http, "_get", fake_get)
    monkeypatch.setattr(mcp_http, "_post", fake_post)
    monkeypatch.setenv("ONTOBRICKS_URL", "http://testserver")

    server = mcp_app.create_mcp_server("standalone")
    import asyncio

    listed = asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
        server.list_tools()
    )
    tools = {t.name: t.fn for t in listed}
    return tools, state


async def _select(tools, domain: str, ctx: FakeContext | None = None) -> str:
    ctx = ctx or FakeContext()
    return await tools["select_domain"](domain_name=domain, ctx=ctx)


# ---------------------------------------------------------------------------
# Registration contract


def test_registered_tools_match_the_backend_catalogue(mcp_env) -> None:
    from back.core.mcp_tools import MCP_DOMAIN_TOOL_NAMES, MCP_REGISTRY_TOOLS

    tools, _ = mcp_env
    assert set(tools) == MCP_REGISTRY_TOOLS | MCP_DOMAIN_TOOL_NAMES


def test_select_domain_does_not_leak_ctx_into_the_client_schema(mcp_env) -> None:
    """``ctx`` is injected by FastMCP — a client must never be asked for it."""
    from server.app import create_mcp_server  # type: ignore[import-not-found]
    import asyncio

    server = create_mcp_server("standalone")
    listed = asyncio.get_event_loop_policy().new_event_loop().run_until_complete(
        server.list_tools()
    )
    schema = [t for t in listed if t.name == "select_domain"][0].parameters
    assert set(schema.get("properties", {})) == {"domain_name"}


# ---------------------------------------------------------------------------
# Status success flag


async def test_select_domain_refuses_a_failed_status_without_message(mcp_env) -> None:
    tools, state = mcp_env
    state["status"] = {"success": False}

    text = await _select(tools, "finance")
    assert text.startswith("Error selecting domain")
    assert "No domain selected" in await tools["list_entity_types"]()


async def test_get_status_reports_a_failed_status(mcp_env) -> None:
    tools, state = mcp_env
    await _select(tools, "finance")
    state["status"] = {"success": False, "message": "Graph unreachable"}

    text = await tools["get_status"]()
    assert "Status:  OK" not in text
    assert "Graph unreachable" in text


# ---------------------------------------------------------------------------
# Visibility recomputation


async def test_select_domain_hides_the_disabled_tool(mcp_env) -> None:
    tools, _ = mcp_env
    ctx = FakeContext()
    await _select(tools, "customer360", ctx)

    disables = [c for c in ctx.calls if c[0] == "disable"]
    assert len(disables) == 1
    assert disables[0][1] == {"query_graphql"}
    assert disables[0][2] == {"tool"}


async def test_visibility_is_reset_before_being_narrowed(mcp_env) -> None:
    """Without the reset first, rules accumulate across domain switches."""
    tools, _ = mcp_env
    ctx = FakeContext()
    await _select(tools, "customer360", ctx)

    kinds = [c[0] for c in ctx.calls]
    assert kinds.index("reset") < kinds.index("disable")


async def test_domain_without_a_policy_only_resets(mcp_env) -> None:
    tools, _ = mcp_env
    ctx = FakeContext()
    await _select(tools, "finance", ctx)

    assert [c[0] for c in ctx.calls] == ["reset"]


async def test_switching_domains_restores_a_previously_hidden_tool(mcp_env) -> None:
    tools, _ = mcp_env
    await _select(tools, "customer360")
    assert "not available" in await tools["query_graphql"](query="{ a }")

    await _select(tools, "finance")
    result = await tools["query_graphql"](query="{ a }")
    assert "not available" not in result


async def test_select_domain_reports_the_unavailable_tools(mcp_env) -> None:
    tools, _ = mcp_env
    text = await _select(tools, "customer360")
    assert "Not available for this domain: query_graphql" in text


async def test_selection_survives_a_visibility_transport_failure(mcp_env) -> None:
    """Visibility is presentation; the call-time guard is the real backstop."""
    tools, _ = mcp_env
    await _select(tools, "customer360", FakeContext(explode=True))

    assert "not available" in await tools["query_graphql"](query="{ a }")


# ---------------------------------------------------------------------------
# Registry tools are never governed by a policy


async def test_registry_tools_cannot_be_hidden(mcp_env) -> None:
    tools, state = mcp_env
    state["domains"] = [
        {
            "name": "hostile",
            "description": "",
            "mcp_policy": {
                "disabled_tools": [
                    "select_domain", "list_domains", "list_domain_versions",
                    "get_design_status", "get_status",
                ]
            },
        }
    ]
    ctx = FakeContext()
    await _select(tools, "hostile", ctx)

    disables = [c for c in ctx.calls if c[0] == "disable"]
    assert disables[0][1] == {"get_status"}


async def test_get_design_status_stays_callable_under_a_hostile_policy(
    mcp_env,
) -> None:
    tools, state = mcp_env
    state["domains"] = [
        {
            "name": "hostile",
            "description": "",
            "mcp_policy": {"disabled_tools": ["get_design_status"]},
        }
    ]
    await _select(tools, "hostile")

    assert "not available" not in await tools["get_design_status"]()


# ---------------------------------------------------------------------------
# Call-time guards


async def test_tool_refuses_before_any_domain_is_selected(mcp_env) -> None:
    tools, _ = mcp_env
    assert "No domain selected" in await tools["list_entity_types"]()


async def test_get_status_accepts_domain_name_without_select(mcp_env) -> None:
    """Issue #209: Playground opens a new session per call — pass domain_name."""
    tools, _ = mcp_env
    text = await tools["get_status"](domain_name="finance")
    assert "No domain selected" not in text
    assert "Domain: finance" in text


async def test_get_status_uses_mcp_default_domain_env(mcp_env, monkeypatch) -> None:
    tools, _ = mcp_env
    monkeypatch.setenv("MCP_DEFAULT_DOMAIN", "finance")
    text = await tools["get_status"]()
    assert "Domain: finance" in text


async def test_get_status_auto_picks_the_only_published_domain(mcp_env) -> None:
    tools, state = mcp_env
    state["domains"] = [
        {"name": "finance", "description": "Fin", "mcp_policy": _HIDES_NOTHING},
    ]
    text = await tools["get_status"]()
    assert "Domain: finance" in text


async def test_query_graphql_policy_applies_when_passing_domain_name(mcp_env) -> None:
    tools, _ = mcp_env
    message = await tools["query_graphql"](query="{ a }", domain_name="customer360")
    assert "query_graphql" in message
    assert "customer360" in message
    assert "MCP policy" in message


async def test_refusal_names_the_tool_and_the_domain(mcp_env) -> None:
    tools, _ = mcp_env
    await _select(tools, "customer360")
    message = await tools["query_graphql"](query="{ a }")

    assert "query_graphql" in message
    assert "customer360" in message
    assert "MCP policy" in message


async def test_a_disabled_tool_never_reaches_the_backend(mcp_env) -> None:
    """The guard must run before the HTTP call, not filter its result.

    Uses ``list_entity_types`` because it goes through the stubbed transport,
    so an escaping call is actually observable.
    """
    tools, state = mcp_env
    state["domains"] = [
        {
            "name": "d",
            "description": "",
            "mcp_policy": {"disabled_tools": ["list_entity_types"]},
        }
    ]
    await _select(tools, "d")
    state["get_calls"].clear()

    assert "not available" in await tools["list_entity_types"]()
    assert state["get_calls"] == []


async def test_an_allowed_tool_does_reach_the_backend(mcp_env) -> None:
    """Control for the test above: without a policy the call goes through."""
    tools, state = mcp_env
    await _select(tools, "finance")
    state["get_calls"].clear()

    await tools["list_entity_types"]()
    assert state["get_calls"] != []


async def test_policy_loads_lazily_when_list_domains_was_skipped(mcp_env) -> None:
    """Nothing forces a client to call list_domains first."""
    tools, state = mcp_env
    await _select(tools, "customer360")

    assert mcp_api_domains_fetched(state)
    assert "not available" in await tools["query_graphql"](query="{ a }")


def mcp_api_domains_fetched(state) -> bool:
    from server.app import API_V1_DOMAINS  # type: ignore[import-not-found]

    return API_V1_DOMAINS in state["get_calls"]


# ---------------------------------------------------------------------------
# Context-element guards


async def test_actions_disabled_refuses_invocation_though_tool_is_exposed(
    mcp_env,
) -> None:
    tools, state = mcp_env
    state["domains"] = [
        {"name": "d", "description": "", "mcp_policy": _ACTIONS_OFF}
    ]
    ctx = FakeContext()
    await _select(tools, "d", ctx)

    # The tool itself was never hidden ...
    assert not [c for c in ctx.calls if c[0] == "disable"]
    # ... but the element being off still refuses the call.
    message = await tools["invoke_entity_action"](
        entity_uri="https://example.com/Customer/CUST001",
        action="main.ops.recompute_risk",
    )
    assert "Actions are disabled" in message
    assert "d" in message


async def test_refused_action_never_reaches_the_backend(mcp_env) -> None:
    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": _ACTIONS_OFF}]
    await _select(tools, "d")
    state["get_calls"].clear()

    await tools["invoke_entity_action"](entity_uri="u", action="a")
    assert state["get_calls"] == []


@pytest.mark.parametrize(
    "policy,kwarg,label",
    [
        (_DATASET_OFF, "fetch_dataset_rows", "Datasets"),
        (_BRIDGES_OFF, "follow_bridges", "Bridges"),
        (_VIRTUAL_OFF, "compute_virtual_attributes", "Virtual attributes"),
    ],
)
async def test_get_entity_context_refuses_a_disabled_argument(
    mcp_env, policy, kwarg, label
) -> None:
    """Refuse the argument so the model learns, instead of silently emptying."""
    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": policy}]
    await _select(tools, "d")

    message = await tools["get_entity_context"](
        entity_uri="https://example.com/Customer/CUST001", **{kwarg: True}
    )
    assert f"{label} are disabled" in message


async def test_get_entity_context_allows_the_untouched_argument(mcp_env) -> None:
    """Disabling bridges must not block a dataset row fetch."""
    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": _BRIDGES_OFF}]
    await _select(tools, "d")

    message = await tools["get_entity_context"](
        entity_uri="https://example.com/Customer/CUST001", fetch_dataset_rows=True
    )
    assert "disabled" not in message
    assert "Node Context" in message


async def test_plain_get_entity_context_still_works_with_a_context_policy(
    mcp_env,
) -> None:
    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": _DATASET_OFF}]
    await _select(tools, "d")

    message = await tools["get_entity_context"](
        entity_uri="https://example.com/Customer/CUST001"
    )
    assert "Node Context" in message


async def test_virtual_attributes_disabled_refuses_the_compute_tool(
    mcp_env,
) -> None:
    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": _VIRTUAL_OFF}]
    await _select(tools, "d")

    message = await tools["compute_virtual_attributes"](
        entity_uri="https://example.com/Customer/CUST001"
    )
    assert "Virtual attributes are disabled" in message


async def test_compute_virtual_attributes_reaches_the_backend(mcp_env) -> None:
    tools, state = mcp_env
    await _select(tools, "finance")
    state["get_calls"].clear()

    message = await tools["compute_virtual_attributes"](
        entity_uri="https://example.com/Customer/CUST001"
    )
    from server.app import API_V1_DT_NODE_VIRTUAL_ATTRIBUTES  # type: ignore[import-not-found]

    assert API_V1_DT_NODE_VIRTUAL_ATTRIBUTES in state["get_calls"]
    assert "risk_score = 0.82" in message


async def test_disabled_virtual_attributes_never_reaches_the_backend(mcp_env) -> None:
    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": _VIRTUAL_OFF}]
    await _select(tools, "d")
    state["get_calls"].clear()

    await tools["compute_virtual_attributes"](
        entity_uri="https://example.com/Customer/CUST001"
    )
    assert state["get_calls"] == []


# ---------------------------------------------------------------------------
# Ontology-only domains (has_graph = False)


def _onto_only(name: str = "onto", policy: dict | None = None) -> dict:
    """A published-but-never-built domain: graph tools must all disappear."""
    return {
        "name": name,
        "description": "",
        "mcp_policy": policy or {},
        "has_graph": False,
    }


async def test_ontology_only_domain_hides_every_graph_tool(mcp_env) -> None:
    tools, state = mcp_env
    state["domains"] = [_onto_only()]
    ctx = FakeContext()
    await _select(tools, "onto", ctx)

    from server.app import GRAPH_TOOLS  # type: ignore[import-not-found]

    disables = [c for c in ctx.calls if c[0] == "disable"]
    assert len(disables) == 1
    # Exactly the graph tools are hidden — describe_ontology stays.
    assert disables[0][1] == set(GRAPH_TOOLS)
    assert "describe_ontology" not in disables[0][1]
    assert disables[0][2] == {"tool"}


async def test_ontology_only_selection_message_flags_the_domain(mcp_env) -> None:
    tools, state = mcp_env
    state["domains"] = [_onto_only()]
    text = await _select(tools, "onto")

    assert "ontology only" in text
    assert "describe_ontology" in text


async def test_ontology_only_domain_refuses_a_graph_tool_at_call_time(
    mcp_env,
) -> None:
    """A stale client that calls a hidden graph tool still gets a refusal."""
    tools, state = mcp_env
    state["domains"] = [_onto_only()]
    await _select(tools, "onto", FakeContext(explode=True))
    state["get_calls"].clear()

    message = await tools["describe_entity"](search="CUST001")
    assert "not available" in message
    assert "ontology only" in message
    # The guard runs before any HTTP call.
    assert state["get_calls"] == []


async def test_describe_ontology_is_allowed_on_an_ontology_only_domain(
    mcp_env,
) -> None:
    tools, state = mcp_env
    state["domains"] = [_onto_only()]
    state["classes"] = [
        {"name": "Customer", "uri": "https://example.com/customer360#Customer",
         "dataset": {"fullName": "main.crm.customers"}},
        {"name": "Contract", "uri": "https://example.com/customer360#Contract"},
    ]
    await _select(tools, "onto")

    text = await tools["describe_ontology"]()
    assert "not available" not in text
    assert "Base URI:   https://example.com/customer360#" in text
    assert "Customer" in text and "Contract" in text
    # The dataset attachment is surfaced as a tag ...
    assert "[dataset]" in text
    # ... and the raw OWL rides along.
    assert ":Customer a owl:Class ." in text


async def test_describe_ontology_can_target_a_domain_without_select(mcp_env) -> None:
    tools, state = mcp_env
    state["domains"] = [_onto_only()]
    # No select_domain call: passing domain_name must still work.
    text = await tools["describe_ontology"](domain_name="onto")
    assert "Ontology — onto" in text


async def test_describe_ontology_honours_a_policy_that_hides_it(mcp_env) -> None:
    tools, state = mcp_env
    state["domains"] = [
        {
            "name": "d",
            "description": "",
            "mcp_policy": {"disabled_tools": ["describe_ontology"]},
        }
    ]
    await _select(tools, "d")

    message = await tools["describe_ontology"]()
    assert "not available" in message
    assert "MCP policy" in message


async def test_graph_domain_keeps_its_graph_tools(mcp_env) -> None:
    """Control: a built domain (has_graph defaults True) hides nothing."""
    tools, state = mcp_env
    state["domains"] = [{"name": "built", "description": "", "mcp_policy": {}}]
    ctx = FakeContext()
    await _select(tools, "built", ctx)

    assert not [c for c in ctx.calls if c[0] == "disable"]
    # And a graph tool actually reaches the backend.
    state["get_calls"].clear()
    await tools["list_entity_types"]()
    assert state["get_calls"] != []


# ---------------------------------------------------------------------------
# Entity SHACL facets (.planning/entity-shacl-facets/SPEC.md)

_SHACL_OFF = {"disabled_tools": ["check_entity_shacl_facets"]}


async def test_check_entity_shacl_facets_reaches_the_backend(mcp_env) -> None:
    tools, state = mcp_env
    await _select(tools, "finance")
    state["get_calls"].clear()

    message = await tools["check_entity_shacl_facets"](
        entity_uri="https://example.com/Customer/CUST001"
    )
    from server.app import API_V1_DT_NODE_SHACL  # type: ignore[import-not-found]

    assert state["get_calls"] == [API_V1_DT_NODE_SHACL]
    assert "Customer.email must not be empty" in message


async def test_disabled_shacl_tool_is_hidden_on_select(mcp_env) -> None:
    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": _SHACL_OFF}]
    ctx = FakeContext()
    await _select(tools, "d", ctx)

    disables = [c for c in ctx.calls if c[0] == "disable"]
    assert disables[0][1] == {"check_entity_shacl_facets"}


async def test_disabled_shacl_tool_refuses_at_call_time(mcp_env) -> None:
    """A stale client that still calls the hidden tool gets a refusal, not data."""
    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": _SHACL_OFF}]
    await _select(tools, "d", FakeContext(explode=True))
    state["get_calls"].clear()

    message = await tools["check_entity_shacl_facets"](
        entity_uri="https://example.com/Customer/CUST001"
    )
    assert "not available" in message
    assert state["get_calls"] == []


async def test_entity_context_hints_at_the_shacl_tool(mcp_env) -> None:
    tools, state = mcp_env
    state["node_context"] = {**state["node_context"], "shacl_shape_count": 3}
    await _select(tools, "finance")

    text = await tools["get_entity_context"](
        entity_uri="https://example.com/Customer/CUST001"
    )
    assert "SHACL shapes (3)" in text
    assert "check_entity_shacl_facets(entity_uri)" in text


async def test_entity_context_omits_the_hint_when_the_tool_is_disabled(mcp_env) -> None:
    """Pointing the model at a tool it cannot call wastes a turn."""
    tools, state = mcp_env
    state["domains"] = [{"name": "d", "description": "", "mcp_policy": _SHACL_OFF}]
    state["node_context"] = {**state["node_context"], "shacl_shape_count": 3}
    await _select(tools, "d")

    text = await tools["get_entity_context"](
        entity_uri="https://example.com/Customer/CUST001"
    )
    assert "check_entity_shacl_facets" not in text


def test_shacl_tool_is_a_graph_tool(mcp_env) -> None:
    """It reads the triple store, so an ontology-only domain must hide it."""
    from server.app import GRAPH_TOOLS  # type: ignore[import-not-found]

    assert "check_entity_shacl_facets" in GRAPH_TOOLS
