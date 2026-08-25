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
        "get_calls": [],
    }

    async def fake_get(client, path, params=None):
        state["get_calls"].append(path)
        if path == mcp_app.API_V1_DOMAINS:
            return {"success": True, "domains": state["domains"]}
        if path == mcp_app.API_V1_DT_STATUS:
            return {
                "success": True, "has_data": True, "count": 3,
                "graph_name": "g", "view_table": "v",
            }
        if path == mcp_app.API_V1_DOMAIN_CLASSES:
            return {"success": True, "classes": []}
        if path == mcp_app.API_V1_DT_NODE_CONTEXT:
            return state["node_context"]
        return {"success": True}

    async def fake_post(client, path, json=None):
        state["get_calls"].append(path)
        return {"success": True, "entity_uri": "", "action": "", "rows": []}

    monkeypatch.setattr(mcp_app, "_get", fake_get)
    monkeypatch.setattr(mcp_app, "_post", fake_post)
    monkeypatch.setenv("ONTOBRICKS_API_URL", "http://testserver")

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


def test_all_eleven_tools_are_registered(mcp_env) -> None:
    tools, _ = mcp_env
    assert len(tools) == 11


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
