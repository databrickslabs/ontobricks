"""Unit tests for ``examples/mcp_client.py`` (reference MCP client, PR #193)."""

from __future__ import annotations

import ast
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from ._fakes import FakeLLM, FakeMCP, final_response, raw_tool, tool_call_response

MCP_SRC = Path(__file__).resolve().parents[3] / "src" / "mcp-server"


def _server_tool_names() -> set[str]:
    tree = ast.parse((MCP_SRC / "server" / "tools.py").read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for deco in node.decorator_list:
                target = deco.func if isinstance(deco, ast.Call) else deco
                if isinstance(target, ast.Attribute) and target.attr == "tool":
                    names.add(node.name)
    return names


def _graphql_text(entities: list[dict], field: str = "Order", domain: str = "salesdomain") -> str:
    from server.formatting import _format_graphql_response

    return _format_graphql_response({"data": {field: entities}}, domain)


def _orders(n: int, *, special_index: int | None = None, special_customer: str = "Acme") -> list[dict]:
    rows = []
    for i in range(n):
        customer = special_customer if i == special_index else f"Customer{i:02d}"
        rows.append({"id": f"O{i:03d}", "customer": customer, "status": "open"})
    return rows


# ---------------------------------------------------------------------------
# OPT 1 — tool descriptions


class TestToolDescriptions:
    def test_curated_descriptions_cover_every_server_tool(self, mcp_client):
        assert set(mcp_client._TOOL_DESCRIPTIONS) == _server_tool_names()

    def test_size_tables_only_name_real_tools(self, mcp_client):
        tools = _server_tool_names()
        assert set(mcp_client._RESULT_CAPS) <= tools
        assert set(mcp_client._MAX_ROWS) <= tools

    def test_curated_description_replaces_server_docstring(self, mcp_client):
        out = mcp_client.tools_to_openai([raw_tool("describe_entity", "Long\n\ndocstring")])
        fn = out[0]["function"]
        assert out[0]["type"] == "function"
        assert fn["name"] == "describe_entity"
        assert fn["description"] == mcp_client._TOOL_DESCRIPTIONS["describe_entity"]

    def test_uncurated_mode_uses_first_docstring_line(self, mcp_client):
        out = mcp_client.tools_to_openai(
            [raw_tool("describe_entity", "\n  First line.\nSecond line.")], curated=False
        )
        assert out[0]["function"]["description"] == "  First line."

    def test_unknown_tool_falls_back_to_truncated_docstring(self, mcp_client):
        out = mcp_client.tools_to_openai([raw_tool("brand_new_tool", "x" * 300)])
        assert out[0]["function"]["description"] == "x" * 200

    def test_input_schema_is_passed_through_or_defaulted(self, mcp_client):
        schema = {"type": "object", "properties": {"q": {"type": "string"}}}
        with_schema, without = mcp_client.tools_to_openai(
            [raw_tool("query_graphql", schema=schema), raw_tool("get_status")]
        )
        assert with_schema["function"]["parameters"] == schema
        assert without["function"]["parameters"] == {"type": "object", "properties": {}}


# ---------------------------------------------------------------------------
# OPT 2 + 3 — domain warm-up and system prompt


class TestWarmDomainAndPrompt:
    def test_warm_domain_selects_then_preloads_schema(self, mcp_client):
        mcp = FakeMCP({"get_graphql_schema": "type Order { id: ID! }"})
        schema = mcp_client.warm_domain(mcp, "salesdomain")
        assert mcp.calls == [
            ("select_domain", {"domain_name": "salesdomain"}),
            ("get_graphql_schema", {}),
        ]
        assert schema == "type Order { id: ID! }"

    def test_warm_domain_without_schema_preload(self, mcp_client):
        mcp = FakeMCP()
        assert mcp_client.warm_domain(mcp, "salesdomain", preload_schema=False) is None
        assert [name for name, _ in mcp.calls] == ["select_domain"]

    def test_prompt_with_preselected_domain_skips_warm_up(self, mcp_client):
        prompt = mcp_client.build_system_prompt("salesdomain", None)
        assert "Domain already selected: **salesdomain**" in prompt
        assert "Do NOT call list_domains or select_domain" in prompt
        assert "Workflow:" not in prompt
        assert "GraphQL schema" not in prompt

    def test_prompt_without_domain_keeps_warm_up_workflow(self, mcp_client):
        prompt = mcp_client.build_system_prompt(None, None)
        assert "(1) list_domains, (2) select_domain" in prompt

    def test_prompt_injects_preloaded_schema(self, mcp_client):
        prompt = mcp_client.build_system_prompt("salesdomain", "type Order { id: ID! }")
        assert "do NOT call get_graphql_schema" in prompt
        assert "```graphql\ntype Order { id: ID! }\n```" in prompt


# ---------------------------------------------------------------------------
# OPT 5 — entity-block summarization against the real server format


class TestSummarizeGraphql:
    def test_keeps_matching_entity_and_reports_counts(self, mcp_client):
        text = _graphql_text(_orders(20, special_index=13))
        out = mcp_client._summarize_graphql(text, "What did Acme order?", max_rows=3)
        assert "customer: Acme" in out
        assert "Order (20 results)  [showing 3 of 20]" in out
        assert out.startswith("GraphQL Result — salesdomain")

    def test_ties_keep_earliest_blocks_in_original_order(self, mcp_client):
        text = _graphql_text(_orders(10, special_index=7))
        out = mcp_client._summarize_graphql(text, "Acme open orders", max_rows=3)
        kept = [line.split(": ")[1] for line in out.splitlines() if line.strip().startswith("id:")]
        assert kept == ["O000", "O001", "O007"]

    def test_zero_score_guard_keeps_every_block(self, mcp_client):
        text = _graphql_text(_orders(12))
        out = mcp_client._summarize_graphql(text, "Which orders belong to Zyxw?", max_rows=3)
        assert out.count("id: O") == 12
        assert "[showing" not in out

    def test_small_result_is_kept_whole(self, mcp_client):
        text = _graphql_text(_orders(3, special_index=1))
        out = mcp_client._summarize_graphql(text, "Acme", max_rows=8)
        assert out.count("id: O") == 3

    def test_each_section_is_summarized_separately(self, mcp_client):
        from server.formatting import _format_graphql_response

        text = _format_graphql_response(
            {"data": {"Order": _orders(10, special_index=4), "Invoice": _orders(2)}},
            "salesdomain",
        )
        out = mcp_client._summarize_graphql(text, "Acme", max_rows=2)
        assert "Order (10 results)  [showing 2 of 10]" in out
        assert "Invoice (2 results)\n" in out

    def test_text_without_result_sections_is_returned_unchanged(self, mcp_client):
        assert mcp_client._summarize_graphql("GraphQL errors:\n  • boom", "q", 3) == (
            "GraphQL errors:\n  • boom"
        )


class TestCompressResult:
    def test_disabled_returns_text_untouched(self, mcp_client):
        text = "x" * 50_000
        assert mcp_client.compress_result("query_graphql", text, enabled=False) == text

    def test_hard_cap_applies_per_tool(self, mcp_client):
        cap = mcp_client._RESULT_CAPS["list_entity_types"]
        out = mcp_client.compress_result("list_entity_types", "y" * (cap + 500))
        assert out.startswith("y" * cap)
        assert out.endswith(f"[capped at {cap} of {cap + 500} chars]")

    def test_unknown_tool_uses_default_cap(self, mcp_client):
        cap = mcp_client._DEFAULT_CAP
        assert len(mcp_client.compress_result("get_status", "z" * cap)) == cap
        assert "[capped at" in mcp_client.compress_result("get_status", "z" * (cap + 1))

    def test_large_graphql_result_is_summarized_before_the_cap(self, mcp_client):
        text = _graphql_text(_orders(200, special_index=150))
        out = mcp_client.compress_result("query_graphql", text, question="Show Acme orders")
        assert "customer: Acme" in out
        assert "[showing 8 of 200]" in out
        assert len(out) <= mcp_client._RESULT_CAPS["query_graphql"] + 60

    def test_short_graphql_result_skips_summarization(self, mcp_client):
        text = _graphql_text(_orders(10, special_index=9))
        assert len(text) < 2000
        assert mcp_client.compress_result("query_graphql", text, question="Acme") == text


# ---------------------------------------------------------------------------
# Agent loop


class TestAsk:
    def test_tool_round_then_answer(self, mcp_client):
        mcp = FakeMCP({"describe_entity": "Order O001\n  customer: Acme"})
        llm = FakeLLM([
            tool_call_response("describe_entity", '{"search": "O001"}'),
            final_response("O001 belongs to Acme."),
        ])
        answer = mcp_client.ask(
            "Who placed O001?", mcp, llm, [raw_tool("describe_entity", "doc")], domain="salesdomain"
        )
        assert answer == "O001 belongs to Acme."
        assert mcp.calls == [("describe_entity", {"search": "O001"})]
        second_round = llm.sent[1]["messages"]
        assert second_round[-1] == {
            "role": "tool", "tool_call_id": "c1", "content": "Order O001\n  customer: Acme",
        }
        assert "Domain already selected" in second_round[0]["content"]

    def test_reasoning_blocks_are_stripped_from_history(self, mcp_client):
        content = [{"type": "reasoning", "summary": ["thinking"]}, {"type": "text", "text": "Looking up."}]
        llm = FakeLLM([
            tool_call_response("get_status", content=content),
            final_response("done"),
        ])
        mcp_client.ask("status?", FakeMCP(), llm, [raw_tool("get_status")])
        assistant = llm.sent[1]["messages"][2]
        assert assistant["role"] == "assistant"
        assert assistant["content"] == [{"type": "text", "text": "Looking up."}]

    def test_reasoning_only_content_becomes_none(self, mcp_client):
        llm = FakeLLM([
            tool_call_response("get_status", content=[{"type": "reasoning", "summary": []}]),
            final_response("done"),
        ])
        mcp_client.ask("status?", FakeMCP(), llm, [raw_tool("get_status")])
        assert llm.sent[1]["messages"][2]["content"] is None

    def test_final_answer_given_as_blocks_keeps_text_only(self, mcp_client):
        llm = FakeLLM([final_response([
            {"type": "reasoning", "summary": []},
            {"type": "text", "text": "Answer"},
        ])])
        assert mcp_client.ask("q", FakeMCP(), llm, []) == "Answer"

    @pytest.mark.parametrize("content", [None, "", [{"type": "reasoning", "summary": []}]])
    def test_empty_final_answer(self, mcp_client, content):
        assert mcp_client.ask("q", FakeMCP(), FakeLLM([final_response(content)]), []) == "(no response)"

    def test_tool_results_are_compressed_unless_disabled(self, mcp_client):
        big = "r" * (mcp_client._DEFAULT_CAP + 10)
        for compress, capped in ((True, True), (False, False)):
            llm = FakeLLM([tool_call_response("get_status"), final_response("ok")])
            mcp_client.ask("q", FakeMCP({"get_status": big}), llm, [raw_tool("get_status")], compress=compress)
            assert ("[capped at" in llm.sent[1]["messages"][-1]["content"]) is capped

    def test_opt_tools_switch_controls_curated_descriptions(self, mcp_client):
        tools = [raw_tool("get_status", "Server docstring.")]
        for opt_tools, expected in (
            (True, mcp_client._TOOL_DESCRIPTIONS["get_status"]),
            (False, "Server docstring."),
        ):
            llm = FakeLLM([final_response("ok")])
            mcp_client.ask("q", FakeMCP(), llm, tools, opt_tools=opt_tools)
            assert llm.sent[0]["tools"][0]["function"]["description"] == expected

    def test_stops_after_twelve_rounds(self, mcp_client):
        llm = FakeLLM([tool_call_response("get_status") for _ in range(12)])
        assert mcp_client.ask("q", FakeMCP(), llm, [raw_tool("get_status")]) == "(max rounds reached)"
        assert len(llm.sent) == 12


# ---------------------------------------------------------------------------
# Transport: MCP streamable HTTP, Databricks LLM, CLI token


def _mcp_with_transport(mcp_client, handler):
    client = mcp_client.MCPClient("https://mcp.example/mcp", "tok")
    client._http.close()
    client._http = httpx.Client(transport=httpx.MockTransport(handler))
    return client


class TestMCPClientTransport:
    def test_sse_result_is_parsed_and_session_id_reused(self, mcp_client):
        seen = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            seen.append((body.get("method"), request.headers.get("Mcp-Session-Id")))
            payload = {"jsonrpc": "2.0", "id": body.get("id"), "result": {"tools": [{"name": "get_status"}]}}
            return httpx.Response(
                200,
                headers={"Mcp-Session-Id": "sess-1", "Content-Type": "text/event-stream"},
                text=f"event: message\ndata: {json.dumps(payload)}\n\n",
            )

        client = _mcp_with_transport(mcp_client, handler)
        assert client.list_tools() == [{"name": "get_status"}]
        client.list_tools()
        assert seen == [("tools/list", None), ("tools/list", "sess-1")]

    def test_plain_json_body_is_accepted(self, mcp_client):
        client = _mcp_with_transport(
            mcp_client, lambda r: httpx.Response(200, json={"jsonrpc": "2.0", "result": {"ok": 1}})
        )
        assert client._send("ping") == {"ok": 1}

    def test_auth_and_accept_headers_are_sent(self, mcp_client):
        captured = {}

        def handler(request):
            captured.update(request.headers)
            return httpx.Response(200, json={"result": {}})

        _mcp_with_transport(mcp_client, handler)._send("ping")
        assert captured["authorization"] == "Bearer tok"
        assert "text/event-stream" in captured["accept"]

    def test_json_rpc_error_raises(self, mcp_client):
        client = _mcp_with_transport(
            mcp_client, lambda r: httpx.Response(200, json={"error": {"code": -32601, "message": "nope"}})
        )
        with pytest.raises(RuntimeError, match="MCP error"):
            client._send("bogus")

    def test_unparseable_body_returns_none(self, mcp_client):
        client = _mcp_with_transport(mcp_client, lambda r: httpx.Response(200, text="not json"))
        assert client._send("ping") is None

    def test_call_tool_joins_text_parts(self, mcp_client):
        result = {"content": [
            {"type": "text", "text": "line 1"},
            {"type": "image", "data": "..."},
            {"type": "text", "text": "line 2"},
        ]}
        client = _mcp_with_transport(mcp_client, lambda r: httpx.Response(200, json={"result": result}))
        assert client.call_tool("describe_entity", {"search": "x"}) == "line 1\nline 2"

    def test_call_tool_without_text_returns_json_or_placeholder(self, mcp_client):
        client = _mcp_with_transport(
            mcp_client, lambda r: httpx.Response(200, json={"result": {"content": [], "isError": True}})
        )
        assert json.loads(client.call_tool("get_status", {})) == {"content": [], "isError": True}
        empty = _mcp_with_transport(mcp_client, lambda r: httpx.Response(200, json={"result": None}))
        assert empty.call_tool("get_status", {}) == "(no result)"

    def test_initialized_notification_failure_warns(self, mcp_client, capsys):
        def handler(request):
            if json.loads(request.content)["method"] == "notifications/initialized":
                raise httpx.ConnectError("down", request=request)
            return httpx.Response(200, json={"result": {}})

        _mcp_with_transport(mcp_client, handler).initialize()
        assert "WARN: notifications/initialized failed" in capsys.readouterr().err


class TestDatabricksLLM:
    def test_chat_posts_openai_body_to_serving_endpoint(self, mcp_client):
        captured = {}

        def handler(request):
            captured["url"] = str(request.url)
            captured["auth"] = request.headers["authorization"]
            captured["body"] = json.loads(request.content)
            return httpx.Response(200, json=final_response("hi"))

        llm = mcp_client.DatabricksLLM("https://ws.example/", "my-endpoint", "tok")
        llm._http = httpx.Client(transport=httpx.MockTransport(handler))
        tools = [{"type": "function", "function": {"name": "t"}}]
        assert llm.chat([{"role": "user", "content": "q"}], tools=tools) == final_response("hi")
        assert captured["url"] == "https://ws.example/serving-endpoints/my-endpoint/invocations"
        assert captured["auth"] == "Bearer tok"
        assert captured["body"]["model"] == "my-endpoint"
        assert captured["body"]["tools"] == tools
        assert captured["body"]["tool_choice"] == "auto"

    def test_chat_without_tools_omits_tool_choice(self, mcp_client):
        captured = {}

        def handler(request):
            captured.update(json.loads(request.content))
            return httpx.Response(200, json={})

        llm = mcp_client.DatabricksLLM("https://ws.example", "ep", "tok")
        llm._http = httpx.Client(transport=httpx.MockTransport(handler))
        llm.chat([])
        assert "tools" not in captured and "tool_choice" not in captured

    def test_http_error_is_raised(self, mcp_client):
        llm = mcp_client.DatabricksLLM("https://ws.example", "ep", "tok")
        llm._http = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(429)))
        with pytest.raises(httpx.HTTPStatusError):
            llm.chat([])


class TestGetToken:
    def test_reads_access_token_from_cli(self, mcp_client, monkeypatch):
        calls = []

        def fake_run(cmd, **kwargs):
            calls.append(cmd)
            return SimpleNamespace(returncode=0, stdout='{"access_token": "abc"}', stderr="")

        monkeypatch.setattr(subprocess, "run", fake_run)
        assert mcp_client.get_token("dev") == "abc"
        assert calls == [["databricks", "auth", "token", "--profile", "dev"]]

    def test_cli_failure_exits_with_message(self, mcp_client, monkeypatch):
        monkeypatch.setattr(
            subprocess, "run",
            lambda cmd, **kw: SimpleNamespace(returncode=1, stdout="", stderr="not logged in"),
        )
        with pytest.raises(SystemExit, match="not logged in"):
            mcp_client.get_token("dev")
