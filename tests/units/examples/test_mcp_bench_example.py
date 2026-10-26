"""Unit tests for ``examples/mcp_bench.py`` (MCP benchmark harness, PR #193)."""

from __future__ import annotations

from ._fakes import FakeLLM, FakeMCP, final_response, raw_tool, tool_call_response


class _BoomLLM:
    def chat(self, messages, tools=None):
        raise RuntimeError("endpoint down")


def _usage(prompt: int, completion: int) -> dict:
    return {"prompt_tokens": prompt, "completion_tokens": completion}


class TestRunQuestion:
    def test_records_rounds_tokens_and_tool_calls(self, mcp_bench):
        mcp = FakeMCP({"describe_entity": "E" * 120, "get_status": "ok"})
        llm = FakeLLM([
            tool_call_response("describe_entity", '{"search": "O1"}', usage=_usage(100, 10)),
            tool_call_response("get_status", call_id="c2", usage=_usage(150, 5)),
            final_response("The answer.", usage=_usage(200, 20)),
        ])
        r = mcp_bench.run_question("q", mcp, llm, [raw_tool("describe_entity"), raw_tool("get_status")])
        assert r.error is None
        assert r.rounds == 3
        assert (r.prompt_tokens, r.completion_tokens) == (450, 35)
        assert r.answer_chars == len("The answer.")
        assert [(tc.name, tc.raw_chars, tc.round_idx) for tc in r.tool_calls] == [
            ("describe_entity", 120, 0),
            ("get_status", 2, 1),
        ]
        assert r.total_ms >= r.llm_ms >= 0

    def test_answer_blocks_count_text_only(self, mcp_bench):
        llm = FakeLLM([final_response([
            {"type": "reasoning", "summary": ["long thinking"]},
            {"type": "text", "text": "abc"},
        ])])
        assert mcp_bench.run_question("q", FakeMCP(), llm, []).answer_chars == 3

    def test_raw_size_is_measured_before_compression(self, mcp_bench, mcp_client):
        big = "x" * (mcp_client._DEFAULT_CAP * 2)
        llm = FakeLLM([tool_call_response("get_status"), final_response("ok")])
        r = mcp_bench.run_question("q", FakeMCP({"get_status": big}), llm, [raw_tool("get_status")])
        assert r.tool_calls[0].raw_chars == len(big)
        assert "[capped at" in llm.sent[1]["messages"][-1]["content"]

    def test_baseline_switches_reach_the_llm_request(self, mcp_bench):
        llm = FakeLLM([final_response("ok")])
        mcp_bench.run_question(
            "q", FakeMCP(), llm, [raw_tool("get_status", "Server docstring.")],
            domain=None, graphql_schema=None, compress=False, opt_tools=False,
        )
        sent = llm.sent[0]
        assert sent["tools"][0]["function"]["description"] == "Server docstring."
        assert "(1) list_domains" in sent["messages"][0]["content"]

    def test_llm_failure_is_captured_as_error(self, mcp_bench):
        r = mcp_bench.run_question("q", FakeMCP(), _BoomLLM(), [])
        assert r.error == "endpoint down"
        assert r.rounds == 0

    def test_max_rounds_is_reported(self, mcp_bench):
        llm = FakeLLM([tool_call_response("get_status") for _ in range(12)])
        r = mcp_bench.run_question("q", FakeMCP(), llm, [raw_tool("get_status")])
        assert r.error == "max rounds (12) reached"
        assert r.rounds == 12


class TestReport:
    def _result(self, mcp_bench, *tools, total_ms=1000.0, error=None, prompt=300, completion=10):
        r = mcp_bench.QuestionResult(
            question="q", total_ms=total_ms, rounds=len(tools) + 1, llm_ms=600.0, mcp_ms=300.0,
            prompt_tokens=prompt, completion_tokens=completion, error=error,
        )
        for i, (name, ms, chars) in enumerate(tools):
            r.tool_calls.append(mcp_bench.ToolCall(name=name, call_ms=ms, raw_chars=chars, round_idx=i))
        return r

    def test_unique_tools_keep_first_seen_order(self, mcp_bench):
        r = self._result(mcp_bench, ("b", 1, 1), ("a", 1, 1), ("b", 1, 1))
        assert r.unique_tools == ["b", "a"]

    def test_tool_stats_aggregate_calls(self, mcp_bench):
        stats = mcp_bench._tool_stats([
            self._result(mcp_bench, ("query_graphql", 100, 1000), ("query_graphql", 300, 3000)),
            self._result(mcp_bench, ("get_status", 50, 10)),
        ])
        assert stats["query_graphql"] == {
            "calls": 2, "total_ms": 400, "max_ms": 300, "raw_chars": 4000, "avg_ms": 200,
        }
        assert stats["get_status"]["calls"] == 1

    def test_report_prints_summary_and_hints(self, mcp_bench, capsys):
        results = [
            self._result(mcp_bench, ("query_graphql", 400, 9000), ("describe_entity", 100, 500),
                         ("get_status", 10, 5), ("get_status", 10, 5)),
            self._result(mcp_bench, error="endpoint down"),
        ]
        mcp_bench.print_report(results, "ep", "NONE (baseline)")
        out = capsys.readouterr().out
        assert "questions: 2  success: 1  failed: 1" in out
        assert "ratio=30:1" in out
        assert "token ratio 30:1" in out
        assert "avg 5.0 rounds" in out
        assert "slowest tool: query_graphql" in out
        assert "✗ endpoint down" in out

    def test_report_with_only_failures_skips_stats(self, mcp_bench, capsys):
        mcp_bench.print_report([self._result(mcp_bench, error="boom")], "ep", "opts")
        out = capsys.readouterr().out
        assert "success: 0  failed: 1" in out
        assert "Optimization hints" not in out
