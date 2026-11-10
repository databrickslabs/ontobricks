"""Engine + tool tests for ``agents.agent_complex_rule_builder``.

``call_serving_endpoint`` is patched to return a scripted sequence of LLM
responses so no real serving endpoint is contacted. We assert on the turn's
``reply`` and the surfaced ``draft`` / ``ready``.
"""

from __future__ import annotations

import json
from unittest.mock import patch

from agents.agent_complex_rule_builder import engine as cr_engine
from agents.agent_complex_rule_builder import tools as cr_tools
from agents.tools.context import ToolContext


_CLASSES = [
    {
        "name": "Customer",
        "uri": "http://ex.org/Customer",
        "label": "Customer",
        "dataProperties": [{"name": "balance"}, {"name": "riskTier"}],
    },
    {"name": "Order", "uri": "http://ex.org/Order", "dataProperties": [{"name": "total"}]},
]
_PROPERTIES = [
    {"name": "placesOrder", "domain": "Customer", "range": "Order", "type": "object"},
]

_QUERY = "CONSTRUCT { ?c a :VIP } WHERE { ?c a :Customer . ?c :balance ?b . FILTER(?b > 1000) }"


def _tool_call_response(name: str, arguments: str = "{}"):
    return {
        "choices": [
            {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {"id": "tc1", "function": {"name": name, "arguments": arguments}}
                    ],
                }
            }
        ],
        "usage": {"prompt_tokens": 100, "completion_tokens": 5},
    }


def _text_response(content: str):
    return {
        "choices": [{"message": {"content": content}}],
        "usage": {"prompt_tokens": 200, "completion_tokens": 50},
    }


def _run(responses, *, user_message="Flag high-balance customers as VIP", current_draft=None):
    with patch.object(cr_engine, "call_serving_endpoint") as mock_llm:
        mock_llm.side_effect = responses
        return cr_engine.run_agent(
            host="https://test.databricks.com",
            token="tok",
            endpoint_name="dbx-llm",
            classes=_CLASSES,
            properties=_PROPERTIES,
            base_uri="http://ex.org/",
            user_message=user_message,
            current_draft=current_draft,
        )


class TestRunAgent:
    def test_update_draft_then_text_surfaces_draft(self):
        update_args = json.dumps(
            {"name": "VIPCustomer", "description": "balance > 1000", "query": _QUERY, "ready": True}
        )
        responses = [
            _tool_call_response("get_ontology_overview"),
            _tool_call_response("update_draft", update_args),
            _text_response("Done — review the query on the right, then Validate and Save."),
        ]
        result = _run(responses)

        assert result.success is True
        assert result.iterations == 3
        assert result.reply.startswith("Done")
        assert result.draft == {
            "name": "VIPCustomer",
            "description": "balance > 1000",
            "query": _QUERY,
        }
        assert result.ready is True
        # usage summed across iterations
        assert result.usage["prompt_tokens"] == 400

    def test_question_only_turn_has_no_draft(self):
        responses = [_text_response("Which class should the rule match?")]
        result = _run(responses)
        assert result.success is True
        assert result.draft is None
        assert result.ready is False

    def test_unchanged_draft_is_not_resurfaced(self):
        existing = {"name": "VIP", "description": "", "query": _QUERY}
        # The LLM rewrites the draft to exactly the same content, then replies.
        update_args = json.dumps({"name": "VIP", "query": _QUERY})
        responses = [
            _tool_call_response("update_draft", update_args),
            _text_response("No change needed; what else?"),
        ]
        result = _run(responses, current_draft=existing)
        assert result.success is True
        assert result.draft is None  # identical to the incoming draft

    def test_existing_draft_is_injected_into_system_prompt(self):
        """Editing an existing rule: the query must reach the model."""
        existing = {"name": "VIP", "description": "big spenders", "query": _QUERY}
        captured = {}

        def _capture(*args, **kwargs):
            # positional signature: (host, token, endpoint_name, messages, ...)
            captured["messages"] = args[3]
            return _text_response("Here's what the current rule does…")

        with patch.object(cr_engine, "call_serving_endpoint", side_effect=_capture):
            cr_engine.run_agent(
                host="h", token="t", endpoint_name="e",
                classes=_CLASSES, properties=_PROPERTIES, base_uri="http://ex.org/",
                user_message="Summarise the current rule.",
                current_draft=existing,
            )

        system_msg = captured["messages"][0]
        assert system_msg["role"] == "system"
        assert _QUERY in system_msg["content"]
        assert "CURRENT DRAFT" in system_msg["content"]

    def test_propose_options_surfaces_on_result(self):
        opts_args = json.dumps(
            {"question": "Match 'Active'?", "options": ["Yes", "No"], "multi": False}
        )
        responses = [
            _tool_call_response("propose_options", opts_args),
            _text_response("Does 'Active' sound right?"),
        ]
        result = _run(responses)
        assert result.success is True
        assert result.options == {
            "question": "Match 'Active'?",
            "options": ["Yes", "No"],
            "multi": False,
        }

    def test_propose_options_multi_is_preserved(self):
        opts_args = json.dumps(
            {"question": "Pick props", "options": ["balance", "riskTier"], "multi": True}
        )
        responses = [
            _tool_call_response("propose_options", opts_args),
            _text_response("Which properties matter?"),
        ]
        result = _run(responses)
        assert result.options["multi"] is True

    def test_no_options_when_tool_unused(self):
        responses = [_text_response("Tell me more about the rule.")]
        result = _run(responses)
        assert result.options is None

    def test_llm_failure_is_reported(self):
        with patch.object(
            cr_engine, "call_serving_endpoint", side_effect=RuntimeError("boom")
        ):
            result = cr_engine.run_agent(
                host="h",
                token="t",
                endpoint_name="e",
                classes=_CLASSES,
                properties=_PROPERTIES,
                base_uri="http://ex.org/",
                user_message="hi",
            )
        assert result.success is False
        assert "boom" in result.error


class TestSystemPrompt:
    def test_prompt_asks_to_review_name_and_description_at_the_end(self):
        prompt = cr_engine.SYSTEM_PROMPT
        assert "rename" in prompt.lower()
        assert "Name" in prompt and "Description" in prompt
        assert "ready=true" in prompt

    def test_prompt_explains_existing_rule_first(self):
        prompt = cr_engine.SYSTEM_PROMPT
        assert "EDITING AN EXISTING RULE" in prompt
        lower = prompt.lower()
        assert "first message" in lower
        assert "matches" in lower and "infers" in lower


class TestTools:
    def test_overview_lists_classes_and_properties(self):
        ctx = ToolContext(
            host="", token="", ontology_classes=_CLASSES, ontology_properties=_PROPERTIES,
            ontology_base_uri="http://ex.org/",
        )
        out = json.loads(cr_tools.tool_get_ontology_overview(ctx))
        assert out["class_count"] == 2
        assert out["property_count"] == 1
        assert out["base_uri"] == "http://ex.org/"
        names = [c["name"] for c in out["classes"]]
        assert "Customer" in names and "Order" in names
        assert out["classes"][0]["dataProperties"] == ["balance", "riskTier"]

    def test_describe_class_returns_object_properties_by_domain(self):
        ctx = ToolContext(
            host="", token="", ontology_classes=_CLASSES, ontology_properties=_PROPERTIES,
        )
        out = json.loads(cr_tools.tool_describe_class(ctx, name="Customer"))
        assert out["name"] == "Customer"
        assert out["dataProperties"] == ["balance", "riskTier"]
        assert [p["name"] for p in out["objectProperties"]] == ["placesOrder"]

    def test_describe_unknown_class_errors(self):
        ctx = ToolContext(host="", token="", ontology_classes=_CLASSES)
        out = json.loads(cr_tools.tool_describe_class(ctx, name="Ghost"))
        assert "error" in out

    def test_update_draft_writes_context(self):
        ctx = ToolContext(host="", token="")
        out = json.loads(
            cr_tools.tool_update_draft(ctx, name="R", description="d", query=_QUERY, ready=True)
        )
        assert out["ok"] is True
        assert ctx.rule_draft == {"name": "R", "description": "d", "query": _QUERY}
        assert ctx.rule_draft_ready is True

    def test_propose_options_writes_context(self):
        ctx = ToolContext(host="", token="")
        out = json.loads(
            cr_tools.tool_propose_options(
                ctx, question="Match 'Active'?", options=["Yes", "No"], multi=False
            )
        )
        assert out == {"ok": True, "count": 2}
        assert ctx.rule_options == {
            "question": "Match 'Active'?",
            "options": ["Yes", "No"],
            "multi": False,
        }

    def test_propose_options_coerces_and_drops_empty(self):
        ctx = ToolContext(host="", token="")
        cr_tools.tool_propose_options(
            ctx, question="Pick", options=["a", "", "  ", "b"], multi=True
        )
        assert ctx.rule_options["options"] == ["a", "b"]
        assert ctx.rule_options["multi"] is True

    def test_propose_options_empty_sets_none(self):
        ctx = ToolContext(host="", token="")
        out = json.loads(cr_tools.tool_propose_options(ctx, question="Q", options=[]))
        assert out == {"ok": True, "count": 0}
        assert ctx.rule_options is None
