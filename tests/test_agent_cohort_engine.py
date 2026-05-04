"""Engine-level tests for ``agents.agent_cohort.engine`` (Stage 2).

We don't talk to a real LLM serving endpoint -- instead we patch
``call_serving_endpoint`` to return a scripted sequence of responses
that drive the agent through:

  user prompt
   -> tool_call: list_classes        (LLM iter 1)
   -> tool_call: propose_rule(rule)  (LLM iter 2)
   -> final text message             (LLM iter 3)

We also patch the cohort tools' ``_client`` factory so the
``list_classes`` HTTP call is satisfied locally. The result we assert
on is :attr:`AgentResult.proposed_rule` -- it must equal the validated,
canonical CohortRule that the LLM passed to ``propose_rule``.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import httpx
import pytest

from agents.agent_cohort import engine as cohort_engine
from agents.agent_cohort import tools as cohort_tools


_ONT_PAYLOAD = {
    "success": True,
    "ontology": {
        "classes": [
            {
                "uri": "https://ex.com/onto/Consultant",
                "name": "Consultant",
                "label": "Consultant",
                "dataProperties": [
                    {
                        "uri": "https://ex.com/onto/status",
                        "label": "status",
                        "range": "xsd:string",
                    },
                ],
            },
        ],
        "properties": [
            {
                "uri": "https://ex.com/onto/staffedOn",
                "label": "staffed on",
                "type": "ObjectProperty",
                "domain": "https://ex.com/onto/Consultant",
                "range": "https://ex.com/onto/Project",
            },
        ],
    },
}


_PROPOSED_RULE = {
    "id": "exempt-pool",
    "label": "Exempt staffing pool",
    "class_uri": "https://ex.com/onto/Consultant",
    "links": [
        {
            "shared_class": "https://ex.com/onto/Project",
            "via": "https://ex.com/onto/staffedOn",
        }
    ],
    "links_combine": "any",
    "compatibility": [
        {
            "type": "value_equals",
            "property": "https://ex.com/onto/status",
            "value": "Exempt",
        }
    ],
    "group_type": "connected",
    "min_size": 2,
    "output": {"graph": True},
}


def _llm_responses():
    """Three scripted LLM messages: tool_call, tool_call, final text."""
    return [
        # iter 1 -- LLM asks for list_classes
        {
            "choices": [
                {
                    "message": {
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "tc1",
                                "function": {
                                    "name": "list_classes",
                                    "arguments": "{}",
                                },
                            }
                        ],
                    }
                }
            ],
            "usage": {"prompt_tokens": 100, "completion_tokens": 5},
        },
        # iter 2 -- LLM submits the proposed rule
        {
            "choices": [
                {
                    "message": {
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "tc2",
                                "function": {
                                    "name": "propose_rule",
                                    "arguments": json.dumps({"rule": _PROPOSED_RULE}),
                                },
                            }
                        ],
                    }
                }
            ],
            "usage": {"prompt_tokens": 200, "completion_tokens": 10},
        },
        # iter 3 -- LLM closes with a markdown explanation
        {
            "choices": [
                {
                    "message": {
                        "content": "Grouping consultants who share a project, restricted to Exempt members.",
                    }
                }
            ],
            "usage": {"prompt_tokens": 250, "completion_tokens": 20},
        },
    ]


@pytest.fixture
def patch_client(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ontology/get-loaded-ontology":
            return httpx.Response(200, json=_ONT_PAYLOAD)
        return httpx.Response(404, text=f"unmocked {request.url.path}")

    def _factory(ctx):
        return httpx.Client(
            base_url=ctx.dtwin_base_url or "http://loopback.invalid",
            transport=httpx.MockTransport(handler),
            timeout=5,
            follow_redirects=False,
        )

    monkeypatch.setattr(cohort_tools, "_client", _factory)
    return monkeypatch


class TestRunAgent:
    def test_full_round_trip_returns_validated_rule(self, patch_client):
        with patch.object(cohort_engine, "call_serving_endpoint") as mock_llm:
            mock_llm.side_effect = _llm_responses()
            result = cohort_engine.run_agent(
                host="https://test.databricks.com",
                token="tok",
                endpoint_name="dbx-llm",
                base_url="http://loopback.invalid",
                domain_name="consulting_demo",
                registry_params={
                    "registry_catalog": "main",
                    "registry_schema": "ontobricks",
                    "registry_volume": "documents",
                },
                session_cookies={},
                user_message="find consultants who can be staffed together — exempts only with exempts",
            )

        assert result.success is True
        assert result.iterations == 3
        assert result.proposed_rule is not None
        assert result.proposed_rule["id"] == "exempt-pool"
        assert result.proposed_rule["class_uri"] == "https://ex.com/onto/Consultant"
        # Reply text is preserved verbatim from the final LLM message.
        assert "Exempt" in result.reply

        tool_call_steps = [s for s in result.steps if s.step_type == "tool_call"]
        tool_result_steps = [s for s in result.steps if s.step_type == "tool_result"]
        assert {s.tool_name for s in tool_call_steps} == {"list_classes", "propose_rule"}
        assert len(tool_result_steps) == 2

        # Token usage is summed across iterations.
        assert result.usage.get("prompt_tokens") == 550
        assert result.usage.get("completion_tokens") == 35

    def test_invalid_rule_does_not_set_proposed_rule(self, patch_client):
        bad_rule = dict(_PROPOSED_RULE)
        bad_rule["class_uri"] = ""  # invalid

        responses = [
            {
                "choices": [
                    {
                        "message": {
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "tc1",
                                    "function": {
                                        "name": "propose_rule",
                                        "arguments": json.dumps({"rule": bad_rule}),
                                    },
                                }
                            ],
                        }
                    }
                ],
                "usage": {"prompt_tokens": 50, "completion_tokens": 5},
            },
            {
                "choices": [
                    {
                        "message": {
                            "content": "Sorry — I could not assemble a valid rule. Could you tell me which class to group?",
                        }
                    }
                ],
                "usage": {"prompt_tokens": 100, "completion_tokens": 20},
            },
        ]

        with patch.object(cohort_engine, "call_serving_endpoint") as mock_llm:
            mock_llm.side_effect = responses
            result = cohort_engine.run_agent(
                host="https://test.databricks.com",
                token="tok",
                endpoint_name="dbx-llm",
                base_url="http://loopback.invalid",
                domain_name="consulting_demo",
                registry_params={},
                session_cookies={},
                user_message="something vague",
            )

        assert result.success is True
        assert result.proposed_rule is None
        assert "could not" in result.reply.lower()

    def test_llm_failure_is_reported(self):
        with patch.object(
            cohort_engine, "call_serving_endpoint", side_effect=RuntimeError("boom")
        ):
            result = cohort_engine.run_agent(
                host="https://test.databricks.com",
                token="tok",
                endpoint_name="dbx-llm",
                base_url="http://loopback.invalid",
                domain_name="consulting_demo",
                registry_params={},
                session_cookies={},
                user_message="hi",
            )

        assert result.success is False
        assert "boom" in result.error
