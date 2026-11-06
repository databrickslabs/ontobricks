"""Unit tests for agent_design_fixer engine."""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.unit

from agents.agent_design_fixer.engine import run_agent  # noqa: E402


def _llm_submit_plan(*_args, **_kwargs):
    plan = {
        "rule": "DC-W03",
        "summary": "Attach orphan attribute paymentmethod to Payment, or remove it.",
        "operations": [
            {
                "op": "attach_attribute",
                "params": {"entity": "Payment"},
                "rationale": "Name suggests Payment owns this field.",
            }
        ],
        "alternatives": [
            {
                "op": "remove_property",
                "rationale": "Drop the orphan attribute.",
            }
        ],
    }
    return {
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "tc1",
                            "type": "function",
                            "function": {
                                "name": "submit_fix_plan",
                                "arguments": json.dumps({"plan": plan}),
                            },
                        }
                    ],
                }
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5},
    }


def test_run_agent_returns_plan():
    finding = {
        "rule": "DC-W03",
        "kind": "attribute",
        "name": "paymentmethod",
        "domain": None,
    }
    classes = [{"name": "Payment", "dataProperties": []}]
    props = [
        {
            "name": "paymentmethod",
            "type": "DatatypeProperty",
            "domain": "",
        }
    ]
    result = run_agent(
        finding,
        classes,
        props,
        [],
        host="h",
        token="t",
        endpoint="ep",
        endpoint_kind="chat",
        call_llm=_llm_submit_plan,
    )
    assert result.success
    assert result.plan["operations"][0]["op"] == "attach_attribute"
    assert any(a["op"] == "remove_property" for a in result.plan["alternatives"])


def test_submit_fix_plan_does_not_mutate_ontology():
    classes = [{"name": "Payment", "dataProperties": []}]
    props = [
        {
            "name": "paymentmethod",
            "type": "DatatypeProperty",
            "domain": "",
        }
    ]
    before = json.dumps(props, sort_keys=True)
    finding = {
        "rule": "DC-W03",
        "kind": "attribute",
        "name": "paymentmethod",
    }
    result = run_agent(
        finding,
        classes,
        props,
        [],
        host="h",
        token="t",
        endpoint="ep",
        call_llm=_llm_submit_plan,
    )
    assert result.success
    assert json.dumps(props, sort_keys=True) == before
    assert classes[0]["dataProperties"] == []
