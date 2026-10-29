"""Engine and tool tests for agent_simulation_facts (scripted LLM)."""

from __future__ import annotations

import json
from unittest.mock import patch

import httpx
import pytest

from agents.agent_simulation_facts import engine as facts_engine
from agents.agent_simulation_facts import tools as facts_tools
from agents.agent_cohort import tools as cohort_tools
from agents.tools.context import ToolContext
from back.core.errors import ValidationError
from back.objects.simulation.SimulationPatch import SimulationPatch

pytestmark = pytest.mark.unit

_PATCH = {
    "op": "set",
    "subject": "https://ex.com/Person/JacobMartin",
    "predicate": "https://ex.com/onto/creditScore",
    "object": "420",
}

_ONT = {
    "success": True,
    "ontology": {
        "classes": [
            {"uri": "https://ex.com/onto/Person", "name": "Person", "label": "Person"}
        ],
        "properties": [],
    },
}


def _ctx():
    return ToolContext(
        host="h",
        token="t",
        dtwin_base_url="http://loopback.invalid",
    )


def _llm_ok():
    return [
        {
            "choices": [
                {
                    "message": {
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "tc1",
                                "function": {
                                    "name": "propose_patches",
                                    "arguments": json.dumps({"patches": [_PATCH]}),
                                },
                            }
                        ],
                    }
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 4},
        },
        {
            "choices": [
                {"message": {"content": "Set Jacob Martin's credit score to 420."}}
            ],
            "usage": {"prompt_tokens": 20, "completion_tokens": 8},
        },
    ]


def test_propose_patches_validates_and_parks():
    ctx = _ctx()
    out = json.loads(facts_tools.tool_propose_patches(ctx, patches=[_PATCH]))
    assert out["valid"] is True and out["count"] == 1
    assert ctx.metadata["proposed_patches"][0]["op"] == "set"
    SimulationPatch.from_dict(ctx.metadata["proposed_patches"][0])


def test_propose_patches_rejects_bad_op():
    ctx = _ctx()
    out = json.loads(
        facts_tools.tool_propose_patches(ctx, patches=[{**_PATCH, "op": "upsert"}])
    )
    assert "error" in out
    assert "proposed_patches" not in ctx.metadata


def test_search_entities_returns_seeds(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/dtwin/sync/filter"
        return httpx.Response(
            200,
            json={"seeds": [{"uri": "https://ex.com/Person/JacobMartin", "label": "Jacob"}]},
        )

    monkeypatch.setattr(
        facts_tools,
        "_client",
        lambda ctx: httpx.Client(
            base_url=ctx.dtwin_base_url,
            transport=httpx.MockTransport(handler),
        ),
    )
    out = json.loads(facts_tools.tool_search_entities(_ctx(), query="Jacob"))
    assert out["seeds"][0]["uri"].endswith("JacobMartin")


def test_run_agent_returns_proposed_patches(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ontology/get-loaded-ontology":
            return httpx.Response(200, json=_ONT)
        return httpx.Response(404)

    def factory(ctx):
        return httpx.Client(
            base_url=ctx.dtwin_base_url or "http://loopback.invalid",
            transport=httpx.MockTransport(handler),
        )

    monkeypatch.setattr(cohort_tools, "_client", factory)
    monkeypatch.setattr(facts_tools, "_client", factory)
    with patch.object(facts_engine, "call_serving_endpoint") as mock_llm:
        mock_llm.side_effect = _llm_ok()
        result = facts_engine.run_agent(
            host="https://test.databricks.com",
            token="tok",
            endpoint_name="dbx-llm",
            base_url="http://loopback.invalid",
            domain_name="demo",
            registry_params={},
            session_cookies={},
            user_message="Set Jacob Martin's credit score to 420",
        )
    assert result.success is True
    assert result.proposed_patches == [_PATCH]
    assert "420" in result.reply


def test_run_agent_reports_llm_failure():
    with patch.object(
        facts_engine, "call_serving_endpoint", side_effect=RuntimeError("boom")
    ):
        result = facts_engine.run_agent(
            host="h",
            token="t",
            endpoint_name="ep",
            base_url="http://loopback.invalid",
            domain_name="demo",
            registry_params={},
            session_cookies={},
            user_message="hi",
        )
    assert result.success is False
    assert "boom" in result.error


def test_eval_dataset_patches_are_valid():
    from pathlib import Path
    from tests.eval.judges.simulation_facts import score_dataset_row

    path = Path(__file__).resolve().parents[2] / "eval/datasets/agent_simulation_facts/baseline.jsonl"
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) >= 20
    assert all(score_dataset_row(row) == 1.0 for row in rows)
    with pytest.raises(ValidationError):
        SimulationPatch.from_dict({"op": "set", "subject": "s", "predicate": "p"})
