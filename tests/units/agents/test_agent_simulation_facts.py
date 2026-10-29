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
from back.objects.simulation import Patch

pytestmark = pytest.mark.unit

_PATCH = {
    "op": "set",
    "subject": "https://ex.com/Person/JacobMartin",
    "predicate": "https://ex.com/onto/creditScore",
    "object": "420",
}
_REMOVE_CLAIM = {
    "op": "remove",
    "subject": "https://ex.com/Person/JacobMartin",
    "predicate": "https://ex.com/onto/hasClaim",
    "object": "https://ex.com/Claim/CLM0000030",
}

_ONT = {
    "success": True,
    "ontology": {
        "classes": [
            {
                "uri": "https://ex.com/onto/Person",
                "name": "Person",
                "label": "Person",
                "dataProperties": [
                    {
                        "uri": "https://ex.com/onto/creditScore",
                        "label": "credit score",
                        "range": "xsd:integer",
                    }
                ],
            }
        ],
        "properties": [
            {
                "uri": "https://ex.com/onto/creditScore",
                "name": "creditScore",
                "type": "DatatypeProperty",
                "domain": "https://ex.com/onto/Person",
            }
        ],
    },
}


def _ctx():
    return ToolContext(
        host="h",
        token="t",
        dtwin_base_url="http://loopback.invalid",
    )


def _tool_turn(name: str, args: dict, call_id: str = "tc1"):
    return {
        "choices": [
            {
                "message": {
                    "content": "",
                    "tool_calls": [
                        {
                            "id": call_id,
                            "function": {
                                "name": name,
                                "arguments": json.dumps(args),
                            },
                        }
                    ],
                }
            }
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 4},
    }


def _llm_ok():
    return [
        _tool_turn(
            "propose_patches",
            {
                "patches": [_PATCH],
                "scenario_description": (
                    "What-if on Jacob Martin: credit score set to 420."
                ),
            },
        ),
        {
            "choices": [
                {"message": {"content": "Set Jacob Martin's credit score to 420."}}
            ],
            "usage": {"prompt_tokens": 20, "completion_tokens": 8},
        },
    ]


def _llm_full_generation():
    """list_classes → list_properties_of → search_entities → propose_patches."""
    return [
        _tool_turn("list_classes", {}, "tc-classes"),
        _tool_turn(
            "list_properties_of",
            {"class_uri": "https://ex.com/onto/Person"},
            "tc-props",
        ),
        _tool_turn("search_entities", {"query": "Jacob"}, "tc-search"),
        _tool_turn(
            "propose_patches",
            {
                "patches": [_PATCH],
                "scenario_description": (
                    "What-if on Jacob Martin: credit score set to 420."
                ),
            },
            "tc-propose",
        ),
        {
            "choices": [
                {"message": {"content": "Set Jacob Martin's credit score to 420."}}
            ],
            "usage": {"prompt_tokens": 20, "completion_tokens": 8},
        },
    ]


def _loopback(monkeypatch, handler):
    def factory(ctx):
        return httpx.Client(
            base_url=ctx.dtwin_base_url or "http://loopback.invalid",
            transport=httpx.MockTransport(handler),
        )

    monkeypatch.setattr(cohort_tools, "_client", factory)
    monkeypatch.setattr(facts_tools, "_client", factory)
    return factory


def test_propose_patches_validates_and_parks():
    ctx = _ctx()
    out = json.loads(
        facts_tools.tool_propose_patches(
            ctx,
            patches=[_PATCH],
            scenario_description="Jacob Martin's credit score drops to 420.",
        )
    )
    assert out["valid"] is True and out["count"] == 1
    assert ctx.metadata["proposed_patches"][0]["op"] == "set"
    assert "420" in ctx.metadata["proposed_description"]
    Patch.from_dict(ctx.metadata["proposed_patches"][0])


def test_propose_patches_allows_empty_list_with_description():
    ctx = _ctx()
    out = json.loads(
        facts_tools.tool_propose_patches(
            ctx, patches=[], scenario_description="Already covered."
        )
    )
    assert out["valid"] is True and out["count"] == 0
    assert ctx.metadata["proposed_patches"] == []
    assert ctx.metadata["proposed_description"] == "Already covered."


def test_propose_patches_rejects_bad_op():
    ctx = _ctx()
    out = json.loads(
        facts_tools.tool_propose_patches(ctx, patches=[{**_PATCH, "op": "upsert"}])
    )
    assert "error" in out
    assert "proposed_patches" not in ctx.metadata


def test_propose_patches_rejects_empty_without_description():
    ctx = _ctx()
    out = json.loads(facts_tools.tool_propose_patches(ctx, patches=[]))
    assert "error" in out
    assert "proposed_patches" not in ctx.metadata


def test_propose_patches_caps_count():
    ctx = _ctx()
    too_many = [{**_PATCH, "object": str(i)} for i in range(51)]
    out = json.loads(facts_tools.tool_propose_patches(ctx, patches=too_many))
    assert "error" in out
    assert "50" in out["error"]


def test_propose_patches_keeps_datatype():
    ctx = _ctx()
    typed = {**_PATCH, "object": "7", "datatype": "xsd:integer"}
    out = json.loads(
        facts_tools.tool_propose_patches(
            ctx, patches=[typed], scenario_description="Seniority 7."
        )
    )
    assert out["valid"] is True
    assert ctx.metadata["proposed_patches"][0]["datatype"] == "xsd:integer"


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


def test_search_entities_includes_simulation_id(monkeypatch):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(json.loads(request.content.decode()))
        return httpx.Response(200, json={"seeds": []})

    monkeypatch.setattr(
        facts_tools,
        "_client",
        lambda ctx: httpx.Client(
            base_url=ctx.dtwin_base_url,
            transport=httpx.MockTransport(handler),
        ),
    )
    ctx = _ctx()
    ctx.dtwin_simulation_id = "sAbc"
    json.loads(facts_tools.tool_search_entities(ctx, query="Jacob"))
    assert seen["simulation"] == "sAbc"
    assert seen["phase"] == "preview"


def test_search_entities_requires_query_or_class():
    out = json.loads(facts_tools.tool_search_entities(_ctx()))
    assert "error" in out


def test_search_entities_http_error(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="unavailable")

    monkeypatch.setattr(
        facts_tools,
        "_client",
        lambda ctx: httpx.Client(
            base_url=ctx.dtwin_base_url,
            transport=httpx.MockTransport(handler),
        ),
    )
    out = json.loads(facts_tools.tool_search_entities(_ctx(), query="Jacob"))
    assert "error" in out
    assert "503" in out["error"]


def test_list_entity_relationships_returns_exact_live_triples(monkeypatch):
    entity = "https://ex.com/Person/JacobMartin"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/dtwin/triples/find"
        assert request.url.params["search"] == entity
        return httpx.Response(
            200,
            json={
                "triples": [
                    {
                        "subject": entity,
                        "predicate": "https://ex.com/onto/hasClaim",
                        "object": _REMOVE_CLAIM["object"],
                    },
                    {
                        "subject": entity,
                        "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        "object": "https://ex.com/onto/Person",
                    },
                ]
            },
        )

    monkeypatch.setattr(
        facts_tools,
        "_client",
        lambda ctx: httpx.Client(
            base_url=ctx.dtwin_base_url,
            transport=httpx.MockTransport(handler),
        ),
    )
    out = json.loads(facts_tools.tool_list_entity_relationships(_ctx(), entity_uri=entity))
    assert out["relationships"] == [
        {
            "subject": entity,
            "predicate": _REMOVE_CLAIM["predicate"],
            "object": _REMOVE_CLAIM["object"],
        }
    ]


def test_list_entity_relationships_requires_uri():
    out = json.loads(facts_tools.tool_list_entity_relationships(_ctx(), entity_uri="Jacob"))
    assert "error" in out


def _run(monkeypatch, llm_turns, **kwargs):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ontology/get-loaded-ontology":
            return httpx.Response(200, json=_ONT)
        if request.url.path == "/dtwin/sync/filter":
            return httpx.Response(
                200,
                json={
                    "seeds": [
                        {
                            "uri": "https://ex.com/Person/JacobMartin",
                            "label": "Jacob Martin",
                        }
                    ]
                },
            )
        if request.url.path == "/dtwin/triples/find":
            return httpx.Response(
                200,
                json={
                    "triples": [
                        {
                            "subject": "https://ex.com/Person/JacobMartin",
                            "predicate": "https://ex.com/onto/hasClaim",
                            "object": _REMOVE_CLAIM["object"],
                        }
                    ]
                },
            )
        return httpx.Response(404)

    _loopback(monkeypatch, handler)
    defaults = dict(
        host="https://test.databricks.com",
        token="tok",
        endpoint_name="dbx-llm",
        base_url="http://loopback.invalid",
        domain_name="demo",
        registry_params={},
        session_cookies={},
        user_message="Set Jacob Martin's credit score to 420",
    )
    defaults.update(kwargs)
    with patch.object(facts_engine, "call_serving_endpoint") as mock_llm:
        mock_llm.side_effect = llm_turns
        return facts_engine.run_agent(**defaults)


def test_run_agent_returns_proposed_patches(monkeypatch):
    result = _run(monkeypatch, _llm_ok())
    assert result.success is True
    assert result.proposed_patches == [_PATCH]
    assert "420" in result.reply
    assert "credit score" in result.proposed_description.lower()


def test_run_agent_generates_facts_via_ontology_and_search(monkeypatch):
    result = _run(monkeypatch, _llm_full_generation())
    tools_used = [s.tool_name for s in result.steps if s.step_type == "tool_call"]
    assert tools_used == [
        "list_classes",
        "list_properties_of",
        "search_entities",
        "propose_patches",
    ]
    assert result.success is True
    assert result.proposed_patches == [_PATCH]
    Patch.from_dict(result.proposed_patches[0])


def test_run_agent_proposes_exact_claim_removal(monkeypatch):
    turns = [
        _tool_turn(
            "list_entity_relationships",
            {"entity_uri": _REMOVE_CLAIM["subject"]},
            "tc-links",
        ),
        _tool_turn(
            "propose_patches",
            {
                "patches": [_REMOVE_CLAIM],
                "scenario_description": "Jacob Martin has no associated claims.",
            },
            "tc-propose",
        ),
        {"choices": [{"message": {"content": "I removed Jacob's claim link."}}]},
    ]
    result = _run(
        monkeypatch,
        turns,
        user_message="Detach all claims from Jacob Martin.",
    )
    assert result.success is True
    assert result.proposed_patches == [_REMOVE_CLAIM]


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


def test_run_agent_no_choices(monkeypatch):
    result = _run(
        monkeypatch,
        [{"choices": [], "usage": {}}],
    )
    assert result.success is False
    assert "No choices" in result.error


def test_run_agent_max_iterations(monkeypatch):
    looping = [
        _tool_turn("list_classes", {}, f"tc-{i}")
        for i in range(facts_engine.MAX_ITERATIONS)
    ]
    result = _run(monkeypatch, looping)
    assert result.success is False
    assert "Max iterations" in result.error
    assert result.iterations == facts_engine.MAX_ITERATIONS
    assert "simplify" in result.reply.lower()


def test_eval_dataset_patches_are_valid():
    from pathlib import Path
    from tests.eval.judges.simulation_facts import score_dataset_row

    path = Path(__file__).resolve().parents[2] / "eval/datasets/agent_simulation_facts/baseline.jsonl"
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) >= 20
    assert all(score_dataset_row(row) == 1.0 for row in rows)
    with pytest.raises(ValidationError):
        Patch.from_dict({"op": "set", "subject": "s", "predicate": "p"})
