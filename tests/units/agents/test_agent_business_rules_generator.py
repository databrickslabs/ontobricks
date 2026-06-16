"""Engine-level tests for ``agents.agent_business_rules_generator.engine``.

We never talk to a real LLM serving endpoint -- ``call_serving_endpoint`` is
patched to return a scripted sequence of responses that drive the agent
through:

  user prompt
   -> tool_call: get_ontology_design     (LLM iter 1)
   -> final text message: JSON rule lists (LLM iter 2)

The result we assert on is the four rule lists on :class:`AgentResult`.
"""

from __future__ import annotations

import json
from unittest.mock import patch

from agents.agent_business_rules_generator import engine as br_engine
from agents.agent_business_rules_generator import tools as br_tools
from agents.tools.context import ToolContext


_ONTOLOGY_DESIGN = {
    "entities": [
        {
            "name": "Customer",
            "uri": "http://ex.org/Customer",
            "attributes": ["age", "balance", "riskTier"],
        },
        {"name": "Order", "uri": "http://ex.org/Order", "attributes": ["total"]},
        # Derived/target classes used by the fixture rules below — under the
        # strict "no invented terms" gate these must exist in the ontology.
        {"name": "Adult", "uri": "http://ex.org/Adult", "attributes": []},
        {"name": "LargeOrder", "uri": "http://ex.org/LargeOrder", "attributes": []},
        {"name": "FrequentBuyer", "uri": "http://ex.org/FrequentBuyer", "attributes": []},
        {"name": "LoyalCustomer", "uri": "http://ex.org/LoyalCustomer", "attributes": []},
    ],
    "relationships": [
        {"name": "placesOrder", "domain": "Customer", "range": "Order"},
    ],
}

_RULES_OUTPUT = {
    "swrl_rules": [
        {
            "name": "AdultCustomer",
            "description": "18+ are adults",
            "antecedent": "Customer(?c) ^ age(?c, ?a) ^ swrlb:greaterThanOrEqual(?a, 18)",
            "consequent": "Adult(?c)",
        }
    ],
    "decision_tables": [
        {
            "name": "RiskTier",
            "target_class": "Customer",
            "hit_policy": "first",
            "row_logic": "and",
            "input_columns": [{"property": "balance", "label": "Balance"}],
            "output_column": {"property": "riskTier", "action": "set", "value": ""},
            "rows": [{"conditions": [{"op": "gt", "value": "100000"}], "action_value": "high"}],
        }
    ],
    "sparql_rules": [
        {
            "name": "FlagBigOrder",
            "query": "CONSTRUCT { ?o a :LargeOrder } WHERE { ?o a :Order . ?o :total ?t . FILTER(?t > 10000) }",
        }
    ],
    "aggregate_rules": [
        {
            "name": "FrequentBuyer",
            "target_class": "Customer",
            "group_by_property": "placesOrder",
            "aggregate_property": "",
            "aggregate_function": "count",
            "operator": "gte",
            "threshold": "10",
            "result_class": "FrequentBuyer",
        }
    ],
}


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


def _run(responses):
    with patch.object(br_engine, "call_serving_endpoint") as mock_llm:
        mock_llm.side_effect = responses
        return br_engine.run_agent(
            host="https://test.databricks.com",
            token="tok",
            endpoint_name="dbx-llm",
            registry={"catalog": "main", "schema": "ob", "volume": "documents"},
            ontology_design=_ONTOLOGY_DESIGN,
            base_uri="http://ex.org/",
        )


class TestRunAgent:
    def test_full_round_trip_returns_four_rule_lists(self):
        responses = [
            _tool_call_response("get_ontology_design"),
            _text_response(json.dumps(_RULES_OUTPUT)),
        ]
        result = _run(responses)

        assert result.success is True
        assert result.iterations == 2
        assert len(result.swrl_rules) == 1
        assert len(result.decision_tables) == 1
        assert len(result.sparql_rules) == 1
        assert len(result.aggregate_rules) == 1
        assert result.total_rules() == 4
        # enabled is defaulted to True on every rule.
        assert all(r["enabled"] is True for r in result.swrl_rules)
        assert result.swrl_rules[0]["name"] == "AdultCustomer"
        # usage summed across iterations.
        assert result.usage["prompt_tokens"] == 300

    def test_json_wrapped_in_code_fences_is_parsed(self):
        fenced = "```json\n" + json.dumps(_RULES_OUTPUT) + "\n```"
        result = _run([_text_response(fenced)])
        assert result.success is True
        assert result.total_rules() == 4

    def test_json_embedded_in_prose_is_extracted(self):
        prose = "Here are the rules:\n" + json.dumps(_RULES_OUTPUT) + "\nDone."
        result = _run([_text_response(prose)])
        assert result.success is True
        assert result.total_rules() == 4

    def test_invalid_json_triggers_retry_then_succeeds(self):
        responses = [
            _text_response("not json at all"),
            _text_response(json.dumps(_RULES_OUTPUT)),
        ]
        result = _run(responses)
        assert result.success is True
        assert result.total_rules() == 4

    def test_persistently_invalid_json_fails(self):
        responses = [_text_response("nope") for _ in range(5)]
        result = _run(responses)
        assert result.success is False
        assert "JSON" in result.error

    def test_swrl_referencing_unknown_entities_is_dropped(self):
        bad = {
            "swrl_rules": [
                {
                    "name": "GoodLoyal",
                    "antecedent": "Customer(?c) ^ placesOrder(?c, ?o)",
                    "consequent": "LoyalCustomer(?c)",
                },
                {
                    "name": "BadGhost",
                    "antecedent": "Widget(?w) ^ frobnicates(?w, ?x)",
                    "consequent": "BigWidget(?w)",
                },
            ]
        }
        result = _run([_text_response(json.dumps(bad))])
        assert result.success is True
        # Only the rule that references existing ontology terms survives.
        names = [r["name"] for r in result.swrl_rules]
        assert names == ["GoodLoyal"]

    def test_tautological_swrl_rule_is_dropped(self):
        out = {
            "swrl_rules": [
                {
                    "name": "UselessSelfType",
                    # ?o is already an Order in the IF; concluding Order(?o) again
                    # infers nothing and must be discarded.
                    "antecedent": "Customer(?c) ^ placesOrder(?c, ?o) ^ Order(?o)",
                    "consequent": "Order(?o)",
                },
                {
                    "name": "AdultCustomer",
                    "antecedent": "Customer(?c) ^ age(?c, ?a) ^ swrlb:greaterThanOrEqual(?a, 18)",
                    "consequent": "Adult(?c)",
                },
            ]
        }
        result = _run([_text_response(json.dumps(out))])
        assert result.success is True
        names = [r["name"] for r in result.swrl_rules]
        assert names == ["AdultCustomer"]

    def test_missing_keys_yield_empty_lists(self):
        result = _run([_text_response(json.dumps({"swrl_rules": _RULES_OUTPUT["swrl_rules"]}))])
        assert result.success is True
        assert len(result.swrl_rules) == 1
        assert result.decision_tables == []
        assert result.sparql_rules == []
        assert result.aggregate_rules == []

    def test_llm_failure_is_reported(self):
        with patch.object(
            br_engine, "call_serving_endpoint", side_effect=RuntimeError("boom")
        ):
            result = br_engine.run_agent(
                host="h",
                token="t",
                endpoint_name="e",
                registry={},
                ontology_design=_ONTOLOGY_DESIGN,
                base_uri="http://ex.org/",
            )
        assert result.success is False
        assert "boom" in result.error


class TestOntologyDesignTool:
    def test_returns_entities_and_relationships(self):
        ctx = ToolContext(host="", token="", ontology=_ONTOLOGY_DESIGN)
        out = json.loads(br_tools.tool_get_ontology_design(ctx))
        assert out["entity_count"] == len(_ONTOLOGY_DESIGN["entities"])
        assert out["relationship_count"] == 1

    def test_empty_ontology_returns_message(self):
        ctx = ToolContext(host="", token="", ontology={})
        out = json.loads(br_tools.tool_get_ontology_design(ctx))
        assert out["entities"] == []
        assert "message" in out
