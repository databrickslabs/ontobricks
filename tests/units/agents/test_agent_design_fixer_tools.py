"""Unit tests for agent_design_fixer read-only tools."""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.unit

from agents.agent_design_fixer.tools import (  # noqa: E402
    ToolContext,
    tool_find_similar_names,
    tool_get_item,
    tool_list_classes,
    tool_neighbourhood,
    tool_submit_fix_plan,
)


def _ctx() -> ToolContext:
    return ToolContext(
        host="h",
        token="t",
        ontology_classes=[
            {
                "name": "Payment",
                "label": "Payment",
                "parent": "",
                "dataProperties": [{"name": "amount"}],
            },
            {"name": "Invoice", "label": "Invoice", "parent": "", "dataProperties": []},
        ],
        ontology_properties=[
            {
                "name": "paymentmethod",
                "type": "DatatypeProperty",
                "domain": "",
                "range": "",
            },
            {
                "name": "hasInvoice",
                "type": "ObjectProperty",
                "domain": "Payment",
                "range": "Invoice",
            },
        ],
        ontology_groups=[{"name": "G1", "members": ["Payment"]}],
    )


def test_list_classes_returns_names():
    data = json.loads(tool_list_classes(_ctx()))
    assert "Payment" in [c["name"] for c in data["classes"]]


def test_neighbourhood_includes_attrs():
    data = json.loads(tool_neighbourhood(_ctx(), name="Payment"))
    assert "amount" in data.get("attributes", [])
    assert any(p["name"] == "hasInvoice" for p in data.get("related_properties", []))


def test_find_similar_names():
    data = json.loads(tool_find_similar_names(_ctx(), token="payment"))
    assert any(
        "Payment" == x or "payment" in x.lower() for x in data.get("matches", [])
    )


def test_get_item_group():
    data = json.loads(tool_get_item(_ctx(), kind="group", name="G1"))
    assert data["item"]["name"] == "G1"


def test_handlers_do_not_mutate():
    ctx = _ctx()
    before = json.dumps(ctx.ontology_classes, sort_keys=True)
    tool_get_item(ctx, kind="class", name="Payment")
    tool_neighbourhood(ctx, name="Payment")
    tool_find_similar_names(ctx, token="pay")
    assert json.dumps(ctx.ontology_classes, sort_keys=True) == before


def test_submit_fix_plan_stores_without_mutating_ontology():
    ctx = _ctx()
    before = json.dumps(ctx.ontology_properties, sort_keys=True)
    plan = {
        "rule": "DC-W03",
        "summary": "Attach or remove",
        "operations": [
            {
                "op": "attach_attribute",
                "params": {"entity": "Payment"},
                "rationale": "Name match",
            }
        ],
        "alternatives": [
            {"op": "remove_property", "rationale": "Drop orphan"}
        ],
    }
    out = json.loads(tool_submit_fix_plan(ctx, plan=plan))
    assert out.get("ok") is True
    assert ctx.submitted_fix_plan["rule"] == "DC-W03"
    assert json.dumps(ctx.ontology_properties, sort_keys=True) == before


def test_submit_rejects_unknown_op():
    ctx = _ctx()
    plan = {
        "rule": "DC-W03",
        "summary": "x",
        "operations": [{"op": "explode", "rationale": "nope"}],
        "alternatives": [],
    }
    out = json.loads(tool_submit_fix_plan(ctx, plan=plan))
    assert "error" in out
    assert ctx.submitted_fix_plan is None
