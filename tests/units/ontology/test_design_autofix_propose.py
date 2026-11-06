"""Unit tests for DesignAutofix.propose orchestration."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.unit

from back.objects.ontology.DesignAutofix import DesignAutofix  # noqa: E402


def test_propose_e01_rule_path():
    findings = [{"rule": "DC-E01", "kind": "class", "name": "Child"}]
    classes = [{"name": "Child", "parent": "Missing"}]
    out = DesignAutofix.propose(findings, classes, [], [], llm_available=False)
    assert out["success"]
    assert out["plan"]["operations"][0]["op"] == "clear_parent"


def test_propose_mixed_rules_rejected():
    findings = [
        {"rule": "DC-E01", "kind": "class", "name": "A"},
        {"rule": "DC-E02", "kind": "property", "name": "p"},
    ]
    out = DesignAutofix.propose(findings, [], [], [], llm_available=False)
    assert out["success"] is False
    assert "same rule" in out["error"].lower()


def test_propose_e06_rejected():
    findings = [{"rule": "DC-E06", "kind": "class", "name": "X"}]
    out = DesignAutofix.propose(findings, [], [], [], llm_available=False)
    assert out["success"] is False
    assert "no auto-fix" in out["error"].lower()


def test_propose_w03_fallback_no_llm():
    findings = [
        {"rule": "DC-W03", "kind": "attribute", "name": "paymentmethod"}
    ]
    out = DesignAutofix.propose(
        findings,
        [{"name": "Payment"}],
        [{"name": "paymentmethod", "type": "DatatypeProperty"}],
        [],
        llm_available=False,
    )
    assert out["success"]
    assert out["llm_available"] is False
    assert out["plan"].get("needs_entity_picker") is True
    assert out["plan"]["operations"][0]["op"] == "attach_attribute"
    assert any(a["op"] == "remove_property" for a in out["plan"]["alternatives"])


def test_propose_w01_requires_llm():
    findings = [{"rule": "DC-W01", "kind": "class", "name": "Orphan"}]
    out = DesignAutofix.propose(
        findings, [{"name": "Orphan"}], [], [], llm_available=False
    )
    assert out["success"] is False
    assert out["llm_available"] is False


def test_propose_ai_with_runner():
    findings = [
        {"rule": "DC-W03", "kind": "attribute", "name": "paymentmethod"}
    ]

    def runner(finding, classes, properties, groups):
        return SimpleNamespace(
            success=True,
            plan={
                "rule": "DC-W03",
                "summary": "Attach to Payment",
                "operations": [
                    {
                        "op": "attach_attribute",
                        "params": {"entity": "Payment"},
                        "rationale": "Name match",
                    }
                ],
                "alternatives": [
                    {"op": "remove_property", "rationale": "Remove"}
                ],
            },
            error="",
        )

    out = DesignAutofix.propose(
        findings,
        [{"name": "Payment"}],
        [{"name": "paymentmethod", "type": "DatatypeProperty"}],
        [],
        llm_available=True,
        llm_runner=runner,
    )
    assert out["success"]
    assert out["plan"]["operations"][0]["params"]["entity"] == "Payment"


def test_propose_w02_fallback():
    findings = [{"rule": "DC-W02", "kind": "class", "name": "customer_account"}]
    out = DesignAutofix.propose(
        findings,
        [{"name": "customer_account", "label": ""}],
        [],
        [],
        llm_available=False,
    )
    assert out["success"]
    assert out["plan"]["operations"][0]["op"] == "set_label"
    assert out["plan"]["operations"][0]["params"]["label"] == "Customer account"
