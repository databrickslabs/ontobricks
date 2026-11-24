"""Unit tests for mechanical Design Autofix rule planners."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from back.objects.ontology.DesignChecks import DesignChecks  # noqa: E402
from back.objects.ontology.autofix_rule_fixers import propose_rule_fix  # noqa: E402


def test_e01_clear_parent():
    finding = {"rule": "DC-E01", "kind": "class", "name": "Child"}
    classes = [{"name": "Child", "parent": "Missing"}]
    plan = propose_rule_fix(finding, classes, [], [])
    assert plan["operations"][0]["op"] == "clear_parent"
    assert plan["operations"][0]["source"] == "rule"


def test_e02_clear_domain():
    finding = {"rule": "DC-E02", "kind": "property", "name": "rel"}
    props = [{"name": "rel", "domain": "Ghost", "type": "ObjectProperty"}]
    plan = propose_rule_fix(finding, [], props, [])
    assert plan["operations"][0]["op"] == "clear_domain"


def test_e03_clear_range():
    finding = {"rule": "DC-E03", "kind": "property", "name": "rel"}
    props = [{"name": "rel", "range": "Ghost", "type": "ObjectProperty"}]
    plan = propose_rule_fix(finding, [], props, [])
    assert plan["operations"][0]["op"] == "clear_range"


def test_e04_remove_group_member():
    finding = {
        "rule": "DC-E04",
        "kind": "group",
        "name": "G1",
        "member": "Missing",
    }
    groups = [{"name": "G1", "members": ["Missing", "Ok"]}]
    plan = propose_rule_fix(finding, [{"name": "Ok"}], [], groups)
    assert plan["operations"][0]["op"] == "remove_group_member"
    assert plan["operations"][0]["params"]["group"] == "G1"
    assert plan["operations"][0]["params"]["member"] == "Missing"


def test_e04_finding_includes_member():
    report = DesignChecks.run(
        classes=[{"name": "Ok"}],
        properties=[],
        groups=[{"name": "G1", "members": ["Missing", "Ok"]}],
    )
    e04 = [f for f in report["findings"] if f["rule"] == "DC-E04"]
    assert len(e04) == 1
    assert e04[0]["member"] == "Missing"


def test_e05_clear_parent_on_cycle():
    finding = {"rule": "DC-E05", "kind": "class", "name": "A"}
    classes = [
        {"name": "A", "parent": "B"},
        {"name": "B", "parent": "A"},
    ]
    plan = propose_rule_fix(finding, classes, [], [])
    assert plan["operations"][0]["op"] == "clear_parent"
    assert plan["operations"][0]["target"]["name"] == "A"


def test_i02_pascal_rename():
    finding = {"rule": "DC-I02", "kind": "class", "name": "bad_name"}
    plan = propose_rule_fix(finding, [{"name": "bad_name"}], [], [])
    assert plan["operations"][0]["op"] == "rename"
    assert plan["operations"][0]["params"]["new_name"] == "BadName"


def test_i03_camel_rename():
    finding = {"rule": "DC-I03", "kind": "property", "name": "Bad_Name"}
    plan = propose_rule_fix(
        finding,
        [],
        [{"name": "Bad_Name", "type": "ObjectProperty"}],
        [],
    )
    assert plan["operations"][0]["op"] == "rename"
    assert plan["operations"][0]["params"]["new_name"] == "badName"


def test_i02_skips_on_collision():
    finding = {"rule": "DC-I02", "kind": "class", "name": "bad_name"}
    classes = [{"name": "bad_name"}, {"name": "BadName"}]
    plan = propose_rule_fix(finding, classes, [], [])
    assert plan["operations"] == []
    assert "collision" in plan["summary"].lower() or "already" in plan[
        "summary"
    ].lower()


def test_unknown_ai_rule_returns_empty():
    finding = {"rule": "DC-W03", "kind": "attribute", "name": "x"}
    plan = propose_rule_fix(finding, [], [], [])
    assert plan["operations"] == []
