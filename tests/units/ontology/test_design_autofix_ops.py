"""Unit tests for Design Autofix operation helpers."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

from back.objects.ontology.autofix_ops import (  # noqa: E402
    RULE_PLANNER,
    finding_id,
    humanize_id,
    make_operation,
    to_camel_case,
    to_pascal_case,
)


def test_routing_table():
    assert RULE_PLANNER["DC-E01"] == "rule"
    assert RULE_PLANNER["DC-W03"] == "ai"
    assert RULE_PLANNER["DC-E06"] == "none"
    assert RULE_PLANNER["DC-I05"] == "none"


def test_finding_id_stable():
    f = {"rule": "DC-W03", "kind": "attribute", "name": "paymentmethod"}
    assert finding_id(f) == "DC-W03:attribute:paymentmethod"
    f2 = {
        "rule": "DC-W02",
        "kind": "attribute",
        "name": "email",
        "domain": "Person",
    }
    assert finding_id(f2) == "DC-W02:attribute:email:Person"


def test_naming_helpers():
    assert humanize_id("payment_method") == "Payment method"
    assert to_pascal_case("payment_method") == "PaymentMethod"
    assert to_camel_case("PaymentMethod") == "paymentMethod"
    assert to_camel_case("Bad_Name") == "badName"


def test_make_operation_defaults():
    op = make_operation(
        op="attach_attribute",
        target={"kind": "attribute", "name": "paymentmethod"},
        params={"entity": "Payment"},
        rationale="Name match",
        source="ai",
        finding_id="DC-W03:attribute:paymentmethod",
    )
    assert op["selected"] is True
    assert op["id"].startswith("op-")
    assert op["params"]["entity"] == "Payment"
