"""Unit tests for Design Autofix apply engine."""

from __future__ import annotations

import copy

import pytest

pytestmark = pytest.mark.unit

from back.objects.ontology.autofix_apply import apply_operations  # noqa: E402
from back.objects.ontology.autofix_ops import make_operation  # noqa: E402

BASE = "http://test.org/ontology#"


def _load_minimal(ps):
    ps._data["ontology"].update(
        {
            "base_uri": BASE,
            "name": "test",
            "classes": [
                {
                    "name": "Payment",
                    "uri": BASE + "Payment",
                    "label": "Payment",
                    "dataProperties": [],
                },
                {
                    "name": "Invoice",
                    "uri": BASE + "Invoice",
                    "label": "Invoice",
                    "parent": "Ghost",
                    "dataProperties": [],
                },
            ],
            "properties": [
                {
                    "name": "paymentmethod",
                    "uri": BASE + "paymentmethod",
                    "type": "DatatypeProperty",
                    "label": "paymentmethod",
                    "domain": "",
                },
                {
                    "name": "hasOrder",
                    "uri": BASE + "hasOrder",
                    "type": "ObjectProperty",
                    "domain": "Payment",
                    "range": "Invoice",
                },
            ],
            "groups": [{"name": "G1", "members": ["Missing", "Payment"]}],
        }
    )
    ps._data["assignment"]["entities"] = [
        {"ontology_class": BASE + "Payment", "table": "payments"}
    ]
    ps._data["assignment"]["relationships"] = []


def test_attach_attribute_updates_domain_and_dataproperties(domain_session):
    _load_minimal(domain_session)
    op = make_operation(
        op="attach_attribute",
        target={"kind": "attribute", "name": "paymentmethod"},
        params={"entity": "Payment"},
        rationale="test",
        source="ai",
        finding_id="DC-W03:attribute:paymentmethod",
    )
    result = apply_operations(domain_session, [op])
    assert result["success"]
    prop = next(
        p for p in domain_session.get_properties() if p["name"] == "paymentmethod"
    )
    assert prop["domain"] == "Payment"
    cls = next(c for c in domain_session.get_classes() if c["name"] == "Payment")
    assert any(a["name"] == "paymentmethod" for a in cls.get("dataProperties", []))


def test_remove_property_prunes(domain_session):
    _load_minimal(domain_session)
    # Attach first so dataProperties has it, then remove.
    apply_operations(
        domain_session,
        [
            make_operation(
                op="attach_attribute",
                target={"kind": "attribute", "name": "paymentmethod"},
                params={"entity": "Payment"},
                rationale="t",
                source="ai",
                finding_id="f1",
            )
        ],
    )
    result = apply_operations(
        domain_session,
        [
            make_operation(
                op="remove_property",
                target={"kind": "attribute", "name": "paymentmethod"},
                rationale="t",
                source="fallback",
                finding_id="f2",
            )
        ],
    )
    assert result["success"]
    assert all(p["name"] != "paymentmethod" for p in domain_session.get_properties())
    cls = next(c for c in domain_session.get_classes() if c["name"] == "Payment")
    assert all(a["name"] != "paymentmethod" for a in cls.get("dataProperties", []))


def test_clear_parent(domain_session):
    _load_minimal(domain_session)
    result = apply_operations(
        domain_session,
        [
            make_operation(
                op="clear_parent",
                target={"kind": "class", "name": "Invoice"},
                rationale="broken",
                source="rule",
                finding_id="DC-E01:class:Invoice",
            )
        ],
    )
    assert result["success"]
    inv = next(c for c in domain_session.get_classes() if c["name"] == "Invoice")
    assert not inv.get("parent")


def test_rename_remaps_mappings(domain_session):
    _load_minimal(domain_session)
    result = apply_operations(
        domain_session,
        [
            make_operation(
                op="rename",
                target={"kind": "class", "name": "Payment"},
                params={"new_name": "Payout"},
                rationale="rename",
                source="rule",
                finding_id="DC-I02:class:Payment",
            )
        ],
    )
    assert result["success"]
    assert any(c["name"] == "Payout" for c in domain_session.get_classes())
    assert domain_session.get_entity_mappings()[0]["ontology_class"] == BASE + "Payout"
    assert result["uri_renames"]["classes"].get(BASE + "Payment") == BASE + "Payout"


def test_stale_target_rejected(domain_session):
    _load_minimal(domain_session)
    result = apply_operations(
        domain_session,
        [
            make_operation(
                op="clear_parent",
                target={"kind": "class", "name": "Gone"},
                rationale="stale",
                source="rule",
                finding_id="DC-E01:class:Gone",
            )
        ],
    )
    assert result["success"] is False
    # Original data untouched
    assert any(c["name"] == "Invoice" for c in domain_session.get_classes())


def test_atomic_no_partial_apply(domain_session):
    _load_minimal(domain_session)
    before = copy.deepcopy(domain_session.get_classes())
    result = apply_operations(
        domain_session,
        [
            make_operation(
                op="clear_parent",
                target={"kind": "class", "name": "Invoice"},
                rationale="ok",
                source="rule",
                finding_id="f1",
            ),
            make_operation(
                op="clear_parent",
                target={"kind": "class", "name": "Gone"},
                rationale="fail",
                source="rule",
                finding_id="f2",
            ),
        ],
    )
    assert result["success"] is False
    assert domain_session.get_classes() == before


def test_attach_duplicate_rejected(domain_session):
    _load_minimal(domain_session)
    op = make_operation(
        op="attach_attribute",
        target={"kind": "attribute", "name": "paymentmethod"},
        params={"entity": "Payment"},
        rationale="t",
        source="ai",
        finding_id="f1",
    )
    assert apply_operations(domain_session, [op])["success"]
    result = apply_operations(domain_session, [op])
    assert result["success"] is False
    assert "already" in result["error"].lower()


def test_remove_group_member(domain_session):
    _load_minimal(domain_session)
    result = apply_operations(
        domain_session,
        [
            make_operation(
                op="remove_group_member",
                target={"kind": "group", "name": "G1"},
                params={"group": "G1", "member": "Missing"},
                rationale="t",
                source="rule",
                finding_id="DC-E04:group:G1",
            )
        ],
    )
    assert result["success"]
    group = next(g for g in domain_session.groups if g["name"] == "G1")
    assert group["members"] == ["Payment"]
