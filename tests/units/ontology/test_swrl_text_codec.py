"""OntoBricks SWRL text serialize / parse round-trips and fail-closed import."""

import pytest

from back.core.reasoning.SWRLTextCodec import parse_rules, serialize_rules

SAMPLE = [
    {
        "name": "Claiming customer must have contract",
        "description": "If claim then contract",
        "antecedent": "Customer(?c) ^ hasClaim(?c, ?cl)",
        "consequent": "hasContract(?c, ?ct)",
    },
    {
        "name": "Payment with invoices",
        "description": "",
        "antecedent": "Payment(?p) ^ hasInvoice(?p, ?i)",
        "consequent": "relatedTo(?p, ?i)",
    },
]


def test_serialize_empty():
    assert serialize_rules([]) == ""


def test_roundtrip():
    text = serialize_rules(SAMPLE)
    assert "# Rule: Claiming customer must have contract" in text
    assert "->" in text
    back = parse_rules(text)
    assert len(back) == 2
    assert back[0]["name"] == SAMPLE[0]["name"]
    assert back[0]["antecedent"] == SAMPLE[0]["antecedent"]
    assert back[0]["consequent"] == SAMPLE[0]["consequent"]
    assert back[0]["description"] == SAMPLE[0]["description"]
    assert back[1]["description"] == ""


def test_missing_rule_name_synthesizes():
    text = "Customer(?c) -> VIP(?c)\n"
    rules = parse_rules(text)
    assert len(rules) == 1
    assert rules[0]["name"].startswith("Imported rule")


def test_bad_arrow_raises():
    with pytest.raises(ValueError, match="implication"):
        parse_rules("# Rule: Bad\nCustomer(?c)\n")


def test_empty_side_raises():
    with pytest.raises(ValueError, match="antecedent|consequent"):
        parse_rules("# Rule: Bad\n -> VIP(?c)\n")


def test_other_hash_comments_ignored():
    text = "# Rule: R1\n# note: ignored\nA(?x) -> B(?x)\n"
    rules = parse_rules(text)
    assert rules[0]["name"] == "R1"
    assert rules[0]["description"] == ""


def test_append_leaves_existing():
    existing = [SAMPLE[0]]
    incoming = parse_rules(serialize_rules([SAMPLE[1]]))
    merged = list(existing) + incoming
    assert len(merged) == 2
    assert merged[0]["name"] == SAMPLE[0]["name"]
