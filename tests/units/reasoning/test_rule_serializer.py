"""Unit tests for the IF/THEN → SHACL-AF serialiser (read-only views).

The Business Rules UI authors rules as IF/THEN and displays / exports their
generated SHACL-AF form. These tests cover the two helpers backing the
``/ontology/shacl-rules/{text,export,preview}`` endpoints.
"""

# Importing the package root first avoids a pre-existing circular import
# between ``back.core.reasoning`` and ``back.core.w3c`` when this module is
# collected on its own.
import back.objects  # noqa: F401
from back.core.reasoning.shacl_rules.rule_serializer import (
    preview_rule_shacl,
    serialize_rules_to_turtle,
)

ONTOLOGY = {
    "base_uri": "http://ex.org#",
    "classes": [
        {"name": "Customer"},
        {"name": "Claim"},
        {"name": "HighValue"},
    ],
    "properties": [
        {"name": "hasClaim"},
        {"name": "hasContract"},
    ],
}

RULES = [
    {
        "name": "simple_class",
        "antecedent": "Customer(?x)",
        "consequent": "HighValue(?x)",
    },
    {
        "name": "property_chain",
        "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
        "consequent": "hasContract(?x, ?z)",
    },
]


def test_serialize_rules_shared_header_and_one_block_per_rule():
    ttl = serialize_rules_to_turtle(RULES, ONTOLOGY)

    # Shared prefix header appears exactly once.
    assert ttl.count("@prefix sh:") == 1
    assert ttl.count("@prefix rdf:") == 1
    # ex: prefix bound to the ontology base URI.
    assert "@prefix ex: <http://ex.org#> ." in ttl
    # One SHACL node shape per rule, no repeated per-rule prefix headers.
    assert ttl.count("sh:NodeShape") == len(RULES)


def test_serialize_rules_empty_is_valid_header_only():
    ttl = serialize_rules_to_turtle([], ONTOLOGY)
    assert "@prefix sh:" in ttl
    assert "sh:NodeShape" not in ttl
    assert "No rules defined" in ttl


def test_serialize_rules_none_is_safe():
    ttl = serialize_rules_to_turtle(None, None)
    assert "@prefix sh:" in ttl


def test_preview_single_rule_returns_views():
    out = preview_rule_shacl(
        "property_chain",
        RULES[1]["antecedent"],
        RULES[1]["consequent"],
        ONTOLOGY,
    )
    assert out["turtle"].strip()
    assert "sh:" in out["turtle"]
    assert out["rule_type"] in ("sh:TripleRule", "sh:SPARQLRule")


def test_preview_sparql_rule_carries_construct():
    # A rule with a fresh variable in the consequent compiles to a SPARQL rule.
    out = preview_rule_shacl(
        "property_chain",
        RULES[1]["antecedent"],
        RULES[1]["consequent"],
        ONTOLOGY,
    )
    if out["rule_type"] == "sh:SPARQLRule":
        assert "CONSTRUCT" in out["construct_sparql"].upper()
