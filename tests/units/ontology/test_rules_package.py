"""Pure rules package: build, header check, upsert, per-rule validation."""

from datetime import datetime, timezone

import pytest

from back.core.errors import ValidationError
from back.objects.ontology.RulesPackage import (
    FORMAT,
    VERSION,
    apply_package,
    build_package,
)

NOW = datetime(2026, 11, 10, 9, 0, 0, tzinfo=timezone.utc)

GOOD_IF_THEN = {
    "name": "LoyalCustomer",
    "description": "d",
    "antecedent": "Customer(?c) ^ loyaltyPoints(?c, ?p)",
    "consequent": "Loyal(?c)",
    "enabled": True,
}
GOOD_SPARQL = {
    "name": "CustomerHasClaim",
    "description": "d",
    "query": "CONSTRUCT { ?c <http://x/hasclaim> ?k } WHERE { ?c a <http://x/Customer> . ?c <http://x/hasclaim> ?k }",
    "enabled": True,
}


def _pkg(**over):
    base = {"format": FORMAT, "version": VERSION, "if_then_rules": [], "sparql_rules": []}
    base.update(over)
    return base


def test_build_package_has_documented_shape_and_keeps_disabled_rules():
    disabled = {**GOOD_IF_THEN, "enabled": False}
    pkg = build_package("Cust360Auto", [disabled], [GOOD_SPARQL], now=NOW)
    assert pkg["format"] == "ontobricks-rules"
    assert pkg["version"] == 1
    assert pkg["exported_at"] == "2026-11-10T09:00:00Z"
    assert pkg["domain"] == "Cust360Auto"
    assert pkg["if_then_rules"] == [disabled]
    assert pkg["sparql_rules"] == [GOOD_SPARQL]


def test_apply_rejects_wrong_format_and_future_version():
    with pytest.raises(ValidationError):
        apply_package({"format": "other", "version": 1}, [], [])
    with pytest.raises(ValidationError):
        apply_package({"format": FORMAT, "version": 2}, [], [])


def test_apply_rejects_non_object_document():
    with pytest.raises(ValidationError):
        apply_package(["not", "an", "object"], [], [])


def test_apply_adds_new_rules_and_replaces_same_name_in_place():
    existing = [{**GOOD_IF_THEN, "name": "A", "description": "old"},
                {**GOOD_IF_THEN, "name": "B"}]
    incoming = {**GOOD_IF_THEN, "name": "A", "description": "new"}
    result = apply_package(_pkg(if_then_rules=[incoming, {**GOOD_IF_THEN, "name": "C"}]),
                           existing, [])
    assert [r["name"] for r in result.if_then_rules] == ["A", "B", "C"]
    assert result.if_then_rules[0]["description"] == "new"
    assert result.added == ["C"]
    assert result.replaced == ["A"]
    assert ("if_then_rules", "replaced", "A") in result.changes
    assert ("if_then_rules", "added", "C") in result.changes


def test_invalid_simple_rule_is_rejected_and_others_still_import():
    bad = {"name": "Bad", "antecedent": "", "consequent": "X(?c)"}
    result = apply_package(_pkg(if_then_rules=[bad, GOOD_IF_THEN]), [], [])
    assert [r["name"] for r in result.if_then_rules] == ["LoyalCustomer"]
    assert result.rejected and result.rejected[0]["name"] == "Bad"
    assert result.rejected[0]["type"] == "if_then_rules"
    assert "antecedent" in result.rejected[0]["reason"]


def test_invalid_enabled_complex_rule_is_rejected():
    bad = {"name": "BadQuery", "query": "NOT A QUERY", "enabled": True}
    result = apply_package(_pkg(sparql_rules=[bad]), [], [])
    assert result.sparql_rules == []
    assert result.rejected and result.rejected[0]["type"] == "sparql_rules"


def test_disabled_complex_rule_skips_the_compile_check():
    off = {"name": "Off", "query": "NOT A QUERY", "enabled": False}
    result = apply_package(_pkg(sparql_rules=[off]), [], [])
    assert [r["name"] for r in result.sparql_rules] == ["Off"]
    assert result.rejected == []


def test_rule_without_name_is_rejected():
    result = apply_package(_pkg(if_then_rules=[{**GOOD_IF_THEN, "name": ""}]), [], [])
    assert result.if_then_rules == []
    assert result.rejected


def test_apply_accepts_version_zero_and_one():
    for version in (0, 1):
        result = apply_package(_pkg(version=version, if_then_rules=[GOOD_IF_THEN]), [], [])
        assert [r["name"] for r in result.if_then_rules] == ["LoyalCustomer"]


def test_apply_rejects_missing_or_non_int_version():
    with pytest.raises(ValidationError):
        apply_package({"format": FORMAT, "version": None}, [], [])
    with pytest.raises(ValidationError):
        apply_package({"format": FORMAT, "version": "1"}, [], [])


def test_apply_treats_missing_rule_lists_as_empty():
    result = apply_package({"format": FORMAT, "version": 1}, [GOOD_IF_THEN], [GOOD_SPARQL])
    assert result.if_then_rules == [GOOD_IF_THEN]
    assert result.sparql_rules == [GOOD_SPARQL]
    assert result.added == result.replaced == result.rejected == []


def test_apply_rejects_non_list_rule_collections():
    with pytest.raises(ValidationError, match="if_then_rules"):
        apply_package(_pkg(if_then_rules={"name": "x"}), [], [])
    with pytest.raises(ValidationError, match="sparql_rules"):
        apply_package(_pkg(sparql_rules="bad"), [], [])


def test_apply_upserts_complex_rules_by_name_in_place():
    existing = [{**GOOD_SPARQL, "name": "A", "description": "old"},
                {**GOOD_SPARQL, "name": "B"}]
    incoming = {**GOOD_SPARQL, "name": "A", "description": "new"}
    result = apply_package(
        _pkg(sparql_rules=[incoming, {**GOOD_SPARQL, "name": "C"}]),
        [],
        existing,
    )
    assert [r["name"] for r in result.sparql_rules] == ["A", "B", "C"]
    assert result.sparql_rules[0]["description"] == "new"
    assert result.added == ["C"]
    assert result.replaced == ["A"]
    assert ("sparql_rules", "replaced", "A") in result.changes
    assert ("sparql_rules", "added", "C") in result.changes


def test_apply_imports_simple_and_complex_together():
    result = apply_package(
        _pkg(if_then_rules=[GOOD_IF_THEN], sparql_rules=[GOOD_SPARQL]),
        [],
        [],
    )
    assert [r["name"] for r in result.if_then_rules] == ["LoyalCustomer"]
    assert [r["name"] for r in result.sparql_rules] == ["CustomerHasClaim"]
    assert set(result.added) == {"LoyalCustomer", "CustomerHasClaim"}
    assert result.rejected == []


def test_apply_rejects_non_object_rule_entries():
    result = apply_package(_pkg(if_then_rules=["not-a-rule", GOOD_IF_THEN]), [], [])
    assert [r["name"] for r in result.if_then_rules] == ["LoyalCustomer"]
    assert any(r["name"] == "" for r in result.rejected)


def test_complex_rule_without_name_or_query_is_rejected():
    no_name = apply_package(_pkg(sparql_rules=[{**GOOD_SPARQL, "name": ""}]), [], [])
    assert no_name.sparql_rules == []
    assert no_name.rejected[0]["reason"] == "Rule name is required"

    no_query = apply_package(
        _pkg(sparql_rules=[{"name": "Empty", "query": "", "enabled": True}]), [], []
    )
    assert no_query.sparql_rules == []
    assert "query" in no_query.rejected[0]["reason"].lower()


def test_build_package_copies_rules_and_defaults_empty_lists():
    rules = [GOOD_IF_THEN]
    pkg = build_package("D", rules, None, now=NOW)
    pkg["if_then_rules"][0]["name"] = "mutated"
    assert rules[0]["name"] == "LoyalCustomer"
    assert pkg["sparql_rules"] == []


def test_empty_import_leaves_existing_rules_untouched():
    existing_if = [GOOD_IF_THEN]
    existing_sp = [GOOD_SPARQL]
    result = apply_package(_pkg(), existing_if, existing_sp)
    assert result.if_then_rules == existing_if
    assert result.sparql_rules == existing_sp
    assert result.changes == []
