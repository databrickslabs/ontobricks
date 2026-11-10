"""The IF-THEN save path and the rules import share one validator."""

from back.objects.ontology.Rules import Rules


def test_validator_reports_each_missing_field():
    errors = Rules.validate_if_then_rule({"name": "", "antecedent": "", "consequent": ""})
    assert "Rule name is required" in errors
    assert "Rule antecedent is required" in errors
    assert "Rule consequent is required" in errors


def test_validator_accepts_a_complete_rule():
    rule = {"name": "R", "antecedent": "Customer(?c)", "consequent": "Payment(?c)"}
    assert Rules.validate_if_then_rule(rule) == []


def test_validator_reports_only_the_missing_fields():
    errors = Rules.validate_if_then_rule(
        {"name": "R", "antecedent": "Customer(?c)", "consequent": ""}
    )
    assert errors == ["Rule consequent is required"]


def test_validator_treats_falsy_name_as_missing():
    # Empty string and None both fail the shared name check.
    errors = Rules.validate_if_then_rule(
        {"name": None, "antecedent": "A(?x)", "consequent": "B(?x)"}
    )
    assert "Rule name is required" in errors
