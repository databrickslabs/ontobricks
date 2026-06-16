"""Unit tests for the business-rules accept-suggestions validation helper.

The ``/ontology/business-rules/accept-suggestions`` route validates each
proposed rule with its engine validator before persisting. We test that
dispatch helper (``_validate_business_rule``) directly so we cover the
SWRL / decision-table / SPARQL / aggregate branches without a full HTTP
round-trip.
"""

from __future__ import annotations

from api.routers.internal.ontology import _rule_signature, _validate_business_rule
from back.objects.ontology import Ontology


class TestRuleSignature:
    """The accept route skips a candidate whose signature already exists."""

    def test_swrl_signature_ignores_whitespace_and_glyph(self):
        a = {"name": "X", "antecedent": "Customer(?c) ^ holds(?c, ?ct)", "consequent": "Adult(?c)"}
        b = {"name": "Other", "antecedent": "Customer(?c)  \u2227 holds(?c,?ct)", "consequent": "Adult(?c)"}
        assert _rule_signature("swrl_rules", a) == _rule_signature("swrl_rules", b)

    def test_swrl_signature_differs_on_content(self):
        a = {"antecedent": "Customer(?c)", "consequent": "Adult(?c)"}
        b = {"antecedent": "Customer(?c)", "consequent": "Senior(?c)"}
        assert _rule_signature("swrl_rules", a) != _rule_signature("swrl_rules", b)

    def test_sparql_signature_ignores_whitespace(self):
        a = {"query": "CONSTRUCT { ?o a :Big } WHERE { ?o :total ?t }"}
        b = {"query": "CONSTRUCT  {  ?o a :Big }   WHERE { ?o :total ?t }"}
        assert _rule_signature("sparql_rules", a) == _rule_signature("sparql_rules", b)

    def test_aggregate_signature_matches_on_fields(self):
        a = {
            "name": "A",
            "target_class": "Customer",
            "group_by_property": "placesOrder",
            "aggregate_function": "count",
            "operator": "gte",
            "threshold": "10",
            "result_class": "FrequentBuyer",
        }
        b = dict(a, name="B")  # different name, same logical rule
        assert _rule_signature("aggregate_rules", a) == _rule_signature("aggregate_rules", b)

    def test_decision_table_signature_matches_on_structure(self):
        a = {
            "name": "RiskTier",
            "target_class": "Account",
            "input_columns": [{"property": "balance"}],
            "output_column": {"property": "riskTier"},
            "rows": [{"conditions": [{"op": "gt", "value": "100000"}], "action_value": "high"}],
        }
        b = dict(a, name="RiskTierCopy")
        assert _rule_signature("decision_tables", a) == _rule_signature("decision_tables", b)


class TestValidateBusinessRule:
    def test_valid_swrl_rule(self):
        rule = {"name": "Adult", "antecedent": "Customer(?c)", "consequent": "Adult(?c)"}
        assert _validate_business_rule("swrl_rules", rule) == []

    def test_invalid_swrl_rule_missing_consequent(self):
        rule = {"name": "Adult", "antecedent": "Customer(?c)"}
        errors = _validate_business_rule("swrl_rules", rule)
        assert errors and any("consequent" in e.lower() for e in errors)

    def test_valid_decision_table(self):
        rule = {
            "name": "RiskTier",
            "target_class": "Customer",
            "input_columns": [{"property": "balance"}],
            "rows": [{"conditions": [{"op": "gt", "value": "100"}], "action_value": "high"}],
        }
        assert _validate_business_rule("decision_tables", rule) == []

    def test_invalid_decision_table_row_arity(self):
        rule = {
            "name": "Bad",
            "target_class": "Customer",
            "input_columns": [{"property": "a"}, {"property": "b"}],
            "rows": [{"conditions": [{"op": "gt", "value": "1"}]}],
        }
        errors = _validate_business_rule("decision_tables", rule)
        assert errors

    def test_valid_sparql_rule(self):
        rule = {
            "name": "Big",
            "query": "CONSTRUCT { ?o a :Big } WHERE { ?o a :Order . ?o :total ?t }",
        }
        assert _validate_business_rule("sparql_rules", rule) == []

    def test_invalid_sparql_rule_not_construct(self):
        rule = {"name": "Bad", "query": "SELECT ?s WHERE { ?s ?p ?o }"}
        errors = _validate_business_rule("sparql_rules", rule)
        assert errors

    def test_valid_aggregate_rule(self):
        rule = {
            "name": "Frequent",
            "target_class": "Customer",
            "group_by_property": "placesOrder",
            "aggregate_function": "count",
            "operator": "gte",
            "threshold": "10",
        }
        assert _validate_business_rule("aggregate_rules", rule) == []

    def test_invalid_aggregate_function(self):
        rule = {"name": "Bad", "target_class": "Customer", "aggregate_function": "median"}
        errors = _validate_business_rule("aggregate_rules", rule)
        assert errors

    def test_unknown_rule_type(self):
        assert _validate_business_rule("mystery", {"name": "x"}) == ["Unknown rule type: mystery"]

    def test_swrl_with_known_refs_passes(self):
        rule = {
            "name": "Loyal",
            "antecedent": "Customer(?c) ^ loyaltyPoints(?c, ?lp) ^ swrlb:greaterThanOrEqual(?lp, 1000)",
            "consequent": "LoyalCustomer(?c)",
        }
        # Consequent class must ALSO exist — no invented derived subtypes.
        classes = {"customer", "loyalcustomer"}
        props = {"loyaltypoints"}
        assert _validate_business_rule("swrl_rules", rule, classes, props) == []

    def test_swrl_unknown_antecedent_entity_rejected(self):
        rule = {
            "name": "Bad",
            "antecedent": "Widget(?w) ^ holds(?w, ?x)",
            "consequent": "BigWidget(?w)",
        }
        errors = _validate_business_rule("swrl_rules", rule, {"customer"}, {"holds"})
        assert any("Widget" in e for e in errors)

    def test_swrl_unknown_relationship_rejected(self):
        rule = {
            "name": "Bad",
            "antecedent": "Customer(?c) ^ frobnicates(?c, ?x)",
            "consequent": "FrobCustomer(?c)",
        }
        errors = _validate_business_rule("swrl_rules", rule, {"customer"}, {"holds"})
        assert any("frobnicates" in e for e in errors)


class TestSwrlReferenceErrors:
    def test_consequent_must_be_existing_class(self):
        rule = {
            "antecedent": "Customer(?c) ^ holds(?c, ?ct) ^ Contract(?ct)",
            "consequent": "EngagedCustomer(?c)",
        }
        # EngagedCustomer is not in the ontology → rejected (no invented classes).
        errors = Ontology.swrl_reference_errors(
            rule, {"customer", "contract"}, {"holds"}
        )
        assert any("EngagedCustomer" in e for e in errors)

    def test_consequent_existing_class_passes(self):
        rule = {
            "antecedent": "Customer(?c) ^ holds(?c, ?ct) ^ Contract(?ct)",
            "consequent": "Contract(?c)",
        }
        assert Ontology.swrl_reference_errors(
            rule, {"customer", "contract"}, {"holds"}
        ) == []

    def test_builtins_and_datatypes_ignored(self):
        rule = {
            "antecedent": "Customer(?c) ^ age(?c, ?a) ^ swrlb:greaterThan(?a, 18) ^ xsd:integer(?a)",
            "consequent": "Adult(?c)",
        }
        # swrlb:/xsd: atoms are skipped; only Customer/Adult/age are checked.
        assert Ontology.swrl_reference_errors(rule, {"customer", "adult"}, {"age"}) == []

    def test_consequent_unknown_property_rejected(self):
        rule = {
            "antecedent": "Customer(?c)",
            "consequent": "assignedTo(?c, ?x)",
        }
        errors = Ontology.swrl_reference_errors(rule, {"customer"}, {"holds"})
        assert any("assignedTo" in e for e in errors)

    def test_tautological_consequent_rejected(self):
        # The consequent only repeats an atom already in the IF → useless.
        rule = {
            "antecedent": (
                "Contract(?c) ^ generates(?c, ?i) ^ Invoice(?i) ^ "
                "settledBy(?i, ?p) ^ Payment(?p)"
            ),
            "consequent": "Invoice(?i)",
        }
        errors = Ontology.swrl_reference_errors(
            rule,
            {"contract", "invoice", "payment"},
            {"generates", "settledby"},
        )
        assert any("infers nothing new" in e for e in errors)

    def test_consequent_new_relationship_passes(self):
        # Concluding a NEW (existing) property between bound entities is fine.
        rule = {
            "antecedent": (
                "Contract(?c) ^ generates(?c, ?i) ^ Invoice(?i) ^ "
                "settledBy(?i, ?p) ^ Payment(?p)"
            ),
            "consequent": "paidBy(?c, ?p)",
        }
        errors = Ontology.swrl_reference_errors(
            rule,
            {"contract", "invoice", "payment"},
            {"generates", "settledby", "paidby"},
        )
        assert errors == []

    def test_relationship_inference_chain_passes(self):
        # Customer raised a Claim about a Contract → Customer holds Contract.
        # Closing the chain into a NEW existing relationship is the preferred
        # SWRL pattern and must validate.
        rule = {
            "antecedent": (
                "Customer(?c) ^ raised(?c, ?cl) ^ Claim(?cl) ^ "
                "relatedTo(?cl, ?ct) ^ Contract(?ct)"
            ),
            "consequent": "holds(?c, ?ct)",
        }
        errors = Ontology.swrl_reference_errors(
            rule,
            {"customer", "claim", "contract"},
            {"raised", "relatedto", "holds"},
        )
        assert errors == []

    def test_partial_repeat_consequent_passes(self):
        # One repeated atom but also a new one → still infers something new.
        rule = {
            "antecedent": "Customer(?c) ^ holds(?c, ?ct) ^ Contract(?ct)",
            "consequent": "Customer(?c) ^ owns(?c, ?ct)",
        }
        errors = Ontology.swrl_reference_errors(
            rule, {"customer", "contract"}, {"holds", "owns"}
        )
        assert errors == []


class TestOtherRuleReferenceErrors:
    classes = {"customer", "order"}
    props = {"balance", "total", "placesorder"}

    def test_decision_table_unknown_target_class_rejected(self):
        rule = {"target_class": "Widget", "input_columns": [{"property": "balance"}]}
        errors = Ontology.decision_table_reference_errors(rule, self.classes, self.props)
        assert any("Widget" in e for e in errors)

    def test_decision_table_unknown_input_property_rejected(self):
        rule = {"target_class": "Customer", "input_columns": [{"property": "ghost"}]}
        errors = Ontology.decision_table_reference_errors(rule, self.classes, self.props)
        assert any("ghost" in e for e in errors)

    def test_decision_table_unknown_output_property_rejected(self):
        rule = {
            "target_class": "Customer",
            "input_columns": [{"property": "balance"}],
            "output_column": {"property": "newRiskTier"},
        }
        errors = Ontology.decision_table_reference_errors(rule, self.classes, self.props)
        assert any("newRiskTier" in e for e in errors)

    def test_decision_table_known_passes(self):
        rule = {
            "target_class": "Customer",
            "input_columns": [{"property": "balance"}],
            "output_column": {"property": "total"},
        }
        assert Ontology.decision_table_reference_errors(rule, self.classes, self.props) == []

    def test_aggregate_unknown_group_property_rejected(self):
        rule = {"target_class": "Customer", "group_by_property": "ghostRel"}
        errors = Ontology.aggregate_reference_errors(rule, self.classes, self.props)
        assert any("ghostRel" in e for e in errors)

    def test_aggregate_unknown_result_class_rejected(self):
        rule = {
            "target_class": "Customer",
            "group_by_property": "placesOrder",
            "result_class": "FrequentBuyer",
        }
        errors = Ontology.aggregate_reference_errors(rule, self.classes, self.props)
        assert any("FrequentBuyer" in e for e in errors)

    def test_aggregate_existing_result_class_passes(self):
        rule = {
            "target_class": "Customer",
            "group_by_property": "placesOrder",
            "result_class": "Order",
        }
        assert Ontology.aggregate_reference_errors(rule, self.classes, self.props) == []

    def test_sparql_where_unknown_entity_rejected(self):
        rule = {"query": "CONSTRUCT { ?o a :Order } WHERE { ?o a :Widget . ?o :total ?t }"}
        errors = Ontology.sparql_reference_errors(rule, self.classes, self.props)
        assert any("Widget" in e for e in errors)

    def test_sparql_where_unknown_property_rejected(self):
        rule = {"query": "CONSTRUCT { ?o a :Order } WHERE { ?o a :Order . ?o :ghostProp ?t }"}
        errors = Ontology.sparql_reference_errors(rule, self.classes, self.props)
        assert any("ghostProp" in e for e in errors)

    def test_sparql_construct_head_unknown_entity_rejected(self):
        rule = {"query": "CONSTRUCT { ?o a :LargeOrder } WHERE { ?o a :Order . ?o :total ?t }"}
        errors = Ontology.sparql_reference_errors(rule, self.classes, self.props)
        assert any("LargeOrder" in e for e in errors)

    def test_sparql_all_existing_passes(self):
        rule = {
            "query": "CONSTRUCT { ?o a :Order } WHERE { ?o a :Order . ?o :total ?t . FILTER(?t > 10000) }"
        }
        assert Ontology.sparql_reference_errors(rule, self.classes, self.props) == []
