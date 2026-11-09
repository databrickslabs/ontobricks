"""Unit tests for the business-rules accept-suggestions validation helper.

The ``/ontology/business-rules/accept-suggestions`` route validates each
proposed rule with its engine validator before persisting. We test that
dispatch helper (``_validate_business_rule``) directly so we cover the
Simple (IF-THEN) / Complex (SPARQL) branches without a full HTTP round-trip.
"""

from __future__ import annotations

from api.routers.internal.ontology import _rule_signature, _validate_business_rule
from back.objects.ontology import Ontology


class TestRuleSignature:
    """The accept route skips a candidate whose signature already exists."""

    def test_if_then_signature_ignores_whitespace_and_glyph(self):
        a = {"name": "X", "antecedent": "Customer(?c) ^ holds(?c, ?ct)", "consequent": "Adult(?c)"}
        b = {"name": "Other", "antecedent": "Customer(?c)  \u2227 holds(?c,?ct)", "consequent": "Adult(?c)"}
        assert _rule_signature("if_then_rules", a) == _rule_signature("if_then_rules", b)

    def test_if_then_signature_differs_on_content(self):
        a = {"antecedent": "Customer(?c)", "consequent": "Adult(?c)"}
        b = {"antecedent": "Customer(?c)", "consequent": "Senior(?c)"}
        assert _rule_signature("if_then_rules", a) != _rule_signature("if_then_rules", b)

    def test_sparql_signature_ignores_whitespace(self):
        a = {"query": "CONSTRUCT { ?o a :Big } WHERE { ?o :total ?t }"}
        b = {"query": "CONSTRUCT  {  ?o a :Big }   WHERE { ?o :total ?t }"}
        assert _rule_signature("sparql_rules", a) == _rule_signature("sparql_rules", b)


class TestValidateBusinessRule:
    def test_valid_if_then_rule(self):
        rule = {"name": "Adult", "antecedent": "Customer(?c)", "consequent": "Adult(?c)"}
        assert _validate_business_rule("if_then_rules", rule) == []

    def test_invalid_if_then_rule_missing_consequent(self):
        rule = {"name": "Adult", "antecedent": "Customer(?c)"}
        errors = _validate_business_rule("if_then_rules", rule)
        assert errors and any("consequent" in e.lower() for e in errors)

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

    def test_unknown_rule_type(self):
        assert _validate_business_rule("mystery", {"name": "x"}) == ["Unknown rule type: mystery"]

    def test_if_then_with_known_refs_passes(self):
        rule = {
            "name": "Loyal",
            "antecedent": "Customer(?c) ^ loyaltyPoints(?c, ?lp) ^ builtin:greaterThanOrEqual(?lp, 1000)",
            "consequent": "LoyalCustomer(?c)",
        }
        # Consequent class must ALSO exist — no invented derived subtypes.
        classes = {"customer", "loyalcustomer"}
        props = {"loyaltypoints"}
        assert _validate_business_rule("if_then_rules", rule, classes, props) == []

    def test_if_then_unknown_antecedent_entity_rejected(self):
        rule = {
            "name": "Bad",
            "antecedent": "Widget(?w) ^ holds(?w, ?x)",
            "consequent": "BigWidget(?w)",
        }
        errors = _validate_business_rule("if_then_rules", rule, {"customer"}, {"holds"})
        assert any("Widget" in e for e in errors)

    def test_if_then_unknown_relationship_rejected(self):
        rule = {
            "name": "Bad",
            "antecedent": "Customer(?c) ^ frobnicates(?c, ?x)",
            "consequent": "FrobCustomer(?c)",
        }
        errors = _validate_business_rule("if_then_rules", rule, {"customer"}, {"holds"})
        assert any("frobnicates" in e for e in errors)


class TestIfThenReferenceErrors:
    def test_consequent_must_be_existing_class(self):
        rule = {
            "antecedent": "Customer(?c) ^ holds(?c, ?ct) ^ Contract(?ct)",
            "consequent": "EngagedCustomer(?c)",
        }
        # EngagedCustomer is not in the ontology → rejected (no invented classes).
        errors = Ontology.if_then_reference_errors(
            rule, {"customer", "contract"}, {"holds"}
        )
        assert any("EngagedCustomer" in e for e in errors)

    def test_consequent_existing_class_passes(self):
        rule = {
            "antecedent": "Customer(?c) ^ holds(?c, ?ct) ^ Contract(?ct)",
            "consequent": "Contract(?c)",
        }
        assert Ontology.if_then_reference_errors(
            rule, {"customer", "contract"}, {"holds"}
        ) == []

    def test_builtins_and_datatypes_ignored(self):
        rule = {
            "antecedent": "Customer(?c) ^ age(?c, ?a) ^ builtin:greaterThan(?a, 18) ^ xsd:integer(?a)",
            "consequent": "Adult(?c)",
        }
        # builtin:/xsd: atoms are skipped; only Customer/Adult/age are checked.
        assert Ontology.if_then_reference_errors(rule, {"customer", "adult"}, {"age"}) == []

    def test_consequent_unknown_property_rejected(self):
        rule = {
            "antecedent": "Customer(?c)",
            "consequent": "assignedTo(?c, ?x)",
        }
        errors = Ontology.if_then_reference_errors(rule, {"customer"}, {"holds"})
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
        errors = Ontology.if_then_reference_errors(
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
        errors = Ontology.if_then_reference_errors(
            rule,
            {"contract", "invoice", "payment"},
            {"generates", "settledby", "paidby"},
        )
        assert errors == []

    def test_relationship_inference_chain_passes(self):
        # Customer raised a Claim about a Contract → Customer holds Contract.
        # Closing the chain into a NEW existing relationship is the preferred
        # IF-THEN pattern and must validate.
        rule = {
            "antecedent": (
                "Customer(?c) ^ raised(?c, ?cl) ^ Claim(?cl) ^ "
                "relatedTo(?cl, ?ct) ^ Contract(?ct)"
            ),
            "consequent": "holds(?c, ?ct)",
        }
        errors = Ontology.if_then_reference_errors(
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
        errors = Ontology.if_then_reference_errors(
            rule, {"customer", "contract"}, {"holds", "owns"}
        )
        assert errors == []


class TestOtherRuleReferenceErrors:
    classes = {"customer", "order"}
    props = {"balance", "total", "placesorder"}

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
