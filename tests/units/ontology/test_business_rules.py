"""Tests for all business rule enhancements.

Covers:
- Phase 1: IF-THEN built-ins (registry, parser, SQL translation)
- Phase 2: SHACL rules (service method)
- Phase 3: Closed-world negation (parser, SQL translation)
- Phase 5: SPARQL CONSTRUCT rules (engine, SQL generation, validation)
- Cross-cutting: RuleViolation model, ReasoningService orchestration
"""

import pytest
from unittest.mock import MagicMock, patch

from back.core.reasoning.results.InferredTriple import InferredTriple
from back.core.reasoning.results.ReasoningResult import ReasoningResult
from back.core.reasoning.results.RuleViolation import RuleViolation
from back.core.reasoning.shacl_rules.RuleExpressionParser import RuleExpressionParser
from back.core.reasoning.shacl_rules.ShaclFlatSqlBackend import ShaclFlatSqlBackend
from back.core.reasoning.shacl_rules.RuleBuiltin import RuleBuiltin
from back.core.reasoning.shacl_rules.RuleBuiltinRegistry import RuleBuiltinRegistry
from back.core.reasoning.rules.SPARQLRuleEngine import SPARQLRuleEngine
from back.core.reasoning.ReasoningService import ReasoningService


# ===========================================================================
# Phase 1: IF-THEN Built-ins
# ===========================================================================


class TestRuleBuiltinRegistry:
    def test_get_builtin_comparison(self):
        bi = RuleBuiltinRegistry.get("greaterThan")
        assert bi is not None
        assert bi.arity == 2
        assert bi.category == "comparison"

    def test_get_builtin_case_insensitive(self):
        assert RuleBuiltinRegistry.get("GREATERTHAN") is not None
        assert RuleBuiltinRegistry.get("GreaterThan") is not None

    def test_is_builtin(self):
        assert RuleBuiltinRegistry.is_builtin("greaterThan")
        assert RuleBuiltinRegistry.is_builtin("add")
        assert RuleBuiltinRegistry.is_builtin("startsWith")
        assert RuleBuiltinRegistry.is_builtin("before")
        assert not RuleBuiltinRegistry.is_builtin("Customer")
        assert not RuleBuiltinRegistry.is_builtin("hasAge")

    def test_all_builtins_non_empty(self):
        builtins = RuleBuiltinRegistry.all()
        assert len(builtins) > 20

    def test_builtins_by_category(self):
        cats = RuleBuiltinRegistry.by_category()
        assert "comparison" in cats
        assert "math" in cats
        assert "string" in cats
        assert "date" in cats
        assert len(cats["comparison"]) >= 6

    def test_literal_detection(self):
        assert RuleBuiltinRegistry.is_literal("18")
        assert RuleBuiltinRegistry.is_literal('"hello"')
        assert RuleBuiltinRegistry.is_literal("'world'")
        assert not RuleBuiltinRegistry.is_literal("?x")
        assert not RuleBuiltinRegistry.is_literal("?age")

    def test_literal_sql(self):
        assert RuleBuiltinRegistry.literal_sql("18") == "18"
        assert RuleBuiltinRegistry.literal_sql('"hello"') == "'hello'"
        assert RuleBuiltinRegistry.literal_sql("Gold") == "'Gold'"

    def test_literal_cypher(self):
        assert RuleBuiltinRegistry.literal_cypher("18") == "18"
        assert RuleBuiltinRegistry.literal_cypher('"hello"') == '"hello"'
        assert RuleBuiltinRegistry.literal_cypher("Gold") == '"Gold"'

    def test_builtin_sql_templates(self):
        bi = RuleBuiltinRegistry.get("greaterThan")
        result = bi.sql_template.format("col1", "42")
        assert "col1" in result
        assert "42" in result
        assert ">" in result

    def test_builtin_cypher_templates(self):
        bi = RuleBuiltinRegistry.get("contains")
        result = bi.cypher_template.format("x.name", '"VIP"')
        assert "contains" in result


class TestParseIfThenAtomsWithBuiltins:
    def test_builtin_atoms_detected(self):
        expr = "Customer(?x), hasAge(?x, ?age), greaterThan(?age, 65)"
        atoms = RuleExpressionParser.parse_atoms(expr)
        assert len(atoms) == 3
        builtin_atoms = [a for a in atoms if a.get("builtin")]
        assert len(builtin_atoms) == 1
        assert builtin_atoms[0]["name"] == "greaterThan"
        assert builtin_atoms[0]["args"] == ["?age", "65"]

    def test_negated_atoms_detected(self):
        expr = "Customer(?x), not(hasPayment(?x, ?y))"
        atoms = RuleExpressionParser.parse_atoms(expr)
        assert len(atoms) == 2
        negated = [a for a in atoms if a.get("negated")]
        assert len(negated) == 1
        assert negated[0]["name"] == "hasPayment"
        assert negated[0]["args"] == ["?x", "?y"]

    def test_mixed_atoms(self):
        expr = (
            "Person(?x), hasAge(?x, ?a), greaterThan(?a, 18), not(hasLicense(?x, ?l))"
        )
        atoms = RuleExpressionParser.parse_atoms(expr)
        regular = [a for a in atoms if not a.get("builtin") and not a.get("negated")]
        builtins = [a for a in atoms if a.get("builtin")]
        negated = [a for a in atoms if a.get("negated")]
        assert len(regular) == 2
        assert len(builtins) == 1
        assert len(negated) == 1

    def test_no_builtins_backward_compatible(self):
        expr = "Customer(?x), hasOrder(?x, ?o), Order(?o)"
        atoms = RuleExpressionParser.parse_atoms(expr)
        assert all(not a.get("builtin") for a in atoms)
        assert all(not a.get("negated") for a in atoms)
        assert len(atoms) == 3


class TestIfThenSQLBuiltinTranslation:
    def test_violation_sql_with_builtin(self):
        translator = ShaclFlatSqlBackend()
        params = {
            "antecedent": "Customer(?x), hasAge(?x, ?age), greaterThan(?age, 65)",
            "consequent": "SeniorCustomer(?x)",
            "base_uri": "http://test.org/ontology#",
            "uri_map": {
                "customer": "http://test.org/ontology#Customer",
                "seniorcustomer": "http://test.org/ontology#SeniorCustomer",
                "hasage": "http://test.org/ontology/hasAge",
            },
        }
        sql = translator.build_violation_sql("triples", params)
        assert sql is not None
        assert "CAST" in sql or ">" in sql
        assert "65" in sql
        assert "SELECT DISTINCT" in sql


# ===========================================================================
# Phase 3: Closed-World Negation
# ===========================================================================


class TestNegationSQL:
    def test_negated_antecedent_sql(self):
        translator = ShaclFlatSqlBackend()
        params = {
            "antecedent": "Customer(?x), not(hasPayment(?x, ?y))",
            "consequent": "UnpaidCustomer(?x)",
            "base_uri": "http://test.org/ontology#",
            "uri_map": {
                "customer": "http://test.org/ontology#Customer",
                "unpaidcustomer": "http://test.org/ontology#UnpaidCustomer",
                "haspayment": "http://test.org/ontology/hasPayment",
            },
        }
        sql = translator.build_violation_sql("triples", params)
        assert sql is not None
        assert "NOT EXISTS" in sql


# ===========================================================================
# Phase 5: SPARQL CONSTRUCT Rules
# ===========================================================================


class TestSPARQLRuleEngine:
    @pytest.fixture
    def sample_rule(self):
        return {
            "id": "sr_test",
            "name": "High Value Customer",
            "query": (
                "CONSTRUCT { ?p rdf:type <http://test.org/HighValue> } "
                "WHERE { ?p rdf:type <http://test.org/Customer> . "
                "?p <http://test.org/purchases> ?amt . "
                "FILTER(?amt > 100000) }"
            ),
            "enabled": True,
        }

    def test_validate_valid_rule(self, sample_rule):
        errors = SPARQLRuleEngine.validate_rule(sample_rule)
        assert errors == []

    def test_validate_missing_name(self):
        errors = SPARQLRuleEngine.validate_rule({"query": "CONSTRUCT {} WHERE {}"})
        assert any("name" in e.lower() for e in errors)

    def test_validate_non_construct(self):
        errors = SPARQLRuleEngine.validate_rule(
            {"name": "Test", "query": "SELECT * WHERE { ?s ?p ?o }"}
        )
        assert any("CONSTRUCT" in e for e in errors)

    def test_validate_rejects_unsupported_construct(self):
        errors = SPARQLRuleEngine.validate_rule(
            {
                "name": "Opt",
                "query": "CONSTRUCT { ?o a :X } WHERE { ?o a :Order OPTIONAL { ?o :n ?n } }",
            }
        )
        assert errors == ["Complex rules do not support OPTIONAL."]

    def test_execute_reports_uncompilable_rule_as_error(self):
        store = MagicMock()
        store.sql_flavor.return_value = "spark"
        store.sql_table_reference.return_value = "triples"
        rule = {"name": "Opt", "query": "CONSTRUCT { ?o a :X } WHERE { ?o a :O } LIMIT 1"}
        result = SPARQLRuleEngine().execute_rules([rule], store, "t", {})
        store.execute_query.assert_not_called()
        assert result.stats["errors"] == 1
        assert "LIMIT" in result.stats["error_details"]

    def test_execute_returns_inferred_rows(self, sample_rule):
        store = MagicMock()
        store.sql_flavor.return_value = "spark"
        store.sql_table_reference.return_value = "triples"
        store.execute_query.return_value = [
            {"s": "http://x/1", "p": "http://p", "o": "http://test.org/HighValue"}
        ]
        result = SPARQLRuleEngine().execute_rules([sample_rule], store, "t", {})
        assert [t.provenance for t in result.inferred_triples] == [
            "sparql:High Value Customer"
        ]


# ===========================================================================
# Cross-cutting: Models
# ===========================================================================


class TestRuleViolationModel:
    def test_rule_type_field(self):
        v = RuleViolation(
            rule_name="test",
            subject="http://ex.org/X",
            message="failed",
            check_type="sparql",
            rule_type="sparql",
        )
        assert v.rule_type == "sparql"

    def test_rule_type_default(self):
        v = RuleViolation(
            rule_name="test",
            subject="http://ex.org/X",
            message="failed",
            check_type="if_then",
        )
        assert v.rule_type == ""

    def test_to_dict_includes_rule_type(self):
        r = ReasoningResult(
            violations=[
                RuleViolation("r1", "s1", "msg", "if_then", rule_type="if_then"),
            ],
        )
        d = r.to_dict()
        assert d["violations"][0]["rule_type"] == "if_then"


# ===========================================================================
# Cross-cutting: ReasoningService orchestration
# ===========================================================================


class TestReasoningServiceNewPhases:
    def _domain_session(self, **overrides):
        domain = MagicMock()
        ontology = {
            "name": "TestOntology",
            "base_uri": "http://test.org/ontology#",
            "classes": [],
            "properties": [],
            "constraints": [],
            "if_then_rules": [],
            "shacl_shapes": [],
            "sparql_rules": [],
        }
        ontology.update(overrides.get("ontology_extras", {}))
        domain.ontology = ontology
        domain.generated_owl = overrides.get("owl", "")
        domain.if_then_rules = ontology.get("if_then_rules", [])
        domain.info = {"name": "test_graph"}
        domain.current_version = "1"
        return domain

    def test_sparql_rules_skipped_when_empty(self):
        svc = ReasoningService(self._domain_session())
        result = svc.run_sparql_rules()
        assert result.stats.get("skipped") is True

    def test_sparql_rules_skipped_no_store(self):
        domain = self._domain_session(
            ontology_extras={
                "sparql_rules": [
                    {"id": "sr1", "name": "Test", "query": "CONSTRUCT {} WHERE {}"}
                ],
            },
        )
        svc = ReasoningService(domain, triplestore_backend=None)
        result = svc.run_sparql_rules()
        assert result.stats.get("skipped") is True

    def test_full_reasoning_new_phases_disabled_by_default(self):
        domain = self._domain_session()
        svc = ReasoningService(domain)
        result = svc.run_full_reasoning({"tbox": False, "if_then": False, "graph": False})
        assert result.stats.get("sparql_rules_skipped") is True

    def test_full_reasoning_enables_new_phases(self):
        domain = self._domain_session()
        svc = ReasoningService(domain)
        result = svc.run_full_reasoning(
            {
                "tbox": False,
                "if_then": False,
                "graph": False,
                "constraints": False,
                "sparql_rules": True,
            }
        )
        assert (
            "sparql_rules_skipped" in result.stats
            or "sparql_rules_duration_seconds" in result.stats
        )
