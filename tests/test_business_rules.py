"""Tests for all business rule enhancements.

Covers:
- Phase 1: SWRL built-ins (registry, parser, SQL/Cypher translation)
- Phase 2: SHACL rules (service method)
- Phase 3: Closed-world negation (parser, SQL/Cypher translation)
- Phase 4: Decision tables (engine, SQL/Cypher generation, validation)
- Phase 5: SPARQL CONSTRUCT rules (engine, SQL generation, validation)
- Phase 6: Aggregate rules (engine, SQL/Cypher generation, validation)
- Cross-cutting: RuleViolation model, ReasoningService orchestration
"""
import pytest
from unittest.mock import MagicMock, patch

from back.core.reasoning.models import InferredTriple, ReasoningResult, RuleViolation
from back.core.reasoning.SWRLParser import SWRLParser
from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator
from back.core.reasoning.SWRLFlatCypherTranslator import SWRLFlatCypherTranslator
from back.core.reasoning.SWRLBuiltinRegistry import SWRLBuiltin, SWRLBuiltinRegistry
from back.core.reasoning.DecisionTableEngine import DecisionTableEngine
from back.core.reasoning.SPARQLRuleEngine import SPARQLRuleEngine
from back.core.reasoning.AggregateRuleEngine import AggregateRuleEngine
from back.core.reasoning.ReasoningService import ReasoningService


# ===========================================================================
# Phase 1: SWRL Built-ins
# ===========================================================================

class TestSWRLBuiltinRegistry:
    def test_get_builtin_comparison(self):
        bi = SWRLBuiltinRegistry.get("greaterThan")
        assert bi is not None
        assert bi.arity == 2
        assert bi.category == "comparison"

    def test_get_builtin_case_insensitive(self):
        assert SWRLBuiltinRegistry.get("GREATERTHAN") is not None
        assert SWRLBuiltinRegistry.get("GreaterThan") is not None

    def test_is_builtin(self):
        assert SWRLBuiltinRegistry.is_builtin("greaterThan")
        assert SWRLBuiltinRegistry.is_builtin("add")
        assert SWRLBuiltinRegistry.is_builtin("startsWith")
        assert SWRLBuiltinRegistry.is_builtin("before")
        assert not SWRLBuiltinRegistry.is_builtin("Customer")
        assert not SWRLBuiltinRegistry.is_builtin("hasAge")

    def test_all_builtins_non_empty(self):
        builtins = SWRLBuiltinRegistry.all()
        assert len(builtins) > 20

    def test_builtins_by_category(self):
        cats = SWRLBuiltinRegistry.by_category()
        assert "comparison" in cats
        assert "math" in cats
        assert "string" in cats
        assert "date" in cats
        assert len(cats["comparison"]) >= 6

    def test_literal_detection(self):
        assert SWRLBuiltinRegistry.is_literal("18")
        assert SWRLBuiltinRegistry.is_literal('"hello"')
        assert SWRLBuiltinRegistry.is_literal("'world'")
        assert not SWRLBuiltinRegistry.is_literal("?x")
        assert not SWRLBuiltinRegistry.is_literal("?age")

    def test_literal_sql(self):
        assert SWRLBuiltinRegistry.literal_sql("18") == "18"
        assert SWRLBuiltinRegistry.literal_sql('"hello"') == "'hello'"
        assert SWRLBuiltinRegistry.literal_sql("Gold") == "'Gold'"

    def test_literal_cypher(self):
        assert SWRLBuiltinRegistry.literal_cypher("18") == "18"
        assert SWRLBuiltinRegistry.literal_cypher('"hello"') == '"hello"'
        assert SWRLBuiltinRegistry.literal_cypher("Gold") == '"Gold"'

    def test_builtin_sql_templates(self):
        bi = SWRLBuiltinRegistry.get("greaterThan")
        result = bi.sql_template.format("col1", "42")
        assert "col1" in result
        assert "42" in result
        assert ">" in result

    def test_builtin_cypher_templates(self):
        bi = SWRLBuiltinRegistry.get("contains")
        result = bi.cypher_template.format("x.name", '"VIP"')
        assert "contains" in result


class TestParseSwrlAtomsWithBuiltins:
    def test_builtin_atoms_detected(self):
        expr = "Customer(?x), hasAge(?x, ?age), greaterThan(?age, 65)"
        atoms = SWRLParser.parse_atoms(expr)
        assert len(atoms) == 3
        builtin_atoms = [a for a in atoms if a.get("builtin")]
        assert len(builtin_atoms) == 1
        assert builtin_atoms[0]["name"] == "greaterThan"
        assert builtin_atoms[0]["args"] == ["?age", "65"]

    def test_negated_atoms_detected(self):
        expr = "Customer(?x), not(hasPayment(?x, ?y))"
        atoms = SWRLParser.parse_atoms(expr)
        assert len(atoms) == 2
        negated = [a for a in atoms if a.get("negated")]
        assert len(negated) == 1
        assert negated[0]["name"] == "hasPayment"
        assert negated[0]["args"] == ["?x", "?y"]

    def test_mixed_atoms(self):
        expr = "Person(?x), hasAge(?x, ?a), greaterThan(?a, 18), not(hasLicense(?x, ?l))"
        atoms = SWRLParser.parse_atoms(expr)
        regular = [a for a in atoms if not a.get("builtin") and not a.get("negated")]
        builtins = [a for a in atoms if a.get("builtin")]
        negated = [a for a in atoms if a.get("negated")]
        assert len(regular) == 2
        assert len(builtins) == 1
        assert len(negated) == 1

    def test_no_builtins_backward_compatible(self):
        expr = "Customer(?x), hasOrder(?x, ?o), Order(?o)"
        atoms = SWRLParser.parse_atoms(expr)
        assert all(not a.get("builtin") for a in atoms)
        assert all(not a.get("negated") for a in atoms)
        assert len(atoms) == 3


class TestSWRLSQLBuiltinTranslation:
    def test_violation_sql_with_builtin(self):
        translator = SWRLSQLTranslator()
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


class TestSWRLFlatCypherBuiltinTranslation:
    def test_violation_cypher_with_builtin(self):
        translator = SWRLFlatCypherTranslator(node_table="Triple")
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
        cypher = translator.build_violation_query(params)
        assert cypher is not None
        assert "RETURN DISTINCT" in cypher


# ===========================================================================
# Phase 3: Closed-World Negation
# ===========================================================================

class TestNegationSQL:
    def test_negated_antecedent_sql(self):
        translator = SWRLSQLTranslator()
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

    def test_negated_antecedent_cypher(self):
        translator = SWRLFlatCypherTranslator(node_table="Triple")
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
        cypher = translator.build_violation_query(params)
        assert cypher is not None
        assert "NOT EXISTS" in cypher


# ===========================================================================
# Phase 4: Decision Tables
# ===========================================================================

class TestDecisionTableEngine:
    @pytest.fixture
    def sample_table(self):
        return {
            "id": "dt_test",
            "name": "Customer Discount",
            "target_class": "Customer",
            "target_class_uri": "http://test.org/ontology#Customer",
            "input_columns": [
                {"property": "tier", "property_uri": "http://test.org/ontology/tier", "label": "Tier"},
                {"property": "amount", "property_uri": "http://test.org/ontology/amount", "label": "Amount"},
            ],
            "output_column": {"property": "discount", "property_uri": "http://test.org/ontology/discount"},
            "rows": [
                {"conditions": [{"op": "eq", "value": "Gold"}, {"op": "gt", "value": "1000"}], "action": "15"},
                {"conditions": [{"op": "eq", "value": "Gold"}, {"op": "lte", "value": "1000"}], "action": "10"},
                {"conditions": [{"op": "any"}, {"op": "any"}], "action": "0"},
            ],
            "hit_policy": "first",
            "enabled": True,
        }

    def test_build_violation_sql(self, sample_table):
        engine = DecisionTableEngine()
        sql = engine.build_violation_sql(sample_table, "triples", "http://test.org/ontology#")
        assert sql is not None
        assert "SELECT DISTINCT" in sql
        assert "http://test.org/ontology#Customer" in sql
        assert "http://test.org/ontology/tier" in sql

    def test_build_violation_cypher(self, sample_table):
        engine = DecisionTableEngine()
        cypher = engine.build_violation_cypher(sample_table, "Triple", "http://test.org/ontology#")
        assert cypher is not None
        assert "RETURN DISTINCT" in cypher

    def test_validate_valid_table(self, sample_table):
        errors = DecisionTableEngine.validate_table(sample_table)
        assert errors == []

    def test_validate_missing_name(self):
        errors = DecisionTableEngine.validate_table({
            "target_class": "X", "input_columns": [{}], "rows": [{"conditions": [{}]}],
        })
        assert any("name" in e.lower() for e in errors)

    def test_validate_missing_rows(self):
        errors = DecisionTableEngine.validate_table({
            "name": "Test", "target_class": "X", "input_columns": [{}],
        })
        assert any("row" in e.lower() for e in errors)


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
        errors = SPARQLRuleEngine.validate_rule({"name": "Test", "query": "SELECT * WHERE { ?s ?p ?o }"})
        assert any("CONSTRUCT" in e for e in errors)

    def test_construct_to_sql(self, sample_rule):
        engine = SPARQLRuleEngine()
        sql = engine._construct_to_sql(
            sample_rule["query"], "triples",
            {"base_uri": "http://test.org/ontology#"},
        )
        assert sql is not None
        assert "SELECT DISTINCT" in sql


# ===========================================================================
# Phase 6: Aggregate Rules
# ===========================================================================

class TestAggregateRuleEngine:
    @pytest.fixture
    def sample_rule(self):
        return {
            "id": "ar_test",
            "name": "High Order Count",
            "target_class": "Customer",
            "target_class_uri": "http://test.org/ontology#Customer",
            "group_by_property": "hasOrder",
            "group_by_property_uri": "http://test.org/ontology/hasOrder",
            "aggregate_property": "",
            "aggregate_property_uri": "",
            "aggregate_function": "count",
            "operator": "gt",
            "threshold": "5",
            "result_class": "FrequentBuyer",
            "result_class_uri": "http://test.org/ontology#FrequentBuyer",
            "enabled": True,
        }

    def test_build_sql(self, sample_rule):
        engine = AggregateRuleEngine()
        sql = engine.build_sql(sample_rule, "triples", "http://test.org/ontology#")
        assert sql is not None
        assert "GROUP BY" in sql
        assert "HAVING" in sql
        assert "> 5" in sql

    def test_build_cypher(self, sample_rule):
        engine = AggregateRuleEngine()
        cypher = engine.build_cypher(sample_rule, "Triple", "http://test.org/ontology#")
        assert cypher is not None
        assert "count" in cypher.lower()
        assert "> 5" in cypher

    def test_validate_valid_rule(self, sample_rule):
        errors = AggregateRuleEngine.validate_rule(sample_rule)
        assert errors == []

    def test_validate_missing_name(self):
        errors = AggregateRuleEngine.validate_rule({
            "target_class": "X", "aggregate_function": "count",
            "group_by_property": "p",
        })
        assert any("name" in e.lower() for e in errors)

    def test_validate_invalid_function(self):
        errors = AggregateRuleEngine.validate_rule({
            "name": "Test", "target_class": "X",
            "aggregate_function": "median",
            "group_by_property": "p",
        })
        assert any("function" in e.lower() for e in errors)

    def test_build_sql_with_agg_property(self):
        engine = AggregateRuleEngine()
        rule = {
            "target_class_uri": "http://test.org/Dept",
            "group_by_property_uri": "http://test.org/hasMember",
            "aggregate_property_uri": "http://test.org/tenure",
            "aggregate_function": "avg",
            "operator": "lt",
            "threshold": "2",
        }
        sql = engine.build_sql(rule, "triples", "http://test.org/")
        assert sql is not None
        assert "AVG" in sql
        assert "< 2" in sql

    def test_build_sql_count_only(self):
        """COUNT entities of a type with no group-by or aggregate property."""
        engine = AggregateRuleEngine()
        rule = {
            "target_class_uri": "http://test.org/Customer",
            "group_by_property_uri": "",
            "aggregate_property_uri": "",
            "aggregate_function": "count",
            "operator": "gt",
            "threshold": "100",
        }
        sql = engine.build_sql(rule, "triples", "http://test.org/")
        assert sql is not None
        assert "COUNT" in sql
        assert "> 100" in sql

    def test_build_cypher_count_only(self):
        engine = AggregateRuleEngine()
        rule = {
            "target_class_uri": "http://test.org/Customer",
            "group_by_property_uri": "",
            "aggregate_property_uri": "",
            "aggregate_function": "count",
            "operator": "gte",
            "threshold": "50",
        }
        cypher = engine.build_cypher(rule, "Triple", "http://test.org/")
        assert cypher is not None
        assert "count" in cypher.lower()
        assert ">= 50" in cypher

    def test_build_sql_group_by_sum(self):
        """Group-by only with SUM should use SUM, not COUNT."""
        engine = AggregateRuleEngine()
        rule = {
            "target_class_uri": "http://test.org/Customer",
            "group_by_property_uri": "http://test.org/orderAmount",
            "aggregate_property_uri": "",
            "aggregate_function": "sum",
            "operator": "gt",
            "threshold": "1000",
        }
        sql = engine.build_sql(rule, "triples", "http://test.org/")
        assert sql is not None
        assert "SUM" in sql
        assert "COUNT" not in sql

    def test_build_cypher_group_by_sum(self):
        engine = AggregateRuleEngine()
        rule = {
            "target_class_uri": "http://test.org/Customer",
            "group_by_property_uri": "http://test.org/orderAmount",
            "aggregate_property_uri": "",
            "aggregate_function": "sum",
            "operator": "gt",
            "threshold": "1000",
        }
        cypher = engine.build_cypher(rule, "Triple", "http://test.org/")
        assert cypher is not None
        assert "sum" in cypher.lower()

    def test_validate_count_only_no_properties(self):
        """COUNT-only rules are valid without group_by or aggregate_property."""
        errors = AggregateRuleEngine.validate_rule({
            "name": "CountCustomers",
            "target_class": "Customer",
            "aggregate_function": "count",
            "operator": "gt",
        })
        assert errors == []

    def test_validate_sum_requires_property(self):
        """Non-COUNT functions require at least one property."""
        errors = AggregateRuleEngine.validate_rule({
            "name": "SumWithout",
            "target_class": "Customer",
            "aggregate_function": "sum",
            "operator": "gt",
        })
        assert any("group_by" in e.lower() or "aggregate" in e.lower() for e in errors)

    def test_resolve_rule_with_data_properties(self):
        """dataProperties on classes should be resolved in the URI map."""
        ontology = {
            "base_uri": "http://test.org/ontology#",
            "classes": [{
                "name": "Customer",
                "uri": "http://test.org/ontology#Customer",
                "dataProperties": [
                    {"name": "age", "uri": "http://test.org/ontology/age"},
                    {"name": "score", "localName": "score"},
                ],
            }],
            "properties": [],
        }
        rule = {
            "target_class": "Customer",
            "group_by_property": "age",
            "aggregate_property": "score",
            "aggregate_function": "avg",
            "operator": "gt",
            "threshold": "5",
        }
        resolved = AggregateRuleEngine._resolve_rule(rule, ontology)
        assert resolved["target_class_uri"] == "http://test.org/ontology#Customer"
        assert resolved["group_by_property_uri"] == "http://test.org/ontology/age"
        assert "score" in resolved["aggregate_property_uri"]


# ===========================================================================
# Cross-cutting: Models
# ===========================================================================

class TestRuleViolationModel:
    def test_rule_type_field(self):
        v = RuleViolation(
            rule_name="test",
            subject="http://ex.org/X",
            message="failed",
            check_type="decision_table",
            rule_type="decision_table",
        )
        assert v.rule_type == "decision_table"

    def test_rule_type_default(self):
        v = RuleViolation(
            rule_name="test",
            subject="http://ex.org/X",
            message="failed",
            check_type="swrl",
        )
        assert v.rule_type == ""

    def test_to_dict_includes_rule_type(self):
        r = ReasoningResult(
            violations=[
                RuleViolation("r1", "s1", "msg", "swrl", rule_type="swrl"),
            ],
        )
        d = r.to_dict()
        assert d["violations"][0]["rule_type"] == "swrl"


# ===========================================================================
# Cross-cutting: ReasoningService orchestration
# ===========================================================================

class TestReasoningServiceNewPhases:
    def _project_session(self, **overrides):
        proj = MagicMock()
        ontology = {
            "name": "TestOntology",
            "base_uri": "http://test.org/ontology#",
            "classes": [],
            "properties": [],
            "constraints": [],
            "swrl_rules": [],
            "shacl_shapes": [],
            "decision_tables": [],
            "sparql_rules": [],
            "aggregate_rules": [],
        }
        ontology.update(overrides.get("ontology_extras", {}))
        proj.ontology = ontology
        proj.generated_owl = overrides.get("owl", "")
        proj.swrl_rules = ontology.get("swrl_rules", [])
        proj.info = {"name": "test_graph"}
        proj.current_version = "1"
        return proj

    def test_decision_tables_skipped_when_empty(self):
        svc = ReasoningService(self._project_session())
        result = svc.run_decision_tables()
        assert result.stats.get("skipped") is True

    def test_sparql_rules_skipped_when_empty(self):
        svc = ReasoningService(self._project_session())
        result = svc.run_sparql_rules()
        assert result.stats.get("skipped") is True

    def test_aggregate_rules_skipped_when_empty(self):
        svc = ReasoningService(self._project_session())
        result = svc.run_aggregate_rules()
        assert result.stats.get("skipped") is True

    def test_decision_tables_skipped_no_store(self):
        proj = self._project_session(
            ontology_extras={
                "decision_tables": [{"id": "dt1", "name": "Test", "rows": [{}]}],
            },
        )
        svc = ReasoningService(proj, triplestore_backend=None)
        result = svc.run_decision_tables()
        assert result.stats.get("skipped") is True

    def test_full_reasoning_new_phases_disabled_by_default(self):
        proj = self._project_session()
        svc = ReasoningService(proj)
        result = svc.run_full_reasoning({"tbox": False, "swrl": False, "graph": False})
        assert result.stats.get("decision_tables_skipped") is True
        assert result.stats.get("sparql_rules_skipped") is True
        assert result.stats.get("aggregate_rules_skipped") is True

    def test_full_reasoning_enables_new_phases(self):
        proj = self._project_session()
        svc = ReasoningService(proj)
        result = svc.run_full_reasoning({
            "tbox": False, "swrl": False, "graph": False,
            "constraints": False,
            "decision_tables": True,
            "sparql_rules": True,
            "aggregate_rules": True,
        })
        assert "decision_tables_skipped" in result.stats or "decision_tables_duration_seconds" in result.stats
