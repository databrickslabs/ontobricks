"""Tests for the flat-triple SQL backend, rule parser, and SHACL rule engine."""

# Importing the package root first avoids a pre-existing circular import
# between ``back.core.reasoning`` and ``back.core.w3c`` when this module is
# collected on its own.
import back.objects  # noqa: F401
from back.core.reasoning.results.InferredTriple import InferredTriple
from back.core.reasoning.results.ReasoningResult import ReasoningResult
from back.core.reasoning.shacl_rules.ShaclFlatSqlBackend import ShaclFlatSqlBackend
from back.core.reasoning.shacl_rules.RuleExpressionParser import RuleExpressionParser


# -- Atom parser tests ----------------------------------------------------


class TestParseIfThenAtoms:
    def test_class_atom(self):
        atoms = RuleExpressionParser.parse_atoms("Customer(?x)")
        assert len(atoms) == 1
        assert atoms[0]["name"] == "Customer"
        assert atoms[0]["args"] == ["?x"]
        assert atoms[0]["arity"] == 1

    def test_property_atom(self):
        atoms = RuleExpressionParser.parse_atoms("hasClaim(?x, ?y)")
        assert len(atoms) == 1
        assert atoms[0]["name"] == "hasClaim"
        assert atoms[0]["args"] == ["?x", "?y"]
        assert atoms[0]["arity"] == 2

    def test_conjunction(self):
        atoms = RuleExpressionParser.parse_atoms(
            "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)"
        )
        assert len(atoms) == 3
        assert atoms[0]["name"] == "Customer"
        assert atoms[1]["name"] == "hasClaim"
        assert atoms[2]["name"] == "Claim"

    def test_empty(self):
        assert RuleExpressionParser.parse_atoms("") == []

    def test_dotted_names(self):
        atoms = RuleExpressionParser.parse_atoms("ns.ClassName(?x)")
        assert atoms[0]["name"] == "ns.ClassName"


# -- URI resolution tests -------------------------------------------------


class TestResolveIfThenUri:
    def test_full_uri_passthrough(self):
        assert RuleExpressionParser.resolve_uri("http://ex.org/Foo", "") == "http://ex.org/Foo"

    def test_uri_map_lookup(self):
        uri_map = {"customer": "http://ex.org/Customer"}
        assert (
            RuleExpressionParser.resolve_uri("Customer", "", uri_map) == "http://ex.org/Customer"
        )

    def test_base_uri_fallback(self):
        assert RuleExpressionParser.resolve_uri("Foo", "http://ex.org") == "http://ex.org#Foo"

    def test_base_uri_with_hash(self):
        assert RuleExpressionParser.resolve_uri("Foo", "http://ex.org#") == "http://ex.org#Foo"

    def test_base_uri_with_slash(self):
        assert RuleExpressionParser.resolve_uri("Foo", "http://ex.org/") == "http://ex.org/Foo"


# -- SQL Translator tests -------------------------------------------------


class TestShaclFlatSqlBackend:
    def setup_method(self):
        self.translator = ShaclFlatSqlBackend()

    def test_simple_rule_violation(self):
        params = {
            "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
            "consequent": "hasContract(?x, ?z)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        sql = self.translator.build_violation_sql("triples", params)
        assert sql is not None
        assert "SELECT DISTINCT" in sql
        assert "FROM triples" in sql
        assert "NOT EXISTS" in sql

    def test_missing_antecedent_returns_none(self):
        params = {"antecedent": "", "consequent": "Foo(?x)", "base_uri": ""}
        assert self.translator.build_violation_sql("t", params) is None

    def test_missing_consequent_returns_none(self):
        params = {"antecedent": "Foo(?x)", "consequent": "", "base_uri": ""}
        assert self.translator.build_violation_sql("t", params) is None

    def test_no_property_atoms_generates_sql(self):
        """Class-only antecedent (e.g. Customer(?x)) with consequent
        hasContract(?x,?z) generates a NOT EXISTS violation query."""
        params = {
            "antecedent": "Customer(?x)",
            "consequent": "hasContract(?x, ?z)",
            "base_uri": "http://ex.org",
        }
        sql = self.translator.build_violation_sql("t", params)
        assert sql is not None
        assert "NOT EXISTS" in sql
        assert "hasContract" in sql

    def test_materialization_sql(self):
        params = {
            "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y) \u2227 Claim(?y)",
            "consequent": "hasContract(?x, ?z)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        sql = self.translator.build_materialization_sql("triples", params)
        assert sql is not None
        assert "INSERT INTO triples" in sql

    def test_inference_translates_string_builtin_to_filter(self):
        params = {
            "antecedent": (
                'Customer(?x) \u2227 Contract(?z) \u2227 Meter(?w) '
                '\u2227 hasContract(?x, ?z) \u2227 hasMeter(?z, ?w) '
                '\u2227 status(?z, ?status) \u2227 builtin:contains(?status, "active")'
            ),
            "consequent": "owns(?x, ?w)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }

        sql = self.translator.build_inference_sql("triples", params)

        assert sql is not None
        assert "object LIKE CONCAT(" in sql
        assert "'active'" in sql
        assert "predicate = 'http://ex.org#contains'" not in sql

    def test_inference_postgres_avoids_spark_try_cast(self):
        from back.core.helpers import SQL_DIALECT_POSTGRES

        params = {
            "antecedent": (
                "Customer(?x) \u2227 hasScore(?x, ?s) "
                "\u2227 builtin:greaterThan(?s, 500)"
            ),
            "consequent": "HighValue(?x)",
            "base_uri": "http://ex.org",
            "uri_map": {},
        }
        sql = ShaclFlatSqlBackend(dialect=SQL_DIALECT_POSTGRES).build_inference_sql(
            "triples", params
        )
        assert sql is not None
        assert "TRY_CAST" not in sql
        assert "::float8" in sql


# -- Built-in registry tests -----------------------------------------------


class TestBuiltinNumericCasts:
    """Built-ins cast triple values, which are strings, to numbers or dates.

    Under ANSI mode a bare ``CAST`` aborts the statement on the first value
    that will not convert, so one malformed row would fail the whole rule
    instead of simply not matching.
    """

    def test_no_builtin_emits_a_bare_cast(self):
        from back.core.reasoning.shacl_rules.RuleBuiltinRegistry import (
            RuleBuiltinRegistry,
        )

        offenders = [
            b.name
            for b in RuleBuiltinRegistry.all().values()
            if "CAST(" in b.sql_template.replace("TRY_CAST(", "")
        ]
        assert offenders == []

    def test_greater_than_uses_try_cast(self):
        from back.core.reasoning.shacl_rules.RuleBuiltinRegistry import (
            RuleBuiltinRegistry,
        )

        builtin = RuleBuiltinRegistry.get("greaterThan")
        assert (
            builtin.sql_template
            == "TRY_CAST({0} AS DOUBLE) > TRY_CAST({1} AS DOUBLE)"
        )

    def test_postgres_templates_drop_spark_only_syntax(self):
        from back.core.helpers import SQL_DIALECT_POSTGRES
        from back.core.reasoning.shacl_rules.RuleBuiltinRegistry import (
            RuleBuiltinRegistry,
        )

        offenders = []
        for builtin in RuleBuiltinRegistry.all().values():
            sql = RuleBuiltinRegistry.sql_template_for(builtin, SQL_DIALECT_POSTGRES)
            if "TRY_CAST" in sql or "RLIKE" in sql or "CURRENT_TIMESTAMP()" in sql:
                offenders.append(builtin.name)
        assert offenders == []


# -- ShaclRuleEngine tests -------------------------------------------------


class TestShaclRuleEngine:
    def test_execute_rules_with_mock_store(self):
        from back.core.reasoning.shacl_rules.ShaclRuleEngine import ShaclRuleEngine
        from unittest.mock import MagicMock

        store = MagicMock()
        store.execute_query.return_value = [
            {
                "subject": "http://ex.org/entity1",
                "predicate": "http://ex.org/hasContract",
                "object": "",
            },
        ]

        engine = ShaclRuleEngine(
            ontology={
                "base_uri": "http://ex.org",
                "classes": [{"name": "Customer"}],
                "properties": [{"name": "hasClaim"}, {"name": "hasContract"}],
            }
        )

        rules = [
            {
                "name": "test_rule",
                "antecedent": "Customer(?x) \u2227 hasClaim(?x, ?y)",
                "consequent": "hasContract(?x, ?z)",
            }
        ]

        result = engine.execute_rules(rules, store, "triples")
        assert result.stats["phase"] == "if_then"
        assert result.stats["rules_count"] == 1
        assert len(result.inferred_triples) >= 1

    def test_execute_empty_rules(self):
        from back.core.reasoning.shacl_rules.ShaclRuleEngine import ShaclRuleEngine
        from unittest.mock import MagicMock

        store = MagicMock()
        engine = ShaclRuleEngine()
        result = engine.execute_rules([], store, "triples")
        assert len(result.inferred_triples) == 0
        assert result.stats["rules_count"] == 0


# -- ReasoningResult deduplication tests -----------------------------------


class TestReasoningResultDeduplication:
    def test_deduplicate_removes_cross_phase_duplicates(self):
        result = ReasoningResult(
            inferred_triples=[
                InferredTriple("s1", "p1", "o1", "tbox"),
                InferredTriple("s1", "p1", "o1", "if_then:Rule1"),
                InferredTriple("s1", "p1", "o1", "graph:transitive"),
                InferredTriple("s2", "p2", "o2", "tbox"),
            ]
        )
        removed = result.deduplicate()
        assert removed == 2
        assert len(result.inferred_triples) == 2
        assert result.inferred_triples[0].provenance == "tbox"
        assert result.inferred_triples[1].subject == "s2"

    def test_deduplicate_no_duplicates(self):
        result = ReasoningResult(
            inferred_triples=[
                InferredTriple("s1", "p1", "o1", "tbox"),
                InferredTriple("s2", "p2", "o2", "if_then:R1"),
            ]
        )
        removed = result.deduplicate()
        assert removed == 0
        assert len(result.inferred_triples) == 2

    def test_deduplicate_empty(self):
        result = ReasoningResult()
        assert result.deduplicate() == 0

    def test_merge_then_deduplicate(self):
        r1 = ReasoningResult(
            inferred_triples=[
                InferredTriple("s1", "p1", "o1", "tbox"),
                InferredTriple("s2", "p2", "o2", "tbox"),
            ]
        )
        r2 = ReasoningResult(
            inferred_triples=[
                InferredTriple("s1", "p1", "o1", "if_then:R1"),
                InferredTriple("s3", "p3", "o3", "if_then:R1"),
            ]
        )
        r1.merge(r2)
        assert len(r1.inferred_triples) == 4
        removed = r1.deduplicate()
        assert removed == 1
        assert len(r1.inferred_triples) == 3
