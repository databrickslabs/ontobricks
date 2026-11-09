"""SQL string literals built by the SQL-emitting engines stay closed.

Spark treats a backslash as an escape inside ``'...'``, so a value ending in
``\\`` breaks out of a literal that only doubles quotes. Postgres (with
``standard_conforming_strings``) keeps backslashes verbatim.
"""

from unittest.mock import MagicMock

import pytest

from back.core.graphdb.delta.DeltaFlatStore import DeltaFlatStore
from back.core.graphdb.lakebase.LakebaseFlatStore import LakebaseFlatStore
from back.core.helpers import SQLHelpers
from back.core.reasoning.rules.AggregateRuleEngine import AggregateRuleEngine
from back.core.reasoning.rules.DecisionTableEngine import DecisionTableEngine
from back.core.reasoning.rules.SPARQLRuleEngine import SPARQLRuleEngine
from back.core.reasoning.swrl.SWRLSQLTranslator import SWRLSQLTranslator
from back.core.w3c.shacl.SHACLService import SHACLService
from back.core.w3c.sparql.SparqlValueFilterSql import SparqlValueFilterSql

pytestmark = pytest.mark.unit

PAYLOAD = "x' OR 1=1 --\\"
BASE = "http://test.org/ontology#"
CLS = "http://test.org/ontology#Customer"
PROP = "http://test.org/ontology/tier"


def _literals(sql: str, dialect: str = "spark") -> list:
    """Decode every ``'...'`` literal; fail if one is left unterminated."""
    found, i = [], 0
    while i < len(sql):
        if sql[i] != "'":
            i += 1
            continue
        i += 1
        buf = []
        while True:
            assert i < len(sql), f"unterminated literal in: {sql}"
            ch = sql[i]
            if ch == "\\" and dialect == "spark":
                buf.append(sql[i + 1])
                i += 2
            elif ch == "'" and sql[i + 1 : i + 2] == "'":
                buf.append("'")
                i += 2
            elif ch == "'":
                i += 1
                break
            else:
                buf.append(ch)
                i += 1
        found.append("".join(buf))
    return found


class TestCanonicalHelper:
    def test_spark_is_the_default_dialect(self):
        assert SQLHelpers.sql_escape(PAYLOAD) == "x'' OR 1=1 --\\\\"

    def test_postgres_keeps_backslashes(self):
        assert SQLHelpers.sql_escape(PAYLOAD, "postgres") == "x'' OR 1=1 --\\"

    @pytest.mark.parametrize("dialect", ["spark", "postgres"])
    def test_literal_round_trips(self, dialect):
        literal = SQLHelpers.sql_literal(PAYLOAD, dialect)
        assert _literals(literal, dialect) == [PAYLOAD]

    def test_ordinary_literal_unchanged(self):
        assert SQLHelpers.sql_literal("Gold") == "'Gold'"

    @pytest.mark.parametrize("value", ["42", "-3.5", "1e10", "2.5E-3", 7, 2.5])
    def test_numeric_literal_accepted(self, value):
        assert SQLHelpers.is_sql_numeric(value) is True

    @pytest.mark.parametrize(
        "value",
        ["nan", "NaN", "inf", "-inf", "Infinity", float("nan"), float("inf"),
         "1_000", " 5", "5\n", "", "1 OR 1=1", "0x1F", None, True],
    )
    def test_non_numeric_rejected(self, value):
        assert SQLHelpers.is_sql_numeric(value) is False


class TestSPARQLRuleEngine:
    @staticmethod
    def _sql(where: str, dialect: str = "spark") -> str:
        query = (
            f"CONSTRUCT {{ ?p rdf:type <http://test.org/{PAYLOAD}> }} "
            f"WHERE {{ ?p rdf:type <{CLS}> . ?p <http://test.org/amt> ?amt . {where} }}"
        )
        return SPARQLRuleEngine()._construct_to_sql(
            query, "triples", {"base_uri": BASE}, dialect=dialect
        )

    def test_construct_iri_stays_closed(self):
        assert f"http://test.org/{PAYLOAD}" in _literals(self._sql(""))

    def test_where_iri_stays_closed(self):
        query = (
            f"CONSTRUCT {{ ?p rdf:type <{CLS}> }} "
            f"WHERE {{ ?p <http://t/{PAYLOAD}> <http://o/{PAYLOAD}> }}"
        )
        sql = SPARQLRuleEngine()._construct_to_sql(query, "triples", {"base_uri": BASE})
        literals = _literals(sql)
        assert f"http://t/{PAYLOAD}" in literals
        assert f"http://o/{PAYLOAD}" in literals

    def test_filter_string_stays_closed(self):
        assert PAYLOAD in _literals(self._sql(f"FILTER(?amt = {PAYLOAD})"))

    def test_postgres_dialect_keeps_backslash(self):
        sql = self._sql(f"FILTER(?amt = {PAYLOAD})", dialect="postgres")
        assert PAYLOAD in _literals(sql, "postgres")

    @pytest.mark.parametrize("token", ["nan", "inf", "1_000"])
    def test_non_finite_filter_is_quoted(self, token):
        sql = self._sql(f"FILTER(?amt > {token})")
        assert f"w1.object > '{token}'" in sql

    def test_numeric_filter_unchanged(self):
        sql = self._sql("FILTER(?amt > 100000)")
        assert "TRY_CAST(w1.object AS DOUBLE) > 100000" in sql

    def test_ordinary_output_unchanged(self):
        sql = self._sql("")
        assert sql.startswith(
            "SELECT DISTINCT w0.subject AS s, "
            "'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' AS p"
        )
        assert f"w0.object = '{CLS}'" in sql

    def test_store_flavor_selects_dialect(self):
        store = MagicMock()
        store.sql_flavor.return_value = "postgres"
        store.sql_table_reference.return_value = "triples"
        store.execute_query.return_value = []
        query = f"CONSTRUCT {{ ?p rdf:type <{CLS}> }} WHERE {{ ?p <http://t/{PAYLOAD}> ?o }}"
        SPARQLRuleEngine()._execute_one("r", query, store, "t", {"base_uri": BASE}, False)
        sql = store.execute_query.call_args.args[0]
        assert f"http://t/{PAYLOAD}" in _literals(sql, "postgres")


def _decision_table(value):
    return {
        "target_class_uri": CLS,
        "input_columns": [{"property": "tier", "property_uri": PROP}],
        "rows": [{"conditions": [{"op": "eq", "value": value}], "action": "x"}],
    }


class TestDecisionTableEngine:
    def test_condition_value_stays_closed(self):
        sql = DecisionTableEngine().build_violation_sql(_decision_table(PAYLOAD), "t", BASE)
        assert PAYLOAD.lower() in _literals(sql)

    @pytest.mark.parametrize("token", ["nan", "inf"])
    def test_non_finite_value_is_quoted(self, token):
        sql = DecisionTableEngine().build_violation_sql(_decision_table(token), "t", BASE)
        assert f"LOWER(inp0.object) = '{token}'" in sql

    def test_numeric_value_unchanged(self):
        sql = DecisionTableEngine().build_violation_sql(_decision_table("100"), "t", BASE)
        assert "inp0.object = 100" in sql


class TestAggregateRuleEngine:
    @staticmethod
    def _rule(**overrides):
        rule = {
            "target_class_uri": CLS,
            "aggregate_property_uri": PROP,
            "aggregate_function": "count",
            "operator": "gt",
            "threshold": "5",
        }
        rule.update(overrides)
        return rule

    def test_uri_stays_closed(self):
        sql = AggregateRuleEngine().build_sql(
            self._rule(target_class_uri=PAYLOAD), "t", BASE
        )
        assert PAYLOAD in _literals(sql)

    @pytest.mark.parametrize("threshold", ["0 OR 1=1", "nan", "inf"])
    def test_non_numeric_threshold_is_rejected(self, threshold):
        assert AggregateRuleEngine().build_sql(self._rule(threshold=threshold), "t", BASE) is None

    def test_non_numeric_threshold_fails_validation(self):
        rule = {"name": "r", "target_class": "C", "aggregate_property": "p",
                "threshold": "0 OR 1=1"}
        assert any("threshold" in e.lower() for e in AggregateRuleEngine.validate_rule(rule))

    def test_numeric_threshold_unchanged(self):
        sql = AggregateRuleEngine().build_sql(self._rule(threshold=5), "t", BASE)
        assert sql.endswith("HAVING COUNT(TRY_CAST(t_agg.object AS DOUBLE)) > 5")


class TestSHACLService:
    @staticmethod
    def _pattern_shape(pattern):
        return {
            "shacl_type": "sh:pattern",
            "target_class_uri": CLS,
            "property_uri": PROP,
            "parameters": {"sh:pattern": pattern},
        }

    def test_pattern_stays_closed(self):
        sql = SHACLService.shape_to_sql(self._pattern_shape(PAYLOAD), "t")
        assert PAYLOAD in _literals(sql)

    def test_condition_value_stays_closed(self):
        shape = self._pattern_shape("^[A-Z]")
        shape["conditions"] = [
            {"property": "tier", "property_uri": PROP, "op": "eq", "value": PAYLOAD}
        ]
        sql = SHACLService.shape_to_sql(shape, "t")
        assert PAYLOAD.lower() in _literals(sql)

    def test_ordinary_pattern_unchanged(self):
        sql = SHACLService.shape_to_sql(self._pattern_shape("^[A-Z]"), "t")
        assert "NOT t2.object RLIKE '^[A-Z]'" in sql


class TestSparqlTranslatorValueFilters:
    @staticmethod
    def _where(value, dialect="spark"):
        return SparqlValueFilterSql.conditions_for(
            {PROP: "tier"},
            [{"predicate_uri": PROP, "operator": "equals", "value": value}],
            "",
            dialect,
        )

    @pytest.mark.parametrize("dialect", ["spark", "postgres"])
    def test_value_stays_closed(self, dialect):
        (cond,) = self._where(PAYLOAD, dialect)
        assert PAYLOAD in _literals(cond, dialect)

    def test_ordinary_value_unchanged(self):
        assert self._where("Gold") == ["TRY_CAST(tier AS STRING) = 'Gold'"]


class TestGraphDBBackendEscape:
    def test_delta_store_escapes_backslash(self):
        store = DeltaFlatStore.__new__(DeltaFlatStore)
        assert store._sql_escape(PAYLOAD) == SQLHelpers.sql_escape(PAYLOAD, "spark")

    def test_lakebase_store_keeps_backslash(self):
        store = LakebaseFlatStore.__new__(LakebaseFlatStore)
        assert store._sql_escape(PAYLOAD) == SQLHelpers.sql_escape(PAYLOAD, "postgres")


class TestSWRLSQLTranslatorEscape:
    def test_escape_uses_canonical_helper(self):
        assert SWRLSQLTranslator._escape(PAYLOAD) == SQLHelpers.sql_escape(PAYLOAD)
